"""Gateway error boundaries and independent QA questions; no live model calls."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import service as s
from test_service import FakeAccounts, prompt_claim, store
from test_qa import Answers, configure, question
from test_single_pass import gateway_controller


@pytest.mark.parametrize('event,code', [
    ({'code': 'test_timeout', 'error': 'private upstream details'}, 'gateway_timeout'),
    ({'code': 'stream_interrupted', 'error': 'private upstream details'}, 'stream_interrupted'),
    ({'error': 'Stream read error: read tcp secret-endpoint: use of closed network connection'}, 'stream_interrupted'),
    ({'error': 'Stream ended before response.completed'}, 'stream_interrupted'),
    ({'code': 'unknown', 'error': 'token secret-value'}, 'upstream_error'),
])
def test_gateway_error_classification_does_not_expose_raw_messages(tmp_path, monkeypatch, event, code):
    control = gateway_controller(tmp_path, monkeypatch, [{'type': 'error', **event}])
    with pytest.raises(s.ProbeError) as caught:
        control.prompt(7, 'gpt-6-astra', 'fixture')
    assert caught.value.code == code
    assert all(secret not in str(caught.value) for secret in ('private', 'secret', 'tcp', 'token'))


@pytest.mark.parametrize('failure', ['stream_interrupted', 'gateway_timeout', 'artifact_format', 'upstream_error'])
def test_failed_drawing_does_not_skip_tibo_or_repeat_either_question(store, failure):
    drawing = {**question('drawing', expected=''), 'kind': 'manual'}
    configure(store, [drawing, question('tibo', expected='True')], auto_pause_enabled=True, auto_pause_threshold=1)
    control = Answers(s.ProbeError('fixture failure', failure), 'False')
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), control)
    assert result['requests'] == result['request_limit'] == len(control.prompts) == 2
    assert result['status'] == 'yellow' and result['outcome'] == failure and not result['round_complete']
    first, second = result['cases']
    assert first['status'] == 'yellow' and first['outcome'] == failure
    assert second['status'] == 'red' and second['answer_preview'] == 'False'
    store.finish_prompt(job, result)
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert not control.calls


@pytest.mark.parametrize('failure', [s.RoundTimeout(), s.ProbeError('route changed', 'comparison_changed')])
def test_round_expiry_and_routing_changes_still_stop_later_questions(store, failure):
    configure(store, [{**question('drawing', expected=''), 'kind': 'manual'}, question('tibo')])
    control = Answers(failure, '21')
    result = s.run_prompt_round(prompt_claim(store), store, FakeAccounts(), control)
    assert result['requests'] == len(control.prompts) == 1
    assert result['outcome'] == failure.code


def test_short_flushed_error_is_received_before_gateway_finishes_closing_upstream(tmp_path, monkeypatch):
    """The old 256-byte read waits for EOF even after the gateway flushed its error."""
    key = tmp_path / 'admin-key'
    key.write_text('fixture')
    delivered = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            raw = b'data: {"type":"error","code":"stream_interrupted"}\n\n'
            self.wfile.write(('%x\r\n' % len(raw)).encode() + raw + b'\r\n')
            self.wfile.flush()
            delivered.wait(3)
            try:
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()
            except OSError:
                pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(s, 'PROBE_READ_TIMEOUT', .5)
    control = s.SchedulingControl(key, 'http://127.0.0.1:' + str(server.server_port))
    try:
        with pytest.raises(s.ProbeError) as caught:
            control.prompt(7, 'gpt-6-astra', 'fixture')
        assert caught.value.code == 'stream_interrupted'
    finally:
        delivered.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
