"""One-pass testing, answer-only limits, and safe document extraction."""
import json
import time

import pytest

import service as s
from test_service import FakeAccounts, Response, prompt_claim, store
from test_qa import Answers, configure, question


DOC = '<!doctype html>\n<html><head><meta charset="utf-8"></head><body>鹈鹕 🚲<svg><circle r="3"/></svg></body></html>'


def gateway_controller(tmp_path, monkeypatch, events):
    key = tmp_path / 'key'
    key.write_text('synthetic')

    class Stream(Response):
        status_code = 200
        headers = {'Content-Type': 'text/event-stream'}
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, **kwargs):
            body, headers = kwargs['json'], kwargs['headers']
            link = {'client_request_id': headers['X-Client-Request-Id'],
                    'upstream_attempt_id': 'synthetic-upstream',
                    'profile': headers['X-Sub2API-Comparison-Profile']}
            return Stream([
                {'type': 'test_request', 'data': {**link, 'details': {
                    'account_id': 7, 'transport': 'codex-http1-openssl36', 'body': {
                        'model': body['model_id'], 'reasoning': {'effort': 'xhigh'},
                        'instructions': s.COMPARISON_INSTRUCTIONS, 'tools': [], 'tool_choice': 'none',
                        'parallel_tool_calls': False, 'text': {'verbosity': 'medium'},
                        'input': [{'role': 'user', 'content': [{'type': 'input_text', 'text': body['prompt']}]}]}}}},
                {'type': 'test_response', 'data': {**link, 'details': {'http_status': 200}}},
                *events])

    monkeypatch.setattr(s.requests, 'Session', Session)
    return s.SchedulingControl(key)


def test_gateway_audit_over_two_mib_does_not_truncate_small_unicode_artwork(tmp_path, monkeypatch):
    events = [{'type': 'upstream_raw_line', 'data': {'details': 'x' * 600000}}] * 8
    events += [{'type': 'content', 'text': c} for c in DOC]
    events += [{'type': 'test_complete', 'success': True}]
    assert sum(len(json.dumps(e)) for e in events) > s.MAX_RESPONSE_BYTES
    controller = gateway_controller(tmp_path, monkeypatch, events)
    assert controller.prompt(7, 'gpt-6-astra', 'draw') == DOC


@pytest.mark.parametrize('mode,expected', [('answer', 'output_limit'), ('wire', 'stream_limit'), ('missing_end', 'test_error')])
def test_limits_and_complete_event_are_still_enforced(tmp_path, monkeypatch, mode, expected):
    if mode == 'answer':
        monkeypatch.setattr(s, 'MAX_RESPONSE_BYTES', 8)
        events = [{'type': 'content', 'text': '鹈鹕🚲'}]
    elif mode == 'wire':
        monkeypatch.setattr(s, 'MAX_STREAM_BYTES', 128)
        events = [{'type': 'upstream_raw_line', 'data': {'details': 'x' * 1000}}]
    else:
        events = [{'type': 'content', 'text': 'partial'}]
    controller = gateway_controller(tmp_path, monkeypatch, events)
    with pytest.raises(s.ProbeError) as exc:
        controller.prompt(7, 'gpt-6-astra', 'fixture')
    assert exc.value.code == expected


def test_direct_stream_ignores_reasoning_overhead_and_preserves_text():
    events = [{'type': 'response.reasoning_text.delta', 'delta': 'x' * 800000}] * 3
    events += [{'type': 'response.output_text.delta', 'delta': '鹈鹕🚲'},
               {'type': 'response.completed', 'response': {'status': 'completed'}}]
    assert s.read_stream(Response(events), 'responses', lambda: False, time.monotonic() + 10) == '鹈鹕🚲'


@pytest.mark.parametrize('wrap', [lambda x: x, lambda x: '说明\n```html\n' + x + '\n```\n结束',
    lambda x: 'to=container.exec code:\n' + json.dumps({'cmd': "cat > never-execute.html <<'EOF'\n" + x + '\nEOF'}, ensure_ascii=False)])
def test_artwork_extracts_document_without_prose_or_escaped_tool_commands(wrap):
    assert s.html_artifact(wrap(DOC)) == DOC


@pytest.mark.parametrize('output', ['', '我已经创建了文件，请打开', '<html><body>incomplete', '{"cmd":"pwd"}'])
def test_non_artwork_is_not_silently_rendered_as_a_broken_picture(output):
    with pytest.raises(s.ProbeError) as exc:
        s.html_artifact(output)
    assert exc.value.code == 'artifact_format'


def test_manual_question_once_uses_inline_document_prompt_and_extracts_preview(store):
    case = {**question('drawing', expected=''), 'kind': 'manual'}
    configure(store, [case])
    control = Answers('Explanation\n```html\n' + DOC + '\n```')
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), control)
    store.finish_prompt(job, result)
    assert result['requests'] == result['request_limit'] == len(control.prompts) == 1
    assert control.prompts[0][2] == s.manual_preview_prompt(case['prompt'])
    saved = store.public(7)['prompt_test']['history'][0]
    artifact = store.artifact(7, saved['result_id'], 'drawing')
    assert artifact['html'] == DOC and artifact['prompt'] == case['prompt']


def test_legacy_artwork_preview_extraction_does_not_rewrite_saved_source(store):
    configure(store, [{**question('drawing', expected=''), 'kind': 'manual'}])
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), Answers(DOC)))
    result_id = store.public(7)['prompt_test']['history'][0]['result_id']
    original = '说明\n```html\n' + DOC + '\n```'
    with store.db() as db:
        db.execute('UPDATE prompt_artifacts SET html=? WHERE result_id=?', (original, result_id))
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    response = client.get(f'/accounts/7/prompt-test/results/{result_id}/artifacts/drawing')
    assert response.json['html'] == original and response.json['preview_html'] == DOC
    assert store.artifact(7, result_id, 'drawing')['html'] == original
