import json
import time

import pytest

import attempts
import service as s
from test_service import FakeAccounts, claim, prompt_claim, store
from test_qa import Answers, configure, question


def test_retest_preserves_both_answers_and_full_output(store):
    configure(store)
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), Answers("29", "21"))
    store.finish_prompt(job, result)
    case = result["cases"][0]
    assert case["answer_preview"] == "29"
    assert [a["output"] for a in case["attempts"]] == ["29", "21"]
    saved = attempts.listing(store, 7, run_id=result["run_id"])["attempts"]
    assert [a["finished"]["output"] for a in saved] == ["21", "29"]
    assert len({a["attempt_id"] for a in saved}) == 2
    assert saved[0]["metadata"]["prompt"] == "Only answer candy"


def test_history_window_does_not_delete_evidence(store):
    configure(store)
    for _ in range(12):
        job = prompt_claim(store)
        store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), Answers("21")))
    assert len(store.public(7)["prompt_test"]["history"]) == 10
    with store.db() as db:
        assert db.execute("SELECT count(*) FROM prompt_results").fetchone()[0] == 12
    assert len(attempts.listing(store, 7)["attempts"]) == 12


def test_cancelled_round_keeps_response_already_received(store):
    configure(store)
    job = prompt_claim(store)
    control = Answers("21")
    def changed(*args):
        store.configure_prompt(7, False, "gpt-6.1-sol", s.DEFAULT_PROMPT)
        return "21"
    control.prompt = changed
    with pytest.raises(s.Cancelled):
        s.run_prompt_round(job, store, FakeAccounts(), control)
    assert attempts.listing(store, 7)["attempts"][0]["finished"]["output"] == "21"


def test_failure_keeps_partial_stream_and_omits_credentials(store):
    account = FakeAccounts().get(7)
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    def failed():
        attempts.request_snapshot("https://chatgpt.com/backend-api/codex/responses",
                                  {"Authorization": "Bearer test-secret", "Cookie": "private", "Version": "0.166.0"},
                                  {"model": job["model"]}, "responses")
        attempts.stream_line(b'data: {"delta":"partial test-secret"}')
        raise s.ProbeError("interrupted")
    with pytest.raises(s.ProbeError):
        attempts.invoke(store, job, "run", account, {"kind": "qa"}, failed)
    entry = attempts.listing(store, 7)["attempts"][0]
    detail = attempts.detail(store, 7, entry["attempt_id"])
    encoded = json.dumps(detail)
    assert "test-secret" not in encoded and "Cookie" not in encoded and "Authorization" not in encoded
    assert "partial" in encoded and "[REDACTED]" in encoded
    assert entry["finished"]["status"] == "failed"


def test_no_completion_after_process_interruption_is_visible(store):
    with store.db() as db:
        db.execute("INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,?,?)",
                   ("interrupted", "run", 7, time.time(), "{}"))
    store.recover()
    assert attempts.listing(store, 7)["attempts"][0]["finished"]["status"] == "unfinished"


def test_evidence_api_auth_account_isolation_and_pagination(store):
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    for i in range(21):
        attempts.invoke(store, job, "run", FakeAccounts().get(7), {"index": i}, lambda: "21")
    client = s.create_app(store, FakeAccounts(), lambda h: h.get("Authorization") == "ok").test_client()
    assert client.get("/accounts/7/attempts").status_code == 401
    headers = {"Authorization": "ok"}
    page = client.get("/accounts/7/attempts", headers=headers).json
    assert len(page["attempts"]) == 20 and page["next_cursor"]
    second = client.get(f'/accounts/7/attempts?before={page["next_cursor"]}', headers=headers).json
    assert len(second["attempts"]) == 1
    identity = page["attempts"][0]["attempt_id"]
    assert client.get(f"/accounts/8/attempts/{identity}", headers=headers).status_code == 404


def test_bank_cannot_confirm_identity_in_new_conditions(monkeypatch):
    monkeypatch.setattr(s, "analyze_global_outputs", lambda *args: {
        "results": [{"model": "gpt-5.6-luna", "probability": .99999}, {"model": "gpt-6-astra", "probability": .00001}],
        "probability": .99999, "prediction": "gpt-5.6-luna", "used_outputs": 3})
    result = s.summarize([{}, {}, {}], "gpt-6-astra")
    assert result["status"] == "yellow" and not result["confident"]
    assert result["bank_applicability"] == "unverified" and result["candidate_separated"]


def test_attribution_analysis_survives_round_cancellation(store, monkeypatch):
    store.configure(7, True, 'gpt-6-astra', challenge_count=1)
    job = claim(store)
    def score(outputs, model):
        store.configure(7, False, model)
        return {"status": "yellow", "prediction": "gpt-5.6-luna", "probability": .95,
                "confident": False, "analysis": {"all_candidates": ["candidate-a", "candidate-b"]}}
    monkeypatch.setattr(s, "summarize", score)
    with pytest.raises(s.Cancelled):
        s.run_round(job, store, FakeAccounts(), "0.166.0", lambda *args: ({"text": "full numeric output"}, job["model"]))
    entry = attempts.listing(store, 7)["attempts"][0]
    events = attempts.detail(store, 7, entry["attempt_id"])["events"]
    analysis = next(e["payload"] for e in events if e["kind"] == "analysis")
    assert analysis["analysis"]["all_candidates"] == ["candidate-a", "candidate-b"]
    assert analysis["attempt_ids"] == [entry["attempt_id"]]


def test_http_error_evidence_is_bounded_redacted_and_marked(store):
    class Response:
        def iter_content(self, chunk_size):
            yield b'failure test-secret: ' + b'x' * 200
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    attempts.invoke(store, job, "error", FakeAccounts().get(7), {},
                    lambda: attempts.error_body(Response(), 80))
    entry = attempts.listing(store, 7)["attempts"][0]
    event = attempts.detail(store, 7, entry["attempt_id"])["events"][0]
    assert event["kind"] == "error_body" and event["payload"]["truncated"]
    assert event["payload"]["received_bytes"] == 81
    assert "test-secret" not in json.dumps(event) and "[REDACTED]" in json.dumps(event)


@pytest.mark.parametrize("change", ["context", "metadata", "proxy", "instructions", "effort", "transport"])
def test_comparison_rejects_changed_conditions_and_keeps_evidence(store, change):
    import copy
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    request = {"profile": s.COMPARISON_PROFILE, "details": {
        "account_id": 7, "proxy_id": 3, "url": "https://chatgpt.com/backend-api/codex/responses",
        "transport": "codex-http1-openssl36", "body": {
            "model": job["model"], "reasoning": {"effort": "xhigh"}, "instructions": s.COMPARISON_INSTRUCTIONS,
            "tools": [], "tool_choice": "none", "parallel_tool_calls": False, "text": {"verbosity": "medium"},
            "input": [{"role": "developer", "content": "fixed context"},
                      {"role": "user", "content": [{"type": "input_text", "text": "question"}]}],
            "client_metadata": {"turn_id": "first", "capability": "original"}}}}
    def send(data):
        data["client_request_id"] = attempts.ACTIVE.get()["id"]
        s.validate_comparison_request(data, data["client_request_id"], 7, job["model"], "question")
        return "answer"
    attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(request))
    fresh = copy.deepcopy(request)
    fresh["details"]["body"]["client_metadata"]["turn_id"] = "second"
    attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(fresh))
    body = fresh["details"]["body"]
    if change == "context": body["input"][0]["content"] = "changed"
    if change == "metadata": body["client_metadata"]["capability"] = "changed"
    if change == "proxy": fresh["details"]["proxy_id"] = 8
    if change == "instructions": body["instructions"] = "changed"
    if change == "effort": body["reasoning"] = {"effort": "low"}
    if change == "transport": fresh["details"]["transport"] = "python-requests"
    with pytest.raises(s.ProbeError):
        attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(fresh))
    saved = attempts.listing(store, 7)["attempts"]
    assert len(saved) == 3 and saved[0]["finished"]["status"] == "failed"


@pytest.mark.parametrize("prompt", [False, True])
def test_legacy_pending_attribution_never_pauses_without_valid_bank(store, prompt):
    from test_service import FakeControl
    table = "prompt_monitors" if prompt else "monitors"
    streak = "failure_streak" if prompt else "mismatch_streak"
    context = "failure_context" if prompt else "mismatch_context"
    if prompt:
        store.configure_prompt(7, True, "gpt-6-sol", "{{challenge}}", True, 1)
    else:
        store.configure(7, True, "gpt-6-sol", True, 1)
    with store.db() as db:
        db.execute(f"UPDATE {table} SET {streak}=1,{context}=?,pause_status='pending'",
                   (s.account_context(FakeAccounts().get(7)).hex(),))
    control = FakeControl()
    if prompt: store.apply_pending_prompt_pause(FakeAccounts(), control)
    else: store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == []
    with store.db() as db:
        assert db.execute(f"SELECT {streak},pause_status FROM {table}").fetchone()[:] == (0, "")
