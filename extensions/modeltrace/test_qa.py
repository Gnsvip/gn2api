"""Isolated functional fixtures: no production accounts, network or model calls."""
import copy
import json

import pytest

import service as s
from test_service import FakeAccounts, PromptControl, account, prompt_claim, store


def question(identity="candy", enabled=True, expected="21"):
    return dict(id=identity, name=identity, enabled=enabled,
                prompt="Only answer " + identity, expected_answer=expected)


def configure(store, cases=None, **kwargs):
    store.configure_prompt(7, True, "gpt-6.1-sol", s.DEFAULT_PROMPT, test_mode="qa",
                           cases=cases if cases is not None else [question()], **kwargs)


class Answers(PromptControl):
    def __init__(self, *answers):
        super().__init__()
        self.answers = iter(answers)

    def prompt(self, account_id, model, prompt, cancelled, deadline, *, reasoning_effort='xhigh'):
        self.prompts.append((account_id, model, prompt))
        answer = next(self.answers)
        if isinstance(answer, Exception):
            raise answer
        return answer


def test_qa_defaults_disabled_and_candy_optional(store):
    state = store.public(7)["prompt_test"]
    assert state["test_mode"] == "qa" and not state["enabled"]
    assert all(not case["enabled"] for case in state["cases"])
    assert state["cases"][0]["expected_answer"] == "21"
    assert store.claim_prompt() is None


def test_qa_skips_disabled_question_and_sends_prompt_unchanged(store):
    cases = [question("bool", expected="True"), question(enabled=False)]
    configure(store, cases)
    control = Answers(" TRUE\n")
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), control)
    store.finish_prompt(job, result)
    assert result["status"] == "green" and result["requests"] == 1
    assert control.prompts == [(7, "gpt-6.1-sol", "Only answer bool")]
    assert [item["id"] for item in result["cases"]] == ["bool"]
    saved = store.public(7)["prompt_test"]["history"][0]
    assert saved["test_mode"] == "qa" and "bank_commit" not in saved and "prediction" not in saved
    assert store.public(7)["history"] == []


@pytest.mark.parametrize("answers,status,count", [
    (["21"], "green", 1),
    (["２１\n"], "green", 1),
    (["121", "121"], "red", 1),
    (["29", "29"], "red", 1),
    (["29", "21"], "red", 1),
    (["29", "20"], "red", 1),
    ([""], "yellow", 1),
    ([s.ProbeError("本轮超时")], "yellow", 1),
])
def test_qa_complete_answer_once_even_when_an_unused_second_answer_differs(store, answers, status, count):
    configure(store)
    result = s.run_prompt_round(prompt_claim(store), store, FakeAccounts(), Answers(*answers))
    assert result["status"] == status and result["requests"] == count


def test_qa_partial_round_error_never_pauses(store):
    configure(store, [question("first"), question("second")], auto_pause_enabled=True, auto_pause_threshold=1)
    control = Answers("29", s.ProbeError("请求失败"))
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), control)
    assert result["status"] == "yellow" and result["cases"][0]["status"] == "red"
    store.finish_prompt(job, result)
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert control.calls == [] and store.public(7)["prompt_test"]["failure_streak"] == 0


def test_qa_confirmed_reds_pause_only_opted_in_prompt_monitor(store):
    store.configure(7, True, "gpt-6-sol")
    configure(store, auto_pause_enabled=True, auto_pause_threshold=2)
    control = Answers("29", "29", "29", "29")
    for _ in range(2):
        job = prompt_claim(store)
        store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), control))
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert control.calls == [7]
    state = store.public(7)
    assert state["enabled"] and state["history"] == []
    assert not state["prompt_test"]["enabled"] and "问答测试" in state["prompt_test"]["pause_message"]


def test_qa_toggle_cancels_active_round_before_next_question(store):
    cases = [question("first"), question("second")]
    configure(store, cases)
    job = prompt_claim(store)
    control = Answers("21")
    original = control.prompt

    def changed(*args, **kwargs):
        value = original(*args, **kwargs)
        cases[1]["enabled"] = False
        configure(store, cases)
        return value

    control.prompt = changed
    with pytest.raises(s.Cancelled):
        s.run_prompt_round(job, store, FakeAccounts(), control)
    store.finish_prompt(job, {"status": "red"})
    assert len(control.prompts) == 1 and store.public(7)["prompt_test"]["history"] == []


def test_qa_same_settings_preserve_schedule_and_generation(store):
    cases = [question(), question("unused", False)]
    configure(store, cases)
    before = store.get_prompt(7)
    configure(store, copy.deepcopy(cases))
    assert store.get_prompt(7) == before
    configure(store, cases, interval_minutes=20)
    assert store.get_prompt(7)["generation"] == before["generation"]


def test_qa_api_save_disabled_draft_enable_and_disable_all(store):
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    cases = [question(enabled=False)]
    body = dict(enabled=False, test_mode="qa", model="gpt-6.1-sol", cases=cases)
    assert client.put("/accounts/7/prompt-test", json=body).status_code == 200
    assert store.claim_prompt() is None
    assert client.put("/accounts/7/prompt-test", json={**body, "enabled": True}).status_code == 400
    cases[0]["enabled"] = True
    assert client.put("/accounts/7/prompt-test", json={**body, "enabled": True}).status_code == 200
    cases[0]["enabled"] = False
    assert client.put("/accounts/7/prompt-test", json=body).status_code == 200
    assert not store.get_prompt(7)["enabled"] and store.claim_prompt() is None
    assert not store.public(7)["prompt_test"]["cases"][0]["enabled"]


def test_qa_full_chinese_case_payload_supported(store):
    cases = [question(str(i)) for i in range(5)]
    for case in cases:
        case["prompt"] = "中" * 4000
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    body = dict(enabled=True, test_mode="qa", model="gpt-6.1-sol", cases=cases)
    assert client.put("/accounts/7/prompt-test", data=json.dumps(body), content_type="application/json").status_code == 200


@pytest.mark.parametrize("cases", [
    [question(expected="")], [question(), question()], [question(str(i)) for i in range(6)],
    [{**question(), "enabled": "false"}], [{**question(), "prompt": ""}],
])
def test_qa_invalid_case_configuration_rejected(store, cases):
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    assert client.put("/accounts/7/prompt-test", json=dict(
        enabled=True, test_mode="qa", model="gpt-6-sol", cases=cases)).status_code == 400
    assert store.get_prompt(7) is None


def test_qa_missing_capability_sends_no_questions(store):
    configure(store)
    control = Answers()
    def unavailable(reasoning_effort='xhigh'):
        raise s.ProbeError("接口不支持")
    control.check_prompt_support = unavailable
    result = s.run_prompt_round(prompt_claim(store), store, FakeAccounts(), control)
    assert result["status"] == "yellow" and result["requests"] == 0 and not control.prompts


def test_qa_migration_preserves_existing_attribution_settings_and_history(store):
    store.configure_prompt(7, True, "gpt-6-sol", "{{challenge}}", True, 3)
    store.finish_prompt(prompt_claim(store), {"status": "green"})
    before = store.public(7)["prompt_test"]
    with store.db() as db:
        db.execute("ALTER TABLE prompt_monitors DROP COLUMN test_mode")
        db.execute("ALTER TABLE prompt_monitors DROP COLUMN cases")
    migrated = s.Store(store.path).public(7)["prompt_test"]
    assert migrated == before and migrated["test_mode"] == "attribution"


def test_candy_minimum_fixed_shape_selection():
    # Exhaust every possible apple/peach count for both shapes; watermelon fills the rest.
    failing = set()
    for apple_round in range(8):
        for peach_round in range(10):
            for apple_star in range(8):
                for peach_star in range(7):
                    if apple_round * peach_star or peach_round * apple_star:
                        continue
                    for watermelon_round in range(9):
                        for watermelon_star in range(5):
                            failing.add((apple_round + peach_round + watermelon_round,
                                         apple_star + peach_star + watermelon_star))
    guaranteed = [(r + t, r, t) for r in range(25) for t in range(18) if (r, t) not in failing]
    assert min(guaranteed) == (21, 9, 12)
