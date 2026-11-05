"""Bundled QA correction shared by startup and upgrade preservation checks."""
import json

CANDY_QUESTION = """在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。现已知不同口味的糖和不同形状的数量统计如下表。参赛者需要在活动前决定摸出的糖果数目，那么，最少取出多少个糖果才能保证手中同时拥有不同形状的苹果味和桃子味的糖？（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）

苹果味 桃子味 西瓜味

圆形 7 9 8

五角星形 7 6 4"""
CANDY_PROMPT = CANDY_QUESTION + "\n\n只回答最少颗数的阿拉伯数字，不要解释。"
LEGACY_CANDY_PROMPT = (
    "黑色袋子中有苹果味、桃子味、西瓜味三种糖果。圆形分别有 7、9、8 颗，五角星形分别有 7、6、4 颗。"
    "可以靠手感辨别并选择形状，但不能靠手感辨别口味。活动前决定取出的总数，最少取多少颗，"
    "才能保证同时有圆形苹果味和五角星桃子味，或者圆形桃子味和五角星苹果味？只回答最少颗数的阿拉伯数字，不要解释。"
)


def migrated_prompt_policy(row):
    """Return only the explicitly authorized changes; never rewrite history."""
    if not {'cases', 'test_mode', 'auto_pause_threshold'} <= row.keys():
        return {}
    try:
        cases = json.loads(row['cases'])
    except (ValueError, TypeError):
        return {}
    changed = False
    for case in cases if isinstance(cases, list) else []:
        if (isinstance(case, dict) and case.get('id') == 'candy'
                and case.get('prompt') == LEGACY_CANDY_PROMPT
                and case.get('kind', 'exact') == 'exact' and case.get('expected_answer') == '21'):
            case['prompt'] = CANDY_PROMPT
            changed = True
    threshold = 1 if row['test_mode'] == 'qa' else row['auto_pause_threshold']
    if not changed and threshold == row['auto_pause_threshold']:
        return {}
    paused = row['pause_status'] == 'paused'
    return dict(cases=json.dumps(cases, ensure_ascii=False, sort_keys=True), auto_pause_threshold=threshold,
                generation=row['generation'] + 1, running=0, failure_streak=0, failure_context='', pause_retry_at=0,
                pause_status='paused' if paused else '', message=row['message'] if paused else '')
