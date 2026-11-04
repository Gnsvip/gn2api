"""Browser smoke tests with intercepted APIs; no live model requests."""
import json
import time
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

from playwright.sync_api import sync_playwright
DEFAULT_QA_CASES = [dict(id='candy', name='糖果形状题', enabled=False,
                        prompt='圆形苹果/桃子/西瓜 7/9/8，五角星 7/6/4。可按形状挑选，最少取几颗保证异形苹果桃子配对？只回答数字。',
                        expected_answer='21')]

ROOT = Path(__file__).resolve().parent
NOW = time.time()
def state(id, enabled=False, count=0):
    return dict(account_id=id, enabled=enabled, model='gpt-6-sol', available=True, reason='',
                models=['gpt-5.4', 'gpt-6-sol', 'gpt-6-astra'], mapping={}, running=False, message='',
                schedulable=True, auto_pause_enabled=False, auto_pause_threshold=3,
                mismatch_streak=0, pause_status='', paused_at=None, pause_message='',
                interval_minutes=10, interval=600, challenge_count=3,
                prompt_test=dict(enabled=False, available=True, reason='', model='gpt-6-sol',
                                 test_mode='attribution', cases=[], qa_presets=DEFAULT_QA_CASES,
                                 qa_available=True, qa_reason='', attribution_available=True, attribution_reason='',
                                 prompt='{{challenge}}', challenge_count=3, running=False,
                                 interval_minutes=10, message='', auto_pause_enabled=False,
                                 auto_pause_threshold=3, failure_streak=0, pause_status='',
                                 paused_at=None, pause_message='', history=[]),
                history=[dict(status='green', expected='gpt-6-sol', prediction='gpt-6-sol',
                              probability=.97, checked_at=NOW - (count - i) * 600, message='与预期一致') for i in range(count)])

HTML = '''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><style>
body{font:14px system-ui;background:#f8fafc;margin:32px;color:#334155}h1{font-size:22px}table{border-collapse:collapse;background:white;width:1060px}th{text-align:left;background:#f1f5f9;font-size:12px}td,th{padding:16px 22px;border-bottom:1px solid #e2e8f0}small{background:#f1f5f9;padding:4px 6px;color:#64748b;border-radius:4px}.bar{height:5px;width:92px;background:#e2e8f0;display:inline-block;border-radius:5px;margin:0 8px}.tag{font-size:11px;color:#6366f1;background:#e0e7ff;padding:2px 7px}.line{margin:8px 0;font-size:11px}.links{font-size:11px;color:#6366f1;margin-top:12px}</style></head><body><h1>账号管理</h1><table><thead><tr><th>ID</th><th>账号</th><th>用量窗口</th><th>状态</th></tr></thead><tbody>
<tr data-row-id="7"><td>#7</td><td>示例账号 A</td><td><small>159 req</small> <small>7.1M</small> <small>A $5.30</small><div class="line"><span class="tag">5h</span><span class="bar"></span>0% 现在</div><small>156 req</small> <small>7.1M</small> <small>U $1.58</small><div class="line"><span class="tag">7d</span><span class="bar"></span>1% 6d 23h</div><div class="links">查询　次数　重置　点数 —　可邀请 —　邀请用户</div></td><td>正常</td></tr>
<tr data-row-id="8"><td>#8</td><td>示例账号 B</td><td><small>42 req</small><div class="line"><span class="tag">5h</span><span class="bar"></span>12%</div></td><td>正常</td></tr>
</tbody></table></body></html>'''

states = {'7': state(7, True, 10), '8': state(8)}
states['8']['model'] = ''  # A new account must prefer astra after live catalog filtering.
states['7']['history'][-1].update(requests=4, valid_samples=3, insufficient_samples=1)
live_models = ['gpt-6-sol', 'gpt-6-astra', 'gpt-6.1-sol']
writes = []
traffic_requests = []
traffic_error = False
traffic_data = dict(mode='observe_only', started_at='2026-11-04T00:00:00Z', generated_at='2026-11-04T01:00:00Z',
    capacity=100000, capacity_limited=False, window_complete=True, valid_samples=24, excluded={'forward_error': 2, 'missing_ttft': 1},
    groups=[dict(model='gpt-6-astra', effort='xhigh', tier='unspecified', transport='websocket', mode='ws_semantic', context='128–230k',
        stats=dict(count=24, mean_ms=2500, p50_ms=1800, p95_ms=6500), earlier=dict(count=12, p50_ms=1600), later=dict(count=12, p50_ms=2000))], omitted_groups=0)
errors = []
fail_prompt_save = False
accept_discard = True
discard_questions = []
evidence_requests = []
large_events = [{'sequence':i+1, 'at':NOW, 'kind':'stream',
                 'payload':{'output':'29', 'literal':'<img src=x onerror=alert(1)>', 'line':'x' * 430}}
                for i in range(15254)]
large_events[-1].update(kind='analysis', payload={'prediction':'gpt-5.6-luna','probability':.99999})
with sync_playwright() as p:
    browser = p.chromium.launch(channel='msedge', headless=True)
    page = browser.new_page(viewport={'width': 1160, 'height': 630}, device_scale_factor=1)
    def open_custom():
        page.locator('[data-row-id="8"] .mt-label').click()
        page.get_by_role('tab', name='自定义题目', exact=True).click()
    # Fixture login belongs to the admin page, never the isolated artwork frame.
    page.add_init_script("if (window === window.top) localStorage.setItem('auth_token','fixture-admin');")
    page.on('pageerror', lambda e: errors.append(str(e)))
    def handle_dialog(dialog):
        discard_questions.append(dialog.message)
        dialog.accept() if accept_discard else dialog.dismiss()
    page.on('dialog', handle_dialog)
    def route(r):
        if '/traffic-observation' in r.request.url:
            traffic_requests.append((r.request.method, parse_qs(urlsplit(r.request.url).query)))
            r.fulfill(status=503 if traffic_error else 200, json={'code': 503, 'message': '暂时无法读取'} if traffic_error else {'code': 0, 'data': traffic_data})
        elif '/api/v1/admin/accounts/' in r.request.url:
            r.fulfill(json={'code': 0, 'data': [{'id': model} for model in live_models]})
        elif '/custom/modeltrace/api/' in r.request.url:
            if '/artifacts/' in r.request.url:
                r.fulfill(json={'html':'说明和工具调用，不应进入画面', 'prompt':'画一只鹈鹕',
                    'preview_html':'<!doctype html><html><head><meta charset="utf-8"></head><body><svg xmlns="http://www.w3.org/2000/svg" width="200" height="120"><circle cx="60" cy="60" r="40" fill="orange"/><text y="110">鹈鹕 🚲</text></svg></body></html>'})
            elif '/attempts/' in r.request.url:
                parts = urlsplit(r.request.url)
                identity, action = parts.path.rsplit('/', 2)[-2:]
                evidence_requests.append(action)
                events = large_events if identity == 'initial' else [dict(sequence=1, kind='finished', at=NOW, payload={'output':'21'})]
                if action == 'download':
                    r.fulfill(json={'attempt_id':identity, 'events':events})
                elif action == 'answer':
                    r.fulfill(json={'output': long_answer})
                elif action == 'events':
                    args = parse_qs(parts.query)
                    after = int(args.get('after', ['0'])[0])
                    filtered = events if args.get('view', ['key'])[0] == 'all' else [e for e in events if e['kind'] != 'stream']
                    rows = [e for e in filtered if e['sequence'] > after][:40]
                    r.fulfill(json={'attempt_id':identity, 'event_count':len(filtered), 'total_event_count':len(events),
                        'events':[{k:e[k] for k in ('sequence','at','kind')} | {'preview':json.dumps(e['payload']), 'truncated':False} for e in rows],
                        'next_cursor':rows[-1]['sequence'] if rows and rows[-1]['sequence'] < filtered[-1]['sequence'] else None})
                else:
                    r.fulfill(status=500, json={'error':'UI must not fetch unbounded full JSON'})
            elif '/attempts?' in r.request.url:
                more = 'before=' in r.request.url
                identities = ['interrupted'] if more else ['initial', 'retest']
                r.fulfill(json={'attempts': [dict(attempt_id=i, started_at=NOW,
                    metadata={'case_id': 'candy', 'stage': n+1}, finished={'status': 'unfinished' if more else 'completed'})
                    for n, i in enumerate(identities)], 'next_cursor': None if more else 10,
                    'saved_evidence': [] if more or 'run_id=' in r.request.url else [dict(slot=slot,
                        run_id='saved-' + slot, created_at=NOW, condition_sha256='abcdef123456',
                        summary=dict(case_id='candy', model='gpt-6-astra', reasoning_effort='xhigh'))
                        for slot in ('normal', 'anomaly')]})
            elif r.request.method == 'PUT':
                parts = r.request.url.rstrip('/').split('/')
                id = parts[-2] if parts[-1] == 'prompt-test' else parts[-1]
                data = r.request.post_data_json
                writes.append((id, data))
                if parts[-1] == 'prompt-test' and fail_prompt_save:
                    r.fulfill(status=503, json={'error': '测试保存失败，请重试'})
                    return
                if parts[-1] == 'prompt-test':
                    states[id]['prompt_test'].update(data)
                else:
                    states[id].update(data)
                r.fulfill(json={'account': states[id]})
            else:
                r.fulfill(json={'accounts': states})
        else:
            r.fulfill(status=200, content_type='text/html', body=HTML)
    page.route('https://fixture.test/**', route)
    page.goto('https://fixture.test/admin/accounts')
    page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.wait_for_function("document.querySelectorAll('.mt-dot').length === 10")
    assert page.locator('[data-row-id="8"] .mt-toggle').get_attribute('aria-checked') == 'false'
    assert writes == []
    detail = page.locator('[data-row-id="7"] .mt-dot').last.get_attribute('title')
    assert '本轮请求：4 次' in detail and '有效挑战回答：3 份；数字样本不足：1 次' in detail
    assert 'undefined' not in page.locator('[data-row-id="7"] .mt-dot').first.get_attribute('title')
    bounds = page.locator('[data-row-id="7"] .mt-dot').evaluate_all('(els) => els.map(e => ({x:e.getBoundingClientRect().x,y:e.getBoundingClientRect().y}))')
    assert len(set(b['y'] for b in bounds)) == 1
    assert bounds[0]['x'] < bounds[-1]['x']
    out = ROOT.parent / 'outputs' / 'modeltrace-ui-preview.png'
    out.parent.mkdir(exist_ok=True)
    page.screenshot(path=str(out), full_page=True)
    page.locator('[data-row-id="8"] .mt-toggle').click()
    page.wait_for_function("document.querySelector('[role=tabpanel]:not([hidden]) select')?.disabled === false")
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-astra'
    assert page.get_by_label('挑战次数', exact=True).input_value() == '3'
    assert page.get_by_label('归因推理等级', exact=True).input_value() == 'xhigh'
    page.get_by_label('归因推理等级', exact=True).select_option('medium')
    assert writes == []  # Choosing the default does not enable or write anything.
    page.get_by_role('switch', name='启用定时模型归因', exact=True).check()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('[data-row-id=\"8\"] .mt-toggle').getAttribute('aria-checked') === 'true'")
    assert writes[-1] == ('8', dict(enabled=True, model='gpt-6-astra', auto_pause_enabled=False,
                                  auto_pause_threshold=3, interval_minutes=10, challenge_count=3, reasoning_effort='medium'))
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('[data-row-id="7"] .mt-toggle').click()
    page.wait_for_function("document.querySelector('[data-row-id=\"7\"] .mt-toggle').getAttribute('aria-checked') === 'false'")
    assert writes[-1][0] == '7' and writes[-1][1]['enabled'] is False
    assert page.locator('[data-row-id="7"] .mt-dot').count() == 10
    page.locator('[data-row-id="7"] .mt-label').click()
    page.locator('dialog').wait_for(state='visible')
    page.wait_for_function("document.querySelector('[role=tabpanel]:not([hidden]) select').disabled === false")
    assert page.get_by_label('检测模型', exact=True).locator('option').count() == 2
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-sol'  # Saved model survives.
    assert page.locator('[role=tabpanel]:visible').locator('.mt-history li').count() == 10
    assert page.locator('[role=tabpanel]:visible').locator('.mt-history li:visible').count() == 3
    assert not page.locator('[role=tabpanel]:visible').locator('.mt-help').evaluate('(e) => e.open')
    assert '整轮最多 6 次请求' not in page.locator('dialog').inner_text()
    page.get_by_text('检测说明', exact=True).click()
    assert '设置几次最多就发几次' in page.locator('dialog').inner_text()
    assert '连续红点达到设置轮数即停止调度，不受校准提示阻拦' in page.locator('dialog').inner_text()
    assert '删除账号后会自动清理' in page.locator('dialog').inner_text()
    page.get_by_text('检测说明', exact=True).click()
    page.get_by_role('button', name='查看全部 10 条', exact=True).click()
    assert page.locator('[role=tabpanel]:visible').locator('.mt-history li:visible').count() == 10
    page.get_by_role('button', name='收起', exact=True).click()
    page.locator('[role=tabpanel]:visible').locator('.mt-result summary').first.click()
    assert '本轮请求：4 次' in page.locator('[role=tabpanel]:visible').locator('.mt-result').first.inner_text()
    page.locator('[role=tabpanel]:visible').locator('.mt-result summary').first.click()
    assert not page.get_by_role('switch', name='连续不符自动停止调度').is_checked()
    assert page.locator('[role=tabpanel]:visible').locator('.mt-policy-fields').is_hidden()
    page.locator('[role=tabpanel]:visible').locator('.mt-body').evaluate('(e) => { e.scrollTop = 0; }')
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-settings-compact.png')))
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.locator('[role=tabpanel]:visible').locator('.mt-body').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
    footer = page.locator('.mt-settings-dialog > .mt-actions').bounding_box()
    assert footer['y'] >= 0 and footer['y'] + footer['height'] <= 844
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-settings-mobile.png')))
    page.evaluate("document.documentElement.classList.add('dark')")
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-settings-dark.png')))
    page.evaluate("document.documentElement.classList.remove('dark')")
    page.set_viewport_size({'width': 1160, 'height': 630})
    page.get_by_role('switch', name='连续不符自动停止调度').check()
    assert page.get_by_role('spinbutton', name='连续不符轮数').input_value() == '3'
    page.get_by_role('spinbutton', name='连续不符轮数').fill('0')
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert page.locator('[role=tabpanel]:visible').locator('.mt-error').inner_text() == '连续轮数请填写 1–100 的整数'
    page.get_by_role('spinbutton', name='连续不符轮数').fill('3')
    assert page.get_by_role('spinbutton', name='检测间隔（分钟）').input_value() == '10'
    page.get_by_role('spinbutton', name='检测间隔（分钟）').fill('0')
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert page.locator('[role=tabpanel]:visible').locator('.mt-error').inner_text() == '检测间隔请填写 1–1440 分钟的整数'
    page.get_by_role('spinbutton', name='检测间隔（分钟）').fill('30')
    page.get_by_label('挑战次数', exact=True).select_option('1')
    assert '每轮最多 1 次，不自动复测' in page.locator('[role=tabpanel]:visible').locator('.mt-estimate').inner_text()
    assert '48 次请求 / 天' in page.locator('[role=tabpanel]:visible').locator('.mt-estimate').inner_text()
    page.get_by_label('挑战次数', exact=True).select_option('2')
    assert '每轮最多 2 次，不自动复测' in page.locator('[role=tabpanel]:visible').locator('.mt-estimate').inner_text()
    assert '96 次请求 / 天' in page.locator('[role=tabpanel]:visible').locator('.mt-estimate').inner_text()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1] == ('7', dict(enabled=False, model='gpt-6-sol', auto_pause_enabled=True, auto_pause_threshold=3, interval_minutes=30, challenge_count=2, reasoning_effort='xhigh'))
    assert states['8']['challenge_count'] == 3
    page.set_viewport_size({'width': 1160, 'height': 1000})
    page.locator('[data-row-id="7"] .mt-label').click()
    page.wait_for_function("document.querySelector('[role=tabpanel]:not([hidden]) select').disabled === false")
    assert page.get_by_role('spinbutton', name='检测间隔（分钟）').input_value() == '30'
    assert page.get_by_label('挑战次数', exact=True).input_value() == '2'
    page.locator('dialog').evaluate('(e) => { e.scrollTop = 0; }')
    page.screenshot(path=str(out.with_name('modeltrace-interval-settings.png')), full_page=True)
    page.get_by_role('button', name='关闭', exact=True).click()
    page.evaluate("const body=document.querySelector('tbody');body.prepend(body.lastElementChild);")
    page.wait_for_timeout(350)
    assert page.locator('tbody tr').first.get_attribute('data-row-id') == '8'
    assert page.locator('[data-row-id="8"] .mt-toggle').get_attribute('aria-checked') == 'true'
    assert page.locator('[data-row-id="7"] .mt-toggle').get_attribute('aria-checked') == 'false'
    states['7'].update(available=False, schedulable=False, pause_status='paused', paused_at=NOW,
                       mismatch_streak=3, pause_message='连续 3 轮复测仍不符，已停止此账号调度')
    states['8'].update(interval_minutes=60, interval=3600, history=[dict(status='green', expected='gpt-6-sol',
        prediction='gpt-6-sol', probability=.97, checked_at=NOW-1800, message='与预期一致')])
    page.reload()
    page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.wait_for_function("document.querySelector('[data-row-id=\"7\"] .mt-state')?.textContent === '已停调度'")
    assert page.locator('[data-row-id="8"] .mt-state').inner_text() == '已开启'
    page.locator('[data-row-id="7"] .mt-label').click()
    assert page.get_by_role('button', name='保存设置', exact=True).is_enabled()
    page.get_by_role('switch', name='连续不符自动停止调度').uncheck()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1][1]['auto_pause_enabled'] is False
    assert writes[-1][1]['enabled'] is False  # Never restores business scheduling.
    assert writes[-1][1]['challenge_count'] == 2
    states['8'].update(enabled=False, model='')
    live_models[:] = ['gpt-6-sol', 'gpt-6.1-sol']
    page.reload()
    page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.wait_for_function("document.querySelector('[data-row-id=\"8\"] .mt-toggle')?.disabled === false")
    before = len(writes)
    page.locator('[data-row-id="8"] .mt-toggle').click()
    page.wait_for_function("document.querySelector('[role=tabpanel]:not([hidden]) select')?.disabled === false")
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-sol'
    assert page.get_by_label('检测模型', exact=True).locator('option').count() == 1  # Never select unavailable astra.
    assert len(writes) == before
    page.get_by_role('button', name='关闭', exact=True).click()
    # Custom prompt testing has its own settings and history, with real color classes.
    attribution_before = json.dumps(states['8']['history'])
    open_custom()
    assert page.get_by_label('测试提示词', exact=True).input_value() == '{{challenge}}'
    page.get_by_role('switch', name='启用此页定时测试', exact=True).check()
    page.get_by_label('测试提示词', exact=True).fill('hello')
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert '{{challenge}}' in page.locator('[role=tabpanel]:visible').locator('.mt-save-error').inner_text()
    page.get_by_label('测试提示词', exact=True).fill('请认真完成：\n{{challenge}}')
    page.get_by_label('测试模型', exact=True).select_option('gpt-6-sol')
    assert page.get_by_label('题目推理等级', exact=True).input_value() == 'xhigh'
    page.get_by_label('题目推理等级', exact=True).select_option('low')
    page.get_by_label('提示词挑战次数', exact=True).select_option('2')
    page.get_by_label('测试间隔（分钟）', exact=True).fill('15')
    page.get_by_role('switch', name='连续红点自动停止调度').check()
    page.get_by_label('连续红点轮数', exact=True).fill('2')
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1] == ('8', dict(enabled=True, model='gpt-6-sol', prompt='请认真完成：\n{{challenge}}',
                                  challenge_count=2, interval_minutes=15, auto_pause_enabled=True, auto_pause_threshold=2, test_mode='attribution', reasoning_effort='low'))
    assert json.dumps(states['8']['history']) == attribution_before
    states['8']['prompt_test']['history'] = [dict(status=color, expected='gpt-6-sol', checked_at=NOW,
                                                 message=color) for color in ['green', 'yellow', 'red']]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.wait_for_function("document.querySelectorAll('[data-row-id=\"8\"] .mt-dot').length === 4")
    for color in ['green', 'yellow', 'red']:
        assert page.locator('[data-row-id="8"] .mt-' + color).count() == (2 if color == 'green' else 1)
    open_custom()
    assert page.get_by_label('测试间隔（分钟）', exact=True).input_value() == '15'
    assert page.get_by_label('题目推理等级', exact=True).input_value() == 'low'
    assert page.locator('.mt-prompt-history li').count() == 3
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-prompt-settings.png')))
    page.get_by_role('switch', name='启用此页定时测试', exact=True).uncheck()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1][0] == '8' and writes[-1][1]['enabled'] is False
    # Optional question selection is saved independently from the timer and attribution.
    attribution_before = json.dumps(states['8']['history'])
    open_custom()
    page.get_by_label('测试方式', exact=True).select_option('qa')
    page.wait_for_function("document.querySelector('[aria-label=\"测试模型\"]').disabled === false")
    assert page.get_by_label('测试提示词', exact=True).is_hidden()
    assert not page.get_by_role('switch', name='启用此页定时测试', exact=True).is_checked()
    assert not page.get_by_label('测试 糖果形状题', exact=True).is_checked()
    candy_case = page.locator('.mt-case').first
    candy_case.locator('summary span').click()
    assert candy_case.get_by_label('预期答案', exact=True).input_value() == '21'
    candy_case.locator('summary span').click()
    page.get_by_role('button', name='＋ 添加自定义题', exact=True).click()
    custom = page.locator('.mt-case').last
    custom.get_by_label('题目名称', exact=True).fill('知识题')
    custom.get_by_label('题目提示词', exact=True).fill('Tell me whether you know who Tibo is in OpenAI based on your knowledge without searching online. Only return True or False.')
    custom.get_by_label('预期答案', exact=True).fill('True')
    page.get_by_label('测试 知识题', exact=True).check()
    page.get_by_role('switch', name='启用此页定时测试', exact=True).check()
    page.get_by_label('测试模型', exact=True).select_option('gpt-6.1-sol')
    page.get_by_label('题目推理等级', exact=True).select_option('high')
    assert '每轮最多 1 次' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    saved = writes[-1][1]
    assert saved['test_mode'] == 'qa' and saved['enabled'] and saved['model'] == 'gpt-6.1-sol'
    assert saved['reasoning_effort'] == 'high'
    assert [item['enabled'] for item in saved['cases']] == [False, True]
    assert saved['cases'][1]['prompt'].endswith('Only return True or False.')
    assert json.dumps(states['8']['history']) == attribution_before
    open_custom()
    page.wait_for_function("document.querySelector('[aria-label=\"测试模型\"]').disabled === false")
    assert page.get_by_label('测试方式', exact=True).input_value() == 'qa'
    assert page.get_by_label('测试模型', exact=True).input_value() == 'gpt-6.1-sol'
    assert not page.get_by_label('测试 糖果形状题', exact=True).is_checked()
    assert page.get_by_label('测试 知识题', exact=True).is_checked()
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-qa-settings.png')))
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.locator('[role=tabpanel]:visible').locator('.mt-body').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
    footer = page.locator('.mt-settings-dialog > .mt-actions').bounding_box()
    assert footer['y'] >= 0 and footer['y'] + footer['height'] <= 844
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-qa-mobile.png')))
    page.get_by_label('测试 知识题', exact=True).uncheck()
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert '至少勾选一道题' in page.locator('[role=tabpanel]:visible').locator('.mt-save-error').inner_text()
    page.get_by_role('switch', name='启用此页定时测试', exact=True).uncheck()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.wait_for_function("document.querySelector('dialog[aria-busy]') === null")
    assert '所有修改已保存' in page.locator('dialog').inner_text()
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert not writes[-1][1]['enabled'] and not any(item['enabled'] for item in writes[-1][1]['cases'])
    # Full attempt details retain the initial and retest answers, with pagination and safe downloads.
    page.locator('[data-row-id="8"] .mt-label').click()
    page.get_by_role('button', name='全部尝试记录（含中断）', exact=True).click()
    evidence = page.locator('.mt-preview-dialog')
    evidence.locator('summary').first.wait_for()
    assert evidence.locator('.mt-evidence-entry > summary').count() == 2
    assert '正常基线' in evidence.locator('.mt-saved-evidence').inner_text()
    assert '首次异常' in evidence.locator('.mt-saved-evidence').inner_text()
    evidence.locator('.mt-evidence-entry > summary').nth(0).click()
    evidence.get_by_role('button', name='下载完整 JSON').wait_for()
    assert evidence.locator('.mt-event').count() == 1
    assert 'gpt-5.6-luna' in evidence.locator('pre').text_content()
    evidence.get_by_label('事件视图', exact=True).select_option('all')
    page.wait_for_function("document.querySelectorAll('.mt-event').length === 40")
    assert evidence.locator('.mt-event').count() == 40
    evidence.locator('.mt-event > summary').first.click()
    assert '29' in evidence.locator('pre').first.inner_text()
    assert evidence.locator('img').count() == 0
    assert len(evidence.text_content()) < 60000 and evidence_requests == ['events', 'events']
    # The browser event loop stays available while paging a production-sized stream.
    assert page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => resolve(true)))')
    evidence.get_by_role('button', name='下一页事件', exact=True).click()
    page.wait_for_function("document.querySelector('.mt-event summary')?.textContent.startsWith('#41')")
    assert evidence.locator('.mt-event').count() == 40
    evidence.get_by_role('button', name='上一页事件', exact=True).click()
    page.wait_for_function("document.querySelector('.mt-event summary')?.textContent.startsWith('#1 ·')")
    with page.expect_download() as download_info:
        evidence.get_by_role('button', name='下载完整 JSON').click()
    assert download_info.value.suggested_filename == 'attempt-initial.json'
    downloaded = json.loads(Path(download_info.value.path()).read_text())
    assert downloaded['attempt_id'] == 'initial' and len(downloaded['events']) == 15254
    evidence.get_by_label('事件视图', exact=True).select_option('key')
    page.wait_for_function("document.querySelectorAll('.mt-event').length === 1")
    evidence.locator('.mt-event > summary').first.click()
    evidence.locator('.mt-evidence-entry > summary').nth(1).click()
    page.wait_for_function("document.querySelectorAll('.mt-preview-dialog [data-loaded=true]').length === 2")
    evidence.locator('.mt-evidence-entry').nth(1).locator('.mt-event > summary').click()
    assert '21' in evidence.locator('.mt-evidence-entry').nth(1).locator('pre').inner_text()
    evidence.get_by_role('button', name='加载更多').click()
    page.wait_for_function("document.querySelectorAll('.mt-preview-dialog .mt-evidence-entry').length === 3")
    assert 'unfinished' in evidence.locator('.mt-evidence-entry > summary').last.inner_text()
    assert evidence.get_by_role('button', name='加载更多').is_hidden()
    evidence.screenshot(path=str(out.with_name('modeltrace-evidence-mobile.png')))
    evidence.get_by_role('button', name='×', exact=True).click()
    page.get_by_role('button', name='关闭', exact=True).click()
    # Long candy answers are absent from result summaries and paged on explicit open.
    long_answer = '糖果解题过程 7+9+8+4=28。' * 600 + '最终答案29'
    states['8']['prompt_test']['history'] = [dict(status='yellow', test_mode='qa', expected='gpt-6-astra', checked_at=NOW,
        message='本轮含待确认结果', requests=2, cases=[dict(name='糖果形状题', kind='exact', expected_answer='21',
        message='两次非预期答案不同', attempts=[dict(output=long_answer),dict(output='29')])])]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    open_custom()
    page.locator('[role=tabpanel]:visible .mt-result > summary').click()
    assert len(page.locator('[role=tabpanel]:visible .mt-result').text_content()) < 1200
    assert '7+9+8+4' not in page.locator('[role=tabpanel]:visible').inner_text()
    page.get_by_role('button', name='糖果形状题：查看回答', exact=True).click()
    answer_dialog = page.locator('.mt-preview-dialog')
    assert len(answer_dialog.locator('pre').inner_text()) == 4000
    answer_dialog.get_by_role('button', name='下一页', exact=True).click()
    assert answer_dialog.locator('pre').inner_text() == long_answer[4000:8000]
    answer_dialog.get_by_role('button', name='×', exact=True).click()
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-candy-collapsed.png')))
    page.get_by_role('button', name='关闭', exact=True).click()
    # New selected evidence fetches the full answer only on click; repeats are explicit previews.
    states['8']['prompt_test']['history'][0]['cases'][0]['attempts'] = [
        dict(attempt_id='saved-answer', output=long_answer[:160], output_truncated=True, evidence_saved=True),
        dict(attempt_id='summary-only', output=long_answer[:160], output_truncated=True, evidence_saved=False)]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    open_custom()
    page.locator('[role=tabpanel]:visible .mt-result > summary').click()
    page.get_by_role('button', name='糖果形状题：查看回答', exact=True).click()
    page.locator('.mt-preview-dialog .mt-answer-text').wait_for()
    assert len(page.locator('.mt-preview-dialog .mt-answer-text').inner_text()) == 4000
    page.locator('.mt-preview-dialog').get_by_role('button', name='×', exact=True).click()
    before_requests = len(evidence_requests)
    page.get_by_role('button', name='糖果形状题 · 历史第 2 次回答').click()
    assert long_answer[:160] in page.locator('.mt-preview-dialog .mt-answer-text').inner_text()
    page.locator('.mt-preview-dialog').get_by_role('button', name='×', exact=True).click()
    assert len(evidence_requests) == before_requests
    page.get_by_role('button', name='关闭', exact=True).click()
    states['8']['history'] = [dict(status='yellow', expected='gpt-6-astra', checked_at=NOW,
        requests=2, reasoning_effort='medium', message=message, outcome=outcome)
        for outcome, message in [('round_timeout', '本轮检测超时'), ('gateway_timeout', '网关测试请求超时'),
                                 ('stream_interrupted', '响应连接中断'), ('upstream_error', '上游处理错误'),
                                 ('bank_unverified', '仅展示候选'), ('insufficient_samples', '有效回答不足')]]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.locator('[data-row-id="8"] .mt-label').click()
    page.get_by_role('tab', name='模型归因', exact=True).click()
    assert page.get_by_label('归因推理等级', exact=True).input_value() == 'medium'
    page.get_by_role('button', name='查看全部 6 条', exact=True).click()
    for title in ('检测超时', '网关测试超时', '响应连接中断', '上游错误', '指纹库未校准', '样本不足'):
        assert page.locator('[role=tabpanel]:visible .mt-result-title').filter(has_text=title).count() == 1
    assert '每轮最多 12 分钟' in page.locator('[role=tabpanel]:visible').text_content()
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-effort-settings.png')))
    page.get_by_role('button', name='关闭', exact=True).click()
    states['8']['history'] = [dict(status='yellow', expected='gpt-6-astra', checked_at=NOW,
        prediction='gpt-5.6-luna', candidate_separated=True, probability=.9999989, bank_applicability='unverified',
        outcome='bank_unverified', message='指纹库未校准'), dict(status='red', expected='gpt-6-astra', checked_at=NOW,
        prediction='gpt-5.6-luna', candidate_separated=True, bank_applicability='unverified',
        outcome='candidate_mismatch_confirmed', message='候选复测仍不符')]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.locator('[data-row-id="8"] .mt-label').click()
    page.get_by_role('tab', name='模型归因', exact=True).click()
    labels = page.locator('[role=tabpanel]:visible .mt-result-title').all_text_contents()
    assert labels == ['候选复测不符 · gpt-5.6-luna', '候选不符 · gpt-5.6-luna（历史单轮结果）']
    page.locator('[role=tabpanel]:visible .mt-result > summary').first.click()
    assert '指纹适用性：当前条件未验证' in page.locator('[role=tabpanel]:visible').inner_text()
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-candidate-mismatch.png')))
    page.get_by_role('button', name='关闭', exact=True).click()
    # One-word answers are visible inline, without an answer fetch or a second dialog.
    states['8']['prompt_test']['history'] = [dict(status='red', test_mode='qa', expected='gpt-6-astra', checked_at=NOW,
        requests=1, sampling_policy='single-pass-v1', result_id=9, cases=[dict(id='tibo', name='Tibo 题', kind='exact',
        status='red', expected_answer='True', message='答案与预期不符', requests=1,
        attempts=[dict(output='False', evidence_saved=True, attempt_id='short-answer')])])]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js')); open_custom()
    assert not page.locator('[role=tabpanel]:visible .mt-result').evaluate('(e) => e.open')
    assert 'Tibo 题：False' in page.locator('[role=tabpanel]:visible').inner_text()
    assert page.get_by_role('button', name='Tibo 题 · 完整回答', exact=True).count() == 0
    assert '次完整回答' not in page.locator('[role=tabpanel]:visible').inner_text()
    page.get_by_text('Tibo 题：False', exact=True).scroll_into_view_if_needed()
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-single-answer.png')))
    page.get_by_role('button', name='关闭', exact=True).click()
    # The extracted document renders in the existing sandbox; source remains available.
    states['8']['prompt_test']['history'][0].update(status='yellow', cases=[dict(id='drawing', name='鹈鹕', kind='manual',
        status='yellow', artifact_available=True, message='待人工评分')])
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js')); open_custom()
    page.locator('[role=tabpanel]:visible .mt-result > summary').click()
    page.get_by_role('button', name='鹈鹕：查看作品', exact=True).click()
    preview = page.locator('.mt-preview-dialog')
    preview.locator('iframe').wait_for()
    assert page.frame_locator('.mt-preview-dialog iframe').locator('svg').count() == 1
    assert page.frame_locator('.mt-preview-dialog iframe').locator('body').inner_text() == '鹈鹕 🚲'
    assert 'allow-same-origin' not in preview.locator('iframe').get_attribute('sandbox')
    preview.screenshot(path=str(out.with_name('modeltrace-extracted-artwork.png')))
    preview.get_by_role('button', name='关闭', exact=True).click()
    page.get_by_role('button', name='关闭', exact=True).click()
    assert all(action in ('events', 'download', 'answer') for action in evidence_requests)
    # One save covers both drafts, validates both first, and retries only the failed page.
    page.set_viewport_size({'width': 1160, 'height': 800})
    page.locator('[data-row-id="8"] .mt-label').click()
    page.get_by_role('tab', name='模型归因', exact=True).click()
    page.get_by_label('归因推理等级', exact=True).select_option('low')
    page.get_by_role('switch', name='连续不符自动停止调度', exact=True).check()
    page.get_by_label('连续不符轮数', exact=True).fill('4')
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-attribution-compact.png')))
    page.get_by_role('tab', name='自定义题目', exact=True).click()
    page.get_by_label('题目推理等级', exact=True).select_option('low')
    page.get_by_label('测试间隔（分钟）', exact=True).fill('0')
    before = len(writes)
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert '测试间隔请填写' in page.locator('[role=tabpanel]:visible').inner_text()
    assert len(writes) == before  # The valid first tab must not be written yet.
    page.get_by_label('测试间隔（分钟）', exact=True).fill('11')
    fail_prompt_save = True
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.get_by_text('有一页未保存成功，请检查提示', exact=True).wait_for()
    assert len(writes) == before + 2
    assert states['8']['reasoning_effort'] == 'low'
    assert page.get_by_label('题目推理等级', exact=True).input_value() == 'low'
    assert page.get_by_label('测试间隔（分钟）', exact=True).input_value() == '11'
    assert page.locator('#mt-tab-attribution').get_attribute('data-dirty') == 'false'
    assert page.locator('#mt-tab-custom').get_attribute('data-dirty') == 'true'
    fail_prompt_save = False
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.get_by_text('所有修改已保存', exact=True).wait_for()
    assert len(writes) == before + 3 and states['8']['prompt_test']['interval_minutes'] == 11
    assert not states['8']['prompt_test']['enabled']  # Disabled settings remain editable.
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert page.get_by_text('没有未保存修改', exact=True).is_visible()
    assert len(writes) == before + 3
    page.get_by_role('tab', name='模型归因', exact=True).click()
    page.get_by_label('归因推理等级', exact=True).select_option('high')
    page.get_by_role('tab', name='自定义题目', exact=True).click()
    page.get_by_label('题目推理等级', exact=True).select_option('medium')
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.get_by_text('所有修改已保存', exact=True).wait_for()
    assert len(writes) == before + 5
    assert states['8']['reasoning_effort'] == 'high' and states['8']['prompt_test']['reasoning_effort'] == 'medium'
    page.get_by_label('测试间隔（分钟）', exact=True).fill('12')
    accept_discard = False
    questions_before = len(discard_questions)
    page.get_by_role('button', name='关闭', exact=True).click()
    assert page.locator('.mt-settings-dialog').is_visible()
    page.keyboard.press('Escape')
    assert len(discard_questions) == questions_before + 2
    assert page.get_by_label('测试间隔（分钟）', exact=True).input_value() == '12'
    accept_discard = True
    page.get_by_role('button', name='关闭', exact=True).click()
    page.locator('.mt-settings-dialog').wait_for(state='detached')
    assert states['8']['prompt_test']['interval_minutes'] == 11 and len(writes) == before + 5
    # Three questions share one collapsed row. Artwork opens without expanding it.
    states['8']['prompt_test']['history'] = [dict(status='yellow', test_mode='qa', expected='gpt-6-astra', checked_at=NOW,
        result_id=10, requests=3, cases=[
            dict(id='tibo', name='Tibo 题', kind='exact', status='red', expected_answer='True', attempts=[dict(output='False')]),
            dict(id='candy', name='糖果题', kind='exact', status='green', expected_answer='21', attempts=[dict(output='21')]),
            dict(id='drawing', name='画图题', kind='manual', status='yellow', artifact_available=True)])]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js')); open_custom()
    summary = page.locator('[role=tabpanel]:visible .mt-result > summary')
    assert summary.count() == 1
    assert 'Tibo 题：False' in summary.inner_text() and '糖果题：21' in summary.inner_text()
    for width in (1160, 390):
        page.set_viewport_size({'width': width, 'height': 844})
        summary.scroll_into_view_if_needed()
        assert page.locator('[role=tabpanel]:visible .mt-body').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
        assert summary.locator('.mt-result-inline').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
        page.locator('dialog').screenshot(path=str(out.with_name(f'modeltrace-mixed-results-{width}.png')))
    page.get_by_role('button', name='画图题：查看作品', exact=True).click()
    page.locator('.mt-preview-dialog iframe').wait_for()
    assert not page.locator('[role=tabpanel]:visible .mt-result').evaluate('(e) => e.open')
    page.locator('.mt-preview-dialog').get_by_role('button', name='关闭', exact=True).click()
    page.get_by_role('button', name='关闭', exact=True).click()
    # A failed request has a compact label and only one full error explanation.
    failure_message = '上游繁忙，未收到完整回答'
    states['8']['prompt_test']['history'] = [dict(status='yellow', test_mode='qa', expected='gpt-6-astra', checked_at=NOW,
        requests=1, message=failure_message, outcome='upstream_overloaded', cases=[dict(name='Tibo 题', kind='exact',
        status='yellow', outcome='upstream_overloaded', message=failure_message, expected_answer='True', attempts=[dict(output='')])])]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js')); open_custom()
    assert 'Tibo 题：上游繁忙' in page.locator('[role=tabpanel]:visible .mt-result > summary').inner_text()
    page.locator('[role=tabpanel]:visible .mt-result > summary').click()
    assert page.locator('[role=tabpanel]:visible .mt-result').inner_text().count(failure_message) == 1
    page.get_by_role('button', name='关闭', exact=True).click()
    # Observation is GET-only: never changes scheduled questions or sends probes.
    paid_writes = len(writes)
    page.locator('[data-row-id="8"] .mt-label').click()
    page.get_by_role('tab', name='模型归因', exact=True).click()
    toggle = page.get_by_role('switch', name='启用定时模型归因', exact=True)
    assert toggle.locator('..').evaluate('(e) => e.parentElement.classList.contains("mt-panel-top")')
    assert toggle.bounding_box()['y'] < page.get_by_label('检测模型', exact=True).bounding_box()['y']
    page.get_by_role('tab', name='首字观察', exact=True).click()
    page.wait_for_function("document.querySelector('#mt-panel-traffic').textContent.includes('有效样本 24')")
    panel = page.locator('#mt-panel-traffic')
    assert '观察中 · 不自动停调度' in panel.inner_text()
    assert 'P50 1.80 秒' in panel.inner_text()
    assert 'WS semantic（含推理信号）' in panel.inner_text()
    assert '转发失败 2' in panel.inner_text()
    assert panel.locator('input').count() == 0
    assert panel.get_by_role('switch').count() == 0
    assert page.locator('.mt-actions .mt-primary').is_hidden()
    assert len(writes) == paid_writes
    for width in (1160, 390):
        page.set_viewport_size({'width': width, 'height': 844})
        assert panel.locator('.mt-body').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
        page.locator('dialog').screenshot(path=str(out.with_name(f'modeltrace-traffic-observation-{width}.png')))
    traffic_data.update(groups=[], valid_samples=0, excluded={}, window_complete=False)
    page.get_by_label('首字观察窗口', exact=True).select_option('1440')
    page.wait_for_function("document.querySelector('#mt-panel-traffic').textContent.includes('暂无有效样本')")
    assert '尚未收满所选窗口' in panel.inner_text()
    assert 'P50 0.00' not in panel.inner_text()
    traffic_error = True
    page.get_by_role('button', name='刷新统计', exact=True).click()
    page.wait_for_function("document.querySelector('#mt-panel-traffic .mt-error').textContent === '暂时无法读取'")
    assert '观察中' not in panel.inner_text()
    traffic_error = False
    page.get_by_role('button', name='刷新统计', exact=True).click()
    page.wait_for_function("document.querySelector('#mt-panel-traffic').textContent.includes('暂无有效样本')")
    assert all(method == 'GET' for method, _ in traffic_requests), traffic_requests
    assert traffic_requests[-1][1] == dict(window_minutes=['1440'])
    assert len(writes) == paid_writes
    page.get_by_role('button', name='关闭', exact=True).click()
    page.evaluate("history.pushState({},'', '/dashboard')")
    page.wait_for_function("document.querySelectorAll('.mt-row').length===0")
    assert errors == [], errors
    browser.close()
print(json.dumps({'ui_checks': 'passed', 'writes': len(writes), 'screenshot': str(out)}, ensure_ascii=False))
