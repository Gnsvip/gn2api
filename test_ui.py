"""Browser smoke tests with intercepted APIs; no live model requests."""
import json
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent
NOW = time.time()
def state(id, enabled=False, count=0):
    return dict(account_id=id, enabled=enabled, model='gpt-6-sol', available=True, reason='',
                models=['gpt-5.4', 'gpt-6-sol', 'gpt-6-astra'], mapping={}, running=False, message='',
                schedulable=True, auto_pause_enabled=False, auto_pause_threshold=3,
                mismatch_streak=0, pause_status='', paused_at=None, pause_message='',
                interval_minutes=10, interval=600, challenge_count=3,
                prompt_test=dict(enabled=False, available=True, reason='', model='gpt-6-sol',
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
errors = []
with sync_playwright() as p:
    browser = p.chromium.launch(channel='msedge', headless=True)
    page = browser.new_page(viewport={'width': 1160, 'height': 630}, device_scale_factor=1)
    page.add_init_script("localStorage.setItem('auth_token','fixture-admin');")
    page.on('pageerror', lambda e: errors.append(str(e)))
    def route(r):
        if '/api/v1/admin/accounts/' in r.request.url:
            r.fulfill(json={'code': 0, 'data': [{'id': model} for model in live_models]})
        elif '/custom/modeltrace/api/' in r.request.url:
            if r.request.method == 'PUT':
                parts = r.request.url.rstrip('/').split('/')
                id = parts[-2] if parts[-1] == 'prompt-test' else parts[-1]
                data = r.request.post_data_json
                writes.append((id, data))
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
    page.wait_for_function("document.querySelector('dialog select')?.disabled === false")
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-astra'
    assert page.get_by_label('挑战次数', exact=True).input_value() == '3'
    assert writes == []  # Choosing the default does not enable or write anything.
    page.get_by_role('button', name='开启检测', exact=True).click()
    page.wait_for_function("document.querySelector('[data-row-id=\"8\"] .mt-toggle').getAttribute('aria-checked') === 'true'")
    assert writes[-1] == ('8', dict(enabled=True, model='gpt-6-astra', auto_pause_enabled=False,
                                  auto_pause_threshold=3, interval_minutes=10, challenge_count=3))
    page.locator('[data-row-id="7"] .mt-toggle').click()
    page.wait_for_function("document.querySelector('[data-row-id=\"7\"] .mt-toggle').getAttribute('aria-checked') === 'false'")
    assert writes[-1][0] == '7' and writes[-1][1]['enabled'] is False
    assert page.locator('[data-row-id="7"] .mt-dot').count() == 10
    page.locator('[data-row-id="7"] .mt-label').click()
    page.locator('dialog').wait_for(state='visible')
    page.wait_for_function("document.querySelector('dialog select').disabled === false")
    assert page.get_by_label('检测模型', exact=True).locator('option').count() == 2
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-sol'  # Saved model survives.
    assert page.locator('.mt-history li').count() == 10
    assert page.locator('.mt-history li:visible').count() == 3
    assert not page.locator('.mt-help').evaluate('(e) => e.open')
    assert '整轮最多 6 次请求' not in page.locator('dialog').inner_text()
    page.get_by_text('检测说明', exact=True).click()
    assert '整轮最多 6 次请求' in page.locator('dialog').inner_text()
    page.get_by_text('检测说明', exact=True).click()
    page.get_by_role('button', name='查看全部 10 条', exact=True).click()
    assert page.locator('.mt-history li:visible').count() == 10
    page.get_by_role('button', name='收起', exact=True).click()
    page.locator('.mt-result summary').first.click()
    assert '本轮请求：4 次' in page.locator('.mt-result').first.inner_text()
    page.locator('.mt-result summary').first.click()
    assert not page.get_by_role('switch', name='连续不符自动停止调度').is_checked()
    assert page.locator('.mt-policy-fields').is_hidden()
    page.locator('.mt-body').evaluate('(e) => { e.scrollTop = 0; }')
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-settings-compact.png')))
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.locator('.mt-body').evaluate('(e) => e.scrollWidth <= e.clientWidth + 1')
    footer = page.locator('.mt-actions').bounding_box()
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
    assert page.locator('dialog .mt-error').inner_text() == '连续轮数请填写 1–100 的整数'
    page.get_by_role('spinbutton', name='连续不符轮数').fill('3')
    assert page.get_by_role('spinbutton', name='检测间隔（分钟）').input_value() == '10'
    page.get_by_role('spinbutton', name='检测间隔（分钟）').fill('0')
    page.get_by_role('button', name='保存设置', exact=True).click()
    assert page.locator('dialog .mt-error').inner_text() == '检测间隔请填写 1–1440 分钟的整数'
    page.get_by_role('spinbutton', name='检测间隔（分钟）').fill('30')
    page.get_by_label('挑战次数', exact=True).select_option('1')
    assert '通常 1 次 / 轮，补采或复测最多 2 次 / 轮' in page.locator('.mt-estimate').inner_text()
    assert '48 次请求 / 天' in page.locator('.mt-estimate').inner_text()
    page.get_by_label('挑战次数', exact=True).select_option('2')
    assert '通常 2 次 / 轮，补采或复测最多 4 次 / 轮' in page.locator('.mt-estimate').inner_text()
    assert '96 次请求 / 天' in page.locator('.mt-estimate').inner_text()
    page.get_by_role('button', name='保存设置', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1] == ('7', dict(enabled=False, model='gpt-6-sol', auto_pause_enabled=True, auto_pause_threshold=3, interval_minutes=30, challenge_count=2))
    assert states['8']['challenge_count'] == 3
    page.set_viewport_size({'width': 1160, 'height': 1000})
    page.locator('[data-row-id="7"] .mt-label').click()
    page.wait_for_function("document.querySelector('dialog select').disabled === false")
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
    page.wait_for_function("document.querySelector('[data-row-id=\"7\"] .mt-state')?.textContent === '已自动停调度'")
    assert page.locator('[data-row-id="8"] .mt-dots:not(.mt-prompt-dots)').get_attribute('data-stale') == 'false'
    assert '60 分钟' in page.locator('[data-row-id="8"] .mt-toggle').get_attribute('title')
    page.locator('[data-row-id="7"] .mt-label').click()
    assert page.get_by_role('button', name='保存设置', exact=True).is_enabled()
    page.get_by_role('switch', name='连续不符自动停止调度').uncheck()
    page.get_by_role('button', name='保存设置', exact=True).click()
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
    page.wait_for_function("document.querySelector('dialog select')?.disabled === false")
    assert page.get_by_label('检测模型', exact=True).input_value() == 'gpt-6-sol'
    assert page.get_by_label('检测模型', exact=True).locator('option').count() == 1  # Never select unavailable astra.
    assert len(writes) == before
    page.get_by_role('button', name='关闭', exact=True).click()
    # Custom prompt testing has its own settings and history, with real color classes.
    attribution_before = json.dumps(states['8']['history'])
    page.locator('[data-row-id="8"] .mt-prompt-button').click()
    assert page.get_by_label('测试提示词', exact=True).input_value() == '{{challenge}}'
    page.get_by_label('测试提示词', exact=True).fill('hello')
    page.get_by_role('button', name='开启测试', exact=True).click()
    assert '{{challenge}}' in page.locator('.mt-save-error').inner_text()
    page.get_by_label('测试提示词', exact=True).fill('请认真完成：\n{{challenge}}')
    page.get_by_label('测试模型', exact=True).select_option('gpt-6-sol')
    page.get_by_label('提示词挑战次数', exact=True).select_option('2')
    page.get_by_label('测试间隔（分钟）', exact=True).fill('15')
    page.get_by_role('switch', name='连续红点自动停止调度').check()
    page.get_by_label('连续红点轮数', exact=True).fill('2')
    page.get_by_role('button', name='开启测试', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1] == ('8', dict(enabled=True, model='gpt-6-sol', prompt='请认真完成：\n{{challenge}}',
                                  challenge_count=2, interval_minutes=15, auto_pause_enabled=True, auto_pause_threshold=2))
    assert json.dumps(states['8']['history']) == attribution_before
    states['8']['prompt_test']['history'] = [dict(status=color, expected='gpt-6-sol', checked_at=NOW,
                                                 message=color) for color in ['green', 'yellow', 'red']]
    page.reload(); page.add_script_tag(path=str(ROOT / 'modeltrace.js'))
    page.wait_for_function("document.querySelectorAll('.mt-prompt-dots .mt-dot').length === 3")
    for color in ['green', 'yellow', 'red']:
        assert page.locator('.mt-prompt-dots .mt-' + color).count() == 1
    page.locator('[data-row-id="8"] .mt-prompt-button').click()
    assert page.get_by_label('测试间隔（分钟）', exact=True).input_value() == '15'
    assert page.locator('.mt-prompt-history li').count() == 3
    page.locator('dialog').screenshot(path=str(out.with_name('modeltrace-prompt-settings.png')))
    page.get_by_label('测试提示词', exact=True).fill('')
    page.get_by_role('button', name='关闭测试', exact=True).click()
    page.locator('dialog').wait_for(state='detached')
    assert writes[-1] == ('8', {'enabled': False})
    page.evaluate("history.pushState({},'', '/dashboard')")
    page.wait_for_function("document.querySelectorAll('.mt-row').length===0")
    assert errors == [], errors
    browser.close()
print(json.dumps({'ui_checks': 'passed', 'writes': len(writes), 'screenshot': str(out)}, ensure_ascii=False))
