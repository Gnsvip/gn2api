/* ModelTrace account monitor. Separate from the sub2api application bundle. */
(() => {
  'use strict';
  if (window.__aifxoModelTraceLoaded) return;
  window.__aifxoModelTraceLoaded = true;
  const API = '/custom/modeltrace/api';
  const states = new Map();
  let loading = false, lastPoll = 0, lastToken = '', lastError = '', reconcileTimer;
  const pending = new Set();
  const style = document.createElement('style');
  style.textContent = `
    .mt-row{display:flex;align-items:center;gap:7px;min-height:22px;margin-bottom:5px;font-size:11px;line-height:18px;white-space:nowrap;color:#64748b}
    .mt-label,.mt-dot,.mt-toggle{appearance:none;border:0;font:inherit;cursor:pointer;padding:0}
    .mt-label{background:transparent;color:#475569;flex-shrink:0}
    .mt-label:hover{color:#2563eb}.dark .mt-label{color:#cbd5e1}
    .mt-toggle{position:relative;width:25px;height:14px;border-radius:9px;background:#cbd5e1;flex-shrink:0;transition:background .15s}
    .mt-toggle:after{content:'';position:absolute;left:2px;top:2px;width:10px;height:10px;border-radius:50%;background:#fff;transition:transform .15s}
    .mt-toggle[aria-checked=true]{background:#10b981}.mt-toggle[aria-checked=true]:after{transform:translateX(11px)}
    .mt-toggle:disabled{opacity:.45;cursor:not-allowed}.mt-toggle:focus-visible,.mt-label:focus-visible,.mt-dot:focus-visible{outline:2px solid #3b82f6;outline-offset:3px}
    .mt-dots{display:inline-flex;align-items:center;gap:4px;flex-wrap:nowrap}
    .mt-dot{width:8px;height:8px;border-radius:50%;background:#94a3b8;flex-shrink:0}
    .mt-green{background:#10b981}.mt-yellow{background:#eab308}.mt-red{background:#ef4444}
    .mt-dots[data-disabled=true]{opacity:.5}.mt-dots[data-stale=true]{opacity:.5}
    .mt-state{font-size:10px;color:#94a3b8}.mt-error{color:#dc2626}
    .mt-dialog{box-sizing:border-box;border:1px solid #e2e8f0;border-radius:16px;padding:0;width:min(520px,calc(100vw - 24px));max-height:90vh;overflow:hidden;color:#334155;background:#fff;box-shadow:0 16px 60px #0003;font:14px/1.6 system-ui,sans-serif}
    .mt-dialog[open]{display:flex;flex-direction:column}.mt-dialog [hidden]{display:none!important}
    .mt-dialog::backdrop{background:#0f172a77}.dark .mt-dialog{background:#111827;color:#e2e8f0;border-color:#374151}
    .mt-header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:20px 22px 12px;flex-shrink:0}
    .mt-dialog h3{font-size:17px;font-weight:600;margin:0}.mt-dialog p{margin:8px 0}.mt-dialog p:empty{display:none}
    .mt-badge{font-size:11px;color:#64748b;background:#f1f5f9;border-radius:12px;padding:2px 9px;white-space:nowrap}.mt-badge[data-enabled=true]{background:#ecfdf5;color:#047857}
    .mt-body{padding:0 22px;overflow:auto;min-height:0}.mt-dialog label{font-size:13px}
    .mt-dialog select{display:block;width:100%;padding:9px 10px;border:1px solid #cbd5e1;border-radius:7px;margin-top:5px;color:inherit;background:transparent;font:inherit}
    .mt-dialog select option{color:#334155;background:#fff}.mt-dialog .mt-note{font-size:12px;color:#64748b}.dark .mt-dialog .mt-note{color:#94a3b8}
    .mt-actions{display:flex;justify-content:flex-end;gap:10px;padding:14px 22px;flex-shrink:0;border-top:1px solid #e2e8f066}.mt-actions button{padding:7px 16px;border-radius:7px;border:1px solid #cbd5e1;background:transparent;color:inherit;cursor:pointer;font:inherit}
    .mt-actions button.mt-primary{background:#059669;border-color:#059669;color:white}.mt-actions button:disabled{opacity:.5;cursor:wait}
    .mt-section-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:18px;font-size:13px;font-weight:600}
    .mt-link{appearance:none;border:0;padding:2px 0;background:transparent;color:#64748b;font:12px system-ui;cursor:pointer}.mt-link:hover{color:#059669}
    .mt-history{padding:0;list-style:none;max-height:260px;overflow:auto;margin:4px 0 0}.mt-history li{border-bottom:1px solid #e2e8f066;font-size:12px}
    .mt-result>summary{display:flex;align-items:center;gap:9px;padding:10px 0;cursor:pointer;list-style:none}.mt-result>summary::-webkit-details-marker{display:none}
    .mt-result>summary::after{content:'›';color:#94a3b8;margin-left:auto}.mt-result[open]>summary::after{transform:rotate(90deg)}
    .mt-result time{color:#64748b;white-space:nowrap;font-variant-numeric:tabular-nums}.mt-result-title{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .mt-result pre{margin:0 0 10px 17px;white-space:pre-wrap;overflow-wrap:anywhere;color:#64748b;font:12px/1.7 system-ui}
    .mt-help{margin:14px 0;font-size:12px;color:#64748b}.mt-help>summary{cursor:pointer;width:fit-content}.mt-help p{line-height:1.7}
    .dark .mt-result pre,.dark .mt-result time,.dark .mt-help,.dark .mt-link{color:#94a3b8}
    .mt-policy{margin:16px 0 0;padding:12px;border:1px solid #e2e8f0;border-radius:9px}.mt-policy label{display:block}.dark .mt-policy{border-color:#374151}
    .mt-policy input[type=checkbox]{margin-right:8px;accent-color:#059669;width:16px;height:16px;vertical-align:middle}
    .mt-policy input[type=number],.mt-interval input{box-sizing:border-box;width:68px;margin:0 8px;padding:5px 7px;border:1px solid #cbd5e1;border-radius:6px;background:transparent;color:inherit;font:inherit}
    .mt-policy-fields{margin-top:12px}.mt-policy-fields .mt-note{margin:6px 0 0}.mt-interval{display:block;margin-top:16px}
    .mt-prompt-button{appearance:none;border:0;background:transparent;color:#7c3aed;font:11px system-ui;cursor:pointer;padding:0 2px}.mt-prompt-button:hover{color:#6d28d9}.mt-prompt-button:disabled{opacity:.45;cursor:not-allowed}
    .mt-prompt-label{display:block;margin-top:14px}.mt-dialog textarea{box-sizing:border-box;width:100%;min-height:90px;resize:vertical;padding:9px 10px;border:1px solid #cbd5e1;border-radius:7px;margin-top:5px;color:inherit;background:transparent;font:inherit;line-height:1.5}.mt-prompt-history .mt-result-title{color:#7c3aed}.mt-prompt-badge[data-enabled=true]{background:#f5f3ff;color:#6d28d9}
    .mt-case{margin:12px 0;padding:12px;border:1px solid #cbd5e1;border-radius:9px}.mt-case>summary{display:flex;gap:8px;align-items:center;cursor:pointer;font-size:13px}.mt-case>summary span{flex:1;overflow-wrap:anywhere}.mt-case input[type=checkbox],.mt-qa-enabled input{accent-color:#059669}
    .mt-case input[type=text]{box-sizing:border-box;width:100%;padding:8px 10px;border:1px solid #cbd5e1;border-radius:7px;background:transparent;color:inherit;font:inherit;margin-top:5px}.mt-case-tools{display:flex;gap:14px;flex-wrap:wrap}.mt-qa-enabled{display:block;margin-top:14px}
  `;
  document.head.append(style);
  const onPage = () => /^\/admin\/accounts\/?$/.test(location.pathname);
  const token = () => localStorage.getItem('auth_token') || '';
  const date = value => new Date(value * 1000).toLocaleString('zh-CN', { hour12: false });
  const shortDate = value => new Date(value * 1000).toLocaleString('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false
  });
  const preferredModel = (choices, saved) => choices.includes(saved) ? saved
    : choices.includes('gpt-6-astra') ? 'gpt-6-astra' : choices[0] || '';
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  };
  const description = result => result.test_mode === 'qa' ? [
    date(result.checked_at), `问答测试 · 请求模型：${result.expected}`,
    `本轮请求：${result.requests || 0} 次`, result.message,
    ...(result.cases || []).map(item => `${item.name}：${item.message || '未完成'}\n预期答案：${item.expected_answer}\n实际回答：${item.answer_preview || '未取得完整回答'}${item.answer_truncated ? '…（已截断）' : ''}`),
    '按答案判断，不代表模型身份归因'
  ].filter(Boolean).join('\n') : [
    date(result.checked_at), `预期：${result.expected}`, result.sent_model && `发送模型：${result.sent_model}`,
    result.prediction && `归因：${result.prediction}`,
    typeof result.probability === 'number' && `候选库内分数：${(result.probability * 100).toFixed(1)}%`,
    typeof result.requests === 'number' && `本轮请求：${result.requests} 次`,
    typeof result.challenge_count === 'number' && `挑战设置：${result.challenge_count} 次`,
    typeof result.valid_samples === 'number' && `有效挑战回答：${result.valid_samples} 份；数字样本不足：${result.insufficient_samples || 0} 次`,
    result.message, '统计归因，仅供参考'
  ].filter(Boolean).join('\n');

  async function api(path, options = {}) {
    const currentToken = token();
    if (!currentToken) throw new Error('请使用管理员账号登录');
    const response = await fetch(API + path, {
      ...options, headers: { 'Authorization': 'Bearer ' + currentToken, 'Content-Type': 'application/json' },
      credentials: 'same-origin', cache: 'no-store', signal: AbortSignal.timeout(12000)
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || '检测服务暂不可用');
    if (currentToken !== token()) throw new Error('登录状态已变化');
    return data;
  }

  function targets() {
    const found = [];
    if (!onPage()) return found;
    for (const table of document.querySelectorAll('table')) {
      const headers = [...table.querySelectorAll('thead th')];
      const usage = headers.findIndex(th => /用量窗口|Usage Window|Usage Windows/i.test(th.textContent));
      if (usage < 0) continue;
      for (const row of table.querySelectorAll('tbody tr[data-row-id]')) {
        const id = Number(row.dataset.rowId);
        if (Number.isSafeInteger(id) && id > 0 && row.children[usage]) found.push({ id, cell: row.children[usage] });
      }
    }
    for (const field of document.querySelectorAll('[data-field="usage"]')) {
      const siblings = field.parentElement;
      const idField = siblings.querySelector('[data-field="id"]');
      const match = idField && idField.textContent.match(/#(\d+)/);
      const content = field.lastElementChild;
      if (match && content) found.push({ id: Number(match[1]), cell: content });
    }
    return found;
  }

  function render(target) {
    const state = states.get(target.id);
    let row = target.cell.querySelector(':scope > .mt-row');
    if (!row) {
      row = el('div', 'mt-row');
      row.dataset.accountId = String(target.id);
      row.addEventListener('click', event => event.stopPropagation());
      target.cell.prepend(row);
    }
    const history = state?.history || [];
    const currentHistory = history.filter(r => r.expected === state?.model);
    const stale = state?.enabled && currentHistory.length > 0 && Date.now() / 1000 - currentHistory.at(-1).checked_at > Math.max((state.interval_minutes || 10) * 120, 300);
    const signature = JSON.stringify([target.id, state, pending.has(target.id), lastError, !!stale]);
    if (row.dataset.signature === signature) return;
    row.dataset.signature = signature;
    row.dataset.accountId = String(target.id);
    const toggle = el('button', 'mt-toggle');
    toggle.type = 'button';
    toggle.setAttribute('role', 'switch');
    toggle.setAttribute('aria-label', `账号 ${target.id} 模型归因开关`);
    toggle.setAttribute('aria-checked', String(!!state?.enabled));
    toggle.disabled = !state || pending.has(target.id) || (!state.available && !state.enabled) || !!lastError;
    toggle.title = state?.reason || `仅此账号：每 ${state?.interval_minutes || 10} 分钟检测一次`;
    toggle.addEventListener('click', () => {
      if (state.enabled) save(target.id, false, state.model);
      else if (state.model && state.models.includes(state.model)) save(target.id, true, state.model);
      else openSettings(target.id, true);
    });
    const label = el('button', 'mt-label', '模型归因');
    label.type = 'button';
    label.title = (state?.model ? '当前模型：' + state.model + '\n' : '') + '查看模型设置和最近 10 次结果';
    label.disabled = !state;
    label.addEventListener('click', () => openSettings(target.id, false));
    const promptButton = el('button', 'mt-prompt-button', '提示词测试');
    promptButton.type = 'button';
    promptButton.disabled = !state?.prompt_test || pending.has(target.id) || !!lastError;
    promptButton.title = state?.prompt_test?.reason || '配置提示词模板、测试间隔和连续红点停调度';
    promptButton.addEventListener('click', () => openPromptSettings(target.id));
    const promptHistory = state?.prompt_test?.history || [];
    const promptDots = el('span', 'mt-dots mt-prompt-dots');
    promptDots.dataset.disabled = String(!state?.prompt_test?.enabled);
    for (const result of promptHistory.slice(-10)) {
      const color = ['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow';
      const dot = el('button', 'mt-dot mt-' + color); dot.type = 'button';
      dot.title = (result.test_mode === 'qa' ? '问答测试\n' : '提示词归因\n') + description(result); dot.setAttribute('aria-label', dot.title);
      dot.addEventListener('click', () => openPromptSettings(target.id)); promptDots.append(dot);
    }
    const promptState = state?.prompt_test;
    const promptNote = el('span', 'mt-state mt-prompt-state', promptState?.pause_status === 'paused' ? '已停调度'
      : ['pending', 'failed'].includes(promptState?.pause_status) ? '停调度待确认'
      : promptState?.running ? '测试中' : promptState?.enabled ? (promptState.auto_pause_enabled ? `连续红 ${promptState.failure_streak || 0}/${promptState.auto_pause_threshold}` : '已开启') : '已关闭');
    promptNote.title = promptState?.message || '';
    const dots = el('span', 'mt-dots');
    dots.dataset.disabled = String(!state?.enabled);
    dots.dataset.stale = String(!!stale);
    for (const result of history.slice(-10)) {
      const color = ['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow';
      const dot = el('button', 'mt-dot mt-' + color);
      dot.type = 'button';
      dot.title = description(result);
      if (state?.model && result.expected !== state.model) {
        dot.style.opacity = '.35';
        dot.title = '历史模型，非当前选择\n' + dot.title;
      }
      dot.setAttribute('aria-label', dot.title);
      dot.addEventListener('click', () => openSettings(target.id, false));
      dots.append(dot);
    }
    let text = !state ? (lastError ? '暂不可用' : '加载中') : !state.enabled ? '已关闭'
      : state.running ? '检测中' : stale ? '结果已过期' : !currentHistory.length ? '等待检测' : '';
    if (state && !state.available && !state.enabled) text = '暂不可测';
    if (state?.auto_pause_enabled && state.enabled && !state.running) text = `连续不符 ${state.mismatch_streak || 0}/${state.auto_pause_threshold}`;
    if (state?.pause_status === 'pending') text = '正在停止调度';
    if (state?.pause_status === 'failed') text = '停调度未确认';
    if (state?.pause_status === 'paused' && !state.schedulable) text = '已自动停调度';
    if (lastError && state) text = '暂不可用';
    const note = el('span', 'mt-state', text);
    note.title = lastError || state?.message || state?.reason || '';
    if (['paused', 'failed'].includes(state?.pause_status)) note.classList.add('mt-error');
    row.replaceChildren(toggle, label, dots, note, promptButton, promptDots, promptNote);
  }

  function reconcile() {
    clearTimeout(reconcileTimer);
    if (!onPage()) {
      document.querySelectorAll('.mt-row,.mt-dialog').forEach(node => node.remove());
      return;
    }
    const currentToken = token();
    if (lastToken !== currentToken) {
      states.clear(); lastPoll = 0; lastError = ''; lastToken = currentToken;
    }
    const rows = targets();
    rows.forEach(render);
    const ids = [...new Set(rows.map(r => r.id))];
    const missing = ids.some(id => !states.has(id));
    if (ids.length && currentToken && !loading && (Date.now() - lastPoll > 15000 || (missing && Date.now() - lastPoll > 1000))) poll(ids);
  }

  async function poll(ids) {
    loading = true; lastPoll = Date.now();
    try {
      for (let start = 0; start < ids.length; start += 100) {
        const data = await api('/accounts?ids=' + ids.slice(start, start + 100).join(','));
        for (const [id, state] of Object.entries(data.accounts)) states.set(Number(id), state);
      }
      lastError = '';
    } catch (error) {
      lastError = error.name === 'TimeoutError' ? '检测服务连接超时' : error.message;
    } finally {
      loading = false;
      targets().forEach(render);
    }
  }

  async function save(id, enabled, model, dialog, policy = {}) {
    if (pending.has(id)) return;
    pending.add(id); targets().filter(t => t.id === id).forEach(render);
    const errorBox = dialog?.querySelector('.mt-save-error');
    if (errorBox) errorBox.textContent = '';
    if (dialog) dialog.querySelectorAll('button').forEach(b => b.disabled = true);
    try {
      const data = await api('/accounts/' + id, { method: 'PUT', body: JSON.stringify({ enabled, model, ...policy }) });
      states.set(id, data.account); lastError = '';
      if (dialog) dialog.close();
    } catch (error) {
      if (errorBox) errorBox.textContent = error.message;
      else { lastError = error.message; lastPoll = 0; }
    } finally {
      pending.delete(id);
      if (dialog?.open) dialog.querySelectorAll('button').forEach(b => b.disabled = false);
      targets().filter(t => t.id === id).forEach(render);
    }
  }

  function openSettings(id, enable) {
    const state = states.get(id);
    if (!state) return;
    document.querySelectorAll('.mt-dialog').forEach(node => node.remove());
    const dialog = el('dialog', 'mt-dialog');
    const title = el('h3', '', `模型归因 · 账号 #${id}`);
    const header = el('div', 'mt-header');
    const badge = el('span', 'mt-badge', state.running ? '检测中' : state.enabled ? '已开启' : '已关闭');
    badge.dataset.enabled = String(!!state.enabled);
    header.append(title, badge);
    const body = el('div', 'mt-body');
    const label = el('label', '', '检测模型');
    const select = el('select');
    const catalogNote = el('p', 'mt-note', '正在读取此账号的可用模型…');
    select.setAttribute('aria-label', '检测模型');
    for (const model of state.models) {
      const option = el('option', '', model); option.value = model; select.append(option);
    }
    select.value = preferredModel(state.models, state.model);
    select.disabled = !state.available;
    label.append(select);
    const mapping = el('p', 'mt-note');
    const refreshMapping = () => {
      const mapped = state.mapping[select.value];
      mapping.textContent = mapped && mapped !== select.value ? `账号将 ${select.value} 映射到 ${mapped}；归因仍以所选模型为预期。` : '';
    };
    select.addEventListener('change', refreshMapping); refreshMapping();
    const intervalLabel = el('label', 'mt-interval', '检测间隔');
    const interval = el('input'); interval.type = 'number'; interval.min = '1'; interval.max = '1440'; interval.step = '1';
    interval.value = String(state.interval_minutes || 10); interval.setAttribute('aria-label', '检测间隔（分钟）');
    intervalLabel.append(interval, document.createTextNode('分钟'));
    const challengeLabel = el('label', 'mt-interval', '挑战次数');
    const challengeCount = el('select'); challengeCount.setAttribute('aria-label', '挑战次数');
    for (const [value, text] of [[1, '1 次 · 省额度'], [2, '2 次 · 折中'], [3, '3 次 · 更稳定']]) {
      const option = el('option', '', text); option.value = String(value); challengeCount.append(option);
    }
    challengeCount.value = String(state.challenge_count || 3);
    challengeLabel.append(challengeCount);
    const estimate = el('p', 'mt-note mt-estimate');
    const refreshEstimate = () => {
      const minutes = Number(interval.value);
      const count = Number(challengeCount.value);
      estimate.textContent = Number.isInteger(minutes) && minutes >= 1 && minutes <= 1440
        ? `通常 ${count} 次 / 轮，补采或复测最多 ${count * 2} 次 / 轮；通常约 ${Math.round(1440 * count / minutes)} 次请求 / 天。` : '';
    };
    interval.addEventListener('input', refreshEstimate); refreshEstimate();
    challengeCount.addEventListener('change', refreshEstimate);
    const help = el('details', 'mt-help');
    help.append(el('summary', '', '检测说明'),
      el('p', '', '绿色：与预期一致；黄色：待确认或检测失败；红色：复测仍不符。统计归因仅供参考。'),
      el('p', '', '挑战次数可选 1、2、3，默认 3。1 次最省额度，2 次折中，3 次更稳定；较少样本更容易误判。样本不足只补缺少部分，归因不符则用同样份数独立复测。整轮最多为所选次数的 2 倍（3 次模式整轮最多 6 次请求）；无法凑齐所选份数或完成复测记黄点，不触发自动停调度。'),
      el('p', '', '自动停止只计连续红点，绿点或黄点清零。更改次数、策略或模型后重新计数。更改次数会取消旧轮次的后续请求和结果，下一轮仍按原计划执行；已发出的请求可能已计费。停止后当前业务请求继续，检测暂停，恢复调度需手动操作。'),
      el('p', '', '间隔支持 1–1440 分钟。仅改间隔不打断当前检测，下一轮从保存时重新计时。关闭页面仍会检测，繁忙时可能延后。'),
      el('p', '', '模型列表仅显示账号可用且指纹库已收录的模型。首次选择优先 gpt-6-astra，保留已有设置；当前库尚未收录 gpt-6.1-sol。'));
    const reason = el('p', 'mt-note', state.reason || '');
    const policy = el('div', 'mt-policy');
    const autoLabel = el('label');
    const auto = el('input'); auto.type = 'checkbox'; auto.checked = !!state.auto_pause_enabled;
    auto.setAttribute('role', 'switch'); auto.setAttribute('aria-label', '连续不符自动停止调度');
    autoLabel.append(auto, document.createTextNode('连续不符自动停止调度'));
    const thresholdLabel = el('label', '', '连续');
    const threshold = el('input'); threshold.type = 'number'; threshold.min = '1'; threshold.max = '100'; threshold.step = '1';
    threshold.value = String(state.auto_pause_threshold || 3); threshold.disabled = !auto.checked;
    threshold.setAttribute('aria-label', '连续不符轮数');
    const policyFields = el('div', 'mt-policy-fields'); policyFields.hidden = !auto.checked;
    auto.addEventListener('change', () => { threshold.disabled = !auto.checked; policyFields.hidden = !auto.checked; });
    thresholdLabel.append(threshold, document.createTextNode('轮复测不符后停止'));
    const progress = el('p', 'mt-note', `当前连续不符 ${state.mismatch_streak || 0} / ${state.auto_pause_threshold || 3} 轮 · 仅计红点`);
    policyFields.append(thresholdLabel, progress);
    policy.append(autoLabel, policyFields);
    if (state.pause_status === 'pending' || state.pause_status === 'failed') policy.append(el('p', 'mt-error', state.message));
    if (state.paused_at) policy.append(el('p', 'mt-note', `上次停调度：${date(state.paused_at)} · ${state.pause_message}。排查后请在 sub2api 恢复账号调度，再开启模型归因。`));
    const list = el('ul', 'mt-history');
    const historyHead = el('div', 'mt-section-head');
    historyHead.append(el('span', '', '最近结果'));
    const more = el('button', 'mt-link'); more.type = 'button';
    more.hidden = state.history.length <= 3;
    let expanded = false;
    const historyItems = [];
    for (const result of [...state.history].reverse().slice(0, 10)) {
      const item = el('li');
      const dot = el('span', 'mt-dot mt-' + (['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow'));
      const details = el('details', 'mt-result');
      const summary = el('summary');
      const when = el('time', '', shortDate(result.checked_at)); when.dateTime = new Date(result.checked_at * 1000).toISOString();
      const text = result.status === 'green' ? '与预期一致' : result.status === 'red' ? '复测仍不符'
        : result.requests === 0 ? '本轮跳过' : '待确认';
      summary.append(dot, when, el('span', 'mt-result-title', text));
      summary.title = result.message;
      details.append(summary, el('pre', '', description(result)));
      item.append(details); list.append(item); historyItems.push(item);
    }
    const refreshHistory = () => {
      historyItems.forEach((item, index) => { item.hidden = !expanded && index >= 3; });
      more.textContent = expanded ? '收起' : `查看全部 ${historyItems.length} 条`;
      more.setAttribute('aria-expanded', String(expanded));
    };
    more.addEventListener('click', () => { expanded = !expanded; refreshHistory(); }); refreshHistory();
    historyHead.append(more);
    const empty = el('p', 'mt-note', state.history.length ? '' : '暂无检测记录');
    const error = el('p', 'mt-error mt-save-error');
    const actions = el('div', 'mt-actions');
    const close = el('button', '', '关闭'); close.type = 'button'; close.addEventListener('click', () => dialog.close());
    actions.append(close);
    let submit;
    {
      submit = el('button', 'mt-primary', enable ? '开启检测' : '保存设置');
      submit.type = 'button';
      submit.disabled = !!enable;
      submit.addEventListener('click', () => {
        const count = Number(threshold.value);
        if (!Number.isInteger(count) || count < 1 || count > 100) { error.textContent = '连续轮数请填写 1–100 的整数'; return; }
        const minutes = Number(interval.value);
        if (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440) { error.textContent = '检测间隔请填写 1–1440 分钟的整数'; return; }
        const challenges = Number(challengeCount.value);
        if (![1, 2, 3].includes(challenges)) { error.textContent = '挑战次数请选择 1、2 或 3 次'; return; }
        save(id, enable || state.enabled, state.available ? (select.value || state.model) : state.model, dialog,
          { auto_pause_enabled: auto.checked, auto_pause_threshold: count, interval_minutes: minutes, challenge_count: challenges });
      });
      actions.append(submit);
    }
    body.append(label, catalogNote, mapping, intervalLabel, challengeLabel, estimate, reason, policy, historyHead, list, empty, help, error);
    dialog.append(header, body, actions);
    dialog.addEventListener('close', () => dialog.remove());
    dialog.addEventListener('click', e => { if (e.target === dialog) { const b = dialog.getBoundingClientRect(); if (e.clientX < b.left || e.clientX > b.right || e.clientY < b.top || e.clientY > b.bottom) dialog.close(); } });
    document.body.append(dialog); dialog.showModal();
    if (state.available) {
      select.disabled = true;
      const currentToken = token();
      fetch('/api/v1/admin/accounts/' + id + '/models', {
        headers: { Authorization: 'Bearer ' + currentToken }, cache: 'no-store', signal: AbortSignal.timeout(15000)
      }).then(async response => {
        if (!response.ok) throw new Error('无法读取账号可用模型，请稍后重试');
        const data = (await response.json()).data;
        const entries = Array.isArray(data) ? data : data?.models || data?.data;
        if (!Array.isArray(entries)) throw new Error('账号模型列表格式已变化，请稍后重试');
        if (currentToken !== token() || !dialog.open) return;
        const live = new Set(entries.map(item => typeof item === 'string' ? item : item.id));
        const choices = state.models.filter(model => live.has(model));
        select.replaceChildren();
        for (const model of choices) { const option = el('option', '', model); option.value = model; select.append(option); }
        select.value = preferredModel(choices, state.model);
        refreshMapping();
        select.disabled = !choices.length;
        if (submit) submit.disabled = !!enable && !choices.length;
        catalogNote.textContent = choices.length ? '' : '此账号当前可用模型尚未被指纹库收录，暂不能开启。';
      }).catch(error => {
        if (!dialog.open) return;
        catalogNote.textContent = error.name === 'TimeoutError' ? '读取模型列表超时，请关闭后重试。' : error.message;
      });
    } else catalogNote.textContent = '';
  }

  async function savePrompt(id, payload, dialog, errorBox) {
    if (pending.has(id)) return;
    pending.add(id);
    if (errorBox) errorBox.textContent = '';
    const buttons = dialog ? [...dialog.querySelectorAll('button')].map(button => [button, button.disabled]) : [];
    buttons.forEach(([button]) => { button.disabled = true; });
    try {
      const data = await api('/accounts/' + id + '/prompt-test', { method: 'PUT', body: JSON.stringify(payload) });
      states.set(id, data.account); lastError = '';
      if (dialog) dialog.close();
    } catch (error) {
      if (errorBox) errorBox.textContent = error.message;
      else lastError = error.message;
    } finally {
      pending.delete(id);
      if (dialog?.open) buttons.forEach(([button, disabled]) => { button.disabled = disabled; });
      targets().filter(target => target.id === id).forEach(render);
    }
  }

  function qaEditor(initial, presets, changed) {
    const root = el('div', 'mt-qa-editor');
    const list = el('div');
    const rows = [];
    const tools = el('div', 'mt-case-tools');
    const add = el('button', 'mt-link', '＋ 添加自定义题'); add.type = 'button';
    const candy = el('button', 'mt-link', '＋ 添加糖果题'); candy.type = 'button';
    const update = () => { add.disabled = candy.disabled = rows.length >= 5; changed(); };
    function append(data) {
      if (rows.length >= 5) return;
      const card = el('details', 'mt-case'); card.open = !data.name;
      const summary = el('summary');
      const enabled = el('input'); enabled.type = 'checkbox'; enabled.checked = !!data.enabled;
      const title = el('span', '', data.name || '未命名题目');
      const remove = el('button', 'mt-link', '删除'); remove.type = 'button';
      summary.append(enabled, title, remove); card.append(summary);
      const fields = {};
      for (const [key, label, limit] of [['name', '题目名称', 80], ['prompt', '题目提示词', 4000], ['expected_answer', '预期答案', 200]]) {
        const wrapper = el('label', 'mt-prompt-label', label);
        const input = el(key === 'prompt' ? 'textarea' : 'input');
        if (key !== 'prompt') input.type = 'text';
        input.value = data[key] || ''; input.maxLength = limit; input.setAttribute('aria-label', label);
        wrapper.append(input); card.append(wrapper); fields[key] = input;
      }
      const identity = data.id || 'q-' + crypto.randomUUID();
      const row = { card, get: () => ({ id: identity, enabled: enabled.checked,
        name: fields.name.value, prompt: fields.prompt.value, expected_answer: fields.expected_answer.value }) };
      rows.push(row); list.append(card);
      const rename = () => { title.textContent = fields.name.value || '未命名题目'; enabled.setAttribute('aria-label', '测试 ' + title.textContent); };
      rename(); fields.name.addEventListener('input', rename);
      enabled.addEventListener('click', event => event.stopPropagation());
      enabled.addEventListener('change', update);
      remove.addEventListener('click', event => { event.preventDefault(); rows.splice(rows.indexOf(row), 1); card.remove(); update(); });
      update();
    }
    add.addEventListener('click', () => append({ enabled: false }));
    candy.disabled = !presets?.length;
    candy.addEventListener('click', () => { if (presets?.[0]) append({ ...presets[0], id: undefined, enabled: false }); });
    tools.append(add); if (presets?.length) tools.append(candy);
    root.append(el('p', 'mt-note', '最多 5 道题。只测试勾选的题；关闭的题不发送请求。可修改每道题的提示词和预期答案。'), list, tools,
      el('p', 'mt-note', '按完整答案匹配，忽略首尾空白、大小写和全角差异。建议要求只输出答案；回答中仅包含答案数字不算通过。'));
    initial.forEach(append);
    return { root, values: () => rows.map(row => row.get()) };
  }

  function openPromptSettings(id) {
    const state = states.get(id);
    const promptState = state?.prompt_test;
    if (!state || !promptState) return;
    document.querySelectorAll('.mt-dialog').forEach(node => node.remove());
    const dialog = el('dialog', 'mt-dialog');
    const header = el('div', 'mt-header');
    const title = el('h3', '', `自定义提示词测试 · 账号 #${id}`);
    const badge = el('span', 'mt-badge mt-prompt-badge', promptState.running ? '测试中' : promptState.enabled ? '已开启' : '已关闭');
    badge.dataset.enabled = String(!!promptState.enabled);
    header.append(title, badge);
    const body = el('div', 'mt-body');
    const modeLabel = el('label', 'mt-prompt-label', '测试方式');
    const mode = el('select'); mode.setAttribute('aria-label', '测试方式');
    for (const [value, text] of [['qa', '问答测试（按预期答案）'], ['attribution', '数字归因模板（原算法）']]) {
      const option = el('option', '', text); option.value = value; mode.append(option);
    }
    mode.value = promptState.test_mode || 'attribution'; modeLabel.append(mode);
    const qaEnabledLabel = el('label', 'mt-qa-enabled');
    const qaEnabled = el('input'); qaEnabled.type = 'checkbox';
    qaEnabled.checked = mode.value === 'qa' && !!promptState.enabled;
    qaEnabled.setAttribute('role', 'switch'); qaEnabled.setAttribute('aria-label', '启用定时问答测试');
    qaEnabledLabel.append(qaEnabled, document.createTextNode('启用定时问答测试'));
    const modelLabel = el('label', 'mt-prompt-label', '测试模型');
    const model = el('select');
    let desiredModel = promptState.model || state.model;
    model.addEventListener('change', () => { desiredModel = model.value; });
    let choices = state.models || [], liveChoices = null, catalogReady = false;
    for (const name of choices) { const option = el('option', '', name); option.value = name; model.append(option); }
    model.value = preferredModel(choices, promptState.model || state.model);
    model.setAttribute('aria-label', '测试模型'); modelLabel.append(model);
    const promptLabel = el('label', 'mt-prompt-label', '测试提示词模板');
    const prompt = el('textarea'); prompt.maxLength = 4000; prompt.value = promptState.prompt || '{{challenge}}';
    prompt.setAttribute('aria-label', '测试提示词'); promptLabel.append(prompt);
    const templateNote = el('p', 'mt-note', '保留一个 {{challenge}}，每次自动插入新的数字挑战。普通问答不能直接用于此归因算法；修改模板可能影响统计可靠性。');
    const challengeLabel = el('label', 'mt-prompt-label', '每轮挑战次数');
    const challenges = el('select'); challenges.setAttribute('aria-label', '提示词挑战次数');
    for (const count of [1, 2, 3]) { const option = el('option', '', String(count)); option.value = String(count); challenges.append(option); }
    challenges.value = String(promptState.challenge_count || 3); challengeLabel.append(challenges);
    const intervalLabel = el('label', 'mt-interval', '测试间隔');
    const interval = el('input'); interval.type = 'number'; interval.min = '1'; interval.max = '1440'; interval.step = '1';
    interval.value = String(promptState.interval_minutes || 10); interval.setAttribute('aria-label', '测试间隔（分钟）');
    intervalLabel.append(interval, document.createTextNode('分钟'));
    const estimate = el('p', 'mt-note');
    let editor;
    const refreshEstimate = () => {
      const minutes = Number(interval.value), samples = mode.value === 'qa'
        ? (editor?.values() || []).filter(item => item.enabled).length : Number(challenges.value);
      estimate.textContent = Number.isInteger(minutes) && minutes >= 1 && minutes <= 1440
        ? `通常 ${samples} 次 / 轮，${mode.value === 'qa' ? '不符复测' : '补采或复测'}最多 ${samples * 2} 次 / 轮；每天通常约 ${Math.ceil(1440 / minutes) * samples} 次，最多约 ${Math.ceil(1440 / minutes) * samples * 2} 次，会消耗账号额度。` : '';
    };
    const initialCases = promptState.cases?.length || mode.value === 'qa' ? (promptState.cases || []) : (promptState.qa_presets || []);
    editor = qaEditor(initialCases, promptState.qa_presets, refreshEstimate);
    interval.addEventListener('input', refreshEstimate); challenges.addEventListener('change', refreshEstimate); refreshEstimate();
    const policy = el('div', 'mt-policy');
    const autoLabel = el('label');
    const auto = el('input'); auto.type = 'checkbox'; auto.checked = !!promptState.auto_pause_enabled;
    auto.setAttribute('role', 'switch'); auto.setAttribute('aria-label', '连续红点自动停止调度');
    autoLabel.append(auto, document.createTextNode('连续红点自动停止调度'));
    const thresholdLabel = el('label', '', '连续');
    const threshold = el('input'); threshold.type = 'number'; threshold.min = '1'; threshold.max = '100'; threshold.step = '1';
    threshold.value = String(promptState.auto_pause_threshold || 3); threshold.disabled = !auto.checked;
    threshold.setAttribute('aria-label', '连续红点轮数');
    const policyFields = el('div', 'mt-policy-fields'); policyFields.hidden = !auto.checked;
    auto.addEventListener('change', () => { threshold.disabled = !auto.checked; policyFields.hidden = !auto.checked; });
    thresholdLabel.append(threshold, document.createTextNode('轮红点后停止'));
    policyFields.append(thresholdLabel, el('p', 'mt-note', `当前连续红点 ${promptState.failure_streak || 0} / ${promptState.auto_pause_threshold || 3}；绿、黄均清零。`));
    policy.append(autoLabel, policyFields);
    if (promptState.pause_status === 'pending' || promptState.pause_status === 'failed') policy.append(el('p', 'mt-error', promptState.message));
    if (promptState.paused_at) policy.append(el('p', 'mt-note', `上次停调度：${date(promptState.paused_at)} · ${promptState.pause_message}`));
    const help = el('details', 'mt-help');
    help.append(el('summary', '', '测试说明'),
      el('p', '', '测试通过 GN2API 的账号测试接口发送，使用该账号现有出站链路；传输是否为 WS 取决于实际选路，不能由归因颜色判断。'),
      el('p', '', '绿：高分归因与预期一致。黄：样本不足、区分度不足、请求失败或复测不稳定。红：两轮独立归因均高分指向同一个其他模型。候选库内分数不是真伪证明。'),
      el('p', '', '问答模式：绿表示所选题目全部符合预期；黄表示请求失败、未完成或复测不稳定；红表示至少一题两次返回同一非预期答案，且本轮没有黄色结果。问答颜色不证明模型身份。每道开启的题每轮发一次，不符时最多复测一次。'),
      el('p', '', '停止调度只停止后续新请求，不终止已经进行中的业务请求；恢复调度需要在 sub2api 中手动操作。'));
    const history = el('ul', 'mt-history mt-prompt-history');
    const historyHead = el('div', 'mt-section-head'); historyHead.append(el('span', '', '最近测试结果'));
    const historyItems = [];
    for (const result of [...(promptState.history || [])].reverse().slice(0, 10)) {
      const item = el('li');
      const color = ['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow';
      const dot = el('span', 'mt-dot mt-' + color);
      const details = el('details', 'mt-result');
      const summary = el('summary');
      summary.append(dot, el('time', '', shortDate(result.checked_at)), el('span', 'mt-result-title',
        (result.test_mode === 'qa' ? '问答 · ' : '归因 · ') + (color === 'green' ? '与预期一致' : color === 'red' ? '复测仍不符' : '待确认')));
      summary.title = result.message || '';
      details.append(summary, el('pre', '', description(result)));
      item.append(details); history.append(item); historyItems.push(item);
    }
    const empty = el('p', 'mt-note', historyItems.length ? '' : '暂无测试记录');
    const error = el('p', 'mt-error mt-save-error');
    const actions = el('div', 'mt-actions');
    const close = el('button', '', '关闭'); close.type = 'button'; close.addEventListener('click', () => dialog.close());
    const saveButton = el('button', 'mt-primary', promptState.enabled ? '保存设置' : '开启测试'); saveButton.type = 'button';
    const availability = el('p', 'mt-error');
    const catalogNote = el('p', 'mt-note', '正在读取账号可用模型…');
    function refreshMode() {
      const qa = mode.value === 'qa';
      promptLabel.hidden = templateNote.hidden = challengeLabel.hidden = qa;
      editor.root.hidden = qaEnabledLabel.hidden = !qa;
      choices = liveChoices ? liveChoices.filter(name => qa || (state.models || []).includes(name)) : (state.models || []);
      model.replaceChildren();
      for (const name of choices) { const option = el('option', '', name); option.value = name; model.append(option); }
      model.value = preferredModel(choices, desiredModel);
      model.disabled = !catalogReady || !choices.length;
      const available = (qa ? promptState.qa_available : promptState.attribution_available) ?? promptState.available;
      availability.textContent = available ? '' : ((qa ? promptState.qa_reason : promptState.attribution_reason) || promptState.reason);
      saveButton.textContent = qa ? '保存设置' : promptState.enabled ? '保存设置' : '开启测试';
      saveButton.disabled = qa && !qaEnabled.checked ? false : (!available || !catalogReady || !choices.length);
      refreshEstimate();
    }
    mode.addEventListener('change', () => {
      auto.checked = mode.value === (promptState.test_mode || 'attribution') && !!promptState.auto_pause_enabled;
      threshold.disabled = !auto.checked; policyFields.hidden = !auto.checked;
      error.textContent = ''; refreshMode();
    });
    qaEnabled.addEventListener('change', refreshMode);
    saveButton.addEventListener('click', () => {
      const minutes = Number(interval.value), count = Number(threshold.value);
      const qa = mode.value === 'qa';
      if ((!qa || qaEnabled.checked) && !model.value.trim()) { error.textContent = '请填写测试模型'; return; }
      if (!qa && (!prompt.value.trim() || prompt.value.length > 4000)) { error.textContent = '提示词不能为空且不能超过 4000 个字符'; return; }
      if (!qa && prompt.value.split('{{challenge}}').length !== 2) { error.textContent = '请保留且仅保留一个 {{challenge}} 数字挑战占位符'; return; }
      if (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440) { error.textContent = '测试间隔请填写 1–1440 分钟的整数'; return; }
      if (!Number.isInteger(count) || count < 1 || count > 100) { error.textContent = '连续红点轮数请填写 1–100 的整数'; return; }
      const cases = editor.values();
      if (qa && qaEnabled.checked && !cases.some(item => item.enabled)) { error.textContent = '请至少勾选一道题，或关闭定时测试后保存'; return; }
      if (qa && cases.some(item => item.enabled && (!item.name.trim() || !item.prompt.trim() || !item.expected_answer.trim()))) {
        error.textContent = '请填写已勾选题目的名称、提示词和预期答案'; return;
      }
      const payload = { enabled: qa ? qaEnabled.checked : true, model: model.value || promptState.model || 'gpt-6-astra',
        prompt: prompt.value, challenge_count: Number(challenges.value),
        auto_pause_enabled: auto.checked, auto_pause_threshold: count, interval_minutes: minutes };
      if (qa || promptState.test_mode === 'qa') payload.test_mode = mode.value;
      if (qa) payload.cases = cases;
      savePrompt(id, payload, dialog, error);
    });
    const disable = el('button', '', '关闭测试'); disable.type = 'button'; disable.disabled = !promptState.enabled;
    disable.addEventListener('click', () => savePrompt(id, { enabled: false }, dialog, error));
    actions.append(close, disable, saveButton);
    body.append(modeLabel, modelLabel, catalogNote, qaEnabledLabel, editor.root, promptLabel, templateNote, intervalLabel, challengeLabel, estimate,
      availability, policy, historyHead, history, empty, help, error);
    dialog.append(header, body, actions);
    dialog.addEventListener('close', () => dialog.remove());
    document.body.append(dialog); dialog.showModal();
    refreshMode();
    const currentToken = token();
    fetch('/api/v1/admin/accounts/' + id + '/models', {
      headers: { Authorization: 'Bearer ' + currentToken }, cache: 'no-store', signal: AbortSignal.timeout(15000)
    }).then(async response => {
      if (!response.ok) throw new Error('无法读取账号可用模型，请稍后重试');
      const data = (await response.json()).data;
      const entries = Array.isArray(data) ? data : data?.models || data?.data;
      if (!Array.isArray(entries)) throw new Error('账号模型列表格式已变化');
      if (!dialog.open || currentToken !== token()) return;
      liveChoices = [...new Set(entries.map(item => typeof item === 'string' ? item : item.id).filter(name => typeof name === 'string' && name))];
      catalogReady = true; catalogNote.textContent = liveChoices.length ? '' : '账号当前没有可用模型'; refreshMode();
    }).catch(error => { if (dialog.open) catalogNote.textContent = error.name === 'TimeoutError' ? '读取模型列表超时，请关闭后重试' : error.message; });
  }

  const observer = new MutationObserver(records => {
    if (!onPage()) return;
    // Ignore changes made inside this extension to avoid render/observer loops.
    if (!records.some(r => !(r.target instanceof Element) || !r.target.closest('.mt-row,.mt-dialog'))) return;
    clearTimeout(reconcileTimer); reconcileTimer = setTimeout(reconcile, 180);
  });
  observer.observe(document.body, { childList: true, subtree: true });
  setInterval(() => { if (!document.hidden) reconcile(); }, 3000);
  window.addEventListener('popstate', reconcile);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) reconcile(); });
  reconcile();
})();
