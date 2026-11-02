/* ModelTrace account monitor. Separate from the sub2api application bundle. */
(() => {
  'use strict';
  if (window.__aifxoModelTraceLoaded) return;
  window.__aifxoModelTraceLoaded = true;
  const API = '/custom/modeltrace/api';
  const states = new Map();
  let loading = false, lastPoll = 0, lastToken = '', lastError = '', reconcileTimer, mutationRevision = 0;
  const pending = new Set();
  const actionErrors = new Map();
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
    .mt-dots{display:inline-flex;align-items:center;gap:4px;flex-wrap:nowrap}.mt-dots:empty{display:none}
    .mt-dot{width:8px;height:8px;border-radius:50%;background:#94a3b8;flex-shrink:0}
    .mt-green{background:#10b981}.mt-yellow{background:#eab308}.mt-red{background:#ef4444}
    .mt-dots[data-disabled=true]{opacity:.5}.mt-dots[data-stale=true]{opacity:.5}
    .mt-state{font-size:10px;color:#94a3b8}.mt-error{color:#dc2626}
    .mt-dialog{box-sizing:border-box;border:1px solid #e2e8f0;border-radius:16px;padding:0;width:min(520px,calc(100vw - 24px));max-height:90vh;overflow:hidden;color:#334155;background:#fff;box-shadow:0 16px 60px #0003;font:14px/1.6 system-ui,sans-serif}
    .mt-dialog[open]{display:flex;flex-direction:column}.mt-dialog [hidden]{display:none!important}
    .mt-dialog::backdrop{background:#0f172a77}.dark .mt-dialog{background:#111827;color:#e2e8f0;border-color:#374151}
    .mt-header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px 10px;flex-shrink:0}
    .mt-close{appearance:none;display:grid;place-items:center;width:32px;height:32px;border:1px solid #cbd5e1;border-radius:6px;background:transparent;color:inherit;font:22px/1 system-ui;cursor:pointer;flex-shrink:0}.mt-close:hover{background:#f1f5f9}.mt-close:focus-visible{outline:2px solid #3b82f6;outline-offset:2px}
    .mt-dialog h3{font-size:17px;font-weight:600;margin:0}.mt-dialog p{margin:8px 0}.mt-dialog p:empty{display:none}
    .mt-badge{font-size:11px;color:#64748b;background:#f1f5f9;border-radius:12px;padding:2px 9px;white-space:nowrap}.mt-badge[data-enabled=true]{background:#ecfdf5;color:#047857}
    .mt-body{padding:0 18px;overflow:auto;min-height:0}.mt-dialog label{font-size:13px}
    .mt-tabs{display:flex;gap:6px;padding:0 18px 10px;flex-shrink:0}.mt-tab{position:relative;flex:1;padding:7px;border:0;border-radius:6px;background:transparent;color:inherit;font:inherit;cursor:pointer}.mt-tab[aria-selected=true]{background:#ecfdf5;color:#047857}.dark .mt-tab[aria-selected=true]{background:#064e3b;color:#a7f3d0}.mt-tab:focus-visible{outline:2px solid #3b82f6;outline-offset:2px}.mt-tab:disabled{opacity:.5;cursor:wait}.mt-tab[data-dirty=true]::after{content:'';position:absolute;right:7px;top:7px;width:6px;height:6px;border-radius:50%;background:#dc2626}
    .mt-settings-dialog{width:min(760px,calc(100vw - 24px))}.mt-settings-panel{display:flex;flex-direction:column;min-height:0;overflow:hidden}.mt-panel-status{margin:0 0 10px!important}.mt-save-error:not(.mt-error){color:#059669}.mt-inline-toggle{display:flex;align-items:center;gap:5px;padding-bottom:8px;white-space:nowrap}.mt-header h3{margin-right:auto}.mt-actions>.mt-note{margin-right:auto;align-self:center}.mt-tab[data-dirty=true]::after{content:'未保存';font-size:10px;position:static;display:inline;margin-left:8px;width:auto;height:auto;background:transparent;color:#b45309}
    .mt-dialog select{display:block;width:100%;padding:9px 10px;border:1px solid #cbd5e1;border-radius:7px;margin-top:5px;color:inherit;background:transparent;font:inherit}
    .mt-dialog select option{color:#334155;background:#fff}.mt-dialog .mt-note{font-size:12px;color:#64748b}.dark .mt-dialog .mt-note{color:#94a3b8}
    .mt-actions{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:10px;padding:10px 18px;flex-shrink:0;border-top:1px solid #e2e8f066}.mt-actions button{padding:7px 16px;border-radius:6px;border:1px solid #cbd5e1;background:transparent;color:inherit;cursor:pointer;font:inherit}
    .mt-actions button.mt-primary{background:#059669;border-color:#059669;color:white}.mt-actions button:disabled{opacity:.5;cursor:wait}
    .mt-section-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:18px;font-size:13px;font-weight:600}
    .mt-link{appearance:none;border:0;padding:2px 0;background:transparent;color:#64748b;font:12px system-ui;cursor:pointer}.mt-link:hover{color:#059669}
    .mt-history{padding:0;list-style:none;max-height:260px;overflow:auto;margin:4px 0 0}.mt-history li{border-bottom:1px solid #e2e8f066;font-size:12px}
    .mt-result>summary{display:flex;align-items:center;gap:8px;padding:10px 0;cursor:pointer;list-style:none;min-width:0}.mt-result>summary::-webkit-details-marker{display:none}
    .mt-result>summary::after{content:'›';color:#94a3b8;margin-left:auto}.mt-result[open]>summary::after{transform:rotate(90deg)}
    .mt-result time{color:#64748b;white-space:nowrap;font-variant-numeric:tabular-nums;flex-shrink:0}.mt-result-title{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex-shrink:1;min-width:54px}.mt-result-inline{display:flex;align-items:center;gap:8px;min-width:0;overflow:hidden;white-space:nowrap;color:#64748b}.mt-result-case{max-width:145px;overflow:hidden;text-overflow:ellipsis;flex-shrink:1}.mt-result-case button{max-width:145px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:middle}.mt-result>summary::after{flex-shrink:0}
    .mt-result pre{margin:0 0 10px 17px;white-space:pre-wrap;overflow-wrap:anywhere;color:#64748b;font:12px/1.7 system-ui}
    .mt-answer-links{display:flex;flex-direction:column;align-items:flex-start;margin:0 0 10px 17px;gap:5px}.mt-answer-text{white-space:pre-wrap;overflow-wrap:anywhere;font:13px/1.7 system-ui}.mt-evidence-events{max-height:45vh;overflow:auto}.mt-evidence-paging{display:flex;gap:14px;align-items:center;margin:10px 0}.mt-evidence-entry{border-bottom:1px solid #e2e8f066}
    .mt-help{margin:14px 0;font-size:12px;color:#64748b}.mt-help>summary{cursor:pointer;width:fit-content}.mt-help p{line-height:1.7}
    .dark .mt-result pre,.dark .mt-result time,.dark .mt-help,.dark .mt-link{color:#94a3b8}
    .mt-setting-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px 12px;align-items:end}.mt-setting-grid .mt-prompt-label{display:block;min-width:0;margin:0}.mt-setting-grid select{margin-top:4px;padding:7px 8px}
    .mt-schedule-row{display:grid;grid-template-columns:max-content max-content minmax(0,1fr);gap:9px;align-items:center;margin-top:10px}.mt-interval{display:flex;align-items:center;gap:4px;white-space:nowrap;margin:0}.mt-interval input{width:58px;margin:0 2px;padding:5px 6px;border:1px solid #cbd5e1;border-radius:6px;background:transparent;color:inherit;font:inherit}
    .mt-schedule-row .mt-interval select{display:inline-block;width:auto;max-width:105px;margin:0;padding:5px}.mt-schedule-qa{grid-template-columns:180px 1fr}.mt-schedule-row>.mt-prompt-label{display:flex;align-items:center;gap:5px;margin:0;white-space:nowrap}.mt-schedule-row>.mt-prompt-label select{width:55px;margin:0;padding:5px}.mt-info{display:inline-flex;position:relative;align-items:center;font-size:12px}.mt-info>summary{list-style:none;cursor:pointer}.mt-info[open]>span{position:absolute;z-index:2;right:0;top:24px;width:240px;white-space:normal;padding:10px;background:#fff;color:#475569;border:1px solid #cbd5e1;border-radius:6px;box-shadow:0 4px 12px #0002}.dark .mt-info[open]>span{background:#111827;color:#e2e8f0}
    .mt-policy{display:flex;align-items:center;gap:8px;min-width:0;margin:0;padding:0;border:0}.mt-policy label{display:flex;align-items:center;gap:6px;white-space:nowrap}.mt-policy input[type=checkbox]{margin:0;accent-color:#059669;width:16px;height:16px;vertical-align:middle}.dark .mt-policy{border-color:#374151}
    .mt-policy input[type=number]{box-sizing:border-box;width:54px;margin:0;padding:4px 6px;border:1px solid #cbd5e1;border-radius:6px;background:transparent;color:inherit;font:inherit}.mt-policy-fields{display:flex;align-items:center;gap:8px;min-width:0}.mt-policy-fields[hidden]{display:none}.mt-policy-fields .mt-note{display:none}.mt-policy-explanation{margin:4px 0 0!important}.mt-estimate{margin:4px 0 0!important}.mt-prompt-label{display:block;margin-top:10px}.mt-dialog textarea{box-sizing:border-box;width:100%;min-height:90px;resize:vertical;padding:9px 10px;border:1px solid #cbd5e1;border-radius:7px;margin-top:5px;color:inherit;background:transparent;font:inherit;line-height:1.5}.mt-prompt-history .mt-result-title{color:#7c3aed}
    .mt-case{margin:12px 0;padding:12px;border:1px solid #cbd5e1;border-radius:9px}.mt-case>summary{display:flex;gap:8px;align-items:center;cursor:pointer;font-size:13px}.mt-case>summary span{flex:1;overflow-wrap:anywhere}.mt-case input[type=checkbox],.mt-qa-enabled input{accent-color:#059669}
    .mt-case input[type=text]{box-sizing:border-box;width:100%;padding:8px 10px;border:1px solid #cbd5e1;border-radius:7px;background:transparent;color:inherit;font:inherit;margin-top:5px}.mt-case-tools{display:flex;gap:14px;flex-wrap:wrap}.mt-qa-enabled{display:block;margin-top:14px}
    .mt-preview-dialog{width:min(960px,calc(100vw - 24px))}.mt-preview-dialog iframe{display:block;width:100%;height:55vh;min-height:240px;border:1px solid #e2e8f0;border-radius:8px;background:#fff}.mt-preview-dialog .mt-actions{flex-wrap:wrap}.mt-preview-dialog .mt-header h3{overflow-wrap:anywhere}.mt-preview-dialog textarea{min-height:180px;font:12px/1.5 monospace}.mt-preview-link{display:block;margin:0 0 12px 17px;color:#7c3aed}.mt-actions .mt-review-green{border-color:#059669;color:#047857}.mt-actions .mt-review-yellow{border-color:#ca8a04;color:#a16207}.mt-actions .mt-review-red{border-color:#dc2626;color:#dc2626}
    @media(max-width:600px){.mt-header{padding:12px 12px 8px}.mt-tabs{padding:0 12px 8px}.mt-body{padding:0 12px}.mt-actions{padding:9px 12px}.mt-setting-grid{grid-template-columns:1fr 1fr}.mt-schedule-row{grid-template-columns:1fr 1fr}.mt-schedule-row .mt-policy{grid-column:1/-1;flex-wrap:wrap}.mt-result>summary{gap:5px;flex-wrap:wrap}.mt-result-inline{order:2;flex-basis:100%;flex-wrap:wrap;white-space:normal;padding-left:13px}.mt-result-case,.mt-result-case button{max-width:100%;font-size:12px}.mt-dialog{width:calc(100vw - 16px);max-height:94vh}}
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
  const effortField = (value, name) => {
    const label = el('label', 'mt-prompt-label', name);
    const select = el('select'); select.setAttribute('aria-label', name);
    for (const [effort, title] of [['low', '低 · low'], ['medium', '中 · medium'], ['high', '高 · high'], ['xhigh', '最高 · xhigh']]) {
      const option = el('option', '', title); option.value = effort; select.append(option);
    }
    select.value = value || 'xhigh'; label.append(select);
    return { label, select };
  };
  const resultLabel = result => {
    if (result.test_mode !== 'qa') {
      if (result.outcome === 'candidate_mismatch_confirmed') return `候选复测不符 · ${result.prediction}`;
      if (result.outcome === 'candidate_unstable') return '候选不稳定';
      if (result.outcome === 'candidate_match') return `候选相符 · ${result.prediction}`;
      if (result.outcome === 'candidate_ambiguous') return '候选分数未拉开';
      if (result.outcome === 'candidate_mismatch' || (result.outcome === 'bank_unverified'
          && result.candidate_separated && result.prediction && result.prediction !== result.expected)) {
        return `候选不符 · ${result.prediction}${result.sampling_policy === 'single-pass-v1' ? '' : '（历史单轮结果）'}`;
      }
    }
    if (result.status === 'green') return result.test_mode === 'qa' ? '通过' : '与预期一致';
    if (result.status === 'red') return result.test_mode === 'qa' ? '未通过' : '复测仍不符';
    const titles = { round_timeout: '检测超时', gateway_timeout: '网关测试超时', stream_interrupted: '响应连接中断',
      proxy_connect_timeout: '代理连接超时', proxy_connect_error: '代理连接失败',
      connect_timeout: '连接上游超时', connect_error: '连接上游失败', response_header_timeout: '等待上游超时',
      tls_handshake_timeout: 'TLS 握手超时', tls_handshake_error: 'TLS 握手失败',
      upstream_overloaded: '上游繁忙', upstream_error: '上游错误', insufficient_samples: '样本不足',
      bank_unverified: '指纹库未校准', capability_unavailable: '接口不支持此档位',
      output_limit: '回答文本超限', stream_limit: '传输数据超限', artifact_limit: '作品过大', artifact_format: '作品格式不正确' };
    if (titles[result.outcome]) return titles[result.outcome];
    // Old results retain their original evidence; only their display label changes.
    if (/超时/.test(result.message || '')) return '检测超时';
    if (result.bank_applicability === 'unverified') return '指纹库未校准';
    if (/样本不足|有效回答.*上限/.test(result.message || '')) return '样本不足';
    if (/输出超出限制/.test(result.message || '')) return '输出超限';
    if (/测试失败|上游.*错误|上游.*报错/.test(result.message || '')) return '上游错误';
    if (result.outcome === 'test_error') return '检测失败';
    if (result.requests === 0) return '本轮跳过';
    if ((result.cases || []).some(item => item.kind === 'manual' && item.status === 'yellow')) return '待人工评分';
    if (result.test_mode === 'qa' && result.round_complete) return '回答不一致';
    return '待确认';
  };
  const description = result => result.test_mode === 'qa' ? [
    date(result.checked_at), `问答测试 · 请求模型：${result.expected}`,
    `推理等级：${result.reasoning_effort || '旧记录未保存'}`,
    result.budget_seconds && `本轮时间预算：${result.budget_seconds} 秒`,
    `本轮请求：${result.requests || 0} 次`, result.cases?.length ? `本轮共 ${result.cases.length} 题` : result.message,
    ...(result.cases || []).map(item => item.kind === 'manual'
      ? `${item.name}：${item.message || '待人工确认'}${item.reviewed_at ? '\n人工评分时间：' + date(item.reviewed_at) : ''}`
      : `${item.name}：${item.message || item.answer_preview || '未完成'}\n预期答案：${item.expected_answer} · 本题请求：${item.requests ?? item.attempts?.length ?? 0} 次`),
    result.manual_reviewed && '人工评分只更新历史记录，不追溯触发自动停调度',
    '按答案判断，不代表模型身份归因'
  ].filter(Boolean).join('\n') : [
    date(result.checked_at), `预期：${result.expected}`, result.sent_model && `发送模型：${result.sent_model}`,
    `推理等级：${result.reasoning_effort || '旧记录未保存'}`,
    result.budget_seconds && `本轮时间预算：${result.budget_seconds} 秒；实际耗时：${result.elapsed_seconds ?? '未知'} 秒`,
    result.prediction && `闭集候选：${result.prediction}`,
    typeof result.probability === 'number' && `候选库内分数：${(result.probability * 100).toFixed(1)}%`,
    typeof result.requests === 'number' && `本轮请求：${result.requests} 次`,
    typeof result.challenge_count === 'number' && `挑战设置：${result.challenge_count} 次`,
    typeof result.valid_samples === 'number' && `有效挑战回答：${result.valid_samples} 份；数字样本不足：${result.insufficient_samples || 0} 次`,
    result.message, result.bank_applicability === 'unverified' && '指纹适用性：当前条件未验证（不影响候选比较）', '候选分数不是模型身份概率'
  ].filter(Boolean).join('\n');

  async function apiResponse(path, options = {}) {
    const currentToken = token();
    if (!currentToken) throw new Error('请使用管理员账号登录');
    const response = await fetch(API + path, {
      ...options, headers: { 'Authorization': 'Bearer ' + currentToken, 'Content-Type': 'application/json' },
      credentials: 'same-origin', cache: 'no-store', signal: AbortSignal.timeout(12000)
    });
    if (!response.ok) {
      const data = await response.json();
      throw new Error(data.error || '检测服务暂不可用');
    }
    if (currentToken !== token()) throw new Error('登录状态已变化');
    return response;
  }

  async function api(path, options = {}) {
    const currentToken = token(), data = await (await apiResponse(path, options)).json();
    if (currentToken !== token()) throw new Error('登录状态已变化');
    return data;
  }

  function saveBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const link = el('a'); link.href = url; link.download = filename; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function openAnswer(title, output) {
    const dialog = el('dialog', 'mt-dialog mt-preview-dialog');
    const header = el('div', 'mt-header');
    const close = el('button', 'mt-close', '×'); close.type = 'button'; close.onclick = () => dialog.close();
    header.append(el('h3', '', title), close);
    const body = el('div', 'mt-body'), text = el('pre', 'mt-answer-text'); body.append(text);
    const actions = el('div', 'mt-actions'), info = el('span', 'mt-note');
    const prev = el('button', '', '上一页'), next = el('button', '', '下一页');
    let page = 0;
    const render = () => {
      text.textContent = output.slice(page * 4000, (page + 1) * 4000);
      info.textContent = `第 ${page + 1}/${Math.max(1, Math.ceil(output.length / 4000))} 页 · 共 ${output.length} 字`;
      prev.disabled = page === 0; next.disabled = (page + 1) * 4000 >= output.length; body.scrollTop = 0;
    };
    prev.onclick = () => { page--; render(); }; next.onclick = () => { page++; render(); };
    actions.append(info, prev, next); dialog.append(header, body, actions);
    dialog.addEventListener('close', () => dialog.remove());
    document.body.append(dialog); render(); dialog.showModal();
  }

  function answerControls(id, result) {
    const links = el('div', 'mt-answer-links');
    const inline = el('span', 'mt-result-inline');
    for (const item of result.cases || []) {
      if (item.kind === 'manual') continue;
      const answers = item.attempts?.length ? item.attempts : item.answer_preview
        ? [{ output: item.answer_preview, output_truncated: item.answer_truncated, legacy: true }] : [];
      const answer = answers[0];
      const output = answer?.output || '';
      const compact = output.trim() ? output.trim().replace(/\s+/g, ' ') : resultLabel({ ...item, test_mode: 'qa' });
      const fullAnswer = answer && (!answer.output_truncated && !answer.legacy || answer.evidence_saved);
      const title = `${item.name} · ${fullAnswer ? '完整回答' : '回答摘要'}`;
      const value = el('span', 'mt-result-case');
      value.title = `${item.name}：${compact.slice(0, 160)}`;
      if (answer && (answer.output_truncated || answer.legacy || compact.length > 42)) {
        const button = el('button', 'mt-link', `${item.name}：查看${fullAnswer ? '回答' : '摘要'}`);
        button.type = 'button';
        button.addEventListener('click', async event => {
          event.preventDefault(); event.stopPropagation(); button.disabled = true;
          try {
            const saved = answer.evidence_saved ? await api(`/accounts/${id}/attempts/${answer.attempt_id}/answer`) : answer;
            openAnswer(title, (saved.output || '') + (fullAnswer ? '' : '\n\n本轮只保留摘要，未重复保存原文。'));
          } catch (error) { openAnswer('读取回答失败', error.message); }
          finally { button.disabled = false; }
        });
        value.append(button);
      } else {
        value.textContent = `${item.name}：${compact.slice(0, 42)}`;
      }
      inline.append(value);
      // Historical rounds may contain multiple attempts. Keep them accessible
      // without rendering large answers in the settings dialog.
      answers.slice(1).forEach((previous, index) => {
        const button = el('button', 'mt-link', `${item.name} · 历史第 ${index + 2} 次回答`);
        button.type = 'button';
        button.onclick = async () => {
          button.disabled = true;
          try {
            const saved = previous.evidence_saved ? await api(`/accounts/${id}/attempts/${previous.attempt_id}/answer`) : previous;
            openAnswer(button.textContent, (saved.output || '') + (previous.output_truncated && !previous.evidence_saved ? '\n\n本轮只保留摘要，未重复保存原文。' : ''));
          } catch (error) { openAnswer('读取回答失败', error.message); }
          finally { button.disabled = false; }
        };
        links.append(button);
      });
    }
    return { links, inline };
  }

  function evidenceButton(id, runId) {
    const button = el('button', 'mt-link', runId ? '查看本轮完整尝试' : '全部尝试记录（含中断）');
    button.type = 'button';
    button.addEventListener('click', () => openEvidence(id, runId));
    return button;
  }

  async function openEvidence(id, runId) {
    const dialog = el('dialog', 'mt-dialog mt-preview-dialog');
    const header = el('div', 'mt-header');
    const close = el('button', 'mt-close', '×'); close.type = 'button'; close.onclick = () => dialog.close();
    header.append(el('h3', '', '完整尝试记录'), close);
    const body = el('div', 'mt-body');
    body.append(el('p', 'mt-note', '同账号、模型、档位和题目仅保留一次正常基线与首次异常完整诊断，不覆盖、不自动删除。其余尝试保留摘要；旧记录仍可查看。这里的管理 HTTP 测试不代表桌面 WS 流量。'));
    const next = el('button', 'mt-link', '加载更多'); next.type = 'button';
    dialog.append(header, body, next);
    dialog.addEventListener('close', () => dialog.remove());
    document.body.append(dialog); dialog.showModal();
    let cursor;
    const load = async () => {
      next.disabled = true;
      try {
        const params = new URLSearchParams();
        if (runId) params.set('run_id', runId);
        if (cursor) params.set('before', cursor);
        const data = await api(`/accounts/${id}/attempts?${params}`);
        if (data.saved_evidence?.length) {
          const saved = el('div', 'mt-saved-evidence');
          saved.append(el('h4', '', '已保留的正常 / 首次异常对照'));
          for (const record of data.saved_evidence) {
            const item = el('div');
            item.append(el('span', '', `${record.slot === 'normal' ? '正常基线' : '首次异常'} · ${record.summary.case_id || '归因候选'} · ${record.summary.model} / ${record.summary.reasoning_effort} · ${date(record.created_at)} · 条件 ${record.condition_sha256.slice(0, 8)} `), evidenceButton(id, record.run_id));
            saved.append(item);
          }
          body.append(saved);
        }
        if (!data.attempts.length && !cursor) body.append(el('p', 'mt-note', '暂无完整记录。旧版本未保存的原文无法恢复。'));
        for (const attempt of data.attempts) {
          const details = el('details', 'mt-result mt-evidence-entry');
          const captureLabel = {normal: '正常基线已保留', anomaly: '首次异常已保留'}[attempt.metadata.capture_slot] || '';
          details.append(el('summary', '', `${date(attempt.started_at)} · ${attempt.metadata.case_id || '数字归因'} · 第 ${attempt.metadata.stage} 次 · ${attempt.finished.status}${captureLabel ? ' · ' + captureLabel : ''}`));
          const content = el('div'); details.append(content);
          details.addEventListener('toggle', async () => {
            if (!details.open || details.dataset.loaded || details.dataset.loading) return;
            details.dataset.loading = 'true'; content.textContent = '正在加载记录…';
            try {
              const base = `/accounts/${id}/attempts/${attempt.attempt_id}`;
              const first = await api(`${base}/events`);
              if (!dialog.open) return;
              const note = el('p', 'mt-note'), events = el('div', 'mt-evidence-events');
              const view = el('select'); view.setAttribute('aria-label', '事件视图');
              for (const [value, title] of [['key', '回答、归因及请求关联'], ['all', '全部记录（旧版含流事件）']]) {
                const option = el('option', '', title); option.value = value; view.append(option);
              }
              const paging = el('div', 'mt-evidence-paging');
              const prev = el('button', 'mt-link', '上一页事件'), nextPage = el('button', 'mt-link', '下一页事件');
              prev.type = nextPage.type = 'button';
              let cursors = [0], page = 0, nextCursor;
              const renderPage = data => {
                events.replaceChildren(); nextCursor = data.next_cursor;
                note.textContent = `${attempt.metadata.capture_mode === 'results-only-v1' ? '本次未记录原始流量。' : ''}此视图 ${data.event_count} 条 / 全部 ${data.total_event_count} 条 · 第 ${page + 1} 页。长内容显示前 2000 字；完整记录可下载 JSON。`;
                for (const event of data.events) {
                  const entry = el('details', 'mt-result mt-event');
                  entry.append(el('summary', '', `#${event.sequence} · ${event.kind}${event.truncated ? ' · 长内容预览' : ''}`),
                    el('pre', '', event.preview));
                  events.append(entry);
                }
                prev.disabled = page === 0; nextPage.disabled = nextCursor == null; events.scrollTop = 0;
              };
              const turnPage = async direction => {
                prev.disabled = nextPage.disabled = view.disabled = true;
                const target = page + direction, cursor = direction > 0 ? nextCursor : cursors[target];
                try {
                  const data = await api(`${base}/events?after=${cursor}&view=${view.value}`);
                  if (!dialog.open) return;
                  page = target; cursors[page] = cursor; renderPage(data);
                } catch (error) { note.textContent = error.message; }
                finally { view.disabled = false; prev.disabled = page === 0; nextPage.disabled = nextCursor == null; }
              };
              prev.onclick = () => turnPage(-1); nextPage.onclick = () => turnPage(1);
              view.onchange = async () => {
                prev.disabled = nextPage.disabled = view.disabled = true;
                try {
                  const data = await api(`${base}/events?view=${view.value}`);
                  if (!dialog.open) return;
                  cursors = [0]; page = 0; renderPage(data); view.dataset.loaded = view.value;
                } catch (error) { view.value = view.dataset.loaded || 'key'; note.textContent = error.message; }
                finally { view.disabled = false; prev.disabled = page === 0; nextPage.disabled = nextCursor == null; }
              };
              const download = el('button', 'mt-link', '下载完整 JSON'); download.type = 'button';
              download.onclick = async () => {
                download.disabled = true;
                try {
                  const currentToken = token(), blob = await (await apiResponse(`${base}/download`)).blob();
                  if (currentToken !== token()) throw new Error('登录状态已变化');
                  saveBlob(blob, `attempt-${attempt.attempt_id}.json`);
                }
                catch (error) { note.textContent = error.message; }
                finally { download.disabled = false; }
              };
              paging.append(prev, nextPage, download); content.replaceChildren(view, note, events, paging);
              renderPage(first); details.dataset.loaded = 'true';
            } catch (error) { content.textContent = error.message; }
            finally { delete details.dataset.loading; }
          });
          body.append(details);
        }
        cursor = data.next_cursor; next.hidden = !cursor;
      } catch (error) { body.append(el('p', 'mt-error', error.message)); }
      finally { next.disabled = false; }
    };
    next.onclick = load; await load();
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
    const promptState = state?.prompt_test;
    const modes = [state, promptState].filter(Boolean);
    const enabled = modes.some(mode => mode.enabled);
    const stale = modes.some(mode => {
      const latest = (mode.history || []).filter(result => result.expected === mode.model).at(-1);
      return mode.enabled && latest && Date.now() / 1000 - latest.checked_at > Math.max((mode.interval_minutes || 10) * 120, 300);
    });
    const actionError = actionErrors.get(target.id);
    const signature = JSON.stringify([target.id, state, pending.has(target.id), lastError, actionError, stale]);
    if (row.dataset.signature === signature) return;
    row.dataset.signature = signature;
    row.dataset.accountId = String(target.id);
    const toggle = el('button', 'mt-toggle');
    toggle.type = 'button';
    toggle.setAttribute('role', 'switch');
    toggle.setAttribute('aria-label', `账号 ${target.id} 智商检测总开关`);
    toggle.setAttribute('aria-checked', String(enabled));
    toggle.disabled = !state || pending.has(target.id) || !!lastError;
    toggle.title = enabled ? '点击关闭此账号的模型归因和自定义题目定时检测' : '打开设置，选择并保存要启用的检测';
    toggle.addEventListener('click', () => {
      if (enabled) disableDetection(target.id);
      else openSettings(target.id);
    });
    const label = el('button', 'mt-label', '智商检测');
    label.type = 'button';
    label.title = '设置模型归因、自定义题目及人工预览；圆点为两类检测合计最近 10 次结果';
    label.disabled = !state || pending.has(target.id);
    label.addEventListener('click', () => openSettings(target.id));
    const history = [
      ...(state?.history || []).map(result => ({ result, mode: state, tab: 'attribution', source: '模型归因' })),
      ...(promptState?.history || []).map(result => ({ result, mode: promptState, tab: 'custom',
        source: result.test_mode === 'qa' ? '自定义题目' : '自定义数字归因模板' }))
    ].sort((a, b) => a.result.checked_at - b.result.checked_at).slice(-10);
    const dots = el('span', 'mt-dots');
    dots.setAttribute('aria-label', '两类检测合计最近 10 次结果');
    for (const { result, mode, tab, source } of history) {
      const color = ['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow';
      const dot = el('button', 'mt-dot mt-' + color);
      dot.type = 'button';
      dot.title = source + '\n' + description(result);
      if (mode.model && result.expected !== mode.model) {
        dot.style.opacity = '.35';
        dot.title = '历史模型，非当前选择\n' + dot.title;
      } else if (!mode.enabled) dot.style.opacity = '.5';
      dot.setAttribute('aria-label', dot.title);
      dot.disabled = pending.has(target.id);
      dot.addEventListener('click', () => openSettings(target.id, tab));
      dots.append(dot);
    }
    let text = !state ? '加载中' : !enabled ? '已关闭'
      : modes.some(mode => mode.running) ? '检测中' : stale ? '结果已过期' : '已开启';
    if (modes.some(mode => mode.pause_status === 'pending')) text = '正在停调度';
    if (modes.some(mode => mode.pause_status === 'failed')) text = '停调度未确认';
    if (modes.some(mode => mode.pause_status === 'paused') && !state?.schedulable) text = '已停调度';
    if (actionError) text = '操作未完成';
    if (lastError) text = '暂不可用';
    const note = el('span', 'mt-state', text);
    note.title = lastError || actionError || [
      state && '模型归因：' + (state.enabled ? '已开启' : '已关闭') + (state.message ? '\n' + state.message : ''),
      promptState && '自定义题目 / 模板：' + (promptState.enabled ? '已开启' : '已关闭') + (promptState.message ? '\n' + promptState.message : '')
    ].filter(Boolean).join('\n');
    if (actionError || modes.some(mode => ['paused', 'failed'].includes(mode.pause_status))) note.classList.add('mt-error');
    row.replaceChildren(toggle, label, dots, note);
  }

  function reconcile() {
    clearTimeout(reconcileTimer);
    if (!onPage()) {
      document.querySelectorAll('.mt-row,.mt-dialog').forEach(node => node.remove());
      return;
    }
    const currentToken = token();
    if (lastToken !== currentToken) {
      states.clear(); actionErrors.clear(); lastPoll = 0; lastError = ''; lastToken = currentToken;
      document.querySelectorAll('.mt-dialog').forEach(node => node.remove());
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
        const revision = mutationRevision;
        const data = await api('/accounts?ids=' + ids.slice(start, start + 100).join(','));
        if (revision !== mutationRevision) { lastPoll = 0; continue; }
        for (const [id, state] of Object.entries(data.accounts)) {
          if (!pending.has(Number(id))) states.set(Number(id), state);
        }
      }
      lastError = '';
    } catch (error) {
      lastError = error.name === 'TimeoutError' ? '检测服务连接超时' : error.message;
    } finally {
      loading = false;
      targets().forEach(render);
    }
  }

  async function disableDetection(id) {
    if (pending.has(id)) return;
    const state = states.get(id);
    if (!state) return;
    const jobs = [];
    if (state.enabled) jobs.push(['模型归因', '/accounts/' + id]);
    if (state.prompt_test?.enabled) jobs.push(['自定义题目 / 模板', '/accounts/' + id + '/prompt-test']);
    pending.add(id); mutationRevision++; actionErrors.delete(id); targets().filter(t => t.id === id).forEach(render);
    const errors = [];
    try {
      for (const [name, path] of jobs) {
        try {
          const data = await api(path, { method: 'PUT', body: JSON.stringify({ enabled: false }) });
          states.set(id, data.account);
        } catch (error) { errors.push(name + '关闭未确认：' + error.message); }
      }
      // A timeout can leave a successful write without a response. Read the actual state.
      if (errors.length) {
        try {
          const data = await api('/accounts?ids=' + id);
          if (data.accounts[id]) states.set(id, data.accounts[id]);
        } catch (_) { /* Keep the last confirmed state and the explicit error. */ }
        actionErrors.set(id, errors.join('\n') + '\n请打开智商检测查看各项开关。');
      }
    } finally {
      pending.delete(id); mutationRevision++; lastPoll = 0;
      targets().filter(t => t.id === id).forEach(render);
    }
  }

  async function saveSettings(id, path, payload, dialog, errorBox, onSaved) {
    if (pending.has(id)) return false;
    actionErrors.delete(id);
    pending.add(id); mutationRevision++; targets().filter(t => t.id === id).forEach(render);
    errorBox.textContent = ''; errorBox.classList.add('mt-error');
    const controls = [...dialog.querySelectorAll('button,input,select,textarea')].map(node => [node, node.disabled]);
    controls.forEach(([node]) => { node.disabled = true; });
    dialog.setAttribute('aria-busy', 'true');
    try {
      const data = await api(path, { method: 'PUT', body: JSON.stringify(payload) });
      states.set(id, data.account); lastError = '';
      onSaved(data.account);
      errorBox.classList.remove('mt-error'); errorBox.textContent = '当前页设置已保存';
      return true;
    } catch (error) {
      errorBox.textContent = error.message;
      return false;
    } finally {
      pending.delete(id); mutationRevision++; lastPoll = 0;
      controls.forEach(([node, disabled]) => { node.disabled = disabled; });
      dialog.removeAttribute('aria-busy');
      dialog.dispatchEvent(new Event('mt-save-end'));
      targets().filter(t => t.id === id).forEach(render);
    }
  }

  function openSettings(id, activeTab) {
    const state = states.get(id);
    if (!state) return;
    document.querySelectorAll('.mt-dialog').forEach(node => node.remove());
    const dialog = el('dialog', 'mt-dialog mt-settings-dialog');
    const title = el('h3', '', `智商检测 · 账号 #${id}`); title.id = 'mt-settings-title';
    dialog.setAttribute('aria-labelledby', title.id);
    const header = el('div', 'mt-header');
    const badge = el('span', 'mt-badge');
    const forms = [];
    const footerNotice = el('span', 'mt-note');
    const actions = el('div', 'mt-actions');
    const saveAllButton = el('button', 'mt-primary', '保存设置'); saveAllButton.type = 'button';
    const refreshDirty = () => {
      for (const page of pages) {
        const form = forms.find(entry => entry.key === page.key);
        const dirty = !!form && form.dirty();
        page.button.dataset.dirty = String(dirty);
        page.button.title = dirty ? '本页有未保存修改' : '';
      }
      saveAllButton.dataset.dirty = String(forms.some(form => form.dirty()));
    };
    const registerForm = (key, panel, form) => {
      forms.push({ ...form, key, panel });
      panel.addEventListener('input', refreshDirty);
      panel.addEventListener('change', refreshDirty);
      refreshDirty();
    };
    const close = el('button', 'mt-close', '×'); close.type = 'button';
    close.setAttribute('aria-label', '关闭'); close.title = '关闭';
    close.addEventListener('click', () => {
      if (saveAllButton.disabled) return;
      if (forms.some(form => form.dirty()) && !window.confirm('有未保存的设置，确定放弃修改并关闭？')) return;
      dialog.close();
    });
    dialog.addEventListener('cancel', event => { event.preventDefault(); close.click(); });
    const refreshStatus = () => {
      const current = states.get(id);
      const enabled = !!(current?.enabled || current?.prompt_test?.enabled);
      badge.textContent = current?.running || current?.prompt_test?.running ? '检测中' : enabled ? '已开启' : '已关闭';
      badge.dataset.enabled = String(enabled);
      for (const status of dialog.querySelectorAll('[data-mode-status]')) {
        const mode = status.dataset.modeStatus === 'custom' ? current?.prompt_test : current;
        status.textContent = '此项：' + (mode?.running ? '检测中' : mode?.enabled ? '已开启' : '已关闭')
          + ' · 分页修改会保留，保存设置一并生效';
      }
    };
    header.append(title, badge, close);
    const tabs = el('div', 'mt-tabs'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', '智商检测设置');
    dialog.append(header, tabs);
    let catalog;
    const loadModels = () => catalog ||= (async () => {
      const currentToken = token();
      const response = await fetch('/api/v1/admin/accounts/' + id + '/models', {
        headers: { Authorization: 'Bearer ' + currentToken }, cache: 'no-store', signal: AbortSignal.timeout(15000)
      });
      if (!response.ok) throw new Error('无法读取账号可用模型，请关闭后重试');
      const data = (await response.json()).data;
      const entries = Array.isArray(data) ? data : data?.models || data?.data;
      if (!Array.isArray(entries)) throw new Error('账号模型列表格式已变化');
      if (currentToken !== token()) throw new Error('登录状态已变化');
      return [...new Set(entries.map(item => typeof item === 'string' ? item : item.id).filter(name => typeof name === 'string' && name))];
    })();
    const pages = [];
    const selectTab = (key, focus = false) => {
      for (const page of pages) {
        const selected = page.key === key;
        page.panel.hidden = !selected;
        page.button.setAttribute('aria-selected', String(selected));
        page.button.tabIndex = selected ? 0 : -1;
        if (selected && focus) page.button.focus();
      }
    };
    for (const [key, name, build] of [
      ['attribution', '模型归因', buildAttributionPanel], ['custom', '自定义题目', buildPromptPanel]
    ]) {
      if (key === 'custom' && !state.prompt_test) continue;
      const button = el('button', 'mt-tab', name); button.type = 'button'; button.id = 'mt-tab-' + key;
      button.setAttribute('role', 'tab'); button.setAttribute('aria-controls', 'mt-panel-' + key);
      const panel = el('section', 'mt-settings-panel'); panel.id = 'mt-panel-' + key;
      panel.setAttribute('role', 'tabpanel'); panel.setAttribute('aria-labelledby', button.id);
      button.addEventListener('click', () => selectTab(key));
      button.addEventListener('keydown', event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault();
        const index = pages.findIndex(page => page.key === key);
        const next = event.key === 'Home' ? 0 : event.key === 'End' ? pages.length - 1
          : (index + (event.key === 'ArrowRight' ? 1 : -1) + pages.length) % pages.length;
        selectTab(pages[next].key, true);
      });
      pages.push({ key, button, panel }); tabs.append(button); dialog.append(panel);
      build(id, dialog, panel, loadModels, form => registerForm(key, panel, form));
    }
    actions.append(footerNotice, saveAllButton); dialog.append(actions);
    saveAllButton.addEventListener('click', async () => {
      const dirty = forms.filter(form => form.dirty());
      if (!dirty.length) { footerNotice.textContent = '没有未保存修改'; return; }
      for (const form of dirty) {
        if (!form.validate()) { selectTab(form.key, true); refreshDirty(); return; }
      }
      saveAllButton.disabled = true; footerNotice.textContent = '正在保存…';
      try {
        for (const form of dirty) {
          const ok = await form.save();
          if (!ok) { selectTab(form.key, true); footerNotice.textContent = '有一页未保存成功，请检查提示'; return; }
          form.commit(); refreshDirty();
        }
        footerNotice.textContent = '所有修改已保存';
      } finally {
        saveAllButton.disabled = false; refreshStatus(); refreshDirty();
      }
    });
    dialog.addEventListener('mt-save-end', refreshStatus);
    dialog.addEventListener('close', () => dialog.remove());
    activeTab ||= state.prompt_test?.enabled && !state.enabled ? 'custom' : 'attribution';
    selectTab(pages.some(page => page.key === activeTab) ? activeTab : 'attribution'); refreshStatus();
    document.body.append(dialog); dialog.showModal();
  }

  function buildAttributionPanel(id, dialog, panel, loadModels, registerForm) {
    let state = states.get(id);
    const body = el('div', 'mt-body');
    const status = el('p', 'mt-note mt-panel-status'); status.dataset.modeStatus = 'attribution';
    const enabledLabel = el('label', 'mt-inline-toggle');
    const enabled = el('input'); enabled.type = 'checkbox'; enabled.checked = !!state.enabled;
    enabled.setAttribute('role', 'switch'); enabled.setAttribute('aria-label', '启用定时模型归因');
    enabledLabel.append(enabled, document.createTextNode('启用定时模型归因'));
    let catalogReady = false;
    const label = el('label', 'mt-prompt-label', '检测模型');
    const select = el('select');
    const catalogNote = el('p', 'mt-note', '正在读取此账号的可用模型…');
    select.setAttribute('aria-label', '检测模型');
    for (const model of state.models) {
      const option = el('option', '', model); option.value = model; select.append(option);
    }
    select.value = preferredModel(state.models, state.model);
    select.disabled = !state.available;
    label.append(select);
    const effort = effortField(state.reasoning_effort, '归因推理等级');
    const settingsGrid = el('div', 'mt-setting-grid'); settingsGrid.append(label, effort.label, enabledLabel);
    const budgetNote = el('p', 'mt-note', '每轮最多 12 分钟，只跑一轮，不自动补采或复测。若检测超过设置的间隔，完成后再等一个间隔，避免连续补跑。修改推理等级会取消旧轮次后续请求；已发出的请求可能已计费。');
    const bankNote = el('p', 'mt-note', '当前指纹库尚未完成各档位校准，候选分数不证明实际型号。开启自动停止后，仍按你设置的连续不符轮数停止调度。');
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
    for (const [value, text] of [[1, '1 次'], [2, '2 次'], [3, '3 次']]) {
      const option = el('option', '', text); option.value = String(value); challengeCount.append(option);
    }
    challengeCount.value = String(state.challenge_count || 3);
    challengeLabel.append(challengeCount);
    const estimate = el('p', 'mt-note mt-estimate');
    const refreshEstimate = () => {
      const minutes = Number(interval.value);
      const count = Number(challengeCount.value);
      estimate.textContent = Number.isInteger(minutes) && minutes >= 1 && minutes <= 1440
        ? `每轮最多 ${count} 次，不自动复测；通常约 ${Math.round(1440 * count / minutes)} 次请求 / 天。` : '';
    };
    interval.addEventListener('input', refreshEstimate); refreshEstimate();
    challengeCount.addEventListener('change', refreshEstimate);
    const help = el('details', 'mt-help');
    help.append(el('summary', '', '检测说明'),
      el('p', '', '一轮候选分数拉开后，与请求模型不符即标红，相符标绿；分数未拉开或样本不足标黄。这些颜色表示候选比较，不是型号认证。开启自动停止后，连续红点达到设置轮数即停止调度，不受校准提示阻拦。'),
      el('p', '', '挑战次数可选 1、2、3，默认 3，设置几次最多就发几次；样本不足不追加请求。校准需要已知型号在相同档位、指令、上下文及传输条件下采样，并通过独立样本验证误判率，不能靠同一待测账号反复出现同一候选来校准。'),
      el('p', '', '自动停止只计连续红点，绿点或黄点清零。连续 3 轮与每轮 3 个挑战是两个设置，一轮最多累计 1 次不符。更改次数、策略或模型后重新计数。更改次数会取消旧轮次的后续请求和结果，下一轮仍按原计划执行；已发出的请求可能已计费。停止后当前业务请求继续，检测暂停，恢复调度需手动操作。'),
      el('p', '', '间隔支持 1–1440 分钟。仅改间隔不打断当前检测，下一轮从保存时重新计时。关闭页面仍会检测，繁忙时可能延后。'),
      el('p', '', '模型列表仅显示账号可用且指纹库已收录的模型。首次选择优先 gpt-6-astra，保留已有设置；当前库尚未收录 gpt-6.1-sol。'),
      el('p', '', '删除账号后会自动清理该账号的扩展设置、测试记录、留证和作品；停用或限流不会删除记录。清理在检测轮次之间执行。'));
    const reason = el('p', 'mt-note', state.reason || '');
    const policy = el('div', 'mt-policy');
    const autoLabel = el('label');
    const auto = el('input'); auto.type = 'checkbox'; auto.checked = !!state.auto_pause_enabled;
    auto.setAttribute('role', 'switch'); auto.setAttribute('aria-label', '连续不符自动停止调度');
    autoLabel.append(auto, document.createTextNode('连续不符停调度'));
    const thresholdLabel = el('label');
    const threshold = el('input'); threshold.type = 'number'; threshold.min = '1'; threshold.max = '100'; threshold.step = '1';
    threshold.value = String(state.auto_pause_threshold || 3); threshold.disabled = !auto.checked;
    threshold.setAttribute('aria-label', '连续不符轮数');
    const policyFields = el('div', 'mt-policy-fields'); policyFields.hidden = !auto.checked;
    const refreshPolicy = () => { threshold.disabled = !auto.checked; policyFields.hidden = !auto.checked; progress.hidden = !auto.checked; };
    auto.addEventListener('change', refreshPolicy);
    thresholdLabel.append(document.createTextNode('连续'), threshold, document.createTextNode('轮'));
    const progress = el('p', 'mt-note', `当前连续不符 ${state.mismatch_streak || 0} / ${state.auto_pause_threshold || 3} 轮 · 仅计红点`);
    policyFields.append(thresholdLabel);
    policy.append(autoLabel, policyFields);
    const policyNotes = el('div', 'mt-policy-notes'); policyNotes.append(progress);
    const policyInfo = el('details', 'mt-info');
    policyInfo.append(el('summary', '', 'ⓘ'), el('span', '', '每轮最多计一次不符；绿、黄清零。达到设置轮数即停止调度，校准提示不阻拦。挑战次数与连续轮数分别计算。'));
    policyInfo.querySelector('summary').setAttribute('aria-label', '自动停止说明');
    policy.append(policyInfo);
    if (state.pause_status === 'pending' || state.pause_status === 'failed') policyNotes.append(el('p', 'mt-error', state.message));
    if (state.paused_at) policyNotes.append(el('p', 'mt-note', `上次停调度：${date(state.paused_at)} · ${state.pause_message}`));
    refreshPolicy();
    const schedule = el('div', 'mt-schedule-row'); schedule.append(intervalLabel, challengeLabel, policy);
    const list = el('ul', 'mt-history');
    const historyHead = el('div', 'mt-section-head');
    historyHead.append(el('span', '', '最近结果'), evidenceButton(id));
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
      const text = resultLabel(result);
      summary.append(dot, when, el('span', 'mt-result-title', text));
      summary.title = resultLabel(result);
      details.append(summary, el('pre', '', description(result)));
      if (result.run_id) details.append(evidenceButton(id, result.run_id));
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
    let baseline;
    const formPayload = () => ({
      enabled: enabled.checked, model: state.available ? (select.value || state.model) : state.model,
      reasoning_effort: effort.select.value,
      auto_pause_enabled: auto.checked, auto_pause_threshold: Number(threshold.value),
      interval_minutes: Number(interval.value), challenge_count: Number(challengeCount.value)
    });
    const validate = () => {
      error.classList.add('mt-error');
      const count = Number(threshold.value), minutes = Number(interval.value), challenges = Number(challengeCount.value);
      const message = !Number.isInteger(count) || count < 1 || count > 100 ? '连续轮数请填写 1–100 的整数'
        : !Number.isInteger(minutes) || minutes < 1 || minutes > 1440 ? '检测间隔请填写 1–1440 分钟的整数'
          : ![1, 2, 3].includes(challenges) ? '挑战次数请选择 1、2 或 3 次'
            : enabled.checked && !select.value ? '请选择检测模型' : '';
      error.textContent = message;
      return !message;
    };
    const saved = account => {
      state = account; enabled.checked = !!state.enabled;
      progress.textContent = `当前连续不符 ${state.mismatch_streak || 0} / ${state.auto_pause_threshold || 3} 轮 · 仅计红点`;
    };
    const refreshAvailability = () => {
      select.disabled = !state.available || !catalogReady || !select.options.length;
    };
    enabled.addEventListener('change', refreshAvailability);
    dialog.addEventListener('mt-save-end', refreshAvailability);
    body.addEventListener('input', () => { error.textContent = ''; error.classList.add('mt-error'); });
    help.append(bankNote, budgetNote);
    body.append(status, settingsGrid, catalogNote, mapping, schedule, estimate, policyNotes, reason, historyHead, list, empty, help, error);
    panel.append(body); refreshAvailability();
    const serialize = value => JSON.stringify(value);
    const form = {
      dirty: () => baseline !== serialize(formPayload()), validate,
      save: () => saveSettings(id, '/accounts/' + id, formPayload(), dialog, error, saved),
      commit: () => { baseline = serialize(formPayload()); }
    };
    form.commit(); registerForm(form);
    if (state.available) {
      loadModels().then(models => {
        if (!dialog.open) return;
        const pristine = !form.dirty();
        const desired = select.value;
        const live = new Set(models);
        const choices = state.models.filter(model => live.has(model));
        select.replaceChildren();
        for (const model of choices) { const option = el('option', '', model); option.value = model; select.append(option); }
        select.value = preferredModel(choices, pristine ? state.model : desired);
        refreshMapping();
        catalogReady = true;
        if (!pending.has(id)) refreshAvailability();
        catalogNote.textContent = choices.length ? '' : '此账号当前可用模型尚未被指纹库收录，暂不能开启。';
        if (pristine) form.commit();
        panel.dispatchEvent(new Event('change'));
      }).catch(error => {
        if (!dialog.open) return;
        catalogNote.textContent = error.name === 'TimeoutError' ? '读取模型列表超时，请关闭后重试。' : error.message;
      });
    } else catalogNote.textContent = '';
  }

  async function openArtifactPreview(id, result, item, onReview) {
    const dialog = el('dialog', 'mt-dialog mt-preview-dialog');
    const header = el('div', 'mt-header'); header.append(el('h3', '', item.name + ' · 人工预览'));
    const body = el('div', 'mt-body');
    const status = el('p', 'mt-note', '正在读取作品…');
    const metadata = el('p', 'mt-note', `请求模型：${result.expected} · ${date(result.checked_at)}`);
    const note = el('p', 'mt-note', '查看是否符合提示词后再标记颜色。预览支持内联 HTML、SVG、CSS 和 JavaScript，外部资源不加载。评分不会追溯停止账号调度。');
    const frame = el('iframe'); frame.title = item.name + '作品预览';
    // Generated code gets an opaque origin and no access to the administration page.
    frame.setAttribute('sandbox', 'allow-scripts');
    frame.setAttribute('referrerpolicy', 'no-referrer');
    frame.setAttribute('allow', "camera 'none'; microphone 'none'; geolocation 'none'; clipboard-read 'none'; clipboard-write 'none'");
    const source = el('details', 'mt-help'); source.append(el('summary', '', '查看生成的 HTML'));
    const code = el('textarea'); code.readOnly = true; code.setAttribute('aria-label', '生成的 HTML 源码'); source.append(code);
    const promptDetails = el('details', 'mt-help'); promptDetails.append(el('summary', '', '本次使用的提示词'));
    const originalPrompt = el('p'); promptDetails.append(originalPrompt);
    const error = el('p', 'mt-error');
    const actions = el('div', 'mt-actions');
    const close = el('button', '', '关闭'); close.type = 'button'; close.addEventListener('click', () => dialog.close());
    actions.append(close);
    const reviewButtons = [];
    const base = '/accounts/' + id + '/prompt-test/results/' + result.result_id;
    const casePath = encodeURIComponent(item.id);
    for (const [color, title] of [['green', '绿 · 符合要求'], ['yellow', '黄 · 待确认'], ['red', '红 · 不符合']]) {
      const button = el('button', 'mt-review-' + color, title); button.type = 'button'; button.disabled = true;
      button.addEventListener('click', async () => {
        reviewButtons.forEach(node => { node.disabled = true; }); error.textContent = '';
        try {
          const data = await api(base + '/reviews/' + casePath, { method: 'PUT', body: JSON.stringify({ status: color }) });
          states.set(id, data.account);
          const updated = data.account.prompt_test.history.find(entry => entry.result_id === result.result_id);
          const reviewed = updated?.cases.find(entry => entry.id === item.id);
          status.textContent = reviewed?.message || '人工评分已保存';
          if (updated) onReview(updated);
          targets().filter(target => target.id === id).forEach(render);
        } catch (failure) { if (dialog.open) error.textContent = failure.message; }
        finally { if (dialog.open) reviewButtons.forEach(node => { node.disabled = false; }); }
      });
      reviewButtons.push(button); actions.append(button);
    }
    body.append(status, metadata, note, promptDetails, frame, source, error); dialog.append(header, body, actions);
    dialog.addEventListener('close', () => { frame.srcdoc = ''; dialog.remove(); });
    document.body.append(dialog); dialog.showModal();
    try {
      const data = await api(base + '/artifacts/' + casePath);
      if (!dialog.open || !dialog.isConnected) return;
      if (typeof data.html !== 'string') throw new Error('作品内容格式无效');
      code.value = data.html;
      originalPrompt.textContent = data.prompt || '';
      const policy = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:; connect-src 'none'; frame-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'";
      const preview = Object.hasOwn(data, 'preview_html') ? data.preview_html : data.html;
      if (typeof preview !== 'string') throw new Error(data.preview_error || '未找到可预览的完整 HTML / SVG；可展开源码查看原回答。');
      frame.srcdoc = '<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="' + policy + '"><meta name="referrer" content="no-referrer"></head><body>' + preview + '</body></html>';
      status.textContent = item.message || '待人工确认';
      reviewButtons.forEach(node => { node.disabled = false; });
    } catch (failure) { if (dialog.open) error.textContent = failure.message; }
  }

  function qaEditor(initial, presets, changed) {
    const root = el('div', 'mt-qa-editor');
    const list = el('div');
    const rows = [];
    const tools = el('div', 'mt-case-tools');
    const add = el('button', 'mt-link', '＋ 添加自定义题'); add.type = 'button';
    const presetButtons = [];
    const tiboPreset = (presets || []).find(preset => preset.id === 'tibo');
    const update = () => { add.disabled = rows.length >= 5; presetButtons.forEach(button => { button.disabled = add.disabled; }); changed(); root.dispatchEvent(new Event('change', { bubbles: true })); };
    function append(data) {
      if (rows.length >= 5) return;
      if ((data.kind || 'exact') === 'exact' && tiboPreset?.expected_answer
          && data.prompt === tiboPreset.prompt && !(data.expected_answer || '').trim()) {
        data = { ...data, expected_answer: tiboPreset.expected_answer };
      }
      const card = el('details', 'mt-case'); card.open = !data.name;
      const summary = el('summary');
      const enabled = el('input'); enabled.type = 'checkbox'; enabled.checked = !!data.enabled;
      const title = el('span', '', data.name || '未命名题目');
      const remove = el('button', 'mt-link', '删除'); remove.type = 'button';
      summary.append(enabled, title, remove); card.append(summary);
      const fields = {};
      const wrappers = {};
      const kindLabel = el('label', 'mt-prompt-label', '判断方式');
      const kind = el('select'); kind.setAttribute('aria-label', '题目判断方式');
      for (const [value, title] of [['exact', '预期答案匹配'], ['manual', '人工预览 HTML / SVG']]) {
        const option = el('option', '', title); option.value = value; kind.append(option);
      }
      kind.value = data.kind || 'exact'; kindLabel.append(kind); card.append(kindLabel);
      for (const [key, label, limit] of [['name', '题目名称', 80], ['prompt', '题目提示词', 4000], ['expected_answer', '预期答案', 200]]) {
        const wrapper = el('label', 'mt-prompt-label', label);
        const input = el(key === 'prompt' ? 'textarea' : 'input');
        if (key !== 'prompt') input.type = 'text';
        input.value = data[key] || ''; input.maxLength = limit; input.setAttribute('aria-label', label);
        wrapper.append(input); card.append(wrapper); fields[key] = input; wrappers[key] = wrapper;
      }
      const manualNote = el('p', 'mt-note', '每轮生成一次，不自动复测。作品先记黄点，之后在最近结果中预览并人工评分；不按固定答案判断。');
      card.append(manualNote);
      const refreshKind = () => { wrappers.expected_answer.hidden = kind.value === 'manual'; manualNote.hidden = kind.value !== 'manual'; update(); };
      kind.addEventListener('change', refreshKind);
      const identity = data.id || 'q-' + crypto.randomUUID();
      const row = { card, get: () => ({ id: identity, enabled: enabled.checked, kind: kind.value,
        name: fields.name.value, prompt: fields.prompt.value, expected_answer: fields.expected_answer.value }) };
      rows.push(row); list.append(card);
      const rename = () => { title.textContent = fields.name.value || '未命名题目'; enabled.setAttribute('aria-label', '测试 ' + title.textContent); };
      rename(); fields.name.addEventListener('input', rename);
      enabled.addEventListener('click', event => event.stopPropagation());
      enabled.addEventListener('change', update);
      remove.addEventListener('click', event => { event.preventDefault(); rows.splice(rows.indexOf(row), 1); card.remove(); update(); });
      refreshKind();
    }
    add.addEventListener('click', () => append({ enabled: false }));
    tools.append(add);
    for (const preset of presets || []) {
      const title = preset.id === 'candy' ? '糖果题' : preset.id === 'pelican_bicycle' ? '鹈鹕动画题' : preset.name;
      const button = el('button', 'mt-link', '＋ 添加' + title); button.type = 'button';
      button.addEventListener('click', () => append({ ...preset, id: undefined, enabled: false }));
      presetButtons.push(button); tools.append(button);
    }
    root.append(el('p', 'mt-note', '最多 5 道题。只测试勾选的题；关闭的题不发送请求。可修改每道题的提示词和预期答案。'), list, tools,
      el('p', 'mt-note', '答案匹配题按完整答案比较，忽略首尾空白、大小写和全角差异。作品题选择人工预览，无需填写预期答案。'));
    initial.forEach(append);
    return { root, values: () => rows.map(row => row.get()) };
  }

  function buildPromptPanel(id, dialog, panel, loadModels, registerForm) {
    const state = states.get(id);
    let promptState = state.prompt_test;
    const body = el('div', 'mt-body');
    const status = el('p', 'mt-note mt-panel-status'); status.dataset.modeStatus = 'custom';
    const modeLabel = el('label', 'mt-prompt-label', '测试方式');
    const mode = el('select'); mode.setAttribute('aria-label', '测试方式');
    for (const [value, text] of [['qa', '自定义题目（答案匹配 / 人工预览）'], ['attribution', '数字归因模板（原算法）']]) {
      const option = el('option', '', text); option.value = value; mode.append(option);
    }
    mode.value = promptState.test_mode || 'attribution'; modeLabel.append(mode);
    const enabledLabel = el('label', 'mt-qa-enabled');
    const enabled = el('input'); enabled.type = 'checkbox'; enabled.checked = !!promptState.enabled;
    enabled.setAttribute('role', 'switch'); enabled.setAttribute('aria-label', '启用此页定时测试');
    enabledLabel.append(enabled, document.createTextNode('启用此页定时测试'));
    const modelLabel = el('label', 'mt-prompt-label', '测试模型');
    const model = el('select');
    let desiredModel = promptState.model || state.model;
    model.addEventListener('change', () => { desiredModel = model.value; });
    let choices = state.models || [], liveChoices = null, catalogReady = false;
    for (const name of choices) { const option = el('option', '', name); option.value = name; model.append(option); }
    model.value = preferredModel(choices, promptState.model || state.model);
    model.setAttribute('aria-label', '测试模型'); modelLabel.append(model);
    const effort = effortField(promptState.reasoning_effort, '题目推理等级');
    const settingsGrid = el('div', 'mt-setting-grid'); settingsGrid.append(modeLabel, modelLabel, effort.label);
    const budgetNote = el('p', 'mt-note', '每轮最多 12 分钟；每题一次，数字归因按所选次数运行，不自动补采或复测。修改推理等级会取消旧轮次后续请求。数字归因仍需匹配对应档位的指纹库校准条件；问答按所设答案判断。');
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
      const selected = (editor?.values() || []).filter(item => item.enabled);
      const minutes = Number(interval.value), samples = mode.value === 'qa' ? selected.length : Number(challenges.value);
      const maximum = samples;
      estimate.textContent = Number.isInteger(minutes) && minutes >= 1 && minutes <= 1440
        ? `每轮最多 ${maximum} 次，不自动复测；每天最多约 ${Math.ceil(1440 / minutes) * maximum} 次，会消耗账号额度。` : '';
    };
    const initialCases = promptState.cases?.length || mode.value === 'qa' ? (promptState.cases || []) : (promptState.qa_presets || []);
    editor = qaEditor(initialCases, promptState.qa_presets, refreshEstimate);
    interval.addEventListener('input', refreshEstimate); challenges.addEventListener('change', refreshEstimate); refreshEstimate();
    const policy = el('div', 'mt-policy');
    const autoLabel = el('label');
    const auto = el('input'); auto.type = 'checkbox'; auto.checked = !!promptState.auto_pause_enabled;
    auto.setAttribute('role', 'switch'); auto.setAttribute('aria-label', '连续红点自动停止调度');
    autoLabel.append(auto, document.createTextNode('连续红点停调度'));
    const thresholdLabel = el('label');
    const threshold = el('input'); threshold.type = 'number'; threshold.min = '1'; threshold.max = '100'; threshold.step = '1';
    threshold.value = String(promptState.auto_pause_threshold || 3); threshold.disabled = !auto.checked;
    threshold.setAttribute('aria-label', '连续红点轮数');
    const policyFields = el('div', 'mt-policy-fields'); policyFields.hidden = !auto.checked;
    const refreshPolicy = () => { threshold.disabled = !auto.checked; policyFields.hidden = !auto.checked; progress.hidden = !auto.checked; };
    auto.addEventListener('change', refreshPolicy);
    thresholdLabel.append(document.createTextNode('连续'), threshold, document.createTextNode('轮'));
    const progress = el('p', 'mt-note', `当前连续红点 ${promptState.failure_streak || 0} / ${promptState.auto_pause_threshold || 3}；绿、黄均清零。`);
    policyFields.append(thresholdLabel);
    policy.append(autoLabel, policyFields);
    const policyNotes = el('div', 'mt-policy-notes'); policyNotes.append(progress);
    const policyInfo = el('details', 'mt-info');
    policyInfo.append(el('summary', '', 'ⓘ'), el('span', '', '只计完整红点；绿、黄清零。包含人工预览题的轮次不累计自动停调度红点；人工评分只更新记录。'));
    policyInfo.querySelector('summary').setAttribute('aria-label', '自动停止说明');
    policy.append(policyInfo);
    if (promptState.pause_status === 'pending' || promptState.pause_status === 'failed') policyNotes.append(el('p', 'mt-error', promptState.message));
    if (promptState.paused_at) policyNotes.append(el('p', 'mt-note', `上次停调度：${date(promptState.paused_at)} · ${promptState.pause_message}`));
    refreshPolicy();
    const schedule = el('div', 'mt-schedule-row'); schedule.append(intervalLabel, challengeLabel, policy);
    const help = el('details', 'mt-help');
    help.append(el('summary', '', '测试说明'),
      el('p', '', '测试通过 GN2API 的账号测试接口发送，使用该账号现有出站链路；传输是否为 WS 取决于实际选路，不能由归因颜色判断。'),
      el('p', '', '归因模式：按设置的挑战次数只跑一轮，候选分数拉开且不符标红，相符标绿；样本不足或分数未拉开标黄。候选比较不认证实际型号；开启自动停止后，连续不符达到设置轮数仍会停止调度。'),
      el('p', '', '问答模式：每道开启的题只请求一次，不符也不复测。绿表示全部符合预期；红表示至少一题不符，且本轮没有未完成题；黄表示请求失败、未完成或待人工评分。问答颜色不证明模型身份。'),
      el('p', '', '人工预览题：每轮生成一次作品，先记黄点待确认。在最近结果中点击“查看作品”，手动选择绿、黄或红。人工评分会标记来源和时间，不计入自动停调度。作品与最近 10 轮历史一起保留，每件最多 512 KiB。'),
      el('p', '', '问答失败按题显示一次；代理连接、TLS 握手与上游繁忙分别标注。连续不符停止只统计完整红点，连接错误和黄色结果不计入。'),
      el('p', '', '间隔、挑战次数与自动停止参数在一行设置。自动停止只统计用户选定阈值，绿、黄都会清零。'),
      el('p', '', '停止调度只停止后续新请求，不终止已经进行中的业务请求；恢复调度需要在 sub2api 中手动操作。'),
      el('p', '', '删除账号后自动清理其测试记录、留证和作品；清理在检测轮次之间执行，停用或限流不会触发删除。'));
    const history = el('ul', 'mt-history mt-prompt-history');
    const historyHead = el('div', 'mt-section-head'); historyHead.append(el('span', '', '最近测试结果'), evidenceButton(id));
    const historyItems = [];
    for (const initialResult of [...(promptState.history || [])].reverse().slice(0, 10)) {
      let result = initialResult;
      const item = el('li');
      const dot = el('span', 'mt-dot');
      const details = el('details', 'mt-result');
      const summary = el('summary');
      const resultTitle = el('span', 'mt-result-title');
      const resultDescription = el('pre');
      const answers = result.test_mode === 'qa' ? answerControls(id, result) : null;
      const inline = answers?.inline || el('span', 'mt-result-inline');
      const refreshResult = () => {
        const color = ['green', 'yellow', 'red'].includes(result.status) ? result.status : 'yellow';
        const manual = (result.cases || []).some(entry => entry.kind === 'manual');
        dot.className = 'mt-dot mt-' + color;
        resultTitle.textContent = (manual ? '含人工预览 · ' : result.test_mode === 'qa' ? '问答 · ' : '归因 · ')
          + resultLabel(result);
        summary.title = resultLabel(result); resultDescription.textContent = description(result);
      };
      for (const entry of result.cases || []) {
        if (entry.kind !== 'manual') continue;
        const value = el('span', 'mt-result-case');
        if (entry.artifact_available && result.result_id) {
          const preview = el('button', 'mt-link', `${entry.name}：查看作品`); preview.type = 'button';
          preview.addEventListener('click', event => {
            event.preventDefault(); event.stopPropagation();
            openArtifactPreview(id, result, result.cases.find(current => current.id === entry.id),
              updated => { result = updated; refreshResult(); });
          });
          value.append(preview);
        } else value.textContent = `${entry.name}：${entry.message || '待评分'}`;
        inline.append(value);
      }
      summary.append(dot, el('time', '', shortDate(result.checked_at)), resultTitle, inline);
      refreshResult(); details.append(summary, resultDescription);
      if (answers) details.append(answers.links);
      if (result.run_id) details.append(evidenceButton(id, result.run_id));
      item.append(details);
      history.append(item); historyItems.push(item);
    }
    const empty = el('p', 'mt-note', historyItems.length ? '' : '暂无测试记录');
    const error = el('p', 'mt-error mt-save-error');
    const saved = account => {
      promptState = account.prompt_test; enabled.checked = !!promptState.enabled;
      progress.textContent = `当前连续红点 ${promptState.failure_streak || 0} / ${promptState.auto_pause_threshold || 3}；绿、黄均清零。`;
    };
    const availability = el('p', 'mt-error');
    const catalogNote = el('p', 'mt-note', '正在读取账号可用模型…');
    function refreshMode() {
      const qa = mode.value === 'qa';
      promptLabel.hidden = templateNote.hidden = challengeLabel.hidden = qa;
      editor.root.hidden = !qa;
      choices = liveChoices ? liveChoices.filter(name => qa || (state.models || []).includes(name)) : (state.models || []);
      model.replaceChildren();
      for (const name of choices) { const option = el('option', '', name); option.value = name; model.append(option); }
      model.value = preferredModel(choices, desiredModel);
      model.disabled = !catalogReady || !choices.length;
      const available = (qa ? promptState.qa_available : promptState.attribution_available) ?? promptState.available;
      availability.textContent = available ? '' : ((qa ? promptState.qa_reason : promptState.attribution_reason) || promptState.reason);
      schedule.classList.toggle('mt-schedule-qa', qa);
      refreshEstimate();
    }
    mode.addEventListener('change', () => {
      auto.checked = mode.value === (promptState.test_mode || 'attribution') && !!promptState.auto_pause_enabled;
      refreshPolicy();
      error.textContent = ''; refreshMode();
    });
    enabled.addEventListener('change', refreshMode);
    dialog.addEventListener('mt-save-end', refreshMode);
    body.addEventListener('input', () => { error.textContent = ''; error.classList.add('mt-error'); });
    const validate = () => {
      error.classList.add('mt-error');
      const minutes = Number(interval.value), count = Number(threshold.value);
      const qa = mode.value === 'qa';
      if (enabled.checked && !model.value.trim()) { error.textContent = '请填写测试模型'; return; }
      if (!qa && (!prompt.value.trim() || prompt.value.length > 4000)) { error.textContent = '提示词不能为空且不能超过 4000 个字符'; return; }
      if (!qa && prompt.value.split('{{challenge}}').length !== 2) { error.textContent = '请保留且仅保留一个 {{challenge}} 数字挑战占位符'; return; }
      if (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440) { error.textContent = '测试间隔请填写 1–1440 分钟的整数'; return; }
      if (!Number.isInteger(count) || count < 1 || count > 100) { error.textContent = '连续红点轮数请填写 1–100 的整数'; return; }
      const cases = editor.values();
      if (qa && enabled.checked && !cases.some(item => item.enabled)) { error.textContent = '请至少勾选一道题，或关闭定时测试后保存'; return; }
      if (qa && cases.some(item => item.enabled && (!item.name.trim() || !item.prompt.trim() || (item.kind !== 'manual' && !item.expected_answer.trim())))) {
        error.textContent = '请填写已勾选题目的名称和提示词；答案匹配题还需填写预期答案'; return;
      }
      error.textContent = '';
      return true;
    };
    const formPayload = () => {
      const payload = { enabled: enabled.checked, model: model.value || promptState.model || 'gpt-6-astra',
        reasoning_effort: effort.select.value,
        prompt: prompt.value, challenge_count: Number(challenges.value),
        auto_pause_enabled: auto.checked, auto_pause_threshold: Number(threshold.value), interval_minutes: Number(interval.value) };
      payload.test_mode = mode.value;
      if (mode.value === 'qa') payload.cases = editor.values();
      return payload;
    };
    body.append(status, enabledLabel, settingsGrid, catalogNote, schedule, estimate, policyNotes,
      availability, editor.root, promptLabel, templateNote, historyHead, history, empty, help, error);
    help.append(budgetNote);
    panel.append(body);
    refreshMode();
    let baseline = JSON.stringify(formPayload());
    const form = { dirty: () => baseline !== JSON.stringify(formPayload()), validate,
      save: () => saveSettings(id, '/accounts/' + id + '/prompt-test', formPayload(), dialog, error, saved),
      commit: () => { baseline = JSON.stringify(formPayload()); } };
    registerForm(form);
    loadModels().then(models => {
      if (!dialog.open) return;
      const pristine = !form.dirty();
      liveChoices = models; catalogReady = true;
      catalogNote.textContent = liveChoices.length ? '' : '账号当前没有可用模型';
      if (!pending.has(id)) refreshMode();
      if (pristine) form.commit();
      panel.dispatchEvent(new Event('change'));
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
