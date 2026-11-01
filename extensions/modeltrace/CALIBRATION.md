# 当前指纹库与推理档位

核对来源为 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)，核对日期 2026-11-01。

- [GitHub Pages 实现](https://github.com/xqy2006/ModelTrace/blob/main/static/pages-app.js) 只生成题目、接收手动粘贴的回答并在本地评分，不向模型发请求，因此没有模型推理等级默认值。
- [自动 API 请求构造](https://github.com/xqy2006/ModelTrace/blob/main/enrollment.py#L193-L240) 没有设置 `reasoning_effort` 或 `reasoning.effort`，也没有为 gpt-6-astra 指定档位。未发送参数只能表述为跟随所接上游，不能证明实际采用 low、medium、high 或 xhigh。
- [GPT 建库原始记录](https://github.com/xqy2006/ModelTrace/blob/main/data/gpt_reference.jsonl) 中有 36 条 gpt-6-astra 记录，均没有推理等级字段。记录中的 `temperature: provider_default` 是温度，不能用于推断推理等级。

因此当前四档请求都仍标记 `bank_applicability=unverified`。本地修复提供明确的“指纹库未校准”状态及候选分数，不把固定黄灯改成无证据的绿灯，不启动依赖归因身份结论的自动停调度。问答题继续按用户设置的完整答案匹配规则评分。

要恢复经校准的统计判定，需要：

1. 使用来源可信的参考账号，为各候选模型、各推理档位采集样本；固定客户端版本、指令、上下文、工具策略、温度及传输路径，保存实际出站请求、完整回答和关联 ID。不能将正在被质疑的账号输出自动标注成真实模型身份。
2. 按账号、采集批次和环境划分建库与独立验证样本，检查混淆矩阵、未知模型误归类、候选分数可靠性。现有建库样本直接回测本库不能代替独立验证。
3. 将验证范围绑定库哈希及请求条件；模型、档位、指令或传输条件改变时，不能继续沿用旧验证结论。仅在独立验证达到审阅过的标准后启用相应统计结果与确认流程。

本次没有执行新的付费模型采集，也没有生成或宣称任何新的校准通过结论。
