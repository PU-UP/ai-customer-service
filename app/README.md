AI 客服机器人（WeCom -> Worker -> LLM/FAQ/敏感词 -> 回答）。

本目录是 Python 包，目标是把原本集中在 `wechat_bot.py` 的职责拆到清晰的层：
- `channels/`：外部接入（目前为企业微信）
- `core/`：核心业务（策略、消息编排、LLM、worker）
- `db/`：SQLite 访问层（表结构 + 读写 + 清理）
- `main.py`：启动编排

场景差异（prompt / faq / profile）尽量通过 JSON 资产与策略层加载来体现，而不是通过改代码。

