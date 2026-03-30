# AI Customer Service

这是一个可切换渠道 + 可切换场景的智能客服项目：
- `channel`：外部输入输出方式（企业微信 webhook / 终端）
- `core`：客服策略与 LLM 编排
- `scenario`：个性化资产（`system_prompt.txt` / `club_profile.json` / `faq.json`）

---

## 1. 快速开始（推荐）

### 第一步：准备环境变量
在项目根目录创建 `.env`（可由 `.env.example` 复制）：

```bash
cp .env.example .env
```

然后按你的实际环境填写密钥（尤其是企业微信和 LLM key）。

### 第二步：选择运行模式
在 `.env` 中设置：

- `CHANNEL_DRIVER=wecom_webhook` 或 `CHANNEL_DRIVER=terminal_cli`
- `SCENARIO=tennis_club`（未来可替换成其它场景）

### 第三步：启动

```bash
python run_customer_service.py
```

仅需要根目录启动即可。

---

## 2. 两种使用方式

### A) 终端模式（本地调试最快）
配置：

```env
CHANNEL_DRIVER=terminal_cli
SCENARIO=tennis_club
```

启动后直接在终端输入问题，输入 `exit` 退出。

适合：
- 快速验证 FAQ/提示词效果
- 不依赖企业微信联调

### B) 企业微信 Webhook 模式（线上接入）
配置：

```env
CHANNEL_DRIVER=wecom_webhook
SCENARIO=tennis_club
```

并确保以下环境变量正确：
- `TOKEN`
- `ENCODING_AES_KEY`
- `CORP_ID`
- `CORP_SECRET`
- `LLM_API_KEY`（或 `OPENAI_API_KEY` / `ALI_API_KEY` / `DASHSCOPE_API_KEY`）

启动后提供：
- `GET /wechat`：企业微信验签
- `POST /wechat`：消息回调入口

---

## 3. 场景切换用法（核心）

默认场景目录结构（示例资产，随仓库提交）：

```text
app/scenarios/<SCENARIO>/
├─ system_prompt.txt
├─ club_profile.json
└─ faq.json
```

例如当前示例：

```text
app/scenarios/tennis_club/
```

但实际使用时，程序会**优先读取本地场景目录**（默认加入 `.gitignore`，用于放你的私有/可变内容）：

```text
app/scenarios_local/<SCENARIO>/
├─ system_prompt.txt
├─ club_profile.json
└─ faq.json
```

你要切换到另一个业务（比如足球俱乐部）时，推荐流程：
1. 从示例复制一份到本地目录：`app/scenarios/tennis_club/` -> `app/scenarios_local/football_club/`（或直接新建并放入这 3 个文件）
2. 将 `.env` 改为 `SCENARIO=football_club`
3. 重启服务

不需要改 `core` 代码。

---

## 4. 关键配置说明（.env）

- `CHANNEL_DRIVER`：渠道驱动  
  - `wecom_webhook`：企业微信
  - `terminal_cli`：终端交互
- `SCENARIO`：场景名称（决定默认资产目录）
- `SCENARIOS_LOCAL_DIR`：可选，本地场景根目录（默认 `app/scenarios_local/`）
- `SYSTEM_PROMPT_PATH`：可选，手动覆盖 prompt 路径
- `CLUB_PROFILE_PATH`：可选，手动覆盖 profile 路径
- `FAQ_PATH`：可选，手动覆盖 FAQ 路径
- `SQLITE_PATH`：SQLite 文件路径（默认在 `app/data/`）

---

## 5. 目录用途（简版）

```text
app/
├─ channels/   # 外部接入层
├─ core/       # 智能客服核心
├─ db/         # 数据访问层代码（不是数据库文件）
├─ data/       # 运行时数据文件目录（db 文件在这里）
├─ scenarios/        # 场景示例资产目录（随仓库提交）
└─ scenarios_local/  # 本地场景资产目录（默认忽略，优先读取）
```

---

## 6. 注意事项

- `.env` 必须放在项目根目录（与 `app/` 同级）
- 不要提交真实 `.env`（已在 `.gitignore`）
- `db/` 放的是 Python 代码；数据库文件应在 `data/` 或外部挂载目录

---

## 7. 管理后台（网页）使用说明

该项目内置一个简单的管理后台，用于在网页里编辑当前 `SCENARIO` 的 3 份资产：
- `club_profile.json`
- `faq.json`
- `system_prompt.txt`

### 开启与访问

1) 在 `.env` 中设置管理员口令（必填）：

```env
ADMIN_TOKEN=your_admin_token
```

2) 启动服务（需要 `CHANNEL_DRIVER=wecom_webhook`，因为后台挂在 Flask 服务上）后访问：
- `GET /admin`（未登录会跳转到 `/admin/login`）

### 保存写到哪里

- 后台保存时会**写入本地覆盖目录**：`app/scenarios_local/<SCENARIO>/`（默认已加入 `.gitignore`）
- 写入后会触发**热加载**，进程会立刻使用新配置（无需重启）

---

## 数据库查看脚本（简易）

脚本：`scripts/customer_db_visualize.py`

默认读取：
- 项目根目录 `.env`
- 数据库路径优先取 `SQLITE_PATH`，没有则回退到 `app/data/ai_customer_service.db`

常用用法：
- 用户列表：`python scripts/customer_db_visualize.py --limit 20`
- 查看用户对话：`python scripts/customer_db_visualize.py --user "external_userid" --open-kfid "open_kfid"`
- 昵称模糊检索：`python scripts/customer_db_visualize.py --nickname "张"`
- 查看数据库结构：`python scripts/customer_db_visualize.py --schema`

