# AI Customer Service

这是一个可切换渠道 + 可切换场景的智能客服项目：
- `channel`：外部输入输出方式（企业微信 webhook / 终端）
- `core`：客服策略与 LLM 编排
- `assets`：个性化资产（`system_prompt.txt` / `club_profile.json` / `faq.json`）

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
```

启动后直接在终端输入问题，输入 `exit` 退出。

适合：
- 快速验证 FAQ/提示词效果
- 不依赖企业微信联调

### B) 企业微信 Webhook 模式（线上接入）
配置：

```env
CHANNEL_DRIVER=wecom_webhook
```

并确保以下环境变量正确：
- `WECOM_WEBHOOK_TOKEN`（企业微信回调 URL 上配置的 Token，用于签名校验）
- `ENCODING_AES_KEY`
- `CORP_ID`
- `CORP_SECRET`
- `LLM_API_KEY`（或 `OPENAI_API_KEY` / `ALI_API_KEY` / `DASHSCOPE_API_KEY`）

启动后提供：
- `GET /wechat`：企业微信验签
- `POST /wechat`：消息回调入口

---

## 3. 资产目录用法（核心）

程序只从 `USER_WORK_DIR/assets` 读取定制化资产（扁平结构，不分场景子目录）：

```text
<USER_WORK_DIR>/assets/
├─ system_prompt.txt
├─ club_profile.json
└─ faq.json
```

你要切换到另一套业务配置时，只需要把 `USER_WORK_DIR` 指向另一份目录（或替换 `assets/` 下的这 3 个文件），然后重启服务即可。

---

## 4. 关键配置说明（.env）

- `CHANNEL_DRIVER`：渠道驱动  
  - `wecom_webhook`：企业微信
  - `terminal_cli`：终端交互
- `USER_WORK_DIR`：用户工作目录（默认 `app/user_workdir/`）
  - `assets/`：本地资产目录（直接放 3 个文件）
  - `data/`：运行时数据目录（SQLite 默认在这里）

---

## 5. 目录用途（简版）

```text
app/
├─ channels/   # 外部接入层
├─ core/       # 智能客服核心
├─ db/         # 数据访问层代码（不是数据库文件）
└─ user_workdir/
   ├─ assets/  # 本地资产目录（默认忽略，运行时读取）
   └─ data/    # 运行时数据目录（db 文件在这里）
```

---

## 6. 注意事项

- `.env` 必须放在项目根目录（与 `app/` 同级）
- 不要提交真实 `.env`（已在 `.gitignore`）
- `db/` 放的是 Python 代码；数据库文件应在 `data/` 或外部挂载目录

---

## 7. 管理后台（网页）使用说明

该项目内置一个简单的管理后台，用于在网页里编辑本地资产目录中的 3 份资产：
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

- 后台保存时会**写入本地资产目录**：`<USER_WORK_DIR>/assets/`（默认在 `app/user_workdir/assets/`，且已加入 `.gitignore`）
- 写入后会触发**热加载**，进程会立刻使用新配置（无需重启）

---

## 数据库查看脚本（简易）

脚本：`scripts/customer_db_visualize.py`

默认读取：
- 项目根目录 `.env`
- 数据库路径默认取 `<USER_WORK_DIR>/data/ai_customer_service.db`（也可用脚本参数 `--db` 显式指定）

常用用法：
- 用户列表：`python scripts/customer_db_visualize.py --limit 20`
- 查看用户对话：`python scripts/customer_db_visualize.py --user "external_userid" --open-kfid "open_kfid"`
- 昵称模糊检索：`python scripts/customer_db_visualize.py --nickname "张"`
- 查看数据库结构：`python scripts/customer_db_visualize.py --schema`

