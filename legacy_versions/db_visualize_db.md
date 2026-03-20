# db_visualize.py 使用说明

本文档说明 `db_visualize.py` 的常用用法，包含：
- 默认查看聊过的用户列表（含昵称和最近对话时间）
- 指定用户查看完整对话记录（含每条消息时间）
- 按昵称模糊检索用户
- 查看数据库结构（schema 模式）

## 1. 脚本默认行为

直接运行：

```bash
python3 db_visualize.py
```

默认输出“聊天用户列表”，字段为：
- `open_kfid`
- `external_userid`
- `nickname`
- `last_time`（最近对话时间）

可配合 `--limit` 控制展示条数：

```bash
python3 db_visualize.py --limit 50
```

## 2. 查看某个用户的完整对话

按 `external_userid` 查询：

```bash
python3 db_visualize.py --user "wmxxxxxxxxxxxxxxxxxxxxxx"
```

输出内容包含：
- 用户信息：`open_kfid / external_userid / nickname`
- 对话表格：`time / role / content`

若同一个 `external_userid` 在多个客服账号下都有记录，可指定 `--open-kfid`：

```bash
python3 db_visualize.py --user "wmxxxxxxxxxxxxxxxxxxxxxx" --open-kfid "wkd4yMXwAAlqedSUzQAtxVtmMvamHBtQ"
```

## 3. 按昵称检索用户（新增）

支持模糊匹配昵称：

```bash
python3 db_visualize.py --nickname "张"
```

也可限定某个客服账号：

```bash
python3 db_visualize.py --nickname "张" --open-kfid "wkd4yMXwAAlqedSUzQAtxVtmMvamHBtQ"
```

返回结果字段：
- `open_kfid`
- `external_userid`
- `nickname`
- `last_time`

## 4. 查看数据库结构（schema 模式）

```bash
python3 db_visualize.py --schema
```

会输出：
- 表总览（表名 + 行数）
- 每张表字段
- 索引信息
- 最近记录样例

常见附加参数：

```bash
# 仅看某一张表
python3 db_visualize.py --schema --only customer_profiles

# 每张表显示更多样例
python3 db_visualize.py --schema --limit 30

# 展示建表 SQL
python3 db_visualize.py --schema --show-sql
```

## 5. 参数总览

- `--db PATH`：指定 sqlite 文件路径
- `--limit N`：列表/检索结果最大条数；schema 模式下为每表样例条数
- `--user ID`：指定 `external_userid`，查看完整对话
- `--open-kfid ID`：指定客服账号 ID（配合 `--user` 或 `--nickname`）
- `--nickname TXT`：按昵称模糊检索用户
- `--schema`：切换到数据库结构查看模式
- `--only TABLE`：schema 模式下仅查看某张表
- `--show-sql`：schema 模式下显示建表/索引 SQL
- `--max-cell N`：表格单元格最大宽度
- `-h` / `--help`：显示帮助

## 6. 数据来源说明

- 用户昵称等资料来自 `customer_profiles` 表（由 `wechat_bot.py` 调用企业微信接口后写入）。
- 对话记录来自 `conversation_messages` 表。
- 如果 `customer_profiles` 里暂无某用户资料，昵称会显示为 `-`。
