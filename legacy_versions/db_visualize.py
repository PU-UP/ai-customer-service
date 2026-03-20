#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Iterable, Sequence


def load_dotenv(dotenv_path: str = ".env") -> None:
    """
    极简 .env 加载器：读取 KEY=VALUE 行写入 os.environ（不覆盖已有环境变量）
    与 wechat_bot.py 保持一致，避免引入第三方依赖。
    """
    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


def _now_ts() -> int:
    return int(time.time())


def _fmt_ts(ts: Any) -> str:
    try:
        if ts is None:
            return "-"
        n = int(ts)
        if n <= 0:
            return str(ts)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(n))
    except Exception:
        return str(ts)


def _term_width(default: int = 120) -> int:
    try:
        return int(os.get_terminal_size().columns)
    except Exception:
        return default


def _clip(s: Any, max_len: int) -> str:
    t = "" if s is None else str(s)
    t = t.replace("\r", " ").replace("\n", " ").strip()
    if max_len <= 0:
        return ""
    if len(t) <= max_len:
        return t
    return t[: max(1, max_len - 1)] + "…"


def _to_text(v: Any, col: str | None = None) -> str:
    if col in {"created_at", "processed_at", "create_time", "updated_at"}:
        return _fmt_ts(v)
    if v is None:
        return "-"
    if isinstance(v, (bytes, bytearray)):
        return f"<bytes {len(v)}>"
    return str(v)


def _rule(char: str = "─") -> str:
    return char * max(10, _term_width())


def _print_kv(title: str, kv: dict[str, Any]) -> None:
    parts: list[str] = []
    for k, v in kv.items():
        parts.append(f"{k}={v}")
    print(f"[{title}] " + " ".join(parts))


def _print_title(title: str) -> None:
    print(_rule("═"))
    print(f"【{title}】")
    print(_rule("═"))


@dataclass(frozen=True)
class Table:
    name: str
    sql: str | None


def list_tables(conn: sqlite3.Connection) -> list[Table]:
    rows = conn.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name ASC
        """
    ).fetchall()
    out: list[Table] = []
    for r in rows:
        out.append(Table(name=str(r[0]), sql=str(r[1]) if r[1] is not None else None))
    return out


def list_indexes(conn: sqlite3.Connection, table: str) -> list[tuple[str, str | None]]:
    rows = conn.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type='index' AND tbl_name=?
        ORDER BY name ASC
        """,
        (table,),
    ).fetchall()
    return [(str(r[0]), str(r[1]) if r[1] is not None else None) for r in rows]


def table_row_count(conn: sqlite3.Connection, table: str) -> int | None:
    try:
        row = conn.execute(f'SELECT COUNT(1) AS c FROM "{table}"').fetchone()
        if not row:
            return 0
        return int(row[0])
    except Exception:
        return None


def table_columns(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    cols: list[dict[str, Any]] = []
    # pragma table_info: cid, name, type, notnull, dflt_value, pk
    for r in rows:
        cols.append(
            {
                "cid": r[0],
                "name": r[1],
                "type": r[2],
                "notnull": r[3],
                "default": r[4],
                "pk": r[5],
            }
        )
    return cols


def query_recent_rows(conn: sqlite3.Connection, table: str, limit: int) -> tuple[list[str], list[sqlite3.Row]]:
    # 针对本项目已知表做更“合理”的倒序；否则用 ROWID 兜底
    if table == "conversation_messages":
        sql = f'SELECT * FROM "{table}" ORDER BY created_at DESC, id DESC LIMIT ?'
        cols = [c["name"] for c in table_columns(conn, table)]
        rows = conn.execute(sql, (limit,)).fetchall()
        return cols, rows
    if table == "llm_usage_logs":
        sql = f'SELECT * FROM "{table}" ORDER BY created_at DESC, id DESC LIMIT ?'
        cols = [c["name"] for c in table_columns(conn, table)]
        rows = conn.execute(sql, (limit,)).fetchall()
        return cols, rows
    if table == "processed_messages":
        sql = f'SELECT * FROM "{table}" ORDER BY processed_at DESC LIMIT ?'
        cols = [c["name"] for c in table_columns(conn, table)]
        rows = conn.execute(sql, (limit,)).fetchall()
        return cols, rows
    if table == "kf_sync_cursors":
        sql = f'SELECT * FROM "{table}" ORDER BY updated_at DESC LIMIT ?'
        cols = [c["name"] for c in table_columns(conn, table)]
        rows = conn.execute(sql, (limit,)).fetchall()
        return cols, rows

    cols = [c["name"] for c in table_columns(conn, table)]
    # SQLite ROWID 可能不存在（WITHOUT ROWID），但我们这里兜底失败时改用 LIMIT
    try:
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid DESC LIMIT ?', (limit,)).fetchall()
        return cols, rows
    except Exception:
        rows = conn.execute(f'SELECT * FROM "{table}" LIMIT ?', (limit,)).fetchall()
        return cols, rows


def render_table(headers: Sequence[str], rows: Iterable[Sequence[Any]], *, max_cell: int) -> str:
    rows_list = [list(r) for r in rows]
    headers_list = list(headers)
    ncol = len(headers_list)
    # 计算每列宽度：标题/内容取 max，并做上限
    widths = [len(h) for h in headers_list]
    for r in rows_list:
        for i in range(ncol):
            v = r[i] if i < len(r) else ""
            widths[i] = min(max(widths[i], len(str(v))), max_cell)

    # 适配终端宽度：避免横向溢出（过窄时仍尽量展示）
    # 行宽近似：sum(widths) + 3*ncol + 1
    term_w = _term_width()
    min_w = 6
    if ncol > 0:
        current = sum(widths) + 3 * ncol + 1
        if current > term_w:
            over = current - term_w
            # 逐步从最宽的列开始收缩，直到不溢出或达到最小宽度
            while over > 0:
                # 找到可收缩的最宽列
                idx = -1
                best = -1
                for i, w in enumerate(widths):
                    if w > min_w and w > best:
                        best = w
                        idx = i
                if idx < 0:
                    break
                widths[idx] -= 1
                over -= 1

    def fmt_row(items: Sequence[Any]) -> str:
        cells: list[str] = []
        for i in range(ncol):
            raw = items[i] if i < len(items) else ""
            t = _clip(raw, widths[i])
            pad = " " * max(0, widths[i] - len(t))
            cells.append(t + pad)
        return "│ " + " │ ".join(cells) + " │"

    top = "┌" + "┬".join("─" * (w + 2) for w in widths) + "┐"
    mid = "├" + "┼".join("─" * (w + 2) for w in widths) + "┤"
    bot = "└" + "┴".join("─" * (w + 2) for w in widths) + "┘"
    out_lines = [top, fmt_row(headers_list), mid]
    for r in rows_list:
        out_lines.append(fmt_row(r))
    out_lines.append(bot)
    return "\n".join(out_lines)


def _parse_args(argv: list[str]) -> dict[str, Any]:
    args = {
        "db": None,
        "limit": 10,
        "show_sql": False,
        "only": None,
        "max_cell": 64,
        "schema": False,
        "user": None,
        "open_kfid": None,
        "nickname": None,
    }
    it = iter(argv[1:])
    for a in it:
        if a in {"-h", "--help"}:
            raise SystemExit(0)
        if a in {"--db"}:
            args["db"] = next(it, None)
            continue
        if a in {"--limit"}:
            v = next(it, None)
            args["limit"] = int(v) if v is not None else args["limit"]
            continue
        if a in {"--user"}:
            args["user"] = next(it, None)
            continue
        if a in {"--open-kfid"}:
            args["open_kfid"] = next(it, None)
            continue
        if a in {"--nickname"}:
            args["nickname"] = next(it, None)
            continue
        if a in {"--schema"}:
            args["schema"] = True
            continue
        if a in {"--only"}:
            args["only"] = next(it, None)
            continue
        if a in {"--show-sql"}:
            args["show_sql"] = True
            continue
        if a in {"--max-cell"}:
            v = next(it, None)
            args["max_cell"] = int(v) if v is not None else args["max_cell"]
            continue
    return args


def _print_help() -> None:
    print(
        "\n".join(
            [
                "用法：",
                "  - 用户列表（默认）：python3 db_visualize.py [--db PATH] [--limit N]",
                "  - 指定用户对话：  python3 db_visualize.py --user EXTERNAL_USERID [--open-kfid OKFID] [--db PATH]",
                "  - 昵称检索用户：  python3 db_visualize.py --nickname 关键字 [--open-kfid OKFID] [--db PATH]",
                "  - 查看数据库结构： python3 db_visualize.py --schema [--db PATH] [--limit N] [--only TABLE] [--show-sql]",
                "",
                "说明：",
                "  - 默认读取项目根目录 .env（不覆盖已有环境变量），并使用 SQLITE_PATH 或 ./ai_customer_service.db",
                "  - 默认输出：所有聊过的用户列表（nickname + 最近对话时间）",
                "  - --user 输出：指定用户完整对话记录（带时间戳）",
                "",
                "参数：",
                "  --db PATH        指定 sqlite 文件路径",
                "  --limit N        用户列表最多展示 N 行；或 --schema 时每张表展示最近 N 行（默认 10）",
                "  --user ID        指定 external_userid，输出该用户完整对话",
                "  --open-kfid ID   指定 open_kfid（同一 external_userid 可能出现在多个客服账号下）",
                "  --nickname TXT   按昵称模糊检索聊过的用户（可配合 --open-kfid）",
                "  --schema         输出 DB 结构/表/索引/样例（原模式）",
                "  --only TABLE     仅展示某一张表",
                "  --show-sql       展示建表 SQL / 索引 SQL（可能较长）",
                "  --max-cell N     单元格最大宽度（默认 64，越大越不截断）",
            ]
        )
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def list_chatted_users(conn: sqlite3.Connection, limit: int) -> tuple[list[str], list[list[Any]]]:
    """
    输出：open_kfid / external_userid / nickname / last_time
    nickname 从 customer_profiles 取；若无该表或无记录则用 '-'
    """
    if not _table_exists(conn, "conversation_messages"):
        return ["open_kfid", "external_userid", "nickname", "last_time"], []
    has_profiles = _table_exists(conn, "customer_profiles")
    if has_profiles:
        sql = """
        SELECT
          cm.open_kfid AS open_kfid,
          cm.external_userid AS external_userid,
          COALESCE(cp.nickname, '-') AS nickname,
          MAX(cm.created_at) AS last_time
        FROM conversation_messages cm
        LEFT JOIN customer_profiles cp
          ON cp.open_kfid = cm.open_kfid AND cp.external_userid = cm.external_userid
        GROUP BY cm.open_kfid, cm.external_userid
        ORDER BY last_time DESC
        LIMIT ?
        """
    else:
        sql = """
        SELECT
          cm.open_kfid AS open_kfid,
          cm.external_userid AS external_userid,
          '-' AS nickname,
          MAX(cm.created_at) AS last_time
        FROM conversation_messages cm
        GROUP BY cm.open_kfid, cm.external_userid
        ORDER BY last_time DESC
        LIMIT ?
        """
    rows = conn.execute(sql, (int(limit),)).fetchall()
    out: list[list[Any]] = []
    for r in rows:
        out.append([r["open_kfid"], r["external_userid"], r["nickname"], _fmt_ts(r["last_time"])])
    return ["open_kfid", "external_userid", "nickname", "last_time"], out


def search_users_by_nickname(
    conn: sqlite3.Connection,
    nickname_keyword: str,
    *,
    open_kfid: str | None = None,
    limit: int = 100,
) -> tuple[list[str], list[list[Any]]]:
    """
    按昵称模糊检索用户：
    - 使用 customer_profiles.nickname LIKE
    - last_time 取 conversation_messages 里的最近时间
    """
    if not _table_exists(conn, "customer_profiles") or not _table_exists(conn, "conversation_messages"):
        return ["open_kfid", "external_userid", "nickname", "last_time"], []
    kw = f"%{(nickname_keyword or '').strip()}%"
    where_sql = "WHERE cp.nickname LIKE ?"
    params: list[Any] = [kw]
    if open_kfid:
        where_sql += " AND cp.open_kfid = ?"
        params.append(open_kfid)
    sql = f"""
    SELECT
      cp.open_kfid AS open_kfid,
      cp.external_userid AS external_userid,
      COALESCE(cp.nickname, '-') AS nickname,
      MAX(cm.created_at) AS last_time
    FROM customer_profiles cp
    LEFT JOIN conversation_messages cm
      ON cm.open_kfid = cp.open_kfid AND cm.external_userid = cp.external_userid
    {where_sql}
    GROUP BY cp.open_kfid, cp.external_userid, cp.nickname
    ORDER BY last_time DESC
    LIMIT ?
    """
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    out: list[list[Any]] = []
    for r in rows:
        out.append([r["open_kfid"], r["external_userid"], r["nickname"], _fmt_ts(r["last_time"])])
    return ["open_kfid", "external_userid", "nickname", "last_time"], out


def resolve_latest_open_kfid(conn: sqlite3.Connection, external_userid: str) -> str | None:
    if not _table_exists(conn, "conversation_messages"):
        return None
    row = conn.execute(
        """
        SELECT open_kfid, MAX(created_at) AS last_time
        FROM conversation_messages
        WHERE external_userid = ?
        GROUP BY open_kfid
        ORDER BY last_time DESC
        LIMIT 1
        """,
        (external_userid,),
    ).fetchone()
    if not row:
        return None
    return str(row["open_kfid"] or "") or None


def get_nickname(conn: sqlite3.Connection, open_kfid: str, external_userid: str) -> str:
    if not _table_exists(conn, "customer_profiles"):
        return "-"
    row = conn.execute(
        """
        SELECT nickname
        FROM customer_profiles
        WHERE open_kfid = ? AND external_userid = ?
        """,
        (open_kfid, external_userid),
    ).fetchone()
    if not row:
        return "-"
    t = "" if row["nickname"] is None else str(row["nickname"]).strip()
    return t or "-"


def show_user_conversation(conn: sqlite3.Connection, open_kfid: str, external_userid: str, *, max_cell: int) -> None:
    if not _table_exists(conn, "conversation_messages"):
        _print_title("用户对话记录")
        _print_kv("error", {"reason": "conversation_messages 表不存在"})
        return
    nick = get_nickname(conn, open_kfid, external_userid)
    _print_title("用户对话记录")
    _print_kv("user", {"open_kfid": open_kfid, "external_userid": external_userid, "nickname": nick})
    rows = conn.execute(
        """
        SELECT role, content, created_at
        FROM conversation_messages
        WHERE open_kfid = ? AND external_userid = ?
        ORDER BY created_at ASC, id ASC
        """,
        (open_kfid, external_userid),
    ).fetchall()
    print()
    if not rows:
        print("对话：<空>")
        return
    data_rows: list[list[Any]] = []
    for r in rows:
        data_rows.append([_fmt_ts(r["created_at"]), str(r["role"] or "-"), str(r["content"] or "")])
    print(render_table(["time", "role", "content"], data_rows, max_cell=max_cell))


def main() -> int:
    try:
        args = _parse_args(sys.argv)
    except SystemExit as e:
        _print_help()
        return int(getattr(e, "code", 0) or 0)

    # 与 wechat_bot.py 一致：从脚本所在目录加载 .env
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(base_dir, ".env"))

    db_path = args["db"] or (os.getenv("SQLITE_PATH", "").strip() or os.path.join(base_dir, "ai_customer_service.db"))
    limit = int(args["limit"] or 10)
    if limit <= 0:
        limit = 10
    show_sql = bool(args["show_sql"])
    only = args["only"]
    max_cell = int(args["max_cell"] or 64)
    schema_mode = bool(args.get("schema"))
    user_id = (args.get("user") or "").strip() if args.get("user") else ""
    open_kfid = (args.get("open_kfid") or "").strip() if args.get("open_kfid") else ""
    nickname_kw = (args.get("nickname") or "").strip() if args.get("nickname") else ""
    if max_cell <= 8:
        max_cell = 8

    if not os.path.exists(db_path):
        _print_title("SQLite 数据库可视化")
        _print_kv("error", {"db": db_path, "reason": "文件不存在"})
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # 默认模式：用户列表 / 指定用户对话
        if not schema_mode:
            if user_id:
                okfid = open_kfid or (resolve_latest_open_kfid(conn, user_id) or "")
                if not okfid:
                    _print_title("用户对话记录")
                    _print_kv("error", {"external_userid": user_id, "reason": "未找到该用户的对话"})
                    return 4
                show_user_conversation(conn, okfid, user_id, max_cell=max_cell)
                return 0

            if nickname_kw:
                _print_title("昵称检索结果")
                _print_kv("meta", {"db": db_path, "now": _fmt_ts(_now_ts()), "nickname": nickname_kw, "limit": limit, "open_kfid": open_kfid or "-"})
                headers, rows = search_users_by_nickname(conn, nickname_kw, open_kfid=open_kfid or None, limit=limit)
                print()
                if not rows:
                    print("结果：<空>")
                    return 0
                print(render_table(headers, rows, max_cell=max_cell))
                return 0

            _print_title("聊天用户列表")
            _print_kv("meta", {"db": db_path, "now": _fmt_ts(_now_ts()), "limit": limit})
            headers, rows = list_chatted_users(conn, limit=limit)
            print()
            if not rows:
                print("用户：<空>")
                return 0
            print(render_table(headers, rows, max_cell=max_cell))
            return 0

        # --schema：原模式（结构/表/索引/样例）
        tables = list_tables(conn)
        if only:
            tables = [t for t in tables if t.name == only]

        _print_title("SQLite 数据库可视化")
        _print_kv(
            "meta",
            {
                "db": db_path,
                "now": _fmt_ts(_now_ts()),
                "tables": len(tables),
                "limit": limit,
            },
        )

        # 总览：表 + 行数
        overview_rows: list[list[Any]] = []
        for t in tables:
            overview_rows.append([t.name, table_row_count(conn, t.name)])
        print()
        print("【表总览】")
        print(render_table(["table", "rows"], overview_rows, max_cell=max_cell))

        # 每张表详情
        for t in tables:
            print()
            print(_rule("─"))
            print(f"【表】{t.name}")
            print(_rule("─"))

            cols = table_columns(conn, t.name)
            col_rows: list[list[Any]] = []
            for c in cols:
                col_rows.append(
                    [
                        c["cid"],
                        c["name"],
                        c["type"],
                        "Y" if int(c["notnull"] or 0) else "",
                        "Y" if int(c["pk"] or 0) else "",
                        _clip(c["default"], 32),
                    ]
                )
            print("字段：")
            print(render_table(["cid", "name", "type", "notnull", "pk", "default"], col_rows, max_cell=max_cell))

            idx = list_indexes(conn, t.name)
            if idx:
                idx_rows = [[name, _clip(sql, 120) if sql else "-"] for (name, sql) in idx]
                print()
                print("索引：")
                print(render_table(["name", "sql"], idx_rows, max_cell=max_cell))

            if show_sql and t.sql:
                print()
                print("建表 SQL：")
                print(t.sql.strip())

            # 样例行
            print()
            headers, rows = query_recent_rows(conn, t.name, limit=limit)
            if not rows:
                print("最近记录：<空>")
                continue
            data_rows: list[list[Any]] = []
            for r in rows:
                one: list[Any] = []
                for h in headers:
                    one.append(_to_text(r[h], h))
                data_rows.append(one)
            print(f"最近记录（{min(limit, len(rows))} 条）：")
            print(render_table(headers, data_rows, max_cell=max_cell))

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

