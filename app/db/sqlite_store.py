from __future__ import annotations

import json
import os
import sqlite3
import threading
import traceback
from typing import Any

from app.config import CONV_HISTORY_MAX_CHARS, CUSTOMER_PROFILE_REFRESH_SECONDS
from app.core.logging import log
from app.core.utils import mask_id, now_ts
from app.channels.wecom import kf_customer_batchget

_DB_LOCK = threading.Lock()
_DB_CONN: sqlite3.Connection | None = None


def init_db(db_path: str) -> None:
    """
    Initialize sqlite database (create tables + keep backward compatible migrations).
    """

    global _DB_CONN
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with _DB_LOCK:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # ---- 迁移（兼容旧库）----
        def _table_exists(name: str) -> bool:
            row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
            return row is not None

        def _get_columns(name: str) -> set[str]:
            cols: set[str] = set()
            try:
                rows = conn.execute(f"PRAGMA table_info({name})").fetchall()
                for r in rows:
                    cols.add(str(r[1]))
            except Exception:
                return set()
            return cols

        # processed_messages：从 (msgid PK) 迁移到 (open_kfid, msgid) 复合主键
        if _table_exists("processed_messages"):
            cols = _get_columns("processed_messages")
            if "open_kfid" not in cols:
                log("db.migrate.begin", {"table": "processed_messages"})
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_messages_v2 (
                        open_kfid TEXT NOT NULL,
                        msgid TEXT NOT NULL,
                        external_userid TEXT NOT NULL,
                        create_time INTEGER,
                        processed_at INTEGER NOT NULL,
                        PRIMARY KEY (open_kfid, msgid)
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_messages_v2(open_kfid, msgid, external_userid, create_time, processed_at)
                    SELECT '', msgid, external_userid, create_time, processed_at
                    FROM processed_messages
                    """
                )
                conn.execute("DROP TABLE processed_messages;")
                conn.execute("ALTER TABLE processed_messages_v2 RENAME TO processed_messages;")
                log("db.migrate.done", {"table": "processed_messages"})

        # conversation_messages：增加 open_kfid 字段，按 (open_kfid, external_userid) 隔离会话
        if _table_exists("conversation_messages"):
            cols = _get_columns("conversation_messages")
            if "open_kfid" not in cols:
                log("db.migrate.begin", {"table": "conversation_messages"})
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS conversation_messages_v2 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        open_kfid TEXT NOT NULL,
                        external_userid TEXT NOT NULL,
                        role TEXT NOT NULL,
                        content TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO conversation_messages_v2(open_kfid, external_userid, role, content, created_at)
                    SELECT '', external_userid, role, content, created_at
                    FROM conversation_messages
                    ORDER BY id ASC
                    """
                )
                conn.execute("DROP TABLE conversation_messages;")
                conn.execute("ALTER TABLE conversation_messages_v2 RENAME TO conversation_messages;")
                log("db.migrate.done", {"table": "conversation_messages"})

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                open_kfid TEXT NOT NULL,
                msgid TEXT NOT NULL,
                external_userid TEXT NOT NULL,
                create_time INTEGER,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (open_kfid, msgid)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                open_kfid TEXT NOT NULL,
                external_userid TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS customer_profiles (
                open_kfid TEXT NOT NULL,
                external_userid TEXT NOT NULL,
                nickname TEXT,
                avatar TEXT,
                gender INTEGER,
                unionid TEXT,
                enter_session_context TEXT,
                updated_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                PRIMARY KEY (open_kfid, external_userid)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_userid TEXT,
                model TEXT,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                total_tokens INTEGER,
                created_at INTEGER NOT NULL,
                msgid TEXT,
                user_message_preview TEXT,
                answer_preview TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kf_sync_cursors (
                open_kfid TEXT PRIMARY KEY,
                cursor TEXT,
                updated_at INTEGER NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_user_time ON conversation_messages(open_kfid, external_userid, created_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_usage_created_at ON llm_usage_logs(created_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_customer_profiles_last_seen ON customer_profiles(open_kfid, last_seen_at);")
        conn.commit()

    _DB_CONN = conn
    log("db.init_ok", {"path": db_path})


def _db() -> sqlite3.Connection:
    if _DB_CONN is None:
        raise RuntimeError("DB 未初始化")
    return _DB_CONN


def get_kf_cursor(open_kfid: str) -> str | None:
    if not open_kfid:
        return None
    try:
        with _DB_LOCK:
            row = _db().execute("SELECT cursor FROM kf_sync_cursors WHERE open_kfid = ?", (open_kfid,)).fetchone()
        if not row:
            return None
        cursor = row["cursor"]
        return str(cursor) if cursor is not None else None
    except Exception:
        log("db.kf_cursor.read_failed", {"open_kfid": mask_id(open_kfid)})
        log("db.kf_cursor.read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return None


def set_kf_cursor(open_kfid: str, cursor: str) -> None:
    if not open_kfid:
        return
    try:
        now = now_ts()
        with _DB_LOCK:
            _db().execute(
                """
                INSERT INTO kf_sync_cursors(open_kfid, cursor, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(open_kfid) DO UPDATE SET
                    cursor = excluded.cursor,
                    updated_at = excluded.updated_at
                """,
                (open_kfid, cursor, now),
            )
            _db().commit()
    except Exception:
        log("db.kf_cursor.write_failed", {"open_kfid": mask_id(open_kfid)})
        log("db.kf_cursor.write_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


def is_msg_processed(open_kfid: str, msgid: str) -> bool:
    if not open_kfid or not msgid:
        return False
    try:
        with _DB_LOCK:
            row = _db().execute(
                "SELECT msgid FROM processed_messages WHERE open_kfid = ? AND msgid = ?",
                (open_kfid, msgid),
            ).fetchone()
        return row is not None
    except Exception:
        log("db.processed_messages.query_failed", {"open_kfid": mask_id(open_kfid), "msgid": msgid})
        log("db.processed_messages.query_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return False


def mark_msg_processed(open_kfid: str, msgid: str, external_userid: str, create_time: int | None) -> bool:
    """
    Mark message as processed (idempotent).
    Returns True if this insert happened (not previously processed).
    """

    if not open_kfid or not msgid:
        return False
    try:
        with _DB_LOCK:
            cur = _db().execute(
                """
                INSERT OR IGNORE INTO processed_messages(open_kfid, msgid, external_userid, create_time, processed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (open_kfid, msgid, external_userid or "", int(create_time or 0) or None, now_ts()),
            )
            _db().commit()
            return cur.rowcount == 1
    except Exception:
        log(
            "db.processed_messages.insert_failed",
            {"open_kfid": mask_id(open_kfid), "msgid": msgid, "external_userid": mask_id(external_userid)},
        )
        log("db.processed_messages.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return False


def append_conversation_message(open_kfid: str, external_userid: str, role: str, content: str) -> None:
    if not open_kfid or not external_userid or not role or not content:
        return
    try:
        with _DB_LOCK:
            _db().execute(
                "INSERT INTO conversation_messages(open_kfid, external_userid, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (open_kfid, external_userid, role, content, now_ts()),
            )
            _db().commit()
    except Exception:
        log(
            "db.conversation.insert_failed",
            {"open_kfid": mask_id(open_kfid), "external_userid": mask_id(external_userid), "role": role},
        )
        log("db.conversation.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


def upsert_customer_profile(
    open_kfid: str,
    external_userid: str,
    *,
    nickname: str | None,
    avatar: str | None,
    gender: int | None,
    unionid: str | None,
    enter_session_context: dict | None,
    last_seen_at: int | None = None,
) -> None:
    if not open_kfid or not external_userid:
        return
    now = now_ts()
    if last_seen_at is None or int(last_seen_at or 0) <= 0:
        last_seen_at = now
    esc_text: str | None = None
    try:
        if isinstance(enter_session_context, dict):
            esc_text = json.dumps(enter_session_context, ensure_ascii=False)
    except Exception:
        esc_text = None

    try:
        with _DB_LOCK:
            _db().execute(
                """
                INSERT INTO customer_profiles(
                    open_kfid, external_userid,
                    nickname, avatar, gender, unionid, enter_session_context,
                    updated_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(open_kfid, external_userid) DO UPDATE SET
                    nickname = COALESCE(excluded.nickname, customer_profiles.nickname),
                    avatar = COALESCE(excluded.avatar, customer_profiles.avatar),
                    gender = COALESCE(excluded.gender, customer_profiles.gender),
                    unionid = COALESCE(excluded.unionid, customer_profiles.unionid),
                    enter_session_context = COALESCE(excluded.enter_session_context, customer_profiles.enter_session_context),
                    updated_at = excluded.updated_at,
                    last_seen_at = CASE
                        WHEN excluded.last_seen_at > customer_profiles.last_seen_at THEN excluded.last_seen_at
                        ELSE customer_profiles.last_seen_at
                    END
                """,
                (
                    open_kfid,
                    external_userid,
                    (nickname or "").strip() or None,
                    (avatar or "").strip() or None,
                    int(gender) if gender is not None else None,
                    (unionid or "").strip() or None,
                    esc_text,
                    now,
                    int(last_seen_at or 0),
                ),
            )
            _db().commit()
    except Exception:
        log(
            "db.customer_profiles.upsert_failed",
            {"open_kfid": mask_id(open_kfid), "external_userid": mask_id(external_userid)},
        )
        log("db.customer_profiles.upsert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


def get_customer_profile(open_kfid: str, external_userid: str) -> dict[str, Any] | None:
    if not open_kfid or not external_userid:
        return None
    try:
        with _DB_LOCK:
            row = _db().execute(
                """
                SELECT open_kfid, external_userid, nickname, avatar, gender, unionid, enter_session_context, updated_at, last_seen_at
                FROM customer_profiles
                WHERE open_kfid = ? AND external_userid = ?
                """,
                (open_kfid, external_userid),
            ).fetchone()
        if not row:
            return None
        return dict(row)
    except Exception:
        log(
            "db.customer_profiles.read_failed",
            {"open_kfid": mask_id(open_kfid), "external_userid": mask_id(external_userid)},
            debug_only=True,
        )
        return None


def ensure_customer_profile(open_kfid: str, external_userid: str, *, last_seen_at: int | None = None) -> dict[str, Any] | None:
    """
    Ensure customer profile exists (at least nickname).
    """

    if not open_kfid or not external_userid:
        return None
    now = now_ts()
    prof = get_customer_profile(open_kfid, external_userid)

    need_refresh = True
    if isinstance(prof, dict):
        try:
            updated_at = int(prof.get("updated_at") or 0)
        except Exception:
            updated_at = 0
        nickname = (prof.get("nickname") or "").strip() if isinstance(prof.get("nickname"), str) else ""
        if nickname and updated_at > 0 and now - updated_at < CUSTOMER_PROFILE_REFRESH_SECONDS:
            need_refresh = False

    if not need_refresh:
        upsert_customer_profile(
            open_kfid,
            external_userid,
            nickname=None,
            avatar=None,
            gender=None,
            unionid=None,
            enter_session_context=None,
            last_seen_at=last_seen_at,
        )
        return get_customer_profile(open_kfid, external_userid)

    resp = kf_customer_batchget([external_userid], need_enter_session_context=0)
    if str(resp.get("errcode")) != "0":
        upsert_customer_profile(
            open_kfid,
            external_userid,
            nickname=None,
            avatar=None,
            gender=None,
            unionid=None,
            enter_session_context=None,
            last_seen_at=last_seen_at,
        )
        return get_customer_profile(open_kfid, external_userid)

    customers = resp.get("customer_list") or []
    if isinstance(customers, list) and customers and isinstance(customers[0], dict):
        c0 = customers[0]
        nick = (c0.get("nickname") or "").strip() if isinstance(c0.get("nickname"), str) else ""
        if nick:
            log("customer.profile.nickname", {"open_kfid": open_kfid, "external_userid": mask_id(external_userid), "nickname": nick})
        upsert_customer_profile(
            open_kfid,
            external_userid,
            nickname=c0.get("nickname"),
            avatar=c0.get("avatar"),
            gender=int(c0.get("gender")) if c0.get("gender") is not None else None,
            unionid=c0.get("unionid"),
            enter_session_context=c0.get("enter_session_context") if isinstance(c0.get("enter_session_context"), dict) else None,
            last_seen_at=last_seen_at,
        )
    else:
        upsert_customer_profile(
            open_kfid,
            external_userid,
            nickname=None,
            avatar=None,
            gender=None,
            unionid=None,
            enter_session_context=None,
            last_seen_at=last_seen_at,
        )
    return get_customer_profile(open_kfid, external_userid)


def get_recent_conversation_messages(open_kfid: str, external_userid: str, limit: int) -> list[dict[str, str]]:
    """
    Only return model-needed fields: role/content.
    Apply both:
    - recent message count (limit)
    - total characters capped by CONV_HISTORY_MAX_CHARS
    """

    if not open_kfid or not external_userid:
        return []
    try:
        with _DB_LOCK:
            rows = _db().execute(
                """
                SELECT role, content
                FROM conversation_messages
                WHERE open_kfid = ? AND external_userid = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (open_kfid, external_userid, int(limit) * 3),
            ).fetchall()

        selected_rev: list[dict[str, str]] = []
        total_chars = 0
        for r in rows:
            role = str(r["role"])
            content = str(r["content"])
            content_len = len(content)
            if selected_rev and len(selected_rev) >= limit:
                break
            if total_chars + content_len > CONV_HISTORY_MAX_CHARS:
                break
            selected_rev.append({"role": role, "content": content})
            total_chars += content_len

        selected = list(reversed(selected_rev))
        log(
            "conversation.history_built",
            {
                "open_kfid": mask_id(open_kfid),
                "external_userid": mask_id(external_userid),
                "limit": limit,
                "actual_len": len(selected),
                "approx_total_chars": total_chars,
            },
            debug_only=True,
        )
        return selected
    except Exception:
        log("db.conversation.read_failed", {"open_kfid": mask_id(open_kfid), "external_userid": mask_id(external_userid)})
        log("db.conversation.read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return []


def append_llm_usage_log(
    external_userid: str,
    model: str,
    usage: dict[str, int] | None,
    msgid: str | None,
    user_message_preview: str,
    answer_preview: str,
) -> None:
    if usage is None:
        usage = {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or 0)
    log(
        "token.usage",
        {
            "external_userid": mask_id(external_userid),
            "msgid": msgid or "",
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        },
    )
    try:
        with _DB_LOCK:
            _db().execute(
                """
                INSERT INTO llm_usage_logs (
                    external_userid, model,
                    prompt_tokens, completion_tokens, total_tokens,
                    created_at, msgid, user_message_preview, answer_preview
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    external_userid or "",
                    model or "",
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    now_ts(),
                    msgid or "",
                    user_message_preview,
                    answer_preview,
                ),
            )
            _db().commit()
    except Exception:
        log("db.llm_usage.insert_failed", {"external_userid": mask_id(external_userid)})
        log("db.llm_usage.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


def cleanup_old_data() -> None:
    """
    Light cleanup:
    - processed_messages keep recent 30 days
    - conversation_messages per external_userid keep recent 30 rows
    - llm_usage_logs keep recent 90 days
    """

    now = now_ts()
    cutoff_processed = now - 30 * 24 * 60 * 60
    cutoff_llm = now - 90 * 24 * 60 * 60

    try:
        with _DB_LOCK:
            _db().execute("DELETE FROM processed_messages WHERE processed_at < ?", (cutoff_processed,))
            _db().execute("DELETE FROM llm_usage_logs WHERE created_at < ?", (cutoff_llm,))

            users = _db().execute("SELECT DISTINCT open_kfid, external_userid FROM conversation_messages").fetchall()
            for row in users:
                okfid = str(row["open_kfid"])
                uid = str(row["external_userid"])
                old_rows = _db().execute(
                    """
                    SELECT id FROM conversation_messages
                    WHERE open_kfid = ? AND external_userid = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT -1 OFFSET 30
                    """,
                    (okfid, uid),
                ).fetchall()
                if not old_rows:
                    continue
                ids = [(int(r["id"]),) for r in old_rows]
                _db().executemany("DELETE FROM conversation_messages WHERE id = ?", ids)
            _db().commit()
    except Exception:
        log("db.cleanup_failed", {})
        log("db.cleanup_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


_CLEANUP_MIN_INTERVAL_SECONDS = 30 * 60  # at most every 30 minutes
_LAST_CLEANUP_TS = 0
_CLEANUP_LOCK = threading.Lock()


def maybe_cleanup_old_data() -> None:
    """
    Trigger cleanup with interval guard.
    """

    global _LAST_CLEANUP_TS
    now = now_ts()
    if now - _LAST_CLEANUP_TS < _CLEANUP_MIN_INTERVAL_SECONDS:
        return

    with _CLEANUP_LOCK:
        now2 = now_ts()
        if now2 - _LAST_CLEANUP_TS < _CLEANUP_MIN_INTERVAL_SECONDS:
            return
        _LAST_CLEANUP_TS = now2

    log("db.cleanup_maybe_run", {"ts": _LAST_CLEANUP_TS}, debug_only=True)
    cleanup_old_data()

