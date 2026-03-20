import base64
import hashlib
import hmac
import json
import os
import queue
import sqlite3
import struct
import threading
import time
import traceback
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

from flask import Flask, request
from Crypto.Cipher import AES

try:
    # OpenAI 兼容 SDK（用于阿里云百炼兼容模式）
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


app = Flask(__name__)


def load_dotenv(dotenv_path: str = ".env") -> None:
    """
    极简 .env 加载器：读取 KEY=VALUE 行写入 os.environ（不覆盖已有环境变量）
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


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


TOKEN = os.getenv("TOKEN", "")
ENCODING_AES_KEY = os.getenv("ENCODING_AES_KEY", "")
CORP_ID = os.getenv("CORP_ID", "")
CORP_SECRET = os.getenv("CORP_SECRET", "")

ALI_API_KEY = os.getenv("ALI_API_KEY", "") or os.getenv("DASHSCOPE_API_KEY", "")
ALI_BASE_URL = os.getenv("ALI_BASE_URL", "").strip() or "https://dashscope.aliyuncs.com/compatible-mode/v1"
ALI_MODEL = os.getenv("ALI_MODEL", "").strip() or "qwen-plus"
SQLITE_PATH = os.getenv("SQLITE_PATH", "").strip() or os.path.join(os.path.dirname(__file__), "ai_customer_service.db")

DEBUG = str(os.getenv("DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
CLUB_PROFILE_PATH = os.getenv("CLUB_PROFILE_PATH", "").strip() or os.path.join(os.path.dirname(__file__), "club_profile.json")
FAQ_PATH = os.getenv("FAQ_PATH", "").strip() or os.path.join(os.path.dirname(__file__), "faq.json")
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "1") or "1")
if WORKER_COUNT <= 0:
    WORKER_COUNT = 1

LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "15") or "15")
CONV_HISTORY_LIMIT = int(os.getenv("CONV_HISTORY_LIMIT", "8") or "8")  # 最近 6~10 条，默认 8
CONV_HISTORY_MAX_CHARS = int(os.getenv("CONV_HISTORY_MAX_CHARS", "2000") or "2000")  # 会话总字数上限，避免 prompt 过长

FALLBACK_TEXT = "已收到你的消息，我先帮你记录，稍后由老师进一步为你确认。"
NON_TEXT_REPLY = "目前仅支持文字咨询，你可以直接发送想了解的课程、时间、地址或试听问题。"

SENSITIVE_FALLBACK_TEXT = "这个问题我先帮你记录，涉及退款/投诉/合同/发票/法律或费用细则等内容，建议由老师人工进一步确认后再回复你。"

SENSITIVE_KEYWORDS = [
    "退款",
    "投诉",
    "合同",
    "发票",
    "法律",
    "维权",
    "精准费用",
]

_CLUB_PROFILE: dict[str, Any] = {}
_FAQ_ITEMS: list[dict[str, Any]] = []


def _fmt_preview(s: str, limit: int = 120) -> str:
    t = (s or "").replace("\n", " ").strip()
    if len(t) <= limit:
        return t
    return t[:limit] + "…"


def _now_clock() -> str:
    # 形如 15:27:35，便于扫日志
    return time.strftime("%H:%M:%S", time.localtime())


_LOG_CTX = threading.local()


def _set_log_trace(trace_id: str | None) -> None:
    try:
        setattr(_LOG_CTX, "trace_id", trace_id or "")
    except Exception:
        pass


def _get_log_trace() -> str:
    try:
        return str(getattr(_LOG_CTX, "trace_id", "") or "")
    except Exception:
        return ""


def _log_prefix() -> str:
    trace = _get_log_trace()
    if trace:
        return f"[{_now_clock()}][{trace}]"
    return f"[{_now_clock()}]"


def _log_separator(title: str, kv: dict[str, Any] | None = None) -> None:
    """
    在每次 webhook / worker 任务 / 单条消息处理开始时打印清晰分隔块。
    """
    if kv is None:
        kv = {}
    # 80 列左右的视觉边界，避免日志糊成一团
    bar = "─" * 84
    parts: list[str] = []
    for k, v in kv.items():
        if v is None or v == "":
            continue
        parts.append(f"{k}={v}")
    suffix = ("  " + " ".join(parts)) if parts else ""
    print("")
    print(f"{_log_prefix()} {bar}")
    print(f"{_log_prefix()} {title}{suffix}")
    print(f"{_log_prefix()} {bar}")


def _print_kv(title: str, kv: dict[str, Any]) -> None:
    parts: list[str] = []
    for k, v in kv.items():
        parts.append(f"{k}={v}")
    print(f"{_log_prefix()} [{title}] " + " ".join(parts))


def _print_block(title: str, lines: list[str]) -> None:
    print(f"{_log_prefix()} [{title}]")
    for ln in lines:
        print(f"{_log_prefix()}   - {ln}")


def _log(event: str, data: dict | None = None, *, debug_only: bool = False) -> None:
    """
    统一日志入口：
    - DEBUG=1 时打印更详细内容
    - 非 DEBUG 模式仅打印精简摘要（debug_only=True 的日志不打印）
    """
    if debug_only and not DEBUG:
        return
    if data is None:
        data = {}
    # DEBUG：保留结构化字典，便于排查
    if DEBUG:
        try:
            base = {"ts": _now_ts(), "event": event}
            base.update(data)
            base["_clock"] = _now_clock()
            trace = _get_log_trace()
            if trace:
                base["_trace"] = trace
            print(base)
        except Exception:
            print(f"{_log_prefix()} [{event}]")
        return

    # 非 DEBUG：输出人类可读的关键信息
    try:
        if event == "llm.context":
            lines: list[str] = []
            lines.append(f"user={data.get('external_userid')} msgid={data.get('msgid')} model={data.get('model')}")
            history = data.get("history_messages") or []
            lines.append(f"history_len={len(history)}")
            for i, m in enumerate(history, 1):
                role = m.get("role")
                content = m.get("content")
                lines.append(f"history[{i}] {role}: {_fmt_preview(str(content), 140)}")
            lines.append(f"user: {_fmt_preview(str(data.get('user_text') or ''), 180)}")
            _print_block("LLM输入摘要", lines)
            return

        if event == "llm.result":
            lines = [
                f"user={data.get('external_userid')} msgid={data.get('msgid')} model={data.get('model')}",
                f"success={data.get('success')} tokens(prompt/comp/total)={data.get('prompt_tokens')}/{data.get('completion_tokens')}/{data.get('total_tokens')}",
                f"answer: {_fmt_preview(str(data.get('answer') or ''), 220)}",
            ]
            _print_block("LLM输出摘要", lines)
            return

        if event == "route.webhook":
            _print_kv("Webhook", {"hit_kf_event": data.get("hit_kf_event"), "content_len": data.get("content_len")})
            return

        if event == "msg.flow":
            _print_kv(
                "消息处理",
                {
                    "open_kfid": data.get("open_kfid"),
                    "msgid": data.get("msgid"),
                    "user": data.get("external_userid"),
                    "msgtype": data.get("msgtype"),
                    "dedup_new": data.get("dedup_new"),
                    "path": data.get("path"),
                },
            )
            return

        if event == "reply.final":
            _print_kv(
                "最终回复",
                {
                    "source": data.get("source"),
                    "msgid": data.get("msgid"),
                    "user": data.get("external_userid"),
                    "preview": _fmt_preview(str(data.get("preview") or ""), 160),
                },
            )
            return

        if event == "token.usage":
            _print_kv(
                "Token用量",
                {
                    "msgid": data.get("msgid"),
                    "user": data.get("external_userid"),
                    "model": data.get("model"),
                    "prompt": data.get("prompt_tokens"),
                    "completion": data.get("completion_tokens"),
                    "total": data.get("total_tokens"),
                },
            )
            return

        _print_kv(event, data)
    except Exception:
        print(f"{_log_prefix()} [{event}]")


def _read_json_file(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        _log("config.json_missing", {"path": path})
        return None
    except Exception:
        _log("config.json_read_failed", {"path": path, "err": "read/parse failed"})
        _log("config.json_read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return None


def load_club_profile(path: str) -> dict[str, Any]:
    obj = _read_json_file(path)
    if isinstance(obj, dict):
        return obj
    return {}


def load_faq(path: str) -> list[dict[str, Any]]:
    obj = _read_json_file(path)
    if isinstance(obj, list):
        out: list[dict[str, Any]] = []
        for it in obj:
            if not isinstance(it, dict):
                continue
            keywords = it.get("keywords")
            answer = (it.get("answer") or "").strip()
            if not isinstance(keywords, list) or not answer:
                continue
            out.append(it)
        return out
    return []


def load_prompt_template(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_system_prompt(profile: dict[str, Any]) -> str:
    template_path = os.path.join(os.path.dirname(__file__), "prompts", "system_prompt.txt")
    template = load_prompt_template(template_path)
    profile_json = json.dumps(profile, ensure_ascii=False, indent=2)
    return template.replace("{{club_profile}}", profile_json)


def _normalize_text(s: str) -> str:
    return (s or "").strip().lower()


def match_sensitive_keyword(user_text: str) -> str | None:
    t = _normalize_text(user_text)
    for kw in SENSITIVE_KEYWORDS:
        if kw and kw in t:
            return kw
    return None


def match_faq(user_text: str) -> dict[str, Any] | None:
    """
    简单关键词命中：
    - 任一关键词出现在用户文本中即命中
    - 若多条命中，优先“关键词更长”的条目（更具体），再按 FAQ 中出现顺序
    """
    t = _normalize_text(user_text)
    if not t:
        return None

    best: dict[str, Any] | None = None
    best_len = -1
    for item in _FAQ_ITEMS:
        keywords = item.get("keywords") or []
        if not isinstance(keywords, list):
            continue
        for kw in keywords:
            if not isinstance(kw, str):
                continue
            k = kw.strip().lower()
            if not k:
                continue
            if k in t:
                if len(k) > best_len:
                    best = item
                    best_len = len(k)
                break
    return best


_ACCESS_TOKEN_CACHE = {"access_token": "", "expires_at": 0}


def _mask_id(s: str, keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep * 2:
        return s
    return s[:keep] + "..." + s[-keep:]


def _now_ts() -> int:
    return int(time.time())


def http_get_json(url: str, timeout: int = 10) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    return json.loads(body) if body else {}


def http_post_json(url: str, payload: dict, timeout: int = 10) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    return json.loads(body) if body else {}


def get_access_token() -> str:
    """
    通过 corpid + corpsecret 获取 access_token（内存缓存）
    """
    now = _now_ts()
    if _ACCESS_TOKEN_CACHE["access_token"] and now < int(_ACCESS_TOKEN_CACHE["expires_at"]) - 60:
        return _ACCESS_TOKEN_CACHE["access_token"]

    if not CORP_SECRET:
        _log("wecom.gettoken.missing_secret", {"missing": "CORP_SECRET"})
        return ""

    url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={CORP_ID}&corpsecret={CORP_SECRET}"
    resp = http_get_json(url)
    _log("wecom.gettoken", {"errcode": resp.get("errcode"), "errmsg": resp.get("errmsg"), "expires_in": resp.get("expires_in")})
    access_token = resp.get("access_token", "")
    expires_in = int(resp.get("expires_in") or 0)
    if access_token and expires_in > 0:
        _ACCESS_TOKEN_CACHE["access_token"] = access_token
        _ACCESS_TOKEN_CACHE["expires_at"] = now + expires_in
    return access_token


def kf_sync_msg(open_kfid: str, token: str, cursor: str | None = None, limit: int = 100) -> dict:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/sync_msg?access_token={access_token}"
    payload: dict = {"open_kfid": open_kfid, "token": token, "limit": limit}
    if cursor:
        payload["cursor"] = cursor
    _log("wecom.kf_sync_msg.request", {"open_kfid": open_kfid, "cursor": cursor, "limit": limit}, debug_only=not DEBUG)
    _log("wecom.kf_sync_msg.request.detail", payload, debug_only=True)
    resp = http_post_json(url, payload, timeout=10)
    _log(
        "wecom.kf_sync_msg.response",
        {"errcode": resp.get("errcode"), "errmsg": resp.get("errmsg"), "has_more": resp.get("has_more"), "msg_list_len": len(resp.get("msg_list") or [])},
    )
    msg_list = resp.get("msg_list") or []
    _log("wecom.kf_sync_msg.response.cursor", {"next_cursor": resp.get("next_cursor")}, debug_only=not DEBUG)
    return resp


def kf_send_text(open_kfid: str, external_userid: str, content: str) -> dict:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/send_msg?access_token={access_token}"
    payload = {
        "touser": external_userid,
        "open_kfid": open_kfid,
        "msgtype": "text",
        "text": {"content": content},
    }
    _log("wecom.kf_send_msg.request", {"touser": _mask_id(external_userid), "open_kfid": open_kfid, "msgtype": "text", "content_preview": (content or "")[:200]})
    resp = http_post_json(url, payload, timeout=10)
    _log("wecom.kf_send_msg.response", {"errcode": resp.get("errcode"), "errmsg": resp.get("errmsg"), "msgid": resp.get("msgid")})
    return resp


class PKCS7Encoder:
    """企业微信消息体使用 PKCS7 补位"""

    @staticmethod
    def encode(plaintext: bytes) -> bytes:
        block_size = 32
        pad_len = block_size - (len(plaintext) % block_size)
        if pad_len == 0:
            pad_len = block_size
        return plaintext + bytes([pad_len]) * pad_len

    @staticmethod
    def decode(decrypted: bytes) -> bytes:
        pad = decrypted[-1]
        if pad < 1 or pad > 32:
            pad = 0
        return decrypted[:-pad]


def sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    """
    企业微信签名算法：
    token, timestamp, nonce, encrypt 四个参数排序后拼接，再 sha1
    """
    arr = [token, timestamp, nonce, encrypt]
    arr.sort()
    raw = "".join(arr)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def decrypt_wecom_message(encoding_aes_key: str, corp_id: str, encrypt_text: str) -> str:
    """
    解密企业微信消息 / echostr
    """
    aes_key = base64.b64decode(encoding_aes_key + "=")
    iv = aes_key[:16]

    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
    encrypted = base64.b64decode(encrypt_text)
    decrypted = cipher.decrypt(encrypted)
    decrypted = PKCS7Encoder.decode(decrypted)

    # 企业微信消息结构：
    # 16字节随机串 + 4字节消息长度 + 明文 + receiveid(corpid)
    content = decrypted[16:]
    xml_len = struct.unpack(">I", content[:4])[0]
    xml_content = content[4:4 + xml_len]
    from_corpid = content[4 + xml_len:].decode("utf-8")

    if from_corpid != corp_id:
        raise ValueError(f"CorpID 不匹配: {from_corpid} != {corp_id}")

    return xml_content.decode("utf-8")


def parse_wecom_plain_xml(plain_xml: str) -> dict:
    """
    解析解密后的企业微信 XML，为后续业务分发提供结构化数据
    """
    root = ET.fromstring(plain_xml)
    out = {}
    for child in list(root):
        tag = child.tag
        text = (child.text or "").strip()
        if text:
            out[tag] = text
    return out


def extract_encrypt_from_xml(xml_text: str) -> str:
    """
    从 XML 中提取 <Encrypt> 的内容。
    使用 XML 解析，避免因空格/换行/格式变化导致提取失败。
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        raise ValueError(f"XML 解析失败: {e}") from e

    encrypt_node = root.find("Encrypt")
    if encrypt_node is None or encrypt_node.text is None or not encrypt_node.text.strip():
        raise ValueError("XML 中未找到 Encrypt 字段或内容为空")

    return encrypt_node.text.strip()


# ----------------------------
# SQLite：去重 + 会话记忆
# ----------------------------

_DB_LOCK = threading.Lock()
_DB_CONN: sqlite3.Connection | None = None

# 客户资料刷新频率：避免每条消息都调用 batchget
_CUSTOMER_PROFILE_REFRESH_SECONDS = int(os.getenv("CUSTOMER_PROFILE_REFRESH_SECONDS", "86400") or "86400")  # 默认 24h


def init_db(db_path: str) -> None:
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
                    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
                    cols.add(str(r[1]))
            except Exception:
                return set()
            return cols

        # processed_messages：从 (msgid PK) 迁移到 (open_kfid, msgid) 复合主键
        if _table_exists("processed_messages"):
            cols = _get_columns("processed_messages")
            if "open_kfid" not in cols:
                _log("db.migrate.begin", {"table": "processed_messages"})
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
                # 旧数据没有 open_kfid 维度，迁移时写入空串，保持“原去重语义”
                conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_messages_v2(open_kfid, msgid, external_userid, create_time, processed_at)
                    SELECT '', msgid, external_userid, create_time, processed_at
                    FROM processed_messages
                    """
                )
                conn.execute("DROP TABLE processed_messages;")
                conn.execute("ALTER TABLE processed_messages_v2 RENAME TO processed_messages;")
                _log("db.migrate.done", {"table": "processed_messages"})

        # conversation_messages：增加 open_kfid 字段，按 (open_kfid, external_userid) 隔离会话
        if _table_exists("conversation_messages"):
            cols = _get_columns("conversation_messages")
            if "open_kfid" not in cols:
                _log("db.migrate.begin", {"table": "conversation_messages"})
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
                _log("db.migrate.done", {"table": "conversation_messages"})

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
    _log("db.init_ok", {"path": db_path})


def _db() -> sqlite3.Connection:
    if _DB_CONN is None:
        raise RuntimeError("DB 未初始化")
    return _DB_CONN


def get_kf_cursor(open_kfid: str) -> str | None:
    """
    从 SQLite 读取指定 open_kfid 的 cursor。
    如不存在或查询失败，返回 None。
    """
    if not open_kfid:
        return None
    try:
        with _DB_LOCK:
            row = _db().execute(
                "SELECT cursor FROM kf_sync_cursors WHERE open_kfid = ?",
                (open_kfid,),
            ).fetchone()
        if not row:
            return None
        cursor = row["cursor"]
        return str(cursor) if cursor is not None else None
    except Exception:
        _log("db.kf_cursor.read_failed", {"open_kfid": _mask_id(open_kfid)})
        _log("db.kf_cursor.read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return None


def set_kf_cursor(open_kfid: str, cursor: str) -> None:
    """
    将最新 cursor 持久化到 SQLite。
    使用 UPSERT 语义，避免重复插入。
    """
    if not open_kfid:
        return
    try:
        now = _now_ts()
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
        _log("db.kf_cursor.write_failed", {"open_kfid": _mask_id(open_kfid)})
        _log("db.kf_cursor.write_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


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
        _log("db.processed_messages.query_failed", {"open_kfid": _mask_id(open_kfid), "msgid": msgid})
        _log("db.processed_messages.query_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return False


def mark_msg_processed(open_kfid: str, msgid: str, external_userid: str, create_time: int | None) -> bool:
    """
    标记已处理。利用 PRIMARY KEY(msgid) 防止并发重复写入。
    返回 True 表示本次成功写入（即此前未处理）；False 表示已存在或写入失败。
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
                (open_kfid, msgid, external_userid or "", int(create_time or 0) or None, _now_ts()),
            )
            _db().commit()
            # rowcount == 1 表示本次真的插入成功；0 表示此前已存在（被 IGNORE）
            return cur.rowcount == 1
    except Exception:
        _log(
            "db.processed_messages.insert_failed",
            {"open_kfid": _mask_id(open_kfid), "msgid": msgid, "external_userid": _mask_id(external_userid)},
        )
        _log("db.processed_messages.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return False


def append_conversation_message(open_kfid: str, external_userid: str, role: str, content: str) -> None:
    if not open_kfid or not external_userid or not role or not content:
        return
    try:
        with _DB_LOCK:
            _db().execute(
                "INSERT INTO conversation_messages(open_kfid, external_userid, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (open_kfid, external_userid, role, content, _now_ts()),
            )
            _db().commit()
    except Exception:
        _log(
            "db.conversation.insert_failed",
            {"open_kfid": _mask_id(open_kfid), "external_userid": _mask_id(external_userid), "role": role},
        )
        _log("db.conversation.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


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
    now = _now_ts()
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
        _log(
            "db.customer_profiles.upsert_failed",
            {"open_kfid": _mask_id(open_kfid), "external_userid": _mask_id(external_userid)},
        )
        _log("db.customer_profiles.upsert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


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
        _log(
            "db.customer_profiles.read_failed",
            {"open_kfid": _mask_id(open_kfid), "external_userid": _mask_id(external_userid)},
            debug_only=True,
        )
        return None


def kf_customer_batchget(external_userid_list: list[str], need_enter_session_context: int = 0) -> dict:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/customer/batchget?access_token={access_token}"
    payload = {
        "external_userid_list": [x for x in external_userid_list if (x or "").strip()],
        "need_enter_session_context": int(need_enter_session_context or 0),
    }
    _log(
        "wecom.kf_customer.batchget.request",
        {"count": len(payload["external_userid_list"]), "need_esc": payload["need_enter_session_context"]},
        debug_only=not DEBUG,
    )
    _log("wecom.kf_customer.batchget.request.detail", payload, debug_only=True)
    resp = http_post_json(url, payload, timeout=10)
    _log(
        "wecom.kf_customer.batchget.response",
        {
            "errcode": resp.get("errcode"),
            "errmsg": resp.get("errmsg"),
            "customer_list_len": len(resp.get("customer_list") or []),
            "invalid_external_userid_len": len(resp.get("invalid_external_userid") or []),
        },
    )
    return resp


def ensure_customer_profile(open_kfid: str, external_userid: str, *, last_seen_at: int | None = None) -> dict[str, Any] | None:
    """
    确保 customer_profiles 至少有 nickname（尽力而为）：
    - 若不存在或 updated_at 过久，则调用 kf/customer/batchget 拉取一次
    - 成功后 upsert 到 DB
    """
    if not open_kfid or not external_userid:
        return None
    now = _now_ts()
    prof = get_customer_profile(open_kfid, external_userid)

    need_refresh = True
    if isinstance(prof, dict):
        try:
            updated_at = int(prof.get("updated_at") or 0)
        except Exception:
            updated_at = 0
        nickname = (prof.get("nickname") or "").strip() if isinstance(prof.get("nickname"), str) else ""
        if nickname and updated_at > 0 and now - updated_at < _CUSTOMER_PROFILE_REFRESH_SECONDS:
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
            _log("customer.profile.nickname", {"open_kfid": open_kfid, "external_userid": _mask_id(external_userid), "nickname": nick})
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
    仅返回模型所需的语义字段：role/content
    双重限制：
    - 最近若干条消息（limit）
    - 总字数不超过 CONV_HISTORY_MAX_CHARS
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
                # 先多取一些，后续再按总长度裁剪，避免超长历史
                (open_kfid, external_userid, int(limit) * 3),
            ).fetchall()
        # rows 此时为时间倒序（新 -> 旧），按总字数上限从近到远挑选
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

        # 取出后需要按时间正序给模型
        selected = list(reversed(selected_rev))
        _log(
            "conversation.history_built",
            {
                "open_kfid": _mask_id(open_kfid),
                "external_userid": _mask_id(external_userid),
                "limit": limit,
                "actual_len": len(selected),
                "approx_total_chars": total_chars,
            },
            debug_only=True,
        )
        return selected
    except Exception:
        _log("db.conversation.read_failed", {"open_kfid": _mask_id(open_kfid), "external_userid": _mask_id(external_userid)})
        _log("db.conversation.read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
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
    _log(
        "token.usage",
        {
            "external_userid": _mask_id(external_userid),
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
                    _now_ts(),
                    msgid or "",
                    user_message_preview,
                    answer_preview,
                ),
            )
            _db().commit()
    except Exception:
        _log("db.llm_usage.insert_failed", {"external_userid": _mask_id(external_userid)})
        _log("db.llm_usage.insert_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


def cleanup_old_data() -> None:
    """
    轻量清理：
    - processed_messages 只保留最近 30 天
    - conversation_messages 每个 external_userid 只保留最近 30 条
    - llm_usage_logs 只保留最近 90 天
    """
    now = _now_ts()
    cutoff_processed = now - 30 * 24 * 60 * 60
    cutoff_llm = now - 90 * 24 * 60 * 60

    try:
        with _DB_LOCK:
            _db().execute("DELETE FROM processed_messages WHERE processed_at < ?", (cutoff_processed,))
            _db().execute("DELETE FROM llm_usage_logs WHERE created_at < ?", (cutoff_llm,))
            # 会话按 (open_kfid, external_userid) 只保留最近 30 条：对每个会话单独清理
            users = _db().execute("SELECT DISTINCT open_kfid, external_userid FROM conversation_messages").fetchall()
            for row in users:
                okfid = str(row["open_kfid"])
                uid = str(row["external_userid"])
                # 找出需要删除的 id：从第 31 条开始的历史记录
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
        _log("db.cleanup_failed", {})
        _log("db.cleanup_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)


# cleanup_old_data 降频执行控制：避免每条消息都执行全表扫描
_CLEANUP_MIN_INTERVAL_SECONDS = 30 * 60  # 至多每 30 分钟执行一次
_LAST_CLEANUP_TS = 0
_CLEANUP_LOCK = threading.Lock()


def maybe_cleanup_old_data() -> None:
    """
    按时间间隔触发 cleanup_old_data：
    - 在多 worker 场景下通过锁+二次检查保证线程安全
    - 若距离上次执行不足 _CLEANUP_MIN_INTERVAL_SECONDS，则直接返回
    """
    global _LAST_CLEANUP_TS
    now = _now_ts()
    if now - _LAST_CLEANUP_TS < _CLEANUP_MIN_INTERVAL_SECONDS:
        return
    with _CLEANUP_LOCK:
        # 二次检查，避免并发重复执行
        now2 = _now_ts()
        if now2 - _LAST_CLEANUP_TS < _CLEANUP_MIN_INTERVAL_SECONDS:
            return
        _LAST_CLEANUP_TS = now2
    _log("db.cleanup_maybe_run", {"ts": _LAST_CLEANUP_TS}, debug_only=True)
    cleanup_old_data()


# ----------------------------
# LLM（阿里 OpenAI 兼容）
# ----------------------------


def _llm_env_ok() -> bool:
    if not OpenAI:
        _log("llm.env_missing", {"missing": "openai"})
        return False
    if not ALI_API_KEY:
        _log("llm.env_missing", {"missing": "ALI_API_KEY"})
        return False
    if not ALI_BASE_URL:
        _log("llm.env_missing", {"missing": "ALI_BASE_URL"})
        return False
    if not ALI_MODEL:
        _log("llm.env_missing", {"missing": "ALI_MODEL"})
        return False
    return True


def ask_llm(messages: list[dict[str, str]]) -> tuple[str, dict[str, int], bool]:
    """
    使用 OpenAI 兼容方式调用阿里模型，返回 assistant 文本。
    若失败/超时/空内容：返回兜底话术（不抛异常）。
    第三个返回值 fallback 表示本次是否为兜底回复（包括调用失败、空响应等）。
    """
    usage: dict[str, int] = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    if not _llm_env_ok():
        return FALLBACK_TEXT, usage, True

    try:
        client = OpenAI(api_key=ALI_API_KEY, base_url=ALI_BASE_URL, timeout=float(LLM_TIMEOUT_SECONDS))
        resp = client.chat.completions.create(
            model=ALI_MODEL,
            messages=messages,  # type: ignore[arg-type]
        )
        content = ""
        try:
            content = (resp.choices[0].message.content or "").strip()
        except Exception:
            content = ""
        try:
            u = getattr(resp, "usage", None)
            if u is not None:
                usage = {
                    "prompt_tokens": int(getattr(u, "prompt_tokens", 0) or 0),
                    "completion_tokens": int(getattr(u, "completion_tokens", 0) or 0),
                    "total_tokens": int(getattr(u, "total_tokens", 0) or 0),
                }
        except Exception:
            _log("llm.usage_parse_failed", {}, debug_only=True)
        if not content:
            _log("llm.empty_response", {})
            return FALLBACK_TEXT, usage, True
        return content, usage, False
    except Exception as e:
        _log("llm.call_failed", {"err": repr(e)})
        _log("llm.call_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return FALLBACK_TEXT, usage, True


def ask_llm_for_user(open_kfid: str, external_userid: str, user_text: str, msgid: str | None = None) -> tuple[str, bool]:
    history = get_recent_conversation_messages(open_kfid, external_userid, limit=CONV_HISTORY_LIMIT)
    system_prompt = build_system_prompt(_CLUB_PROFILE)
    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_text})

    _log(
        "llm.context",
        {
            "open_kfid": _mask_id(open_kfid),
            "external_userid": _mask_id(external_userid),
            "msgid": msgid or "",
            "model": ALI_MODEL,
            "history_messages": history,
            "user_text": user_text,
        },
    )
    _log(
        "llm.will_call",
        {
            "external_userid": _mask_id(external_userid),
            "history_len": len(history),
            "timeout_s": LLM_TIMEOUT_SECONDS,
        },
        debug_only=True,
    )
    answer, usage, fallback = ask_llm(messages)
    ok = (not fallback) and bool((answer or "").strip())
    _log(
        "llm.result",
        {
            "external_userid": _mask_id(external_userid),
            "msgid": msgid or "",
            "model": ALI_MODEL,
            "success": ok,
            "prompt_tokens": int(usage.get("prompt_tokens") or 0),
            "completion_tokens": int(usage.get("completion_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens") or 0),
            "answer": answer or "",
        },
    )
    _log(
        "llm.done",
        {
            "success": ok,
            "answer_preview": (answer or "")[:200],
            "fallback": fallback,
        },
        debug_only=True,
    )
    try:
        append_llm_usage_log(
            external_userid=external_userid,
            model=ALI_MODEL,
            usage=usage,
            msgid=msgid,
            user_message_preview=user_text[:200],
            answer_preview=(answer or "")[:200],
        )
    except Exception:
        # 日志已在 append_llm_usage_log 内部处理
        pass
    return answer, not fallback


# ----------------------------
# 后台队列 worker：快速 ack
# ----------------------------


_TASK_Q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=100)
_WORKER_STARTED = False


def _enqueue_task(task: dict[str, Any]) -> None:
    try:
        _TASK_Q.put_nowait(task)
        _log("worker.task_enqueued", {"type": task.get("type"), "open_kfid": task.get("open_kfid")})
    except queue.Full:
        _log("worker.queue_full_drop", {"type": task.get("type")})


def _extract_customer_messages(msg_list: list[dict]) -> list[dict]:
    """
    从 sync_msg 返回中提取“客户侧消息”，并按时间顺序输出：
    - 仅保留带 external_userid 的消息
    - 优先使用 send_time 作为排序字段，缺失则回退到原顺序
    """
    if not msg_list:
        return []
    # 先过滤出“看起来像客户消息”的记录
    candidates: list[tuple[int, dict]] = []
    for idx, m in enumerate(msg_list):
        if not isinstance(m, dict):
            continue
        if not m.get("external_userid"):
            continue
        send_ts = 0
        try:
            send_ts = int(m.get("send_time") or 0)
        except Exception:
            send_ts = 0
        # 记录原始顺序和时间戳，后续排序使用
        m_with_ts = dict(m)
        m_with_ts["_send_ts"] = send_ts
        candidates.append((idx, m_with_ts))

    if not candidates:
        return []

    # 按 send_time 升序，若相同则按原顺序
    candidates.sort(key=lambda t: (t[1].get("_send_ts", 0), t[0]))
    ordered: list[dict] = []
    for _, m in candidates:
        m.pop("_send_ts", None)
        ordered.append(m)
    return ordered


def _coalesce_customer_messages(ordered_msgs: list[dict]) -> list[dict]:
    """
    轻量合并策略（可选）：
    - 同一批次、同一 external_userid 的多条“文本消息”合并为一条，避免短时间内刷屏
    - 合并方式：按时间顺序把文本内容用换行拼接，代表消息使用“最后一条文本消息”
    - 非文本消息：若该用户本批没有文本消息，则仅保留“最后一条非文本消息”代表性处理一次
    - 返回列表项形如：
      {
        "msg": <代表性消息dict>,
        "extra_msgids": [需要仅做去重标记而不触发回复的其它 msgid 列表]
      }
    """
    if not ordered_msgs:
        return []
    groups: dict[str, list[dict]] = {}
    for m in ordered_msgs:
        uid = str(m.get("external_userid") or "")
        if not uid:
            # 正常不会走到这里，因为 _extract_customer_messages 已过滤
            uid = "_"
        groups.setdefault(uid, []).append(m)

    coalesced: list[dict] = []
    for uid, items in groups.items():
        # 拆分文本 / 非文本
        text_items = [x for x in items if str(x.get("msgtype") or "") == "text"]
        non_text_items = [x for x in items if str(x.get("msgtype") or "") != "text"]
        if text_items:
            # 取全部文本内容合并，代表消息取最后一条文本
            merged_texts: list[str] = []
            extra_msgids: list[str] = []
            for it in text_items:
                t = (it.get("text") or {}).get("content") if isinstance(it.get("text"), dict) else ""
                t = (t or "").strip()
                if t:
                    merged_texts.append(t)
            rep = text_items[-1]
            # 覆盖代表消息的文本内容为合并后字符串
            rep = dict(rep)  # 复制一份，避免影响原对象
            rep["text"] = {"content": "\n".join(merged_texts)}
            # 其它文本消息的 msgid 仅做去重标记
            extra_msgids = [str(it.get("msgid") or "") for it in text_items[:-1] if it.get("msgid")]
            coalesced.append({"msg": rep, "extra_msgids": extra_msgids})
        else:
            # 无文本，仅保留最后一条非文本
            rep = non_text_items[-1]
            extra_msgids = [str(it.get("msgid") or "") for it in non_text_items[:-1] if it.get("msgid")]
            coalesced.append({"msg": rep, "extra_msgids": extra_msgids})

    # 不同用户之间，保持整体时间顺序：按代表消息的 send_time 排序
    def _key(o: dict) -> int:
        m = o.get("msg") or {}
        try:
            return int(m.get("send_time") or 0)
        except Exception:
            return 0

    coalesced.sort(key=_key)
    return coalesced


def truncate_reply(text: str, max_chars: int = 150) -> str:
    """
    轻量后处理：控制回复长度，尽量不破坏中文阅读体验。
    - 超过 max_chars 时，优先在该范围内找“。！？”，截断到句子末尾；
    - 若找不到句末标点，则直接截断到 max_chars。
    """
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    # 在前 max_chars 范围内从后往前找句末标点
    cut_region = t[:max_chars]
    for ch in "。！？!?.":
        idx = cut_region.rfind(ch)
        if idx >= 20:  # 避免太短就截断
            return cut_region[: idx + 1]
    return cut_region


def _handle_one_customer_message(open_kfid: str, msg: dict) -> None:
    """
    处理单条“客户侧消息”，包含：
    - msgid 去重
    - 非文本固定回复
    - 敏感词优先
    - FAQ 优先
    - 最终走 LLM
    """
    msgid = str(msg.get("msgid") or "")
    external_userid = str(msg.get("external_userid") or "")
    msgtype = str(msg.get("msgtype") or "")
    # 单条消息处理的“调用边界”分隔块（非常高频，保持信息密度）
    _log_separator(
        "CALL msg.handle",
        {
            "open_kfid": open_kfid,
            "msgid": msgid,
            "user": _mask_id(external_userid),
            "msgtype": msgtype,
        },
    )
    create_time = None
    try:
        create_time = int(msg.get("send_time") or 0) or None
    except Exception:
        create_time = None

    # 落库客户资料（nickname 等）：用于后续可视化展示用户列表
    try:
        ensure_customer_profile(open_kfid, external_userid, last_seen_at=int(create_time or 0) or None)
    except Exception:
        _log("customer.profile.ensure_failed", {"open_kfid": _mask_id(open_kfid), "external_userid": _mask_id(external_userid)}, debug_only=True)

    _log("msg.candidate", {"msgid": msgid, "external_userid": _mask_id(external_userid), "msgtype": msgtype}, debug_only=True)

    if not msgid:
        _log("msg.missing_msgid_skip", {"external_userid": _mask_id(external_userid)})
        return

    if is_msg_processed(open_kfid, msgid):
        _log("msg.duplicate_skip", {"msgid": msgid})
        return

    # 先抢占写入去重标记，避免并发情况下重复回复
    if not mark_msg_processed(open_kfid, msgid, external_userid, create_time):
        _log("msg.dedup_mark_failed_skip", {"msgid": msgid})
        return
    _log("msg.dedup_mark_ok", {"msgid": msgid})

    # 非文本统一回复固定话术
    if msgtype != "text":
        _log("msg.non_text", {"msgtype": msgtype, "reply_preview": NON_TEXT_REPLY[:120]})
        if NON_TEXT_REPLY.strip():
            reply = truncate_reply(NON_TEXT_REPLY)
            _log(
                "msg.flow",
                {
                    "open_kfid": open_kfid,
                    "msgid": msgid,
                    "external_userid": _mask_id(external_userid),
                    "msgtype": msgtype,
                    "dedup_new": True,
                    "path": "non_text_fixed_reply",
                },
            )
            _log("reply.final", {"source": "non_text_fixed_reply", "msgid": msgid, "external_userid": _mask_id(external_userid), "preview": reply[:200]})
            kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)
        return

    text = (msg.get("text") or {}).get("content") if isinstance(msg.get("text"), dict) else ""
    user_text = (text or "").strip()
    _log("msg.text", {"len": len(user_text), "preview": user_text[:200]}, debug_only=not DEBUG)

    if not user_text:
        _log("msg.empty_text_skip", {"msgid": msgid})
        return

    # 1) 敏感词优先：不调用大模型
    hit_sensitive = match_sensitive_keyword(user_text)
    if hit_sensitive:
        _log("policy.sensitive_hit", {"hit": True, "keyword": hit_sensitive})
        reply = truncate_reply(SENSITIVE_FALLBACK_TEXT)
        if reply.strip():
            # 敏感词场景：不调用 LLM，直接在会话中记录一问一答
            append_conversation_message(open_kfid, external_userid, "user", user_text)
            append_conversation_message(open_kfid, external_userid, "assistant", reply)
            _log(
                "msg.flow",
                {
                    "open_kfid": open_kfid,
                    "msgid": msgid,
                    "external_userid": _mask_id(external_userid),
                    "msgtype": "text",
                    "dedup_new": True,
                    "path": "sensitive_rule",
                },
            )
            _log("reply.final", {"source": "sensitive_rule", "msgid": msgid, "external_userid": _mask_id(external_userid), "preview": reply[:200]})
            kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)
        return
    _log("policy.sensitive_hit", {"hit": False})

    # 2) FAQ 优先：命中则直接回复，不调用大模型
    faq_item = match_faq(user_text)
    if faq_item:
        faq_id = str(faq_item.get("id") or "")
        faq_title = str(faq_item.get("title") or "")
        answer = str(faq_item.get("answer") or "").strip()
        _log("faq.hit", {"hit": True, "id": faq_id, "title": faq_title})
        _log("llm.will_call", {"skip": True, "reason": "faq_hit"}, debug_only=True)
        if answer:
            reply = truncate_reply(answer)
            append_conversation_message(open_kfid, external_userid, "user", user_text)
            append_conversation_message(open_kfid, external_userid, "assistant", reply)
            _log(
                "msg.flow",
                {
                    "open_kfid": open_kfid,
                    "msgid": msgid,
                    "external_userid": _mask_id(external_userid),
                    "msgtype": "text",
                    "dedup_new": True,
                    "path": f"faq:{faq_id}",
                },
            )
            _log("reply.final", {"source": f"faq:{faq_id}", "msgid": msgid, "external_userid": _mask_id(external_userid), "preview": reply[:200]})
            kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)
        return
    _log("faq.hit", {"hit": False})

    # 3) 未命中：调用大模型
    _log(
        "msg.flow",
        {
            "open_kfid": open_kfid,
            "msgid": msgid,
            "external_userid": _mask_id(external_userid),
            "msgtype": "text",
            "dedup_new": True,
            "path": "llm",
        },
    )

    # 调用 LLM 前不写入当前 user，只带历史 + 当前 user 文本进入 prompt，避免重复
    answer, is_real = ask_llm_for_user(open_kfid, external_userid, user_text, msgid=msgid)
    answer = (answer or "").strip()
    if not answer:
        _log("reply.empty_skip", {"msgid": msgid})
        return

    # 先写入当前 user，再视情况写入 assistant（避免兜底话术污染历史）
    reply = truncate_reply(answer)
    append_conversation_message(open_kfid, external_userid, "user", user_text)
    if is_real:
        append_conversation_message(open_kfid, external_userid, "assistant", reply)
        _log(
            "conversation.append_assistant",
            {"msgid": msgid, "external_userid": _mask_id(external_userid), "from": "llm"},
            debug_only=True,
        )
        reply_source = "llm"
    else:
        _log(
            "conversation.skip_assistant_fallback",
            {"msgid": msgid, "external_userid": _mask_id(external_userid)},
        )
        reply_source = "llm_fallback"

    _log("reply.final", {"source": reply_source, "msgid": msgid, "external_userid": _mask_id(external_userid), "preview": reply[:200]})
    kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)


def _mark_extra_msgids_processed(open_kfid: str, extra_msgids: list[str], external_userid: str) -> None:
    """
    对“已合并但不单独处理/不回复”的 msgid 做去重标记，避免下次再被处理。
    - 仅执行 mark_msg_processed，不触发任何回复或会话写入
    - create_time 无法准确取到时用 None（不影响去重）
    """
    if not extra_msgids:
        return
    for mid in extra_msgids:
        msgid = (mid or "").strip()
        if not msgid:
            continue
        if is_msg_processed(open_kfid, msgid):
            continue
        ok = mark_msg_processed(open_kfid, msgid, external_userid, None)
        _log(
            "msg.dedup_mark_coalesced",
            {"open_kfid": _mask_id(open_kfid), "msgid": msgid, "external_userid": _mask_id(external_userid), "ok": ok},
            debug_only=True,
        )


def _pick_latest_customer_message_from_sync(open_kfid: str, token: str, cursor: str | None) -> tuple[dict | None, str | None]:
    """
    冷启动保护用：
    - 从指定 cursor 开始不断 sync_msg 翻页推进，直到 has_more=0
    - 过程中记录“最新一条客户侧消息”（按 send_time 最大，其次按出现顺序）
    - 返回 (latest_msg_dict_or_none, final_next_cursor_or_none)
    注意：这里只挑选消息，不做任何回复/去重写入。
    """
    latest: dict | None = None
    latest_send_time = -1
    latest_seen_seq = -1

    cur = cursor
    seq = 0
    final_cursor: str | None = None
    while True:
        sync_resp = kf_sync_msg(open_kfid=open_kfid, token=token, cursor=cur, limit=100)
        if str(sync_resp.get("errcode")) != "0":
            _log("wecom.kf_sync_msg.failed", {"errcode": sync_resp.get("errcode"), "errmsg": sync_resp.get("errmsg")})
            return None, None

        msg_list: list[dict] = sync_resp.get("msg_list") or []
        customer_msgs = _extract_customer_messages(msg_list)
        for m in customer_msgs:
            seq += 1
            st = 0
            try:
                st = int(m.get("send_time") or 0)
            except Exception:
                st = 0
            if st > latest_send_time or (st == latest_send_time and seq > latest_seen_seq):
                latest = m
                latest_send_time = st
                latest_seen_seq = seq

        next_cursor = (sync_resp.get("next_cursor") or "").strip() or None
        final_cursor = next_cursor or final_cursor

        has_more = str(sync_resp.get("has_more") or "0")
        if has_more != "1":
            break
        if not next_cursor:
            break
        cur = next_cursor

    return latest, final_cursor


def process_one_kf_message(open_kfid: str, token: str) -> None:
    """
    处理一次客服 sync_msg：
    - 从 SQLite 读取 cursor
    - 调用企业微信 kf_sync_msg
    - 按时间顺序遍历本批次所有客户侧消息
    - 全部处理完成后，再将 next_cursor 写回 SQLite
    """
    raw_cursor = get_kf_cursor(open_kfid)
    cursor = raw_cursor or None
    cursor_missing = raw_cursor is None

    # 冷启动保护：cursor 缺失时，默认只考虑“最新一条客户消息”
    if cursor_missing:
        _log("kf_cursor.missing_cold_start_guard", {"open_kfid": open_kfid})
        latest_msg, final_cursor = _pick_latest_customer_message_from_sync(open_kfid=open_kfid, token=token, cursor=None)
        if final_cursor:
            set_kf_cursor(open_kfid, final_cursor)

        if not isinstance(latest_msg, dict):
            _log("cold_start.no_customer_message", {"open_kfid": open_kfid})
            return

        # 若最新消息距离现在超过 5 分钟，则忽略不回复（但 cursor 已推进到最新）
        try:
            st = int(latest_msg.get("send_time") or 0)
        except Exception:
            st = 0
        age_s = _now_ts() - st
        if st <= 0 or age_s > 5 * 60:
            _log(
                "cold_start.latest_too_old_skip_reply",
                {"open_kfid": open_kfid, "latest_send_time": st, "age_s": age_s},
            )
            return

        _log(
            "cold_start.will_reply_latest_only",
            {"open_kfid": open_kfid, "msgid": latest_msg.get("msgid"), "external_userid": _mask_id(str(latest_msg.get("external_userid") or ""))},
        )
        try:
            _handle_one_customer_message(open_kfid, latest_msg)
        except Exception:
            _log("msg.handle_failed", {"open_kfid": open_kfid, "msgid": latest_msg.get("msgid")})
            _log("msg.handle_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return

    sync_resp = kf_sync_msg(open_kfid=open_kfid, token=token, cursor=cursor, limit=100)
    if str(sync_resp.get("errcode")) != "0":
        _log("wecom.kf_sync_msg.failed", {"errcode": sync_resp.get("errcode"), "errmsg": sync_resp.get("errmsg")})
        return

    msg_list: list[dict] = sync_resp.get("msg_list") or []
    _log(
        "wecom.kf_sync_msg.ok",
        {
            "open_kfid": open_kfid,
            "msg_list_len": len(msg_list),
            "has_more": sync_resp.get("has_more"),
        },
    )

    customer_msgs = _extract_customer_messages(msg_list)
    _log(
        "msg.batch_customer_messages",
        {
            "open_kfid": open_kfid,
            "total_msg_list_len": len(msg_list),
            "customer_msg_count": len(customer_msgs),
        },
        debug_only=True,
    )

    if not customer_msgs:
        _log("msg.none_customer_message", {"open_kfid": open_kfid})
        # 即便本批没有客户消息，cursor 仍然前进，避免重复拉取同一批
        next_cursor = sync_resp.get("next_cursor") or ""
        if next_cursor:
            set_kf_cursor(open_kfid, next_cursor)
            _log("kf_cursor.updated_no_customer", {"open_kfid": open_kfid}, debug_only=True)
        return

    # 轻量合并：同一批次同一用户多条文本合并，避免刷屏
    coalesced_items = _coalesce_customer_messages(customer_msgs)
    _log(
        "msg.batch_coalesced",
        {
            "open_kfid": open_kfid,
            "customer_msg_count": len(customer_msgs),
            "coalesced_count": len(coalesced_items),
        },
        debug_only=True,
    )

    # 按时间顺序逐条处理（合并后的）客户消息，确保不漏
    for item in coalesced_items:
        msg = item.get("msg") if isinstance(item, dict) else None
        extra_msgids = item.get("extra_msgids") if isinstance(item, dict) else None
        if not isinstance(msg, dict):
            continue
        try:
            _handle_one_customer_message(open_kfid, msg)
            # 本条处理结束后，把合并的其它 msgid 标记已处理，避免下次重复
            external_userid = str(msg.get("external_userid") or "")
            if isinstance(extra_msgids, list) and external_userid:
                _mark_extra_msgids_processed(open_kfid, extra_msgids, external_userid)
        except Exception:
            _log(
                "msg.handle_failed",
                {
                    "open_kfid": open_kfid,
                    "msgid": msg.get("msgid"),
                },
            )
            _log("msg.handle_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)

    # 本批次所有客户消息处理结束后，再推进 cursor，避免“先推进、后处理”导致漏消息
    next_cursor = sync_resp.get("next_cursor") or ""
    if next_cursor:
        set_kf_cursor(open_kfid, next_cursor)
        _log("kf_cursor.updated", {"open_kfid": open_kfid}, debug_only=True)

    # 低频清理历史数据
    maybe_cleanup_old_data()


def _worker_loop() -> None:
    _log("worker.started", {})
    while True:
        task = _TASK_Q.get()
        try:
            t = task.get("type")
            if t == "kf_msg_or_event":
                open_kfid = str(task.get("open_kfid") or "")
                token = str(task.get("token") or "")
                # worker 任务级别分隔块 + 绑定 trace，便于把同一次任务的日志串起来
                trace = f"wk/{_now_ts()}-{(open_kfid[-6:] if open_kfid else 'nokf')}"
                _set_log_trace(trace)
                _log_separator("CALL worker.task", {"type": t, "open_kfid": open_kfid, "token_len": len(token)})
                _log("worker.begin_task", {"type": t, "open_kfid": open_kfid, "token_len": len(token)})
                if open_kfid and token:
                    lk = _get_open_kfid_lock(open_kfid)
                    with lk:
                        process_one_kf_message(open_kfid=open_kfid, token=token)
                else:
                    _log("worker.task_missing_fields_skip", {"type": t})
            else:
                _log("worker.unknown_task_skip", {"type": t})
        except Exception:
            _log("worker.task_failed", {"err": "exception"})
            _log("worker.task_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        finally:
            # 清理 trace，避免串到下一条任务
            _set_log_trace(None)
            _TASK_Q.task_done()


def start_worker_once() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    for i in range(WORKER_COUNT):
        th = threading.Thread(target=_worker_loop, name=f"kf-worker-{i+1}", daemon=True)
        th.start()
    _WORKER_STARTED = True


# ----------------------------
# open_kfid 维度串行化：避免 cursor 竞态
# ----------------------------

_OPEN_KFID_LOCKS: dict[str, threading.Lock] = {}
_OPEN_KFID_LOCKS_GUARD = threading.Lock()


def _get_open_kfid_lock(open_kfid: str) -> threading.Lock:
    if not open_kfid:
        # 兜底：空 open_kfid 也给个全局锁，避免并发写 cursor/DB
        return _DB_LOCK
    with _OPEN_KFID_LOCKS_GUARD:
        lk = _OPEN_KFID_LOCKS.get(open_kfid)
        if lk is None:
            lk = threading.Lock()
            _OPEN_KFID_LOCKS[open_kfid] = lk
        return lk


# ----------------------------
# Flask 路由
# ----------------------------


@app.route("/", methods=["GET"])
def index():
    return "server is running"


@app.route("/wechat", methods=["GET"])
def wechat_verify():
    """
    企业微信 URL 验证接口（保留原逻辑）
    """
    msg_signature = request.args.get("msg_signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    echostr = request.args.get("echostr", "")

    if not any([msg_signature, timestamp, nonce, echostr]):
        return "wechat get ok"
    if not all([msg_signature, timestamp, nonce, echostr]):
        return "missing params", 400

    try:
        local_signature = sha1_signature(TOKEN, timestamp, nonce, echostr)
        if not hmac.compare_digest(local_signature, msg_signature):
            return "signature error", 403
        plain_text = decrypt_wecom_message(ENCODING_AES_KEY, CORP_ID, echostr)
        _log("wecom.verify_ok", {"plain_preview": plain_text[:200]}, debug_only=True)
        return plain_text
    except Exception as e:
        _log("wecom.verify_failed", {"err": repr(e)})
        _log("wecom.verify_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return "verify failed", 500


@app.route("/wechat", methods=["POST"])
def wechat_callback():
    """
    企业微信消息回调接口：
    - webhook 只做验签+解密+解析+投递任务，快速 ack
    - 真正的 sync_msg / LLM / send_msg 在后台 worker
    """
    msg_signature = request.args.get("msg_signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    if not all([msg_signature, timestamp, nonce]):
        return "missing params", 400

    raw_xml = request.data.decode("utf-8", errors="ignore")
    # 非 DEBUG 模式只打印关键摘要，原始 XML 仅在 DEBUG 打印
    _log("wecom.webhook.raw_xml", {"raw_xml": raw_xml}, debug_only=True)

    try:
        # webhook 调用级别分隔块 + 绑定 trace
        trace = f"wb/{_now_ts()}-{(nonce[-4:] if nonce else '0000')}"
        _set_log_trace(trace)
        _log_separator(
            "CALL webhook",
            {
                "timestamp": timestamp,
                "nonce": nonce,
            },
        )
        encrypt_text = extract_encrypt_from_xml(raw_xml)
        local_signature = sha1_signature(TOKEN, timestamp, nonce, encrypt_text)
        if not hmac.compare_digest(local_signature, msg_signature):
            _log("wecom.webhook.signature_failed", {})
            return "signature error", 403

        plain_xml = decrypt_wecom_message(ENCODING_AES_KEY, CORP_ID, encrypt_text)
        _log("wecom.webhook.decrypted", {"plain_len": len(plain_xml)})
        _log("wecom.webhook.decrypted_xml", {"plain_xml": plain_xml}, debug_only=True)

        parsed = parse_wecom_plain_xml(plain_xml)
        _log("wecom.webhook.parsed", {"keys": sorted(list(parsed.keys()))})
        _log("wecom.webhook.parsed.detail", {"parsed": parsed}, debug_only=True)

        is_kf_event = parsed.get("Event") == "kf_msg_or_event"
        _log("route.webhook", {"hit_kf_event": is_kf_event, "content_len": len(raw_xml)})

        if is_kf_event:
            open_kfid = parsed.get("OpenKfId") or ""
            token = parsed.get("Token") or ""
            _log("wecom.webhook.kf_event", {"OpenKfId": open_kfid, "Token_len": len(token)})
            _enqueue_task({"type": "kf_msg_or_event", "open_kfid": open_kfid, "token": token})
        else:
            _log("wecom.webhook.non_kf_event_ack", {})

        return "success"
    except Exception as e:
        _log("wecom.webhook.failed", {"err": repr(e)})
        _log("wecom.webhook.failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return "fail", 500
    finally:
        _set_log_trace(None)


def _print_config_summary() -> None:
    _log(
        "config.summary",
        {
            "DEBUG": DEBUG,
            "TOKEN": bool(TOKEN),
            "ENCODING_AES_KEY": bool(ENCODING_AES_KEY),
            "CORP_ID": CORP_ID,
            "CORP_SECRET": bool(CORP_SECRET),
            "ALI_API_KEY": bool(ALI_API_KEY),
            "ALI_BASE_URL": ALI_BASE_URL,
            "ALI_MODEL": ALI_MODEL,
            "SQLITE_PATH": SQLITE_PATH,
            "CLUB_PROFILE_PATH": CLUB_PROFILE_PATH,
            "FAQ_PATH": FAQ_PATH,
            "LLM_TIMEOUT_SECONDS": LLM_TIMEOUT_SECONDS,
            "CONV_HISTORY_LIMIT": CONV_HISTORY_LIMIT,
            "WORKER_COUNT": WORKER_COUNT,
        },
    )


if __name__ == "__main__":
    # Delegate to the refactored entrypoint.
    from app.main import run as _run

    _run(host="0.0.0.0", port=5000)

