from __future__ import annotations

import re

from app.config import REPLY_TRUNCATE_MAX_CHARS
from app.core.llm import ask_llm_for_user
from app.core.logging import log
from app.core.policy import FALLBACK_TEXT, SENSITIVE_FALLBACK_TEXT, match_faq, match_sensitive_keyword
from app.core.utils import mask_id
from app.db.sqlite_store import append_conversation_message


def _truncate_reply(text: str, max_chars: int = REPLY_TRUNCATE_MAX_CHARS) -> str:
    t = (text or "").strip()
    if max_chars <= 0:
        return t
    if len(t) <= max_chars:
        return t
    cut_region = t[:max_chars]
    for ch in "。！？!?.":
        idx = cut_region.rfind(ch)
        if idx >= 20:
            return cut_region[: idx + 1]
    return cut_region


_MARKDOWN_CHARS = re.compile(r"[`*_#>]")
_WEIRD_SYMBOLS = re.compile(r"[✅⭐🔹🔸🔺🔻•●◆■□◇▪️◾◽—]+")
_LEADING_LIST_PREFIX = re.compile(r"^\s*(?:[-*•]|[0-9]+\)|[0-9]+\.)\s*")
_EMPTY_LINE_RE = re.compile(r"\n{3,}")
_BAD_ENDING_RE = re.compile(r"(需要我帮你.*吗\??|要不要我.*\??|是否需要我.*\??)\s*$")


def _smart_shorten_for_wecom(text: str, max_chars: int = 120) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    cut_region = t[:max_chars]
    for ch in "。！？!?":
        idx = cut_region.rfind(ch)
        if idx >= 24:
            return cut_region[: idx + 1]
    return cut_region.rstrip("，,;；:：")


def _sanitize_reply_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return t

    # 企微文本兜底清洗：去 markdown 与装饰符，避免机器人感。
    t = _MARKDOWN_CHARS.sub("", t)
    t = _WEIRD_SYMBOLS.sub("", t)
    lines = [line.strip() for line in t.splitlines()]
    lines = [_LEADING_LIST_PREFIX.sub("", line) for line in lines if line]
    t = "\n".join(lines)
    t = _EMPTY_LINE_RE.sub("\n\n", t).strip()
    t = _BAD_ENDING_RE.sub("", t).strip()
    t = _smart_shorten_for_wecom(t, max_chars=120)
    return t if t else FALLBACK_TEXT


def reply_for_text(
    *,
    open_kfid: str,
    external_userid: str,
    user_text: str,
    msgid: str | None = None,
) -> tuple[str, str]:
    """
    Core smart-customer-service logic for one text message.
    Return: (reply_text, reply_source)
    """

    # 1) sensitive rule first
    hit_sensitive = match_sensitive_keyword(user_text)
    if hit_sensitive:
        log("policy.sensitive_hit", {"hit": True, "keyword": hit_sensitive})
        reply = _sanitize_reply_text(_truncate_reply(SENSITIVE_FALLBACK_TEXT))
        if reply.strip():
            append_conversation_message(open_kfid, external_userid, "user", user_text)
            append_conversation_message(open_kfid, external_userid, "assistant", reply)
            return reply, "sensitive_rule"
        return "", "sensitive_rule_empty"
    log("policy.sensitive_hit", {"hit": False})

    # 2) faq first
    faq_item = match_faq(user_text)
    if faq_item:
        faq_id = str(faq_item.get("id") or "")
        faq_title = str(faq_item.get("title") or "")
        answer = str(faq_item.get("answer") or "").strip()
        log("faq.hit", {"hit": True, "id": faq_id, "title": faq_title})
        log("llm.will_call", {"skip": True, "reason": "faq_hit"}, debug_only=True)
        if answer:
            reply = _sanitize_reply_text(_truncate_reply(answer))
            append_conversation_message(open_kfid, external_userid, "user", user_text)
            append_conversation_message(open_kfid, external_userid, "assistant", reply)
            return reply, f"faq:{faq_id}"
        return "", f"faq:{faq_id}:empty"
    log("faq.hit", {"hit": False})

    # 3) llm fallback
    log(
        "msg.flow",
        {
            "open_kfid": open_kfid,
            "msgid": msgid or "",
            "external_userid": mask_id(external_userid),
            "msgtype": "text",
            "dedup_new": True,
            "path": "llm",
        },
    )
    answer, is_real = ask_llm_for_user(open_kfid, external_userid, user_text, msgid=msgid)
    answer = (answer or "").strip()
    if not answer:
        return "", "llm_empty"

    reply = _sanitize_reply_text(_truncate_reply(answer))
    append_conversation_message(open_kfid, external_userid, "user", user_text)
    if is_real:
        append_conversation_message(open_kfid, external_userid, "assistant", reply)
        return reply, "llm"
    return reply, "llm_fallback"

