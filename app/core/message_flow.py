from __future__ import annotations

import traceback
from typing import Any

from app.channels.wecom import kf_send_text, kf_sync_msg
from app.config import REPLY_TRUNCATE_MAX_CHARS
from app.core.logging import log, log_separator
from app.core.policy import (
    NON_TEXT_REPLY,
)
from app.core.service import reply_for_text
from app.core.utils import mask_id, now_ts
from app.db.sqlite_store import (
    ensure_customer_profile,
    get_kf_cursor,
    is_msg_processed,
    mark_msg_processed,
    maybe_cleanup_old_data,
    set_kf_cursor,
)


def truncate_reply(text: str, max_chars: int = REPLY_TRUNCATE_MAX_CHARS) -> str:
    """
    Light post-process:
    - If over max_chars, cut near punctuation within that region.
    """

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


def _extract_customer_messages(msg_list: list[dict]) -> list[dict]:
    """
    Extract customer-side messages from sync_msg results:
    - keep only records with external_userid
    - sort by send_time (if missing, use original order)
    """

    if not msg_list:
        return []

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
        m_with_ts = dict(m)
        m_with_ts["_send_ts"] = send_ts
        candidates.append((idx, m_with_ts))

    if not candidates:
        return []

    candidates.sort(key=lambda t: (t[1].get("_send_ts", 0), t[0]))
    ordered: list[dict] = []
    for _, m in candidates:
        m.pop("_send_ts", None)
        ordered.append(m)
    return ordered


def _coalesce_customer_messages(ordered_msgs: list[dict]) -> list[dict]:
    """
    Coalesce strategy:
    - Merge multiple text messages from same external_userid in this batch
    - Keep represent message as the last text message
    - Non-text: keep only last non-text message as representative
    """

    if not ordered_msgs:
        return []
    groups: dict[str, list[dict]] = {}
    for m in ordered_msgs:
        uid = str(m.get("external_userid") or "")
        if not uid:
            uid = "_"
        groups.setdefault(uid, []).append(m)

    coalesced: list[dict] = []
    for uid, items in groups.items():
        text_items = [x for x in items if str(x.get("msgtype") or "") == "text"]
        non_text_items = [x for x in items if str(x.get("msgtype") or "") != "text"]
        if text_items:
            merged_texts: list[str] = []
            extra_msgids: list[str] = []
            for it in text_items:
                t = (it.get("text") or {}).get("content") if isinstance(it.get("text"), dict) else ""
                t = (t or "").strip()
                if t:
                    merged_texts.append(t)
            rep = text_items[-1]
            rep = dict(rep)
            rep["text"] = {"content": "\n".join(merged_texts)}
            extra_msgids = [str(it.get("msgid") or "") for it in text_items[:-1] if it.get("msgid")]
            coalesced.append({"msg": rep, "extra_msgids": extra_msgids})
        else:
            rep = non_text_items[-1]
            extra_msgids = [str(it.get("msgid") or "") for it in non_text_items[:-1] if it.get("msgid")]
            coalesced.append({"msg": rep, "extra_msgids": extra_msgids})

    def _key(o: dict) -> int:
        m = o.get("msg") or {}
        try:
            return int(m.get("send_time") or 0)
        except Exception:
            return 0

    coalesced.sort(key=_key)
    return coalesced


def _handle_one_customer_message(open_kfid: str, msg: dict) -> None:
    """
    Handle one customer-side message:
    - dedup check
    - fixed non-text reply
    - sensitive keyword rule
    - FAQ rule
    - fallback to LLM
    """

    msgid = str(msg.get("msgid") or "")
    external_userid = str(msg.get("external_userid") or "")
    msgtype = str(msg.get("msgtype") or "")

    log_separator(
        "CALL msg.handle",
        {"open_kfid": open_kfid, "msgid": msgid, "user": mask_id(external_userid), "msgtype": msgtype},
    )

    create_time = None
    try:
        create_time = int(msg.get("send_time") or 0) or None
    except Exception:
        create_time = None

    # Persist customer profile for later visualization
    try:
        ensure_customer_profile(open_kfid, external_userid, last_seen_at=int(create_time or 0) or None)
    except Exception:
        log(
            "customer.profile.ensure_failed",
            {"open_kfid": mask_id(open_kfid), "external_userid": mask_id(external_userid)},
            debug_only=True,
        )

    log("msg.candidate", {"msgid": msgid, "external_userid": mask_id(external_userid), "msgtype": msgtype}, debug_only=True)

    if not msgid:
        log("msg.missing_msgid_skip", {"external_userid": mask_id(external_userid)})
        return

    if is_msg_processed(open_kfid, msgid):
        log("msg.duplicate_skip", {"msgid": msgid})
        return

    # Reserve dedup record first
    if not mark_msg_processed(open_kfid, msgid, external_userid, create_time):
        log("msg.dedup_mark_failed_skip", {"msgid": msgid})
        return
    log("msg.dedup_mark_ok", {"msgid": msgid})

    # Non-text fixed reply
    if msgtype != "text":
        log("msg.non_text", {"msgtype": msgtype, "reply_preview": NON_TEXT_REPLY[:120]})
        if NON_TEXT_REPLY.strip():
            reply = truncate_reply(NON_TEXT_REPLY)
            log(
                "msg.flow",
                {
                    "open_kfid": open_kfid,
                    "msgid": msgid,
                    "external_userid": mask_id(external_userid),
                    "msgtype": msgtype,
                    "dedup_new": True,
                    "path": "non_text_fixed_reply",
                },
            )
            log("reply.final", {"source": "non_text_fixed_reply", "msgid": msgid, "external_userid": mask_id(external_userid), "preview": reply[:200]})
            kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)
        return

    text = (msg.get("text") or {}).get("content") if isinstance(msg.get("text"), dict) else ""
    user_text = (text or "").strip()
    log("msg.text", {"len": len(user_text), "preview": user_text[:200]}, debug_only=True)

    if not user_text:
        log("msg.empty_text_skip", {"msgid": msgid})
        return

    reply, reply_source = reply_for_text(
        open_kfid=open_kfid,
        external_userid=external_userid,
        user_text=user_text,
        msgid=msgid,
    )
    if not reply:
        log("reply.empty_skip", {"msgid": msgid})
        return

    log("reply.final", {"source": reply_source, "msgid": msgid, "external_userid": mask_id(external_userid), "preview": reply[:200]})
    kf_send_text(open_kfid=open_kfid, external_userid=external_userid, content=reply)


def _mark_extra_msgids_processed(open_kfid: str, extra_msgids: list[str], external_userid: str) -> None:
    """
    Mark coalesced-but-not-replied msgids as processed (no reply/body writes).
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
        log(
            "msg.dedup_mark_coalesced",
            {"open_kfid": mask_id(open_kfid), "msgid": msgid, "external_userid": mask_id(external_userid), "ok": ok},
            debug_only=True,
        )


def _pick_latest_customer_message_from_sync(open_kfid: str, token: str, cursor: str | None) -> tuple[dict | None, str | None]:
    """
    Cold-start guard: keep pushing cursor but only pick the latest customer message.
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
            log("wecom.kf_sync_msg.failed", {"errcode": sync_resp.get("errcode"), "errmsg": sync_resp.get("errmsg")})
            return None, None

        msg_list: list[dict] = sync_resp.get("msg_list") or []
        customer_msgs = _extract_customer_messages(msg_list)
        for m in customer_msgs:
            seq += 1
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
    Process one客服 sync_msg:
    - read cursor from DB
    - call wecom kf_sync_msg
    - handle all customer messages in time order
    - then persist next_cursor back to DB
    """

    raw_cursor = get_kf_cursor(open_kfid)
    cursor = raw_cursor or None
    cursor_missing = raw_cursor is None

    # Cold start guard
    if cursor_missing:
        log("kf_cursor.missing_cold_start_guard", {"open_kfid": open_kfid})
        latest_msg, final_cursor = _pick_latest_customer_message_from_sync(open_kfid=open_kfid, token=token, cursor=None)
        if final_cursor:
            set_kf_cursor(open_kfid, final_cursor)

        if not isinstance(latest_msg, dict):
            log("cold_start.no_customer_message", {"open_kfid": open_kfid})
            return

        try:
            st = int(latest_msg.get("send_time") or 0)
        except Exception:
            st = 0
        age_s = now_ts() - st
        if st <= 0 or age_s > 5 * 60:
            log("cold_start.latest_too_old_skip_reply", {"open_kfid": open_kfid, "latest_send_time": st, "age_s": age_s})
            return

        log(
            "cold_start.will_reply_latest_only",
            {"open_kfid": open_kfid, "msgid": latest_msg.get("msgid"), "external_userid": mask_id(str(latest_msg.get("external_userid") or ""))},
        )
        try:
            _handle_one_customer_message(open_kfid, latest_msg)
        except Exception:
            log("msg.handle_failed", {"open_kfid": open_kfid, "msgid": latest_msg.get("msgid")})
            log("msg.handle_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return

    sync_resp = kf_sync_msg(open_kfid=open_kfid, token=token, cursor=cursor, limit=100)
    if str(sync_resp.get("errcode")) != "0":
        log("wecom.kf_sync_msg.failed", {"errcode": sync_resp.get("errcode"), "errmsg": sync_resp.get("errmsg")})
        return

    msg_list: list[dict] = sync_resp.get("msg_list") or []
    log("wecom.kf_sync_msg.ok", {"open_kfid": open_kfid, "msg_list_len": len(msg_list), "has_more": sync_resp.get("has_more")})

    customer_msgs = _extract_customer_messages(msg_list)
    log("msg.batch_customer_messages", {"open_kfid": open_kfid, "total_msg_list_len": len(msg_list), "customer_msg_count": len(customer_msgs)}, debug_only=True)

    if not customer_msgs:
        log("msg.none_customer_message", {"open_kfid": open_kfid})
        next_cursor = sync_resp.get("next_cursor") or ""
        if next_cursor:
            set_kf_cursor(open_kfid, next_cursor)
            log("kf_cursor.updated_no_customer", {"open_kfid": open_kfid}, debug_only=True)
        return

    coalesced_items = _coalesce_customer_messages(customer_msgs)
    log(
        "msg.batch_coalesced",
        {"open_kfid": open_kfid, "customer_msg_count": len(customer_msgs), "coalesced_count": len(coalesced_items)},
        debug_only=True,
    )

    for item in coalesced_items:
        msg = item.get("msg") if isinstance(item, dict) else None
        extra_msgids = item.get("extra_msgids") if isinstance(item, dict) else None
        if not isinstance(msg, dict):
            continue
        try:
            _handle_one_customer_message(open_kfid, msg)
            external_userid = str(msg.get("external_userid") or "")
            if isinstance(extra_msgids, list) and external_userid:
                _mark_extra_msgids_processed(open_kfid, extra_msgids, external_userid)
        except Exception:
            log("msg.handle_failed", {"open_kfid": open_kfid, "msgid": msg.get("msgid") if isinstance(msg, dict) else None})
            log("msg.handle_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)

    next_cursor = sync_resp.get("next_cursor") or ""
    if next_cursor:
        set_kf_cursor(open_kfid, next_cursor)
        log("kf_cursor.updated", {"open_kfid": open_kfid}, debug_only=True)

    maybe_cleanup_old_data()

