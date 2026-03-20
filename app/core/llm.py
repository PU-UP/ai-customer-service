from __future__ import annotations

import traceback
from typing import Any

from app.config import ALI_API_KEY, ALI_BASE_URL, ALI_MODEL, CONV_HISTORY_LIMIT, LLM_TIMEOUT_SECONDS
from app.core.logging import log
from app.core.policy import FALLBACK_TEXT, build_system_prompt, get_club_profile
from app.core.utils import mask_id
from app.db.sqlite_store import append_llm_usage_log, get_recent_conversation_messages

try:
    # OpenAI 兼容 SDK（用于阿里云百炼兼容模式）
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


def _llm_env_ok() -> bool:
    if not OpenAI:
        log("llm.env_missing", {"missing": "openai"})
        return False
    if not ALI_API_KEY:
        log("llm.env_missing", {"missing": "ALI_API_KEY"})
        return False
    if not ALI_BASE_URL:
        log("llm.env_missing", {"missing": "ALI_BASE_URL"})
        return False
    if not ALI_MODEL:
        log("llm.env_missing", {"missing": "ALI_MODEL"})
        return False
    return True


def ask_llm(messages: list[dict[str, str]]) -> tuple[str, dict[str, int], bool]:
    """
    Call LLM and return:
    - assistant text
    - usage tokens dict
    - fallback flag
    """

    usage: dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if not _llm_env_ok():
        return FALLBACK_TEXT, usage, True

    try:
        client = OpenAI(api_key=ALI_API_KEY, base_url=ALI_BASE_URL, timeout=float(LLM_TIMEOUT_SECONDS))
        resp = client.chat.completions.create(model=ALI_MODEL, messages=messages)  # type: ignore[arg-type]
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
            log("llm.usage_parse_failed", {}, debug_only=True)

        if not content:
            log("llm.empty_response", {})
            return FALLBACK_TEXT, usage, True
        return content, usage, False
    except Exception as e:
        log("llm.call_failed", {"err": repr(e)})
        log("llm.call_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return FALLBACK_TEXT, usage, True


def ask_llm_for_user(open_kfid: str, external_userid: str, user_text: str, msgid: str | None = None) -> tuple[str, bool]:
    history = get_recent_conversation_messages(open_kfid, external_userid, limit=CONV_HISTORY_LIMIT)
    system_prompt = build_system_prompt(get_club_profile())

    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_text})

    log(
        "llm.context",
        {
            "open_kfid": mask_id(open_kfid),
            "external_userid": mask_id(external_userid),
            "msgid": msgid or "",
            "model": ALI_MODEL,
            "history_messages": history,
            "user_text": user_text,
        },
    )
    log(
        "llm.will_call",
        {"external_userid": mask_id(external_userid), "history_len": len(history), "timeout_s": LLM_TIMEOUT_SECONDS},
        debug_only=True,
    )

    answer, usage, fallback = ask_llm(messages)
    ok = (not fallback) and bool((answer or "").strip())
    log(
        "llm.result",
        {
            "external_userid": mask_id(external_userid),
            "msgid": msgid or "",
            "model": ALI_MODEL,
            "success": ok,
            "prompt_tokens": int(usage.get("prompt_tokens") or 0),
            "completion_tokens": int(usage.get("completion_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens") or 0),
            "answer": answer or "",
        },
    )
    log(
        "llm.done",
        {"success": ok, "answer_preview": (answer or "")[:200], "fallback": fallback},
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
        # Append failed but should not block answering.
        pass
    return answer, not fallback

