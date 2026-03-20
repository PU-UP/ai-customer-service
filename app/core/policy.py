from __future__ import annotations

import json
import traceback
from typing import Any

from app.config import SYSTEM_PROMPT_PATH
from app.core.logging import log

_CLUB_PROFILE: dict[str, Any] = {}
_FAQ_ITEMS: list[dict[str, Any]] = []
_SYSTEM_PROMPT_PATH: str = SYSTEM_PROMPT_PATH

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


def _read_json_file(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log("config.json_missing", {"path": path})
        return None
    except Exception:
        log("config.json_read_failed", {"path": path, "err": "read/parse failed"})
        log("config.json_read_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
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
    template = ""
    try:
        if _SYSTEM_PROMPT_PATH and os.path.exists(_SYSTEM_PROMPT_PATH):
            template = load_prompt_template(_SYSTEM_PROMPT_PATH)
        else:
            # Low-risk compatibility: avoid crashing when template is missing.
            log("prompt.template_missing_fallback", {"path": _SYSTEM_PROMPT_PATH}, debug_only=True)
            template = "{{club_profile}}"
    except Exception:
        log("prompt.template_read_failed_fallback", {"path": _SYSTEM_PROMPT_PATH})
        log("prompt.template_read_failed_fallback.trace", {"trace": traceback.format_exc()}, debug_only=True)
        template = "{{club_profile}}"

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
    Simple keyword hit:
    - If any keyword appears in user text => hit
    - If multiple hits => prefer longer keyword item, then by appearance order
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


def load_assets(club_profile_path: str, faq_path: str, system_prompt_path: str | None = None) -> None:
    global _CLUB_PROFILE, _FAQ_ITEMS, _SYSTEM_PROMPT_PATH
    _CLUB_PROFILE = load_club_profile(club_profile_path)
    _FAQ_ITEMS = load_faq(faq_path)
    if system_prompt_path:
        _SYSTEM_PROMPT_PATH = system_prompt_path


def get_club_profile() -> dict[str, Any]:
    return _CLUB_PROFILE


def get_faq_items() -> list[dict[str, Any]]:
    return _FAQ_ITEMS

