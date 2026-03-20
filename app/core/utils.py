from __future__ import annotations

import time


def fmt_preview(s: str, limit: int = 120) -> str:
    t = (s or "").replace("\n", " ").strip()
    if len(t) <= limit:
        return t
    return t[:limit] + "…"


def now_ts() -> int:
    return int(time.time())


def mask_id(s: str, keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep * 2:
        return s
    return s[:keep] + "..." + s[-keep:]

