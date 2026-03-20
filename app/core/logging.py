from __future__ import annotations

import threading
import time
from typing import Any

from app.config import DEBUG
from app.core.utils import fmt_preview, now_ts


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


def _now_clock() -> str:
    # 形如 15:27:35，便于扫日志
    return time.strftime("%H:%M:%S", time.localtime())


def _log_prefix() -> str:
    trace = _get_log_trace()
    if trace:
        return f"[{_now_clock()}][{trace}]"
    return f"[{_now_clock()}]"


def _log_separator(title: str, kv: dict[str, Any] | None = None) -> None:
    """
    Print separator block at webhook / worker task / single message flow start.
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
    Unified logger:
    - DEBUG=1 -> print structured data
    - Non-DEBUG -> print concise summaries (debug_only logs suppressed)
    """

    if debug_only and not DEBUG:
        return
    if data is None:
        data = {}

    if DEBUG:
        try:
            base = {"ts": now_ts(), "event": event}
            base.update(data)
            base["_clock"] = _now_clock()
            trace = _get_log_trace()
            if trace:
                base["_trace"] = trace
            print(base)
        except Exception:
            print(f"{_log_prefix()} [{event}]")
        return

    try:
        if event == "llm.context":
            lines: list[str] = []
            lines.append(f"user={data.get('external_userid')} msgid={data.get('msgid')} model={data.get('model')}")
            history = data.get("history_messages") or []
            lines.append(f"history_len={len(history)}")
            for i, m in enumerate(history, 1):
                role = m.get("role")
                content = m.get("content")
                lines.append(f"history[{i}] {role}: {fmt_preview(str(content), 140)}")
            lines.append(f"user: {fmt_preview(str(data.get('user_text') or ''), 180)}")
            _print_block("LLM输入摘要", lines)
            return

        if event == "llm.result":
            lines = [
                f"user={data.get('external_userid')} msgid={data.get('msgid')} model={data.get('model')}",
                f"success={data.get('success')} tokens(prompt/comp/total)={data.get('prompt_tokens')}/{data.get('completion_tokens')}/{data.get('total_tokens')}",
                f"answer: {fmt_preview(str(data.get('answer') or ''), 220)}",
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
                    "preview": fmt_preview(str(data.get("preview") or ""), 160),
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


# Re-export internal helpers (used by other modules)
set_log_trace = _set_log_trace
log = _log
log_separator = _log_separator

