from __future__ import annotations

import sys

from app.core.logging import log
from app.core.service import reply_for_text
from app.core.utils import now_ts


def _read_user_line() -> str:
    """
    交互式 TTY 上，内核行编辑 / GNU readline 对 UTF-8 与全角提示符常出现退格与光标错位
    （表现为行首少删一个字或内容截断）。prompt_toolkit 按 Unicode 处理输入与退格。
    """
    if not sys.stdin.isatty():
        line = sys.stdin.readline()
        if line == "":
            raise EOFError
        return line.rstrip("\r\n").strip()

    try:
        from prompt_toolkit.shortcuts import prompt as pt_prompt
    except ImportError:
        # 无依赖时仅用 ASCII 提示符，避免把全角放进 input/readline 的 prompt
        return input("> ").strip()

    return (pt_prompt("你> ") or "").strip()


def run_terminal_cli() -> None:
    """
    Terminal channel driver.
    Reuses the same smart-customer-service core logic.
    """

    open_kfid = "terminal_cli"
    external_userid = "terminal_user"

    print("Terminal 客服模式已启动，输入 exit 退出。")
    while True:
        try:
            user_text = _read_user_line()
        except EOFError:
            print("")
            break
        except KeyboardInterrupt:
            print("")
            break

        if not user_text:
            continue
        if user_text.lower() in {"exit", "quit", "q"}:
            break

        msgid = f"cli-{now_ts()}"
        reply, source = reply_for_text(
            open_kfid=open_kfid,
            external_userid=external_userid,
            user_text=user_text,
            msgid=msgid,
        )
        if not reply:
            reply = "已收到你的问题，我先帮你记录。"
            source = "empty_fallback"
        log("reply.final", {"source": source, "msgid": msgid, "external_userid": external_userid, "preview": reply[:200]})
        print(f"客服> {reply}")

