from __future__ import annotations

from app.core.logging import log
from app.core.service import reply_for_text
from app.core.utils import now_ts


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
            user_text = input("你> ").strip()
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

