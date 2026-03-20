from __future__ import annotations

from app.channels.routes import create_app
from app.channels.terminal_cli import run_terminal_cli
from app.config import (
    CHANNEL_DRIVER,
    CLUB_PROFILE_PATH,
    CORP_ID,
    CORP_SECRET,
    DEBUG,
    FAQ_PATH,
    LLM_TIMEOUT_SECONDS,
    LLM_API_KEY,
    LLM_BASE_URL,
    LLM_MODEL,
    SCENARIO,
    SQLITE_PATH,
    SYSTEM_PROMPT_PATH,
    TOKEN,
    ENCODING_AES_KEY,
    WORKER_COUNT,
    CONV_HISTORY_LIMIT,
)
from app.core.logging import log
from app.core.policy import load_assets
from app.db.sqlite_store import cleanup_old_data, init_db
from app.core.worker import start_worker_once


def _print_config_summary() -> None:
    log(
        "config.summary",
        {
            "DEBUG": DEBUG,
            "TOKEN": bool(TOKEN),
            "ENCODING_AES_KEY": bool(ENCODING_AES_KEY),
            "CORP_ID": CORP_ID,
            "CORP_SECRET": bool(CORP_SECRET),
            "LLM_API_KEY": bool(LLM_API_KEY),
            "LLM_BASE_URL": LLM_BASE_URL,
            "LLM_MODEL": LLM_MODEL,
            "CHANNEL_DRIVER": CHANNEL_DRIVER,
            "SCENARIO": SCENARIO,
            "SQLITE_PATH": SQLITE_PATH,
            "CLUB_PROFILE_PATH": CLUB_PROFILE_PATH,
            "FAQ_PATH": FAQ_PATH,
            "SYSTEM_PROMPT_PATH": SYSTEM_PROMPT_PATH,
            "LLM_TIMEOUT_SECONDS": LLM_TIMEOUT_SECONDS,
            "CONV_HISTORY_LIMIT": CONV_HISTORY_LIMIT,
            "WORKER_COUNT": WORKER_COUNT,
        },
    )


def run(host: str = "0.0.0.0", port: int = 5000) -> None:
    _print_config_summary()

    load_assets(club_profile_path=CLUB_PROFILE_PATH, faq_path=FAQ_PATH, system_prompt_path=SYSTEM_PROMPT_PATH)
    from app.core.policy import get_club_profile, get_faq_items  # local import to avoid circular deps

    cp = get_club_profile()
    faq = get_faq_items()
    log("config.loaded", {"club_profile_keys": sorted(list(cp.keys())), "faq_items": len(faq)})

    init_db(SQLITE_PATH)
    cleanup_old_data()
    if CHANNEL_DRIVER == "wecom_webhook":
        start_worker_once()
        create_app().run(host=host, port=port)
        return
    if CHANNEL_DRIVER == "terminal_cli":
        run_terminal_cli()
        return
    raise ValueError(f"Unsupported CHANNEL_DRIVER: {CHANNEL_DRIVER}")
