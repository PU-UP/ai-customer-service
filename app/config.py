from __future__ import annotations

import os


def _project_root() -> str:
    # app/config.py -> project_root/
    return os.path.dirname(os.path.dirname(__file__))


def load_dotenv(dotenv_path: str) -> None:
    """
    Minimal .env loader:
    - Parse lines like KEY=VALUE
    - Ignore empty lines and comments
    - Do not override existing environment variables
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
        return


PROJECT_ROOT = _project_root()
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def _pick_first_existing_path(candidates: list[str], default_to_first: bool = True) -> str:
    for p in candidates:
        if os.path.exists(p):
            return p
    return candidates[0] if candidates and default_to_first else ""


SCENARIO = (os.getenv("SCENARIO", "") or "tennis_club").strip() or "tennis_club"
CHANNEL_DRIVER = (os.getenv("CHANNEL_DRIVER", "") or "wecom_webhook").strip() or "wecom_webhook"

# Prefer local (gitignored) scenarios; fall back to repo examples.
SCENARIOS_LOCAL_DIR = os.getenv("SCENARIOS_LOCAL_DIR", "").strip() or os.path.join(PROJECT_ROOT, "app", "scenarios_local")
SCENARIOS_REPO_DIR = os.path.join(PROJECT_ROOT, "app", "scenarios")

def _is_flat_scenarios_dir(base_dir: str) -> bool:
    """
    Treat SCENARIOS_LOCAL_DIR as a "flat" assets directory when it directly contains
    the three asset files (no <SCENARIO>/ subdirectory layout).
    """
    if not base_dir:
        return False
    return any(
        os.path.exists(os.path.join(base_dir, fn))
        for fn in ("club_profile.json", "faq.json", "system_prompt.txt")
    )


def _scenario_candidates(filename: str) -> list[str]:
    # If the local dir is "flat", prefer <SCENARIOS_LOCAL_DIR>/<filename>.
    local_first = (
        [os.path.join(SCENARIOS_LOCAL_DIR, filename)]
        if _is_flat_scenarios_dir(SCENARIOS_LOCAL_DIR)
        else [os.path.join(SCENARIOS_LOCAL_DIR, SCENARIO, filename)]
    )
    return [
        *local_first,
        os.path.join(SCENARIOS_REPO_DIR, SCENARIO, filename),
    ]


def _scenario_path(filename: str) -> str:
    # Keep old helper name for readability; now it returns the first existing path.
    return _pick_first_existing_path(_scenario_candidates(filename))


TOKEN = os.getenv("TOKEN", "")
ENCODING_AES_KEY = os.getenv("ENCODING_AES_KEY", "")
CORP_ID = os.getenv("CORP_ID", "")
CORP_SECRET = os.getenv("CORP_SECRET", "")

LLM_API_KEY = (
    os.getenv("LLM_API_KEY", "").strip()
    or os.getenv("OPENAI_API_KEY", "").strip()
    or os.getenv("ALI_API_KEY", "").strip()
    or os.getenv("DASHSCOPE_API_KEY", "").strip()
)

# Keep backward compatible aliases (existing code imports ALI_*)
LLM_BASE_URL = (
    os.getenv("LLM_BASE_URL", "").strip()
    or os.getenv("OPENAI_BASE_URL", "").strip()
    or os.getenv("ALI_BASE_URL", "").strip()
    or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)

LLM_MODEL = (
    os.getenv("LLM_MODEL", "").strip()
    or os.getenv("OPENAI_MODEL", "").strip()
    or os.getenv("ALI_MODEL", "").strip()
    or "qwen-plus"
)

ALI_API_KEY = LLM_API_KEY
ALI_BASE_URL = LLM_BASE_URL
ALI_MODEL = LLM_MODEL

DEBUG = str(os.getenv("DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

SQLITE_PATH = (
    os.getenv("SQLITE_PATH", "").strip()
    or _pick_first_existing_path(
        [
            os.path.join(PROJECT_ROOT, "app", "data", "ai_customer_service.db"),
            os.path.join(PROJECT_ROOT, "ai_customer_service.db"),
        ]
    )
)

_club_profile_default = _pick_first_existing_path(
    [
        *_scenario_candidates("club_profile.json"),
        os.path.join(PROJECT_ROOT, "app", "club_profile.json"),
        os.path.join(PROJECT_ROOT, "club_profile.json"),
    ]
)
CLUB_PROFILE_PATH = os.getenv("CLUB_PROFILE_PATH", "").strip() or _club_profile_default

_faq_default = _pick_first_existing_path(
    [
        *_scenario_candidates("faq.json"),
        os.path.join(PROJECT_ROOT, "app", "faq.json"),
        os.path.join(PROJECT_ROOT, "faq.json"),
    ]
)
FAQ_PATH = os.getenv("FAQ_PATH", "").strip() or _faq_default

SYSTEM_PROMPT_PATH = os.getenv("SYSTEM_PROMPT_PATH", "").strip() or _pick_first_existing_path(
    [
        *_scenario_candidates("system_prompt.txt"),
        os.path.join(PROJECT_ROOT, "system_prompts.txt"),
        os.path.join(PROJECT_ROOT, "prompts", "system_prompt.txt"),
    ]
)


def scenarios_local_write_dir() -> str:
    """
    Where admin UI writes assets:
    - If SCENARIOS_LOCAL_DIR is flat (contains asset files directly), write into it.
    - Otherwise, write into SCENARIOS_LOCAL_DIR/<SCENARIO>/.
    """
    base = (SCENARIOS_LOCAL_DIR or "").strip()
    if _is_flat_scenarios_dir(base):
        return base
    return os.path.join(base, SCENARIO)

WORKER_COUNT = int(os.getenv("WORKER_COUNT", "1") or "1")
if WORKER_COUNT <= 0:
    WORKER_COUNT = 1

LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "15") or "15")
CONV_HISTORY_LIMIT = int(os.getenv("CONV_HISTORY_LIMIT", "8") or "8")
CONV_HISTORY_MAX_CHARS = int(os.getenv("CONV_HISTORY_MAX_CHARS", "2000") or "2000")
REPLY_TRUNCATE_MAX_CHARS = int(os.getenv("REPLY_TRUNCATE_MAX_CHARS", "0") or "0")

CUSTOMER_PROFILE_REFRESH_SECONDS = int(os.getenv("CUSTOMER_PROFILE_REFRESH_SECONDS", "86400") or "86400")
