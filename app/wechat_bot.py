from __future__ import annotations

import os
import sys

# When executing `uv run app/wechat_bot.py`, python's sys.path[0] points to
# `<project_root>/app`, so `import app.*` would fail (it would look for `<project_root>/app/app`).
# Add project_root to sys.path for stable package imports.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.main import run


if __name__ == "__main__":
    run(host="0.0.0.0", port=5000)

