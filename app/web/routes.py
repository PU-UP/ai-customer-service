from __future__ import annotations

import hmac
import json
import os
from typing import Any

from flask import Blueprint, Response, abort, make_response, redirect, request, send_from_directory, url_for

from app.config import CLUB_PROFILE_PATH, FAQ_PATH, SCENARIO, scenarios_local_write_dir, SYSTEM_PROMPT_PATH
from app.core.logging import log
from app.core.policy import reload_assets


bp = Blueprint("admin_web", __name__, url_prefix="/admin")


def _admin_token() -> str:
    return (os.getenv("ADMIN_TOKEN", "") or "").strip()


def _get_request_token() -> str:
    # Prefer explicit header; allow Bearer for convenience.
    t = (request.headers.get("X-Admin-Token", "") or "").strip()
    if t:
        return t
    c = (request.cookies.get("admin_token", "") or "").strip()
    if c:
        return c
    auth = (request.headers.get("Authorization", "") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def _require_admin() -> None:
    tok = _admin_token()
    if not tok:
        abort(403)
    provided = _get_request_token()
    if not provided or not hmac.compare_digest(provided, tok):
        abort(401)


def _json(data: Any, status: int = 200) -> Response:
    return Response(json.dumps(data, ensure_ascii=False), status=status, mimetype="application/json")


def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def _read_json(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _ensure_scenarios_local_dir() -> str:
    target = scenarios_local_write_dir()
    os.makedirs(target, exist_ok=True)
    return target


def _atomic_write_text(path: str, content: str) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def _atomic_write_json(path: str, obj: Any) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _set_admin_cookie(resp: Response) -> Response:
    tok = _get_request_token()
    # Value is the raw token; HttpOnly prevents JS access.
    resp.set_cookie("admin_token", tok, httponly=True, samesite="Lax")
    return resp


@bp.route("/login", methods=["GET"])
def admin_login_page() -> Response:
    tok = _admin_token()
    if not tok:
        return make_response("ADMIN_TOKEN is not set", 403)
    ui_dir = os.path.join(os.path.dirname(__file__), "ui")
    return send_from_directory(ui_dir, "login.html")


@bp.route("/login", methods=["POST"])
def admin_login_post() -> Response:
    tok = _admin_token()
    if not tok:
        abort(403)
    provided = (request.form.get("token", "") or "").strip()
    if not provided or not hmac.compare_digest(provided, tok):
        return redirect(url_for("admin_web.admin_login_page"))
    resp = redirect(url_for("admin_web.admin_index"))
    resp.set_cookie("admin_token", provided, httponly=True, samesite="Lax")
    return resp


@bp.route("/logout", methods=["POST"])
def admin_logout() -> Response:
    resp = redirect(url_for("admin_web.admin_login_page"))
    resp.set_cookie("admin_token", "", expires=0)
    return resp


@bp.route("/", methods=["GET"])
def admin_index() -> Response:
    # If user hasn't logged in yet, send them to the login page.
    if not _get_request_token():
        return redirect(url_for("admin_web.admin_login_page"))
    _require_admin()
    ui_dir = os.path.join(os.path.dirname(__file__), "ui")
    return send_from_directory(ui_dir, "index.html")


@bp.route("/ui/<path:filename>", methods=["GET"])
def admin_ui_static(filename: str) -> Response:
    if not _get_request_token():
        return redirect(url_for("admin_web.admin_login_page"))
    _require_admin()
    ui_dir = os.path.join(os.path.dirname(__file__), "ui")
    return send_from_directory(ui_dir, filename)


@bp.route("/api/assets", methods=["GET"])
def api_assets() -> Response:
    _require_admin()
    data = {
        "scenario": SCENARIO,
        "paths": {
            "club_profile": CLUB_PROFILE_PATH,
            "faq": FAQ_PATH,
            "system_prompt": SYSTEM_PROMPT_PATH,
        },
        "club_profile": _read_json(CLUB_PROFILE_PATH) or {},
        "faq": _read_json(FAQ_PATH) or [],
        "system_prompt": _read_text(SYSTEM_PROMPT_PATH),
    }
    return _json(data)


@bp.route("/api/club_profile", methods=["PUT"])
def api_put_club_profile() -> Response:
    _require_admin()
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return _json({"ok": False, "error": "club_profile must be a JSON object"}, status=400)
    target_dir = _ensure_scenarios_local_dir()
    out_path = os.path.join(target_dir, "club_profile.json")
    _atomic_write_json(out_path, payload)
    reload_assets()
    log("admin.assets.saved", {"type": "club_profile", "scenario": SCENARIO, "path": out_path})
    return _json({"ok": True, "path": out_path})


def _validate_faq(items: Any) -> str | None:
    if not isinstance(items, list):
        return "faq must be a JSON array"
    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            return f"faq[{idx}] must be an object"
        keywords = it.get("keywords")
        answer = it.get("answer")
        if not isinstance(keywords, list) or not all(isinstance(k, str) and k.strip() for k in keywords):
            return f"faq[{idx}].keywords must be an array of non-empty strings"
        if not isinstance(answer, str) or not answer.strip():
            return f"faq[{idx}].answer must be a non-empty string"
    return None


@bp.route("/api/faq", methods=["PUT"])
def api_put_faq() -> Response:
    _require_admin()
    payload = request.get_json(silent=True)
    err = _validate_faq(payload)
    if err:
        return _json({"ok": False, "error": err}, status=400)
    target_dir = _ensure_scenarios_local_dir()
    out_path = os.path.join(target_dir, "faq.json")
    _atomic_write_json(out_path, payload)
    reload_assets()
    log("admin.assets.saved", {"type": "faq", "scenario": SCENARIO, "path": out_path})
    return _json({"ok": True, "path": out_path})


@bp.route("/api/system_prompt", methods=["PUT"])
def api_put_system_prompt() -> Response:
    _require_admin()
    if request.is_json:
        body = request.get_json(silent=True)
        content = body.get("text") if isinstance(body, dict) else None
    else:
        content = request.data.decode("utf-8", errors="ignore")
    if not isinstance(content, str):
        return _json({"ok": False, "error": "system_prompt must be plain text or JSON {text}"}, status=400)
    target_dir = _ensure_scenarios_local_dir()
    out_path = os.path.join(target_dir, "system_prompt.txt")
    _atomic_write_text(out_path, content)
    reload_assets()
    log("admin.assets.saved", {"type": "system_prompt", "scenario": SCENARIO, "path": out_path})
    return _json({"ok": True, "path": out_path})

