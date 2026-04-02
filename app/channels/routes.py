from __future__ import annotations

import hmac
import traceback

from flask import Flask, request

from app.config import CORP_ID, ENCODING_AES_KEY, WECOM_WEBHOOK_TOKEN
from app.core.logging import log, log_separator, set_log_trace
from app.core.utils import now_ts
from app.core.worker import enqueue_kf_msg_or_event
from app.channels.wecom import decrypt_wecom_message, extract_encrypt_from_xml, parse_wecom_plain_xml, sha1_signature


app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return "server is running"


@app.route("/wechat", methods=["GET"])
def wechat_verify():
    """
    WeCom URL verification endpoint.
    """

    msg_signature = request.args.get("msg_signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    echostr = request.args.get("echostr", "")

    if not any([msg_signature, timestamp, nonce, echostr]):
        return "wechat get ok"
    if not all([msg_signature, timestamp, nonce, echostr]):
        return "missing params", 400

    try:
        local_signature = sha1_signature(WECOM_WEBHOOK_TOKEN, timestamp, nonce, echostr)
        if not hmac.compare_digest(local_signature, msg_signature):
            return "signature error", 403
        plain_text = decrypt_wecom_message(ENCODING_AES_KEY, CORP_ID, echostr)
        log("wecom.verify_ok", {"plain_preview": plain_text[:200]}, debug_only=True)
        return plain_text
    except Exception as e:
        log("wecom.verify_failed", {"err": repr(e)})
        log("wecom.verify_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return "verify failed", 500


@app.route("/wechat", methods=["POST"])
def wechat_callback():
    """
    WeCom message callback:
    - webhook does signature+decrypt+parse then quickly enqueue worker task
    - LLM + sync processing is done in background worker
    """

    msg_signature = request.args.get("msg_signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    if not all([msg_signature, timestamp, nonce]):
        return "missing params", 400

    raw_xml = request.data.decode("utf-8", errors="ignore")
    log("wecom.webhook.raw_xml", {"raw_xml": raw_xml}, debug_only=True)

    try:
        trace = f"wb/{now_ts()}-{(nonce[-4:] if nonce else '0000')}"
        set_log_trace(trace)
        log_separator("CALL webhook", {"timestamp": timestamp, "nonce": nonce})

        encrypt_text = extract_encrypt_from_xml(raw_xml)
        local_signature = sha1_signature(WECOM_WEBHOOK_TOKEN, timestamp, nonce, encrypt_text)
        if not hmac.compare_digest(local_signature, msg_signature):
            log("wecom.webhook.signature_failed", {})
            return "signature error", 403

        plain_xml = decrypt_wecom_message(ENCODING_AES_KEY, CORP_ID, encrypt_text)
        log("wecom.webhook.decrypted", {"plain_len": len(plain_xml)})
        log("wecom.webhook.decrypted_xml", {"plain_xml": plain_xml}, debug_only=True)

        parsed = parse_wecom_plain_xml(plain_xml)
        log("wecom.webhook.parsed", {"keys": sorted(list(parsed.keys()))})
        log("wecom.webhook.parsed.detail", {"parsed": parsed}, debug_only=True)

        is_kf_event = parsed.get("Event") == "kf_msg_or_event"
        log("route.webhook", {"hit_kf_event": is_kf_event, "content_len": len(raw_xml)})

        if is_kf_event:
            open_kfid = parsed.get("OpenKfId") or ""
            token = parsed.get("Token") or ""
            log("wecom.webhook.kf_event", {"OpenKfId": open_kfid, "Token_len": len(token)})
            enqueue_kf_msg_or_event(open_kfid, token)
        else:
            log("wecom.webhook.non_kf_event_ack", {})

        return "success"
    except Exception as e:
        log("wecom.webhook.failed", {"err": repr(e)})
        log("wecom.webhook.failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        return "fail", 500
    finally:
        set_log_trace(None)


def create_app() -> Flask:
    # Placeholder for future expansion (local debug / multiple channels).
    try:
        from app.web.routes import bp as admin_bp

        app.register_blueprint(admin_bp)
    except Exception as e:
        # Admin UI is optional; avoid breaking webhook if missing deps/files.
        log("admin.blueprint.register_failed", {"err": repr(e)}, debug_only=True)
    return app

