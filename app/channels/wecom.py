from __future__ import annotations

import base64
import hashlib
import hmac
import json
import struct
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

from Crypto.Cipher import AES

from app.config import CORP_ID, CORP_SECRET, DEBUG
from app.core.logging import log
from app.core.utils import mask_id
from app.core.utils import now_ts

_ACCESS_TOKEN_CACHE = {"access_token": "", "expires_at": 0}


def http_get_json(url: str, timeout: int = 10) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    return json.loads(body) if body else {}


def http_post_json(url: str, payload: dict, timeout: int = 10) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    return json.loads(body) if body else {}


def get_access_token() -> str:
    """
    Get access_token by corpid + corpsecret (in-memory cache).
    """

    now = now_ts()
    if _ACCESS_TOKEN_CACHE["access_token"] and now < int(_ACCESS_TOKEN_CACHE["expires_at"]) - 60:
        return _ACCESS_TOKEN_CACHE["access_token"]

    if not CORP_SECRET:
        log("wecom.gettoken.missing_secret", {"missing": "CORP_SECRET"})
        return ""

    url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={CORP_ID}&corpsecret={CORP_SECRET}"
    resp = http_get_json(url)
    log(
        "wecom.gettoken",
        {"errcode": resp.get("errcode"), "errmsg": resp.get("errmsg"), "expires_in": resp.get("expires_in")},
    )
    access_token = resp.get("access_token", "")
    expires_in = int(resp.get("expires_in") or 0)
    if access_token and expires_in > 0:
        _ACCESS_TOKEN_CACHE["access_token"] = access_token
        _ACCESS_TOKEN_CACHE["expires_at"] = now + expires_in
    return access_token


def kf_sync_msg(open_kfid: str, token: str, cursor: str | None = None, limit: int = 100) -> dict[str, Any]:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/sync_msg?access_token={access_token}"
    payload: dict[str, Any] = {"open_kfid": open_kfid, "token": token, "limit": limit}
    if cursor:
        payload["cursor"] = cursor
    log("wecom.kf_sync_msg.request", {"open_kfid": open_kfid, "cursor": cursor, "limit": limit}, debug_only=not DEBUG)
    log("wecom.kf_sync_msg.request.detail", payload, debug_only=True)
    resp = http_post_json(url, payload, timeout=10)
    log(
        "wecom.kf_sync_msg.response",
        {
            "errcode": resp.get("errcode"),
            "errmsg": resp.get("errmsg"),
            "has_more": resp.get("has_more"),
            "msg_list_len": len(resp.get("msg_list") or []),
        },
    )
    log("wecom.kf_sync_msg.response.cursor", {"next_cursor": resp.get("next_cursor")}, debug_only=not DEBUG)
    return resp


def kf_send_text(open_kfid: str, external_userid: str, content: str) -> dict[str, Any]:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/send_msg?access_token={access_token}"
    payload = {
        "touser": external_userid,
        "open_kfid": open_kfid,
        "msgtype": "text",
        "text": {"content": content},
    }
    log(
        "wecom.kf_send_msg.request",
        {"touser": mask_id(external_userid), "open_kfid": open_kfid, "msgtype": "text", "content_preview": (content or "")[:200]},
    )
    resp = http_post_json(url, payload, timeout=10)
    log("wecom.kf_send_msg.response", {"errcode": resp.get("errcode"), "errmsg": resp.get("errmsg"), "msgid": resp.get("msgid")})
    return resp


class PKCS7Encoder:
    """PKCS7 padding used by enterprise wecom messages."""

    @staticmethod
    def encode(plaintext: bytes) -> bytes:
        block_size = 32
        pad_len = block_size - (len(plaintext) % block_size)
        if pad_len == 0:
            pad_len = block_size
        return plaintext + bytes([pad_len]) * pad_len

    @staticmethod
    def decode(decrypted: bytes) -> bytes:
        pad = decrypted[-1]
        if pad < 1 or pad > 32:
            pad = 0
        return decrypted[:-pad]


def sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    """
    WeCom signature:
    Sort token/timestamp/nonce/encrypt then SHA1.
    """

    arr = [token, timestamp, nonce, encrypt]
    arr.sort()
    raw = "".join(arr)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def decrypt_wecom_message(encoding_aes_key: str, corp_id: str, encrypt_text: str) -> str:
    """
    Decrypt wecom message / echostr.
    """

    aes_key = base64.b64decode(encoding_aes_key + "=")
    iv = aes_key[:16]

    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
    encrypted = base64.b64decode(encrypt_text)
    decrypted = cipher.decrypt(encrypted)
    decrypted = PKCS7Encoder.decode(decrypted)

    # 企业微信消息结构：
    # 16字节随机串 + 4字节消息长度 + 明文 + receiveid(corpid)
    content = decrypted[16:]
    xml_len = struct.unpack(">I", content[:4])[0]
    xml_content = content[4 : 4 + xml_len]
    from_corpid = content[4 + xml_len :].decode("utf-8")

    if from_corpid != corp_id:
        raise ValueError(f"CorpID 不匹配: {from_corpid} != {corp_id}")

    return xml_content.decode("utf-8")


def parse_wecom_plain_xml(plain_xml: str) -> dict[str, Any]:
    """
    Parse decrypted wecom XML into a simple key/value dict.
    """

    root = ET.fromstring(plain_xml)
    out: dict[str, Any] = {}
    for child in list(root):
        tag = child.tag
        text = (child.text or "").strip()
        if text:
            out[tag] = text
    return out


def extract_encrypt_from_xml(xml_text: str) -> str:
    """
    Extract <Encrypt> from XML.
    """

    root = ET.fromstring(xml_text)
    encrypt_node = root.find("Encrypt")
    if encrypt_node is None or encrypt_node.text is None or not encrypt_node.text.strip():
        raise ValueError("XML 中未找到 Encrypt 字段或内容为空")
    return encrypt_node.text.strip()


def kf_customer_batchget(external_userid_list: list[str], need_enter_session_context: int = 0) -> dict[str, Any]:
    access_token = get_access_token()
    if not access_token:
        return {"errcode": -1, "errmsg": "missing access_token"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/customer/batchget?access_token={access_token}"
    payload = {
        "external_userid_list": [x for x in external_userid_list if (x or "").strip()],
        "need_enter_session_context": int(need_enter_session_context or 0),
    }
    log(
        "wecom.kf_customer.batchget.request",
        {"count": len(payload["external_userid_list"]), "need_esc": payload["need_enter_session_context"]},
        debug_only=not DEBUG,
    )
    log("wecom.kf_customer.batchget.request.detail", payload, debug_only=True)
    resp = http_post_json(url, payload, timeout=10)
    log(
        "wecom.kf_customer.batchget.response",
        {
            "errcode": resp.get("errcode"),
            "errmsg": resp.get("errmsg"),
            "customer_list_len": len(resp.get("customer_list") or []),
            "invalid_external_userid_len": len(resp.get("invalid_external_userid") or []),
        },
    )
    return resp


def verify_signature(token: str, timestamp: str, nonce: str, encrypt: str, msg_signature: str) -> bool:
    local_signature = sha1_signature(token, timestamp, nonce, encrypt)
    return hmac.compare_digest(local_signature, msg_signature)

