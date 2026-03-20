from __future__ import annotations

import queue
import threading
import traceback
from typing import Any

from app.config import WORKER_COUNT
from app.core.logging import log, log_separator, set_log_trace
from app.core.message_flow import process_one_kf_message
from app.core.utils import now_ts

_TASK_Q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=100)
_WORKER_STARTED = False


def _enqueue_task(task: dict[str, Any]) -> None:
    try:
        _TASK_Q.put_nowait(task)
        log("worker.task_enqueued", {"type": task.get("type"), "open_kfid": task.get("open_kfid")})
    except queue.Full:
        log("worker.queue_full_drop", {"type": task.get("type")})


def enqueue_kf_msg_or_event(open_kfid: str, token: str) -> None:
    _enqueue_task({"type": "kf_msg_or_event", "open_kfid": open_kfid, "token": token})


def _worker_loop() -> None:
    log("worker.started", {})
    while True:
        task = _TASK_Q.get()
        try:
            t = task.get("type")
            if t == "kf_msg_or_event":
                open_kfid = str(task.get("open_kfid") or "")
                token = str(task.get("token") or "")
                trace = f"wk/{now_ts()}-{(open_kfid[-6:] if open_kfid else 'nokf')}"
                set_log_trace(trace)
                log_separator("CALL worker.task", {"type": t, "open_kfid": open_kfid, "token_len": len(token)})
                log("worker.begin_task", {"type": t, "open_kfid": open_kfid, "token_len": len(token)})
                if open_kfid and token:
                    lk = _get_open_kfid_lock(open_kfid)
                    with lk:
                        process_one_kf_message(open_kfid=open_kfid, token=token)
                else:
                    log("worker.task_missing_fields_skip", {"type": t})
            else:
                log("worker.unknown_task_skip", {"type": t})
        except Exception:
            log("worker.task_failed", {"err": "exception"})
            log("worker.task_failed.trace", {"trace": traceback.format_exc()}, debug_only=True)
        finally:
            set_log_trace(None)
            _TASK_Q.task_done()


_OPEN_KFID_LOCKS: dict[str, threading.Lock] = {}
_OPEN_KFID_LOCKS_GUARD = threading.Lock()
_EMPTY_OPEN_KFID_LOCK = threading.Lock()


def _get_open_kfid_lock(open_kfid: str) -> threading.Lock:
    if not open_kfid:
        return _EMPTY_OPEN_KFID_LOCK
    with _OPEN_KFID_LOCKS_GUARD:
        lk = _OPEN_KFID_LOCKS.get(open_kfid)
        if lk is None:
            lk = threading.Lock()
            _OPEN_KFID_LOCKS[open_kfid] = lk
        return lk


def start_worker_once() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    for i in range(WORKER_COUNT):
        th = threading.Thread(target=_worker_loop, name=f"kf-worker-{i+1}", daemon=True)
        th.start()
    _WORKER_STARTED = True

