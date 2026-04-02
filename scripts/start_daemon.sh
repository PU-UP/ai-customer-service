#!/usr/bin/env bash
# 在后台启动客服服务，stdout/stderr 追加写入项目根目录 logs/app.log
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p logs
PID_FILE="$ROOT/logs/run.pid"
LOG_FILE="$ROOT/logs/app.log"
BK_FILE="$ROOT/logs/app.log.bk"

# 日志轮转：超过 10 天未修改或超过 100MB 时，备份为 app.log.bk（覆盖旧备份）并清空 app.log
_rotate_log_if_needed() {
  [[ -f "$LOG_FILE" ]] || return 0
  local size_bytes mtime now age_sec
  if stat -c%s "$LOG_FILE" >/dev/null 2>&1; then
    size_bytes="$(stat -c%s "$LOG_FILE")"
    mtime="$(stat -c %Y "$LOG_FILE")"
  else
    size_bytes="$(stat -f%z "$LOG_FILE")"
    mtime="$(stat -f %m "$LOG_FILE")"
  fi
  now="$(date +%s)"
  age_sec=$((now - mtime))
  local max_age=$((10 * 24 * 3600))
  local max_bytes=$((100 * 1024 * 1024))
  if (( size_bytes > max_bytes || age_sec > max_age )); then
    cp -f "$LOG_FILE" "$BK_FILE"
    : >"$LOG_FILE"
    echo "已轮转日志：备份 -> ${BK_FILE}，已清空 ${LOG_FILE}" >&2
  fi
}

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(tr -d ' \n\r\t' <"$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${OLD_PID}" ]] && kill -0 "${OLD_PID}" 2>/dev/null; then
    echo "已在运行（pid ${OLD_PID}）。请先执行 scripts/stop_daemon.sh。" >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi

_rotate_log_if_needed

# 优先使用项目内虚拟环境
if [[ -x "$ROOT/.venv/bin/python3" ]]; then
  PY="$ROOT/.venv/bin/python3"
elif [[ -x "$ROOT/venv/bin/python3" ]]; then
  PY="$ROOT/venv/bin/python3"
else
  PY="python3"
fi

nohup "$PY" run_customer_service.py >>"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
echo "已启动 pid $(cat "$PID_FILE")，日志：$LOG_FILE"
