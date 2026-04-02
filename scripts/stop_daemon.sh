#!/usr/bin/env bash
# 根据 logs/run.pid 停止后台客服进程
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/logs/run.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "未找到 $PID_FILE，可能未通过 start_daemon.sh 启动。" >&2
  exit 1
fi

PID="$(tr -d ' \n\r\t' <"$PID_FILE" 2>/dev/null || true)"
if [[ -z "${PID}" ]]; then
  rm -f "$PID_FILE"
  echo "pid 文件为空，已删除。" >&2
  exit 1
fi

if ! kill -0 "$PID" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "进程 ${PID} 已不存在，已清理陈旧 pid 文件。" >&2
  exit 0
fi

kill "$PID" 2>/dev/null || true
for _ in $(seq 1 30); do
  if ! kill -0 "$PID" 2>/dev/null; then
    break
  fi
  sleep 0.5
done
if kill -0 "$PID" 2>/dev/null; then
  echo "进程未在 15s 内退出，发送 SIGKILL…" >&2
  kill -9 "$PID" 2>/dev/null || true
fi

rm -f "$PID_FILE"
echo "已停止 pid ${PID}"
