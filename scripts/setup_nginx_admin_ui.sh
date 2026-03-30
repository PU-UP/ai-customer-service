#!/usr/bin/env bash
set -euo pipefail

#
# Configure:
# 1) Nginx default site to serve /admin/ui static files from this repo
# 2) File permissions so nginx (www-data) can read /home/ubuntu/... repo path
#
# Usage:
#   bash scripts/setup_nginx_admin_ui.sh \
#     --server-name "mangosalad.cn www.mangosalad.cn" \
#     --web-root "/var/www/mangosalad" \
#     --repo-root "/home/ubuntu/github/ai-customer-service" \
#     --upstream "http://127.0.0.1:5000"
#
# Optional:
#   --skip-acl       Don't change filesystem ACL
#   --skip-nginx     Don't write nginx config
#

SERVER_NAME="mangosalad.cn www.mangosalad.cn"
WEB_ROOT="/var/www/mangosalad"
REPO_ROOT=""
UPSTREAM="http://127.0.0.1:5000"
SKIP_ACL="0"
SKIP_NGINX="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-name) SERVER_NAME="${2:-}"; shift 2 ;;
    --web-root) WEB_ROOT="${2:-}"; shift 2 ;;
    --repo-root) REPO_ROOT="${2:-}"; shift 2 ;;
    --upstream) UPSTREAM="${2:-}"; shift 2 ;;
    --skip-acl) SKIP_ACL="1"; shift 1 ;;
    --skip-nginx) SKIP_NGINX="1"; shift 1 ;;
    -h|--help)
      sed -n '1,80p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$REPO_ROOT" ]]; then
  # Best-effort infer repo root from script location.
  REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

ADMIN_UI_DIR="$REPO_ROOT/app/web/ui"
TEMPLATE_PATH="$REPO_ROOT/scripts/nginx/default.mangosalad.template"

if [[ "$SKIP_ACL" != "1" ]]; then
  if ! command -v setfacl >/dev/null 2>&1; then
    echo "Installing 'acl' package (for setfacl)..." >&2
    sudo apt-get update
    sudo apt-get install -y acl
  fi

  # Allow nginx worker user to traverse repo path under /home/ubuntu/...
  # Use minimal perms: execute on directories, read+execute on ui dir tree.
  # NOTE: this assumes nginx runs as www-data (Ubuntu default).
  echo "Setting ACL for www-data to read admin UI..." >&2

  # Walk each parent directory and grant traverse.
  # shellcheck disable=SC2046
  for d in $(python3 - <<'PY'
import os, sys
target = os.environ["ADMIN_UI_DIR"]
parts = target.strip("/").split("/")
cur = ""
dirs = []
for p in parts:
    cur += "/" + p
    dirs.append(cur)
print("\n".join(dirs[:-1]))
PY
  ); do
    sudo setfacl -m u:www-data:--x "$d" || true
  done

  sudo setfacl -R -m u:www-data:rx "$ADMIN_UI_DIR"
fi

if [[ "$SKIP_NGINX" != "1" ]]; then
  if [[ ! -f "$TEMPLATE_PATH" ]]; then
    echo "Missing template: $TEMPLATE_PATH" >&2
    exit 1
  fi

  TMP_CONF="$(mktemp)"
  sed \
    -e "s#__SERVER_NAME__#${SERVER_NAME}#g" \
    -e "s#__WEB_ROOT__#${WEB_ROOT}#g" \
    -e "s#__ADMIN_UI_ALIAS_DIR__#${ADMIN_UI_DIR}#g" \
    -e "s#__UPSTREAM__#${UPSTREAM}#g" \
    "$TEMPLATE_PATH" > "$TMP_CONF"

  echo "Writing nginx site config..." >&2
  sudo install -d /etc/nginx/sites-available /etc/nginx/sites-enabled

  if [[ -f /etc/nginx/sites-available/default ]]; then
    ts="$(date +%Y%m%d-%H%M%S)"
    sudo cp -a /etc/nginx/sites-available/default "/etc/nginx/sites-available/default.bak.${ts}"
    echo "Backup: /etc/nginx/sites-available/default.bak.${ts}" >&2
  fi

  sudo cp -a "$TMP_CONF" /etc/nginx/sites-available/default

  # On Ubuntu, sites-enabled/default is usually a symlink to sites-available/default.
  # Ensure it's present and points correctly.
  if [[ -e /etc/nginx/sites-enabled/default && ! -L /etc/nginx/sites-enabled/default ]]; then
    ts="$(date +%Y%m%d-%H%M%S)"
    sudo mv /etc/nginx/sites-enabled/default "/etc/nginx/sites-enabled/default.bak.${ts}"
    echo "Moved non-symlink enabled default to .bak.${ts}" >&2
  fi
  if [[ ! -e /etc/nginx/sites-enabled/default ]]; then
    sudo ln -s /etc/nginx/sites-available/default /etc/nginx/sites-enabled/default
  fi

  echo "Validating nginx config..." >&2
  sudo nginx -t
  echo "Reloading nginx..." >&2
  sudo systemctl reload nginx

  rm -f "$TMP_CONF"
fi

echo "Done." >&2

