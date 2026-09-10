#!/bin/sh
# Schreibt config.js beim Containerstart aus der Umgebung (SCRUM-91).
set -eu

TARGET="${NGINX_HTML_DIR:-/usr/share/nginx/html}/config.js"
BASE="${API_BASE_URL:-}"

case "$BASE" in
    *[\"\'\\\\]*|*'<'*|*'>'*)
        echo "API_BASE_URL enthaelt unerlaubte Zeichen: $BASE" >&2
        exit 1
        ;;
esac

cat > "$TARGET" <<EOF
window.APP_CONFIG = { apiBase: "${BASE}" };
EOF

if [ -n "$BASE" ]; then
    echo "config.js geschrieben: apiBase=\"$BASE\""
else
    echo "config.js geschrieben: apiBase leer, gleiche Herkunft wie die UI"
fi
