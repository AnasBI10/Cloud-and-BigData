#!/bin/sh
# Schreibt config.js beim Containerstart aus der Umgebung (SCRUM-91).
#
# Damit laeuft dasselbe Image lokal gegen http://localhost:8000 und im Cluster
# gegen den internen Service, ohne dass die Adresse im Bundle steht. Leer
# bedeutet "gleiche Herkunft wie die UI" — im Cluster ist das der Normalfall,
# weil Traefik /api auf die Serving-API leitet.
set -eu

TARGET="${NGINX_HTML_DIR:-/usr/share/nginx/html}/config.js"
BASE="${API_BASE_URL:-}"

# Der Wert landet unveraendert in einer JavaScript-Zeichenkette. Ein
# Anfuehrungszeichen oder Backslash darin waere eine Moeglichkeit, eigenen
# Code in jede ausgelieferte Seite zu schreiben. Lieber laut abbrechen als
# still etwas Unerwartetes ausliefern.
case "$BASE" in
    *[\"\'\\\\]*|*'<'*|*'>'*)
        echo "API_BASE_URL enthaelt unerlaubte Zeichen: $BASE" >&2
        exit 1
        ;;
esac

cat > "$TARGET" <<EOF
/* Beim Containerstart erzeugt. Nicht bearbeiten — siehe
   src/ui/docker-entrypoint.d/20-api-base-url.sh */
window.APP_CONFIG = { apiBase: "${BASE}" };
EOF

if [ -n "$BASE" ]; then
    echo "config.js geschrieben: apiBase=\"$BASE\""
else
    echo "config.js geschrieben: apiBase leer, gleiche Herkunft wie die UI"
fi
