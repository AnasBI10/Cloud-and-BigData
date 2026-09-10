# Build-Kontext ist das Repo-Wurzelverzeichnis, wie bei den anderen Images:
#   docker build -f src/ui/Dockerfile -t congestion-watch/ui:0.1.0 .
#
# Kein Build-Schritt und keine Node-Toolchain: das Bundle ist reines
# HTML/CSS/ES-Modules und wird so ausgeliefert, wie es im Repo liegt. Das
# Image besteht damit aus nginx plus rund 60 kB Dateien.
#
# nginx-unprivileged statt nginx: laeuft ab Werk als uid 101 mit passend
# gesetzten Verzeichnisrechten. Dieselbe Linie wie beim Serving-Image, nur
# ohne die Handarbeit an /var/cache und /var/run.
FROM nginxinc/nginx-unprivileged:1.27-alpine

USER root

COPY src/ui/nginx.conf /etc/nginx/conf.d/default.conf

COPY src/ui/index.html src/ui/dashboard.html src/ui/styles.css \
     /usr/share/nginx/html/
COPY src/ui/js/ /usr/share/nginx/html/js/

# Wird beim Start ueberschrieben; als Datei vorhanden, damit ein Aufruf ohne
# Entrypoint (etwa docker run --entrypoint) keine 404 auf config.js liefert.
COPY src/ui/config.js /usr/share/nginx/html/config.js

# Die nginx-Images fuehren beim Start alles unter /docker-entrypoint.d/ in
# alphabetischer Reihenfolge aus. Sie bringen selbst 10-, 15-, 20- und
# 30-Skripte mit; 40- laeuft eindeutig danach.
COPY src/ui/docker-entrypoint.d/40-api-base-url.sh /docker-entrypoint.d/40-api-base-url.sh

# Das Entrypoint-Skript schreibt config.js zur Laufzeit — dafuer braucht der
# unprivilegierte Nutzer Schreibrecht auf das Verzeichnis.
RUN chmod +x /docker-entrypoint.d/40-api-base-url.sh \
    && chown -R 101:0 /usr/share/nginx/html \
    && chmod -R g+w /usr/share/nginx/html

USER 101

# Leer = gleiche Herkunft wie die UI. Im Cluster stimmt das, weil Traefik
# /api auf die Serving-API leitet.
ENV API_BASE_URL=""

EXPOSE 8080
