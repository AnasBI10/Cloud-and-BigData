# Build-Kontext ist das Repo-Wurzelverzeichnis:
#   docker build -f src/ui/Dockerfile -t congestion-watch/ui:0.1.0 .
FROM nginxinc/nginx-unprivileged:1.27-alpine

USER root

COPY src/ui/nginx.conf /etc/nginx/conf.d/default.conf

COPY src/ui/index.html src/ui/ingest.html src/ui/styles.css \
     /usr/share/nginx/html/
COPY src/ui/js/ /usr/share/nginx/html/js/
COPY src/ui/config.js /usr/share/nginx/html/config.js
COPY src/ui/docker-entrypoint.d/40-api-base-url.sh /docker-entrypoint.d/40-api-base-url.sh

RUN chmod +x /docker-entrypoint.d/40-api-base-url.sh \
    && chown -R 101:0 /usr/share/nginx/html \
    && chmod -R g+w /usr/share/nginx/html

USER 101

ENV API_BASE_URL=""

EXPOSE 8080
