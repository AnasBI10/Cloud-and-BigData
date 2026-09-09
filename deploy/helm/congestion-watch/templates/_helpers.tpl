{{/* Gemeinsame Labels fuer alle Ressourcen */}}
{{- define "congestion-watch.labels" -}}
app.kubernetes.io/part-of: congestion-watch
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
