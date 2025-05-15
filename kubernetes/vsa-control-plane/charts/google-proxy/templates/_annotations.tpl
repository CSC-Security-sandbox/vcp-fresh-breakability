{{- define "google_proxy.kubernetes.annotations" -}}
{{- end -}}

{{/*
Extra annotations specific for google-proxy ServiceAccount. Fetched from .Values.serviceAccountAnnotations
*/}}
{{- define "google_proxy.serviceAccountAnnotations" -}}
{{- with .Values.serviceAccountAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{- define "google_proxy.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}