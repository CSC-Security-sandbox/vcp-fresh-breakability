{{- define "google_proxy.kubernetes.annotations" -}}
{{- end -}}

{{- define "google_proxy.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}