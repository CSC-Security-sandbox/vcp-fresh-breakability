{{- define "core.kubernetes.annotations" -}}
{{- end -}}

{{- define "core.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}