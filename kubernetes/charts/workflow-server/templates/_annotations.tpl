{{- define "workflow_server.kubernetes.annotations" -}}
{{- end -}}

{{- define "workflow_server.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}