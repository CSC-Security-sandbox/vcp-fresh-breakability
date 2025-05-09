{{- define "core.kubernetes.annotations" -}}
{{- end -}}

{{- define "core.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}

{{- define "vcp-dbmigrate.kubernetes.annotations" -}}
{{- end -}}

{{- define "vcp-dbmigrate.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}
