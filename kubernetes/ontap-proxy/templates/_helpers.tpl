{{/*
Helper function to generate pod selector labels for ontap-proxy.
*/}}
{{- define "ontap-proxy.podSelectorLabels" -}}
app: {{ .Chart.Name | quote }}
{{- end -}}
