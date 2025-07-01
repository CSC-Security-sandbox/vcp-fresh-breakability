{{- define "core.kubernetes.annotations" -}}
{{- end -}}

{{- define "core.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}

{{/*
Extra annotations specific for core serviceAccount. Fetched from .Values.serviceAccountAnnotations
*/}}
{{- define "core.serviceAccountAnnotations" -}}
{{- with .Values.serviceAccountAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{- define "vcp-dbmigrate.kubernetes.annotations" -}}
{{- end -}}

{{- define "vcp-dbmigrate.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
sidecar.istio.io/inject: {{ "false" | quote }}
{{- end -}}
