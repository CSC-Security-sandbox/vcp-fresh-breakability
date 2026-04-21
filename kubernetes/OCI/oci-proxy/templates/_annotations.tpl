{{- define "oci_proxy.kubernetes.annotations" -}}
{{- end -}}

{{- define "oci_proxy.serviceAccountAnnotations" -}}
{{- with .Values.serviceAccountAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{- define "oci_proxy.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}
