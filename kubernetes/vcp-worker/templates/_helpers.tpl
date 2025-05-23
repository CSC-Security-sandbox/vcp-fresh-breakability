{{/*
Helper function to generate the secret name by appending "-secret" to the app name.
*/}}
{{- define "vcp-worker.secretName" -}}
{{- printf "%s-secret" .Values.app.name -}}
{{- end -}}


{{/*
Helper function to generate the configMap name by appending "-config" to the app name.
*/}}
{{- define "vcp-worker.configMapName" -}}
{{- printf "%s-config" .Values.app.name -}}
{{- end -}}
