{{- define "imageRegistryFullPath" -}}
{{- if eq .Values.global.primaryImageRegistryPath "" }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}
{{- else }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}/{{ .Values.global.primaryImageRegistryPath }}
{{- end -}}
{{- end -}}

{{- define "secondImageRegistryFullPath" -}}
{{- if and ( ne .Values.global.secondaryImageRegistry "" ) ( eq .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}
{{- else if and ( ne .Values.global.secondaryImageRegistry "" ) ( ne .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}/{{ .Values.global.secondaryImageRegistryPath }}
{{- end -}}
{{- end -}}


{{/*
Helper function to get the final URL of the image to be used in the deployment.
*/}}
{{- define "containerImage" -}}
{{- $context := index . 0 -}}
{{- $args := index . 1 -}}
{{- $imageValueName := index $args "name" -}}
{{- $imageConfig := index $context.Values.images $imageValueName -}}
{{- $imageName := $imageConfig.name -}}
{{- $imageTag := $imageConfig.tag -}}
{{- $imageDigest := index $args "sha" -}}
{{- $isSecondary := index $args "secondary" -}}
{{- $imageVersion := index $args "version" -}}
{{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- if $context.Values.global.useTags -}}
{{- $finaltag := toString $imageVersion -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@sha256:%s" $registry $imageName $imageDigest -}}
{{- end -}}
{{- end -}}

{{/*
Helper function to generate the secret name by appending "-secret" to the app name.
*/}}
{{- define "vcp-worker.secretName" -}}
{{- printf "%s-secret" . -}}
{{- end -}}


{{/*
Helper function to generate the configMap name by appending "-config" to the app name.
*/}}
{{- define "vcp-worker.configMapName" -}}
{{- printf "%s-config" . -}}
{{- end -}}

{{/* Helper function to convert a string to upper snake case.
   Example: "myVariableName" becomes "MY_VARIABLE_NAME"
*/}}
{{- define "toUpperSnakeCase" -}}
{{- $camel := . -}}
{{- $snake := regexReplaceAll "([a-z])([A-Z])" $camel "${1}_${2}" -}}
{{- upper $snake -}}
{{- end -}}

{{/*
Helper function to conditionally include Cloud SQL Proxy sidecar container
Only included when global.cloudSqlIamAuthEnabled is true
*/}}
{{- define "worker.databaseProxyContainer" -}}
{{- if .Values.global.cloudSqlIamAuthEnabled }}
{{- $instanceConnectionName := "" }}
{{- if .Values.global.coreConfig }}
{{- if .Values.global.coreConfig.gcp }}
{{- if .Values.global.coreConfig.gcp.instanceConnectionName }}
{{- $instanceConnectionName = .Values.global.coreConfig.gcp.instanceConnectionName }}
{{- end }}
{{- end }}
{{- end }}
{{- if eq $instanceConnectionName "" }}
{{- $project := .Values.workerConfig.smcProjectId }}
{{- $region := .Values.workerConfig.localRegion }}
{{- $instance := printf "%s-db-postgres" $project }}
{{- $instanceConnectionName = printf "%s:%s:%s" $project $region $instance }}
{{- end }}
- name: cloud-sql-proxy
  image: {{ .Values.global.cloudSqlProxy.image | default "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.21.1" | quote }}
  args:
    - "--private-ip"
    - "--auto-iam-authn"
    - "--structured-logs"
    - "--port=5432"
    - "{{ $instanceConnectionName }}"
  securityContext:
    runAsNonRoot: true
  resources:
    limits:
      cpu: 500m
      memory: 512Mi
    requests:
      cpu: 100m
      memory: 128Mi
{{- end }}
{{- end -}}