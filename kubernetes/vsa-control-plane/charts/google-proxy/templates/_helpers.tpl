{{/*
Helper function to generate the pod selector labels for the google-proxy app.
*/}}
{{- define "google-proxy.podSelectorLabels" -}}
app: {{ .Chart.Name | quote }}
{{- end -}}

{{- define "imageRegistryFullPath" -}}
{{- $global := .Values.global | default dict -}}
{{- $primaryRegistryPath := (get $global "primaryImageRegistryPath") | default "" -}}
{{- $primaryRegistry := (get $global "primaryImageRegistry") | default "" -}}
{{- $chartPrimaryRegistry := (get $global "chartPrimaryImageRegistry") | default "" -}}
{{- if eq $primaryRegistryPath "" }}
{{ $chartPrimaryRegistry | default $primaryRegistry }}
{{- else }}
{{ $chartPrimaryRegistry | default $primaryRegistry }}/{{ $primaryRegistryPath }}
{{- end -}}
{{- end -}}

{{- define "secondImageRegistryFullPath" -}}
{{- $global := .Values.global | default dict -}}
{{- $secondaryRegistry := (get $global "secondaryImageRegistry") | default "" -}}
{{- $secondaryRegistryPath := (get $global "secondaryImageRegistryPath") | default "" -}}
{{- $chartSecondaryRegistry := (get $global "chartSecondaryImageRegistry") | default "" -}}
{{- if and ( ne $secondaryRegistry "" ) ( eq $secondaryRegistryPath "" ) }}
{{ $chartSecondaryRegistry | default $secondaryRegistry }}
{{- else if and ( ne $secondaryRegistry "" ) ( ne $secondaryRegistryPath "" ) }}
{{ $chartSecondaryRegistry | default $secondaryRegistry }}/{{ $secondaryRegistryPath }}
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
{{- $imageDigest := $imageConfig.digest -}}
{{- $isSecondary := index $args "secondary" -}}
{{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- $global := $context.Values.global | default dict -}}
{{- $useTags := (get $global "useTags") | default false -}}
{{- if $useTags -}}
{{- $finaltag := toString $imageTag | default (toString $context.Chart.Version) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@%s" $registry $imageName $imageDigest -}}
{{- end -}}
{{- end -}}

{{- define "toCapitalUnderscore" -}}
{{- $key := . -}}
{{- $key = regexReplaceAll "[A-Z]" $key "_${0}" -}}
{{- $key = regexReplaceAll "^_" $key "" -}}
{{- upper $key -}}
{{- end -}}

{{- define "google_proxy.generateConfigMapData" -}}
{{- $globalConfig := .Values.global.coreConfig -}}
{{- $overrideConfig := .Values.overrideCoreConfig -}}
{{- $hyperscaler := .Values.global.hyperscaler | lower -}}

{{- if hasKey $globalConfig $hyperscaler }}
{{- range $key, $value := index $globalConfig $hyperscaler }}
{{- if or (not (hasKey $overrideConfig $hyperscaler)) (not (hasKey (index $overrideConfig $hyperscaler) $key)) (eq (index (index $overrideConfig $hyperscaler) $key) "") }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ index (index $overrideConfig $hyperscaler) $key | quote }}
{{- end }}
{{- end }}
{{- end }}

{{- range $key, $value := $globalConfig }}
{{- if not (eq $key $hyperscaler) }}
{{- if or (not (hasKey $overrideConfig $key)) (eq (index $overrideConfig $key) "") }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ index $overrideConfig $key | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Helper function to conditionally include Cloud SQL Proxy sidecar container
Only included when global.cloudSqlIamAuthEnabled is true
Used for long-running deployments (does not include graceful shutdown flags)
*/}}
{{- define "google-proxy.databaseProxyContainer" -}}
{{- if .Values.global.cloudSqlIamAuthEnabled }}
{{- $instanceName := "" }}
{{- $proxyImage := "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.21.1" }}
{{- if and .Values.global.cloudSqlProxy .Values.global.cloudSqlProxy.image }}
  {{- $proxyImage = .Values.global.cloudSqlProxy.image }}
{{- end }}
{{- if and .Values.global.cloudSqlProxy .Values.global.cloudSqlProxy.instanceConnectionName }}
  {{- $instanceName = .Values.global.cloudSqlProxy.instanceConnectionName }}
{{- else if and .Values.global.coreConfig .Values.global.coreConfig.gcp .Values.global.coreConfig.gcp.instanceConnectionName }}
  {{- $instanceName = .Values.global.coreConfig.gcp.instanceConnectionName }}
{{- else if .Values.cloudSqlConnector }}
  {{- $instanceName = .Values.cloudSqlConnector }}
{{- end }}
- name: cloud-sql-proxy
  image: {{ $proxyImage | quote }}
  args:
    - "--private-ip"
    - "--auto-iam-authn"
    - "--structured-logs"
    - "--port=5432"
    - "{{ $instanceName }}"
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
