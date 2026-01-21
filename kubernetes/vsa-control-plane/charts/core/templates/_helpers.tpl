{{- define "imageRegistryFullPath" -}}
{{- if eq .Values.global.primaryImageRegistryPath "" }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}
{{- else }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}/{{ .Values.global.primaryImageRegistryPath }}
{{- end -}}
{{- end -}}

{{/*
Helper function to generate the pod selector labels for the core app.
*/}}
{{- define "core.podSelectorLabels" -}}
app: {{ .Chart.Name | quote }}
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
{{- $imageDigest := $imageConfig.digest -}}
{{- $isSecondary := index $args "secondary" -}}
{{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- if $context.Values.global.useTags -}}
{{- $finaltag := toString $imageTag | default (toString $context.Chart.Version) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@%s" $registry $imageName $imageDigest -}}
{{- end -}}
{{- end -}}

{{- define "toCapitalUnderscore" -}}
{{- $key := . -}}
{{- if eq $key "enableJSwapVersionCheck" }}
{{- "ENABLE_JSWAP_VERSION_CHECK" -}}
{{- else }}
{{- $key = regexReplaceAll "[A-Z]" $key "_${0}" -}}
{{- $key = regexReplaceAll "^_" $key "" -}}
{{- upper $key -}}
{{- end }}
{{- end -}}

{{/*
Helper function to process a value and convert it to appropriate format
*/}}
{{- define "core.processValue" -}}
{{- $key := index . "key" -}}
{{- $value := index . "value" -}}
{{- if kindIs "slice" $value }}
{{- if eq $key "requiredTakeoverReasons" }}
{{ include "toCapitalUnderscore" $key }}: {{ join "," $value | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | toJson | quote }}
{{- end }}
{{- else if kindIs "map" $value }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | toJson | quote }}
{{- else if kindIs "bool" $value }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- else if kindIs "float64" $value }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- else if kindIs "int" $value }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- end }}
{{- end -}}

{{/*
Helper function to check if a key should be excluded from processing
*/}}
{{- define "core.shouldExcludeKey" -}}
{{- $key := . -}}
{{- $excludedKeys := list "images" "service" "database" "resources" "telemetryDeployer" "serviceAccountAnnotations" "externalSecrets" "podAffinity" "global" "overrideCoreConfig" "cloudSqlIamAuthEnabled" -}}
{{- if has $key $excludedKeys }}
{{- true -}}
{{- else }}
{{- false -}}
{{- end }}
{{- end -}}

{{- define "core.generateConfigMapData" -}}
{{- $globalConfig := .Values.global.coreConfig | default dict -}}
{{- $overrideConfig := .Values.overrideCoreConfig | default dict -}}
{{- $hyperscaler := .Values.global.hyperscaler | default "gcp" | lower -}}
{{- $cloudSqlIamAuthEnabled := .Values.global.cloudSqlIamAuthEnabled | default false }}

{{/* Process global.coreConfig values */}}
{{- if hasKey $globalConfig $hyperscaler }}
{{- range $key, $value := index $globalConfig $hyperscaler }}
{{- $skipKey := and $cloudSqlIamAuthEnabled (or (eq $key "dbHost") (eq $key "metricsHost") (eq $key "dbUser") (eq $key "metricsDbUser")) }}
{{- $hasValue := and (not (kindIs "invalid" $value)) (ne (toString $value) "") }}
{{- if and (not $skipKey) $hasValue }}
{{- if or (not (hasKey $overrideConfig $hyperscaler)) (not (hasKey (index $overrideConfig $hyperscaler) $key)) (eq (index (index $overrideConfig $hyperscaler) $key) "") }}
{{- if eq $key "regionNumberMap" }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | toJson | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- end }}
{{- else }}
{{- if eq $key "regionNumberMap" }}
{{ include "toCapitalUnderscore" $key }}: {{ index (index $overrideConfig $hyperscaler) $key | toJson | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ index (index $overrideConfig $hyperscaler) $key | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- range $key, $value := $globalConfig }}
{{- if not (eq $key $hyperscaler) }}
{{- $skipKey := and $cloudSqlIamAuthEnabled (or (eq $key "dbHost") (eq $key "metricsHost") (eq $key "dbUser") (eq $key "metricsDbUser")) }}
{{- $hasValue := and (not (kindIs "invalid" $value)) (ne (toString $value) "") }}
{{- if and (not $skipKey) $hasValue }}
{{- if or (not (hasKey $overrideConfig $key)) (eq (index $overrideConfig $key) "") }}
{{- if eq $key "regionNumberMap" }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | toJson | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ $value | quote }}
{{- end }}
{{- else }}
{{- if eq $key "regionNumberMap" }}
{{ include "toCapitalUnderscore" $key }}: {{ index $overrideConfig $key | toJson | quote }}
{{- else }}
{{ include "toCapitalUnderscore" $key }}: {{ index $overrideConfig $key | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/* Collect all keys that were processed from global config to avoid duplicates */}}
{{- $processedKeys := list -}}
{{- if hasKey $globalConfig $hyperscaler }}
{{- range $key, $value := index $globalConfig $hyperscaler }}
{{- $processedKeys = append $processedKeys $key -}}
{{- end }}
{{- end }}
{{- range $key, $value := $globalConfig }}
{{- if not (eq $key $hyperscaler) }}
{{- $processedKeys = append $processedKeys $key -}}
{{- end }}
{{- end }}

{{/* Process global values that are not in coreConfig */}}
{{- if .Values.global }}
{{- range $key, $value := .Values.global }}
{{- if and (not (eq $key "coreConfig")) (not (eq $key "hyperscaler")) }}
{{- $shouldExclude := include "core.shouldExcludeKey" $key }}
{{- $skipIamKeys := and $cloudSqlIamAuthEnabled (or (eq $key "dbHost") (eq $key "dbUser") (eq $key "metricsHost") (eq $key "metricsDbUser")) }}
{{- if and (not (eq $shouldExclude "true")) (not $skipIamKeys) }}
{{- if not (kindIs "invalid" $value) }}
{{- if not (has $key $processedKeys) }}
{{- include "core.processValue" (dict "key" $key "value" $value) }}
{{- $processedKeys = append $processedKeys $key -}}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/* Process values from core section if it exists (when running core chart directly with overrides) */}}
{{- if hasKey .Values "core" }}
{{- range $key, $value := .Values.core }}
{{- $shouldExclude := include "core.shouldExcludeKey" $key }}
{{- $skipIamKeys := and $cloudSqlIamAuthEnabled (or (eq $key "dbHost") (eq $key "dbUser") (eq $key "metricsHost") (eq $key "metricsDbUser")) }}
{{- if and (not (eq $shouldExclude "true")) (not $skipIamKeys) }}
{{- if not (kindIs "invalid" $value) }}
{{- if not (has $key $processedKeys) }}
{{- include "core.processValue" (dict "key" $key "value" $value) }}
{{- $processedKeys = append $processedKeys $key -}}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/* Process all other values from values.yaml, but skip already processed keys */}}
{{- range $key, $value := .Values }}
{{- $shouldExclude := include "core.shouldExcludeKey" $key }}
{{- $skipIamKeys := and $cloudSqlIamAuthEnabled (or (eq $key "dbHost") (eq $key "dbUser") (eq $key "metricsHost") (eq $key "metricsDbUser")) }}
{{- if and (not (eq $shouldExclude "true")) (not $skipIamKeys) }}
{{- if not (kindIs "invalid" $value) }}
{{- if not (has $key $processedKeys) }}
{{- include "core.processValue" (dict "key" $key "value" $value) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "otelsecondImageRegistryFullPath" -}}
{{- if and ( ne .Values.global.secondaryImageRegistry "" ) ( eq .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}
{{- else if and ( ne .Values.global.secondaryImageRegistry "" ) ( ne .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}
{{- end -}}
{{- end -}}

{{/*
Helper function to get the final URL of the image to be used in the deployment.
*/}}
{{- define "otelContainerImage" -}}
{{- $context := index . 0 -}}
{{- $args := index . 1 -}}
{{- $imageValueName := index $args "name" -}}
{{- $imageConfig := index $context.Values.images $imageValueName -}}
{{- $imageName := $imageConfig.name -}}
{{- $imageTag := $imageConfig.tag -}}
{{- $imageDigest := $imageConfig.digest -}}
{{- $isSecondary := index $args "secondary" -}}
{{- $registry := ternary (include "otelsecondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- if $context.Values.global.useTags -}}
{{- $finaltag := toString $imageTag | default (toString $context.Chart.Version) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@%s" $registry $imageName $imageDigest -}}
{{- end -}}
{{- end -}}

{{/*
Helper function to conditionally include Cloud SQL Proxy sidecar container
Only included when global.cloudSqlIamAuthEnabled is true
Used for long-running deployments (does not include graceful shutdown flags)
*/}}
{{- define "core.databaseProxyContainer" -}}
{{- if .Values.global.cloudSqlIamAuthEnabled }}
- name: cloud-sql-proxy
  image: {{ .Values.global.cloudSqlProxy.image | default "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.15.1" | quote }}
  args:
    - "--private-ip"
    - "--auto-iam-authn"
    - "--structured-logs"
    - "--port=5432"
    - "{{ .Values.global.coreConfig.gcp.instanceConnectionName | default .Values.telemetryDeployer.cloudSqlConnector }}"
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

{{/*
Helper function to conditionally include Cloud SQL Proxy sidecar container for Jobs
Only included when global.cloudSqlIamAuthEnabled is true
Includes graceful shutdown flags (--quitquitquit and --admin-port=9091) for Jobs
*/}}
{{- define "core.databaseProxyContainerForJob" -}}
{{- if .Values.global.cloudSqlIamAuthEnabled }}
- name: cloud-sql-proxy
  image: {{ .Values.global.cloudSqlProxy.image | default "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.15.1" | quote }}
  args:
    - "--private-ip"
    - "--auto-iam-authn"
    - "--structured-logs"
    - "--quitquitquit"
    - "--admin-port=9091"
    - "--port=5432"
    - "{{ .Values.global.coreConfig.gcp.instanceConnectionName | default .Values.telemetryDeployer.cloudSqlConnector }}"
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