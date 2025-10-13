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
{{- $key = regexReplaceAll "[A-Z]" $key "_${0}" -}}
{{- $key = regexReplaceAll "^_" $key "" -}}
{{- upper $key -}}
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
{{- $excludedKeys := list "images" "service" "database" "resources" "telemetryDeployer" "serviceAccountAnnotations" "externalSecrets" "podAffinity" "global" "overrideCoreConfig" -}}
{{- if has $key $excludedKeys }}
{{- true -}}
{{- else }}
{{- false -}}
{{- end }}
{{- end -}}

{{- define "core.generateConfigMapData" -}}
{{- $globalConfig := .Values.global.coreConfig -}}
{{- $overrideConfig := .Values.overrideCoreConfig -}}
{{- $hyperscaler := .Values.global.hyperscaler | lower -}}

{{/* Process global.coreConfig values */}}
{{- if hasKey $globalConfig $hyperscaler }}
{{- range $key, $value := index $globalConfig $hyperscaler }}
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

{{- range $key, $value := $globalConfig }}
{{- if not (eq $key $hyperscaler) }}
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

{{/* Process all other values from values.yaml */}}
{{- range $key, $value := .Values }}
{{- $shouldExclude := include "core.shouldExcludeKey" $key }}
{{- if not (eq $shouldExclude "true") }}
{{- if $value }}
{{- include "core.processValue" (dict "key" $key "value" $value) }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}