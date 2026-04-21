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
{{- $forceSecondary := default false $context.Values.global.useSecondaryImageRegistry -}}
{{- $useSecondary := or $isSecondary $forceSecondary -}}
{{- $secondaryRegistry := include "secondImageRegistryFullPath" $context | trim -}}
{{- $registry := include "imageRegistryFullPath" $context | trim -}}
{{- if and $useSecondary (ne $secondaryRegistry "") -}}
{{- $registry = $secondaryRegistry -}}
{{- end -}}
{{- if $context.Values.global.useTags -}}
{{- $finaltag := toString ($imageTag | default $imageVersion) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@sha256:%s" $registry $imageName $imageDigest -}}
{{- end -}}
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