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
{{- $isSecondary := index $args "secondary" -}}
{{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- $finaltag := toString $imageTag | default (toString $context.Chart.Version) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- end -}}