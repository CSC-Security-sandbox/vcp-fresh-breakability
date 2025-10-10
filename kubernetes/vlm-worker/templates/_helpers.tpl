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
    {{- $imageName := index $args "vlmImageName" -}}
    {{- $vlmImageDigest := index $args "vlmImageDigest" -}}
    {{- $vlmImageTag := index $args "vlmImageTag" -}}
    {{- $isSecondary := index $args "secondary" -}}
    {{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
    {{- if $context.Values.global.useTags -}}
        {{- printf "%s/%s:%s" $registry $imageName $vlmImageTag -}}
    {{- else -}}
        {{- printf "%s/%s@%s" $registry $imageName $vlmImageDigest -}}
    {{- end -}}
{{- end -}}


{{/*
Helper function to generate the configMap name by appending version and "-config" to the app name.
Usage: include "vlm-worker.configMapName" (list $ $version)
*/}}
{{- define "vlm-worker.configMapName" -}}
    {{- $context := index . 0 -}}
    {{- $version := index . 1 -}}
    {{- printf "%s-%s-config" $context.Values.app.name $version -}}
{{- end -}}

{{/*
Helper function to generate task queue name based on ontap version.
*/}}
{{- define "vlm-worker.taskQueueName" -}}
{{- printf "%s-%s" .Values.workerConfig.taskQueuePrefix .Values.ontapVersion -}}
{{- end -}}

{{/*
Helper function to generate the secret name by appending "-secret" to the app name.
*/}}
{{- define "vlm-worker.secretName" -}}
{{- printf "%s-secret" .Values.app.name -}}
{{- end -}}
