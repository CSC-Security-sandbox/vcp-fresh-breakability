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
Helper to get container image URL for VLM worker.
*/}}
{{- define "containerImage" -}}
    {{- $context := index . 0 -}}
    {{- $args := index . 1 -}}
    {{- $imageName := index $args "vlmImageName" -}}
    {{- $vlmImageDigest := index $args "vlmImageDigest" -}}
    {{- $vlmImageTag := index $args "vlmImageTag" -}}
    {{- $isSecondary := index $args "secondary" -}}
    {{- $forceSecondary := default false $context.Values.global.useSecondaryImageRegistry -}}
    {{- $useSecondary := or $isSecondary $forceSecondary -}}
    {{- $secondaryRegistry := include "secondImageRegistryFullPath" $context | trim -}}
    {{- $registry := include "imageRegistryFullPath" $context | trim -}}
    {{- if and $useSecondary (ne $secondaryRegistry "") -}}
        {{- $registry = $secondaryRegistry -}}
    {{- end -}}
    {{- if $context.Values.global.useTags -}}
        {{- printf "%s/%s:%s" $registry $imageName $vlmImageTag -}}
    {{- else -}}
        {{- printf "%s/%s@%s" $registry $imageName $vlmImageDigest -}}
    {{- end -}}
{{- end -}}

{{- define "vlm-worker.configMapName" -}}
    {{- $context := index . 0 -}}
    {{- $version := index . 1 -}}
    {{- printf "%s-%s-config" $context.Values.app.name $version -}}
{{- end -}}

{{- define "vlm-worker.taskQueueName" -}}
{{- printf "%s-%s" .Values.workerConfig.taskQueuePrefix .Values.ontapVersion -}}
{{- end -}}

{{- define "vlm-worker.secretName" -}}
{{- printf "%s-secret" .Values.app.name -}}
{{- end -}}

{{- define "vlm-worker.ociAuthSecretName" -}}
{{- if .Values.workerConfig.ociAuthSecretName -}}
{{- .Values.workerConfig.ociAuthSecretName | quote -}}
{{- else if .Values.workerConfig.ociAuth.createSecret -}}
{{- printf "%s-oci-auth" .Values.app.name -}}
{{- end -}}
{{- end -}}
