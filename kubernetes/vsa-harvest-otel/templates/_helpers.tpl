{{/*
Helper function to generate the pod selector labels for the gcnv-harvest app.
*/}}
{{- define "gcnv-harvest.podSelectorLabels" -}}
app: {{ .Chart.Name | quote }}
{{- end -}}

{{- define "harvest.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "gcnv-harvest.name" -}}
vsa-harvest-poller
{{- end }}

{{/*
Expand the namespace of the chart.
*/}}

{{- define "gcnv-harvest.namespace" -}}
{{- default "default" .Release.Namespace -}}
{{- end }}
{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "gcnv-harvest.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}`

{{- define "ontap-opentelemetry-collector.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "gcnv-harvest.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ontap-opentelemetry-collector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gcnv-harvest.labels" -}}
helm.sh/chart: {{ include "gcnv-harvest.chart" . }}
{{ include "gcnv-harvest.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "ontap-opentelemetry-collector.labels" -}}
helm.sh/chart: {{ include "ontap-opentelemetry-collector.chart" . }}
{{ include "ontap-opentelemetry-collector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gcnv-harvest.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gcnv-harvest.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- define "ontap-opentelemetry-collector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gcnv-harvest.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "gcnv-harvest.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gcnv-harvest.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create a proper imageRegistry path with support for deprecated registry.qstack.com (until safely deprecated)
*/}}
{{- define "imageRegistryFullPath" -}}
{{- if eq .Values.global.primaryImageRegistryPath "" }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}
{{- else }}
{{ .Values.global.chartPrimaryImageRegistry | default .Values.global.primaryImageRegistry }}/{{ .Values.global.primaryImageRegistryPath }}
{{- end -}}
{{- end -}}

{{/*
Create a proper imageRegistry path with support for deprecated registry.qstack.com (until safely deprecated)
*/}}
{{- define "secondImageRegistryFullPath" -}}
{{- if and ( ne .Values.global.secondaryImageRegistry "" ) ( eq .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}
{{- else if and ( ne .Values.global.secondaryImageRegistry "" ) ( ne .Values.global.secondaryImageRegistryPath "" ) }}
{{ .Values.global.chartSecondaryImageRegistry | default .Values.global.secondaryImageRegistry }}/{{ .Values.global.secondaryImageRegistryPath }}
{{- end -}}
{{- end -}}

{{/*
Decide if container will use tagged image or digest one
*/}}
{{- define "containerImage" -}}
{{- $context := index . 0 -}}
{{- $args := index . 1 -}}
{{- $imageName := index $args "name" -}}
{{- $isSecondary := index $args "secondary" -}}
{{- $image := index $context.Values.images $imageName -}}
{{- $registry := ternary (include "secondImageRegistryFullPath" $context) (include "imageRegistryFullPath" $context) $isSecondary -}}
{{- $separator := ternary "@" ":" $context.Values.global.imageDigestTags -}}
{{- $tagOrDigest := "" -}}
{{- if eq $imageName "cloudSecretsHelper" -}}
  {{- if $context.Values.global.imageDigestTags -}}
    {{- if ne $context.Values.global.cloudSecretsHelperDigest "" -}}
      {{- $tagOrDigest = $context.Values.global.cloudSecretsHelperDigest -}}
    {{- else -}}
      {{- $tagOrDigest = $image.digest -}}
    {{- end -}}
  {{- else -}}
    {{- if ne $context.Values.global.cloudSecretsHelperTag "" -}}
      {{- $tagOrDigest = $context.Values.global.cloudSecretsHelperTag -}}
    {{- else -}}
      {{- $tagOrDigest = $image.tag -}}
    {{- end -}}
  {{- end -}}
{{- else -}}
  {{- $tagOrDigest = ternary $image.digest (toString $image.tag | default (toString $context.Chart.Version)) $context.Values.global.imageDigestTags -}}
{{- end -}}
{{- printf "%s/%s%s%s" $registry $image.name $separator $tagOrDigest -}}
{{- end -}}

{{/*
Helper function to generate the configMap name by appending "-config" to the app name.
*/}}
{{- define "harvest.configMapName" -}}
{{- printf "%s-config " .Chart.Name -}}
{{- end -}}
