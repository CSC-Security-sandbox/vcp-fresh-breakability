{{/*
Helper function to generate the pod selector labels for the oci-proxy app.
*/}}
{{- define "oci-proxy.podSelectorLabels" -}}
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
{{- $forceSecondary := default false $context.Values.global.useSecondaryImageRegistry -}}
{{- $useSecondary := or $isSecondary $forceSecondary -}}
{{- $secondaryRegistry := include "secondImageRegistryFullPath" $context | trim -}}
{{- $registry := include "imageRegistryFullPath" $context | trim -}}
{{- if and $useSecondary (ne $secondaryRegistry "") -}}
{{- $registry = $secondaryRegistry -}}
{{- end -}}
{{- $global := $context.Values.global | default dict -}}
{{- $useTags := (get $global "useTags") | default false -}}
{{- if or $useTags (not $imageDigest) -}}
{{- $finaltag := toString $imageTag | default (toString $context.Chart.Version) -}}
{{- printf "%s/%s:%s" $registry $imageName $finaltag -}}
{{- else -}}
{{- printf "%s/%s@%s" $registry $imageName $imageDigest -}}
{{- end -}}
{{- end -}}
