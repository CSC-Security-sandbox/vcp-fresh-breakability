{{- define "adc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels specific for kubernetes. Starts with app.kubernetes. These should be hardcoded in code only
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.kubernetes.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | replace "+" "_" }}
{{- end -}}

{{/*
Labels specific for helmcharts. Starts with helm.sh. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.helmchart.labels" -}}
helm.sh/chart: vsa-control-plane-{{ .Chart.Version | replace "+" "_" }}
helm.sh/parent-chart: vsa-control-plane
{{- end -}}

{{/*
Pod selector labels specific for adc. Once deployed, they cant be changed.
*/}}
{{- define "adc.podSelectorLabels" -}}
app: {{ include "adc.name" . }}
{{- end -}}

{{/*
Service selector labels specific for adc. Once deployed, they cant be changed.
*/}}
{{- define "adc.serviceSelectorLabels" -}}
app: {{ include "adc.name" . }}
{{- end -}}

{{/*
Extra labels specific for adc pods. Fetched from .Values.podLabels
*/}}
{{- define "adc.podLabels" -}}
{{- with .Values.podLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra labels specific for adc statefulSet. Fetched from .Values.statefulSetLabels
*/}}
{{- define "adc.statefulSetLabels" -}}
{{- with .Values.statefulSetLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra labels specific for adc configmap. Fetched from .Values.configmapLabels
*/}}
{{- define "adc.configmapLabels" -}}
{{- with .Values.configmapLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra labels specific for adc secret. Fetched from .Values.secretLabels
*/}}
{{- define "adc.secretLabels" -}}
{{- with .Values.secretLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra labels specific for adc service. Fetched from .Values.serviceLabels
*/}}
{{- define "adc.serviceLabels" -}}
{{- with .Values.serviceLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra labels specific for adc serviceAccount. Fetched from .Values.serviceAccountLabels
*/}}
{{- define "adc.serviceAccountLabels" -}}
{{- with .Values.serviceAccountLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Common labels specific for adc. Fetched from .Values.commonLabels
*/}}
{{- define "adc.commonLabels" -}}
{{- with .Values.commonLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

