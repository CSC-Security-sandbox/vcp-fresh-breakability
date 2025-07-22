{{/*
Labels specific for kubernetes. Starts with app.kubernetes. These should be hardcoded in code only
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "harvest.kubernetes.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | replace "+" "_" }}
app.kubernetes.io/component: {{ .ClusterName }}
{{- end -}}

{{/*
Labels specific for kubernetes. Starts with app.kubernetes. These should be hardcoded in code only
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "harvest.serviceaccount.labels" -}}
{{- end -}}

{{/*
Labels specific for helmcharts. Starts with helm.sh. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "harvest.helmchart.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/*
Labels specific for NetApp starting with cvs.netapp.com. These should be hardcoded in code only
They are the same for a whole chart. Name starts with chart name (not component)
*/}}
{{- define "harvest.netapp.labels" -}}
{{- end -}}

{{/*
Pod selector labels specific for harvest. Once deployed, they cant be changed.
*/}}
{{- define "harvest.podSelectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service selector labels specific for harvest. Once deployed, they cant be changed.
*/}}
{{- define "harvest.serviceSelectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .ClusterName }}
{{- end -}}



