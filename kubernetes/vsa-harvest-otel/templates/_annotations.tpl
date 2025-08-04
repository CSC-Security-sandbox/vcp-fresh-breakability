{{/*
Annotations specific for kubernetes. Starts with app.kubernetes. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "harvest.kubernetes.annotations" -}}
prometheus.io/scrape: 'true'
prometheus.io/path: '/metrics'
gke-gcsfuse/volumes: 'true'
{{- end -}}

{{- define "harvest.netapp.annotations" -}}
{{- end -}}

{{- define "harvest.serviceaccount.annotations" -}}
iam.gke.io/gcp-service-account: {{ .Values.serviceAccount.gcpServiceAccount }}
helm.sh/hook: pre-install,pre-upgrade
helm.sh/hook-weight: "-5"
{{- end -}}

{{- define "harvest.helmchart.annotations" -}}
helm.sh/chart: '{{ .Chart.Name }}-{{ .Chart.Version }}'
{{- end -}}
