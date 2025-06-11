{{/*
Annotations specific for kubernetes. Starts with app.kubernetes. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.kubernetes.annotations" -}}
{{- end -}}

{{/*
Annotations specific for helmcharts. Starts with helm.sh. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.helmchart.annotations" -}}
helm.sh/deprecated: {{ .Chart.Deprecated | quote }}
{{- end -}}

{{/*
Annotations specific for NetApp starting with cvs.netapp.com. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.netapp.annotations" -}}
{{- end -}}

{{/*
Annotations specific for NetApp which are dynamically changing on each SDE upgrade. 
Applied only to deployment/sts to prevent redeploy of everything on each upgrade. These should be hardcoded in code only.
They are the same for a whole chart. Name starts with chart name (not component).
*/}}
{{- define "adc.dynamic.annotations" -}}
{{- end -}}

{{/*
Extra annotations specific for adc workloadIdentities component. These should be hardcoded in code only
*/}}
{{- define "adc.workloadIdentity.annotations" -}}
{{- if .Values.global.workloadIdentityEnabled }}
{{- if and (or (eq .Values.global.hyperscaler "gcp") (eq .Values.global.hyperscaler "dev")) .Values.workloadIdentity.gcpServiceAccount }}
iam.gke.io/gcp-service-account: {{ .Values.workloadIdentity.gcpServiceAccount | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc deployment. Fetched from .Values.statefulSetAnnotations
*/}}
{{- define "adc.statefulSetAnnotations" -}}
{{- with .Values.statefulSetAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc pods. Fetched from .Values.podAnnotations
*/}}
{{- define "adc.podAnnotations" -}}
{{- with .Values.podAnnotations }}
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc secret. Fetched from .Values.secretAnnotations
*/}}
{{- define "adc.secretAnnotations" -}}
{{- with .Values.secretAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc configmap. Fetched from .Values.configmapAnnotations
*/}}
{{- define "adc.configmapAnnotations" -}}
{{- with .Values.configmapAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc service. Fetched from .Values.serviceAnnotations
*/}}
{{- define "adc.serviceAnnotations" -}}
{{- with .Values.serviceAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Extra annotations specific for adc serviceAccount. Fetched from .Values.serviceAccountAnnotations
*/}}
{{- define "adc.serviceAccountAnnotations" -}}
{{- with .Values.serviceAccountAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Common annotations specific for adc. Fetched from .Values.commonAnnotations
*/}}
{{- define "adc.commonAnnotations" -}}
{{- with .Values.commonAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

