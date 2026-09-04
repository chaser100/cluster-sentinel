{{- define "clustersentinel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "clustersentinel.fullname" -}}
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

{{- define "clustersentinel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "clustersentinel.labels" -}}
helm.sh/chart: {{ include "clustersentinel.chart" . }}
{{ include "clustersentinel.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "clustersentinel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clustersentinel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "clustersentinel.serviceAccountName" -}}
{{- default "clustersentinel" .Values.clustersentinel.serviceAccount.name }}
{{- end }}

{{- define "clustersentinel.mcpSecretName" -}}
{{- default .Values.mcpAuth.secretName .Values.mcpAuth.existingSecret }}
{{- end }}
