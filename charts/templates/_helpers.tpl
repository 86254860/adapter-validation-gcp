{{/*
Expand the name of the chart.
*/}}
{{- define "validation-gcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "validation-gcp.fullname" -}}
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
{{- define "validation-gcp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "validation-gcp.labels" -}}
helm.sh/chart: {{ include "validation-gcp.chart" . }}
{{ include "validation-gcp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "validation-gcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "validation-gcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "validation-gcp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "validation-gcp.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the ConfigMap to use
*/}}
{{- define "validation-gcp.configMapName" -}}
{{- printf "%s-config" (include "validation-gcp.fullname" .) }}
{{- end }}

{{/*
Create the name of the broker ConfigMap to use
*/}}
{{- define "validation-gcp.brokerConfigMapName" -}}
{{- printf "%s-broker-config" (include "validation-gcp.fullname" .) }}
{{- end }}

{{/*
Get the adapter config file name based on deployment mode
*/}}
{{- define "validation-gcp.adapterConfigFile" -}}
{{- if eq .Values.deploymentMode "dummy" }}
{{- "validation-dummy-adapter.yaml" }}
{{- else if eq .Values.deploymentMode "real" }}
{{- "validation-gcp-adapter.yaml" }}
{{- else }}
{{- fail "deploymentMode must be either 'dummy' or 'real'" }}
{{- end }}
{{- end }}
