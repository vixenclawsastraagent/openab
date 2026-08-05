{{- define "openab-kubernetes-session.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "openab-kubernetes-session.fullname" -}}
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

{{- define "openab-kubernetes-session.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "openab-kubernetes-session.labels" -}}
helm.sh/chart: {{ include "openab-kubernetes-session.chart" . }}
{{ include "openab-kubernetes-session.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "openab-kubernetes-session.selectorLabels" -}}
app.kubernetes.io/name: {{ include "openab-kubernetes-session.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "openab-kubernetes-session.image" -}}
{{- if and .Values.image.tag .Values.image.digest }}
{{- fail "image.tag and image.digest are mutually exclusive" }}
{{- else if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{- define "openab-kubernetes-session.controllerNetworkPolicyName" -}}
{{- $suffix := "controller-net" -}}
{{- $prefix := include "openab-kubernetes-session.fullname" . | trunc 48 | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix $suffix -}}
{{- end }}

{{- define "openab-kubernetes-session.workerNetworkPolicyName" -}}
{{- $suffix := "worker-net" -}}
{{- $prefix := include "openab-kubernetes-session.fullname" . | trunc 52 | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix $suffix -}}
{{- end }}

{{- define "openab-kubernetes-session.workerDefaultDenyNetworkPolicyName" -}}
{{- $suffix := "worker-deny" -}}
{{- $prefix := include "openab-kubernetes-session.fullname" . | trunc 51 | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix $suffix -}}
{{- end }}
