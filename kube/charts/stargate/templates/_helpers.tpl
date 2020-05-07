{{/* vim: set filetype=mustache: */}}
{{/*
    Copyright DataStax, Inc.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/}}
{{/*
Expand the name of the chart.
*/}}
{{- define "stargate.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "stargate.fullname" -}}
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
{{- define "stargate.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "stargate.labels" -}}
helm.sh/chart: {{ include "stargate.chart" . }}
{{ include "stargate.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "stargate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stargate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "stargate.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "stargate.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the cluster role binding to use
*/}}
{{- define "stargate.clusterRoleBindingName" -}}
{{- if .Values.clusterRoleBinding.create }}
{{- default (include "stargate.fullname" .) .Values.clusterRoleBinding.name }}
{{- else }}
{{- default "default" .Values.clusterRoleBinding.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the cluster role to use
*/}}
{{- define "stargate.clusterRoleName" -}}
{{- if .Values.clusterRole.create }}
{{- default (include "stargate.fullname" .) .Values.clusterRoleName.name }}
{{- else }}
{{- default "default" .Values.clusterRole.name }}
{{- end }}
{{- end }}


{{/*
Create the name of the role binding to use
*/}}
{{- define "stargate.roleBindingName" -}}
{{- if .Values.roleBinding.create }}
{{- default (include "stargate.fullname" .) .Values.roleBinding.name }}
{{- else }}
{{- default "default" .Values.roleBinding.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "stargate.roleName" -}}
{{- if .Values.role.create }}
{{- default (include "stargate.fullname" .) .Values.role.name }}
{{- else }}
{{- default "default" .Values.role.name }}
{{- end }}
{{- end }}
