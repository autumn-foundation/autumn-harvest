{{/* The chart name. */}}
{{- define "harvest-worker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* The full name. Kubernetes names hold at most 63 characters. */}}
{{- define "harvest-worker.fullname" -}}
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

{{- define "harvest-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "harvest-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* The worker pods. The Deployment, the Service and the PDB select them. */}}
{{- define "harvest-worker.workerSelectorLabels" -}}
{{ include "harvest-worker.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{- define "harvest-worker.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "harvest-worker.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* The image reference. A digest wins over a tag. */}}
{{- define "harvest-worker.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{- define "harvest-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "harvest-worker.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
"true" when `profile` is dev. It matches the runner: the name is trimmed and
compared without case, and `development` is an alias of `dev`.
*/}}
{{- define "harvest-worker.isDev" -}}
{{- has (lower (trim .Values.profile)) (list "dev" "development") }}
{{- end }}

{{/* The database Secret. The chart refuses to render without it. */}}
{{- define "harvest-worker.databaseSecret" -}}
{{- required "database.existingSecret is required: create a Secret that holds the database URL" .Values.database.existingSecret }}
{{- end }}

{{/* `HARVEST_DATABASE_URL` for the read-only check. It uses the app role. */}}
{{- define "harvest-worker.checkEnv" -}}
- name: HARVEST_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "harvest-worker.databaseSecret" . }}
      key: {{ .Values.database.secretKey }}
{{- end }}

{{/* `HARVEST_DATABASE_URL` for the migration hook. It may use a DDL role. */}}
{{- define "harvest-worker.migrationEnv" -}}
- name: HARVEST_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ default (include "harvest-worker.databaseSecret" .) .Values.migrations.existingSecret }}
      key: {{ default .Values.database.secretKey .Values.migrations.secretKey }}
{{- end }}

{{/* `--include-dir` once for each migration directory. */}}
{{- define "harvest-worker.includeDirs" -}}
{{- range .Values.migrations.includeDirs }}
- --include-dir
- {{ . | quote }}
{{- end }}
{{- end }}
