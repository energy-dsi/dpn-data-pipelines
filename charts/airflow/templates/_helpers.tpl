{{/*
Common name prefix (central control)
*/}}
{{- define "airflow.name" -}}
{{- default "dpn-airflow" .Values.airflow.name -}}
{{- end }}

{{/*
Full resource name
*/}}
{{- define "airflow.fullname" -}}
{{- include "airflow.name" . -}}
{{- end }}

{{/*
Common labels for all resources
*/}}
{{- define "airflow.labels" -}}
app.kubernetes.io/name: {{ include "airflow.name" . }}  
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: Helm
{{- end }}

{{/*
Selector labels (used in matchLabels)
*/}}
{{- define "airflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "airflow.name" . }} 
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Redis name
*/}}
{{- define "airflow.redis.name" -}}
{{- default (printf "%s-redis" (include "airflow.name" .)) .Values.redis.name -}} 
{{- end }}

{{/*
Postgres name
*/}}
{{- define "airflow.postgres.name" -}}
{{- default (printf "%s-postgres" (include "airflow.name" .)) .Values.postgres.name -}}  
{{- end }}

{{/*
Webserver name
*/}}
{{- define "airflow.webserver.name" -}}
{{- default (printf "%s-webserver" (include "airflow.name" .)) .Values.airflow.webserverName -}}
{{- end }}

{{/*
Scheduler name
*/}}
{{- define "airflow.scheduler.name" -}}
{{- default (printf "%s-scheduler" (include "airflow.name" .)) .Values.airflow.schedulerName -}}
{{- end }}

{{/*
Worker name
*/}}
{{- define "airflow.worker.name" -}}
{{- default (printf "%s-worker" (include "airflow.name" .)) .Values.airflow.workerName -}}
{{- end }}

{{/*
Triggerer name
*/}}
{{- define "airflow.triggerer.name" -}}
{{- default (printf "%s-triggerer" (include "airflow.name" .)) .Values.airflow.triggererName -}} 
{{- end }}

{{/*
ConfigMap name
*/}}
{{- define "airflow.config.name" -}}
{{ printf "%s-config" (include "airflow.name" .) }} 
{{- end }}

{{/*
DAG ConfigMap name
*/}}
{{- define "airflow.dags.name" -}}
{{ printf "%s-dags" (include "airflow.name" .) }} 
{{- end }}