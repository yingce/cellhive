{{- define "cellhive.labels" -}}
app.kubernetes.io/name: cellhive
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "cellhive.secretName" -}}
{{- if .Values.existingSecret }}{{ .Values.existingSecret }}{{ else }}{{ .Release.Name }}-secrets{{ end -}}
{{- end -}}

{{- define "cellhive.doRuntimeService" -}}
{{- if .Values.doRuntime.gate }}{{ .Release.Name }}-do-runtime-gated{{ else }}{{ .Release.Name }}-do-runtime{{ end -}}
{{- end -}}

{{- define "cellhive.commonEnv" -}}
- name: CELLHIVE_CELL_URL
  value: "http://{{ .Release.Name }}-cell-agent:7001"
- name: CELLHIVE_DATA_DIR
  value: {{ .Values.config.dataDir | quote }}
- name: CELLHIVE_RUNTIME_DIR
  value: {{ .Values.config.runtimeDir | quote }}
- name: CELLHIVE_WORKERD
  value: /usr/local/bin/workerd
- name: CELLHIVE_USER_RUNTIME_JS
  value: /app/workerd/user-runtime
- name: CELLHIVE_DO_RUNTIME_JS
  value: /app/workerd/do-runtime
- name: CELLHIVE_FACADES_JS
  value: /app/workerd/platform/facades.js
{{- end -}}
