apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ .Release.Name }}-api-gateway-server
  labels:
    {{- include "jobs-api.labels" . | nindent 4 }}
spec:
  secretName: {{ .Release.Name }}-api-gateway-server
  dnsNames:
    - "jobs-api.{{ .Release.Namespace }}.svc"
    - "jobs-api.{{ .Release.Namespace }}.svc.{{ .Values.certificates.internalClusterName }}.local"
  usages:
    - server auth
  duration: "504h" # 21d
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef: {{ .Values.certificates.apiGatewayCa | toYaml | nindent 4 }}
