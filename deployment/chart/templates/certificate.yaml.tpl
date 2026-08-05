---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: "jobs-api-certificate"
  labels:
    name: "jobs-api"
spec:
  commonName: "service:jobs-api"
  dnsNames:
    - "jobs-api.{{ .Release.Namespace }}.svc"
    - "jobs-api.{{ .Release.Namespace }}.svc.{{ .Values.certificates.internalClusterDomain }}"
  secretName: "jobs-api-certificate"
  subject:
    organizations:
      - toolforge
  usages:
    - server auth
    - client auth
  duration: "504h" # 21d
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef: {{ .Values.certificates.apiGatewayCa | toYaml | nindent 4 }}
