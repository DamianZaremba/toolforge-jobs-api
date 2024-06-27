---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: jobs-api
  labels:
    {{- include "jobs-api.labels" . | nindent 4 }}
rules:
- apiGroups:
  - ""
  resources:
  - configmaps
  verbs:
  - watch
  - get
  - list
