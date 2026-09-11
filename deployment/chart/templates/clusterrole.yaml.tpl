---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: toolforge-jobs-owner
  labels:
    {{- include "jobs-api.labels" . | nindent 4 }}
rules:
- apiGroups:
  - jobs-api.toolforge.org
  resources:
  - continuous-jobs
  - scheduled-jobs
  - webservice-jobs
  verbs:
  - "*"
