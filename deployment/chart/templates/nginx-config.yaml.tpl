apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-config
  labels:
    {{- include "jobs-api.labels" . | nindent 4 }}
data:
  nginx.conf: |
    user nginx;
    worker_processes auto;

    events {
        worker_connections 1024;
    }

    http {
        server {
            listen 8443 ssl;

            # NOTE! This certificate stuff is really important, since both the
            # API gateway (the client in this context) and jobs-api (the server)
            # verify that each other have certificates signed by the api gateway
            # backend CA.
            ssl_certificate        /etc/nginx/api-gateway-ssl/tls.crt;
            ssl_certificate_key    /etc/nginx/api-gateway-ssl/tls.key;
            ssl_client_certificate /etc/nginx/api-gateway-ssl/ca.crt;
            ssl_verify_client      on;
            ssl_protocols          TLSv1.2;
            ssl_ciphers            HIGH:!aNULL:!MD5;

            location .*/logs$ {
                proxy_pass http://127.0.0.1:8000;
                # logs have to wait for the pods to come up to start streaming
                # so they might take a long time
                proxy_read_timeout 10m;
                # If the app passes X-Accel-Buffering to disable nginx response buffering,
                # we also need to pass that to the api-gateway nginx instance.
                proxy_pass_header "X-Accel-Buffering";
            }

            location / {
                proxy_pass http://127.0.0.1:8000;
                # If the app passes X-Accel-Buffering to disable nginx response buffering,
                # we also need to pass that to the api-gateway nginx instance.
                proxy_pass_header "X-Accel-Buffering";
            }
        }

        server {
            listen 9000;

            location = /metrics {
                proxy_pass http://127.0.0.1:9200;
            }

            location = /healthz {
                proxy_pass http://127.0.0.1:8000;
            }
        }
    }
