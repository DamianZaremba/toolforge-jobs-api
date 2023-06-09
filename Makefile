SHELL := /bin/bash

# not generating any local files
.PHONY: run image kind_load rollout build-and-deploy-local

image:
	bash -c "source <(minikube docker-env) || : && docker build --target image -f .pipeline/blubber.yaml . -t jobs-api:dev"

kind_load:
	bash -c "hash kind 2>/dev/null && kind load docker-image docker.io/library/jobs-api:dev --name toolforge || :"

rollout:
	kubectl rollout restart -n jobs-api deployment jobs-api

build-and-deploy-local: image kind_load rollout
	./deploy.sh
