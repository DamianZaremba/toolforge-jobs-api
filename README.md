# Jobs API

This is the source code of the Jobs API component of the
[Toolforge Jobs Service](https://wikitech.wikimedia.org/wiki/Portal:Toolforge/Admin/Jobs_Service),
part of [Wikimedia Toolforge](https://toolforge.org).

The jobs-api creates an abstraction layer over kubernetes Jobs, CronJobs and
Deployments to allow operating a Kubernetes installation as if it were a Grid
(like GridEngine).

## Local installation

Run `./deploy.sh`.

## Development

You need a local kubernetes cluster with a fake Toolforge installed to it. There
are several ways of doing that. The author of this README recommends the
lima-kilo project.

1. Get the lima-kilo setup on your laptop:

Follow docs at <https://gitlab.wikimedia.org/repos/cloud/toolforge/lima-kilo>

1. Build the jobs-framework-api docker image within lima-kilo

```shell
lima-kilo:~$ git clone https://gitlab.wikimedia.org/repos/cloud/toolforge/jobs-api
lima-kilo::~/jobs-api$ cd jobs-api
lima-kilo::~/jobs-api$ docker buildx build --target image -f .pipeline/blubber.yaml -t toolsbeta-harbor.wmcloud.org/toolforge/jobs-api:dev .
```

1. Load the docker image into kind (or minikube)

This way the docker image can be used in k8s deployments and such. Like having
the image on a docker registry.

```shell
lima-kilo::~/jobs-api$ kind load docker-image toolsbeta-harbor.wmcloud.org/toolforge/jobs-api:dev -n toolforge
```

1. Deploy the component into your local kubernetes:

```shell
lima-kilo::~/jobs-api$ ./deploy.sh local
```

1. At this point, hopefully, it should work:

```shell
lima-kilo::~/jobs-api$ curl -k "https://localhost:30003/jobs/v1/images/" \
  --cert ~/.toolforge-lima-kilo/chroot/data/project/tf-test/.toolskube/client.crt \
  --key ~/.toolforge-lima-kilo/chroot/data/project/tf-test/.toolskube/client.key
```

1. Development iteration:

Make code changes, and follow from step 1 onwards. Probably something like this:

```shell
lima-kilo::~/jobs-api$ git fetch --all && git reset --hard FETCH_HEAD && docker buildx build --target image -f .pipeline/blubber.yaml -t toolsbeta-harbor.wmcloud.org/toolforge/jobs-api:dev . && kind load docker-image toolsbeta-harbor.wmcloud.org/toolforge/jobs-api:dev -n toolforge && kubectl -n jobs-api rollout restart deployment/jobs-api
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to
discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[AGPLv3](https://choosealicense.com/licenses/agpl-3.0/)
