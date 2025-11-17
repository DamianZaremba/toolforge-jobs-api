# Jobs API

This is the source code of the Jobs API component of the
[Toolforge Jobs Service](https://wikitech.wikimedia.org/wiki/Portal:Toolforge/Admin/Jobs_Service),
part of [Wikimedia Toolforge](https://toolforge.org).

The jobs-api creates an abstraction layer over kubernetes Jobs, CronJobs and
Deployments to allow operating a Kubernetes installation as if it were a Grid
(like GridEngine).

## Development tricks

### Manually building into lima-kilo

If you don't want to rely on gitlab CI building the image for you or want to
tweak closely any step of the build process, you can manually build this
component from within lima-kilo from the source code.

- Make sure you mounted your toolforge repos when creating your
  [lima-kilo](https://gitlab.wikimedia.org/repos/cloud/toolforge/lima-kilo/) vm
  (see
  [TOOLFORGE_REPOS_DIR](https://gitlab.wikimedia.org/repos/cloud/toolforge/lima-kilo#mounting-the-toolforge-repos-within-the-lima-vm))
- Use the builtin script to build and deploy (check the script for details)

```shell
lima-kilo:~$ toolforge_deploy.py jobs-api local
```

## Deploying an MR into lima-kilo

Useful if you want to test someone's MR or if you don't care if your code is
built by gitlab, recommended for most use cases.

1. Start a local Toolforge cluster using
   [lima-kilo](https://gitlab.wikimedia.org/repos/cloud/toolforge/lima-kilo/).
1. Commit your changes and create a branch + MR in gitlab for them
1. Run `toolforge_deploy.py ingress-admission` to deploy the changes in
   lima-kilo

## Deploying to Toolforge

This project uses the
[standard workflow](https://wikitech.wikimedia.org/wiki/Portal:Toolforge/Admin#Deploying_a_component)
