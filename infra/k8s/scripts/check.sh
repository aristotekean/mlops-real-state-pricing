#!/bin/sh
# Static check of infra/k8s, meant as a pre-commit step: .env renders into
# infra/k8s/settings and infra/k8s/generated (see render-env.sh), both
# kustomizations must build, and the committed migrations ConfigMap must match
# infra/database (see render-migrations.sh). Client-side only. Commit
# infra/k8s/settings if the render changed it.
#
#   bash infra/k8s/scripts/check.sh
set -eu

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

bash "$ROOT/infra/k8s/scripts/render-env.sh" > /dev/null
kubectl kustomize "$ROOT/infra/k8s" > /dev/null
kubectl kustomize "$ROOT/infra/k8s/generated" > /dev/null
sh "$ROOT/infra/k8s/scripts/render-migrations.sh" --check
echo "infra/k8s: .env rendered, kustomize build OK, migrations ConfigMap up to date"
