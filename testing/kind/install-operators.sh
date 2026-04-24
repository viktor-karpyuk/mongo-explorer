#!/usr/bin/env bash
#
# v2.8.1 Q2.8-L — kind IT fixture bootstrapper.
#
# Installs the two Mongo operators the v2.8.1 production pipeline
# provisions against — MongoDB Community Operator (MCO) and Percona
# Server for MongoDB Operator (PSMDB) — into an existing kind
# cluster, so `./gradlew :app:k8sKindTest` can exercise real
# provisioning ITs instead of bottoming out at /version probes.
#
# Usage:
#
#   kind create cluster --name mex-k8s-it
#   ./testing/kind/install-operators.sh
#   export MEX_K8S_IT=kind
#   ./gradlew :app:k8sKindTest
#
# Safe to re-run — every step is idempotent (apply / create ... ||
# true). Intentionally uses the kubectl on PATH rather than
# embedding a specific version so the script tracks whatever the
# user has.

set -euo pipefail

KUBECTL=${KUBECTL:-kubectl}
KIND_CTX=${KIND_CTX:-kind-mex-k8s-it}

# ------------------------------------------------------------------
# 0. Sanity — make sure the expected kind context is the target.
# ------------------------------------------------------------------
if ! "$KUBECTL" config get-contexts -o name | grep -qx "$KIND_CTX"; then
  echo "kind context '$KIND_CTX' not found. Create one with:"
  echo "    kind create cluster --name mex-k8s-it"
  exit 1
fi
"$KUBECTL" config use-context "$KIND_CTX" >/dev/null

echo "▶ installing operators into $KIND_CTX"

# ------------------------------------------------------------------
# 1. cert-manager — required by psmdb-sharded + mco-tls-keyfile
#    templates. Pin a minor version the v2.8.1 blessed matrix has
#    been smoke-tested against.
# ------------------------------------------------------------------
CERT_MGR_VERSION=${CERT_MGR_VERSION:-v1.15.1}
echo "  • cert-manager $CERT_MGR_VERSION"
"$KUBECTL" apply -f \
  "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MGR_VERSION}/cert-manager.yaml" \
  >/dev/null
"$KUBECTL" -n cert-manager rollout status deploy/cert-manager --timeout=180s
"$KUBECTL" -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s
"$KUBECTL" -n cert-manager rollout status deploy/cert-manager-cainjector --timeout=180s

# ------------------------------------------------------------------
# 2. MongoDB Community Operator (MCO).
# ------------------------------------------------------------------
MCO_VERSION=${MCO_VERSION:-0.10.0}
echo "  • MongoDB Community Operator $MCO_VERSION"
"$KUBECTL" create namespace mongodb --dry-run=client -o yaml | "$KUBECTL" apply -f - >/dev/null
"$KUBECTL" apply \
  -f "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator/v${MCO_VERSION}/config/crd/bases/mongodbcommunity.mongodb.com_mongodbcommunity.yaml" \
  >/dev/null
"$KUBECTL" -n mongodb apply \
  -f "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator/v${MCO_VERSION}/config/rbac/role.yaml" \
  >/dev/null
"$KUBECTL" -n mongodb apply \
  -f "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator/v${MCO_VERSION}/config/rbac/role_binding.yaml" \
  >/dev/null
"$KUBECTL" -n mongodb apply \
  -f "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator/v${MCO_VERSION}/config/rbac/service_account.yaml" \
  >/dev/null
"$KUBECTL" -n mongodb apply \
  -f "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator/v${MCO_VERSION}/config/manager/manager.yaml" \
  >/dev/null
"$KUBECTL" -n mongodb rollout status deploy/mongodb-kubernetes-operator --timeout=180s

# ------------------------------------------------------------------
# 3. Percona Server for MongoDB Operator (PSMDB).
# ------------------------------------------------------------------
PSMDB_VERSION=${PSMDB_VERSION:-1.17.0}
echo "  • Percona Server for MongoDB Operator $PSMDB_VERSION"
"$KUBECTL" create namespace psmdb --dry-run=client -o yaml | "$KUBECTL" apply -f - >/dev/null
"$KUBECTL" apply \
  -f "https://raw.githubusercontent.com/percona/percona-server-mongodb-operator/v${PSMDB_VERSION}/deploy/bundle.yaml" \
  -n psmdb >/dev/null
"$KUBECTL" -n psmdb rollout status deploy/percona-server-mongodb-operator --timeout=180s

echo "✓ operators up. Run ./gradlew :app:k8sKindTest"
