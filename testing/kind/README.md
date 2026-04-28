# kind IT fixture

Bootstrap script + docs for running the v2.8.1 `k8sKindTest` suite
against a real kind cluster with the two Mongo operators
pre-installed.

## One-time setup

```
brew install kind kubectl helm          # macOS
# or: choco install kind kubernetes-cli  (Windows)
# or: apt / official installers          (Linux)
```

## Per-run

```
kind create cluster --name mex-k8s-it
./testing/kind/install-operators.sh
export MEX_K8S_IT=kind
./gradlew :app:k8sKindTest
```

`install-operators.sh` lands **cert-manager**, the **MongoDB
Community Operator (MCO)**, and the **Percona Server for MongoDB
Operator (PSMDB)** — the three pieces the v2.8.1 production
provisioning pipeline needs. Every apply is idempotent, so re-running
the script on an existing cluster is safe.

## Matrix runs (blessed-matrix soak)

The blessed matrix is Kubernetes 1.29 / 1.30 / 1.31 (milestone §7.8).
To exercise all three:

```
for v in 1.29.14 1.30.10 1.31.6; do
  kind delete cluster --name mex-k8s-it || true
  kind create cluster --name mex-k8s-it --image "kindest/node:v${v}"
  ./testing/kind/install-operators.sh
  MEX_K8S_IT=kind ./gradlew :app:k8sKindTest
done
```

`K8sBlessedMatrixIT` asserts the live cluster's reported version is
on the blessed set — so if a newer kind image slips in, that test
fails with a clear pointer to the matrix definition.

## Environment overrides

| Var | Default | Role |
|-----|---------|------|
| `KUBECTL` | `kubectl` | Binary used for every apply |
| `KIND_CTX` | `kind-mex-k8s-it` | Target context — change if your cluster is named differently |
| `CERT_MGR_VERSION` | `v1.15.1` | cert-manager release to install |
| `MCO_VERSION` | `0.10.0` | MCO release to install |
| `PSMDB_VERSION` | `1.17.0` | Percona operator release to install |

## What the kind IT suite covers

- `KubeClientKindIT` — kubeconfig discovery, ApiClient factory,
  `/version` probe.
- `LiveApplyKindIT` — YAML apply + delete + update idempotency +
  404-on-delete safety.
- `K8sBlessedMatrixIT` — asserts the live kind cluster's version is
  on the blessed matrix (run three times with the three blessed
  images to cover all of 1.29 / 1.30 / 1.31).

The operators installed by this script make deeper provisioning ITs
runnable without further manual setup — they unblock the test
surface targeted by the v2.8.1 Q2.8-L hardening workstream.
