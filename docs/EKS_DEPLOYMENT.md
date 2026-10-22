# Deploying the self-service data pipeline to EKS

This is the step-by-step path from nothing to a working self-service data
product platform on Amazon EKS: Kafka + storage reachable, the self-service
UI live, and a data product created through it actually spinning up real
pods. It mirrors [`AKS_DEPLOYMENT.md`](AKS_DEPLOYMENT.md) — same
architecture, same charts — with the AWS-specific parts (ECR/GHCR, IAM,
S3, EKS load balancer annotations) filled in.

It assumes you already have an EKS cluster and a container registry
(ECR, or GHCR — see [Step 2](#step-2--build-and-push-all-images) for why
this repo's own CD pipeline uses GHCR instead of ECR). If not, see
[Step 0](#step-0--cluster-and-registry-one-time-setup).

**The automated GitHub Actions path (`aws-pipelines/`) has been temporarily
removed from this repo** — see git history to restore it. Everything below
still works as a manual path in the meantime; references to the automated
pipeline further down are stale until it's brought back.

## Architecture recap

- **Producer** — one generic image per pathway (file, topic); each adaptor
  absorbed its own schema mapper stage entirely, so a data product is a
  single Deployment (`dpn-producer-<pathway>-<product>`) regardless of
  pathway. Created **dynamically** by the self-service UI via the
  Kubernetes API (`service/k8s_ops.py`) — not via Helm, not via this
  repo's CI/CD.
- **Consumer** — two images, one Deployment each, **fixed singletons**
  (`consumer/{file,topic}/extractor`) — each extractor absorbed its own
  schema mapper stage entirely too, same as the producer side — helm-
  deployed once, shared across every data product.
- **Self-service UI** — one Deployment (`service/charts`), the control
  plane. Runs with `DPN_RUNTIME=kubernetes` and a ServiceAccount scoped (via
  RBAC) to create/patch/delete Deployments only in the producer namespace.

## Step 0 — Cluster and registry (one-time setup)

```bash
aws configure   # or an assumed role via `aws sts assume-role` / SSO

eksctl create cluster \
  --name dpn-eks \
  --region <aws-region> \
  --nodegroup-name dpn-nodes \
  --nodes 3 \
  --managed

aws eks update-kubeconfig --region <aws-region> --name dpn-eks
kubectl create namespace ns-dpn-dp-01
```

Registry — pick one:

- **ECR** (needs an EKS node IAM role with `AmazonEC2ContainerRegistryReadOnly`
  — attach it via `eksctl create iamserviceaccount` or the node group's own
  role, no `imagePullSecret` needed either way):

  ```bash
  aws ecr create-repository --repository-name dsi/dpn-producer-file-adaptor-generic
  # ...repeat per image name in Step 2, or use one repository with distinct tags.
  ```

- **GHCR** — what this repo's own `dpn-self-service-cd.yaml` actually uses
  (see that file's header comment: it shares the same `ghcr.io/energy-dsi`
  namespace the older `dsi-data-pipelines-ghcr-cd.yaml` pipeline already
  pushes to). GHCR packages are private by default, so every namespace
  needs a pull secret — see Step 3.

## Step 1 — Kafka and storage reachable from the cluster

This repo's charts assume:
- A Kafka bootstrap endpoint reachable as
  `dpn-kafka.<namespace>.svc.cluster.local:9092` (or your own — every
  values.yaml below is overridable).
- S3 (or Azure Blob, if this deployment's data products use the Azure
  pathway instead — `service/env_builder.py`'s `PLATFORM_CLOUD_PROVIDER`
  picks per-deployment, not per-chart) reachable with credentials stored in
  Kubernetes Secrets referenced by the charts (`awsAccessSecret`,
  `storageConnectionSecret`, `kafkaOauthSecretName`, `kafkaTlsSecretName`).

If you don't already have a Kafka cluster for this environment, install one
(e.g. Strimzi, MSK, or the official `apache/kafka` image via a simple
Deployment for non-production). Create the referenced Secrets before
deploying any component — every chart's env vars reference them with
`optional: true`, so a missing secret won't fail the Helm install, but the
container itself will fail its own `validate_cloud_config()`/
`validate_kafka_config()` checks at startup and exit — check `kubectl logs`
if a pod is crash-looping.

For S3 specifically, this deployment's own IAM user/role needs at minimum
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, and
`s3:CreateBucket` (data products auto-create their own source/target
buckets on first run — see `utils/data_transection.py`'s
`ensure_container_exists()`) on whichever bucket name(s) this account's
data products will use.

```bash
kubectl create secret generic aws-access-secret -n ns-dpn-dp-01 \
  --from-literal=AWS_ACCESS_KEY_ID=<key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret>
```

If you'd rather not hold long-lived AWS keys in a Secret at all, attach an
IRSA (IAM Roles for Service Accounts) role to the relevant ServiceAccounts
instead and drop `awsAccessSecret`/`AWS_ACCESS_KEY_ID`/
`AWS_SECRET_ACCESS_KEY` from the values below entirely — the AWS SDK
already falls back to the pod's own IRSA-injected credentials when none are
explicitly configured. This repo's charts don't wire that up themselves
(see `docs/AKS_DEPLOYMENT.md`'s "Known gaps" section for the equivalent
Azure caveat), so it's a values override + a ServiceAccount annotation you
add on top, not a flag any chart exposes.

## Step 2 — Build and push all images

Either run the automated pipeline (`dpn-self-service-cd.yaml`, GHCR) or do
it manually once to verify the path end-to-end. This example uses ECR —
substitute the GHCR login/tag if you picked that instead.

```bash
REGISTRY=<account-id>.dkr.ecr.<region>.amazonaws.com
TAG=1.0.0

aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin $REGISTRY

build_and_push() {
  local dockerfile="$1" image_repo="$2"
  docker build -t "$REGISTRY/dsi/${image_repo}:$TAG" -f "$dockerfile" .
  docker push "$REGISTRY/dsi/${image_repo}:$TAG"
}

build_and_push blueprints/producer/file/adaptor/Dockerfile        dpn-producer-file-adaptor-generic
build_and_push blueprints/producer/topic/adaptor/Dockerfile       dpn-producer-topic-adaptor-generic

build_and_push blueprints/consumer/file/extractor/Dockerfile        consumer-file-extractor
build_and_push blueprints/consumer/topic/extractor/Dockerfile       consumer-topic-extractor

build_and_push service/Dockerfile dpn-self-service
```

Plain `docker build`/`docker push` — matches
[`AKS_DEPLOYMENT.md`](AKS_DEPLOYMENT.md)'s own reasoning for avoiding a
registry-managed remote build service, and matches what this repo's real
AWS CD pipeline does today (see that file's `build_and_push` shell
function).

## Step 3 — Deploy the consumer singletons

```bash
# GHCR only — skip if using ECR (nodes pull via their own IAM role, no
# pull secret needed there, same as ACR on AKS). Needs a GitHub PAT
# (classic, read:packages scope) — see dpn-self-service-eks-setup.md's
# step 5 for the fuller rationale.
kubectl create secret docker-registry ghcr-pull-secret \
  --docker-server=ghcr.io \
  --docker-username=<github-username> \
  --docker-password=<PAT with read:packages> \
  -n ns-dpn-dp-01

helm upgrade --install dpn-consumer-file-extractor blueprints/consumer/file/extractor/charts \
  --set namespace=ns-dpn-dp-01,imageRegistry=$REGISTRY/dsi,imageTag=$TAG -n ns-dpn-dp-01

helm upgrade --install dpn-consumer-topic-extractor blueprints/consumer/topic/extractor/charts \
  --set namespace=ns-dpn-dp-01,acrName=$REGISTRY/dsi,imageTag=$TAG -n ns-dpn-dp-01
```

Note the inconsistent override key between the file-pathway and
topic-pathway consumer charts (`imageRegistry` vs `acrName`) — a real,
pre-existing quirk in how each chart's `templates/deployment.yaml` builds
its image reference, not an AWS-specific one; see each chart's own
`values-local-dpn01.yaml` comment for exactly which knob it renders.

Verify: `kubectl get deployments -n ns-dpn-dp-01` should show both
`Running` with `1/1` ready.

## Step 4 — Deploy the self-service UI

```bash
helm upgrade --install dpn-self-service service/charts \
  --set namespace=ns-dpn-dp-01,producerNamespace=ns-dpn-dp-01 \
  --set imageRegistry=$REGISTRY/dsi,imageTag=$TAG \
  --set imagePullSecretName=ghcr-pull-secret \
  --set env.DPN_IMAGE_REGISTRY="$REGISTRY/dsi" \
  --set env.DPN_IMAGE_TAG=$TAG \
  --set env.DPN_NAMESPACE=ns-dpn-dp-01 \
  --set env.DPN_BOOTSTRAP_SERVER="<your-kafka-bootstrap>:9092" \
  --set env.DPN_CLOUD_PROVIDER=aws \
  --set platformStorageSecretName=aws-access-secret \
  --set loadBalancer.cloud=aws \
  -n ns-dpn-dp-01

kubectl rollout status deployment/dpn-self-service -n ns-dpn-dp-01
```

`loadBalancer.cloud=aws` (see `service/charts/templates/lb.yaml`) renders
the EKS in-tree Service annotation
(`service.beta.kubernetes.io/aws-load-balancer-internal: "true"`) instead
of AKS's — this is the one template in this repo that actually branches on
cloud provider; every other chart's Kubernetes manifests are identical
across clouds.

Drop `imagePullSecretName` entirely if using ECR — nodes already pull from
it via their own IAM role, same as ACR on AKS.

This installs, alongside the UI's own Deployment/Service:
- A ServiceAccount for the UI.
- A `Role`/`RoleBinding` (`templates/rbac.yaml`) scoping it to create/patch/delete
  Deployments and read pod logs — **only** in `producerNamespace`, never
  cluster-wide.
- A PVC for the JSON registry (`service/data/data_products.json`) so it
  survives pod restarts. This is a single-replica, single-writer stopgap —
  move the registry to Postgres before running more than one UI replica or
  needing HA (see `service/README.md`, "Moving this to production").
  It's also mirrored to cloud storage on every write for disaster recovery
  (`service/registry_backup.py`) — see that module's docstring.

Access it locally to verify before wiring up an Ingress:

```bash
kubectl port-forward -n ns-dpn-dp-01 svc/dpn-self-service 8000:8000
# open http://localhost:8000
```

## Step 5 — Create a data product and watch it deploy for real

Through the UI (`/create-new-data-product` → review → confirm), or directly:

```bash
curl -X POST http://localhost:8000/data-products/confirm \
  -d "product_type=eks-smoke-test" -d "pipeline_type=file" -d "schedule_seconds=60" \
  -d "source_container_name=eks-smoke-test-src" -d "target_container_name=eks-smoke-test-target" \
  -d "target_topic_name=dpn-producer-file-eks-smoke-test-target"

kubectl get deployments -n ns-dpn-dp-01 | grep eks-smoke-test
# dpn-producer-file-eks-smoke-test   1/1     1            1

kubectl logs -n ns-dpn-dp-01 deployment/dpn-producer-file-eks-smoke-test
```

If the pod shows `CrashLoopBackOff`, it's almost always a missing/incorrect
Kafka or S3 Secret — check the logs for `config.validation.failed` (from
`utils/config_validator.py`), which names exactly which env var was empty
or malformed. `Azure container creation failed`/an S3 equivalent instead
means the AWS credentials are present but lack the S3 permissions listed
in [Step 1](#step-1--kafka-and-storage-reachable-from-the-cluster).

## Rolling back / removing a data product

- **Pause** (keep running, skip scheduled work): the UI's Pause button, or
  `POST /data-products/{product_type}/pause` — patches `EXECUTION_MODE` in
  place (a normal rolling update on Kubernetes, unlike the local Podman
  backend which has to recreate the container).
- **Stop** (remove the Deployment): the UI's Stop button, or
  `POST /data-products/{product_type}/stop`.
- **Uninstall the platform**: `helm uninstall dpn-self-service dpn-consumer-file-extractor dpn-consumer-file-mapper dpn-consumer-topic-extractor dpn-consumer-topic-mapper -n ns-dpn-dp-01`,
  then manually delete any remaining per-product Deployments the UI created
  (`kubectl get deployments -n ns-dpn-dp-01 -l 'app'`) — Helm never tracked
  those, since they weren't created by `helm install`.

## Known gaps to close before treating this as production-hardened

Same list as [`AKS_DEPLOYMENT.md`](AKS_DEPLOYMENT.md#known-gaps-to-close-before-treating-this-as-production-hardened),
plus:

- **No IRSA wired into any chart by default** — every producer/consumer/
  self-service pod authenticates to S3 with a plain `AWS_ACCESS_KEY_ID`/
  `AWS_SECRET_ACCESS_KEY` Secret (see Step 1) unless you add IRSA
  annotations yourself. Long-lived keys in a Secret are the simpler
  starting point but the weaker one — move to IRSA before this is a real
  production account, not just a dev/test cluster.
- **GHCR vs ECR is a per-environment choice this repo doesn't enforce.**
  `dpn-self-service-cd.yaml` is wired for GHCR specifically (see its own
  header comment on why: no extra ECR IAM permissions needed for the
  Build/Push stage, only for the Deploy stage's
  `aws eks update-kubeconfig`) — switching to ECR means updating that
  pipeline's registry login step and this doc's Step 2, not just a values
  override.
