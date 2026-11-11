# Deploying the self-service data pipeline to AKS

This is the step-by-step path from nothing to a working self-service data
product platform on Azure Kubernetes Service: Kafka + storage reachable,
the self-service UI live, and a data product created through it actually
spinning up real pods.

It assumes you already have an AKS cluster and an Azure Container Registry
(ACR). If not, see [Step 0](#step-0--cluster-and-registry-one-time-setup).

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
az login
az group create --name dpn-rg --location uksouth

az acr create --resource-group dpn-rg --name <your-acr-name> --sku Standard

az aks create --resource-group dpn-rg --name dpn-aks \
  --node-count 3 --enable-managed-identity \
  --generate-ssh-keys

# Let AKS pull from ACR with no imagePullSecret at all.
az aks update --resource-group dpn-rg --name dpn-aks --attach-acr <your-acr-name>

az aks get-credentials --resource-group dpn-rg --name dpn-aks
kubectl create namespace ns-dpn-dp-01
```

## Step 1 — Kafka and blob storage reachable from the cluster

This repo's charts assume:
- A Kafka bootstrap endpoint reachable as `dpn-kafka.<namespace>.svc.cluster.local:9092` (or your own — every values.yaml below is overridable).
- Azure Blob Storage (or AWS S3/MinIO) reachable with connection strings/keys
  stored in Kubernetes Secrets referenced by the charts (`storageConnectionSecret`, `awsAccessSecret`, `kafkaOauthSecretName`, `kafkaTlsSecretName`).

If you don't already have a Kafka cluster for this environment, install one
(e.g. Strimzi, or the official `apache/kafka` image via a simple Deployment
for non-production). Create the referenced Secrets before deploying any
component — every chart's env vars reference them with `optional: true`, so
missing secrets won't fail the deploy, but the container will fail its own
`validate_cloud_config()`/`validate_kafka_config()` checks at startup and
exit — check `kubectl logs` if a pod is crash-looping.

## Step 2 — Build and push all images

Either run the new CD pipeline (`.pipelines/azure-pipelines/cd-pipelines/dpn-self-service-cd.yaml`)
or do it manually once to verify the path end-to-end:

```bash
ACR=<your-acr-name>
TAG=1.0.0

az acr login --name $ACR

build_and_push() {
  local dockerfile="$1" image_repo="$2"
  docker build -t "$<<CONTAINER_REGISTRY_URL>>/dsi/${image_repo}:$TAG" -f "$dockerfile" .
  docker push "$<<CONTAINER_REGISTRY_URL>>/dsi/${image_repo}:$TAG"
}

build_and_push blueprints/producer/file/adaptor/Dockerfile        dpn-producer-file-adaptor-generic
build_and_push blueprints/producer/topic/adaptor/Dockerfile       dpn-producer-topic-adaptor-generic

build_and_push blueprints/consumer/file/extractor/Dockerfile        consumer-file-extractor
build_and_push blueprints/consumer/topic/extractor/Dockerfile       consumer-topic-extractor

build_and_push service/Dockerfile dpn-self-service
```

Deliberately plain `docker build`/`docker push` (authenticated via `az acr
login`) rather than `az acr build`: the latter offloads the actual build to
ACR's own managed Tasks infrastructure, which runs from ACR-side IPs outside
a network-restricted registry's firewall allow-list — a real failure hit
while validating this pipeline (`client with IP ... is not allowed access`).
`docker build`/`push` runs entirely on whatever host you're on (or the
pipeline's own agent), matching what `dsi-data-pipelines-ci.yaml` already
does successfully against this same registry.

## Step 3 — Deploy the consumer singletons

```bash
helm upgrade --install dpn-consumer-file-extractor blueprints/consumer/file/extractor/charts \
  --set namespace=ns-dpn-dp-01,acrName=$ACR,imageTag=$TAG -n ns-dpn-dp-01

helm upgrade --install dpn-consumer-topic-extractor blueprints/consumer/topic/extractor/charts \
  --set namespace=ns-dpn-dp-01,acrName=$ACR,imageTag=$TAG -n ns-dpn-dp-01
```

Verify: `kubectl get deployments -n ns-dpn-dp-01` should show both
`Running` with `1/1` ready.

## Step 4 — Deploy the self-service UI

```bash
helm upgrade --install dpn-self-service service/charts \
  --set namespace=ns-dpn-dp-01,producerNamespace=ns-dpn-dp-01 \
  --set acrName=$ACR,imageTag=$TAG \
  --set env.DPN_IMAGE_REGISTRY="$<<CONTAINER_REGISTRY_URL>>/dsi" \
  --set env.DPN_IMAGE_TAG=$TAG \
  --set env.DPN_NAMESPACE=ns-dpn-dp-01 \
  --set env.DPN_BOOTSTRAP_SERVER="<your-kafka-bootstrap>:9092" \
  -n ns-dpn-dp-01

kubectl rollout status deployment/dpn-self-service -n ns-dpn-dp-01
```

This installs, alongside the UI's own Deployment/Service:
- A ServiceAccount for the UI.
- A `Role`/`RoleBinding` (`templates/rbac.yaml`) scoping it to create/patch/delete
  Deployments and read pod logs — **only** in `producerNamespace`, never
  cluster-wide.
- A PVC for the JSON registry (`service/data/data_products.json`) so it
  survives pod restarts. This is a single-replica, single-writer stopgap —
  move the registry to Postgres before running more than one UI replica or
  needing HA (see `service/README.md`, "Moving this to production").

Access it locally to verify before wiring up an Ingress:

```bash
kubectl port-forward -n ns-dpn-dp-01 svc/dpn-self-service 8000:8000
# open http://localhost:8000
```

## Step 5 — Create a data product and watch it deploy for real

Through the UI (`/create-new-data-product` → review → confirm), or directly:

```bash
curl -X POST http://localhost:8000/data-products/confirm \
  -d "product_type=aks-smoke-test" -d "pipeline_type=file" -d "schedule_seconds=60"

kubectl get deployments -n ns-dpn-dp-01 | grep aks-smoke-test
# dpn-producer-file-aks-smoke-test   1/1     1            1

kubectl logs -n ns-dpn-dp-01 deployment/dpn-producer-file-aks-smoke-test
```

If the pods show `CrashLoopBackOff`, it's almost always a missing/incorrect
Kafka or storage Secret — check the logs for `config.validation.failed`
(from `utils/config_validator.py`), which names exactly which env var was
empty or malformed.

## Rolling back / removing a data product

- **Pause** (keep running, skip scheduled work): the UI's Pause button, or
  `POST /data-products/{product_type}/pause` — patches `EXECUTION_MODE` in
  place (a normal rolling update on Kubernetes, unlike the local Podman
  backend which has to recreate the container).
- **Stop** (remove the Deployments): the UI's Stop button, or
  `POST /data-products/{product_type}/stop`.
- **Uninstall the platform**: `helm uninstall dpn-self-service dpn-consumer-file-extractor dpn-consumer-file-mapper dpn-consumer-topic-extractor dpn-consumer-topic-mapper -n ns-dpn-dp-01`,
  then manually delete any remaining per-product Deployments the UI created
  (`kubectl get deployments -n ns-dpn-dp-01 -l 'app' `) — Helm never tracked
  those, since they weren't created by `helm install`.

## Known gaps to close before treating this as production-hardened

- **No image signing/scanning wired into `dpn-self-service-cd.yaml`.** The
  existing `dsi-data-pipelines-ci.yaml`/`dsi-data-pipelines-cd.yaml`
  pipelines have Checkmarx, Sonar, cosign signing, and JFrog X-Ray stages for
  the old per-product architecture — fold this pipeline's build/deploy steps
  into that stack rather than shipping unsigned/unscanned images.
- **Single-replica self-service UI, JSON-file registry.** Fine for the
  first real deployment; move to Postgres before scaling UI replicas.
- **`kafkaTlsSecretName`/`awsAccessSecret`/etc. are optional in every
  chart** (`optional: true` on each `secretKeyRef`) so a chart install never
  fails outright, but the container itself will refuse to run without valid
  values — create the Secrets first.
