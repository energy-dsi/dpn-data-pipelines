# DPN Data Product Self-Service (prototype)

This is a working proof of concept for the self-service architecture discussed
for `dpn-data-pipeline`: a business user should be able to define a new data
product (name, integration pathway, schedule) through a UI, and have the
producer container(s) for it spin up automatically — without anyone copying
a blueprint folder, renaming it, and building a new Docker image. There is no
"schema type" concept at all: organisation and cloud provider are one-time,
platform-level settings for the whole self-service deployment (see
`service/env_builder.py`'s `PLATFORM_ORG_NAME`/`PLATFORM_CLOUD_PROVIDER`),
never chosen per product.

## What changed to make this possible

Both integration pathways have genericized runtime images under
`blueprints/producer/{file,topic}/`. Every actual identifier (`PRODUCT_NAME`,
`orgName`, container/topic names) is read purely from environment variables —
one image serves **every** data product on a pathway; a new data product is
just a new set of environment variables passed to `podman run` (or, in
production, a Kubernetes pod spec).

Both pathways are single-container: each adaptor absorbed its own schema
mapper stage entirely, so a data product is one container/Deployment
regardless of pathway. The **file** pathway's adaptor reads the source
container and writes straight to the target container under the exact
filename it arrived with (no renaming at all), then publishes one Kafka
notification — no intermediate staging container. The **topic** pathway's
adaptor reads the source topic, enriches each message with the same
metadata headers (org/product/timestamp/offset) the removed schema mapper
always added, and produces straight to the target topic it computes
itself — no intermediate RAW topic. See
`blueprints/producer/file/adaptor/main.py` and
`blueprints/producer/topic/adaptor/main.py`'s changelog entries for the
full rationale on each.

The consumer side mirrors this exactly, one merge behind: each of the two
fixed platform pipelines (file/topic) is now a single extractor container
too — the old schema_mapper stage was absorbed the same way. See
`blueprints/consumer/file/extractor/main.py` and
`blueprints/consumer/topic/extractor/main.py`'s changelog entries.

## The pieces

- `service/models.py` — `DataProductCreate` (the fields a business user
  supplies: `product_type`, `pipeline_type` [file/topic], `schedule_seconds`,
  `execution_mode`) and `DataProductDefinition` (adds the storage container /
  Kafka topic names for whichever pathway was chosen, plus the snapshotted
  platform `org_name`/`cloud_provider`). Those names are *proposed*
  deterministically from `product_type` but are plain, overridable fields,
  not fixed derivations — see the review step below. File-pathway container
  names are additionally validated against the target cloud's real
  Azure/S3/GCS naming rules (`validate_storage_container_name()`) before a
  product is ever created.
- `service/registry.py` — a flat JSON file standing in for the Postgres
  registry table from the target architecture, mirrored to cloud storage on
  every write for disaster recovery (`service/registry_backup.py`).
  Swappable without touching the FastAPI routes or `podman_ops.py`/`k8s_ops.py`.
- `service/podman_ops.py` (local) / `service/k8s_ops.py` (AKS/EKS/GKE) —
  launches the one generic image for whichever pathway the product uses,
  with that product's env vars. `adaptor_container_name` (Kubernetes
  Deployment / podman container name) is
  `dpn-producer-<pipeline_type>-<product_type>` — distinct from any
  storage container name, which is a name in a cloud storage account, not
  a Kubernetes resource.
- `ui/app.py` + `templates/` — the FastAPI UI, as a two-step flow:
  1. `/create-new-data-product` — product name, file-or-topic pathway, schedule, execution mode.
  2. `/data-products/preview` → `templates/review.html` — shows the proposed
     container/topic names for that pathway (file: source/target storage
     containers — free-text, not derived from the product name, only
     charset/length-validated — plus one notification topic, all
     user-editable; topic: source topic and consumer group id, plus the
     adaptor's always-computed target topic shown read-only) so they can be
     confirmed or overridden before anything launches — every confirmed
     name must be brand-new (`service/precheck.py` checks the real broker/
     storage account and blocks creation on a collision), since this
     platform only ever creates fresh topics/containers for a product.

  The index page lists data products with live container status (a single
  Adaptor status per product, either pathway), start/stop, and a logs view
  per product.

## Running it locally

```bash
# 1. Bring up the shared platform (Kafka + MinIO as a local blob-storage stand-in)
podman machine start   # if not already running
podman-compose -f service/compose.yaml up -d

# 2. Install the UI's dependencies and run it on the host
#    (see service/Dockerfile for why this runs on the host, not in a
#    container, for the local demo)
pip install -r service/requirements.txt

# DPN_AWS_ENDPOINT_URL defaults to http://dpn-minio:9000 (service/env_builder.py),
# the podman-compose network hostname for MinIO — only reachable from inside that
# network. Running uvicorn on the host instead, this must point at MinIO's
# published port (9010, per service/compose.yaml) or every storage-container
# precheck (new-product preview/confirm) hangs until oauth2-proxy's upstream
# timeout kills it with a 502.
export DPN_AWS_ENDPOINT_URL=http://localhost:9010
uvicorn ui.app:app --reload --port 8000

# 3. Open http://localhost:8000, click "New Data Product", fill in:
#      product_type:   demo-product
#      pipeline_type:  file (or topic)
#      schedule:       30
#    Review the proposed container/topic names on the next screen (override
#    if needed — e.g. to point at an already-existing source topic/bucket),
#    then confirm. Submitting launches dpn-producer-file-demo-product (file
#    pathway) or dpn-producer-topic-demo-product (topic pathway) — one
#    container either way — from the generic image for that pathway — no
#    build step, no folder copy.
```

The adaptor image is built once automatically (`podman build`) the first
time any data product is created on a given pathway; subsequent products
reuse the already-built image.

## Scheduling — no Airflow here, by design (for now)

Every container uses `SCHEDULER_BACKEND=interval` — a fully supported backend
already built into `utils/scheduler_backend.py`, not a hack. Each container
self-schedules: the "schedule" field maps to `scheduleInterval` (file
pathway's poll cadence) or `TOPIC_TASK_TIMEOUT_SECS` (topic pathway's
consumer-session window — its own IntervalBackend tick barely matters since
`run_window()` already blocks for that long before returning). No Airflow,
Postgres, or Redis is required to run this prototype.

The tradeoff: every data product needs its own always-running container, and
there's no centralized run history/alerting across products. Moving to the
DAG-factory + `KubernetesPodOperator`/`kafka-trigger` architecture discussed
alongside this prototype is a deliberate follow-up increment, not something
this prototype does implicitly — it was scoped out for now to keep the
infrastructure footprint small while proving the core self-service concept.

## Keycloak authentication (Kubernetes deployment)

The UI itself does no login — like `dpn-portal` in `dpn-health-monitoring-service`
(`app/dpn-portal/main.py`), it sits behind an `oauth2-proxy` sidecar that terminates
the OIDC login against the same `dpn-realm` this repo already talks to for Kafka
OAuth. `service/charts` renders that proxy as part of this same Helm release
(`oauth2-proxy` templates under `charts/templates/`), toggled by
`oauth2Proxy.enabled` in `charts/values.yaml` (**off by default** — see the
comment above that key: it's disabled until `oauth2Proxy.externalUrl` is set
per environment, since that value is `required` the moment this flips on):

```
browser ──http──▶ oauth2-proxy (LB) ──http──▶ dpn-self-service:8000 (ClusterIP)
                        │ OIDC
                        ▼
              dpn-keycloak-service-lb.ns-dpn-01.svc.cluster.local:8443
```

- **Reuses `dpn-service-client` / `dpn-realm`** — the same Keycloak client already
  configured for Kafka OAuth (`env.DPN_KAFKA_OAUTH_*` in `values.yaml`) — and its
  client secret is read a second time from the same `kafkaOauthSecretName` Secret,
  so there's nothing new to provision in Keycloak or Key Vault.
- **⚠️ That client is provisioned machine-to-machine ONLY by default — this is
  the step that actually blocks login, not anything in this chart.**
  `dpn-authentication-service/charts/keycloak/scripts/configure-keycloak.sh`
  (the only automation that touches `dpn-service-client`) creates it with
  `standardFlowEnabled: false` and `redirectUris: []` — it was built for
  Kafka's server-to-server OAuthBearer exchange, which needs no browser leg.
  oauth2-proxy's OIDC login *requires* the standard (authorization code) flow.
  Before enabling `oauth2Proxy` here, in the Keycloak admin console for
  `dpn-realm` → Clients → `dpn-service-client` → Settings, confirm/set:
  - **Standard flow**: On
  - **Valid redirect URIs**: add `<oauth2Proxy.externalUrl>/oauth2/callback`
  - **Valid post logout redirect URIs**: add `<oauth2Proxy.externalUrl>`
  - **Web origins**: add `<oauth2Proxy.externalUrl>` (or `+`)
  If `dpn-portal` already works, its callback URL was added the same manual
  way — that only proves the client's standard flow is on, not that this UI's
  own callback URL is registered too; each UI's URL must be added separately.
  Also confirm the `dpnadmin`/`dpnreader` realm roles are assigned to the
  actual human users who should get admin/read-only access — the script above
  only maps those roles onto the client's own service-account user, not onto
  any person.
- **`oauth2Proxy.externalUrl` is required** — the browser-facing address of the
  proxy (what `charts/templates/lb.yaml`'s external LB now points at instead of the
  app directly). Register `<externalUrl>/oauth2/callback` as a redirect URI, and
  `<externalUrl>` as a post-logout redirect URI, on the `dpn-service-client` client
  (see the client-config caveat above — this is the same step, just from this
  chart's side of the required value).
- **Three roles may log in** (`oauth2Proxy.keycloak.allowedRoles`): `dpnadmin`,
  `dpnoperator`, and `dpnreader`. `dpnadmin` and `dpnoperator` can both
  create/start/stop/pause/resume/run a data product — `service/auth.py`'s
  `require_operator` dependency enforces this server-side (403 otherwise) on
  those routes; only `dpnadmin` can permanently delete one, gated by the
  separate `require_admin` dependency. `dpnreader` gets a read-only view
  (`index.html` hides those controls entirely for a non-operator/non-admin).
- **Plain HTTP, cookie session** — no TLS termination or Redis session store here
  (unlike the health-monitoring cluster's nine-UI setup), since this is a single
  low-traffic internal tool behind the same internal-only Azure LB it always had.
- **Local dev is unaffected** — `uvicorn ui.app:app --reload` (no
  oauth2-proxy in front, and no `DPN_REQUIRE_PROXY_AUTH` env var since no
  chart is involved) sees neither `X-Forwarded-User` nor `X-Forwarded-Groups`,
  and `auth.is_admin()` treats that as unrestricted, exactly like today.
- **A real deployment rejects requests that skip oauth2-proxy** — every chart
  install sets `DPN_REQUIRE_PROXY_AUTH=true` whenever `oauth2Proxy.enabled`
  (`charts/templates/deployment.yaml`), which `ui/app.py`'s
  `require_proxy_auth` middleware uses to 401 any request missing those same
  two headers, on every route (not just the mutating ones `require_admin`/
  `require_operator` already gated) — including a `kubectl port-forward`
  straight to the pod or the `dpn-self-service` ClusterIP, which used to
  bypass oauth2-proxy (and Keycloak) entirely and land as full admin access.
  `/healthz` stays exempt since kubelet's probes hit the pod directly.

## Moving this to production

- `registry.py` → Postgres, per the architecture discussion (product
  registry table), written to via this same FastAPI backend.
- `podman_ops.py` → a Kubernetes-native equivalent: either an Airflow DAG
  factory using `KubernetesPodOperator` reading the registry (recommended
  first step, keeps the existing Airflow investment), or a custom
  `DataProduct` CRD + operator for a more cloud-native fit. Either way, the
  same two generic images built here are what gets deployed — this
  prototype already proves they don't need to be product-specific.
- Cloud/Kafka connection details (currently hardcoded to the local
  MinIO/Kafka demo endpoints in `podman_ops.py`) become per-environment
  config alongside the per-product fields in the registry.

## Licensing note

Kafka here is the official `apache/kafka` image (Apache License 2.0).
**MinIO Server is AGPLv3**, not Apache 2.0 — it's used only as a disposable
local stand-in for cloud blob storage (mirroring this repo's existing
`awsEndpointUrl`/`minio-service` convention in the blueprint `values.yaml`
files). Before treating this as a fully Apache-2.0 stack, swap MinIO for
real cloud storage or an Apache-2.0 alternative (Localstack, SeaweedFS).
FastAPI/Starlette/Uvicorn/Pydantic (this UI's dependencies) are MIT-licensed,
not Apache 2.0 — flagging this since "fully Apache 2.0" was an explicit
requirement for the production architecture; it's a permissive license but
not a literal match.
