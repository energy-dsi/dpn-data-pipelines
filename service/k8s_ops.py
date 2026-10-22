# Copyright 2026 DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-14  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Kubernetes-backed container lifecycle for data products — the AKS/EKS/GKE
runtime backend (see service/runtime.py for how this is selected).

Same role as podman_ops.py, same env vars (service/env_builder.py),
different execution surface: a data product's single adaptor container
becomes a one-replica Deployment in the self-service UI's namespace,
images pulled from ACR (or any registry — set DPN_IMAGE_REGISTRY), instead
of a local podman container. This is what "creating a data product"
actually deploys to on AKS.

Requires the `kubernetes` package (service/requirements.txt) and
either in-cluster credentials (when this app itself runs as a pod — the
normal case, via charts/self-service) or a local kubeconfig (for running
the self-service UI against a real cluster from a dev machine, e.g. while
testing this module before it's deployed).
"""
from __future__ import annotations

import os

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from service import env_builder
from service.models import DataProductDefinition

NAMESPACE = os.getenv("DPN_NAMESPACE", "ns-dpn-dp-01")

IMAGE_REGISTRY = os.getenv("DPN_IMAGE_REGISTRY", "")  # e.g. <<CONTAINER_REGISTRY_URL>> or ghcr.io/energy-dsi
IMAGE_TAG = os.getenv("DPN_IMAGE_TAG", "latest")

# Name of a docker-registry Secret (already existing in NAMESPACE) to pull
# IMAGE_REGISTRY from — same rationale as the Helm charts' imagePullSecretName
# (see e.g. service/charts/templates/deployment.yaml): required for a
# private registry the node has no other pull credentials for, such as
# GHCR. Left empty (the default), no imagePullSecrets field is set at
# all — unneeded for ACR on AKS, which nodes already pull from via their
# managed identity.
IMAGE_PULL_SECRET = os.getenv("DPN_IMAGE_PULL_SECRET", "")

# The producer image basenames' leading segment — "dpn" by default,
# producing "dpn-producer-...-generic" (what every CI/CD pipeline, on every
# cloud, builds and pushes — Azure/AWS/GCP all share the same image names
# in the same GHCR/ACR namespace, so there's no per-cloud collision to
# avoid). Overridable via DPN_IMAGE_NAME_PREFIX only if a deployment ever
# needs a distinct image namespace of its own.
IMAGE_NAME_PREFIX = os.getenv("DPN_IMAGE_NAME_PREFIX", "dpn")


def _image(pipeline_type: str) -> str:
    """Every pathway is single-container now (see env_builder.adaptor_env's
    dispatch) — there is no more 'stage' to pick between."""
    name = f"{IMAGE_NAME_PREFIX}-producer-{pipeline_type}-adaptor-generic"
    if IMAGE_REGISTRY:
        return f"{IMAGE_REGISTRY}/{name}:{IMAGE_TAG}"
    return f"{name}:{IMAGE_TAG}"


def _load_config() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


_load_config()
_apps = client.AppsV1Api()
_core = client.CoreV1Api()
_autoscaling = client.AutoscalingV2Api()


def _deployment_spec(name: str, image: str, env: dict[str, str]) -> client.V1Deployment:
    # Mirrors the static charts' dpn-tls volume (e.g.
    # producer/file/generic/adaptor/charts/templates/deployment.yaml): when
    # env_builder.KAFKA_TLS_SECRET_NAME is set, env_builder also points
    # KAFKA_SSL_CA_LOCATION at this exact mount path, so the two must stay in
    # sync — see service/env_builder.py.
    volume_mounts = []
    volumes = []
    if env_builder.KAFKA_TLS_SECRET_NAME:
        volume_mounts.append(
            client.V1VolumeMount(name="dpn-tls", mount_path="/etc/kafka/secrets", read_only=True)
        )
        # Same dpn-tls Secret, mounted a second/third time at the exact
        # default paths utils/kafka_security.py's CA-trust and keystore
        # auto-discovery already look for (DEFAULT_TRUSTSTORE_PATH /
        # DEFAULT_KEYSTORE_PATH) — this is what makes TRUSTSTORE_PASSWORD /
        # KEYSTORE_PASSWORD (see env_builder.py) actually mean something for
        # a dynamically-launched producer pod, matching the static blueprint
        # charts. The whole-secret mount above stays as the KAFKA_SSL_CA_LOCATION
        # ca.crt fallback for a cluster whose dpn-tls has no truststore.jks yet.
        volume_mounts.append(
            client.V1VolumeMount(
                name="dpn-tls",
                mount_path="/etc/secrets/truststore.jks",
                sub_path="truststore.jks",
                read_only=True,
            )
        )
        volume_mounts.append(
            client.V1VolumeMount(
                name="dpn-tls",
                mount_path="/etc/secrets/keystore.jks",
                sub_path="keystore.jks",
                read_only=True,
            )
        )
        volumes.append(
            client.V1Volume(
                name="dpn-tls",
                secret=client.V1SecretVolumeSource(
                    secret_name=env_builder.KAFKA_TLS_SECRET_NAME, optional=True
                ),
            )
        )

    # Same pattern for the OTel collector's CA (see
    # charts/tls-ca-sync/health-manifests.yaml and
    # env_builder.OTEL_TLS_SECRET_NAME) — env_builder also points
    # OTEL_EXPORTER_OTLP_CERTIFICATE at this exact mount path, so the two
    # must stay in sync.
    if env_builder.OTEL_TLS_SECRET_NAME:
        volume_mounts.append(
            client.V1VolumeMount(name="dpn-otel-ca", mount_path="/etc/otel/certs", read_only=True)
        )
        volumes.append(
            client.V1Volume(
                name="dpn-otel-ca",
                secret=client.V1SecretVolumeSource(
                    secret_name=env_builder.OTEL_TLS_SECRET_NAME, optional=True
                ),
            )
        )

    container = client.V1Container(
        name=name,
        image=image,
        env=[client.V1EnvVar(name=k, value=v) for k, v in env.items()],
        volume_mounts=volume_mounts,
        resources=client.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "256Mi"},
        ),
        security_context=client.V1SecurityContext(
            allow_privilege_escalation=False,
            run_as_non_root=True,
            run_as_user=10000,
            capabilities=client.V1Capabilities(drop=["ALL"]),
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
        ),
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": name}),
        spec=client.V1PodSpec(
            containers=[container],
            restart_policy="Always",
            volumes=volumes,
            image_pull_secrets=(
                [client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)] if IMAGE_PULL_SECRET else None
            ),
        ),
    )
    return client.V1Deployment(
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels={"app": name}),
        spec=client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": name}),
            template=template,
        ),
    )


def _deployment_exists(name: str) -> bool:
    try:
        _apps.read_namespaced_deployment(name, NAMESPACE)
        return True
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise


def _apply(name: str, image: str, env: dict[str, str]) -> None:
    """Create the Deployment if absent, else patch its env vars in place
    (a normal rolling update — no delete/recreate needed on Kubernetes,
    unlike the podman backend)."""
    spec = _deployment_spec(name, image, env)
    if _deployment_exists(name):
        _apps.patch_namespaced_deployment(name, NAMESPACE, spec)
    else:
        _apps.create_namespaced_deployment(NAMESPACE, spec)


def _hpa_spec(name: str) -> client.V2HorizontalPodAutoscaler:
    """
    Pinned at minReplicas=maxReplicas=1 — deliberately never scales. Both
    adaptor pathways are singletons with no leader election/work
    partitioning between replicas: a second concurrently-running replica
    of the file pathway would race another one to copy the same
    newly-arrived file, risking a duplicate Kafka notification to the
    Federator. This exists only so every self-service-provisioned
    Deployment has an HPA object present (matching the static charts'
    own optional one — see blueprints/producer/{file,topic}/adaptor/charts/
    templates/hpa.yaml), not to actually autoscale.
    """
    return client.V2HorizontalPodAutoscaler(
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels={"app": name}),
        spec=client.V2HorizontalPodAutoscalerSpec(
            scale_target_ref=client.V2CrossVersionObjectReference(
                api_version="apps/v1", kind="Deployment", name=name
            ),
            min_replicas=1,
            max_replicas=1,
            metrics=[
                client.V2MetricSpec(
                    type="Resource",
                    resource=client.V2ResourceMetricSource(
                        name="cpu",
                        target=client.V2MetricTarget(type="Utilization", average_utilization=80),
                    ),
                ),
            ],
        ),
    )


def _hpa_exists(name: str) -> bool:
    try:
        _autoscaling.read_namespaced_horizontal_pod_autoscaler(name, NAMESPACE)
        return True
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise


def _apply_hpa(name: str) -> None:
    spec = _hpa_spec(name)
    if _hpa_exists(name):
        _autoscaling.patch_namespaced_horizontal_pod_autoscaler(name, NAMESPACE, spec)
    else:
        _autoscaling.create_namespaced_horizontal_pod_autoscaler(NAMESPACE, spec)


def _hpa_delete(name: str) -> None:
    try:
        _autoscaling.delete_namespaced_horizontal_pod_autoscaler(name, NAMESPACE)
    except ApiException as exc:
        if exc.status != 404:
            raise


def launch(dp: DataProductDefinition) -> None:
    """One Deployment (plus its pinned-at-1 HPA — see _hpa_spec()) per
    product — every pathway's adaptor now covers its whole pipeline alone
    (see env_builder.adaptor_env's dispatch and both
    blueprints/producer/{file,topic}/adaptor/main.py's v3.0.0 changelog
    entries: each absorbed its own schema mapper)."""
    name = dp.adaptor_container_name
    _apply(name, _image(dp.pipeline_type), env_builder.adaptor_env(dp))
    _apply_hpa(name)


def container_delete(name: str) -> None:
    """Delete a single Deployment by name. Also used directly by
    ui/app.py's /consumer-pipelines/{key}/delete: unlike a producer (which the
    self-service app can always relaunch, since it knows the generic
    image/env for that pathway), the fixed consumer pipelines are deployed
    by their own separate Helm charts this app doesn't own the image/spec
    for — there is no equivalent "Start" to bring one back here, only a
    redeploy of that chart. 404 (already gone) is not an error: this is
    also what makes deleting an already-absent Deployment safe to retry.
    """
    try:
        _apps.delete_namespaced_deployment(name, NAMESPACE)
    except ApiException as exc:
        if exc.status != 404:
            raise


def stop(dp: DataProductDefinition) -> None:
    """Removes the Deployment and its HPA together — an HPA left behind
    with no scaleTargetRef Deployment to act on is just dead weight."""
    container_delete(dp.adaptor_container_name)
    _hpa_delete(dp.adaptor_container_name)


def set_execution_mode(dp: DataProductDefinition) -> None:
    """Patch EXECUTION_MODE in place — a normal rolling update, not a
    delete/recreate (that's a podman_ops.py-specific limitation)."""
    launch(dp)


def set_container_execution_mode(name: str, mode: str) -> None:
    """
    Patch just the EXECUTION_MODE env var on an already-existing Deployment
    this app doesn't own the full spec for — the fixed consumer pipelines
    (see service/consumers.py), deployed by their own separate Helm charts
    rather than through launch()/_apply() above, so there is no
    env_builder-built env dict to pass a full desired spec with here.

    Container.env has patchMergeKey "name" in the Kubernetes API, so a
    strategic merge patch naming just this one EnvVar (the default patch
    type patch_namespaced_deployment uses for a dict body) updates it in
    place and leaves every other env var/volume/image untouched, unlike
    launch()'s _apply(), which always supplies the complete desired spec.
    """
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {"name": name, "env": [{"name": "EXECUTION_MODE", "value": mode}]}
                    ]
                }
            }
        }
    }
    _apps.patch_namespaced_deployment(name, NAMESPACE, patch)


def _deployment_status(name: str) -> str:
    try:
        dep = _apps.read_namespaced_deployment_status(name, NAMESPACE)
    except ApiException as exc:
        if exc.status == 404:
            return "absent"
        raise
    ready = dep.status.ready_replicas or 0
    desired = dep.spec.replicas or 0
    if ready >= desired and desired > 0:
        return "running"
    return "pending"


def status(dp: DataProductDefinition) -> dict[str, str]:
    return {"adaptor": _deployment_status(dp.adaptor_container_name)}


# Public single-name status lookup — same underlying check as status()'s two
# calls above, exposed directly for callers (ui/app.py's
# /consumer-pipelines route) that aren't working with a DataProductDefinition pair,
# e.g. the fixed consumer pipelines in service/consumers.py.
container_status = _deployment_status


def logs(name: str, tail: int = 100) -> str:
    pods = _core.list_namespaced_pod(NAMESPACE, label_selector=f"app={name}")
    if not pods.items:
        return ""
    pod_name = pods.items[0].metadata.name
    try:
        # _preload_content=False, not the default True: kubernetes==36.0.3's
        # default deserialization for this plain-text (non-JSON) endpoint
        # returns str(<bytes response>) — the literal "b'...'" repr, escaped
        # newlines and all — instead of decoding it, so every log view
        # rendered that garbled text and run_history.record_from_logs()
        # could never parse a single record out of it (no real newlines to
        # split on). Reading the raw urllib3 response and decoding it
        # ourselves bypasses that broken auto-deserialization entirely.
        response = _core.read_namespaced_pod_log(
            pod_name, NAMESPACE, tail_lines=tail, _preload_content=False
        )
        return response.data.decode("utf-8", errors="replace")
    except ApiException:
        return ""


def run_once(name: str) -> str:
    """
    Kubernetes equivalent of podman_ops.run_once(): execs `python main.py
    --run-once` in the Deployment's running pod (see
    utils/scheduler_backend.py's run_once()), forcing one manual pipeline
    cycle regardless of EXECUTION_MODE without disturbing the pod's own
    long-running process.

    Same caveat as the podman backend — this is a separate exec session,
    not the pod's own log stream, so the caller persists the returned text
    itself (see ui/app.py).
    """
    pods = _core.list_namespaced_pod(NAMESPACE, label_selector=f"app={name}")
    if not pods.items:
        raise RuntimeError(f"no running pod for '{name}' — start the data product first")
    pod_name = pods.items[0].metadata.name

    # A dedicated CoreV1Api, not the shared module-level `_core`. stream()
    # (kubernetes.stream.stream) monkey-patches the `request` method on
    # whichever ApiClient instance is bound to the method it's given, for
    # the duration of this call — reusing `_core` here would race with any
    # concurrent logs()/status() call on that same shared instance (this
    # module is called from a FastAPI threadpool plus a background task,
    # see ui/app.py's /run and /run-history routes), and an
    # unrelated call caught mid-patch fails with a WebSocket exception
    # instead of its normal REST response.
    exec_client = client.CoreV1Api()
    return stream(
        exec_client.connect_get_namespaced_pod_exec,
        pod_name,
        NAMESPACE,
        command=["python", "main.py", "--run-once"],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
