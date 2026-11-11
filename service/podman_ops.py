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
Podman-backed container lifecycle for data products — the local-demo
runtime backend (see service/runtime.py for how this is selected).

This module plays the role a Kubernetes control loop (KubernetesPodOperator,
a custom operator/CRD reconciler, or Argo Workflows) would play in
production: given a DataProductDefinition, launch the *same* generic
adaptor image (one per pathway — each absorbed its own schema mapper
stage, see blueprints/producer/{file,topic}/adaptor/main.py's v3.0.0
changelog entries) with product-specific environment variables — no image
build, no blueprint-folder copy, per data product.

Locally we drive `podman` directly via subprocess rather than the
Kubernetes API, since that's the runtime this prototype stands up. On
AKS/EKS/GKE, service/k8s_ops.py is used instead (same env vars, from
service/env_builder.py, applied to Kubernetes Deployments rather than
podman containers).
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from service import env_builder
from service.models import DataProductDefinition

REPO_ROOT = Path(__file__).resolve().parent.parent

# Despite the module name, this shells out to whichever CLI DPN_CONTAINER_ENGINE
# names (default "podman", matching service/compose.yaml's documented
# `podman-compose` flow) — set DPN_CONTAINER_ENGINE=docker to drive a local
# `docker compose` stack instead with no other changes. Every subcommand used
# below (`inspect`, `run`, `rm`, `logs`, `exec`, `network create`, `build`) is
# identical between the two CLIs; the one incompatibility (podman's
# `container exists` / `image exists` / `network exists` have no docker
# equivalent) is worked around by using `inspect` for the existence checks
# instead, which both CLIs support with the same 0/non-zero exit semantics.
CONTAINER_ENGINE = os.getenv("DPN_CONTAINER_ENGINE", "podman").strip().lower()

NETWORK = "dpn-net"

# One image per pathway now — neither has a "schema_mapper" entry any
# more, both adaptors absorbed that stage entirely (see
# env_builder.adaptor_env's dispatch).
IMAGES: dict[str, str] = {
    "file": "dpn-producer-file-adaptor-generic:latest",
    "topic": "dpn-producer-topic-adaptor-generic:latest",
}
DOCKERFILES: dict[str, str] = {
    "file": "blueprints/producer/file/adaptor/Dockerfile",
    "topic": "blueprints/producer/topic/adaptor/Dockerfile",
}


def _run(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        [CONTAINER_ENGINE, *args],
        capture_output=True,
        text=True,
        check=check,
    )


def ensure_network() -> None:
    # `network inspect`, not podman's `network exists` — docker has no
    # `exists` subcommand, but both CLIs support `inspect` with the same
    # 0-if-present exit code.
    result = _run(["network", "inspect", NETWORK], check=False)
    if result.returncode != 0:
        _run(["network", "create", NETWORK])


def ensure_images_built(pipeline_type: str) -> list[str]:
    """Build this pathway's one generic image, if not present yet."""
    image = IMAGES[pipeline_type]
    dockerfile = DOCKERFILES[pipeline_type]
    # `image inspect`, not podman's `image exists` — see ensure_network().
    exists = _run(["image", "inspect", image], check=False).returncode == 0
    if exists:
        return []
    proc = subprocess.run(
        [CONTAINER_ENGINE, "build", "-f", dockerfile, "-t", image, "."],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{CONTAINER_ENGINE} build failed for {image}:\n{proc.stdout}\n{proc.stderr}")
    return [image]


def _env_args(env: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key, value in env.items():
        args += ["-e", f"{key}={value}"]
    return args


def _container_exists(name: str) -> bool:
    # `inspect`, not podman's `container exists` — see ensure_network().
    return _run(["inspect", name], check=False).returncode == 0


def launch(dp: DataProductDefinition) -> None:
    """Start the one container for this data product — every pathway is
    single-container now (see env_builder.adaptor_env's dispatch)."""
    ensure_network()
    ensure_images_built(dp.pipeline_type)

    image_adaptor = IMAGES[dp.pipeline_type]

    if not _container_exists(dp.adaptor_container_name):
        _run([
            "run", "-d", "--restart", "unless-stopped",
            "--name", dp.adaptor_container_name,
            "--network", NETWORK,
            *_env_args(env_builder.adaptor_env(dp)),
            image_adaptor,
        ])


def container_delete(name: str) -> None:
    """Remove a single container by name. Also used directly by
    ui/app.py's /consumer-pipelines/{key}/delete — see
    k8s_ops.container_delete()'s docstring for why there's no matching
    "recreate" path for a consumer pipeline, unlike a producer's Start.
    """
    if _container_exists(name):
        _run(["rm", "-f", name], check=False)


def stop(dp: DataProductDefinition) -> None:
    container_delete(dp.adaptor_container_name)


def set_execution_mode(dp: DataProductDefinition) -> None:
    """
    Recreate the running container so EXECUTION_MODE picks up
    dp.execution_mode. Env vars are fixed at `podman run` time — there is
    no live toggle — so this removes and relaunches the container with the
    definition's current execution_mode baked in. The container keeps its
    name/identity from the caller's perspective; only the underlying
    process restarts.
    """
    stop(dp)
    launch(dp)


def set_container_execution_mode(name: str, mode: str) -> None:
    """
    Recreate a container this app doesn't own the launch spec for — the
    fixed consumer pipelines (see service/consumers.py), started by
    podman-compose rather than launch() above — with only EXECUTION_MODE
    overridden. Unlike set_execution_mode(), which already knows the full
    desired env from env_builder for its own producer containers, this
    reads the running container's actual image/env/network back via
    `podman inspect` first, since this app never built that spec itself.
    """
    result = _run(["inspect", name])
    data = json.loads(result.stdout)[0]
    image = data["Config"]["Image"]
    env = dict(entry.split("=", 1) for entry in data["Config"]["Env"])
    env["EXECUTION_MODE"] = mode
    networks = list((data.get("NetworkSettings", {}).get("Networks") or {}).keys()) or [NETWORK]

    container_delete(name)
    args = ["run", "-d", "--restart", "unless-stopped", "--name", name]
    for network in networks:
        args += ["--network", network]
    args += _env_args(env)
    args.append(image)
    _run(args)


def container_status(name: str) -> str:
    if not _container_exists(name):
        return "absent"
    result = _run(["inspect", "-f", "{{.State.Status}}", name], check=False)
    return result.stdout.strip() or "unknown"


def status(dp: DataProductDefinition) -> dict[str, str]:
    return {"adaptor": container_status(dp.adaptor_container_name)}


def logs(name: str, tail: int = 100) -> str:
    if not _container_exists(name):
        return ""
    result = _run(["logs", "--tail", str(tail), name], check=False)
    return (result.stdout or "") + (result.stderr or "")


def run_once(name: str) -> str:
    """
    Force one manual pipeline cycle inside an already-running container —
    backs the self-service app's "Run Now" button. `podman exec`s `python main.py
    --run-once` (see utils/scheduler_backend.py's run_once()), which runs
    regardless of EXECUTION_MODE, unlike the container's own persistent
    scheduler loop that this leaves untouched.

    Note this is a separate exec session, not the container's PID-1
    process — its output never shows up in `podman logs`/logs() above, so
    the caller is expected to persist the returned text itself (see
    ui/app.py, which feeds it straight into
    run_history.record_from_logs()).
    """
    if not _container_exists(name):
        raise RuntimeError(f"container '{name}' is not running — start the data product first")
    result = _run(["exec", name, "python", "main.py", "--run-once"], check=False)
    return (result.stdout or "") + (result.stderr or "")
