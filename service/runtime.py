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
Runtime backend selector.

DPN_RUNTIME=podman (default) — local Podman demo (service/podman_ops.py).
DPN_RUNTIME=kubernetes        — AKS/EKS/GKE (service/k8s_ops.py).

app.py imports this module rather than podman_ops/k8s_ops directly, so it
doesn't need to know which backend is active. k8s_ops (and its kubeconfig
load) is only imported when actually selected, so local Podman-only
development never needs a kubeconfig present.
"""
from __future__ import annotations

import os

_BACKEND = os.getenv("DPN_RUNTIME", "podman").strip().lower()

if _BACKEND == "kubernetes":
    from service import k8s_ops as _impl
elif _BACKEND == "podman":
    from service import podman_ops as _impl
else:
    raise RuntimeError(f"Unknown DPN_RUNTIME '{_BACKEND}' — expected 'podman' or 'kubernetes'")

launch = _impl.launch
stop = _impl.stop
set_execution_mode = _impl.set_execution_mode
status = _impl.status
logs = _impl.logs
run_once = _impl.run_once

# Single-container/Deployment status lookup and delete by name — used for
# the fixed consumer pipelines (service/consumers.py), which aren't a
# DataProductDefinition pair like status()/stop() above expect.
container_status = _impl.container_status
container_delete = _impl.container_delete

# Same "by name, not by DataProductDefinition" shape as container_status/
# container_delete above — the consumer pipelines' own Pause/Resume toggle
# (see service/consumers.py's docstring for why this patches the running
# container in place rather than going through launch()/set_execution_mode()).
set_container_execution_mode = _impl.set_container_execution_mode
