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
# | 1.1.0   | Simplified topic/container naming convention              | DSI Team      | 2026-10-01  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Data product definition — the self-service replacement for a manual
blueprint-folder copy + rename + Docker build.

A business user starts from three fields (name, integration pathway,
schedule) — organisation name and the cloud storage account are platform-
level settings configured once at self-service deploy time (see
service/env_builder.py's PLATFORM_* values, injected from a Secret) and
apply to every data product, not chosen per product. Every storage
container name and Kafka topic name is then *proposed* deterministically
from product_type — but those proposed names are shown back to the user on
a review step (ui/templates/review.html) before anything launches, since a
product-derived default can collide with something that already exists.
Every confirmed name must in fact be brand-new (see ui/app.py's
_find_name_collision()): this platform only ever creates fresh
topics/containers for a product, it never attaches to a pre-existing one,
so the review step exists to let the user pick a non-colliding name, not
to point at existing infrastructure. The generic adaptor
image is reused unchanged either way — nothing gets copied or rebuilt per
product, and no schema type is required: the pipeline carries no
per-schema logic at all. Both pathways are single-container: each
adaptor absorbed its own schema mapper stage entirely (no more mapper
container/topic on either pathway) — see
blueprints/producer/{file,topic}/adaptor/main.py's v3.0.0 changelog
entries.
"""
from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

PIPELINE_TYPES: tuple[str, ...] = ("file", "topic")

# Only meaningful for pipeline_type="file" — the topic pathway is Kafka-only
# and never touches blob storage. This is a platform-level setting (one
# cloud per self-service deployment — see env_builder.PLATFORM_CLOUD_PROVIDER),
# not a per-product choice, so it is no longer part of the create-product
# form; kept here only as the Literal type shared with env_builder.
CLOUD_PROVIDERS: tuple[str, ...] = ("aws", "azure", "gcp")

# How the product's containers decide when to run a cycle. Maps directly to
# the EXECUTION_MODE env var read by utils/scheduler_backend.py's
# _resolve_execution_mode():
#   "automatic" ("Schedule") — the container's own IntervalBackend ticks
#       every schedule_seconds, same as today.
#   "manual"    ("Manual")   — every scheduled tick is skipped; the only way
#       to run a cycle is the "Run Now" button (service/runtime.py's
#       run_once(), see index.html), which execs one cycle on demand,
#       independent of schedule_seconds entirely.
# Chosen up front here (step 1 of /create-new-data-product) rather than only after the fact via
# Pause/Resume, so a product meant to be manual-only never needs a
# throwaway automatic cycle before someone gets around to pausing it.
EXECUTION_MODES: tuple[str, ...] = ("automatic", "manual")

_PRODUCT_TYPE_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")

# product_type becomes a Kubernetes "app" LABEL value (not just a resource
# name) via adaptor_container_name below
# (f"dpn-producer-{pipeline_type}-{product_type}") — label values are
# capped at 63 characters by Kubernetes itself (RFC 1123). "dpn-producer-
# topic-" (19 chars) is the longer of the two pipeline-type prefixes,
# leaving 44 chars of headroom — the storage container name (file pathway
# only) is no longer templated from product_type at all (it's plain
# free-text UI input, see default_file_names()), so this Kubernetes label
# is the only remaining structural constraint on product_type's length.
# Comfortably clear of dsm-management-node's mn.product.name column too
# (VARCHAR(355) as of that repo's V20260617120000__alter_column_lengths.sql).
_PRODUCT_TYPE_MAX_LENGTH = 44


_CONTAINER_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")

# Label per cloud provider — used only in the validation error message
# below, so a business user knows which service actually rejected the name.
_CONTAINER_LABEL: dict[str, str] = {
    "azure": "Azure Storage container",
    "aws": "S3 bucket",
    "gcp": "GCS bucket",
}


def validate_storage_container_name(name: str, cloud_provider: str) -> None:
    """
    Raise ValueError with a clear, cloud-specific message if `name` would be
    rejected by the storage service itself. Azure Blob containers, S3
    buckets, and GCS buckets each have their own naming rules, none of which
    match the Kubernetes label / Kafka topic naming already validated
    elsewhere in this file — checking this here, at data-product setup
    time, turns what would otherwise be a runtime
    "storage.container.create.failed" crash on the adaptor's very first
    cycle (see blueprints/producer/file/adaptor/main.py's
    ensure_container_exists() calls) into a clear form error before
    anything launches.

    The rule enforced is the strict INTERSECTION of all three providers'
    real rules (3-63 chars, lowercase letters/digits/hyphens only, must
    start and end with a letter or digit, no consecutive hyphens) rather
    than a per-provider carve-out — e.g. S3/GCS both also permit periods,
    but a period breaks virtual-hosted-style S3 URLs and TLS wildcard
    certs, so it's excluded everywhere for one rule every product can rely
    on regardless of which cloud this deployment happens to target.
    """
    label = _CONTAINER_LABEL.get(cloud_provider, "storage container")
    if not (3 <= len(name) <= 63):
        raise ValueError(
            f"'{name}' is not a valid {label} name: must be 3-63 characters long (got {len(name)})."
        )
    if "--" in name or not _CONTAINER_NAME_RE.match(name):
        raise ValueError(
            f"'{name}' is not a valid {label} name: lowercase letters, digits, and single "
            "hyphens only, starting and ending with a letter or digit."
        )


class DataProductCreate(BaseModel):
    """
    Step 1 — fields a business user supplies before seeing any naming.

    org_name and cloud_provider are NOT here: both are platform-level
    settings, fixed for the lifetime of this self-service deployment (one
    organisation, one cloud storage account per deployment) — see
    service/env_builder.py's PLATFORM_ORG_NAME / PLATFORM_CLOUD_PROVIDER,
    injected from a Secret at pod startup. Every data product inherits them
    automatically; see DataProductDefinition.org_name / .cloud_provider
    below, which read the platform config rather than user input.
    """

    product_type: str = Field(..., description="Unique data product identifier, hyphens only")
    pipeline_type: Literal["file", "topic"] = Field(..., description="Integration pathway")
    schedule_seconds: int = Field(60, ge=5, le=3600, description="Poll / re-subscribe interval, in seconds")
    execution_mode: Literal["automatic", "manual"] = Field(
        "automatic",
        description="'automatic' runs on schedule_seconds; 'manual' only runs via the Run Now button",
    )

    @field_validator("product_type")
    @classmethod
    def _validate_product_type(cls, value: str) -> str:
        value = value.strip().lower()
        if not _PRODUCT_TYPE_RE.match(value):
            raise ValueError(
                "product_type must be lowercase alphanumeric with hyphens only "
                "(e.g. 'bp-natural-gas-02')"
            )
        if len(value) > _PRODUCT_TYPE_MAX_LENGTH:
            raise ValueError(
                f"product_type must be at most {_PRODUCT_TYPE_MAX_LENGTH} characters "
                "(it becomes a Kubernetes label value, capped at 63 chars, once "
                "prefixed with 'dpn-producer-<pathway>-')"
            )
        return value


class DataProductDefinition(DataProductCreate):
    """
    A registered data product. Storage container names and Kafka topic
    names are plain fields here (not re-derived properties) because the
    review step may have overridden them — what's stored is whatever the
    user actually confirmed, whether that's the proposed default or not.
    """

    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # execution_mode is inherited from DataProductCreate — chosen up front
    # on /create-new-data-product, and changeable afterwards via the index page's
    # Schedule/Manual dropdown (POST .../pause or .../resume). Maps
    # directly to the EXECUTION_MODE env var read by
    # utils/scheduler_backend.py::_resolve_execution_mode(). "manual" keeps
    # the container running and heartbeating but every SchedulerBackend
    # skips the actual pipeline run — distinct from is_active/Stop, which
    # removes the container entirely. Env vars are fixed at container
    # creation, so changing this means recreating the container with the
    # new value (see podman_ops.set_execution_mode), not a live toggle.

    # Snapshotted from service/env_builder.py's PLATFORM_ORG_NAME /
    # PLATFORM_CLOUD_PROVIDER at creation time (ui/app.py's
    # confirm_data_product) — not user input. Read-only on every template
    # (new.html/review.html just display them). Storage credentials
    # themselves are never stored here at all: env_builder injects the
    # platform Secret's values straight into the container env at launch
    # time, the same way it already does for Kafka OAuth.
    org_name: str = ""
    cloud_provider: Literal["aws", "azure", "gcp"] = "aws"

    # --- file pathway ---
    # No mapper container/topic — the adaptor pulls straight from
    # source_container_name, applies the same rename it always used to hand
    # off to the (now-removed) schema mapper, and writes straight to
    # target_container_name itself — see
    # blueprints/producer/file/adaptor/main.py. Both container names are
    # plain, free-text UI input (no auto-derived naming template beyond the
    # proposed default in default_file_names()) — only their charset/length
    # is validated (see validate_storage_container_name() below), since
    # they name a real Azure/S3/GCS storage container, not a Kubernetes
    # resource. target_topic_name is the one remaining notification topic,
    # equally free-text/overridable on the review step
    # (ui/templates/review.html).
    source_container_name: Optional[str] = None
    target_container_name: Optional[str] = None
    target_topic_name: Optional[str] = None

    # --- topic pathway only ---
    # No mapper topic/group either — the adaptor absorbed the topic
    # pathway's schema mapper too (enrich + produce straight to the target
    # topic; see blueprints/producer/topic/adaptor/main.py). src_topic_name
    # is overridable but, per ui/app.py's _find_name_collision(), must
    # name a topic that does not already exist — this platform only ever
    # creates brand-new topics, it never attaches to a pre-existing one.
    src_topic_name: Optional[str] = None
    src_group_id: Optional[str] = None

    @model_validator(mode="after")
    def _validate_file_container_names(self) -> "DataProductDefinition":
        if self.pipeline_type == "file":
            for field_name in ("source_container_name", "target_container_name"):
                value = getattr(self, field_name)
                if value:
                    validate_storage_container_name(value, self.cloud_provider)
        return self

    @property
    def org_slug(self) -> str:
        """
        Sanitized form of org_name, sent as the orgName env var to both
        pathways' adaptors via common_env().

        Only the topic pathway actually reads it any more (to compute its
        target topic name and stamp it onto every message's headers — see
        blueprints/producer/topic/adaptor/main.py, which applies its own
        `_sanitize()` on top of this regardless). The file pathway's
        adaptor no longer renames files at all — a file lands in the
        target container under the exact name it arrived with, org_name
        included nowhere in that path (see
        blueprints/producer/file/adaptor/main.py's v3.1.0 changelog entry)
        — so orgName is injected into its env but simply unused there.
        """
        return re.sub(r"[^a-z0-9]+", "-", self.org_name.lower()).strip("-")

    @property
    def adaptor_container_name(self) -> str:
        """
        The Kubernetes Deployment (or podman container) name for this
        product's one and only container — every pathway is single-
        container now (see blueprints/producer/{file,topic}/adaptor/main.py's
        v3.0.0 changelog entries: both absorbed their schema mapper).
        Distinct from any storage container name (source_container_name /
        target_container_name above), which is a name in a cloud storage
        account, not a Kubernetes resource, and carries no such prefix.
        """
        return f"dpn-producer-{self.pipeline_type}-{self.product_type}"


def default_file_names(product_type: str) -> dict[str, str]:
    """
    Proposed (overridable) names shown on the review step for
    pipeline_type=file. These name real Azure/S3/GCS storage containers,
    not Kubernetes resources, so — unlike adaptor_container_name above —
    they carry no fixed prefix/template: this is only a starting
    suggestion, freely replaceable with whatever a business user's actual
    storage account already uses. Still subject to
    validate_storage_container_name() at confirm time (whether left as
    this default or overridden).
    """
    return {
        "source_container_name": f"{product_type}-stage",
        "target_container_name": f"{product_type}-target",
        "target_topic_name": f"dpn-producer-file-{product_type}-target",
    }


def default_topic_names(product_type: str) -> dict[str, str]:
    """
    Proposed (overridable) names shown on the review step for
    pipeline_type=topic. src_topic_name in particular is just a guess and
    the field most likely to need overriding — but, per
    ui/app.py's _find_name_collision(), the confirmed name must not
    already exist on the broker; this platform only ever creates
    brand-new topics for a product, never attaches to a pre-existing one.
    target_topic_name is a real, editable field here too (not a read-only
    computed value). Deliberately NOT the same string
    blueprints/producer/topic/adaptor/main.py's own _sanitize()-based
    fallback would compute (that one still includes org_name and strips
    hyphens — see its dpn-producer-<org>-<product>-target formula): this
    platform's self-service flow always confirms a real target_topic_name
    here, stored in the registry and forwarded to the adaptor directly
    (service/env_builder.py's topic_adaptor_env()), so the adaptor's own
    fallback formula only ever matters for a manual chart deploy outside
    the self-service UI entirely — see that file's v3.1.0 changelog entry.

    No src_group_id here: that consumer group id is always calculated
    from product_type server-side (see ui/app.py's confirm_data_product)
    rather than shown/overridable on the review screen.
    """
    return {
        "src_topic_name": f"dpn-producer-{product_type}-stage",
        "target_topic_name": f"dpn-producer-{product_type}-target",
    }
