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
# | 1.1.0   | Close port-forward auth bypass (fail closed, no proxy)    | DSI Team      | 2026-09-29  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Keycloak identity/authorization helpers for the self-service UI.

Auth itself is NOT handled here. Like dpn-portal in
dpn-health-monitoring-service (app/dpn-portal/main.py), this app sits behind
an oauth2-proxy sidecar (see service/charts/templates/oauth2-proxy-*.yaml)
which terminates the Keycloak OIDC login against the same dpn-realm this repo
already talks to for Kafka OAuth (see DPN_KAFKA_OAUTH_* in
service/charts/values.yaml). oauth2-proxy runs in its default "proxy"
mode (upstreams is set, so it forwards straight to this app) with
`pass_user_headers = true`, which adds X-Forwarded-User,
X-Forwarded-Preferred-Username and X-Forwarded-Groups (comma-separated) to
the request before it reaches us. This module just reads those headers - no
session/cookie/token handling of its own.

Unlike dpn-portal (a read-only tile page), this UI creates/starts/stops/runs
data-product pipelines - it mutates Kubernetes state. So on top of
oauth2-proxy's `allowed_groups` gate (dpnadmin, dpnoperator, and dpnreader
may all log in - see values.yaml's oauth2Proxy.keycloak.allowedRoles), this
module adds a second, in-app check, matching the platform's 3 realm roles
(dpn-authentication-service's configure-keycloak.sh) exactly:

  - dpnadmin    - full read/write: create, delete, start/stop/pause/resume/
                  run every data product.
  - dpnoperator - can create a data product and trigger/enable/disable its
                  execution (start/stop/pause/resume/run), and view
                  everything, but cannot delete an existing one.
  - dpnreader   - read-only: every page renders, no route that mutates
                  anything is reachable.
"""
from __future__ import annotations

import os
from urllib.parse import quote

from fastapi import HTTPException, Request

# Keycloak grants these to every user automatically (offline_access for
# refresh tokens, uma_authorization for the authz services feature) - they
# ride along in the same "roles" claim as dpnadmin/dpnoperator/dpnreader but
# aren't meaningful access-control groups. Mirrors app/dpn-portal/main.py's
# NOISE_ROLES exactly, since both read the same realm's "roles" claim.
NOISE_ROLES = {"offline_access", "uma_authorization"}

ADMIN_ROLE = "dpnadmin"
OPERATOR_ROLE = "dpnoperator"
READER_ROLE = "dpnreader"

# Any group that grants operate-or-better access - used by require_operator()
# so an admin never has to also hold the operator group separately.
OPERATE_ROLES = {ADMIN_ROLE, OPERATOR_ROLE}


def user_groups(request: Request) -> set[str]:
    raw = request.headers.get("X-Forwarded-Groups", "")
    groups = {g.strip() for g in raw.split(",") if g.strip()}
    return groups - NOISE_ROLES


def username(request: Request) -> str:
    # X-Forwarded-User is always the token's "sub" (a GUID) - oauth2-proxy
    # puts the human-readable preferred_username in this separate header
    # instead (user_id_claim in the proxy config), same as dpn-portal.
    return request.headers.get("X-Forwarded-Preferred-Username", "")


def _no_proxy_headers(request: Request) -> bool:
    # Neither header is present at all - this request never passed through
    # oauth2-proxy (e.g. `uvicorn ui.app:app --reload` per
    # service/README.md's local-dev instructions, or a kubectl port-forward
    # straight to the pod, bypassing the proxy Service entirely).
    return "X-Forwarded-User" not in request.headers and "X-Forwarded-Groups" not in request.headers


# Set by service/charts/templates/deployment.yaml whenever oauth2Proxy.enabled
# - i.e. every real cluster deployment, where oauth2-proxy is guaranteed to be
# the only path in. Left unset for local dev (`uvicorn ui.app:app --reload`,
# no chart involved), which never runs behind oauth2-proxy at all.
REQUIRE_PROXY_AUTH = os.environ.get("DPN_REQUIRE_PROXY_AUTH", "").lower() == "true"


def request_is_authenticated(request: Request) -> bool:
    """Gate applied to every route (see ui/app.py's require_proxy_auth
    middleware) before any handler - including read-only ones - runs.

    Without this, a request that reaches the app without ever passing
    through oauth2-proxy (a `kubectl port-forward` straight to the pod or
    the dpn-self-service ClusterIP, skipping the LB+proxy entirely) carries
    neither X-Forwarded-User nor X-Forwarded-Groups, and every route was
    reachable - full admin access - with no Keycloak login at all. That is
    exactly the fully-open behavior local dev depends on (see
    service/README.md's "Local dev is unaffected"), so it can only be closed
    where a proxy is actually guaranteed to be in front: REQUIRE_PROXY_AUTH is
    true. NetworkPolicy (charts/templates/networkpolicy.yaml) can't close this
    gap itself - kubectl port-forward/exec tunnel through the kubelet API, not
    the pod network path a CNI's NetworkPolicy enforcement inspects."""
    return not REQUIRE_PROXY_AUTH or not _no_proxy_headers(request)


def is_admin(request: Request) -> bool:
    if _no_proxy_headers(request):
        return True
    return ADMIN_ROLE in user_groups(request)


def is_operator(request: Request) -> bool:
    """True for dpnadmin OR dpnoperator - anyone allowed to trigger/pause/
    resume/stop/run a data product (but not necessarily create/delete one)."""
    if _no_proxy_headers(request):
        return True
    return bool(user_groups(request) & OPERATE_ROLES)


def require_admin(request: Request) -> None:
    """FastAPI dependency for every route that permanently deletes a data
    product. oauth2-proxy's allowed_groups already restricts login to
    dpnadmin/dpnoperator/dpnreader (see values.yaml) - this is the second
    gate that keeps dpnoperator/dpnreader from deleting data products."""
    if not is_admin(request):
        raise HTTPException(
            status_code=403,
            detail="dpnadmin is required to delete data products.",
        )


def require_operator(request: Request) -> None:
    """FastAPI dependency for every route that creates a data product, or
    that starts, stops, pauses, resumes, or runs an EXISTING one -
    dpnoperator's whole reason to exist (create, trigger/enable/disable,
    manage task state) without the delete power require_admin gates.
    dpnadmin passes this too, since it can do everything dpnoperator can."""
    if not is_operator(request):
        raise HTTPException(
            status_code=403,
            detail="Read-only access - dpnadmin or dpnoperator is required to change a data product's run state.",
        )


# Set by service/charts/templates/deployment.yaml from
# oauth2Proxy.keycloak.publicUrl / oauth2Proxy.keycloak.realm / oauth2Proxy.
# externalUrl in values.yaml. Empty locally, where logout_url() below falls
# back to a plain proxy sign-out.
KEYCLOAK_PUBLIC_URL = os.environ.get("KEYCLOAK_PUBLIC_URL", "")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "")
SELF_SERVICE_EXTERNAL_URL = os.environ.get("SELF_SERVICE_EXTERNAL_URL", "")

# The Keycloak client both this UI's oauth2-proxy and the Kafka OAuth config
# authenticate as - see oauth2Proxy.keycloak.clientId in values.yaml.
KEYCLOAK_CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID", "dpn-service-client")

# Base URL of the unified DPN UI portal this app is embedded/linked under
# (FR-D-016m-18/-20). Set once per environment, no default: an unset value
# means no portal integration exists yet for this deployment, and
# logout_url() below falls back to redirecting back to this app itself
# rather than to a guessed/broken portal URL.
UNIFIED_PORTAL_URL = os.environ.get("UNIFIED_PORTAL_URL", "")


def logout_url() -> str:
    # /oauth2/sign_out alone only clears THIS proxy's own session cookie - it
    # does not touch Keycloak's SSO session, so a "logged out" user is
    # transparently re-authenticated on their very next request. Chaining
    # through Keycloak's real end-session endpoint actually ends the SSO
    # session - see app/dpn-portal/main.py's _logout_url for the full
    # rationale, mirrored here exactly.
    #
    # post_logout_redirect_uri prefers UNIFIED_PORTAL_URL (send the user back
    # to the unified portal, not this individual UI) and falls back to this
    # app's own external URL when no portal is configured for this
    # deployment yet.
    redirect_target = UNIFIED_PORTAL_URL or SELF_SERVICE_EXTERNAL_URL
    if not KEYCLOAK_PUBLIC_URL or not KEYCLOAK_REALM or not redirect_target:
        # Misconfigured or running without oauth2-proxy (local dev) - at
        # least clear this proxy's own cookie instead of a broken link, and
        # do nothing at all if there's no proxy in front (harmless 404).
        return "/oauth2/sign_out"

    keycloak_logout = (
        f"{KEYCLOAK_PUBLIC_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/logout"
        f"?client_id={KEYCLOAK_CLIENT_ID}&post_logout_redirect_uri={quote(redirect_target, safe='')}"
    )
    return f"/oauth2/sign_out?rd={quote(keycloak_logout, safe='')}"
