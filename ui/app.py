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
# | 1.2.0   | Wire Kafka topic deletion into data product delete        | DSI Team      | 2026-10-01  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Self-service data product UI.

Two-step flow:
  1. /create-new-data-product — business user picks a product name, integration
                       pathway (file/topic), and schedule. Org and cloud
                       provider are platform-level, injected automatically
                       (see service/env_builder.py's PLATFORM_ORG_NAME /
                       PLATFORM_CLOUD_PROVIDER) rather than chosen here.
  2. /data-products/preview — shows the proposed storage container /
                       Kafka topic names for that pathway (auto-derived
                       from product_type, but editable) so the user can
                       confirm or override them before anything launches.
Submitting the review writes one registry row and immediately launches the
one generic, already-built adaptor container for the chosen pathway (both
pathways' adaptors absorbed their own schema mapper stage — see
service/env_builder.py's adaptor_env() dispatch), with the confirmed names
injected via environment variables.
"""
from __future__ import annotations

import logging
import os
import traceback
from pathlib import Path
from urllib.parse import urlencode

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from service import auth, consumers, env_builder, lifecycle_events, precheck, registry, run_history, runtime
from service.models import (
    EXECUTION_MODES,
    PIPELINE_TYPES,
    DataProductCreate,
    DataProductDefinition,
    default_file_names,
    default_topic_names,
)

# Unlike the blueprint containers (which configure logging themselves via
# dpn_observability_sdk's OtelLogger), this app has no entrypoint that ever
# calls logging.basicConfig()/sets the root logger's level — so every 
# _logger.info() in service/registry.py and service/registry_backup.py was
# silently dropped by the default root level (WARNING), never actually
# reaching uvicorn's stderr. This is that missing configuration, done once
# here since ui.app is what every run path (uvicorn, TestClient) imports.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
_logger = logging.getLogger(__name__)

app = FastAPI(title="DPN Data Product Self-Service")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
# Serves ui/static/{css,js} for the Figma-matched shell (base.html's header/
# sidenav/footer) — pure static assets, no bearing on any route's behavior.
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


@app.middleware("http")
async def require_proxy_auth(request: Request, call_next):
    """Rejects any request that reached this app without passing through
    oauth2-proxy, when service/auth.REQUIRE_PROXY_AUTH says one is guaranteed
    to be in front (see that flag's docstring for why this can't be left to
    NetworkPolicy or to the per-route require_admin/require_operator checks
    alone - those only gated mutating routes, leaving every read-only page
    reachable with no login at all to whatever reaches the app directly, e.g.
    `kubectl port-forward` straight to the pod/ClusterIP).

    /healthz is exempt - kubelet's readiness/liveness probes hit the pod
    directly, carrying no oauth2-proxy headers themselves."""
    if request.url.path != "/healthz" and not auth.request_is_authenticated(request):
        return JSONResponse(
            status_code=401,
            content={"detail": "Authentication required - access this UI through its oauth2-proxy URL, not directly."},
        )
    return await call_next(request)


@app.on_event("startup")
def _restore_registry_from_backup_if_missing() -> None:
    """See registry.restore_from_backup_if_missing()'s own docstring —
    this is the disaster-recovery path for a brand-new/empty PVC."""
    registry.restore_from_backup_if_missing()


def _identity(request: Request) -> dict:
    """Shared template context every page renders its identity/logout line
    from - see service/auth.py for where these come from."""
    return {
        "username": auth.username(request),
        "groups": sorted(auth.user_groups(request)),
        "is_admin": auth.is_admin(request),
        "is_operator": auth.is_operator(request),
        "logout_url": auth.logout_url(),
        "unified_portal_url": auth.UNIFIED_PORTAL_URL,
    }


# Local-dev aid: renders any unhandled exception as a page instead of a bare
# 500 with no body, so a bug shows up right in the browser instead of only
# in uvicorn's stderr. Leaks exception messages/tracebacks to whoever hits
# the broken route — fine for this app's local/demo usage, but flip
# DPN_UI_SHOW_TRACEBACK=false (or make this handler conditional) before
# pointing it at anything with real users.
SHOW_TRACEBACK = os.getenv("DPN_UI_SHOW_TRACEBACK", "true").lower() == "true"


@app.exception_handler(Exception)
async def _unhandled_exception(request: Request, exc: Exception):
    # Starlette only falls through to this (registered for the base
    # Exception class) when no more specific handler matches — HTTPException
    # (every deliberate 404/403/409 raised across this file) still resolves
    # to FastAPI's own default handler first, untouched by this one.
    _logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    exc_type, exc_message = type(exc).__name__, str(exc)
    if _wants_panel(request):
        # The New Data Product flow's fetch()-based Step 1/confirm submit
        # (inline-review.js) expects either a redirect or a fragment with a
        # `.dpn-alert--error` it can merge onto the form already on screen
        # (mergeFormErrors) — not the full error.html page below, which it
        # has no markup to parse for, so an exception here would otherwise
        # just silently stop the button's spinner with nothing shown.
        return templates.TemplateResponse(
            request,
            "_error_panel.html",
            {"request": request, "exc_type": exc_type, "exc_message": exc_message},
            status_code=500,
            headers={_PANEL_RESPONSE_HEADER: "form"},
        )
    return templates.TemplateResponse(
        request,
        "error.html",
        {
            "request": request,
            "exc_type": exc_type,
            "exc_message": exc_message,
            "traceback": traceback.format_exc() if SHOW_TRACEBACK else None,
            **_identity(request),
        },
        status_code=500,
    )


def _new_form_context(
    request: Request,
    error: str | None = None,
    field_error: str | None = None,
    step1: DataProductCreate | None = None,
) -> dict:
    return {
        "request": request,
        "pipeline_types": PIPELINE_TYPES,
        "execution_modes": EXECUTION_MODES,
        # Platform-level, read-only — see service/env_builder.py's
        # PLATFORM_ORG_NAME/PLATFORM_CLOUD_PROVIDER docstring: one
        # organisation and one cloud storage account per self-service
        # deployment, injected from a Secret, never chosen per product.
        "org_name": env_builder.PLATFORM_ORG_NAME,
        "cloud_provider": env_builder.PLATFORM_CLOUD_PROVIDER,
        "error": error,
        # Which form field `error` is about, if any — lets new.html render
        # it inline on that one field (see the dpn-form-validation skill)
        # instead of only as a page-level banner. None means "not tied to a
        # specific field" (e.g. a pipeline_type/schedule_seconds mismatch).
        "field_error": field_error,
        # Set when Step 1 was already successfully parsed (e.g. a duplicate
        # product_type caught after that) so new.html can re-populate what
        # the user had already entered instead of resetting the form to its
        # defaults. None (a parse failure, or the bare GET) means "show
        # defaults".
        "step1": step1,
        **_identity(request),
    }


# Sent by ui/static/js/inline-review.js on every Step 1/Step 2 form submit
# so the New Data Product flow can stay on one page (see new.html's
# #review-panel) instead of navigating to a separate /data-products/preview
# page per step.
_PANEL_REQUEST_HEADER = "X-DPN-Panel-Request"
_PANEL_RESPONSE_HEADER = "X-DPN-Panel"


def _wants_panel(request: Request) -> bool:
    return request.headers.get(_PANEL_REQUEST_HEADER) == "1"


def _render_new(request: Request, context: dict, status_code: int = 200):
    """Render new.html (Step 1) — either a normal full-page response, or,
    for the AJAX flow, still the full page (there's no separate partial for
    it — inline-review.js lifts the alert/field-error state back out of it
    itself, see mergeFormErrors()), tagged with the X-DPN-Panel: form
    header so the script knows to do that instead of injecting the raw
    document into #review-panel."""
    headers = {_PANEL_RESPONSE_HEADER: "form"} if _wants_panel(request) else None
    return templates.TemplateResponse(request, "new.html", context, status_code=status_code, headers=headers)


def _render_review(request: Request, context: dict, status_code: int = 200):
    """Render Step 2 — the full review.html page normally, or, when
    inline-review.js asked for it, just _review_panel.html's fragment
    (same context, no base.html chrome) tagged with X-DPN-Panel: review so
    the script knows to inject the response straight into #review-panel."""
    if _wants_panel(request):
        return templates.TemplateResponse(
            request, "_review_panel.html", context, status_code=status_code,
            headers={_PANEL_RESPONSE_HEADER: "review"},
        )
    return templates.TemplateResponse(request, "review.html", context, status_code=status_code)


@app.get("/")
def root_redirect():
    """"/" is kept as a stable alias for Home/bookmarks — the real page
    lives at /producer-pipelines, named for its subject like every other
    top-level page (/consumer-pipelines, /run-history)."""
    return RedirectResponse(url="/producer-pipelines", status_code=307)


@app.get("/producer-pipelines")
def index(request: Request, created: str | None = None):
    products = registry.list_products()
    rows = []
    for product in products:
        try:
            state = runtime.status(product)
        except Exception:
            # A runtime.status() failure for one product (e.g. a transient
            # Kubernetes API error) must not 500 the whole home page for
            # every other product — same rationale as _refresh_history()'s
            # try/except around runtime.logs() below.
            _logger.exception("status lookup failed for %s", product.product_type)
            state = {"adaptor": "unknown"}
        rows.append({"product": product, "status": state})
    # `created` is set only by confirm_data_product()'s success redirect
    # (?created=<product_type>) — its value isn't used in the message
    # itself (kept identical to the Figma design's copy), just as the flag
    # that a creation just happened. See ui/templates/_toast.html, which
    # only renders when toast_message is set, and ui/static/js/toast.js,
    # which strips the query param right after so a refresh doesn't
    # re-show it.
    toast_message = (
        "Your Data Product Pipeline has been created and published successfully"
        if created else None
    )
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "rows": rows,
            "active": "producer",
            "toast_message": toast_message,
            **_identity(request),
        },
    )


def _refresh_history(products: list[DataProductDefinition]) -> None:
    """Pull recent container logs and persist any new completed/failed runs."""
    for product in products:
        # Every pathway is single-container now — both producer adaptors
        # absorbed their own schema mapper stage (see
        # env_builder.adaptor_env's dispatch), so there is only ever one
        # log stream to pull per product.
        try:
            log_text = runtime.logs(product.adaptor_container_name, tail=500)
        except Exception:
            # A runtime.logs() failure (e.g. a Kubernetes API error) for
            # one product must not 500 the whole /history page for
            # every other product — skip it, next page load retries.
            continue
        run_history.record_from_logs(product.product_type, "adaptor", log_text)

    # The two fixed consumer pipelines (service/consumers.py) — single
    # extractor container per pathway, same as the producer side (the old
    # schema_mapper stage was absorbed entirely) — run the same generic
    # business logic, so they emit the same structured JSON log lines.
    # Keyed here by pipeline key (e.g. "file"), not container name.
    for pipeline in consumers.CONSUMER_PIPELINES:
        try:
            log_text = runtime.logs(pipeline["extractor_container"], tail=500)
        except Exception:
            continue
        run_history.record_from_logs(pipeline["key"], "extractor", log_text)


_CONSUMER_PIPELINE_KEYS = {pipeline["key"] for pipeline in consumers.CONSUMER_PIPELINES}

# Sent by ui/static/js/run-history.js on every "Currently viewing"/"Displaying
# results for"/product filter click, so that click can swap just
# #dpn-history-results (see history.html) instead of a full page navigation —
# same convention as _PANEL_REQUEST_HEADER above, just named for this page.
_HISTORY_AJAX_HEADER = "X-DPN-History-Request"


@app.get("/run-history")
def history(request: Request, product_type: str | None = None, role: str | None = None, limit: int = 50):
    _refresh_history(registry.list_products())
    # `role` picks which pipeline family to show — producer runs are
    # recorded under stage "adaptor", consumer runs under "extractor" (see
    # _refresh_history above and service/run_history.py's _parse_line()).
    # Not explicitly chosen (e.g. the "Run Now" redirect below, which only
    # ever passes product_type)? Infer it from product_type when it's one
    # of the two fixed consumer pipeline keys, else default to producer.
    if role not in ("producer", "consumer"):
        role = "consumer" if product_type in _CONSUMER_PIPELINE_KEYS else "producer"
    stage = "extractor" if role == "consumer" else "adaptor"
    limit = 100 if limit == 100 else 50
    runs = run_history.get_history(product_type=product_type, stage=stage, limit=limit)
    context = {
        "request": request,
        "runs": runs,
        "product_type": product_type,
        "role": role,
        "limit": limit,
        # Switching pipeline family resets the product filter (it belongs
        # to the other family); switching the result-count only ever
        # keeps the currently selected family and product filter.
        "producer_switch_url": "/run-history?" + urlencode({"role": "producer", "limit": limit}),
        "consumer_switch_url": "/run-history?" + urlencode({"role": "consumer", "limit": limit}),
        "limit_50_url": "/run-history?" + urlencode(
            {k: v for k, v in {"role": role, "product_type": product_type, "limit": 50}.items() if v}
        ),
        "limit_100_url": "/run-history?" + urlencode(
            {k: v for k, v in {"role": role, "product_type": product_type, "limit": 100}.items() if v}
        ),
        "active": "history",
        **_identity(request),
    }
    if request.headers.get(_HISTORY_AJAX_HEADER) == "1":
        # run-history.js swaps #dpn-history-results' innerHTML with exactly
        # this fragment — same template the full page includes, so there's
        # nothing for the two to drift out of sync on.
        return templates.TemplateResponse(request, "_run_history_results.html", context)
    return templates.TemplateResponse(request, "history.html", context)


@app.get("/consumer-pipelines")
def consumer_monitor(request: Request):
    """
    Read-only status board for the platform's fixed consumer pipelines —
    one row per pathway, one container per row (the old schema_mapper
    stage was absorbed into the extractor entirely, same as the producer
    side's adaptor — see blueprints/consumer/{file,topic}/extractor/
    main.py's changelog). See service/consumers.py's docstring for why
    this is a plain list rather than a create/list/get registry like the
    producer side. Open to any authenticated user (dpnreader included),
    same as /producer-pipelines and /run-history: there is nothing here to mutate, only to look
    at.
    """
    rows = []
    for pipeline in consumers.list_pipelines():
        rows.append({
            **pipeline,
            "extractor_status": runtime.container_status(pipeline["extractor_container"]),
        })
    return templates.TemplateResponse(
        request, "consumers.html", {"request": request, "rows": rows, "active": "consumer", **_identity(request)}
    )


@app.get("/consumer-pipelines/{key}/logs")
def consumer_logs(request: Request, key: str):
    try:
        pipeline = consumers.get_pipeline(key)
    except consumers.ConsumerNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown consumer pipeline")

    return templates.TemplateResponse(
        request,
        "consumer_logs.html",
        {
            "request": request,
            "pipeline": pipeline,
            "extractor_logs": runtime.logs(pipeline["extractor_container"]),
            **_identity(request),
        },
    )


@app.post("/consumer-pipelines/{key}/delete", dependencies=[Depends(auth.require_admin)])
def delete_consumer_pipeline(key: str):
    """
    Removes a consumer pipeline's Deployments (Kubernetes) / containers
    (podman/docker) entirely — the consumer-side equivalent of a producer's
    Stop, named "Delete" instead because there is no matching "Start": this
    app doesn't own the image/spec for consumer/{file,topic}/*/charts (they
    each deploy from their own separate Helm release — see
    .pipelines/azure-pipelines/cd-pipelines/dpn-self-service-cd.yaml), so
    once removed here the only way back is redeploying that chart, not a
    button in this UI. Idempotent — deleting an already-absent Deployment
    is a no-op (runtime.container_delete()), so this is safe to retry.
    """
    try:
        pipeline = consumers.get_pipeline(key)
    except consumers.ConsumerNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown consumer pipeline")

    runtime.container_delete(pipeline["extractor_container"])
    lifecycle_events.emit(pipeline["extractor_container"], "deleted", consumer_key=key)
    return RedirectResponse(url="/consumer-pipelines", status_code=303)


@app.post("/consumer-pipelines/{key}/pause", dependencies=[Depends(auth.require_operator)])
def pause_consumer_pipeline(key: str):
    """EXECUTION_MODE=manual for a consumer pipeline — same semantics as a
    producer's pause_data_product() above, just patched onto the running
    container in place (runtime.set_container_execution_mode) since this
    app doesn't own the consumer chart's Deployment spec to relaunch it
    with (see service/consumers.py's docstring)."""
    try:
        pipeline = consumers.set_execution_mode(key, "manual")
    except consumers.ConsumerNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown consumer pipeline")

    runtime.set_container_execution_mode(pipeline["extractor_container"], "manual")
    lifecycle_events.emit(pipeline["extractor_container"], "paused", consumer_key=key)
    return RedirectResponse(url="/consumer-pipelines", status_code=303)


@app.post("/consumer-pipelines/{key}/resume", dependencies=[Depends(auth.require_operator)])
def resume_consumer_pipeline(key: str):
    """EXECUTION_MODE=automatic — resume a consumer pipeline's scheduled runs."""
    try:
        pipeline = consumers.set_execution_mode(key, "automatic")
    except consumers.ConsumerNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown consumer pipeline")

    runtime.set_container_execution_mode(pipeline["extractor_container"], "automatic")
    lifecycle_events.emit(pipeline["extractor_container"], "resumed", consumer_key=key)
    return RedirectResponse(url="/consumer-pipelines", status_code=303)


@app.post("/consumer-pipelines/{key}/run", dependencies=[Depends(auth.require_operator)])
def run_consumer_once(key: str, background_tasks: BackgroundTasks):
    """"Run Now" for a consumer pipeline — same shape as
    run_data_product_once() below: only available once Execution is
    manual, execs into the already-running extractor container rather
    than restarting it."""
    try:
        pipeline = consumers.get_pipeline(key)
    except consumers.ConsumerNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown consumer pipeline")

    if pipeline["execution_mode"] != "manual":
        raise HTTPException(
            status_code=409,
            detail="Set Execution to manual before using Run Now — automatic mode already runs on its own interval",
        )

    lifecycle_events.emit(pipeline["extractor_container"], "run_triggered", consumer_key=key)
    background_tasks.add_task(
        _run_once_and_record, key, "extractor", pipeline["extractor_container"]
    )
    return RedirectResponse(url=f"/run-history?product_type={key}", status_code=303)


@app.get("/create-new-data-product", dependencies=[Depends(auth.require_operator)])
def new_product_form(request: Request):
    return templates.TemplateResponse(request, "new.html", _new_form_context(request))


@app.post("/data-products/preview", dependencies=[Depends(auth.require_operator)])
def preview_data_product(
    request: Request,
    product_type: str = Form(...),
    pipeline_type: str = Form(...),
    schedule_seconds: int = Form(60),
    execution_mode: str = Form("automatic"),
):
    try:
        step1 = DataProductCreate(
            product_type=product_type,
            pipeline_type=pipeline_type,
            schedule_seconds=schedule_seconds,
            execution_mode=execution_mode,
        )
    except Exception as exc:  # pydantic ValidationError -> surface to the form
        # The one check Step 1 still blocks on: a value it couldn't even
        # parse (e.g. a non-numeric schedule) — there's no meaningful
        # preview to show for that. Every other check (name already taken,
        # already-registered product_type) is surfaced on the review panel
        # below instead, once there's a real preview to attach it to.
        return _render_new(request, _new_form_context(request, error=str(exc)), status_code=400)

    # An already-registered product_type is caught here, before any
    # storage/topic names are even proposed — it's surfaced inline on Step
    # 1's own Data Product Name field (see new.html), not on the review
    # panel: there's nothing to review yet if the product name itself can't
    # be used. ui/static/js/review-live-check.js's product-name live check
    # normally catches this before the user can even submit (the "Next:
    # Review Names" button stays hidden until the name checks out), so this
    # is the server-side backstop for a no-JS client or a fast double-click
    # that beats the debounce.
    if registry_has_product(step1.product_type):
        return _render_new(
            request,
            _new_form_context(
                request,
                error=f"Data product '{step1.product_type}' already exists.",
                field_error="product_type",
                step1=step1,
            ),
            status_code=409,
        )

    if step1.pipeline_type == "file":
        names = default_file_names(step1.product_type)
    else:
        # target_topic_name is a real, editable default here — the adaptor
        # uses whatever is confirmed on the review screen (see
        # blueprints/producer/topic/adaptor/main.py's v3.1.0 changelog
        # entry), not a fixed computed value.
        names = default_topic_names(step1.product_type)

    field_error, error = _find_collision(step1.pipeline_type, names.get) or (None, None)

    return _render_review(
        request,
        {
            "request": request,
            "step1": step1,
            "names": names,
            "org_name": env_builder.PLATFORM_ORG_NAME,
            "cloud_provider": env_builder.PLATFORM_CLOUD_PROVIDER,
            "error": error,
            "field_error": field_error,
            **_identity(request),
        },
        status_code=409 if error else 200,
    )


def registry_has_product(product_type: str) -> bool:
    try:
        registry.get_product(product_type)
        return True
    except registry.ProductNotFoundError:
        return False


def _name_checks(pipeline_type: str) -> list[tuple[str, str, callable]]:
    """(field, label, check_fn) triples for a pathway — check_fn(value)
    returns True if that name already exists on the platform's real Kafka
    broker / storage account. Shared by _find_name_collision() (the
    authoritative gate on preview/confirm) and check_name_available() (the
    review panel's live per-field indicator — see
    ui/static/js/review-live-check.js) so both agree on exactly which
    fields are checked and how."""
    if pipeline_type == "file":
        return [
            ("source_container_name", "Source container",
             lambda v: precheck.storage_container_exists(v, side="source")),
            ("target_container_name", "Target container",
             lambda v: precheck.storage_container_exists(v, side="target")),
            ("target_topic_name", "Target notification topic", precheck.kafka_topic_exists),
        ]
    return [
        ("src_topic_name", "Source topic", precheck.kafka_topic_exists),
        ("target_topic_name", "Target topic", precheck.kafka_topic_exists),
    ]


def _find_duplicate_field_values(pipeline_type: str, get_value) -> tuple[str, str] | None:
    """
    Return (field, error message) if two of this pathway's own editable
    name fields were left/set to the exact same value — e.g. Source
    Container and Target Container both "bp-natural-gas-02-src". This is
    independent of (and checked before) _find_name_collision()'s
    platform-existence check below: two names that neither one already
    exists on the real broker/storage account can still collide with each
    other within this one submission, which an existence check alone would
    never catch (both would individually report "available"). `get_value`
    has the same two-shapes contract as _find_name_collision().
    """
    for field, label, _check_fn in _name_checks(pipeline_type):
        value = get_value(field)
        if not value:
            continue
        for other_field, other_label, _ in _name_checks(pipeline_type):
            if other_field == field:
                break  # only compare against fields already seen, so each pair is reported once
            if get_value(other_field) == value:
                return field, (
                    f"{label} must be different from {other_label.lower()} — both are set to '{value}'."
                )
    return None


def _find_name_collision(pipeline_type: str, get_value) -> tuple[str, str] | None:
    """
    Return (field, error message) for the first proposed name that already
    exists on the platform's real Kafka broker / storage account, or None
    if none do. `get_value(field)` abstracts over the two shapes this is
    called with: preview's plain `names` dict of proposed defaults, and
    confirm's fully-built DataProductDefinition. The field name lets the
    caller highlight that one input inline (see the dpn-form-validation
    skill) instead of only showing a page-level banner.

    Every name is checked, source and target alike, by design: this
    platform only ever creates brand-new topics/containers itself, so an
    already-existing one — even what would elsewhere be a legitimate
    pre-existing upstream resource — is treated as a collision to block
    on, not something to reuse. This is also the final, authoritative
    check made right before /data-products/confirm actually creates
    anything — the review panel's live per-field checks (see
    check_name_available() below) are a faster-feedback UX layer on top,
    never a replacement for this one.
    """
    for field, label, check_fn in _name_checks(pipeline_type):
        value = get_value(field)
        if value and check_fn(value):
            return field, (
                f"{label} '{value}' already exists — this platform only creates "
                "brand-new topics/containers, choose a different name."
            )
    return None


def _find_collision(pipeline_type: str, get_value) -> tuple[str, str] | None:
    """Entry point used by both preview and confirm: a same-request
    duplicate among this pathway's own fields (_find_duplicate_field_values)
    is checked before — and independently of — whether any one of them
    already exists on the real platform (_find_name_collision)."""
    return _find_duplicate_field_values(pipeline_type, get_value) or _find_name_collision(pipeline_type, get_value)


@app.post("/data-products/check-name", dependencies=[Depends(auth.require_operator)])
def check_name_available(
    pipeline_type: str = Form(...),
    field: str = Form(...),
    value: str = Form(...),
):
    """
    Live per-field version of the collision check _find_name_collision()
    also does at preview/confirm time — called by
    ui/static/js/review-live-check.js on every edit to a review-step field
    (debounced) so the "already exists" state shows up immediately next to
    the field that's wrong, instead of only after "Create & Launch" is
    clicked. Never itself blocks creation — /data-products/confirm always
    re-runs _find_name_collision() (and registry_has_product()) right
    before create_product(), so a stale/bypassed live check can't let a
    real collision through.
    """
    value = value.strip()
    if not value:
        return JSONResponse({"status": "empty"})

    # product_type isn't one of _name_checks()'s pathway-specific
    # storage/topic fields — it's the review panel's own Data Product Name
    # field (see _review_panel.html), checked the same way
    # preview_data_product()/registry_has_product() do, independent of
    # pipeline_type.
    if field == "product_type":
        return JSONResponse(
            {"status": "duplicate" if registry_has_product(value) else "available"}
        )

    check_fn = next(
        (fn for f, _label, fn in _name_checks(pipeline_type) if f == field), None
    )
    if check_fn is None:
        # Unknown field/pipeline_type combination — nothing to check
        # against (also covers a stale request from a page still open on
        # the other pathway), so don't block the UI on it either way.
        return JSONResponse({"status": "available"})

    try:
        if check_fn(value):
            return JSONResponse({"status": "duplicate"})
    except Exception:  # noqa: BLE001 — mirrors precheck.py's own fail-soft
        _logger.warning("Live name check failed for %r=%r", field, value, exc_info=True)
        return JSONResponse({"status": "available"})

    return JSONResponse({"status": "available"})


@app.post("/data-products/confirm", dependencies=[Depends(auth.require_operator)])
async def confirm_data_product(request: Request):
    form = await request.form()

    try:
        step1 = DataProductCreate(
            product_type=form["product_type"],
            pipeline_type=form["pipeline_type"],
            schedule_seconds=int(form.get("schedule_seconds", 60)),
            execution_mode=form.get("execution_mode", "automatic"),
        )
    except Exception as exc:
        return _render_new(request, _new_form_context(request, error=str(exc)), status_code=400)

    def _field(name: str) -> str | None:
        value = form.get(name)
        return value.strip() if isinstance(value, str) and value.strip() else None

    extra: dict[str, str] = {}
    if step1.pipeline_type == "file":
        # No mapper container/topic: the file pathway's adaptor covers its
        # whole pipeline alone now (see env_builder.adaptor_env's dispatch)
        # — target_topic_name is its only remaining, user-editable topic
        # field. source/target container names are pure free-text storage
        # container names (validated in service/models.py, but never
        # templated to any fixed convention).
        for name in ("source_container_name", "target_container_name", "target_topic_name"):
            value = _field(name)
            if value:
                extra[name] = value
    else:
        # No mapper topic/group either — the topic pathway's adaptor
        # absorbed its schema mapper too. target_topic_name is a real,
        # user-editable field here (see
        # blueprints/producer/topic/adaptor/main.py's v3.1.0 changelog
        # entry) — not a read-only computed value.
        for name in ("src_topic_name", "target_topic_name"):
            value = _field(name)
            if value:
                extra[name] = value
        # src_group_id is never taken from the form at all — always
        # calculated from product_type, the same way it always defaulted
        # to (see service/models.py's former default_topic_names()
        # entry), just with no screen field and no override possible.
        extra["src_group_id"] = f"producer-topic-adaptor-{step1.product_type}"

    # Whatever the user actually typed (not the original proposed defaults)
    # — reused by every review.html re-render below this point so a
    # rejected submission doesn't lose their overrides.
    error_names = {name: _field(name) or "" for name in extra} if step1.pipeline_type == "file" else {
        name: _field(name) or "" for name in ("src_topic_name", "target_topic_name")
    }

    def _review_error(error: str, status_code: int, field_error: str | None = None):
        return _render_review(
            request,
            {
                "request": request,
                "step1": step1,
                "names": error_names,
                "org_name": env_builder.PLATFORM_ORG_NAME,
                "cloud_provider": env_builder.PLATFORM_CLOUD_PROVIDER,
                "error": error,
                "field_error": field_error,
                **_identity(request),
            },
            status_code=status_code,
        )

    # org_name/cloud_provider are platform-level, not user input — snapshot
    # the currently-configured values (env_builder.PLATFORM_ORG_NAME /
    # PLATFORM_CLOUD_PROVIDER) onto the stored definition. Storage
    # credentials themselves are never part of the definition at all; they
    # get injected straight into the container env at launch time
    # (env_builder._platform_cloud_env()).
    try:
        definition = DataProductDefinition(
            **step1.model_dump(),
            org_name=env_builder.PLATFORM_ORG_NAME,
            cloud_provider=env_builder.PLATFORM_CLOUD_PROVIDER,
            **extra,
        )
    except Exception as exc:
        # Most likely validate_storage_container_name() rejecting a
        # source/target container name (see service/models.py).
        return _review_error(str(exc), status_code=400)

    # Re-checked here (not just at /preview) against whatever was actually
    # submitted — the user may have edited a field after preview, and the
    # broker/storage account may have changed since then regardless.
    collision = _find_collision(step1.pipeline_type, lambda f: getattr(definition, f, None))
    if collision:
        collision_field, collision_error = collision
        return _review_error(collision_error, status_code=409, field_error=collision_field)

    try:
        registry.create_product(definition)
    except registry.DuplicateProductError:
        # A race, not a form mistake — another request created this exact
        # product_type between /preview and this /confirm. There's no Data
        # Product Name field on the review panel any more to attach this
        # to (see _review_panel.html), so — same as preview_data_product()'s
        # own check above — send the user back to Step 1's own field rather
        # than showing it on the review panel.
        return _render_new(
            request,
            _new_form_context(
                request,
                error=f"Data product '{step1.product_type}' already exists.",
                field_error="product_type",
                step1=step1,
            ),
            status_code=409,
        )

    # This is the step that replaces "copy blueprint folder, rename, build
    # Dockerfile": the same generic images get launched with this product's
    # env vars. In production this call becomes a Kubernetes API call
    # (KubernetesPodOperator / CRD reconcile) instead of a podman subprocess.
    #
    # confirm_data_product is `async def` (needed for `await request.form()`
    # above), so unlike every other route in this file — plain `def`, which
    # Starlette itself offloads to a worker thread — nothing moves this off
    # the event loop for us. runtime.launch() shells out to `podman`/`docker`
    # synchronously and, the first time a pathway's image hasn't been built
    # yet, blocks on a full `podman build` (easily minutes). Left un-awaited
    # like this it freezes the entire app for that whole time, which reads
    # to the browser/any reverse proxy in front as a hung connection —
    # frequently surfacing as a 502 on "Create & Launch" — rather than a
    # merely slow response.
    await run_in_threadpool(runtime.launch, definition)
    lifecycle_events.emit(
        definition.adaptor_container_name,
        "created",
        product_type=definition.product_type,
        pipeline_type=definition.pipeline_type,
    )

    # ?created=... triggers index()'s "Publishing Successful!" toast (see
    # ui/templates/_toast.html) — the only redirect target in this file
    # that should show it, since it's the only one following an actual
    # creation.
    return RedirectResponse(url=f"/producer-pipelines?created={definition.product_type}", status_code=303)


@app.post("/data-products/{product_type}/stop", dependencies=[Depends(auth.require_operator)])
def stop_data_product(product_type: str):
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    runtime.stop(product)
    registry.set_active(product_type, is_active=False)
    lifecycle_events.emit(product.adaptor_container_name, "stopped", product_type=product_type)
    return RedirectResponse(url="/producer-pipelines", status_code=303)


@app.post("/data-products/{product_type}/delete", dependencies=[Depends(auth.require_admin)])
def delete_data_product(product_type: str):
    """
    Permanently removes a data product — its containers (runtime.stop(),
    same 404-tolerant delete Stop uses, so this is safe even when the
    Deployment was already removed outside this UI — see index.html's
    "gone" row state), its Kafka topic(s) and storage container(s) (see
    precheck.delete_kafka_topic()/delete_storage_container() — the
    teardown counterparts to the exists() checks already used by the
    create flow's precheck/collision logic), AND its registry.py entry.
    Unlike Stop, there is no Start to bring this back afterward; recreating
    it means going through /create-new-data-product again, including a
    fresh name since this platform only ever creates brand-new
    topics/containers (see models.py's DataProductDefinition docstring).
    registry.delete_product() has existed since this registry module was
    written but was never wired to a route until now.
    """
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    runtime.stop(product)

    if product.pipeline_type == "file":
        if product.source_container_name:
            precheck.delete_storage_container(product.source_container_name, side="source")
        if product.target_container_name:
            precheck.delete_storage_container(product.target_container_name, side="target")
    else:
        if product.src_topic_name:
            precheck.delete_kafka_topic(product.src_topic_name)
    if product.target_topic_name:
        precheck.delete_kafka_topic(product.target_topic_name)

    registry.delete_product(product_type)
    lifecycle_events.emit(product.adaptor_container_name, "deleted", product_type=product_type)
    return RedirectResponse(url="/producer-pipelines", status_code=303)


@app.post("/data-products/{product_type}/start", dependencies=[Depends(auth.require_operator)])
def start_data_product(product_type: str):
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    runtime.launch(product)
    registry.set_active(product_type, is_active=True)
    lifecycle_events.emit(product.adaptor_container_name, "started", product_type=product_type)
    return RedirectResponse(url="/producer-pipelines", status_code=303)


@app.post("/data-products/{product_type}/pause", dependencies=[Depends(auth.require_operator)])
def pause_data_product(product_type: str):
    """
    EXECUTION_MODE=manual — the containers keep running and heartbeating,
    but every scheduler backend skips the actual pipeline run. Distinct
    from Stop, which removes the containers entirely.
    """
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    updated = registry.set_execution_mode(product_type, "manual")
    if product.is_active:
        runtime.set_execution_mode(updated)
    lifecycle_events.emit(product.adaptor_container_name, "paused", product_type=product_type)
    return RedirectResponse(url="/producer-pipelines", status_code=303)


@app.post("/data-products/{product_type}/resume", dependencies=[Depends(auth.require_operator)])
def resume_data_product(product_type: str):
    """EXECUTION_MODE=automatic — resume scheduled runs."""
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    updated = registry.set_execution_mode(product_type, "automatic")
    if product.is_active:
        runtime.set_execution_mode(updated)
    lifecycle_events.emit(product.adaptor_container_name, "resumed", product_type=product_type)
    return RedirectResponse(url="/producer-pipelines", status_code=303)


def _run_once_and_record(product_type: str, stage: str, container_name: str) -> None:
    """Background half of /run below — the exec itself can take up to
    schedule_seconds per stage on the topic pathway (see
    utils/scheduler_backend.py's run_once()), so it runs after the HTTP
    response goes out rather than blocking the request."""
    try:
        output = runtime.run_once(container_name)
    except Exception as exc:
        # Broad on purpose: podman_ops raises RuntimeError (container not
        # running), k8s_ops can raise kubernetes.client.exceptions.ApiException
        # (e.g. an RBAC 403 on pods/exec) or a websocket error — anything
        # the backend throws lands here. record_failure() (not
        # record_from_logs(), which only recognizes structured JSON log
        # lines) is what actually gets this into Run History, so a Run Now
        # failure is visible instead of vanishing silently.
        run_history.record_failure(product_type, stage, str(exc))
        return
    run_history.record_from_logs(product_type, stage, output)


@app.post("/data-products/{product_type}/run", dependencies=[Depends(auth.require_operator)])
def run_data_product_once(product_type: str, background_tasks: BackgroundTasks):
    """
    "Run Now" — force one pipeline cycle right now, independent of
    schedule_seconds. Only available when Execution = manual (the index
    page's automatic/manual dropdown, POST .../pause or .../resume) — a
    product left on automatic already runs on its own interval, so this
    stays disabled (both in index.html and here) until it's switched to
    manual. Execs into the already-running container (runtime.run_once)
    rather than restarting it, so the persistent scheduler loop — idle in
    manual, ticking in automatic — is left alone. Every pathway is
    single-container now (see env_builder.adaptor_env's dispatch), so this
    always runs just the one adaptor.
    """
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    if not product.is_active:
        raise HTTPException(status_code=409, detail="Start the data product before running it manually")

    if product.execution_mode != "manual":
        raise HTTPException(
            status_code=409,
            detail="Set Execution to manual before using Run Now — automatic mode already runs on its own interval",
        )

    lifecycle_events.emit(product.adaptor_container_name, "run_triggered", product_type=product_type)
    background_tasks.add_task(
        _run_once_and_record, product_type, "adaptor", product.adaptor_container_name
    )
    return RedirectResponse(url=f"/run-history?product_type={product_type}", status_code=303)




@app.get("/data-products/{product_type}/logs")
def data_product_logs(request: Request, product_type: str):
    try:
        product = registry.get_product(product_type)
    except registry.ProductNotFoundError:
        raise HTTPException(status_code=404, detail="Unknown data product")

    return templates.TemplateResponse(
        request,
        "logs.html",
        {
            "request": request,
            "product": product,
            "adaptor_logs": runtime.logs(product.adaptor_container_name),
            **_identity(request),
        },
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
