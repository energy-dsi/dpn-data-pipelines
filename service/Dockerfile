# Self-service UI image.
#
# Two runtime backends select which of service/{podman_ops,k8s_ops}.py
# actually launches per-product containers (service/runtime.py, via
# DPN_RUNTIME):
#
#   DPN_RUNTIME=podman (default)  — shells out to the `podman` CLI. Only
#     works when this process itself has access to a podman socket, which a
#     plain container generally doesn't (podman-in-podman needs a bind-mounted
#     socket and a podman-remote client — see service/README.md for why
#     the *local* demo instead runs this app directly on the host).
#
#   DPN_RUNTIME=kubernetes — uses the Kubernetes API (in-cluster
#     ServiceAccount credentials) to create Deployments instead. This is the
#     path service/charts/ (the AKS deployment of this image) actually
#     uses — see docs/AKS_DEPLOYMENT.md.
#
# NOTE: python:3.12-slim is a floating tag — its underlying Debian base
# (Bookworm or Trixie) shifts with each upstream rebuild. Pin to a digest
# here to get reproducible builds and avoid silently picking up a new OS:
#   docker pull python:3.12-slim && docker inspect --format='{{index .RepoDigests 0}}' python:3.12-slim
# Replace the tag below with the resulting sha256 digest, e.g.:
#   FROM docker.io/library/python:3.12-slim@sha256:<digest>
# Long-term: rebase to registry.redhat.io/ubi9/python-312-minimal to comply
# with IBM container image security policy (requires compatibility testing).

# ── Builder stage ─────────────────────────────────────────────────────────────
# pip is used HERE for the install. This stage is discarded and never pushed.
# NOTE: pip is ALSO present in the python:3.12-slim base image, so the runtime
# stage below has to strip it AND flatten the result (see "Runtime stage") —
# a later `pip uninstall` alone leaves pip in the base-image layer, which
# JFrog Xray flags (CVE-2018-20225, XRAY-97724: disputed, no fixed version, so
# upgrading pip does not clear it — the only fix is to not ship pip).
FROM docker.io/library/python:3.12-slim AS builder
WORKDIR /app
COPY service/requirements.txt .
# --prefix writes the installed packages under /install/lib/pythonX.Y/
# site-packages, keeping them separate from the base image's own site-packages
# so the COPY below is surgical and does not bring the builder's OS state.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage 1: assemble the filesystem and strip pip ───────────────────
# python:3.12-slim ships pip in its own base layers (/usr/local/lib/python3.12/
# site-packages/pip, /usr/local/bin/pip*, and the ensurepip bundled wheel).
# Removing it in this stage is not enough on its own: the base layer would
# still contain pip, and Xray scans layers individually. Stage 2 below flattens
# this stage into a single fresh layer so pip never exists in any layer of
# the pushed image.
FROM docker.io/library/python:3.12-slim AS runtime-prep

# Apply all available OS-level security patches from the Debian security
# repository. CVEs that still appear after this step (libc6, util-linux,
# ncurses, libpam, perl, zlib, libacl1, libattr1) have a blank FIXED VERSIONS
# column in Xray — Debian has not yet published patches for them. apt-get
# upgrade is already in place and will pick them up automatically on the next
# image rebuild once Debian releases fixes.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Remove pip (CVE-2018-20225). The app's packages are installed by the builder
# stage and copied in below, so pip is not needed at runtime. The explicit rm
# is a belt-and-braces cleanup for anything `pip uninstall` leaves behind, plus
# the ensurepip bundled wheel (which scanners also report as a pip copy).
RUN python -m pip uninstall -y pip \
 && rm -rf /usr/local/lib/python3.12/site-packages/pip \
           /usr/local/lib/python3.12/site-packages/pip-*.dist-info \
           /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.12 \
           /usr/local/lib/python3.12/ensurepip/_bundled/pip-*.whl \
           /root/.cache

# confluent-kafka's PyPI wheel (installed below, unlike the blueprint
# adaptor images which compile it from source against Alpine's own
# librdkafka-dev — see blueprints/producer/topic/adaptor/Dockerfile) bundles
# a statically-linked librdkafka/OpenSSL/libcurl built on a RHEL-style
# baseline. Its OAUTHBEARER/OIDC token-endpoint HTTP client hardcodes
# /etc/pki/tls/certs/ca-bundle.crt for CA trust and ignores ssl.ca.location
# AND the SSL_CERT_FILE env var utils/kafka_security.py sets — confirmed
# directly: OAUTHBEARER calls fail with "error setting certificate file:
# /etc/pki/tls/certs/ca-bundle.crt (-1)" even with SSL_CERT_FILE correctly
# pointed elsewhere, because that path doesn't exist at all on this Debian
# base. Pre-creating it here, owned by appuser, lets
# kafka_security.build_kafka_client_config() write the resolved CA there too
# at runtime (in addition to SSL_CERT_FILE, which still covers any future
# confluent-kafka build that does honor it).
RUN mkdir -p /etc/pki/tls/certs

RUN useradd --system --create-home --uid 10000 appuser
RUN chown appuser:appuser /etc/pki/tls/certs
WORKDIR /app

# Copy the pre-installed Python packages from the builder.
# /install mirrors the /usr/local layout (lib/pythonX.Y/site-packages, bin/),
# so merging it into /usr/local makes all packages importable without any
# PYTHONPATH change. pip itself is not included — pip install --prefix only
# installs the requested packages, not the tool that ran them.
COPY --from=builder /install /usr/local

# service/precheck.py reuses utils/topic_utils.py and utils/data_transection.py
# (Kafka topic / storage container existence checks), and
# service/lifecycle_events.py reuses dpn_observability_sdk/otel_logger.py —
# this image doesn't build the dpn_pipe_line_sdk package the adaptor images
# do (see setup.py), so both are copied in directly rather than installed.
# otel_logger.py's real OTLP export path does need the opentelemetry-*
# packages pinned in service/requirements.txt above (same pins as the
# adaptor images) — it is not stdlib-only.
COPY utils ./utils
COPY dpn_observability_sdk ./dpn_observability_sdk
COPY service ./service
COPY ui ./ui

# ── Runtime stage 2: flatten into one pip-free layer ─────────────────────────
# Copying the whole filesystem of runtime-prep into an empty image produces a
# single layer, so the python:3.12-slim base layer (which contained pip) is
# not part of the pushed image. Image metadata (ENV/WORKDIR/USER/EXPOSE/CMD)
# does NOT carry over from a COPY, so it is re-declared explicitly below —
# the ENV values mirror the python:3.12-slim base image.
FROM scratch
COPY --from=runtime-prep / /
ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin \
    LANG=C.UTF-8
WORKDIR /app
USER appuser
EXPOSE 8000
CMD ["uvicorn", "ui.app:app", "--host", "0.0.0.0", "--port", "8000"]