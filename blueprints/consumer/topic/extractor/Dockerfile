# ---------- builder stage ----------
# Full toolchain lives here ONLY. confluent-kafka compiles a C extension
# against librdkafka at install time, which needs gcc/python3-dev/librdkafka-dev.
# None of this is copied into the final image.
FROM docker.io/library/alpine:edge AS builder

# Install Python + build deps (NO VERSION PINNING)
# Alpine base is edge (rolling); provides Python 3.14, NOT version-pinned.
# Individual apk package pinning is avoided due to repository volatility.
RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    gcc \
    g++ \
    build-base \
    librdkafka-dev

# Build the virtual environment
RUN python3 -m venv /extractor/venv
ENV PATH="/extractor/venv/bin:$PATH"
WORKDIR /extractor

# Copy dependencies
COPY requirements.txt .
COPY utils ./utils
COPY dpn_observability_sdk ./dpn_observability_sdk
COPY setup.py .

# Dependencies resolved from PyPI at build time (see requirements.txt).
# NOTE: install the local package NON-editable (no `-e`). An editable install
# points back at the source tree (/extractor/utils, ...), which is NOT copied
# into the runtime stage — only the venv is — so `import utils` would fail at
# runtime. A regular install bakes utils/ + dpn_observability_sdk/ into the
# venv's site-packages, making the copied venv self-contained.
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir . \
 && pip uninstall -y pip
# ^ pip removed from the venv: it is not needed at runtime and carries its own
#   CVEs (e.g. CVE-2026-8643). Removing it needs no network. If you instead need
#   pip at runtime, drop the `pip uninstall` line (pip>=26.1.2 pulled from PyPI).

# ---------- runtime stage ----------
# Ships only the interpreter and the librdkafka *shared* library.
# Drops binutils / python3-dev / gcc / g++ / build-base entirely,
# clearing CVE-2026-6846 and the python3-dev component of CVE-2026-7210.
FROM docker.io/library/alpine:edge

# Pull in any published security patches for base packages (e.g. the
# pyc / python3-pycache-pyc0 component of CVE-2026-7210 once Alpine ships a fix).
RUN apk upgrade --no-cache

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Install runtime OS packages and immediately remove the system-level pip
# installed by ensurepip in a SINGLE layer. Splitting this into two RUN
# instructions would commit pip into an intermediate layer, which JFrog Xray
# scans independently and would flag as CVE-2018-20225.
RUN apk add --no-cache python3 librdkafka \
 && python3 -m pip uninstall -y pip 2>/dev/null || true

# Copy the pre-built virtual environment from the builder
COPY --from=builder --chown=root:root --chmod=755 /extractor/venv /extractor/venv
ENV PATH="/extractor/venv/bin:$PATH"

WORKDIR /extractor

# Copy application code
COPY --chown=root:root --chmod=755 blueprints/consumer/topic/extractor/main.py .

USER appuser

CMD ["python", "main.py"]
