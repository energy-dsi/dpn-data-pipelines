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
RUN python3 -m venv /adaptor/venv
ENV PATH="/adaptor/venv/bin:$PATH"
WORKDIR /adaptor

# Copy dependencies
COPY requirements.txt .
COPY packages ./packages
COPY utils ./utils
COPY dpn_observability_sdk ./dpn_observability_sdk
COPY setup.py .

# Air-gapped install: --no-index forces resolution from ./packages only, so a
# missing/incompatible wheel fails the build loudly instead of silently reaching
# for PyPI (which is unreachable here anyway).
# NOTE: install the local package NON-editable (no `-e`). An editable install
# points back at the source tree (/adaptor/utils, ...), which is NOT copied
# into the runtime stage — only the venv is — so `import utils` would fail at
# runtime. A regular install bakes utils/ + dpn_observability_sdk/ into the
# venv's site-packages, making the copied venv self-contained.
RUN pip install --no-cache-dir --no-index --find-links packages -r requirements.txt \
 && pip install --no-cache-dir --no-index --find-links packages . \
 && pip uninstall -y pip
# ^ pip removed from the venv: it is not needed at runtime and carries its own
#   CVEs (e.g. CVE-2026-8643). Removing it needs no network. If you instead need
#   pip at runtime, drop the `pip uninstall` line and add a patched
#   pip wheel (pip>=26.1.2) to ./packages so the venv upgrade stays offline.

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

# Runtime needs the python interpreter and the librdkafka shared lib only
RUN apk add --no-cache \
    python3 \
    librdkafka

# Copy the pre-built virtual environment from the builder
COPY --from=builder --chown=root:root --chmod=755 /adaptor/venv /adaptor/venv
ENV PATH="/adaptor/venv/bin:$PATH"

WORKDIR /adaptor

ARG PRODUCT_TYPE

# Copy application code
COPY --chown=root:root --chmod=755 producer/file/${PRODUCT_TYPE}/adaptor/main.py .

USER appuser

CMD ["python", "main.py"]
