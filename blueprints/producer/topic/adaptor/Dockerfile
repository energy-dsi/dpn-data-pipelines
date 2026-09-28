# ---------- builder stage ----------
# Full toolchain lives here ONLY. confluent-kafka compiles a C extension
# against librdkafka at install time, which needs gcc/python3-dev/librdkafka-dev.
# None of this is copied into the final image.
FROM docker.io/library/alpine:edge AS builder

RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    gcc \
    g++ \
    build-base \
    librdkafka-dev

RUN python3 -m venv /adaptor/venv
ENV PATH="/adaptor/venv/bin:$PATH"
WORKDIR /adaptor

COPY requirements.txt .
COPY utils ./utils
COPY dpn_observability_sdk ./dpn_observability_sdk
COPY setup.py .

# Dependencies resolved from PyPI at build time (see requirements.txt).
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir . \
 && pip uninstall -y pip

# ---------- runtime stage ----------
FROM docker.io/library/alpine:edge

RUN apk upgrade --no-cache
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
RUN apk add --no-cache \
    python3 \
    librdkafka

# Alpine's python3 package installs pip via ensurepip even though py3-pip
# is not explicitly listed here. The venv builder above already removed pip
# from the venv (pip uninstall -y pip), but the system-level pip installed
# under /usr/lib/python3.x/ by ensurepip persists and is detected by Xray
# as CVE-2018-20225. Remove it from the system Python installation here —
# it is not needed at runtime since the venv is self-contained.
RUN python3 -m pip uninstall -y pip 2>/dev/null || true

COPY --from=builder --chown=root:root --chmod=755 /adaptor/venv /adaptor/venv
ENV PATH="/adaptor/venv/bin:$PATH"

WORKDIR /adaptor

# No ARG PRODUCT_TYPE and no per-product COPY: this image is genuinely generic.
# srcTopicName, mapperTopicName, srcGroupId, PRODUCT_NAME are all supplied at
# `podman run` / pod-spec time via env vars — the same image serves every
# data product on the topic-based integration pathway.
COPY --chown=root:root --chmod=755 blueprints/producer/topic/adaptor/main.py .

USER appuser

CMD ["python", "main.py"]
