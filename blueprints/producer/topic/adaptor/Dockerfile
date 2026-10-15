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
# Install runtime OS packages and immediately remove the system-level pip
# installed by ensurepip in a SINGLE layer. Splitting this into two RUN
# instructions would commit pip into an intermediate layer, which JFrog Xray
# scans independently and would flag as CVE-2018-20225.
RUN apk add --no-cache python3 librdkafka \
 && python3 -m pip uninstall -y pip 2>/dev/null || true

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
