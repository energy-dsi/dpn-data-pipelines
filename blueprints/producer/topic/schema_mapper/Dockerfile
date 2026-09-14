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

RUN python3 -m venv /schema_mapper/venv
ENV PATH="/schema_mapper/venv/bin:$PATH"
WORKDIR /schema_mapper

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

COPY --from=builder --chown=root:root --chmod=755 /schema_mapper/venv /schema_mapper/venv
ENV PATH="/schema_mapper/venv/bin:$PATH"

WORKDIR /schema_mapper

# No ARG PRODUCT_TYPE and no per-product COPY: this image is genuinely generic.
# mapperTopicName, mapperGroupId, schemaType, orgName, productType are all
# supplied at `podman run` / pod-spec time via env vars.
COPY --chown=root:root --chmod=755 blueprints/producer/topic/schema_mapper/main.py .

USER appuser

CMD ["python", "main.py"]
