ARG ACR_NAME=acr
FROM ${ACR_NAME}.azurecr.io/alpine:3.22

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    gcc \
    g++ \
    build-base \
    librdkafka-dev

RUN python3 -m venv /schema_mapper/venv && chown -R appuser:appgroup /schema_mapper
ENV PATH="/schema_mapper/venv/bin:$PATH"

WORKDIR /schema_mapper
USER appuser

COPY --chown=root:root --chmod=755 requirements.txt .
COPY --chown=root:root --chmod=755 packages ./packages
COPY --chown=root:root --chmod=755 utils ./utils
COPY --chown=root:root --chmod=755 dpn_observability_sdk ./dpn_observability_sdk
COPY --chown=root:root --chmod=755 setup.py .

RUN pip install --no-cache-dir -r requirements.txt && pip install -e .

COPY --chown=root:root --chmod=755 consumer/topic/schema_mapper/main.py .

CMD ["python", "main.py"]
