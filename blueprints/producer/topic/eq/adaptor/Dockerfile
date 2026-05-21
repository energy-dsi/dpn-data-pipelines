ARG ACR_NAME
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

RUN python3 -m venv /adaptor/venv && chown -R appuser:appgroup /adaptor
ENV PATH="/adaptor/venv/bin:$PATH"

WORKDIR /adaptor
USER appuser

COPY --chown=root:root --chmod=755 requirements.txt .
COPY --chown=root:root --chmod=755 packages .
COPY --chown=root:root --chmod=755 utils ./utils
COPY --chown=root:root --chmod=755 setup.py .

RUN pip install --no-cache-dir -r requirements.txt && pip install -e .

COPY --chown=root:root --chmod=755 producer/topic/adaptor/main.py .

CMD ["python", "main.py"]
