ARG ACR_NAME
FROM ${ACR_NAME}.azurecr.io/alpine:3.22
 
# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
 
# Install Python + build deps (NO VERSION PINNING)
# Alpine base image is pinned (3.22), ensuring deterministic package versions.
# Individual apk package pinning is avoided due to repository volatility.
RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    gcc \
    g++ \
    build-base \
    librdkafka-dev
 
# Create virtual environment
RUN python3 -m venv /adaptor/venv && chown -R appuser:appgroup /adaptor
ENV PATH="/adaptor/venv/bin:$PATH"
 
WORKDIR /adaptor
USER appuser
 
# Copy dependencies
COPY --chown=root:root --chmod=755 requirements.txt .
COPY --chown=root:root --chmod=755 utils ./utils
COPY --chown=root:root --chmod=755 setup.py .
 
# Upgrade pip inside venv (this is safe)
RUN pip install --no-cache-dir -r requirements.txt && pip install -e .
 
# Copy application code
COPY --chown=root:root --chmod=755 blueprints/producer/file/eq/adaptor/main.py .
COPY --chown=root:root --chmod=755 blueprints/producer/file/eq/.env .

CMD ["python", "main.py"]