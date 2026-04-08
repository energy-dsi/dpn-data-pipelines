ARG ACR_NAME
FROM ${ACR_NAME}.azurecr.io/alpine:3.22
 
# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
 
# Install Python + build deps (NO VERSION PINNING)
# Alpine base image is pinned (3.22), ensuring deterministic package versions.
# Individual apk package pinning is avoided due to repository volatility.
RUN apk add --no-cache \
    python3=3.11.8-r0 \
    py3-pip=23.3.2-r0 \
    python3-dev=3.11.8-r0 \
    gcc=12.2.1_git20220924-r10 \
    g++=12.2.1_git20220924-r10 \
    build-base=0.5-r3 \
    librdkafka-dev=2.2.0-r0

# Create virtual environment
RUN python3 -m venv /schema_mapper/venv && chown -R appuser:appgroup /schema_mapper
ENV PATH="/schema_mapper/venv/bin:$PATH"
 
WORKDIR /schema_mapper
USER appuser

# Copy dependencies
COPY --chown=root:root --chmod=755 requirements.txt .
COPY --chown=root:root --chmod=755 utils ./utils
COPY --chown=root:root --chmod=755 setup.py .
 
# Upgrade pip inside venv (this is safe)
RUN pip install --no-cache-dir -r requirements.txt && pip install -e .
 
# Copy application code
COPY --chown=root:root --chmod=755 blueprints/producer/file/eq/schema_mapper/main.py .
COPY --chown=root:root --chmod=755 blueprints/producer/file/eq/.env .

CMD ["python", "main.py"]