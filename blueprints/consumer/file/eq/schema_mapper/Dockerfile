FROM alpine:3.22
 
# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
 
# Install Python + build deps (NO VERSION PINNING)
RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    gcc \
    g++ \
    build-base \
    librdkafka-dev
 
# Create virtual environment
RUN python3 -m venv /schema_mapper/venv && chown -R appuser:appgroup /schema_mapper
ENV PATH="/schema_mapper/venv/bin:$PATH"
 
WORKDIR /schema_mapper
USER appuser

# Copy dependencies
COPY --chown=appuser:appgroup requirements.txt .
COPY --chown=appuser:appgroup utils ./utils
COPY --chown=appuser:appgroup setup.py .
 
# Upgrade pip inside venv (this is safe)
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt && pip install -e .
 
# Copy application code
COPY --chown=appuser:appgroup consumer/file/eq/schema_mapper/main.py .
COPY --chown=appuser:appgroup consumer/file/eq/.env .

CMD ["python", "main.py"]