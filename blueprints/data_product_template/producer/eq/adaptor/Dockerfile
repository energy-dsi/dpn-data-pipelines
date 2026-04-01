ARG ACR_NAME
FROM ${ACR_NAME}.azurecr.io/alpine:3.22
 
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
RUN python3 -m venv /adaptor/venv && chown -R appuser:appgroup /adaptor
ENV PATH="/adaptor/venv/bin:$PATH"
 
WORKDIR /adaptor
USER appuser
 
# Copy dependencies
COPY --chown=appuser:appgroup requirements.txt .
 
# Upgrade pip inside venv (this is safe)
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt
 
# Copy application code
COPY --chown=appuser:appgroup main.py .
COPY --chown=appuser:appgroup producer/producer/eq/adaptor/processors ./processor
COPY --chown=appuser:appgroup utils ./utils

CMD ["python", "producer.py"]