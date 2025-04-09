FROM alpine:latest

# Create a non-root user first
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Install pip
RUN apk update && apk add py3-pip=24.3.1-r0

# Install gcc and other necessary build tools
RUN apk update && apk add gcc=14.2.0-r4 g++=14.2.0-r4

# Install python3-dev and build-essential
RUN apk update && apk add python3-dev=3.12.9-r0
RUN apk update && apk add build-base=0.5-r3
RUN apk update && apk add librdkafka-dev=2.6.1-r0

# Create a virtual environment and activate it
RUN python3 -m venv /mapper-extractor/venv && chown -R appuser:appgroup /mapper-extractor
ENV PATH="/mapper-extractor/venv/bin:$PATH"

# Switch to non-root user
USER appuser

# Set the working directory
WORKDIR /mapper-extractor

# Copy the requirements.txt file into the container
COPY --chown=appuser:appgroup ./requirements.txt /mapper-extractor/

# Install the required Python packages
RUN pip install -r requirements.txt

# Install additional python packages
RUN pip install setuptools==70.3.0

# Create needed directories
RUN mkdir -p /mapper-extractor/
RUN mkdir -p /mapper-extractor/validation_files/
RUN mkdir -p /mapper-extractor/telicent_lib/

# Copy the assessment files into the container
COPY --chown=appuser:appgroup ./.env /mapper-extractor/
COPY --chown=appuser:appgroup ./mapper.py /mapper-extractor/
COPY --chown=appuser:appgroup ./mapping_function.py /mapper-extractor/
COPY --chown=appuser:appgroup ./telicent_lib/ /mapper-extractor/telicent_lib

# Entrypoint to execute the mapper
ENTRYPOINT ["python","mapper.py"]
