# dp-consumer-stack

A local 3-container consumer pipeline used to smoke-test the CVE-hardened
multi-stage image. All three containers run on
`localhost/dpn-extractor-fixed:test` (the multi-stage build of
[consumer/file/extractor/Dockerfile](../consumer/file/extractor/Dockerfile),
which drops the build toolchain — no `binutils`, no `python3-dev` — from the
runtime layer).

| Container            | Role   | Pipeline stage                         |
|----------------------|--------|----------------------------------------|
| `dp-consumer-stage`  | source | extracts / stages incoming data        |
| `dp-consumer-trfm`   | mapper | schema-maps / transforms               |
| `dp-consumer-target` | target | writes to the target sink              |

## Usage

```sh
# Build the hardened image first (from repo root):
podman tag docker.io/library/alpine:3.22 <<CONTAINER_REGISTRY_URL>>/alpine:3.22   # local only
podman build -f consumer/file/extractor/Dockerfile -t dpn-extractor-fixed:test .

# Bring the stack up / down:
podman-compose -f dp-consumer-stack/compose.yaml up -d
podman-compose -f dp-consumer-stack/compose.yaml down
```

## Note on real workloads

The containers currently run a keep-alive command that prints their role, so the
stack can be spun up and inspected without a Kafka broker. To run the actual
consumer apps, wire in a Kafka broker + the required env config and change each
service's `command:` to `["python", "main.py"]` (pointing each container at its
respective `main.py`: extractor for stage, schema_mapper for trfm, and the target
writer for target once that Dockerfile receives the same multi-stage treatment).
