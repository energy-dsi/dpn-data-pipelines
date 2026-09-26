
# dsi-data-pipeline

The **dsi-data-pipeline** enables ingestion, transformation, validation, and publishing of data for DSI workloads.
It integrates with Azure Kubernetes Service (AKS), Azure Container Registry (ACR), EventHub/Kafka, and supports modular
adaptors and mappers for different data domains.

This README consolidates:
- Overall pipeline architecture
- Project structure
- Build and deployment workflow
- **Full end‑to‑end implementation steps** (from the uploaded implementation doc)

---

## 🚀 Key Features
- **No hardcoded names** — all container names, filenames, and environment values moved to config.
- **Config-driven architecture** for adaptors, mappers, schema workflows, and processing logic.
- **Local wheel library support** for adaptors/mappers to allow plug‑and‑play extensions.
- **Kafka plaintext protocol** enabled as per pipeline requirement.
- **Podman + ACR container workflow** for building and pushing runtime components.
- **AKS-native deployment** with secret injection, configmaps, jobs, and long-running deployments.

---

## 📁 Project Structure

```
dsi-data-pipelines/
├── .azure-pipelines/        # CI/CD definitions
├── .docs/                   # Internal architecture docs
├── .idea/                   # IDE configs
├── adaptor/                 # Data source adaptors (DL, EQBD, etc.)
├── blueprints/              # Pipeline blueprint definitions
├── mappers/                 # Schema assurance, security labels, extractor consumers
├── smoke-test/              # Smoke tests
├── tests/                   # Integration / Unit tests
├── __init__.py
├── DL-ORG-A.xml.xml
├── DL-ORG-B.xml.xml
├── EQ-ORG-A.xml.xml
├── EQ-ORG-B.xml.xml
├── SSH1-ORG-B.xml.xml
├── SSH2-ORG-B.xml.xml
├── producer-eqbd.tar        # Producer bundle
└── README.md
```

---


## 1️⃣ Authenticate & Configure AKS
```bash
az aks install-cli
az login
az account set --subscription xxxxxx
az aks get-credentials --resource-group rg-aks-dpn-dev-xxx-xxx --name aks-dpn-dev-xxx-xxx --overwrite-existing
kubelogin convert-kubeconfig -l azurecli
kubectl config set-context aks-dpn-dev-uks-xx --namespace=ns-dpn-xx
```

## 2️⃣ Podman Build & Push (ACR Login Included)
```bash
# Acquire token & login to ACR
$accessToken=az acr login --name <<CONTAINER_REGISTRY_URL>> --expose-token
podman login <<CONTAINER_REGISTRY_URL>> --username xxxxx --password $accessToken

# Build & push DL adaptor
podman build --platform linux/amd64 --format docker -f adaptor/dl/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/adaptor-dl:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/adaptor-dl:v1
```

## 3️⃣ Create Kubernetes Secrets (base64 literals as provided)
```bash
kubectl create secret generic dl-adaptor-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-src:9092'  base64)"   --from-literal=BLOB_SAS_TOKEN="$(echo -n 'sv=XXX...' base64)"

kubectl create secret generic eqbd-adaptor-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-src:9092' base64)"   --from-literal=BLOB_SAS_TOKEN="$(echo -n 'sv=XXX...' base64)"

kubectl create secret generic eqbd-extractor-consumer-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-target:9092' base64)"   --from-literal=BLOB_CONNECTION_STRING="$(echo -n 'BlobEndpoint=<<your specific value>>/;SharedAccessSignature=sv=...' base64)"

kubectl create secret generic eqbd-schema-assurance-producer-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-src:9092' base64)"

kubectl create secret generic eqbd-security-labels-producer-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-src:9092' base64)"

kubectl create secret generic eqbd-schema-assurance-consumer-secrets --from-literal=EVENTHUB_CONNECTION_STRING="$(echo -n 'dpn-kafka-target:9092' base64)"
```

## 4️⃣ Apply ConfigMaps, Jobs, Deployments
```bash
# DL job
kubectl apply -f dpl-dl-configmap.yml
kubectl apply -f dpl-dl-job.yml

# EQBD adaptor
kubectl apply -f dpl-eqbd-adaptor-configmap.yml
kubectl delete job eqbd-adaptor
kubectl apply -f dpl-eqbd-adaptor-job.yml

# Producers
kubectl apply -f dpl-eqbd-schema-assurance-producer-configmap.yml
kubectl apply -f dpl-eqbd-schema-assurance-producer-deploy.yml
kubectl apply -f dpl-eqbd-security-labels-producer-configmap.yml
kubectl apply -f dpl-eqbd-security-labels-producer-deploy.yml

# Consumers / Extractor
kubectl apply -f dpl-eqbd-schema-assurance-consumer-configmap.yml
kubectl apply -f dpl-eqbd-schema-assurance-consumer-deploy.yml
kubectl apply -f dpl-eqbd-consumer-extractor-configmap.yml
kubectl apply -f dpl-eqbd-consumer-extractor-deploy.yml
```

## 5️⃣ Additional Podman Builds (EQBD & Mappers)
```bash
podman build --platform linux/amd64 --format docker -f adaptor/eqbd/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/adaptor-eqbd:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/adaptor-eqbd:v1

podman build --platform linux/amd64 --format docker -f mappers/schema-assurance-producer/eqbd/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/producer-schema-assurance-eqbd:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/producer-schema-assurance-eqbd:v1

podman build --platform linux/amd64 --format docker -f mappers/security-labels-producer/eqbd/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/producer-security-labels-eqbd:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/producer-security-labels-eqbd:v1

podman build --platform linux/amd64 --format docker -f mappers/schema-assurance-consumer/eq-dno-org-a/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/consumer-schema-assurance-eqbd:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/consumer-schema-assurance-eqbd:v1

podman build --platform linux/amd64 --format docker -f mappers/extractor-consumer/eq-dno-org-a/Dockerfile -t <<CONTAINER_REGISTRY_URL>>/extractor-consumer-eqbd:v1
podman push --format docker <<CONTAINER_REGISTRY_URL>>/extractor-consumer-eqbd:v1
```

## 6️⃣ Cleanup (Tear‑down)
```bash
# Jobs & Deployments
kubectl delete job eqbd-adaptor
kubectl delete deploy eqbd-schema-assurance-producer
kubectl delete deploy eqbd-security-labels-producer
kubectl delete deploy eqbd-schema-assurance-consumer
kubectl delete deploy eqbd-extractor-consumer

# ConfigMaps
kubectl delete configmap dl-adaptor-config
kubectl delete configmap eqbd-adaptor-config
kubectl delete configmap eqbd-schema-assurance-producer-config
kubectl delete configmap eqbd-security-labels-producer-config
kubectl delete configmap eqbd-schema-assurance-consumer-config
kubectl delete configmap eqbd-extractor-consumer-config

# Secrets
kubectl delete secret dl-adaptor-secrets
kubectl delete secret eqbd-adaptor-secrets
kubectl delete secret eqbd-extractor-consumer-secrets
kubectl delete secret eqbd-schema-assurance-consumer-secrets
kubectl delete secret eqbd-schema-assurance-producer-secrets
kubectl delete secret eqbd-security-labels-producer-secrets

# Kafka consumer group cleanup
kubectl exec -it kafka-src-<pod> -n ns-dpn-xx -- kafka-consumer-groups --bootstrap-server dpn-kafka-src:9092 --group $Default --delete
```

---

## 📜 License
Internal DSI proprietary use only. Redistribution prohibited.
