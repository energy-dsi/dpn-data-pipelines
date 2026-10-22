# Changelog

**Repository:** `dpn-data-pipelines`  
**Description:** `Tracks all notable changes, version history, and roadmap toward 1.0.0 following Semantic Versioning.`

<!-- SPDX-License-Identifier: OGL-UK-3.0 -->

--- 

All notable changes to this repository will be documented in this file.

This project follows **Semantic Versioning (SemVer)** ([semver.org](https://semver.org/)), using the format:

`[MAJOR].[MINOR].[PATCH]`
- **MAJOR** (`X.0.0`) – Incompatible API/feature changes that break backward compatibility.
- **MINOR** (`0.X.0`) – Backward-compatible new features, enhancements, or functionality changes.
- **PATCH** (`0.0.X`) – Backward-compatible bug fixes, security updates, or minor corrections.
- **Pre-release versions** – Use suffixes such as `-alpha`, `-beta`, `-rc.1` (e.g., `2.1.0-beta.1`).
- **Build metadata** – If needed, use `+build` (e.g., `2.1.0+20260314`).

---

## How to Update This Changelog

1. When making changes, update this file under the **Unreleased** section.
2. Before a new release, move changes from **Unreleased** to a new dated section with a version number.
3. Follow **Semantic Versioning** rules to categorise changes correctly.
4. If pre-release versions are used, clearly mark them as `-alpha`, `-beta`, or `-rc.X`.

---

## Release 2.1.0 - October 2026

### Added

- Added a self-service portal (UI and backend service) for creating, running, monitoring and deleting data products, with Keycloak sign-in through oauth2-proxy, role-based access control, pagination, live validation and an error panel.
- Added "Run Now" and manual/automatic execution modes for consumer pipelines, plus a run-once option for topic pipelines.
- Added Keycloak OAuth based authentication for connecting to OTEL, with TLS trust for the collector, in the pipelines and the self-service portal.
- Added a 1-minute consumer heartbeat and lifecycle events for self-service actions.
- Added pre-checks that block creating a data product on an existing Kafka topic or storage container, and automatic creation of Kafka topics and storage containers.
- Added generic, reusable pipeline images and charts: one image per pathway serving every product, configured through environment variables.
- Added autoscaling (HPA) for self-service and consumer workloads, and an optional Kafka client keystore.
- Added self-service CI/CD for Azure, AWS with per-environment approval gates, Checkmarx, FOSSA, unit test and SonarQube stages, and rollback and uninstall pipelines.
- Added a TLS CA sync chart and EKS deployment documentation.
- Added a shared helper module to read the CA certificate from the truststore, used by pipeline services.
- Added generic, reusable pipeline images and charts: one image per pathway serving every schema type and product, configured through environment variables
- Added support for file names containing spaces and special characters without altering or normalizing original filenames during transfer.
- Added self-service CI/CD for Azure, AWS and GCP, with per-environment approval gates, Checkmarx, FOSSA, unit test and SonarQube stages, and rollback and uninstall pipelines.

### Fixed

- Simplified the data pipeline: Each pathway runs as a single container. Files now keep their original name in the target container.
- Moved the Kafka and OTEL truststore to the shared tls-auth-secret and fixed OTEL TLS and endpoint settings across charts.
- Fixed oauth2-proxy startup and certificate chain problems, and restricted self-service backend access to oauth2-proxy only.
- Fixed Checkmarx, JFrog Xray and FOSSA security findings, including dependency upgrades.

### Removed

- Removed the separate schema-mapper containers, topics and staging step from the producer and consumer, file and topic pipelines.
- Removed the per-product producer and consumer folders and the old per-product CI/CD pipelines and Airflow chart, DAGs and packages.
- Removed the org-prefixed file rename on the file pathway.
- Removed Airflow based orchestration.

---

## Release 2.0.0 - September 2026

### Added

- Made telemetry (OTEL) traffic secure by switching it to TLS across all pipeline services.
- Added a shared helper module to read the CA certificate from the truststore, used by both pipeline services and Airflow.
- Kafka client setup now prefers the truststore over the old CA file location, and fails clearly on a wrong password instead of silently falling back.

### Fixed

- Fixed Dockerfile security warnings by setting proper file permissions.
- Updated `kafka-python` to a safer version to fix known security issues.
- Switched Kafka certificate trust to the shared, password-protected truststore instead of a loose certificate file.
- Fixed wrong secret key name and volume mount for the truststore.
- Updated Airflow dependencies, DAG utils, and docs to match.

### Removed

- Removed the old CA certificate setting from  chart files, now unused.

---

## Release 1.0.0 - 2026-07-31

- Established initial project implementation, repository baseline, and README documentation.
- Added producer and consumer pipeline implementations, reintroduced adapter components, and added missing Azure DevOps pipeline definitions.
- Implemented streaming pattern integration and Airflow integration for stream-based and file-based data pipeline processing.
- Added Azure Pipelines CI workflow, schema assurance pipeline, and security label handling with extractor mapper components.
- Added GitHub Actions CI/CD pipelines and a reusable GitHub Actions and GCP CI/CD pipeline template for DPN Data Pipelines.
- Implemented AWS integration using MinIO for object storage and data management.
- Added JFrog and Cosign tooling integration into the build and deployment process.
- Added SonarQube reporting stage and OpenTelemetry integration to improve code quality, monitoring, and observability.
- Automatic Kafka source/topic creation during deployment.
- Added Airflow PostgreSQL persistent volume claim (PVC) configuration and administrator password integration through Azure Key Vault (AKV).
- Added blueprint files, environment configuration JSON files, file extension handling, and supporting deployment resources.
- Added consumer smoke test scenarios and unit tests for Airflow file and stream pipelines to improve validation coverage.
 
---

## Maintained by the National Energy System Operator (NESO)

Copyright 2026 NESO.  This work is licensed under the Open Government Licence 3.0 (OGL). This work has been developed by NESO using content licensed by the Department for Business and Trade (UK) under the OGL.   
 
Licensed under the Open Government Licence v3.0.

For full licensing terms, [OGL_LICENSE.md](./OGL_LICENSE.md)
