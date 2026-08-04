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
