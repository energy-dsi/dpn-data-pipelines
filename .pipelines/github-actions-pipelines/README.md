# GitHub Actions deployment reference

Reference copy of this component's GitHub Actions deployment tooling, sourced from `dpn-containerised-deployment-service` (branch `feature/merge-azure-aws`) on 2026-09-22. Replaces a previous, incorrectly-shaped copy of this folder.

**This is a reference copy, not a runnable pipeline from this location, and does not affect the existing Azure DevOps pipelines under `.pipelines/azure-pipelines/`.** The actual GitHub Actions CD pipeline still runs centrally from `dpn-containerised-deployment-service`, which checks out this repo's charts directly — GitHub Actions only auto-discovers workflows under a repo's own `.github/workflows/`, so nothing here is triggerable from this repo as-is. This folder exists so this repo's own history reflects its GitHub Actions deployment configuration.

- `actions/cloud-login/` — the shared composite action every workflow here uses to authenticate to whichever cloud (azure/aws/gcp) is selected at dispatch time
- `config/{aws,azure}/*.json` — per-environment config, one set per cloud (shared across all DPN components deployed to that environment; each workflow reads only the keys it needs)
- `workflows/dpn-gha-data-pipelines-cd.yaml` — the generic per-service producer/consumer deploy workflow, dynamically discovers and installs whichever chart matches its `configType`/`processType`/`productType` inputs
- `workflows/dpn-gha-airflow-cd.yaml` — installs the Airflow orchestrator chart specifically
- **Not included, deliberately:** `dpn-cd-data-pipelines-aws-poc.yml` — an older, AWS-only proof-of-concept duplicate with its own bespoke logic; not the standard deployment path.
- Both workflows handle all three clouds via their own `cloud` input (not three separate files).

The Helm values for the Airflow chart on each cloud now live where they belong — alongside the chart itself, under `charts/airflow/values/<cloud>/<environment>-<cluster>.yaml` (e.g. `values/aws/dev-dpn01.yaml`) — not in this folder. This mirrors the `values/{aws,azure,gcp}/<environment>-<cluster>.yaml` folder structure used in the source repo `dpn-containerised-deployment-service` (GCP excluded here). It sits alongside, and does not touch, the existing flat `values-<environment>-<cluster>.yaml` files used by the Azure DevOps pipelines. The producer/consumer adaptor/mapper charts elsewhere in this repo are cloud-agnostic (one shared `values.yaml`, selected via `cloudProviderType` at deploy time) and have nothing per-cloud to place anywhere.

No uninstall or rollback workflow exists yet for either workflow.
