# GitHub Actions CD

These workflows are self-contained and runnable directly from this repo — `workflow_dispatch` from the Actions tab, no dependency on any other repo.

- `dpn-gha-data-pipelines-cd.yaml` — the producer/consumer deploy workflow for the blueprint architecture (`blueprints/<configType>/<processType>/<adaptor|extractor>/charts`), one `helm upgrade` per run.
- `dpn-gha-data-pipelines-rollback.yaml` — `helm rollback` by free-text release name (releases are created dynamically per product, so there's no fixed list) — matches the real ADO `dpn-data-pipeline-rollback.yaml` pipeline's `release_name` parameter exactly.
- `dpn-gha-data-pipelines-uninstall.yaml` — tick any combination of `uninstallProducer`/`uninstallConsumer`/`uninstallAirflow` (at least one required); producer/consumer releases are discovered via `helm list` name-prefix filtering. Matches the real ADO `dsi-data-pipelines-uninstall-cd.yaml` pipeline's scope exactly, including the legacy `dpn-airflow` option (the chart is gone, but old deployed releases from before the architecture change may still need uninstalling).
- `dpn-gha-self-service-cd.yaml` — deploys the Self-Service Data Pipelines UI (`service/charts`) plus the two consumer singletons (`dpn-consumer-file-extractor`, `dpn-consumer-topic-extractor`) into `ns-dpn-dp-01`.
- `dpn-gha-self-service-rollback.yaml` / `dpn-gha-self-service-uninstall.yaml` — rollback/uninstall for the same three releases. Validated against the real ADO `dpn-self-service-rollback.yaml` / `dpn-self-service-uninstall-cd.yaml` pipelines — release names and the `ns-dpn-dp-01` namespace match; no changes needed.

**`dpn-gha-airflow-cd.yaml` removed**: `charts/airflow` was deleted from this repo entirely by `develop`'s "Simplify pipeline architecture and add self-service RBAC/multi-cloud support" — Airflow orchestration is retired in favor of the blueprint/self-service architecture above. Keeping the workflow would have deployed a chart that no longer exists.

**`service/` and `ui/` (the Flask self-service UI app + its Helm chart) already exist in this repo** as of merging in `develop` — an earlier commit here had copied an initial snapshot in from `dpn-containerised-deployment-service` since only the deployment *workflow* had been reverse-fed before that, not the application. `develop`'s own, actively-maintained version (which includes the platform-wide `tls-auth-secret` migration and other fixes) now supersedes that snapshot.

**Known limitation, not fixed here**: `service/charts/values/` only has an `aws/` subdirectory — no Azure values exist for the self-service chart, so `dpn-gha-self-service-cd.yaml` is effectively AWS-only today despite accepting an `azure` cloud input.

Still deliberately excluded: `dpn-gha-data-pipelines-aws-poc.yml` (an older AWS-only proof-of-concept duplicate).

## One-time setup required (not a workflow change)

- **GitHub Environments** named `<environment>-<cluster>` for each target (e.g. `dev-dpn01`, `test-dpn02`).
- **Repository or Environment secrets**: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` (Azure OIDC federated identity).
- **AWS path**: relies on OIDC role-chaining into the shared hub role, then your account's `oidc_github_runner_admin` role — no AWS secret needed, but that role/trust must exist.
- **Self-hosted runner**: targets `runs-on: [self-hosted, vmss]` — register a runner with that label, or change `runs-on` to a pool you control.
- **Per-environment config**: `.github/config/{aws,azure}/<environment>-<cluster>.json` already has real target values — update if pointing at different infrastructure.

Nothing in the workflow logic itself needs editing to run these.
