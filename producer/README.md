# Producer Data Products

This `producer/` folder is intentionally **empty** in the released repository.

Producer data products are **not** shipped here - each organisation creates its
own from the schema-type **blueprints** under `blueprints/producer/`. The
`values.yaml` files inside `blueprints/` are templates and **must not** be
modified directly.

## Setup a new data product from a blueprint

1. Define the **`product_type`** name - the identifier for the data product being
   published. Each product type must conform to one of the schema types available
   in the blueprints (`dl`, `eq`, `eqbd`, `ssh`).
2. Define the **`process_type`** - `file` or `topic` - for the data product.
3. If **process_type is file**, copy the relevant schema folder (e.g. `eq`,
   `eqbd`, `dl`, or `ssh`) from
   `blueprints/producer/file/{schema_type}` to `producer/file/{schema_type}`.
4. If **process_type is topic**, copy the relevant schema folder from
   `blueprints/producer/topic/{schema_type}` to `producer/topic/{schema_type}`.
5. Rename the copied `{schema_type}` folder to `{product_type}` (e.g. rename `eq`
   to `eq-dp-01`). **Only hyphens** are permitted as special characters; all
   other special characters are disallowed.
6. Ensure the `product_type` value is passed consistently during the CI pipeline
   run.

Resulting layout (example):

```text
Root-Repository
  └── producer
        └── file
              └── {product_type}          # e.g. eq-sample-1
                    ├── adaptor
                    │     └── charts
                    │           └── values.yaml
                    └── schema_mapper
                          └── charts
                                └── values.yaml
        └── topic
              └── {product_type}          # e.g. eq-sample-1
                    ├── adaptor
                    │     └── charts
                    │           └── values.yaml
                    └── schema_mapper
                          └── charts
                                └── values.yaml
```

> The `values.yaml` file can be replicated per environment / DPN deployment
> (e.g. `values-<env>-dpn01.yaml`). Specify the values file name in the pipeline
> configuration. See the DPN data pipeline configuration guide for the full
> per-parameter details [DPN Integration Playbook](https://github.com/energy-dsi/dpn-integration-playbook)
