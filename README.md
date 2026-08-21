# README

**Repository:** `dpn-data-pipelines`

**Description:** `Responsible for producing and consuming data products of an organisation. This repo contains a collection of data pipelines that perform different operations over the data.`

<!-- SPDX-License-Identifier: Apache-2.0 AND OGL-UK-3.0 -->

---

## Overview

This repository contributes to the development of **secure, scalable, and interoperable data-sharing infrastructure**. It supports DSI's mission to enable **trusted, federated, and decentralised** data-sharing across organisations.

This repository is one of several open-source components that underpin DSI's **Data Preparation Node (DPN)**—a framework designed to allow organisations to manage and exchange data securely while maintaining control over their own information. The DPN is actively deployed and tested across multiple sectors, ensuring its adaptability and alignment with real-world needs.

The DPN Data Pipeline ensures secure and governed data exchange by validating and transforming datasets before and after transmission. It applies schema assurance, security labelling, and controlled processing across producer and consumer stages. This ensures all shared data conforms to required schemas, security classifications, and governance standards, enabling reliable and compliant data sharing.

**DSI DPN Data Pipeline component is aimed to do the following:**

- Transfer the Data Product specific files/stream messages produced by an internal Data Source Stage location to a Data Store target location
- Handoff the produced files/stream messages to the Federator Secure Gateway server to read and pass them to other DPN consumers from different Organisations subscribed to it
- Ingest data product files/stream messages placed by the Federator Secure Gateway client in a data store stage location
- Prepare the Data Product files/stream messages received in a data store location and hand off to the data destination process in the Organisation

## Prerequisites

* Python 3.14
* [Docker](https://www.docker.com/)
* [Git](https://git-scm.com/)
* Kubernetes Cluster
* Blob Storage/S3 Bucket
* cp-kafka:7.5.3
* Apache Airflow:2.9.3 (Optional)

## Configuration & Installation

Detailed configuration and installation instructions for this repository are present in **[dpn-integration-playbook](https://github.com/energy-dsi/dpn-integration-playbook)**

This includes producer/consumer setup, CI/CD pipeline configuration and execution, and deployment validation. Refer to the guide matching your deployment target:

### AWS Deployment

Refer [aws-manual-beta](https://github.com/energy-dsi/dpn-integration-playbook/tree/main/Docs/03-dpn-application-deployment/aws-manual-beta) for AWS specific deployment 

**Note** AWS Manual deployment is an interim solution and GitHub Actions based deployment to replace the manual deployment in future release

### Azure Deployment

Refer [azure-ado-beta](https://github.com/energy-dsi/dpn-integration-playbook/tree/main/Docs/03-dpn-application-deployment/azure-ado-beta) for Azure specific deployment

## Features

The Data Pipeline enables secure, governed data exchange between producing and consuming organisations, supporting both file-based and Kafka topic-based data products. Key features include:

### Producer Pipeline
- **Adaptor**: Reads Data Product files/stream messages from a given object storage location or from a Kafka source topic per data product and keeps it to another object storage or kafka topic read by Schema Mapper process.
- **Schema Mapper**: For file-based products, stores the file to a data store target location and publishes metadata (filename and location) to a Kafka topic. For Kafka topic-based products, moves the validated data to a target topic with metadata carried as a header.
- Handoff of validated data products to the **Federator** Secure Gateway server, which reads and passes them to DPN consumers subscribed from other organisations.

### Consumer Pipeline
- **Extractor**: Moves data product files received by the Federator client from the source data store location to the destination data store container location for the Consumer Mapper, publishing metadata to a Kafka topic. For topic-based products, moves messages from the source Kafka topic to a destination Kafka topic with metadata carried as a header.
- **Schema Mapper**: For file-based products, stores the file to a data store target location and publishes metadata to a Kafka topic. For Kafka topic-based products, moves the validated data to a target topic with metadata as a header.The target location is used for the Organisation data destination to consume the files/messages

### Orchestration
- **Airflow** (optional): Used for orchestration of DPN Data Pipelines, providing a visual representation of pipeline status (Running, Failed, Success). The pipelines can also be configured to run stand-alone, without Airflow.

## Public Funding Acknowledgment

This repository has been developed with public funding as part of the Data Sharing Infrastructure (DSI), a UK Government initiative.

## License

This repository contains both source code and documentation, covered by different licenses:
- **Code:** Licensed under the terms in [LICENSE.md](./LICENSE.md).
- **Documentation:** Licensed under the Open Government Licence v3.0 — see [OGL_LICENSE.md](./OGL_LICENSE.md).

By contributing to this repository, you agree that your contributions will be licensed under these terms.

## Security and Responsible Disclosure

We take security seriously. If you believe you have found a security vulnerability in this repository, please follow our responsible disclosure process outlined in [SECURITY.md](./SECURITY.md).

## Contributing

We welcome contributions that align with the Programme's objectives.

## Acknowledgements

This repository has benefited from collaboration with various organisations.

## Support and Contact

For questions, feedback, or support requests:

- Contact DSI team via email to [dsi@neso.energy](mailto:dsi@neso.energy)

## Maintained by the National Energy System Operator (NESO)

Copyright 2026 NESO.  This work is licensed under the Open Government Licence 3.0 (OGL). This work has been developed by NESO using content licensed by the Department for Business and Trade (UK) under the OGL.   
 
Licensed under the Open Government Licence v3.0.

For full licensing terms, [OGL_LICENSE.md](./OGL_LICENSE.md)