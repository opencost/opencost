---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for install info
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::install-info
- text: The Product returns the Installation's namespace, version, running containers and size
  kind: product
  actor: user
  entities:
  - entity: installation
    effect: reads
    facts:
    - Install namespace
    - Version
    - Running containers
    - Cluster size
  contexts:
    api:
      place: cost-api::cost-reporting::install-info
---

# View the installation

## Trigger

The User is troubleshooting the installation.

## Outcome

The User knows where and what is running.

## Edge cases

- The install namespace alone can be asked for on its own.
