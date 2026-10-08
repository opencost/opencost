---
appliesTo:
- type: entity
  id: cloud-integration
  facts:
  - Configuration
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go
- kind: code
  role: implementation
  target: pkg/cloud/aws/authorizer.go
---

# Cloud integration secrets are never shown

Wherever a cloud integration's configuration is shown or exported, every secret
in it reads REDACTED.
