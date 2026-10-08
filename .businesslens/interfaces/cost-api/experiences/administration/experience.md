---
actors:
- administrator
access: restricted
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#adminAuthMiddleware
- kind: doc
  role: intent
  target: AGENTS.md
  title: Admin Auth
---

# Administration

The operations only the Administrator may call, each with the admin token: the
GCP service key, the Helm values, and managing cloud integrations and their
imported costs. While no admin token is configured, every one of them is
unavailable.
