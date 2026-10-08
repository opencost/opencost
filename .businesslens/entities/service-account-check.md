---
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetServiceAccountStatus
---

# Service account check

A check OpenCost runs on the cloud credentials it was given, such as whether
they can read spot or billing data.

## Information kept

- **Message** — what was checked
- **Status** — whether the check passed
- **Additional information** — details that help fix a failed check
