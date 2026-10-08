---
kind: person
acts: external
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#adminAuthMiddleware
---

# Administrator

Someone who runs the OpenCost installation and calls the cost API's
administration operations with the admin token configured for it. OpenCost
keeps no account for them: the token is the only thing it checks.
