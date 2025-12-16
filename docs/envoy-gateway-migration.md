# Envoy Gateway Migration Analysis for OpenCost

## Executive Summary

This document analyzes the process for replacing OpenCost's current HTTP serving architecture with Envoy Gateway, providing enhanced traffic management, security, and observability capabilities.

---

## 1. Current Architecture Analysis

### 1.1 OpenCost HTTP Stack

OpenCost currently uses a **direct HTTP serving model** with no proxy or gateway layer:

| Component | Technology | Port | Purpose |
|-----------|------------|------|---------|
| **Main API Server** | Go `http.ListenAndServe()` | 9003 | Cost allocation, asset, and cloud cost APIs |
| **Agent Metrics Server** | Go `http.ListenAndServe()` | 9005 | Kubernetes metrics emission (agent mode) |
| **MCP Server** | Go `http.ListenAndServe()` | 8081 | Model Context Protocol for AI agents |

**HTTP Router**: `github.com/julienschmidt/httprouter`
**Middleware Chain** (in order):
1. Panic Handler - Error recovery
2. CORS - `github.com/rs/cors` with Allow-All policy
3. Telemetry/Metrics - Request/response tracking
4. Standard Go HTTP Mux

**Key File Locations**:
- `pkg/cmd/costmodel/costmodel.go:94-100` - Main server setup
- `pkg/costmodel/router.go` - HTTP routing and endpoint definitions
- `pkg/cmd/agent/agent.go:155-161` - Agent mode server

### 1.2 Current API Endpoints

#### Main API (Port 9003)

| Category | Endpoints |
|----------|-----------|
| **Cost Allocation** | `/allocation`, `/allocation/summary`, `/allocation/compute`, `/allocation/compute/summary` |
| **Assets** | `/assets`, `/assets/carbon` |
| **Cloud Cost** | `/cloudCost`, `/cloudCost/view/graph`, `/cloudCost/view/totals`, `/cloudCost/view/table`, `/cloudCost/status`, `/cloudCost/rebuild`, `/cloudCost/repair` |
| **Custom Cost** | `/customCost/total`, `/customCost/timeseries`, `/customCost/status` |
| **Cloud Config** | `/cloud/config/export`, `/cloud/config/enable`, `/cloud/config/disable`, `/cloud/config/delete` |
| **Pricing** | `/costDataModel`, `/allNodePricing`, `/refreshPricing`, `/pricingSourceStatus`, `/pricingSourceSummary`, `/pricingSourceCounts` |
| **Cluster Info** | `/clusterInfo`, `/clusterInfoMap`, `/managementPlatform` |
| **System** | `/healthz`, `/metrics`, `/installNamespace`, `/installInfo`, `/helmValues`, `/orphanedPods`, `/serviceAccountStatus` |
| **Admin** | `/serviceKey`, `/logs/level` |

#### MCP Server (Port 8081)

| Tool | Description |
|------|-------------|
| `get_allocation_costs` | Cost allocation queries |
| `get_asset_costs` | Asset cost data |
| `get_cloud_costs` | Cloud provider costs |
| `get_efficiency` | Resource efficiency metrics |

### 1.3 Current Limitations

| Limitation | Impact |
|------------|--------|
| **No TLS/HTTPS** | Traffic is unencrypted |
| **No Authentication** | All endpoints are public |
| **No Rate Limiting** | Vulnerable to DoS |
| **No Load Balancing** | Single instance only |
| **No Traffic Management** | No canary, A/B, or circuit breaking |
| **Limited Observability** | Basic Prometheus metrics only |
| **Allow-All CORS** | Security risk in production |

---

## 2. Envoy Gateway Overview

### 2.1 What is Envoy Gateway?

Envoy Gateway is an open-source project that provides a **Kubernetes-native API Gateway** using:
- **Envoy Proxy** as the data plane
- **Kubernetes Gateway API** as the configuration interface
- **Control plane** that translates Gateway API resources to Envoy xDS

### 2.2 Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │           Envoy Gateway Controller           │
                    │  (Watches Gateway API resources, generates   │
                    │         Envoy configuration via xDS)         │
                    └──────────────────┬───────────────────────────┘
                                       │
                                       │ xDS Configuration
                                       ▼
┌──────────┐       ┌──────────────────────────────────────────────┐
│ External │       │              Envoy Proxy Fleet               │
│ Traffic  │──────▶│    (Data plane: routing, TLS, auth, etc.)    │
└──────────┘       └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────┼───────────────────┐
                    │                  │                   │
                    ▼                  ▼                   ▼
            ┌──────────────┐  ┌──────────────┐   ┌──────────────┐
            │ OpenCost API │  │ OpenCost MCP │   │ OpenCost     │
            │   :9003      │  │    :8081     │   │ Metrics:9005 │
            └──────────────┘  └──────────────┘   └──────────────┘
```

### 2.3 Key Capabilities

| Feature | Description |
|---------|-------------|
| **TLS Termination** | HTTPS with automatic certificate management |
| **Authentication** | JWT, OAuth2, external auth service (ext_authz) |
| **Rate Limiting** | Global and per-client rate limits |
| **Load Balancing** | Round-robin, least-connections, ring hash |
| **Traffic Splitting** | Canary deployments, A/B testing |
| **Circuit Breaking** | Fault tolerance and resilience |
| **Observability** | Access logs, metrics, distributed tracing |
| **Request Transformation** | Header manipulation, path rewriting |

---

## 3. Migration Process

### Phase 1: Prerequisites and Planning

#### 3.1.1 Kubernetes Requirements
- Kubernetes 1.25+ (for Gateway API v1 support)
- Cluster admin access for CRD installation
- LoadBalancer or NodePort service support

#### 3.1.2 Install Gateway API CRDs

```bash
# Install the standard Gateway API CRDs
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml

# Verify installation
kubectl get crd | grep gateway
```

Expected CRDs:
- `gatewayclasses.gateway.networking.k8s.io`
- `gateways.gateway.networking.k8s.io`
- `httproutes.gateway.networking.k8s.io`
- `referencegrants.gateway.networking.k8s.io`

#### 3.1.3 Install Envoy Gateway

```bash
# Add Envoy Gateway Helm repository
helm install eg oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.0 \
  -n envoy-gateway-system \
  --create-namespace

# Verify installation
kubectl get pods -n envoy-gateway-system
kubectl get gatewayclass
```

### Phase 2: Gateway Resource Configuration

#### 3.2.1 GatewayClass Definition

```yaml
# gatewayclass.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: opencost-gateway-class
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
  description: "Envoy Gateway for OpenCost services"
```

#### 3.2.2 Gateway Definition

```yaml
# gateway.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: opencost-gateway
  namespace: opencost
spec:
  gatewayClassName: opencost-gateway-class
  listeners:
    # Main API listener (HTTP for development)
    - name: http-api
      protocol: HTTP
      port: 80
      allowedRoutes:
        namespaces:
          from: Same

    # Main API listener (HTTPS for production)
    - name: https-api
      protocol: HTTPS
      port: 443
      tls:
        mode: Terminate
        certificateRefs:
          - name: opencost-tls-cert
            kind: Secret
      allowedRoutes:
        namespaces:
          from: Same

    # MCP Server listener
    - name: mcp
      protocol: HTTP
      port: 8081
      allowedRoutes:
        namespaces:
          from: Same
```

#### 3.2.3 HTTPRoute for Main API

```yaml
# httproute-api.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: opencost-api-route
  namespace: opencost
spec:
  parentRefs:
    - name: opencost-gateway
      sectionName: http-api
  rules:
    # Health check endpoint (no auth required)
    - matches:
        - path:
            type: Exact
            value: /healthz
      backendRefs:
        - name: opencost
          port: 9003

    # Metrics endpoint (no auth required for Prometheus scraping)
    - matches:
        - path:
            type: Exact
            value: /metrics
      backendRefs:
        - name: opencost
          port: 9003

    # Cost allocation APIs
    - matches:
        - path:
            type: PathPrefix
            value: /allocation
      backendRefs:
        - name: opencost
          port: 9003

    # Asset APIs
    - matches:
        - path:
            type: PathPrefix
            value: /assets
      backendRefs:
        - name: opencost
          port: 9003

    # Cloud cost APIs
    - matches:
        - path:
            type: PathPrefix
            value: /cloudCost
      backendRefs:
        - name: opencost
          port: 9003

    # Custom cost APIs
    - matches:
        - path:
            type: PathPrefix
            value: /customCost
      backendRefs:
        - name: opencost
          port: 9003

    # Cloud config APIs
    - matches:
        - path:
            type: PathPrefix
            value: /cloud/config
      backendRefs:
        - name: opencost
          port: 9003

    # Cluster info APIs
    - matches:
        - path:
            type: PathPrefix
            value: /clusterInfo
      backendRefs:
        - name: opencost
          port: 9003

    # Pricing APIs
    - matches:
        - path:
            type: PathPrefix
            value: /pricing
        - path:
            type: Exact
            value: /costDataModel
        - path:
            type: Exact
            value: /allNodePricing
        - path:
            type: Exact
            value: /refreshPricing
        - path:
            type: PathPrefix
            value: /pricingSource
      backendRefs:
        - name: opencost
          port: 9003

    # Catch-all for remaining endpoints
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: opencost
          port: 9003
```

#### 3.2.4 HTTPRoute for MCP Server

```yaml
# httproute-mcp.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: opencost-mcp-route
  namespace: opencost
spec:
  parentRefs:
    - name: opencost-gateway
      sectionName: mcp
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: opencost-mcp
          port: 8081
```

### Phase 3: Security Policies

#### 3.3.1 Rate Limiting Policy

```yaml
# ratelimit-policy.yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: opencost-ratelimit
  namespace: opencost
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: opencost-api-route
  rateLimit:
    type: Global
    global:
      rules:
        - limit:
            requests: 100
            unit: Second
```

#### 3.3.2 CORS Policy

```yaml
# cors-policy.yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: SecurityPolicy
metadata:
  name: opencost-cors
  namespace: opencost
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: opencost-api-route
  cors:
    allowOrigins:
      - type: Exact
        value: "https://your-dashboard.example.com"
    allowMethods:
      - GET
      - POST
      - OPTIONS
    allowHeaders:
      - Content-Type
      - Authorization
    exposeHeaders:
      - X-Request-Id
    maxAge: 86400s
```

#### 3.3.3 JWT Authentication (Optional)

```yaml
# jwt-auth-policy.yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: SecurityPolicy
metadata:
  name: opencost-jwt-auth
  namespace: opencost
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: opencost-api-route
  jwt:
    providers:
      - name: auth-provider
        issuer: "https://your-auth-provider.com/"
        audiences:
          - "opencost-api"
        remoteJWKS:
          uri: "https://your-auth-provider.com/.well-known/jwks.json"
```

### Phase 4: OpenCost Service Configuration

#### 3.4.1 Updated Service Definition

The OpenCost Kubernetes Service needs to be updated to work with the Gateway:

```yaml
# opencost-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: opencost
  namespace: opencost
  labels:
    app: opencost
spec:
  type: ClusterIP  # Changed from LoadBalancer - Gateway handles external access
  ports:
    - name: http-api
      port: 9003
      targetPort: 9003
      protocol: TCP
  selector:
    app: opencost
---
apiVersion: v1
kind: Service
metadata:
  name: opencost-mcp
  namespace: opencost
  labels:
    app: opencost
spec:
  type: ClusterIP
  ports:
    - name: http-mcp
      port: 8081
      targetPort: 8081
      protocol: TCP
  selector:
    app: opencost
```

#### 3.4.2 Helm Values Update

Update the OpenCost Helm chart values:

```yaml
# helm-values-envoy-gateway.yaml
opencost:
  exporter:
    # Disable external service - Gateway handles this
    service:
      type: ClusterIP
      # Remove any LoadBalancer annotations

  # Existing environment variables remain unchanged
  extraEnv:
    API_PORT: "9003"
    MCP_HTTP_PORT: "8081"

# The MCP service should also be ClusterIP
mcp:
  enabled: true
  port: 8081
  service:
    type: ClusterIP
```

### Phase 5: Observability Configuration

#### 3.5.1 Access Logging

```yaml
# envoy-access-logging.yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyProxy
metadata:
  name: opencost-proxy-config
  namespace: envoy-gateway-system
spec:
  logging:
    level:
      default: info
  telemetry:
    accessLog:
      settings:
        - format:
            type: JSON
            json:
              timestamp: "%START_TIME%"
              method: "%REQ(:METHOD)%"
              path: "%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%"
              protocol: "%PROTOCOL%"
              responseCode: "%RESPONSE_CODE%"
              responseFlags: "%RESPONSE_FLAGS%"
              bytesReceived: "%BYTES_RECEIVED%"
              bytesSent: "%BYTES_SENT%"
              duration: "%DURATION%"
              upstreamHost: "%UPSTREAM_HOST%"
              xForwardedFor: "%REQ(X-FORWARDED-FOR)%"
              userAgent: "%REQ(USER-AGENT)%"
```

#### 3.5.2 Prometheus Metrics Integration

The Envoy Gateway automatically exposes metrics. Update your Prometheus scrape config:

```yaml
# prometheus-scrape-config.yaml
scrape_configs:
  # Existing OpenCost scrape
  - job_name: opencost
    metrics_path: /metrics
    static_configs:
      - targets: ['opencost.opencost:9003']

  # Add Envoy Gateway metrics
  - job_name: envoy-gateway
    metrics_path: /stats/prometheus
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - opencost
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_gateway_envoyproxy_io_owning_gateway_name]
        action: keep
        regex: opencost-gateway
```

---

## 4. Migration Steps (Detailed)

### Step 1: Pre-Migration Checklist

- [ ] Backup existing OpenCost configuration
- [ ] Document current endpoint usage patterns
- [ ] Identify any custom integrations using OpenCost APIs
- [ ] Verify Kubernetes version compatibility (1.25+)
- [ ] Plan maintenance window for migration

### Step 2: Install Envoy Gateway (Non-Disruptive)

```bash
# 1. Install Gateway API CRDs
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml

# 2. Install Envoy Gateway controller
helm install eg oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.0 \
  -n envoy-gateway-system \
  --create-namespace

# 3. Verify installation
kubectl wait --timeout=5m -n envoy-gateway-system deployment/envoy-gateway --for=condition=Available
```

### Step 3: Deploy Gateway Resources (Parallel to Existing)

```bash
# Create Gateway resources (does not affect existing traffic)
kubectl apply -f gatewayclass.yaml
kubectl apply -f gateway.yaml

# Wait for Gateway to be ready
kubectl wait --timeout=5m -n opencost gateway/opencost-gateway --for=condition=Programmed
```

### Step 4: Deploy HTTPRoutes

```bash
# Deploy routes (still parallel - new ingress path)
kubectl apply -f httproute-api.yaml
kubectl apply -f httproute-mcp.yaml

# Verify routes are attached
kubectl get httproute -n opencost
```

### Step 5: Testing (Before Cutover)

```bash
# Get the Gateway's external address
export GATEWAY_IP=$(kubectl get gateway opencost-gateway -n opencost -o jsonpath='{.status.addresses[0].value}')

# Test health endpoint through Gateway
curl -v http://$GATEWAY_IP/healthz

# Test allocation endpoint
curl -v "http://$GATEWAY_IP/allocation?window=1h"

# Test MCP endpoint (on port 8081)
curl -v http://$GATEWAY_IP:8081/
```

### Step 6: Apply Security Policies

```bash
# Apply rate limiting
kubectl apply -f ratelimit-policy.yaml

# Apply CORS policy (customize allowed origins first!)
kubectl apply -f cors-policy.yaml

# (Optional) Apply JWT authentication
# kubectl apply -f jwt-auth-policy.yaml
```

### Step 7: Cutover

```bash
# Update DNS/Load Balancer to point to Gateway
# OR update OpenCost service type

# Option A: Update existing service to use Gateway
kubectl patch service opencost -n opencost -p '{"spec":{"type":"ClusterIP"}}'

# Option B: Update external DNS to Gateway IP
# (Manual step - update your DNS records)
```

### Step 8: Post-Migration Verification

```bash
# Check Gateway status
kubectl get gateway opencost-gateway -n opencost -o yaml

# Check HTTPRoute status
kubectl get httproute -n opencost -o yaml

# Verify Envoy proxy pods are healthy
kubectl get pods -n opencost -l gateway.envoyproxy.io/owning-gateway-name=opencost-gateway

# Check Envoy Gateway logs
kubectl logs -n envoy-gateway-system -l control-plane=envoy-gateway

# Monitor for errors in OpenCost logs
kubectl logs -n opencost -l app=opencost --tail=100
```

---

## 5. Rollback Plan

### Immediate Rollback

```bash
# Delete Gateway resources (traffic returns to original path)
kubectl delete httproute opencost-api-route opencost-mcp-route -n opencost
kubectl delete gateway opencost-gateway -n opencost

# If service was changed, revert
kubectl patch service opencost -n opencost -p '{"spec":{"type":"LoadBalancer"}}'

# Update DNS back to original endpoint
```

### Full Cleanup

```bash
# Remove all Envoy Gateway components
helm uninstall eg -n envoy-gateway-system
kubectl delete namespace envoy-gateway-system

# Remove Gateway API CRDs (careful - affects all Gateway API users)
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml

kubectl delete gatewayclass opencost-gateway-class
```

---

## 6. Code Changes Required in OpenCost

### 6.1 No Immediate Code Changes Required

The migration to Envoy Gateway is primarily an **infrastructure change**. OpenCost's application code can remain unchanged because:

1. **HTTP serving is preserved** - OpenCost continues to use `http.ListenAndServe()`
2. **Endpoints are unchanged** - All routes remain the same
3. **Envoy acts as a transparent proxy** - Backend services are unaware of the proxy

### 6.2 Recommended Code Enhancements (Future)

| Enhancement | Description | Priority |
|-------------|-------------|----------|
| **Remove Allow-All CORS** | Gateway handles CORS - remove `cors.AllowAll()` | Medium |
| **Add X-Forwarded headers** | Parse `X-Forwarded-For`, `X-Forwarded-Proto` for accurate logging | Low |
| **Health endpoint enhancement** | Add readiness vs liveness distinction | Low |
| **Remove duplicate TLS handling** | If TLS is added to app, remove it (Gateway terminates) | N/A |

### 6.3 Potential Code Changes (pkg/cmd/costmodel/costmodel.go)

```go
// BEFORE: Allow-all CORS
handler := cors.AllowAll().Handler(telemetryHandler)

// AFTER: Minimal CORS (Gateway handles it) or no CORS
// Option 1: Remove CORS entirely (recommended if Gateway handles it)
handler := telemetryHandler

// Option 2: More restrictive CORS as fallback
c := cors.New(cors.Options{
    AllowedOrigins:   []string{"https://your-dashboard.example.com"},
    AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
    AllowCredentials: true,
})
handler := c.Handler(telemetryHandler)
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

No changes required - existing unit tests continue to work.

### 7.2 Integration Tests

| Test Case | Description | Command |
|-----------|-------------|---------|
| Health Check | Verify `/healthz` returns 200 | `curl http://GATEWAY_IP/healthz` |
| Allocation API | Test cost allocation query | `curl "http://GATEWAY_IP/allocation?window=1h"` |
| Assets API | Test asset query | `curl "http://GATEWAY_IP/assets?window=1h"` |
| Cloud Cost | Test cloud cost query | `curl "http://GATEWAY_IP/cloudCost?window=7d"` |
| Metrics | Verify Prometheus metrics | `curl http://GATEWAY_IP/metrics` |
| MCP Server | Test MCP endpoint | `curl -X POST http://GATEWAY_IP:8081/` |

### 7.3 Load Testing

```bash
# Install k6 or use hey
# Test rate limiting works
hey -n 1000 -c 100 "http://GATEWAY_IP/healthz"

# Verify requests are properly load-balanced
hey -n 10000 -c 50 "http://GATEWAY_IP/allocation?window=1h"
```

### 7.4 Security Testing

```bash
# Test CORS is enforced
curl -H "Origin: https://malicious-site.com" \
  -H "Access-Control-Request-Method: GET" \
  -X OPTIONS http://GATEWAY_IP/allocation

# Test rate limiting kicks in
for i in {1..200}; do curl -s -o /dev/null -w "%{http_code}\n" http://GATEWAY_IP/healthz; done | sort | uniq -c

# Test JWT authentication (if enabled)
curl -H "Authorization: Bearer invalid-token" http://GATEWAY_IP/allocation
```

---

## 8. Benefits Summary

| Benefit | Before | After |
|---------|--------|-------|
| **TLS/HTTPS** | Not supported | Full TLS termination |
| **Authentication** | None | JWT, OAuth2, ext_authz |
| **Rate Limiting** | None | Configurable per-route |
| **Load Balancing** | Single instance | Multiple algorithms |
| **Traffic Management** | None | Canary, A/B testing |
| **Observability** | Basic metrics | Full access logs, tracing |
| **CORS** | Allow-all (insecure) | Configurable per-origin |
| **API Gateway Features** | None | Full Gateway API support |

---

## 9. References

- [Envoy Gateway Official Documentation](https://gateway.envoyproxy.io/)
- [Envoy Gateway GitHub Repository](https://github.com/envoyproxy/gateway)
- [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/)
- [Gateway API v1.4 Release Notes](https://kubernetes.io/blog/2025/11/06/gateway-api-v1-4/)
- [Envoy Gateway System Design](https://gateway.envoyproxy.io/contributions/design/system-design/)
- [OpenCost Helm Chart](https://github.com/opencost/opencost-helm-chart)
