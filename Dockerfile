FROM --platform=$BUILDPLATFORM golang:1.25.3-alpine3.21 AS build-env

WORKDIR /app

# This ensures that CGO is disabled for go test running AND for the build
# step. This prevents a build failure when building an ARM64 image with
# docker buildx. I believe this is because the ARM64 version of the
# golang:latest image does not contain GCC, while the AMD64 version does.
ARG CGO_ENABLED=0

# Get dependencies - will also be cached if we won't change mod/sum
COPY go.* .
COPY core/go.* core/
COPY modules/collector-source/go.* modules/collector-source/
COPY modules/prometheus-source/go.* modules/prometheus-source/
RUN go mod download

ARG version=dev
ARG	commit=HEAD

ARG TARGETOS
ARG TARGETARCH
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

# COPY the source code as the last step
COPY . .

# Build the binary
RUN set -e ;\
    cd cmd/costmodel;\
    go build -a -installsuffix cgo \
    -ldflags \
    "-X github.com/opencost/opencost/core/pkg/version.Version=${version} \
    -X github.com/opencost/opencost/core/pkg/version.GitCommit=${commit}" \
    -o /go/bin/app

FROM alpine:latest

LABEL org.opencontainers.image.description="Cross-cloud cost allocation models for Kubernetes workloads"
LABEL org.opencontainers.image.documentation=https://opencost.io/docs/
LABEL org.opencontainers.image.licenses=Apache-2.0
LABEL org.opencontainers.image.source=https://github.com/opencost/opencost
LABEL org.opencontainers.image.title=kubecost-cost-model
LABEL org.opencontainers.image.url=https://opencost.io

RUN apk add --update --no-cache ca-certificates

COPY --from=build-env /go/bin/app /go/bin/app
ADD --chmod=400 ./THIRD_PARTY_LICENSES.txt /THIRD_PARTY_LICENSES.txt
ADD --chmod=500 ./configs/default.json /models/default.json
ADD --chmod=500 ./configs/azure.json /models/azure.json
ADD --chmod=500 ./configs/aws.json /models/aws.json
ADD --chmod=500 ./configs/gcp.json /models/gcp.json
ADD --chmod=500 ./configs/alibaba.json /models/alibaba.json
ADD --chmod=500 ./configs/oracle.json /models/oracle.json
ADD --chmod=500 ./configs/otc.json /models/otc.json
RUN chown -R 1001:1001 /models

USER 1001
ENTRYPOINT ["/go/bin/app"]
