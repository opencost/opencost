FROM golang:latest as build-env

RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .

# This ensures that CGO is disabled for go test running AND for the build
# step. This prevents a build failure when building an ARM64 image with
# docker buildx. I believe this is because the ARM64 version of the
# golang:latest image does not contain GCC, while the AMD64 version does.
ARG CGO_ENABLED=0

ARG version=dev
ARG	commit=HEAD

# COPY the source code as the last step
COPY . .

# Get dependencies - will also be cached if we won't change mod/sum
RUN go mod download

# Build the binary
RUN set -e ;\
    go test ./test/*.go;\
    go test ./pkg/*;\
    cd cmd/costmodel;\
    GOOS=linux \
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

RUN mkdir -p /models

ADD --chmod=644 ./THIRD_PARTY_LICENSES.txt /THIRD_PARTY_LICENSES.txt
ADD --chmod=644 ./configs/default.json /opt/configs/models/default.json
ADD --chmod=644 ./configs/azure.json /opt/configs/models/azure.json
ADD --chmod=644 ./configs/aws.json /opt/configs/models/aws.json
ADD --chmod=644 ./configs/gcp.json /opt/configs/models/gcp.json
ADD --chmod=644 ./configs/alibaba.json /opt/configs/models/alibaba.json
ADD --chmod=644 ./configs/oracle.json /opt/configs/models/oracle.json

RUN adduser 1001 -g 1000 -D
COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chown 1001:1000 -R /models
RUN chown 1001:1000 -R /usr/local/bin/docker-entrypoint.sh

USER 1001
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
