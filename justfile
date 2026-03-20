commonenv := "CGO_ENABLED=0"

version := `./tools/image-tag`
commit := `git rev-parse --short HEAD`

default:
    just --list

# format all Go code
fmt:
    go fmt ./...

# check if code is formatted
fmt-check:
    #!/bin/sh
    echo "Checking code formatting..."
    unformatted="$(gofmt -l .)"
    if [ -n "$unformatted" ]; then \
        echo "The following files are not formatted:"; \
        echo "$unformatted"; \
        echo ""; \
        echo "Run 'just fmt' to format your code"; \
        exit 1; \
    fi
    echo "All files are properly formatted"

# run core unit tests
test-core: 
    {{commonenv}} cd ./core && go test ./... -coverprofile=coverage.out
    {{commonenv}} cd ./core && go vet ./...

# run prometheus-source unit tests 
test-prometheus-source:
    {{commonenv}} cd ./modules/prometheus-source && go test ./... -coverprofile=coverage.out
    {{commonenv}} cd ./modules/prometheus-source && go vet ./...

# run collector-source unit tests
test-collector-source:
    {{commonenv}} cd ./modules/collector-source && go test ./... -coverprofile=coverage.out
    {{commonenv}} cd ./modules/collector-source && go vet ./...

# run the opencost unit tests 
test-opencost: 
    {{commonenv}} go test ./... -coverprofile=coverage.out
    {{commonenv}} go tool cover -html=coverage.out -o coverage.html
    {{commonenv}} go vet ./...

# Run unit tests, merge coverage reports, remove old reports 
test: test-core test-prometheus-source test-collector-source test-opencost
    find . -name "coverage.out" -print0 | xargs -0 cat > coverage.new
    find . -name "coverage.out" -delete
    mv coverage.new coverage.out

# Run unit tests and integration tests
test-integration:
    {{commonenv}} INTEGRATION=true go test ./... -coverprofile=coverage.out

# Compile a local binary
build-local:
    cd ./cmd/costmodel && \
        {{commonenv}} go build \
        -ldflags \
          "-X github.com/opencost/opencost/core/pkg/version.Version={{version}} \
           -X github.com/opencost/opencost/core/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel

# Build multiarch binaries
build-binary VERSION=version:
    cd ./cmd/costmodel && \
        {{commonenv}} GOOS=linux GOARCH=amd64 go build \
        -ldflags \
          "-X github.com/opencost/opencost/core/pkg/version.Version={{VERSION}} \
           -X github.com/opencost/opencost/core/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel-amd64

    cd ./cmd/costmodel && \
        {{commonenv}} GOOS=linux GOARCH=arm64 go build \
        -ldflags \
          "-X github.com/opencost/opencost/core/pkg/version.Version={{VERSION}} \
           -X github.com/opencost/opencost/core/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel-arm64

# Build and push a multi-arch Docker image
build IMAGE_TAG RELEASE_VERSION: (build-binary RELEASE_VERSION)
    docker buildx build \
        --rm \
        --platform "linux/amd64" \
        -f 'Dockerfile.cross' \
        --build-arg binarypath=./cmd/costmodel/costmodel-amd64 \
        --build-arg version={{RELEASE_VERSION}} \
        --build-arg commit={{commit}} \
        --provenance=false \
        -t {{IMAGE_TAG}}-amd64 \
        --push \
        .

    docker buildx build \
        --rm \
        --platform "linux/arm64" \
        -f 'Dockerfile.cross' \
        --build-arg binarypath=./cmd/costmodel/costmodel-arm64 \
        --build-arg version={{RELEASE_VERSION}} \
        --build-arg commit={{commit}} \
        --provenance=false \
        -t {{IMAGE_TAG}}-arm64 \
        --push \
        .

    manifest-tool push from-args \
        --platforms "linux/amd64,linux/arm64" \
        --template {{IMAGE_TAG}}-ARCH \
        --target {{IMAGE_TAG}}

validate-protobuf:
    ./generate.sh
    git diff --exit-code
