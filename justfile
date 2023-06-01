commonenv := "CGO_ENABLED=0"

version := "dev"
commit := `git rev-parse --short HEAD`

default:
    just --list

# Run unit tests
test:
    {{commonenv}} go test ./...

# Compile a local binary
build-local:
    cd ./cmd/costmodel && \
        {{commonenv}} go build \
        -ldflags \
          "-X github.com/opencost/opencost/pkg/version.Version={{version}} \
           -X github.com/opencost/opencost/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel

# Build multiarch binaries
build-binary VERSION=version:
    cd ./cmd/costmodel && \
        {{commonenv}} GOOS=linux GOARCH=amd64 go build \
        -ldflags \
          "-X github.com/opencost/opencost/pkg/version.Version={{VERSION}} \
           -X github.com/opencost/opencost/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel-amd64

    cd ./cmd/costmodel && \
        {{commonenv}} GOOS=linux GOARCH=arm64 go build \
        -ldflags \
          "-X github.com/opencost/opencost/pkg/version.Version={{VERSION}} \
           -X github.com/opencost/opencost/pkg/version.GitCommit={{commit}}" \
        -o ./costmodel-arm64

# Build and push a multi-arch Docker image
build IMAGETAG VERSION=version: test (build-binary VERSION)
    docker buildx build \
        --rm \
        --platform "linux/amd64" \
        -f 'Dockerfile.cross' \
        --build-arg binarypath=./cmd/costmodel/costmodel-amd64 \
        --provenance=false \
        -t {{IMAGETAG}}-amd64 \
        --push \
        .

    docker buildx build \
        --rm \
        --platform "linux/arm64" \
        -f 'Dockerfile.cross' \
        --build-arg binarypath=./cmd/costmodel/costmodel-arm64 \
        --provenance=false \
        -t {{IMAGETAG}}-arm64 \
        --push \
        .

    manifest-tool push from-args \
        --platforms "linux/amd64,linux/arm64" \
        --template {{IMAGETAG}}-ARCH \
        --target {{IMAGETAG}}
