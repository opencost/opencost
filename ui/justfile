version := `../tools/image-tag`
commit := `git rev-parse --short HEAD`

default:
    just --list

build-local:
    npm install

    npx parcel build src/index.html

build IMAGETAG: build-local
    docker buildx build \
        --rm \
        --platform "linux/amd64" \
        -f 'Dockerfile.cross' \
        --provenance=false \
        -t {{IMAGETAG}}-amd64 \
        --build-arg version={{version}} \
        --build-arg commit={{commit}} \
        --push \
        .

    docker buildx build \
        --rm \
        --platform "linux/arm64" \
        -f 'Dockerfile.cross' \
        --provenance=false \
        -t {{IMAGETAG}}-arm64 \
        --build-arg version={{version}} \
        --build-arg commit={{commit}} \
        --push \
        .

    manifest-tool push from-args \
        --platforms "linux/amd64,linux/arm64" \
        --template {{IMAGETAG}}-ARCH \
        --target {{IMAGETAG}}
