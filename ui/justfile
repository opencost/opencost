commit := `git rev-parse --short HEAD`
thirdPartyLicenseFile := "THIRD_PARTY_LICENSES.txt"

default:
    just --list

build-local:
    npm install

    npx parcel build src/index.html

build IMAGE_TAG RELEASE_VERSION: build-local
    cp ../{{thirdPartyLicenseFile}} .
    docker buildx build \
        --rm \
        --platform "linux/amd64" \
        -f 'Dockerfile.cross' \
        --provenance=false \
        -t {{IMAGE_TAG}}-amd64 \
        --build-arg version={{RELEASE_VERSION}} \
        --build-arg commit={{commit}} \
        --push \
        .

    docker buildx build \
        --rm \
        --platform "linux/arm64" \
        -f 'Dockerfile.cross' \
        --provenance=false \
        -t {{IMAGE_TAG}}-arm64 \
        --build-arg version={{RELEASE_VERSION}} \
        --build-arg commit={{commit}} \
        --push \
        .

    manifest-tool push from-args \
        --platforms "linux/amd64,linux/arm64" \
        --template {{IMAGE_TAG}}-ARCH \
        --target {{IMAGE_TAG}}

    rm -f {{thirdPartyLicenseFile}}
