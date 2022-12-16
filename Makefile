GO ?= go
SHELL := bash
IMAGE_TAG ?= $(shell ./tools/image-tag)
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_LAST_COMMIT_DATE := $(shell git log -1 --date=iso-strict --format=%cd)
GORELEASER_ENV := GIT_BRANCH=$(GIT_BRANCH) GIT_REVISION=$(GIT_REVISION) GIT_LAST_COMMIT_DATE=$(GIT_LAST_COMMIT_DATE) IMAGE_TAG=$(IMAGE_TAG)

# Build flags
VPREFIX := github.com/opencost/opencost/pkg/build
GO_LDFLAGS   := -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION) -X $(VPREFIX).BuildDate=$(GIT_LAST_COMMIT_DATE)
GO_FLAGS     := -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS)"

.PHONY: go/bin
go/bin:
	CGO_ENABLED=0 $(GO) build $(GO_FLAGS) ./cmd/costmodel