GO ?= go
SHELL := bash
# Go modules in this repo. golangci-lint and go fix only operate on the module
# rooted at the working directory, so each must be run from its own directory.
MODULES := . ./core ./modules/prometheus-source ./modules/collector-source
IMAGE_TAG ?= $(shell ./tools/image-tag)
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_LAST_COMMIT_DATE := $(shell git log -1 --date=iso-strict --format=%cd)

# Build flags
VPREFIX := github.com/opencost/opencost/core/pkg/version
GO_LDFLAGS   := -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).GitCommit=$(GIT_REVISION)
GO_FLAGS     := -ldflags "-extldflags \"-static\" -s -w $(GO_LDFLAGS)"

.PHONY: go/bin
go/bin:
	CGO_ENABLED=0 $(GO) build $(GO_FLAGS) ./cmd/costmodel

# LINT_BASE is the git ref used as the baseline for "new issues only" linting.
# golangci-lint reports only findings introduced relative to the merge base
# with this ref, so the large backlog of pre-existing issues does not block
# PRs while every newly written/changed line is still gated. Override locally
# to lint against a different base (e.g. LINT_BASE=HEAD~1).
LINT_BASE ?= origin/develop

# ci-lint installs the latest golangci-lint and runs it against every module,
# failing only on issues introduced since the merge base with $(LINT_BASE).
.PHONY: ci-lint
ci-lint:
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@for mod in $(MODULES); do \
		echo "==> golangci-lint run ($$mod)"; \
		( cd $$mod && golangci-lint run --config=$(CURDIR)/.golangci.yml --new-from-merge-base=$(LINT_BASE) ) || exit 1; \
	done

# go-fix-check runs the Go fix tool to update deprecated or outdated API usage
# to current equivalents (e.g. old error patterns, renamed stdlib identifiers).
# -diff prints a unified diff instead of rewriting files, and exits non-zero
# if the diff is non-empty, which causes CI to fail.
.PHONY: go-fix-check
go-fix-check:
	@for mod in $(MODULES); do \
		echo "==> go fix -diff ($$mod)"; \
		( cd $$mod && $(GO) fix -diff ./... ) || exit 1; \
	done