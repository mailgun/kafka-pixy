# syntax=docker/dockerfile:1.3

# This is a buildx compatible Dockerfile. Documentation can be found
# https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/syntax.md

# This Dockerfile uses a base golang image with our pinned golang version
# See https://github.com/mailgun/dockerworld/images for current pinned version

# This creates an cached layer of our dependencies for subsequent builds to use
FROM ghcr.io/mailgun/gobuild:latest AS deps
WORKDIR /go/src
RUN mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan github.com >> ~/.ssh/known_hosts && \
    git config --global url.ssh://git@github.com/.insteadOf https://github.com/ && \
    go env -w GOPRIVATE=github.com/mailgun
RUN --mount=type=bind,target=/go/src,rw \
    --mount=type=ssh,required=true \
    go mod download

# ==========================================================================
# NOTE: Since tests are run in travis, we just build the container image
# ==========================================================================
# Run tests
#FROM deps as test
#RUN --mount=type=bind,target=/go/src,rw \
    #go fmt ./... && \
    #go vet ./... && \
    #go test -v -p 1 -race -parallel=1 -tags holster_test_mode ./...

# Build cmds
FROM test as build
RUN --mount=type=bind,target=/go/src,rw \
    go version && \
    CGO_ENABLED=0 go install -a -v ./...
