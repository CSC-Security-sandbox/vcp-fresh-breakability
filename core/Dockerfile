ARG BASE
FROM golang:1.24.1 as builder

# Pass GitHub token as a build argument
ARG GHVSA_PAT

# Configure Git to use the token for authentication
RUN git config --global url."https://${GHVSA_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

WORKDIR /code
COPY core core/
COPY common common/
COPY database database/
COPY config config/
COPY utils utils/
COPY clients clients/
COPY workflow_engine workflow_engine/
COPY go.mod go.mod
COPY go.sum go.sum

# Download dependencies and cache them
RUN go mod download

# `skaffold debug` sets SKAFFOLD_GO_GCFLAGS to disable compiler optimizations
ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/core
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

FROM $BASE
COPY --from=builder /app /app
COPY --from=builder /code/core/errors/errors.json /errors.json

# Set ownership and permissions
USER nonroot:nonroot

