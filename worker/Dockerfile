ARG BASE
FROM golang:1.24.1 AS builder

ARG GHVSA_PAT
RUN git config --global url."https://${GHVSA_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

WORKDIR /code
COPY go.mod go.mod
COPY go.sum go.sum
COPY core core/
COPY common common/
COPY database database/
COPY config config/
COPY utils utils/
COPY google-proxy google-proxy/
COPY clients clients/
COPY worker worker/
COPY workflow_engine workflow_engine/

# Download dependencies and cache them
RUN go mod download

ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/worker
# Build the Go app
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .
# RUN CGO_ENABLED=0 GOOS=linux go build -o /app main.go
FROM $BASE

# Copy the binary from the builder stage
COPY --from=builder /app /app
COPY --from=builder /code/core/errors/errors.json /errors.json

# Set ownership and permissions
USER nonroot:nonroot