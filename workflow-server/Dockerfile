ARG BASE
FROM golang:1.24.1 as builder
WORKDIR /code
COPY workflow-server workflow-server/
COPY go.mod go.mod
COPY go.sum go.sum

# `skaffold debug` sets SKAFFOLD_GO_GCFLAGS to disable compiler optimizations
ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/workflow-server
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

FROM $BASE
COPY --from=builder /code/workflow-server/config config/
COPY --from=builder /app .
