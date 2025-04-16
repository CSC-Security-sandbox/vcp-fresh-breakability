ARG BASE
FROM golang:1.24.1 as builder
WORKDIR /code
COPY google-proxy google-proxy/
COPY common common/
COPY utils utils/
COPY core core/
COPY config config/
COPY database database/
COPY go.mod go.mod
COPY go.sum go.sum

# `skaffold debug` sets SKAFFOLD_GO_GCFLAGS to disable compiler optimizations
ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/google-proxy
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

FROM $BASE
COPY --from=builder /app .
