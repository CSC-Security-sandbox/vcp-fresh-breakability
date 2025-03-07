ARG BASE
FROM golang:1.24.1 as builder
WORKDIR /code
COPY web.go .
COPY go.mod .
COPY go.sum .
RUN go get
# `skaffold debug` sets SKAFFOLD_GO_GCFLAGS to disable compiler optimizations
ARG SKAFFOLD_GO_GCFLAGS
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

FROM $BASE
COPY --from=builder /app .
