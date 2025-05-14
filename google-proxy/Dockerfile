ARG BASE
FROM golang:1.24.1 as builder


# Pass GitHub token as a build argument
ARG GHVSA_PAT

# Configure Git to use the token for authentication
RUN git config --global url."https://${GHVSA_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"


WORKDIR /code
COPY google-proxy google-proxy/
COPY common common/
COPY utils utils/
COPY core core/
COPY config config/
COPY database database/
COPY clients clients/
COPY workflow_engine workflow_engine/
COPY go.mod go.mod
COPY go.sum go.sum

# `skaffold debug` sets SKAFFOLD_GO_GCFLAGS to disable compiler optimizations
ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/google-proxy
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

FROM $BASE

## Install bash
#RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*


COPY --from=builder /app .
COPY common/vsa_config common/vsa_config