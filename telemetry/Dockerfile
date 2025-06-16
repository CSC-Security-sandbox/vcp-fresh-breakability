ARG BASE
FROM golang:1.24.1 AS builder

# Create and change to the app directory.
WORKDIR /code

ARG GHVSA_PAT
RUN git config --global url."https://${GHVSA_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

COPY go.mod go.mod
COPY go.sum go.sum
COPY telemetry telemetry/
COPY utils utils/

RUN go mod download

ARG SKAFFOLD_GO_GCFLAGS
WORKDIR /code/telemetry

RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}" -trimpath -o /app .

# Use the distroless base image for a lean production container.
FROM $BASE

# Copy the binary to the production image from the builder stage.
COPY --from=builder /app /app

# Document the port the container listens on.
EXPOSE 8080

# Run the web service on container startup.
CMD ["/app"]