FROM golang:1.24.1 AS builder

# Create and change to the app directory.
WORKDIR /app

# Retrieve application dependencies.
# This allows the container build to reuse cached dependencies.
# Expecting to copy go.mod and if present go.sum.
COPY go.* ./
RUN go mod download

# Copy local code to the container image.
COPY . ./

ENV GOOS=linux GOARCH=amd64
# Build the binary.
RUN go build -v -o server

# Use the distroless base image for a lean production container.
FROM gcr.io/distroless/base

# Copy the binary to the production image from the builder stage.
COPY --from=builder /app/server /app/server

# Document the port the container listens on.
EXPOSE 8080

# Run the web service on container startup.
CMD ["/app/server"]