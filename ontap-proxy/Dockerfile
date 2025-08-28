# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy the entire project to get go.mod and dependencies
COPY . .

# Build the ontap-proxy application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./ontap-proxy

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/main .

# Expose port
EXPOSE 8080

# Run the application
CMD ["./main"] 