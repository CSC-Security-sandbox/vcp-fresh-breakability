FROM golang:1.24 AS builder
# Define GOTRACEBACK to mark this container as using the Go language runtime
# for `skaffold debug` (https://skaffold.dev/docs/workflows/debug/).
RUN apt-get update && apt-get install -y bash && apt-get install -y bash iputils-ping && apt-get clean
ENV GOTRACEBACK=single
CMD ["./app"]
