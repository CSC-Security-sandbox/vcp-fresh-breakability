ARG BASE
FROM $BASE

COPY artifacts/core /app
COPY core/errors/errors.json /errors.json

# Set ownership and permissions
USER nonroot:nonroot

