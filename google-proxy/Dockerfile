ARG BASE
FROM $BASE

COPY artifacts/google-proxy /app
COPY core/errors/errors.json /errors.json
COPY common/vsa_config common/vsa_config

# Set ownership and permissions
USER nonroot:nonroot
