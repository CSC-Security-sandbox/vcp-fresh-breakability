FROM ghcr.io/greenqloud/docker-base-images/distroless/debian-static:12
COPY core/build/linux/bin/vcp-db-migrate /vcp-db-migrate