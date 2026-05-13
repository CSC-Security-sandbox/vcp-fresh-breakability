FROM gcr.io/distroless/static
COPY vcp-core/build/linux/bin/vcp-iam-lifecycle /vcp-iam-lifecycle
ENTRYPOINT ["/vcp-iam-lifecycle"]
