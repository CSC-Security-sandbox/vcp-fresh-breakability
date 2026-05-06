FROM gcr.io/distroless/static
COPY core/build/linux/bin/vcp-iam-lifecycle /vcp-iam-lifecycle
ENTRYPOINT ["/vcp-iam-lifecycle"]
