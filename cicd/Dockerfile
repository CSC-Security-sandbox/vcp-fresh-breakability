FROM ghcr.io/vcp-vsa-control-plane/vsa-builder:v8

#ARG RUNNER=github
#ARG GO_VERSION
#ARG GO_FILENAME
#ARG GO_FILENAME_SHA

ARG RUNNER=github
#ARG GO_VERSION
#ARG GO_FILENAME
#ARG GO_FILENAME_SHA

USER root

SHELL [ "/bin/bash", "-e", "-o", "pipefail", "-c" ]

ENV PATH=$PATH:/usr/local/go/bin
ENV GOBIN=/usr/local/go/bin

RUN mkdir -p /usr/local/go && chown github:github /usr/local/go

USER github

#RUN cd /tmp && curl -y 20 -Y 1000 --retry 5 --retry-max-time 30 --connect-timeout 30 --no-progress-meter -SLO https://go.dev/dl/${GO_FILENAME} ; \
#  echo "${GO_FILENAME_SHA} ${GO_FILENAME}" | sha256sum -c - || exit 1 ; \
#  cd /usr/local && \
#  tar zxf /tmp/${GO_FILENAME} && \
#  rm -rf /tmp/* && \
#  /usr/local/go/bin/go version > ~/go-version

COPY bin/build/linux/vsacictl /usr/local/bin/

USER root

RUN chown github:github /usr/local/bin/vsacictl && \
  chmod +x /usr/local/bin/vsacictl

RUN mkdir -p /workspace && chown github:github /workspace

WORKDIR /workspace

USER github
