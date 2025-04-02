FROM ubuntu:22.04
#USER root
ARG SSH_CONFIG


SHELL [ "/usr/bin/bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c" ]
USER root

# *.txt files contain the download URL's for all tools
COPY *.txt /tmp/

COPY scripts/builder.sh /scripts/builder.sh
RUN chmod +x /scripts/builder.sh
RUN /scripts/builder.sh
USER github
RUN mkdir ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/config && chmod 600 ~/.ssh/config && echo ${SSH_CONFIG} >~/.ssh/config