#!/bin/bash
cd .. || exit

  docker build --build-arg SSH_CONFIG="StrictHostKeyChecking" -t ghcr.io/vcp-vsa-control-plane/vsa-builder:v8 --platform=linux/amd64 --push .

