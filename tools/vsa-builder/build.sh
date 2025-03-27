#!/bin/bash
cd .. || exit

  docker build --build-arg SSH_CONFIG="StrictHostKeyChecking" -t vsa-builder --platform=linux/amd64 .

