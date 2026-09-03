#!/bin/bash
set -euo pipefail

SSHD=$(command -v sshd || true)
if [ -z "${SSHD}" ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends openssh-server
  rm -rf /var/lib/apt/lists/*
  SSHD=$(command -v sshd)
fi
mkdir -p /run/sshd
ssh-keygen -A
echo "StrictModes no" >> /etc/ssh/sshd_config
exec "${SSHD}" -De
