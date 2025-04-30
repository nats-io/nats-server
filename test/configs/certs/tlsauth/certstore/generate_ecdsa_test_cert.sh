#!/usr/bin/env bash

set -eou pipefail

SCRIPT_ROOT="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

cert_file_prefix="${SCRIPT_ROOT}/ecdsa_server"
export_password="s3cr3t"

openssl req -x509 \
  -days 3650 \
  -newkey ec \
  -pkeyopt ec_paramgen_curve:secp384r1 \
  -sha384 \
  -subj "/CN=nats-server" \
  --addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
  -nodes \
  -out "${cert_file_prefix}.pem" \
  -keyout "${cert_file_prefix}.key" \
  -outform PEM >/dev/null 2>&1

openssl pkcs12 \
  -inkey "${cert_file_prefix}.key" \
  -in "${cert_file_prefix}.pem" \
  -export \
  -password "pass:${export_password}" \
  -out "${cert_file_prefix}.pfx" >/dev/null 2>&1
