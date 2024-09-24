#!/usr/bin/env bash

set -e

which go
go version

which gotestsum
gotestsum --version

rm -f "${JUNIT_REPORT_PATH}"

echo "🚀 Launching tests..."
gotestsum \
  --format standard-verbose \
  --junitfile "${JUNIT_REPORT_PATH}" \
  -- \
  ./server \
  -run='^TestLong.*' \
  -tags=include_js_cluster_long_running_tests \
  -race \
  -v \
  -count=1 -vet=off -shuffle=on -p=1 \
  -timeout=60m \
;
