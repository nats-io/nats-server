#!/usr/bin/env bash

set -e

which go
go version

which gotestsum
gotestsum --version

rm -f "${JUNIT_REPORT_PATH}"

echo "🚀 Launching tests..."
if gotestsum \
  --format standard-verbose \
  --junitfile "${JUNIT_REPORT_PATH}" \
  -- \
  ./server \
  -run='^TestLongDummy.*' \
  -tags=include_js_cluster_long_running_tests \
  -race \
  -v \
  -count=1 -vet=off -shuffle=on -p=1 \
  -timeout=60m \
; then
  echo "All tests passed";
else
  echo "Some tests failed";
fi

if [[ ! -f "${JUNIT_REPORT_PATH}" ]]; then
  echo "Test report file not found: ${JUNIT_REPORT_PATH}"
  exit 1
fi
