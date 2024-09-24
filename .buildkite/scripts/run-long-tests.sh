#!/usr/bin/env bash

set -e

which go
go version

which gotestsum
gotestsum --version

which curl
curl --version

BUILDKITE_ANALYTICS_TOKEN="${BK_ANALYTICS_TOKEN_long_running_tests}"
if [ -z "${BUILDKITE_ANALYTICS_TOKEN}" ];
then
  echo "Analytics token not set"
  exit 1
fi

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
;
then
  echo "All tests passed";
else
  echo "Some tests failed";
fi

if [[ ! -f "${JUNIT_REPORT_PATH}" ]]; then
  echo "Test report file not found: ${JUNIT_REPORT_PATH}"
  exit 1
fi

curl \
  -X POST \
  --fail-with-body \
  -H "Authorization: Token token=\"$BUILDKITE_ANALYTICS_TOKEN\"" \
  -F "data=@${JUNIT_REPORT_PATH}" \
  -F "format=junit" \
  -F "run_env[CI]=buildkite" \
  -F "run_env[key]=$BUILDKITE_BUILD_ID" \
  -F "run_env[number]=$BUILDKITE_BUILD_NUMBER" \
  -F "run_env[job_id]=$BUILDKITE_JOB_ID" \
  -F "run_env[branch]=$BUILDKITE_BRANCH" \
  -F "run_env[commit_sha]=$BUILDKITE_COMMIT" \
  -F "run_env[message]=$BUILDKITE_MESSAGE" \
  -F "run_env[url]=$BUILDKITE_BUILD_URL" \
  https://analytics-api.buildkite.com/v1/uploads
