#!/usr/bin/env bash

set -e

which curl
curl --version

BUILDKITE_ANALYTICS_TOKEN="${BK_ANALYTICS_TOKEN_long_running_tests}"

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
