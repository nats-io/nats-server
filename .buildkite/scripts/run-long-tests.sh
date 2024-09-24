#!/usr/bin/env bash

set -e
which go
go version

which gotestsum
gotestsum --version

which curl
curl --version

BUILDKITE_ANALYTICS_TOKEN="${BK_ANALYTICS_TOKEN_long_running_tests}"

rm -f junit.xml

# Wrap gotestsum because it may return non-zero exit code

echo "🚀 Launching tests..."
if gotestsum \
  --format standard-verbose \
  --junitfile junit.xml \
  -- \
  ./server \
  -run='^TestLong.*' \
  -tags=include_js_cluster_long_running_tests \
  -race \
  -v \
  -count=1 -vet=off -shuffle=on -p=1 \
  -timeout=60m \
  ; then
    echo "👍 All tests passed!"
  else
    echo "👎 Some tests failed!"
  fi

if [[ -f junit.xml ]]; then
  curl \
    -X POST \
    --fail-with-body \
    -H "Authorization: Token token=\"$BUILDKITE_ANALYTICS_TOKEN\"" \
    -F "data=@junit.xml" \
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
else
  echo "Test report file not found"
  exit 1
fi
