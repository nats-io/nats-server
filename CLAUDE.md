# CLAUDE.md

This file provides guidance to Claude Code when working with the nats-server codebase.

## Project Overview

NATS server is a high-performance, cloud-native messaging system (CNCF project) written in Go. It provides pub/sub, request/reply, JetStream persistence, clustering via Raft, super-cluster gateways, leaf nodes, MQTT bridging, and WebSocket transport. The module path is `github.com/nats-io/nats-server/v2`.

## Build Commands

```bash
go build                                    # Build the server binary
go fmt ./...                                # Format code (required before commits)
golangci-lint run --timeout=5m --config=.golangci.yml  # Lint (uses golangci-lint v2)
go generate                                 # Regenerate jetstream_errors_generated.go from errors.json
```

## Running Tests

Tests are split into categories via build tags and test name prefixes. The CI script `./scripts/runTestsOnTravis.sh` orchestrates them. Always use `-p=1` for server tests to avoid port/resource conflicts.

```bash
# Run a single test
go test -v -run=TestSpecificName ./server -count=1 -vet=off -timeout=30m

# Run a single test with race detection
go test -race -v -run=TestSpecificName ./server -count=1 -vet=off -timeout=30m

# Full test categories (as CI runs them):
go test -race -v -p=1 -run=TestJetStream ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2,skip_js_cluster_tests_3,skip_js_cluster_tests_4,skip_js_super_cluster_tests,skip_js_consumer_tests -count=1 -vet=off -timeout=30m -failfast   # JetStream (non-cluster)
go test -race -v -p=1 -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests_2,skip_js_cluster_tests_3,skip_js_cluster_tests_4 -count=1 -vet=off -timeout=30m -failfast   # JS Cluster batch 1
go test -race -v -p=1 -run=TestJetStreamConsumer ./server -count=1 -vet=off -timeout=30m -failfast   # JS Consumer tests
go test -race -v -p=1 -run=TestJetStreamSuperCluster ./server -count=1 -vet=off -timeout=30m -failfast   # Super-cluster tests
go test -race -v -p=1 ./server/... -run="^TestNRG" -count=1 -vet=off -timeout=30m -failfast   # Raft (NRG) tests
go test -race -v -p=1 -run=TestMQTT ./server -count=1 -vet=off -timeout=30m -failfast   # MQTT tests
go test -race -v -p=1 -run=TestMsgTrace ./server -count=1 -vet=off -timeout=30m -failfast   # Message tracing tests
go test -race -v -p=1 -run=Test\(Store\|FileStore\|MemStore\) ./server -count=1 -vet=off -timeout=30m -failfast   # Store tests
go test -race -v -p=1 ./server/... -run="^TestJWT" -count=1 -vet=off -timeout=30m -failfast   # JWT tests
go test -v -p=1 -run=TestNoRace ./... -tags=skip_no_race_2_tests -count=1 -vet=off -timeout=30m -failfast   # NoRace batch 1 (NO -race flag!)
go test -v -p=1 -run=TestNoRace ./... -tags=skip_no_race_1_tests -count=1 -vet=off -timeout=30m -failfast   # NoRace batch 2 (NO -race flag!)
go test -race -v -p=1 $(go list ./... | grep -v "/server") -count=1 -vet=off -timeout=30m -failfast   # Non-server packages

# Long-running tests (nightly only, opt-in build tag):
go test -race -v -run='^TestLong.*' ./server -tags=include_js_long_tests -count=1 -vet=off -timeout=60m -shuffle on -p 1 -failfast

# External MQTT tests (require mqtt-cli tool):
go test -v --run='TestXMQTT' ./server
```

## Test Build Tags

Tests use build tags extensively to partition CI into parallel jobs:

| Tag | Effect |
|-----|--------|
| `skip_js_tests` | Exclude all JetStream tests |
| `skip_js_cluster_tests` | Exclude JS cluster batch 1 |
| `skip_js_cluster_tests_2` | Exclude JS cluster batch 2 |
| `skip_js_cluster_tests_3` | Exclude JS cluster batch 3 |
| `skip_js_cluster_tests_4` | Exclude JS cluster batch 4 |
| `skip_js_super_cluster_tests` | Exclude super-cluster tests |
| `skip_js_consumer_tests` | Exclude JS consumer tests |
| `skip_mqtt_tests` | Exclude MQTT tests |
| `skip_msgtrace_tests` | Exclude message tracing tests |
| `skip_store_tests` | Exclude store tests |
| `skip_no_race_tests` | Exclude all NoRace tests |
| `skip_no_race_1_tests` | Exclude NoRace batch 1 |
| `skip_no_race_2_tests` | Exclude NoRace batch 2 |
| `include_js_long_tests` | Include long-running JS tests (opt-in) |

## Test Naming Conventions

Test function prefixes determine which CI job runs them:
- `TestJetStream*` -- JetStream (non-cluster)
- `TestJetStreamCluster*` -- JetStream clustering
- `TestJetStreamSuperCluster*` -- Super-cluster
- `TestJetStreamConsumer*` -- Consumer-specific
- `TestNRG*` -- Raft consensus (NATS Raft Groups)
- `TestMQTT*` -- MQTT protocol
- `TestMsgTrace*` -- Message tracing
- `TestFileStore*`, `TestMemStore*`, `TestStore*` -- Storage backends
- `TestJWT*` -- JWT authentication
- `TestNoRace*` -- Must run WITHOUT race detector
- `TestLong*` -- Long-running tests (nightly, needs `include_js_long_tests` tag)
- `TestXMQTT*` -- External MQTT tests (need mqtt-cli installed)

## Test Helper Functions

Tests in `server/` use custom assertion helpers (defined in `server/test_test.go`), not third-party libraries:
- `require_NoError(t, err)`
- `require_Error(t, err, expected...)`
- `require_Equal(t, a, b)` (generic)
- `require_NotEqual(t, a, b)` (generic)
- `require_True(t, b)` / `require_False(t, b)`
- `require_Contains(t, s, subStrs...)`
- `require_Len(t, a, b)`
- `require_NotNil(t, vs...)`
- `require_ChanRead(t, ch, timeout)` / `require_NoChanRead(t, ch, timeout)`
- `checkFor(t, totalWait, sleepDur, f)` -- poll with timeout until `f()` returns nil

Cluster/JetStream test helpers are in `server/jetstream_helpers_test.go`:
- `cluster` and `supercluster` structs for multi-server test setups
- Raft timing is sped up in tests via `init()` in that file

Tests in `test/` package (integration tests) use a separate set of helpers in `test/test.go`.

## Code Style and Linting

The project uses `golangci-lint` v2 with the config in `.golangci.yml`. Key rules:
- **No `fmt.Print*`**: The `forbidigo` linter bans `fmt.Printf`, `fmt.Println`, `fmt.Print` in all code except `main.go`, `server/opts.go`, and test files. Use server logging methods instead.
- **Enabled linters**: `forbidigo`, `govet`, `ineffassign`, `misspell` (US locale), `staticcheck`, `unused`
- **Formatter**: `goimports` (auto-manages import grouping)
- **Staticcheck**: All checks enabled except `QF*` (quickfixes), `ST1003` (naming), `ST1016` (receiver naming)

Additional style conventions:
- Use `_EMPTY_` constant (defined in server package) instead of `""` for empty strings
- Copyright headers on all files: `// Copyright <year>-<year> The NATS Authors`
- Use `t.Helper()` in all test helper functions

## Commit Requirements

- All commits require a `Signed-off-by: Legal Name <email>` trailer (use `git commit -s`)
- PR descriptions must also contain a `Signed-off-by:` line
- CI checks signoffs on every commit in a PR

## Project Structure

```
main.go                     # Entry point, CLI flag parsing
server/                     # Core server package (almost all code lives here)
  server.go                 # Server struct, lifecycle, main loop
  client.go                 # Client connection handling, protocol parsing
  route.go                  # Inter-server routing for clusters
  gateway.go                # Super-cluster gateway connections
  leafnode.go               # Leaf node connections for edge/bridging
  jetstream.go              # JetStream API and management
  jetstream_api.go          # JetStream API request handlers
  jetstream_cluster.go      # Cluster-aware JetStream operations
  jetstream_events.go       # JetStream advisory/event publishing
  jetstream_batching.go     # Batched message storage
  jetstream_versioning.go   # Stream/consumer versioning
  consumer.go               # JetStream consumer implementation
  filestore.go              # File-based storage backend
  memstore.go               # Memory-based storage backend
  raft.go                   # Raft consensus (NRG) implementation
  mqtt.go                   # MQTT protocol bridge
  websocket.go              # WebSocket transport
  monitor.go                # HTTP monitoring/health endpoints
  accounts.go               # Multi-tenancy account system
  auth.go                   # Authentication framework
  auth_callout.go           # External auth callout
  jwt.go                    # JWT-based auth
  nkey.go                   # NKey cryptographic auth
  opts.go                   # Server options/configuration parsing
  reload.go                 # Hot configuration reload
  sublist.go                # Subject matching/subscription engine
  errors.go                 # Error definitions
  errors.json               # Error catalog (source for code generation)
  errors_gen.go             # Code generator for errors
  jetstream_errors_generated.go  # Generated error constants (do not edit)
  const.go                  # Version, constants
  log.go                    # Server logging methods
  test_test.go              # Test assertion helpers (require_*)
  jetstream_helpers_test.go # Cluster/supercluster test setup helpers
  norace_1_test.go          # NoRace tests batch 1 (build tag: !race && !skip_no_race_1_tests)
  norace_2_test.go          # NoRace tests batch 2 (build tag: !race && !skip_no_race_2_tests)
  jetstream_cluster_long_test.go  # Long-running tests (build tag: include_js_long_tests)
  configs/                  # Test configuration files
  testdata/                 # Test data files
  avl/                      # AVL tree / sequence set implementation
  stree/                    # Radix tree (subject tree) implementation
  gsl/                      # Generic subject list
  pse/                      # Process stats (platform-specific)
  sysmem/                   # System memory detection
  certidp/                  # OCSP certificate validation
  certstore/                # Platform certificate store (Windows)
  tpm/                      # TPM support for JetStream encryption
  thw/                      # Thread-safe hash writer
  ats/                      # Atomic timestamp
  elastic/                  # Elastic subject mapping
conf/                       # Configuration file parser (lexer + parser)
logger/                     # Logging package
internal/                   # Internal packages
  antithesis/               # Antithesis fault injection SDK integration
  fastrand/                 # Fast random number generation
  ldap/                     # LDAP authentication support
  ocsp/                     # OCSP helpers
  testhelper/               # Shared test utilities
test/                       # Integration tests (separate package, tests server via public API)
  test.go                   # Integration test helpers (RunServer, etc.)
  configs/                  # Test config files for integration tests
scripts/                    # CI/CD scripts
  runTestsOnTravis.sh       # Test runner (partitions tests for CI)
  cov.sh                    # Code coverage script
docker/                     # Docker build files
```

## Lock Ordering

The codebase has documented lock ordering in `locksordering.txt`. Key hierarchy:
```
jetStream -> jsAccount -> Server -> client -> Account
jetStream -> jsAccount -> stream -> consumer
reloadMu -> Server, reloadMu -> optsMu
jscmMu -> Account -> jsAccount
stream -> clMu, stream -> batchMu -> clMu, stream -> ddMu
```
Violating this ordering causes deadlocks. Always acquire locks in the documented order.

## Key Dependencies

- `github.com/nats-io/nats.go` -- Go client (used extensively in tests)
- `github.com/nats-io/jwt/v2` -- JWT token handling
- `github.com/nats-io/nkeys` -- NKey cryptographic identities
- `github.com/nats-io/nuid` -- Unique ID generation
- `github.com/klauspost/compress` -- S2/Snappy compression
- `github.com/minio/highwayhash` -- HighwayHash for integrity
- `golang.org/x/crypto` -- Cryptographic primitives

## Important Development Notes

1. **Performance-critical codebase**: This is a high-performance messaging system. Benchmark any changes to hot paths. Avoid unnecessary allocations.
2. **No external test frameworks**: Use the built-in `require_*` helpers, not testify or similar.
3. **No unnecessary dependencies**: Adding new entries to `go.mod` is strongly discouraged.
4. **Test isolation**: Tests use `Port: -1` for random port assignment and `t.TempDir()` for storage. Always clean up.
5. **Race detector**: Most tests run with `-race`. `TestNoRace*` tests intentionally run without it (they may test behaviors incompatible with the race detector).
6. **Generated code**: `server/jetstream_errors_generated.go` is generated from `server/errors.json` via `go generate`. Edit `errors.json`, not the generated file.
7. **Config format**: NATS uses its own config format (not TOML/YAML). The parser is in `conf/`. Supports `=`, `:`, or whitespace as key-value separators, `#` and `//` comments.
