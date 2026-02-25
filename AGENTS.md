# AGENTS.md

## Project information

- NATS Server is a Go project that implements the Core NATS protocol and JetStream
- Relevant code largely lives within the `./server` directory and subdirectories
- There are extra tests within the `./test` directory, but most of them also live within `./server` and should generally be added there
- There are very few external dependencies, this is intentional, and we strongly prefer solutions involving the standard library where possible
- Unit tests make use of helper functions from `test_test.go`, `jetstream_helpers_test.go` and proposals to unit tests must follow this standard

## Code review

- When being asked to review changes or pull requests, use your expertise of the Go programming language to provide concise and targeted feedback
- You should always evaluate code for security, correctness and performance. You should guard in particular against panics, data loss or potential for denial of service
- Where comments are present, you should verify that the comments and the code match
- Keep signal to noise ratio in mind when commenting, e.g. you should not comment on anything that is purely stylistic when not prompted to do so
- Pay attention to the unit tests, a good unit test proves the behaviour and guards against regressions
