# Release Workflow

- As of NATS 2.11, the server follows a 6-month release cycle. 2.11 was released in March 2025.
- Version tagging follows semantic versioning with security fixes clearly marked.
- The `main` branch is for active development of features and bug fixes. Release branches are created for preparing patch releases. Releases are tagged with semantic versions
- The current and previous minor series are supported in terms of bug fixes and security patches.
- Binaries are released via [GitHub Releases](https://github.com/nats-io/nats-server/releases) and Docker images are available as an [official image](https://hub.docker.com/_/nats).
- Preview releases for the next minor version, e.g. 2.14.0, become available once feature development is complete.
- [Nightly Docker images](https://hub.docker.com/r/synadia/nats-server/tags) of `main` are available for testing.
