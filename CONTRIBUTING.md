# Contributing

Thanks for your interest in contributing! This document contains **nats-io/nats-server** specific contributing details.

If you are a first-time contributor, please refer to the general [NATS Contributor Guide](https://nats.io/contributing/) to get a comprehensive overview of the contribution process including:

- Governance
- Design goals
- Communication channels
- Reporting issues or proposing features
- Opening pull requests
- Review process

## Branching strategy

The current branching strategy used for this repo includes:

- `main` - tracks changes for the current minor series, e.g. `2.9.x`
- `dev` - tracks changes for the first release of next minor series, e.g. `2.10.0`

Once `2.10.0` is released, `main` will then track patch releases for the `2.10.x` series and `dev` will be bumped to track the `2.11.0` release.

_Note: there may be various other branches present in the repo, owned by core maintainers having branching permission on the repo, however only `main` and `dev` track changes that will result in releases._

## Contributing code

Before writing any code, the branch the changes will be propsoed on needs to be determined.

The `main` branch, tracking the current minor series, can have the following change types proposed:

- bug fixes
- simple optimizations
- documentation/comment updates
- additional tests or benchmarks

The `dev` branch, tracking the next minor release is allowed to have:

- new features
- new configuration options
- new fields on existing APIs
- major refactoring
- major optimizations

Note that depending on the scope and potential impact of a proposed change, the maintainers may request that a proposed change be rebased to target the _other_ branch, typically `main` &rarr; `dev`.
