# Architecture Decision Records

The directory [adr](adr) hold Architecture Decision Records that document major decisions made
in the design of the NATS Server.

A good intro to ADRs can be found in [Documenting Architecture Decisions](http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions) by Michael Nygard.

## When to write an ADR

Not every little decision needs an ADR, and we are not overly prescriptive about the format.
The kind of change that should have an ADR are ones likely to impact many client libraries
or those where we specifically wish to solicit wider community input.

## Format

The [adr-tools](https://github.com/npryce/adr-tools) utility ships with a template that's a
good starting point. We do not have a fixed format at present.

## ADR Statuses

Each ADR has a status, let's try to use `Proposed`, `Approved` and `Rejected`.
