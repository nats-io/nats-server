# Roadmap [DRAFT]

This is the roadmap for the NATS Server. The purpose of this document is to give insight into some of
the major features we plan to implement in the near future. It is a living document, and will change
 as goals and milestones are completed and new plans are made.

This project's release plan is kept updated on [the wiki](https://github.com/nats-io/gnatsd/wiki).

A master project release plan for all NATS projects can be found
[here](https://github.com/nats-io/roadmap/wiki)

### Planning and Tracking

An [Open-Source Planning Process](https://github.com/nats-io/gnatsd/wiki/Open-Source-Planning-Process) is 
used to define the Roadmap. 

This project is managed via GitHub [milestones](https://github.com/nats-io/gnatsd/milestones). 
[Project Pages](https://github.com/nats-io/gnatsd/wiki) define the 
goals for each Milestone and identify current progress.

Upcoming features and bugfixes will be added to the relevant milestone. One or more Milestones will
 in turn be assigned to a release.
If a feature or bugfix is not part of a milestone, it is currently unscheduled for implementation. 

### Short Term

#### "Hot" configuration reload

We will enable changes to a subset of configuration items on a running `gnatsd` without requiring
the instance to be restarted.  
  
See [Issue #338](https://github.com/nats-io/gnatsd/issues/338)
  
#### Integrate Promethus support 

We have done some work recently on exporting NATS monitoring data via Promethus. We plan to 
share this work in an upcoming release.
 
See [Issue #345](https://github.com/nats-io/gnatsd/issues/345)


### Further Out

#### Enable external authentication and authorization via extensible auth provider 

We will improve/extend the auth interface(s) and configuration to support external auth endpoints.

See [Issue #434](https://github.com/nats-io/gnatsd/issues/434)

#### Namespace isolation by account/organization

Multi-tenancy is a popular discussion topic, and one of the requirements we often hear is for the 
ability to isolate/encapsulate the subject namespace by "tenant" (e.g. an account or organization). 
For example, messages published on `FOO.BAR` by tenant A would not be visible to tenant B's 
subscribers on `FOO.BAR`, and vice versa. We'd like to support this in an efficient and performant way. 

See [Issue #347](https://github.com/nats-io/gnatsd/issues/347) and [Issue #348](https://github.com/nats-io/gnatsd/issues/348)

#### Rate limiting

Another multi-tenancy concern is the ability to limit the rate at which publishers can publish messages
to `gnatsd`. We've implemented this in a branch for an internal project but would like to make it public soon.
 
See [Issue #346](https://github.com/nats-io/gnatsd/issues/346)

#### Make auto-unsubscribe atomic to the SUB protocol

Right now auto-unsubscribe is two operations: `SUB foo 1` followed by `UNSUB foo 1 1` (unsubscribe 
after 1 message). For the sake of simplicty, efficiency and usability, we'd like to make that an 
atomic protocol operation, optionally specifying the limit as part of the subscription e.g. `SUB foo 1 1`

See [Issue #344](https://github.com/nats-io/gnatsd/issues/344)

#### Rename `gnatsd` to `nats-server`

The original Ruby-based NATS server (now retired) was called `nats-server`, and the newer Go 
version needed a unique name, hence `gnatsd`. We've recently gone through and attempted to 
normalize names across all NATS projects to make them more predictable. Renaming `gnatsd` to 
`nats-server` would align it more closely with the rest of the components in the NATS family.
 
See See [Issue #226](https://github.com/nats-io/gnatsd/issues/226)

#### Client pruning/rebalancing

Now that cluster auto-discovery is implemented for both clients and servers, NATS clusters can 
be dynamically expanded on demand. When this happens, it would be useful to (optionally) re-balance
 the client connections across the new (larger) cluster.
 
 See See [Issue #343](https://github.com/nats-io/gnatsd/issues/343)
 
 