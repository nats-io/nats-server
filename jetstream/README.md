# JetStream (Technical Preview)

JetStream is the [NATS.io](https://nats.io) persistence engine that will support streaming as well as traditional message and worker queues for At-Least-Once delivery semantics.

JetStream is composed of two major components, Message Sets and Observables. Message Sets determine the interest set, global ordering, retention policy, replicas and resource limits. Observables define how Message Sets are consumed, and have quite a few options.

Information about testing the JetStream Technical Preview can be found [here](https://github.com/nats-io/jetstream#readme)
