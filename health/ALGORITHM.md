# The ALLCALL leader election algorithm.

Jason E. Aten

February 2017

definition, with example value.
-----------

Let heartBeat = 3 sec. This is how frequently
we will assess cluster health by
sending out an allcall ping.

givens
--------

* Given: Let each server have a numeric integer rank, that is distinct
and unique to that server. If necessary an extremely long
true random number is used to break ties between server ranks, so
that we may assert, with probability 1, that all ranks are distinct.
Server can be put into a strict total order.

* Rule: The lower the rank is preferred for being the leader.


ALLCALL Algorithm 
===========================

### I. In a continuous loop

The server always accepts and respond
to allcall broadcasts from other cluster members.

The allcall() ping asks each member of
the server cluster to reply. Replies
provide the responder's own assigned rank
and identity.

### II. Election 

Election are computed locally, after accumulating
responses.

After issuing an allcall, the server
listens for a heartbeat interval.

At the end of the interval, it sorts
the respondents by rank. Since
the replies are on a broadcast
channel, more than one simultaneous
allcall reply may be incorporated into
the set of respondents.

The lowest ranking server is elected
as leader.

## Safety/Convergence: ALLCALL converges to one leader

Suppose two nodes are partitioned and so both are leaders on
their own side of the network. Then suppose the network
is joined again, so the two leaders are brought together
by a healing of the network, or by adding a new link
between the networks. The two nodes exchange Ids and
ranks via responding to allcalls, and compute
who is the new leader.

Hence the two leader situation persists for at
most one heartbeat term after the network join.

## Liveness: a leader will be chosen

Given the total order among nodes, exactly one
will be lowest rank and thus be the preferred
leader. If the leader fails, the next
heartbeat of allcall will omit that
server from the candidate list, and
the next ranking server will be chosen.

Hence, with at least one live
node, the system can run for at most one
heartbeat term before electing a leader.
Since there is a total order on
all live (non-failed) servers, only
one will be chosen.

## commentary

ALLCALL does not guarantee that there will
never be more than one leader. Availability
in the face of network partition is
desirable in many cases, and ALLCALL is
appropriate for these. This is congruent
with Nats design as an always-on system.

ALLCALL does not guarantee that a
leader will always be present, but
with live nodes it does provide
that the cluster will have a leader
after one heartbeat term has
been initiated and completed.

By design, ALLCALL functions well
in a cluster with any number of nodes.
One and two nodes, or an even number
of nodes, will work just fine.

Compared to quorum based elections
like raft and paxos, where an odd
number of at least three
nodes is required to make progress,
this can be very desirable.

ALLCALL is appropriate for AP,
rather than CP, style systems, where
availability is more important
than having a single writer. When
writes are idempotent or deduplicated
downstream, this is typically preferred.

prior art
----------

ALLCALL is a simplified version of
the well known Bully Algorithm[1][2]
for election in a distributed system
of arbitrary graph.

ALLCALL does less bullying, and lets
nodes arrive at their own conclusions.
The essential broadcast and ranking
mechanism, however, is identical.

[1] Hector Garcia-Molina, Elections in a
Distributed Computing System, IEEE
Transactions on Computers,
Vol. C-31, No. 1, January (1982) 48–59

[2] https://en.wikipedia.org/wiki/Bully_algorithm

implementation
------------

ALLCALL is implented on top of
the Nats (https://nats.io) system, see the health/
subdirectory of

https://github.com/nats-io/gnatsd

