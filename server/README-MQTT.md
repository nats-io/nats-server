**MQTT Implementation Overview**

Revision 1.1

Authors: Ivan Kozlovic, Lev Brouk

NATS Server currently supports most of MQTT 3.1.1. This document describes how
it is implementated.

It is strongly recommended to review the [MQTT v3.1.1
specifications](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
and get a detailed understanding before proceeding with this document.

# Contents

1. [Concepts](#1-concepts)
   - [Server, client](#server-client)
   - [Connection, client ID, session](#connection-client-id-session)
   - [Packets, messages, and subscriptions](#packets-messages-and-subscriptions)
   - [Quality of Service (QoS), publish identifier (PI)](#quality-of-service-qos-publish-identifier-pi)
   - [Retained message](#retained-message)
   - [Will message](#will-message)
2. [Use of JetStream](#2-use-of-jetstream)
   - [JetStream API](#jetstream-api)
   - [Streams](#streams)
   - [Consumers and Internal NATS Subscriptions](#consumers-and-internal-nats-subscriptions)
3. [Lifecycles](#3-lifecycles)
   - [Connection, Session](#connection-session)
   - [Subscription](#subscription)
   - [Message](#message)
   - [Retained messages](#retained-messages)
4. [Implementation Notes](#4-implementation-notes)
   - [Hooking into NATS I/O](#hooking-into-nats-io)
   - [Session Management](#session-management)
   - [Processing QoS acks: PUBACK, PUBREC, PUBCOMP](#processing-qos-acks-puback-pubrec-pubcomp)
   - [Subject Wildcards](#subject-wildcards)
5. [Known issues](#5-known-issues)

# 1. Concepts

## Server, client

In the MQTT specification there are concepts of **Client** and **Server**, used
somewhat interchangeably with those of **Sender** and **Receiver**. A **Server**
acts as a **Receiver** when it gets `PUBLISH` messages from a **Sender**
**Client**, and acts as a **Sender** when it delivers them to subscribed
**Clients**.

In the NATS server implementation there are also concepts (types) `server` and
`client`. `client` is an internal representation of a (connected) client and
runs its own read and write loops. Both of these have an `mqtt` field that if
set makes them behave as MQTT-compliant.

The code and comments may sometimes be confusing as they refer to `server` and
`client` sometimes ambiguously between MQTT and NATS.

## Connection, client ID, session

When an MQTT client connects to a server, it must send a `CONNECT` packet to
create an **MQTT Connection**. The packet must include a **Client Identifier**.
The server will then create or load a previously saved **Session** for the (hash
of) the client ID.

## Packets, messages, and subscriptions

The low level unit of transmission in MQTT is a **Packet**. Examples of packets
are: `CONNECT`, `SUBSCRIBE`, `SUBACK`, `PUBLISH`, `PUBCOMP`, etc.

An **MQTT Message** starts with a `PUBLISH` packet that a client sends to the
server. It is then matched against the current **MQTT Subscriptions** and is
delivered to them as appropriate. During the message delivery the server acts as
an MQTT client, and the receiver acts as an MQTT server.

Internally we use **NATS Messages** and **NATS Subscriptions** to facilitate
message delivery. This may be somewhat confusing as the code refers to `msg` and
`sub`. What may be even more confusing is that some MQTT packets (specifically,
`PUBREL`) are represented as NATS messages, and that the original MQTT packet
"metadata" may be encoded as NATS message headers.

## Quality of Service (QoS), publish identifier (PI)

MQTT specifies 3 levels of quality of service (**QoS**):

- `0` for at most once. A single delivery attempt.
- `1` for at least once. Will try to redeliver until acknowledged by the
  receiver.
- `2` for exactly once. See the [SPEC REF] for the acknowledgement flow.

QoS 1 and 2 messages need to be identified with publish identifiers (**PI**s). A
PI is a 16-bit integer that must uniquely identify a message for the duration of
the required exchange of acknowledgment packets.

Note that the QoS applies separately to the transmission of a message from a
sender client to the server, and from the server to the receiving client. There
is no protocol-level acknowledgements between the receiver and the original
sender. The sender passes the ownership of messages to the server, and the
server then delivers them at maximum possible QoS to the receivers
(subscribers). The PIs for in-flight outgoing messages are issued and stored per
session.

## Retained message

A **Retained Message** is not part of any MQTT session and is not removed when the
session that produced it goes away. Instead, the server needs to persist a
_single_ retained message per topic. When a subscription is started, the server
needs to send the “matching” retained messages, that is, messages that would
have been delivered to the new subscription should that subscription had been
running prior to the publication of this message.

Retained messages are removed when the server receives a retained message with
an empty body. Still, this retained message that serves as a “delete” of a
retained message will be processed as a normal published message.

Retained messages can have QoS.

## Will message

The `CONNECT` packet can contain information about a **Will Message** that needs to
be sent to any client subscribing on the Will topic/subject in the event that
the client is disconnected implicitly, that is, not as a result as the client
sending the `DISCONNECT` packet.

Will messages can have the retain flag and QoS.

# 2. Use of JetStream

The MQTT implementation relies heavily on JetStream. We use it to:

- Persist (and restore) the [Session](#connection-client-id-session) state.
- Store and retrieve [Retained messages](#retained-message).
- Persist incoming [QoS 1 and
  2](#quality-of-service-qos-publish-identifier-pi) messages, and
  re-deliver if needed.
- Store and de-duplicate incoming [QoS
  2](#quality-of-service-qos-publish-identifier-pi) messages.
- Persist and re-deliver outgoing [QoS
  2](#quality-of-service-qos-publish-identifier-pi) `PUBREL` packets.

Here is the overview of how we set up and use JetStream **streams**,
**consumers**, and **internal NATS subscriptions**.

## JetStream API

All interactions with JetStream are performed via `mqttJSA` that sends NATS
requests to JetStream. Most are processed synchronously and await a response,
some (e.g. `jsa.sendAck()`) are sent asynchronously. JetStream API is usually
referred to as `jsa` in the code. No special locking is required to use `jsa`,
however the asynchronous use of JetStream may create race conditions with
delivery callbacks.

## Streams

We create the following streams unless they already exist. Failing to ensure the
streams would prevent the client from connecting.

Each stream is created with a replica value that is determined by the size of
the cluster but limited to 3. It can also be overwritten by the stream_replicas
option in the MQTT configuration block.

The streams are created the first time an Account Session Manager is initialized
and are used by all sessions in it. Note that to avoid race conditions, some
subscriptions are created first. The streams are never deleted. See
`mqttCreateAccountSessionManager()` for details.

1. `$MQTT_sess` stores persisted **Session** records. It filters on
   `"$MQTT.sess.>` subject and has a “limits” policy with `MaxMsgsPer` setting
   of 1.
2. `$MQTT_msgs` is used for **QoS 1 and 2 message delivery**.
   It filters on `$MQTT.msgs.>` subject and has an “interest” policy.
3. `$MQTT_rmsgs` stores **Retained Messages**. They are all
   stored (and filtered) on a single subject `$MQTT.rmsg`. This stream has a
   limits policy.
4. `$MQTT_qos2in` stores and deduplicates **Incoming QoS 2 Messages**. It
   filters on `$MQTT.qos2.in.>` and has a "limits" policy with `MaxMsgsPer` of
   1.
5. `$MQTT_out` stores **Outgoing QoS 2** `PUBREL` packets. It filters on
   `$MQTT.out.>` and has a "interest" retention policy.

## Consumers and Internal NATS Subscriptions

### Account Scope

- A durable consumer for [Retained Messages](#retained-message) -
  `$MQTT_rmsgs_<server name hash>`
- A subscription to handle all [jsa](#jetstream-api) replies for the account.
- A subscription to replies to "session persist" requests, so that we can detect
  the use of a session with the same client ID anywhere in the cluster.
- 2 subscriptions to support [retained messages](#retained-message):
  `$MQTT.sub.<nuid>` for the messages themselves, and one to receive replies to
  "delete retained message" JS API (on the JS reply subject var).

### Session Scope

When a new QoS 2 MQTT subscription is detected in a session, we ensure that
there is a durable consumer for [QoS
2](#quality-of-service-qos-publish-identifier-pi) `PUBREL`s out for delivery -
`$MQTT_PUBREL_<session id hash>`

### Subscription Scope

For all MQTT subscriptions, regardless of their QoS, we create internal NATS subscriptions to

- `subject` (directly encoded from `topic`). This subscription is used to
  deliver QoS 0 messages, and messages originating from NATS.
- if needed, `subject fwc` complements `subject` for topics like `topic.#` to
  include `topic` itself, see [top-level wildcards](#subject-wildcards)

For QoS 1 or 2 MQTT subscriptions we ensure:

- A durable consumer for messages out for delivery - `<session ID hash>_<nuid>`
- An internal subscription to `$MQTT.sub.<nuid>` to deliver the messages to the
  receiving client.

### (Old) Notes

As indicated before, for a QoS1 or QoS2 subscription, the server will create a
JetStream consumer with the appropriate subject filter. If the subscription
already existed, then only the NATS subscription is created for the JetStream
consumer’s delivery subject.

Note that JS consumers can be created with an “Replicas” override, which from
recent discussion is problematic with “Interest” policy streams, which
“$MQTT_msgs” is.

We do handle situations where a subscription on the same subject filter is sent
with a different QoS as per MQTT specifications. If the existing was on QoS 1 or
2, and the “new” is for QoS 0, then we delete the existing JS consumer.

Subscriptions that are QoS 0 have a NATS subscription with the callback function
being `mqttDeliverMsgCbQos0()`; while QoS 1 and 2 have a NATS subscription with
callback `mqttDeliverMsgCbQos12()`. Both those functions have comments that
describe the reason for their existence and what they are doing. For instance
the `mqttDeliverMsgCbQos0()` callback will reject any producing client that is
of type JETSTREAM, so that it handles only non JetStream (QoS 1 and 2) messages.

Both these functions end-up calling mqttDeliver() which will first enqueue the
possible retained messages buffer before delivering any new message. The message
itself being delivered is serialized in MQTT format and enqueued to the client’s
outbound buffer and call to addToPCD is made so that it is flushed out of the
readloop.

# 3. Lifecycles

## Connection, Session

An MQTT connection is created when a listening MQTT server receives a `CONNECT`
packet. See `mqttProcessConnect()`. A connection is associated with a session.
Steps:

1. Ensure that we have an `AccountSessionManager` so we can have an
   `mqttSession`. Lazily initialize JetStream streams, and internal consumers
   and subscriptions. See `getOrCreateMQTTAccountSessionManager()`.
2. Find and disconnect any previous session/client for the same ID. See
   `mqttProcessConnect()`.
3. Ensure we have an `mqttSession` - create a new or load a previously persisted
   one. If the clean flag is set in `CONNECT`, clean the session. see
   `mqttSession.clear()`
4. Initialize session's subscriptions, if any.
5. Always send back a `CONNACK` packet. If there were errors in previous steps,
   include the error.

An MQTT connection can be closed for a number of reasons, including receiving a
`DISCONNECT` from the client, explicit internal errors processing MQTT packets,
or the server receiving another `CONNECT` packet with the same client ID. See
`mqttHandleClosedClient()` and `mqttHandleWill()`. Steps:

1. Send out the Will Message if applicable (if not caused by a `DISCONNECT` packet)
2. Delete the JetStream consumers for to QoS 1 and 2 packet delivery through
   JS API calls (if "clean" session flag is set)
3. Delete the session record from the “$MQTT_sess” stream, based on recorded
   stream sequence. (if "clean" session flag is set)
4. Close the client connection.

On an explicit disconnect, that is, the client sends the DISCONNECT packet, the
server will NOT send the Will, as per specifications.

For sessions that had the “clean” flag, the JS consumers corresponding to QoS 1
subscriptions are deleted through JS API calls, the session record is then
deleted (based on recorded stream sequence) from the “$MQTT_sess” stream.

Finally, the client connection is closed

Sessions are persisted on disconnect, and on subscriptions changes.

## Subscription

Receiving an MQTT `SUBSCRIBE` packet creates new subscriptions, or updates
existing subscriptions in a session. Each `SUBSCRIBE` packet may contain several
specific subscriptions (`topic` + QoS in each). We always respond with a
`SUBACK`, which may indicate which subscriptions errored out.

For each subscription in the packet, we:

1. Ignore it if `topic` starts with `$MQTT.sub.`.
2. Set up QoS 0 message delivery - an internal NATS subscription on `topic`.
3. Replay any retained messages for `topic`, once as QoS 0.
4. If we already have a subscription on `topic`, update its QoS
5. If this is a QoS 2 subscription in the session, ensure we have the [PUBREL
   consumer](#session-scope) for the session.
6. If this is a QoS 1 or 2 subscription, ensure we have the [Message
   consumer](#subscription-scope) for this subscription (or delete one if it
   exists and this is now a QoS 0 sub).
7. Add an extra subscription for the [top-level wildcard](#subject-wildcards) case.
8. Update the session, persist it if changed.

When a session is restored (no clean flag), we go through the same steps to
re-subscribe to its stored subscription, except step #8 which would have been
redundant.

When we get an `UNSUBSCRIBE` packet, it can contain multiple subscriptions to
unsubscribe. The parsing will generate a slice of mqttFilter objects that
contain the “filter” (the topic with possibly wildcard of the subscription) and
the QoS value. The server goes through the list and deletes the JS consumer (if
QoS 1 or 2) and unsubscribes the NATS subscription for the delivery subject (if
it was a QoS 1 or 2) or on the actual topic/subject. In case of the “#”
wildcard, the server will handle the “level up” subscriptions that NATS had to
create.

Again, we update the session and persist it as needed in the `$MQTT_sess`
stream.

## Message

1. Detect an incoming PUBLISH packet, parse and check the message QoS. Fill out
   the session's `mqttPublish` struct that contains information about the
   published message. (see `mqttParse()`, `mqttParsePub()`)
2. Process the message according to its QoS (see `mqttProcessPub()`)

   - QoS 0:
     - Initiate message delivery
   - QoS 1:
     - Initiate message delivery
     - Send back a `PUBACK`
   - QoS 2:
     - Store the message in `$MQTT_qos2in` stream, using a PI-specific subject.
       Since `MaxMsgsPer` is set to 1, we will ignore duplicates on the PI.
     - Send back a `PUBREC`
     - "Wait" for a `PUBREL`, then initiate message delivery
     - Remove the previously stored QoS2 message
     - Send back a `PUBCOMP`

3. Initiate message delivery (see `mqttInitiateMsgDelivery()`)

   - Convert the MQTT `topic` into a NATS `subject` using
     `mqttTopicToNATSPubSubject()` function. If there is a known subject
     mapping, then we select the new subject using `selectMappedSubject()`
     function and then convert back this subject into an MQTT topic using
     `natsSubjectToMQTTTopic()` function.
   - Re-serialize the `PUBLISH` packet received as a NATS message. Use NATS
     headers for the metadata, and the deliverable MQTT `PUBLISH` packet as the
     contents.
   - Publish the messages as `subject` (and `subject fwc` if applicable, see
     [subject wildcards](#subject-wildcards)). Use the "standard" NATS
     `c.processInboundClientMsg()` to do that. `processInboundClientMsg()` will
     distribute the message to any NATS subscriptions (including routes,
     gateways, leafnodes) and the relevant MQTT subscriptions.
   - Check for retained messages, process as needed. See
     `c.processInboundClientMsg()` calling `c.mqttHandlePubRetain()` For MQTT
     clients.
   - If the message QoS is 1 or 2, store it in `$MQTT_msgs` stream as
     `$MQTT.msgs.<subject>` for "at least once" delivery with retries.

4. Let NATS and JetStream deliver to the internal subscriptions, and to the
   receiving clients. See `mqttDeliverMsgCb...()`

   - The NATS message posted to `subject` (and `subject fwc`) will be delivered
     to each relevant internal subscription by calling `mqttDeliverMsgCbQoS0()`.
     The function has access to both the publishing and the receiving clients.

     - Ignore all irrelevant invocations. Specifically, do nothing if the
       message needs to be delivered with a higher QoS - that will be handled by
       the other, `...QoS12` callback. Note that if the original message was
       publuished with a QoS 1 or 2, but the subscription has its maximum QoS
       set to 0, the message will be delivered by this callback.
     - Ignore "reserved" subscriptions, as per MQTT spec.
     - Decode delivery `topic` from the NATS `subject`.
     - Write (enqueue) outgoing `PUBLISH` packet.
     - **DONE for QoS 0**

   - The NATS message posted to JetStream as `$MQTT.msgs.subject` will be
     consumed by subscription-specific consumers. Note that MQTT subscriptions
     with max QoS 0 do not have JetStream consumers. They are handled by the
     QoS0 callback.

     The consumers will deliver it to the `$MQTT.sub.<nuid>`
     subject for their respective NATS subscriptions by calling
     `mqttDeliverMsgCbQoS12()`. This callback too has access to both the
     publishing and the receiving clients.

     - Ignore "reserved" subscriptions, as per MQTT spec.
     - See if this is a re-delivery from JetStream by checking `sess.cpending`
       for the JS reply subject. If so, use the existing PI and treat this as a
       duplicate redelivery.
     - Otherwise, assign the message a new PI (see `trackPublish()` and
       `bumpPI()`) and store it in `sess.cpending` and `sess.pendingPublish`,
       along with the JS reply subject that can be used to remove this pending
       message from the consumer once it's delivered to the receipient.
     - Decode delivery `topic` from the NATS `subject`.
     - Write (enqueue) outgoing `PUBLISH` packet.

5. QoS 1: "Wait" for a `PUBACK`. See `mqttProcessPubAck()`.

   - When received, remove the PI from the tracking maps, send an ACK to
     consumer to remove the message.
   - **DONE for QoS 1**

6. QoS 2: "Wait" for a `PUBREC`. When received, we need to do all the same
   things as in the QoS 1 `PUBACK` case, but we need to send out a `PUBREL`, and
   continue using the same PI until the delivery flow is complete and we get
   back a `PUBCOMP`. For that, we add the PI to `sess.pendingPubRel`, and to
   `sess.cpending` with the PubRel consumer durable name.

   We also compose and store a headers-only NATS message signifying a `PUBREL`
   out for delivery, and store it in the `$MQTT_qos2out` stream, as
   `$MQTT.qos2.out.<session-id>`.

7. QoS 2: Deliver `PUBREL`. The PubRel session-specific consumer will publish to
   internal subscription on `$MQTT.qos2.delivery`, calling
   `mqttDeliverPubRelCb()`. We store the ACK reply subject in `cpending` to
   remove the JS message on `PUBCOMP`, compose and send out a `PUBREL` packet.

8. QoS 2: "Wait" for a `PUBCOMP`. See `mqttProcessPubComp()`.
   - When received, remove the PI from the tracking maps, send an ACK to
     consumer to remove the `PUBREL` message.
   - **DONE for QoS 2**

## Retained messages

When we process an inbound `PUBLISH` and submit it to
`processInboundClientMsg()` function, for MQTT clients it will invoke
`mqttHandlePubRetain()` which checks if the published message is “retained” or
not.

If it is, then we construct a record representing the retained message and store
it in the `$MQTT_rmsg` stream, under the single `$MQTT.rmsg` subject. The stored
record (in JSON) contains information about the subject, topic, MQTT flags, user
that produced this message and the message content itself. It is stored and the
stream sequence is remembered in the memory structure that contains retained
messages.

Note that when creating an account session manager, the retained messages stream
is read from scratch to load all the messages through the use of a JS consumer.
The associated subscription will process the recovered retained messages or any
new that comes from the network.

A retained message is added to a map and a subscription is created and inserted
into a sublist that will be used to perform a ReverseMatch() when a subscription
is started and we want to find all retained messages that the subscription would
have received if it had been running prior to the message being published.

If a retained message on topic “foo” already exists, then the server has to
delete the old message at the stream sequence we saved when storing it.

This could have been done with having retained messages stored under
`$MQTT.rmsg.<subject>` as opposed to all under a single subject, and make use of
the `MaxMsgsPer` field set to 1. The `MaxMsgsPer` option was introduced well into
the availability of MQTT and changes to the sessions was made in [PR
#2501](https://github.com/nats-io/nats-server/pull/2501), with a conversion of
existing streams such as `$MQTT*sess*<sess ID>` into a single stream with unique
subjects, but the changes were not made to the retained messages stream.

There are also subscriptions for the handling of retained messages which are
messages that are asked by the publisher to be retained by the MQTT server to be
delivered to matching subscriptions when they start. There is a single message
per topic. Retained messages are deleted when the user sends a retained message
(there is a flag in the PUBLISH protocol) on a given topic with an empty body.
The difficulty with retained messages is to handle them in a cluster since all
servers need to be aware of their presence so that they can deliver them to
subscriptions that those servers may become the leader for.

- `$MQTT_rmsgs` which has a “limits” policy and holds retained messages, all
  under `$MQTT.rmsg` single subject. Not sure why I did not use MaxMsgsPer for
  this stream and not filter `$MQTT.rmsg.>`.

The first step when processing a new subscription is to gather the retained
messages that would be a match for this subscription. To do so, the server will
serialize into a buffer all messages for the account session manager’s sublist’s
ReverseMatch result. We use the returned subscriptions’ subject to find from a
map appropriate retained message (see `serializeRetainedMsgsForSub()` for
details).

# 4. Implementation Notes

## Hooking into NATS I/O

### Starting the accept loop

The MQTT accept loop is started when the server detects that an MQTT port has
been defined in the configuration file. It works similarly to all other accept
loops. Note that for MQTT over websocket, the websocket port has to be defined
and MQTT clients will connect to that port instead of the MQTT port and need to
provide `/mqtt` as part of the URL to redirect the creation of the client to an
MQTT client (with websocket support) instead of a regular NATS with websocket.
See the branching done in `startWebsocketServer()`. See `startMQTT()`.

### Starting the read/write loops

When a TCP connection is accepted, the internal go routine will invoke
`createMQTTClient()`. This function will set a `c.mqtt` object that will make it
become an MQTT client (through the `isMqtt()` helper function). The `readLoop()`
and `writeLoop()` are started similarly to other clients. However, the read loop
will branch out to `mqttParse()` instead when detecting that this is an MQTT
client.

## Session Management

### Account Session Manager

`mqttAccountSessionManager` is an object that holds the state of all sessions in
an account. It also manages the lifecycle of JetStream streams and internal
subscriptions for processing JS API replies, session updates, etc. See
`mqttCreateAccountSessionManager()`. It is lazily initialized upon the first
MQTT `CONNECT` packet received. Account session manager is referred to as `asm`
in the code.

Note that creating the account session manager (and attempting to create the
streams) is done only once per account on a given server, since once created the
account session manager for a given account would be found in the sessions map
of the mqttSessionManager object.

### Find and disconnect previous session/client

Once all that is done, we now go to the creation of the session object itself.
For that, we first need to make sure that it does not already exist, meaning
that it is registered on the server - or anywhere in the cluster. Note that MQTT
dictates that if a session with the same ID connects, the OLD session needs to
be closed, not the new one being created. NATS Server complies with this
requirement.

Once a session is detected to already exists, the old one (as described above)
is closed and the new one accepted, however, the session ID is maintained in a
flappers map so that we detect situations where sessions with the same ID are
started multiple times causing the previous one to be closed. When that
detection occurs, the newly created session is put in “jail” for a second to
avoid a very rapid succession of connect/disconnect. This has already been seen
by users since there was some issue there where we would schedule the connection
closed instead of waiting in place which was causing a panic.

We also protect from multiple clients on a given server trying to connect with
the same ID at the “same time” while the processing of a CONNECT of a session is
not yet finished. This is done with the use of a sessLocked map, keyed by the
session ID.

### Create or restore the session

If everything is good up to that point, the server will either create or restore
a session from the stream. This is done in the `createOrRestoreSession()`
function. The client/session ID is hashed and added to the session’s stream
subject along with the JS domain to prevent clients connecting from different
domains to “pollute” the session stream of a given domain.

Since each session constitutes a subject and the stream has a maximum of 1
message per subject, we attempt to load the last message on the formed subject.
If we don’t find it, then the session object is created “empty”, while if we
find a record, we create the session object based on the record persisted on the
stream.

If the session was restored from the JS stream, we keep track of the stream
sequence where the record was located. When we save the session (even if it
already exists) we will use this sequence number to set the
`JSExpectedLastSubjSeq` header so that we handle possibly different servers in a
(super)cluster to detect the race of clients trying to use the same session ID,
since only one of the write should succeed. On success, the session’s new
sequence is remembered by the server that did the write.

When created or restored, the CONNACK can now be sent back to the client, and if
there were any recovered subscriptions, they are now processed.

## Processing QoS acks: PUBACK, PUBREC, PUBCOMP

When the server delivers a message with QoS 1 or 2 (also a `PUBREL` for QoS 2) to a subscribed client, the client will send back an acknowledgement. See `mqttProcessPubAck()`, `mqttProcessPubRec()`, and `mqttProcessPubComp()`

While the specific logic for each packet differs, these handlers all update the
session's PI mappings (`cpending`, `pendingPublish`, `pendingPubRel`), and if
needed send an ACK to JetStream to remove the message from its consumer and stop
the re-delivery attempts.

## Subject Wildcards

Note that MQTT subscriptions have wildcards too, the `“+”` wildcard is equivalent
to NATS’s `“*”` wildcard, however, MQTT’s wildcard `“#”` is similar to `“>”`, except
that it also includes the level above. That is, a subscription on `“foo/#”` would
receive messages on `“foo/bar/baz”`, but also on `“foo”`.

So, for MQTT subscriptions enging with a `'#'` we are forced to create 2
internal NATS subscriptions, one on `“foo”` and one on `“foo.>”`.

# 5. Known issues
- "active" redelivery for QoS from JetStream (compliant, just a note)
- JetStream QoS redelivery happens out of (original) order
- finish delivery of in-flight messages after UNSUB
- finish delivery of in-flight messages after a reconnect
- consider replacing `$MQTT_msgs` with `$MQTT_out`.
- consider using unique `$MQTT.rmsg.>` and `MaxMsgsPer` for retained messages.
- add a cli command to list/clean old sessions
