# 9. js-idle-heartbeat

Date: 2021-06-30

## Status

Accepted

## Context

The JetStream ConsumerConfig option `idle_heartbeat` enables server-side
heartbeats to be sent to the client. To enable the option on the consumer simply
specify it with a value representing the number of nanoseconds that the server
should use as notification interval.

The server will only notify after the specified interval has elapsed and no new
messages have been delivered to the consumer. Delivering a message to the
consumer resets the interval.

The idle heartbeats notifications are sent to the consumer's subscription as a
regular NATS message. The message will have a `code` of `100` with a
`description` of `Idle Heartbeat`. The message will contain additional headers
that the client can use to re-affirm that it has not lost any messages:

- `Nats-Last-Consumer` indicates the last consumer sequence delivered to the
  client. If `0`, no messages have been delivered.
- `Nats-Last-Stream` indicates the sequence number of the newest message in the
  stream.

Here's an example of a client creating a consumer with an idle_heartbeat of 10
seconds, followed by a server notification.

```
$JS.API.CONSUMER.CREATE.FRHZZ447RL7NR8TAICHCZ6 _INBOX.FRHZZ447RL7NR8TAICHCQ8.FRHZZ447RL7NR8TAICHDQ0 136␍␊
{"config":{"ack_policy":"explicit","deliver_subject":"my.messages","idle_heartbeat":10000000000},
"stream_name":"FRHZZ447RL7NR8TAICHCZ6"}␍␊
...

> HMSG my.messages 2  75 75␍␊NATS/1.0 100 Idle Heartbeat␍␊Nats-Last-Consumer: 0␍␊Nats-Last-Stream: 0␍␊␍␊␍␊
alive - last stream seq: 0 - last consumer seq: 0
```

This feature is intended as an aid to clients to detect when they have been
disconnected. Without it the consumer's subscription may sit idly waiting for
messages, without knowing that the server might have simply gone away and
recovered elsewhere.

## Consequences

Clients can use this information to set client-side timers that track how many
heartbeats have been missed and perhaps take some action such as re-create a
subscription to resume messages.
