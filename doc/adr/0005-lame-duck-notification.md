# 5. lame-duck-notification

Date: 2020-07-20

## Status

Accepted

## Context

This document describes the _Lame Duck Mode_ server notification. When a server enters lame duck mode, it removes itself from being advertised in the cluster, and slowly starts evicting connected clients as per  [`lame_duck_duration`](https://docs.nats.io/nats-server/configuration#runtime-configuration). This document describes how this information is notified
to the client, in order to allow clients to cooperate and initiate an orderly migration to a different server in the cluster.


## Decision

The server notififies that it has entered _lame duck mode_ by sending an [`INFO`](https://docs.nats.io/nats-protocol/nats-protocol#info) update. If the `ldm` property is set to true, the server has entered _lame_duck_mode_ and the client should initiate an orderly self-disconnect or close. Note the `ldm` property is only available on servers that implement the notification feature.

## Consequences

By becoming aware of a server changing state to _lame duck mode_ clients can orderly disconnect from a server, and connect to a different server. Currently clients have no automatic support to _disconnect_ while keeping current state. Future documentation will describe strategies for initiating a new connection and exiting the old one.
