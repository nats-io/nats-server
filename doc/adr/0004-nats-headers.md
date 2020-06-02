# 4. nats-headers

Date: 2020-05-28

## Status

Accepted

## Context

This document describes NATS Headers from the perspective of clients. NATS headers allow clients to specify additional meta-data in the form of headers. The headers are effectively [HTTP Headers](https://tools.ietf.org/html/rfc7230#section-3.2). The reference Go implementation internally uses `http.Header` for their serialization and parsing.

The salient points of the HTTP header specification are:

- Each header field consists of a case-insensitive field name followed by a colon (`:`), optional leading whitespace, the field value, and optional trailing whitespace.
- No spaces are allowed between the header field name and colon.
- Field value may be preceded or followed by optional whitespace.
- The specification may allow any number of strange things like comments/tokens etc.
- The keys can repeat.

The reference NATS Go client uses the `http.Header` infrastructure for serializing NATS headers. Other language clients may not be able to reuse header serialization infrastructure outside of the context of an HTTP request. Furthermore, the implementation of the native header library may diverge from the Go `http.Header` while still being standard. 

NATS Headers:

The only difference between a NATS header and HTTP is the first line. Instead of an HTTP method followed by a resource and the HTTP version (`GET / HTTP/1.1`), NATS will provide a string identifying the header version (`NATS/X.x\r\n`), currently 1.0, so it is rendered as `NATS/1.0\r\n`.

Please refer to the [specification](https://tools.ietf.org/html/rfc7230#section-3.2) for information on how to encode/decode HTTP headers, and the [Go implementation](https://golang.org/src/net/http/header.go).


### Enabling Message Headers

The server that is able to send and receive headers will specify so in it's [`INFO`](https://docs.nats.io/nats-protocol/nats-protocol#info) protocol message. The `headers` field if present, will have a boolean value. If the client wishes to send headers, it has to enable it must add a `headers` field with the `true` value in its [`CONNECT` message](https://docs.nats.io/nats-protocol/nats-protocol#connect):

```
"lang": "node",
"version": "1.2.3",
"protocol": 1,
"headers": true,
...
```

### Publishing Messages With A Header


Messages that include a header have a `HPUB` protocol:

```
HPUB SUBJECT REPLY 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n
HPUB SUBJECT REPLY 23 23\r\nNATS/1.0\r\nHeader: X\r\n\r\n\r\n

HPUB <SUBJ> [REPLY] <HDR_LEN> <TOT_LEN>
<HEADER><PAYLOAD>

```

#### NOTES:

- `HDR_LEN` includes the entire serialized header
- `TOT_LEN` includes the entire payload length including the header

### MSG with Headers

Clients will see `HMSG` protocol lines for `MSG`s that contain headers

```
HMSG SUBJECT 1 REPLY 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n
HPUB SUBJECT 1 REPLY 23 23\r\nNATS/1.0\r\nHeader: X\r\n\r\n\r\n

HMSG <SUBJECT> <SID> [REPLY] <HDR_LEN> <TOT_LEN>
<PAYLOAD>
```

- `HDR_LEN` includes the entire header
- `TOT_LEN` includes the entire payload length including the header


## Decision

Implemented and merged to master.

## Consequences

Use of headers is possible.
