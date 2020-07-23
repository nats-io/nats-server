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

More specifically from [rfc822](https://www.ietf.org/rfc/rfc822.txt) Section 3.1.2:
> Once a field has been unfolded, it may be viewed as being composed of a field-name followed by a colon (":"), followed by a field-body, and  terminated  by  a  carriage-return/line-feed.
> The  field-name must be composed of printable ASCII characters (i.e., characters that  have  values  between  33.  and  126., decimal, except colon).  The field-body may be composed of any ASCII characters, except CR or LF.  (While CR and/or LF may be present  in the actual text, they are removed by the action of unfolding the field.)

The reference NATS Go client uses the `http.Header` infrastructure for serializing NATS headers. Other language clients may not be able to reuse header serialization infrastructure outside of the context of an HTTP request. Furthermore, the implementation of the native header library may diverge from the Go `http.Header` while still being standard.

For example, the Go `http.Header` library, performs the following formatting which while not part of the spec, as header field names are case-insensitive, become a requirement in NATS header fields:
- First character in a header field is capitalized if in the range of [a-z]
- First characters following a dash (`-`) are capitalized if in the range of [a-z]

_If a client doesn't serialize field names as above, it is possible that the server may ignore them._

NATS Headers:

The only difference between a NATS header and HTTP is the first line. Instead of an HTTP method followed by a resource and the HTTP version (`GET / HTTP/1.1`), NATS will provide a string identifying the header version (`NATS/X.x`), currently 1.0, so it is rendered as `NATS/1.0␍␊`.

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
HPUB SUBJECT REPLY 23 30␍␊NATS/1.0␍␊Header: X␍␊␍␊PAYLOAD␍␊
HPUB SUBJECT REPLY 23 23␍␊NATS/1.0␍␊Header: X␍␊␍␊␍␊

HPUB <SUBJ> [REPLY] <HDR_LEN> <TOT_LEN>
<HEADER><PAYLOAD>

```

#### NOTES:

- `HDR_LEN` includes the entire serialized header
- `TOT_LEN` includes the entire payload length including the header

### MSG with Headers

Clients will see `HMSG` protocol lines for `MSG`s that contain headers

```
HMSG SUBJECT 1 REPLY 23 30␍␊NATS/1.0␍␊Header: X␍␊␍␊PAYLOAD␍␊
HPUB SUBJECT 1 REPLY 23 23␍␊NATS/1.0␍␊Header: X␍␊␍␊␍␊

HMSG <SUBJECT> <SID> [REPLY] <HDR_LEN> <TOT_LEN>
<PAYLOAD>
```

- `HDR_LEN` includes the entire header
- `TOT_LEN` includes the entire payload length including the header


## Decision

Implemented and merged to master.

## Consequences

Use of headers is possible.
