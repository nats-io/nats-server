# 3. nats-headers

Date: 2020-05-28

## Status

Accepted

## Context

This document describes NATS Headers from the perspective of clients. NATS headers allow clients to specify additional metadata in the form of headers. The headers borrow strongly from [HTTP Headers](https://tools.ietf.org/html/rfc7230#section-3.2).

The salient points of the HTTP header specification are:

- Each header field consists of a case-insensitive field name followed by a colon (":"), optional leading whitespace, the field value, and optional trailing whitespace.
- No spaces are allowed between the header field name and colon.
- Field value may be preceeded or followed by optional whitespace.
- The specification may allow any number of strange things like comments/tokens etc.
- The keys can repeat.

The reference NATS Go client repurposes the `http.Header` infrastructure for serializing NATS headers. Other language clients may not be able to reuse header infrastructure outside of the context of HTTP. Furthermore, the implementation of the header library may diverge from what is allowed by the Go `http.Header`. 

NATS Headers:

- First line is `NATS/X.x<CRLF>`
- Each header key is separated from its value by a ': ' `<COLON><SPACE>`
- Headers are formatted as MIMEHeaders. 
- Keys are case-insensitive even if formatted otherwise.
- Value is ASCII and may not contains line folding (`\n`, etc)
- Value is terminated by a `<CRLF>`
- Final value adds an extra `<CRLF>`

### Publishing Messages With A Header

_For purposes of easily counting characters on these examples, `<CRLF>` sequences are encoded as `rn`._
Messages that include a header have a `HPUB` protocol:

```
HPUB SUBJECT REPLY 23 30rnNATS/1.0rnHeader: XrnrnPAYLOADrn
HPUB SUBJECT REPLY 23 23rnNATS/1.0rnHeader: Xrnrnrn

HPUB <SUBJ> [REPLY] <HDR_LEN> <TOT_LEN><CRLF><HEADER><PAYLOAD><CRLF>
```

#### NOTES:

- `HEADER_LEN` includes the entire serialized header including <CRLF>s
- `TOT_LEN` includes the entire payload length including the header

### MSG with Headers

Clients will see `HMSG` protocol lines for `MSG`s that contain headers

```
HMSG SUBJECT 1 REPLY 23 30rnNATS/1.0rnHeader: XrnrnPAYLOADrn
HPUB SUBJECT 1 REPLY 23 23rnNATS/1.0rnHeader: Xrnrnrn

HMSG <SUBJECT> <SID> [REPLY] <HDR_LEN> <TOT_LEN><CRLF><PAYLOAD><CRLF>
```

- `HEADER_LEN` includes the entire header including the a double <CRLF>
- `TOT_LEN` includes the entire payload length including the header



## Decision

Implemented and merged to master.

## Consequences

Use of headers is possible.
