# 1. JetStream JSON API Design

Date: 2020-04-30

## Status

Proposed

## Context

At present, the API encoding consists of mixed text and JSON, we should improve consistency and error handling.

## Decision

We anticipate many types of consumer for JetStream including very small devices via MQTT and those written in 
languages without easy to use JSON support like C.

We will make it easy for light-weight clients by using `+OK` and `-ERR` style prefixes for client interactions,
but the server management API will be JSON all the time. The rationale being that such small devices will most
likely have their Consumers and Streams provisioned as part of the platform and often might not care for the
additional context.

In client responses we will use URL Encoding to provide additional context so multiple fields worth of
context can be provided. URL Encoding has [broad language support](https://www.rosettacode.org/wiki/URL_decoding).

### Admin APIs

#### Requests

All Admin APIs that today accept `nil` body should also accept an empty JSON document as request body.

#### Responses

All responses will be JSON objects, a few examples will describe it best. Any error that happens has to be 
communicated within the originally expected message type. Even the case where JetStream is not enabled for
an account, the response has to be the data type the API would normally generate with additional `error`.

Successful Stream Info:

```json
{
  "type": "io.nats.jetstream.api.v1.stream_info",
  "time": "2020-04-23T16:51:18.516363Z",
  "config": {
    "name": "STREAM",
    "subjects": [
      "js.in"
    ],
    "retention": "limits",
    "max_consumers": -1,
    "max_msgs": -1,
    "max_bytes": -1,
    "max_age": 31536000,
    "max_msg_size": -1,
    "storage": "file",
    "num_replicas": 1
  },
  "state": {
    "messages": 95563,
    "bytes": 40104315,
    "first_seq": 34,
    "last_seq": 95596,
    "consumer_count": 1
  }
}
```

Stream Info Error:

```json
{
  "type": "io.nats.jetstream.api.v1.stream_info",
  "time": "2020-04-23T16:51:18.516363Z",
  "error": {
    "message": "unknown stream bob",
    "code": 123,
    "kind": "JetStreamBadRequest"
  },
  "config": {
    "name": "",
    "retention": "",
    "max_consumers": 0,
    "max_msgs": 0,
    "max_bytes": 0,
    "max_age": 0,
    "storage": "",
    "num_replicas": 0
  },
  "state": {
    "messages": 0,
    "bytes": 0,
    "first_seq": 0,
    "last_seq": 0,
    "consumer_count": 0
  }
}
```

Here we have a minimally correct response with the additional error object.

Today the list API's just return `["ORDERS"]`, these will become:

```json
{
  "type": "io.nats.jetstream.api.v1.stream_list",
  "time": "2020-04-23T16:51:18.516363Z",
  "streams": [
    "ORDERS"
  ]
}
```

With the same `error` treatment when some error happens.

### Client Interactions

To keep clients on small devices viable we will standardise our responses as in the examples below. 

 * `+OK` everything is fine no additional context 
 * `+OK stream=ORDERS&sequence=10` everything is fine, we have additional data to pass using standard URL encoding
 * `-ERR` something failed, we have no reason
 * `-ERR reason=reason%20for%20failure&stream=STREAM` something failed, we have additional context to provide using standard URL encoding

Additional information will be needed for example when there are overlapping Streams and one of them fail. 

## Consequences

Even with these changes in JavaScript we'll have decoding issues since once in JSON mode JavaScript always wants JSON data and these 
`+OK` will trip it up.

URL Encoding does not carry data types and the response fields will need documenting.
