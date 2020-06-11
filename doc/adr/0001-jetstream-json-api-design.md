# 1. JetStream JSON API Design

Date: 2020-04-30
Author: @ripienaar

## Status

Partially Implemented

The JSON API has been updated, however the urlencoded replies have not been adopted.

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

Any API that responds with JSON should also accept JSON, for example to delete a message by sequence we accept
`10` as body today, this would need to become `{"seq": 10}` or similar.

#### Responses

All responses will be JSON objects, a few examples will describe it best. Any error that happens has to be 
communicated within the originally expected message type. Even the case where JetStream is not enabled for
an account, the response has to be a valid data type with the addition of `error`. When `error` is present
empty fields may be omitted as long as the response still adheres to the schema.

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
  "error": {
    "description": "unknown stream bob",
    "code": 400
  }
}
```

Here we have a minimally correct response with the additional error object.

In the `error` struct we have `description` as a short human friendly explanation that should include enough context to
identify what Stream or Consumer acted on and whatever else we feel will help the user while not sharing privileged account
information.  `code` will be HTTP like, 400 human error, 500 server error etc.

Ideally the error response includes a minimally valid body of what was requested but this can be very hard to implement correctly.

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
 * `+OK "stream=ORDERS&sequence=10"` everything is fine, we have additional data to pass using standard URL encoding
 * `-ERR` something failed, we have no reason
 * `-ERR "reason=reason%20for%20failure&stream=STREAM"` something failed, we have additional context to provide using standard URL encoding

Additional information will be needed for example when there are overlapping Streams and one of them fail. 

## Implementation

While implementing this in JetStream the following pattern emerged:

```go
type JSApiResponse struct {
	Type  string    `json:"type"`
	Error *ApiError `json:"error,omitempty"`
}

type ApiError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

type JSApiConsumerCreateResponse struct {
	JSApiResponse
	*ConsumerInfo
}
```

This creates error responses without the valid `ConsumerInfo` fields but this is by far the most workable solution.

Validating this in JSON Schema draft 7 is a bit awkward, not impossible and specifically leads to some hard to parse validation errors, but it works.:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://nats.io/schemas/jetstream/api/v1/consumer_create_response.json",
  "description": "A response from the JetStream $JS.API.CONSUMER.CREATE API",
  "title": "io.nats.jetstream.api.v1.consumer_create_response",
  "type": "object",
  "required": ["type"],
  "oneOf": [
    {
      "$ref": "definitions.json#/definitions/consumer_info"
    },
    {
      "$ref": "definitions.json#/definitions/error_response"
    }
  ],
  "properties": {
    "type": {
      "type": "string",
      "const": "io.nats.jetstream.api.v1.consumer_create_response"
    }
  }
}
```

## Consequences

URL Encoding does not carry data types and the response fields will need documenting.
