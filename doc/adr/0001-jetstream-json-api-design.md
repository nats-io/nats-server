# 1. JetStream JSON API Design

Date: 2020-04-30
Author: @ripienaar

## Status

Partially Implemented

## Context

At present, the API encoding consists of mixed text and JSON, we should improve consistency and error handling.

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

Consumer Info Error:

```json
{
  "type": "io.nats.jetstream.api.v1.consumer_info",
  "error": {
    "description": "consumer not found",
    "code": 404,
    "error_code": 10059
  }
}
```

Here we have a minimally correct response with the additional error object.

In the `error` struct we have `description` as a short human friendly explanation that should include enough context to
identify what Stream or Consumer acted on and whatever else we feel will help the user while not sharing privileged account
information.  These strings are not part of the API promises, we can update and re-word or translate them at any time. Programmatic
error handling should look at the `code` which will be HTTP like, 4xx human error, 5xx server error etc. Finally, the `error_code`
indicates the specific reason for the 404 - here `10059` means the stream did not exist, helping developers identify the
real underlying cause. 

More information about the `error_code` system can be found in [ADR-7](0007-error-codes.md).

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

## Implementation

While implementing this in JetStream the following pattern emerged:

```go
type JSApiResponse struct {
	Type  string    `json:"type"`
	Error *ApiError `json:"error,omitempty"`
}

type ApiError struct {
    Code        int    `json:"code"`
    ErrCode     int    `json:"err_code,omitempty"`
    Description string `json:"description,omitempty"`
    URL         string `json:"-"`
    Help        string `json:"-"`
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

URL Encoding does not carry data types, and the response fields will need documenting.
