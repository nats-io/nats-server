# 10. js-purge

Date: 2021-06-30

## Status

Accepted

## Context

JetStream provides the ability to purge streams by sending a request message to:
`$JS.API.STREAM.PURGE.<streamName>`. The request will return a new message with
the following JSON:

```typescript
{
    type: "io.nats.jetstream.api.v1.stream_purge_response", 
    error?: ApiError,
    success: boolean,
    purged: number
}
```

The `error` field is an [ApiError](0007-error-codes.md). The `success` field will be set to `true` if the request
succeeded. The `purged` field will be set to the number of messages that were
purged from the stream.

## Options

More fine-grained control over the purge request can be achieved by specifying
additional options as JSON payload.

```typescript
{
    seq?: number,
    keep?: number,
    filter?: string
}
```

- `seq` is the optional upper-bound sequence for messages to be deleted
  (non-inclusive)
- `keep` is the maximum number of messages to be retained (might be less
  depending on whether the specified count is available).
- The options `seq` and `keep` are mutually exclusive.
- `filter` is an optional subject (may include wildcards) to filter on. Only
  messages matching the filter will be purged.
- `filter` and `seq` purges all messages matching filter having a sequence
  number lower than the value specified.
- `filter` and `keep` purges all messages matching filter keeping at most the
  specified number of messages.
- If `seq` or `keep` is specified, but `filter` is not, the stream will
  remove/keep the specified number of messages.
- To `keep` _N_ number of messages for multiple subjects, invoke `purge` with
  different `filter`s.
- If no options are provided, all messages are purged.

## Consequences

Tooling and services can use this endpoint to remove messages in creative ways.
For example, a stream may contain a number of samples, at periodic intervals a
service can sum them all and replace them with a single aggregate.
