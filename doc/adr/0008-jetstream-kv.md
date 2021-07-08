# 8. JetStream based Key-Value Stores

|Metadata|Value|
|--------|-----|
|Date    |2021-06-30|
|Author  |@ripienaar|
|Status  |Partially Implemented|
|Tags    |jetstream, client, kv|

## Context

This document describes a design and initial implementation of a JetStream backed key-value store. The initial implementation
is available in the CLI as `nats kv` with an initial reference client package implementation in `jsm.go` repository to support
the CLI.

This document aims to guide client developers in implementing this feature in the language clients we maintain.

## Overview

We intend to hit a basic initial feature set as below, with some future facing goals as indicated:

Initial feature list:

 * Multiple named buckets full of keys with n historical values kept per value
 * Put and Get of `string(k)=[]byte(v)` values
 * Put only if the sequence of the last value for a key matches an expected sequence
 * Historic values can be kept per key, setable on creation of the bucket
 * Key deletes preserves history
 * Keys can be expired from the bucket based on a TTL
 * Watching a specific key or the entire bucket for live updates
 * Encoders and Decoders that transforms both keys and values
 * A read-cache that builds an in-memory cache for fast reads
 * read-after-write safety unless read replicas used
 * Valid keys are `\A[-/_a-=zA-Z0-9]+\z` after encoding
 * Valid buckets are `^[a-zA-Z0-9_-]+$`
 * Custom Stream Names and Stream ingest subjects to cater for different domains, mirrors and imports
 * Key starting with `_kv` is reserved for internal use
 * CLI tool to manage the system as part of `nats`, compatible with client implementations
 * Read-Only and Read-Write interfaces

Planned features:

 * Ability to read from regional read replicas maintained by JS Mirroring (planned)
 * A read-only client can talk directly, and only, to a regional read replica
 * Discoverability of read replicas per cluster based on in-bucket configuration values
 * Standard encoders/decoders to deliver zero-trust end-to-end encryption, perhaps using nkeys
 * Pluggable storage backends

## Data Types

Here's rough guidance, for some clients in some places you might not want to use `[]string` but an iterator, that's
fine, the languages should make appropriate choices based on this rough outline.

### Entry

This is the value and associated metadata made available over watchers, `Get()` etc. All backends must return an implementation providing at least this information.

```go
type Entry interface {
	// Bucket is the bucket the data was loaded from
	Bucket() string
	// Key is the key that was retrieved
	Key() string
	// Value is the retrieved value
	Value() []byte
	// Created is the time the data was received in the bucket
	Created() time.Time
	// Sequence is a unique sequence for this value
	Sequence() uint64
	// Delta is distance from the latest value. If history is enabled this is effectively the index of the historical value, 0 for latest, 1 for most recent etc.
	Delta() uint64
	// Operation is the kind of operation this entry represents, enum of PUT and DEL
	Operation() Operation
}
```

### Status

This is the status of the KV as a whole

```go
type Status interface {
	// Bucket the name of the bucket
	Bucket() string

	// Values is how many messages are in the bucket, including historical values
	Values() uint64

	// History returns the configured history kept per key
	History() uint64

	// TTL is how long the bucket keeps values for
	TTL() time.Duration

	// BucketLocation returns a string indicating where the bucket is located, meaning dependent on backend
	BucketLocation() string

	// Keys returns a list of all keys in the bucket - not possible now
	Keys() ([]string, error)

	// BackingStore is a backend specific name for the underlying storage - eg. stream name. Used so that ops tooling can know what to Backup for example
	BackingStore() string
}
```

## RoKV

This is a read-only KV store handle, I call this out here to demonstrate that we need to be sure to support a read-only 
variant of the client. One that will only function against a read replica and cannot support `Put()` etc. 

That capability is important, how you implement this in your language is your choice. You can throw exceptions on `Put()`
when read-only or whatever you like.

The interface here is a guide of what should function in read-only mode.

```go
// RoKV is a read-only interface to a single key-value store bucket
type RoKV interface {
	// Get gets a key from the store
	Get(key string) (Entry, error)

	// History retrieves historic values for a key
	History(ctx context.Context, key string) ([]Entry, error)

	// WatchBucket watches the entire bucket for changes, all keys and values will be traversed including all historic values
	WatchBucket(ctx context.Context) (Watch, error)

	// Watch a key for updates, the same Entry might be delivered more than once
	Watch(ctx context.Context, key string) (Watch, error)

	// Close releases in-memory resources held by the KV, called automatically if the context used to create it is canceled
	Close() error

	// Status retrieves the status of the bucket
	Status() (Status, error)
}
```

## KV

This is the read-write KV store handle, every backend should implement a language equivalent interface. But note the comments
by `RoKV` for why I call these out separately.

```go
// KV is a read-write interface to a single key-value store bucket
type KV interface {
	// Put saves a value into a key
	Put(key string, val []byte, opts ...PutOption) (seq uint64, err error)

	// Delete purges the subject
	Delete(key string) error

	// Destroy removes the entire bucket and all data, KV cannot be used after
	Destroy() error

	// Purge removes all data from the bucket but leaves the bucket
	Purge() error

	RoKV
}
```

## Storage Backends

We do have plans to support, and provide, commercial KV as part of our NGS offering, however there will be value in an
open source KV implementation that can operate outside of NGS, especially one with an identical API.

Today we will support a JetStream backend as documented here, future backends will have to be able to provide these
features, that is, this is the minimal feature set we can expect from any KV backend.

Client developers should keep this in mind and support pluggable backends, for the reference implementation caching
is implemented as a different backend.

### JetStream interactions

The features to support KV is in NATS Server 2.3.1.

#### Buckets

A bucket is a Stream with these properties:

 * The main write bucket must be called `KV_<Bucket Name>`
 * The ingest subjects must be `$KV.<Bucket Name>.*`
 * The bucket history is achieved by setting `max_msgs_per_subject` to the desired history level
 * Write replicas are File backed and can have a varying R value
 * Key TTL is managed using the `max_age` key
 * Duplicate window must be same as `max_age` when `max_age` is less than 2 minutes
 * Maximum value sizes can be capped using `max_msg_size`
 * Maximum number of keys cannot currently be limited - we will add something to the server
 * Overall bucket size can be limited using `max_bytes`

Here is a full example of the `CONFIGURATION` bucket:

```json
{
  "name": "KV_CONFIGURATION",
  "subjects": [
    "$KV.CONFIGURATION.*"
  ],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs_per_subject": 5,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "file",
  "discard": "old",
  "num_replicas": 1,
  "duplicate_window": 120000000000
}
```

#### Storing Values

Writing a key to the bucket is a basic JetStream request.

The KV key `username` in the `CONFIGURATION` bucket is written sent, using a request, to `$KV.CONFIGURATION.username`.

To implement the feature that would accept a write only if the sequence of the current value of a key has a specific sequence
we use the new `Nats-Expected-Last-Subject-Sequence` header.

#### Retrieving Values

There are different situations where messages will be retrieved using different APIs, below describes the different models.

In all cases we return a generic `Entry` type.

Deleted data - (see later section on deletes) - has the `KV-Operation` header set to `DEL`, a value received from either of these
methods with this header set indicates the data was being deleted.  If this is a pending=0 message it means the key does not exist.

##### Get Operation

We have extended the `io.nats.jetstream.api.v1.stream_msg_get_request` API to support loading the latest value for a specific
subject.  Thus a read for `CONFIGURATION.username` becomes a `io.nats.jetstream.api.v1.stream_msg_get_request` with the
`last_by_subject` set to `$KV.CONFIGURATION.username`.

##### History and Watch Operations

These operations requires access to all values for a key, to achieve this we create an ephemeral consumer on filtered by the subject
and read the entire value list in using `deliver_all`.

JetStream will report the Pending count for each message, the latest value from the available history would have a pending of `0`.
When constructing historic values, dumping all values etc we ensure to only return pending 0 messages as the final value

#### Deleting Values

Since the store supports history - via the `max_age` for messages - we should preserve history when deleting keys. To do this we
place a new message in the subject for the key with a nil body and the header `KV-Operation: DEL`.

This preserves history and communicate to watchers, caches and gets that a delete operation should be handled - clear cache,
return no key error etc.

#### Purging a bucket

A purge deletes all messages from a bucket, this is a normal stream purge on the entire stream.

#### Deleting a bucket

Remove the stream entirely

#### Watchers

Watchers support sending received `PUT` and `DEL` operations across a channel or language specific equivelant.

##### Key Watch

This is a consumer set up to get the last value for a key. The watch remains active till cancelled and any PUT/DEL on this key should be 
made available to the client.

##### Bucket Watch

A bucket watch is a consumer set up on the wildcard subject for the bucket - `$KV.<Bucket Name>.*` - and sending every single message
over the channel to the client.

This means historical values will be sent as well as latest values. In this case we cannot provide the delta on a per key basis. But
the delta will represent the remainder for the entire bucket, this allows Watcher clients like the memory cache to set themselves
ready once the full history was read.

When a stream is empty at create time - `NumPending+Delivered.Consumer==0` - a nil is sent across the channel to signal to the
caller that they should not expect any values.

#### Difficult to support features

JetStream does not today support asking a stream for its list of subjects, this means an iterator over keys needs to read the entire
bucket from start to end and consolidate the list extracting key and processing `KV-Operation` etc.

The `Status` interface supports a `Keys()` call, but this is not implemented and might come later.  We could though provide a `Keys()`
function from a local cache since walking the entire stream is implied in how it works.

### Codec support

We support client-side encoding and decoding, the basic interface is `func([]byte) ([]byte, error)`. Users can supply a encode, a decoder 
or a codec.

Read only clients would only need a decoder - and so by extension, not need a private key.

All keys and values should be encoded/decoded. If users wish to encrypt their buckets both the keys and the values will be encrypted.
The valid keys after encryption should still match the pattern for acceptable keys. Base64 encoding encrypted keys is a good option.

### Local Read Caches

In a super-cluster the Bucket can be far away and have some latency, a read-cache is implemented that wraps the JetStream backend.

The cache is warmed up using a Bucket Watch, the cache is ready when it's reached the `pending=0` message from the watch.

While the cache is still warming up it's a pass through to the underlying backend for any reads.

Most operations are pass-through, except:

 * `Get()` will read local cache if ready (fully warmed), and a key is there
 * `Delete()` will evict the key from local cache and pass through
 * `Put()` will evict the key from local cache and pass through
 * `Destroy()` and `Purge()` will delete all messages from local cache and pass through

We specifically do not place the `Put()` value into local cache since an Entry  must know the JetStream sequence.

This design preserves the read-after-write promises.

