# JetStream Batched Storage Sync

| Metadata | Value                       |
|----------|-----------------------------|
| Date     | 2026-07-11                  |
| Author   | @wallyqs                    |
| Status   | Implemented                 |
| Tags     | jetstream, server, storage  |

| Revision | Date       | Author   | Info           |
|----------|------------|----------|----------------|
| 1        | 2026-07-11 | @wallyqs | Initial design |

## Context and Problem Statement

JetStream's file store appends message data to block files using buffered
writes. A write is considered complete once the data reaches the OS page
cache; when it reaches stable media is decided by the sync (fsync) policy.
Prior to this change the server offered exactly two policies:

1. **`sync_interval: <duration>`** (default `2m`) — a background timer pass
   (`syncBlocks`) fsyncs blocks that have pending writes. Between passes, all
   acknowledged writes live only in the page cache.
2. **`sync_interval: always`** — every write is fsynced inline before the
   store call returns (and metadata files are opened with `O_SYNC`).

Neither policy is a good fit for a common class of deployments:

- With the default interval, a whole-node failure (power loss, kernel panic,
  hypervisor kill) can discard up to **two minutes** of acknowledged writes on
  that node. A plain process crash loses nothing — the page cache survives —
  but node-level failures are exactly the ones operators worry about.
- With `always`, durability is strong but throughput collapses, since every
  message pays a full device flush. On the benchmark hardware below this is
  the difference between ~500k msgs/sec and ~1.6k msgs/sec — two to three
  orders of magnitude.

The question this ADR addresses: can we make fsync *timely* — bounding the
loss window to tens of milliseconds — without paying anything close to the
`always` cost?

## Prior Work

- `sync_interval` and `sync_interval: always` in the `jetstream {}` config
  block (`FileStoreConfig.SyncInterval` / `FileStoreConfig.SyncAlways`).
- The per-block flusher (`AsyncFlush`) already batches *writes*; this ADR
  applies the same coalescing idea to *syncs*.
- Group commit in databases (e.g. WAL group commit in Postgres, MySQL
  `innodb_flush_log_at_timeout`, etcd's batched WAL fsync) is the analogous
  well-established technique.

## Design

A third mode, **`sync_interval: batched`**, plumbed as `Options.SyncBatched`
→ `JetStreamConfig.SyncBatched` → `FileStoreConfig.SyncBatched`. It applies
to all file stores the server creates: stream stores, the cluster meta store,
and Raft WALs (so acknowledged-but-unsynced Raft log entries get the same
protection).

### Mechanism

- Each `fileStore` running in batched mode starts one `batchSyncLoop`
  goroutine (alongside the existing `flushStreamStateLoop`), signalled through
  a 1-buffered kick channel (`fs.sbch`), so kicks are non-blocking and
  naturally coalesce.
- Every code path that leaves data written-but-unsynced kicks the loop:
  message block flushes (`flushPendingMsgsLocked`), block compaction rewrites
  (`compactWithFloor`), block truncation via purge/compact, and encryption key
  file creation.
- Once kicked, the loop waits out a coalescing window
  (`defaultSyncBatchWindow`, 25ms) so that all writes landing within the
  window share a single fsync, then runs `syncPendingBlocks`:
  - fsyncs each block marked `needSync` that still has an open write fd
    (one fsync per dirty block, typically just the last block);
  - fsyncs pending encryption key files first, since block data is unreadable
    without its key;
  - leaves blocks whose fds have been closed to the periodic pass rather than
    reopening them (data in closed-fd blocks was already flushed, and the
    periodic pass will sync them);
  - propagates sync errors into the store's write-error state (`fs.werr`),
    the same as `syncBlocks`.
- The periodic `syncBlocks` pass **still runs** at the configured
  `sync_interval` as a safety net, and continues to own compaction scheduling,
  idle fd closing, and syncing the stream state file (`index.db`).
- `sync_interval: always` supersedes batched mode: if both are set, batched is
  disabled, since every write is already synced inline.

### Semantics

- Publish acknowledgements are **unchanged**: the server does not wait for
  the batched fsync before acking (unlike `always`, where the inline flush
  path syncs before returning). Batched mode is therefore a bounded
  loss-window guarantee, not an ack-durability guarantee.
- The loss window on a node-level failure becomes approximately
  `batch window (25ms) + fsync duration`, instead of up to `sync_interval`.
- fsync amplification is bounded: at most ~40 fsyncs per second per actively
  written store, regardless of message rate, and zero when idle (the loop
  only runs when kicked). Device flush concurrency remains bounded by the
  existing disk I/O semaphore.

### Configuration

```text
jetstream {
  store_dir: /data/nats
  sync_interval: batched
}
```

`sync_interval: <duration>` and `sync_interval: always` behave as before.

## Benchmarks

Two benchmarks ship with this change. Environment for both: 4 vCPU Intel
Xeon cloud VM, ext4 on virtio (`/dev/vda`), Linux 6.x. Median of 3 runs.
Absolute numbers are device-dependent — fsync cost dominates the `always`
columns — but the relationships hold generally. Note that both create their
stores under the test temp dir, so set `TMPDIR` to a real disk (not tmpfs)
to get honest fsync numbers.

### Micro benchmark: store write path

`Benchmark_FileStoreStoreMsgSyncModes` measures sequential `StoreMsg` calls
directly against a `fileStore`, isolating the storage-layer cost per sync
mode and message size:

```sh
go test ./server/ -run XXX -bench Benchmark_FileStoreStoreMsgSyncModes -benchtime 5000x -count 3
```

| Msg size | Interval (default) | Batched (new)         | Always                |
|----------|--------------------|-----------------------|-----------------------|
| 64 B     | 1,949 ns/op        | 1,906 ns/op (~1.0x)   | 639,214 ns/op (~330x) |
| 1 KiB    | 3,843 ns/op        | 3,520 ns/op (~1.0x)   | 689,439 ns/op (~180x) |
| 16 KiB   | 14,862 ns/op       | 19,777 ns/op (~1.3x)  | 1,497,061 ns/op (~100x) |

Expressed as sustained single-stream write rates:

| Msg size | Interval        | Batched         | Always        |
|----------|-----------------|-----------------|---------------|
| 64 B     | ~513k msgs/s    | ~525k msgs/s    | ~1.6k msgs/s  |
| 1 KiB    | ~260k msgs/s    | ~284k msgs/s    | ~1.5k msgs/s  |
| 16 KiB   | ~67k msgs/s (1.1 GB/s) | ~51k msgs/s (830 MB/s) | ~0.7k msgs/s (11 MB/s) |

Observations:

- At small and medium message sizes batched mode is indistinguishable from
  the interval-only default: the fsync happens off the write path and
  thousands of messages share each flush (~13k msgs per 25ms window at 64B).
- At large message sizes under saturation there is a measurable (~20–30%)
  overhead. The batch fsync holds the block lock while syncing (mirroring
  `syncBlocks`), so at high data volume writers occasionally wait for an
  in-progress fsync. This is the honest cost of the mode; it remains ~40–75x
  faster than `always`.
- `always` is 2–3 orders of magnitude slower across the board, since every
  message pays a ~0.6–1.5ms device flush.

### End-to-end benchmark: publish through a running server

`BenchmarkJetStreamPublishSyncModes` measures the full publish path — real
server(s), real client, JetStream publish acks — for single-node (R1) and a
3-node cluster (R3, connected to the stream leader), with each sync mode
applied to all servers (stream stores *and* Raft WALs). Async publishing
uses a window of 1000 pending messages:

```sh
go test ./server/ -run XXX -bench BenchmarkJetStreamPublishSyncModes -benchtime 2000x -count 3
```

Async publish (throughput path), median ns/op:

| Case      | Interval | Batched          | Always            |
|-----------|----------|------------------|-------------------|
| R1, 128B  | 4,542    | 4,616 (~1.0x)    | 700,560 (~154x)   |
| R1, 1KiB  | 5,796    | 5,461 (~1.0x)    | 692,095 (~119x)   |
| R3, 128B  | 5,124    | 5,260 (~1.0x)    | 1,680,108 (~328x) |
| R3, 1KiB  | 8,375    | 7,260 (~1.0x)    | 1,796,629 (~215x) |

Sync publish (one message at a time; measures per-message ack latency),
median ns/op:

| Case      | Interval | Batched          | Always              |
|-----------|----------|------------------|---------------------|
| R1, 128B  | 65,745   | 76,277 (+16%)    | 1,034,156 (~15.7x)  |
| R1, 1KiB  | 70,934   | 78,627 (+11%)    | 979,107 (~13.8x)    |
| R3, 128B  | 167,327  | 215,368 (+29%)   | 3,255,295 (~19.5x)  |
| R3, 1KiB  | 184,179  | 245,330 (+33%)   | 3,514,371 (~19x)    |

Observations:

- On the throughput path (async publishing, which is how high-rate
  producers publish) batched mode is at parity with the default in every
  topology, ~200k msgs/sec at R3/128B on this VM, while `always` collapses
  to ~600 msgs/sec.
- Per-message sync publish latency shows the cost of risk #2 end to end:
  with a steady publisher every 25ms window contains data, so every store in
  the path (leader and follower Raft WALs, stream stores) fsyncs each
  window, and a publish that lands while its block is being fsynced waits.
  This adds ~10–15% ack latency at R1 and ~30% at R3 on this disk
  (microseconds-scale), against 14–20x for `always` (milliseconds-scale).
- `always` at R3 pays the fsync on the leader WAL, follower WALs, and the
  stream store apply for every message, compounding to ~3.3–3.5ms per
  acknowledged publish.

## Risks and Mitigations

| # | Risk | Mitigation / Assessment |
|---|------|--------------------------|
| 1 | Acked-but-unsynced data still exists: a power loss / kernel panic within the window loses up to ~25ms + fsync-duration of acknowledged writes on that node. | Orders of magnitude smaller than the 2-minute default window. With replication (R≥2) an ack implies the data is in memory on a quorum, so loss additionally requires a *correlated* node-level failure of a majority within the same ~25ms, before any replica synced. For strict single-node (R1) requirements, `always` remains the only ack-durability guarantee. |
| 2 | Writers to a block stall while its fsync is in flight (block lock held during sync). | Bounded to one fsync per window per block; measurable at high sustained data rates (see 16 KiB micro benchmark, ~20–30%) and as added per-message ack latency in the end-to-end sync-publish benchmark (~10–30%, microseconds-scale). Does not affect the async/throughput path (parity in all end-to-end cases). On slow devices (HDD, ~10ms fsync) the effective stall grows; such deployments should prefer the interval mode or accept the trade. |
| 3 | Higher device flush frequency than the 2-minute default (SSD wear, IOPS). | Capped at ~40 fsyncs/sec per *actively written* store, zero when idle; far below `always` (one per message). The existing disk I/O semaphore bounds cross-store concurrency. |
| 4 | Stream state file (`index.db`) is still only synced by the periodic pass. | `index.db` is a recovery optimization, not the source of truth; on mismatch or corruption the server rebuilds state from the message blocks, which *are* batched-synced. |
| 5 | Consumer state (`o.dat`) is not covered by batched mode. | Unchanged from the default mode (consumer state is never fsynced unless `always`). Consumer state loss degrades to redelivery, which JetStream semantics already tolerate (at-least-once). Extending batched sync to consumer stores is possible follow-up work. |
| 6 | Torn/partial block writes on power loss. | Unchanged by this ADR: per-record checksums and `rebuildStateLocked` detect and truncate torn tails on recovery, in every mode. |
| 7 | One extra goroutine per file store. | Matches the existing per-store `flushStreamStateLoop` precedent; only started when the mode is enabled. |

## Decision

Introduce `sync_interval: batched` as an opt-in third sync policy. Defaults
are unchanged (`2m` interval), and `always` remains available for strict
ack-durability.

**Is batched enough to mitigate the risks?** For the failure classes that
matter in practice:

- *Process crash / restart*: fsync policy is irrelevant (page cache
  survives); all modes are already safe.
- *Node-level failure with replication (R≥3)*: yes. The correlated-loss
  window shrinks from minutes to tens of milliseconds, an ack requires a
  quorum, and the Raft WAL itself is batched-synced. Losing acknowledged data
  now requires a majority of replicas to suffer power/kernel failure within
  the same ~25ms window.
- *Node-level failure at R1 with a hard "no acked-message loss" requirement*:
  no — only `always` (sync-before-ack) provides that guarantee. Batched
  reduces the exposure by ~4,800x (2m → 25ms) but does not eliminate it.

**Does it have good performance?** Yes: on the storage write path it is at
parity with the default at small/medium message sizes and within ~30% at
large sizes under saturation; end to end, async publish throughput is at
parity in every topology, and per-message sync publish latency grows by
~10–30% (microseconds). `always` pays 100–330x on throughput and 14–20x on
ack latency in the same benchmarks.

## Consequences

- Operators get a practical durability/performance middle ground; documenting
  it as the recommended setting for replicated deployments that care about
  correlated failures is reasonable.
- The 25ms window is currently a constant (`defaultSyncBatchWindow`); making
  it configurable is trivial if a need appears.
- Ack semantics are unchanged, which keeps the change small and safe, but
  means batched mode must not be documented as "durable before ack".

## Future Work

- **Group commit**: have the publish ack wait on the coalesced fsync
  (sync-before-ack at batched cost under concurrency). This would give
  `always`-grade guarantees for R1 at a fraction of the cost, at the price of
  added ack latency (up to one window) and a larger change to the store API.
- Configurable batch window (`sync_batch_window`).
- Extend batched syncing to consumer state files.

## References

- `server/filestore.go`: `batchSyncLoop`, `syncPendingBlocks`,
  `kickBatchSync`, `defaultSyncBatchWindow`.
- `server/opts.go`: `sync_interval: batched` parsing.
- `Benchmark_FileStoreStoreMsgSyncModes` in `server/filestore_test.go`.
- `BenchmarkJetStreamPublishSyncModes` in `server/jetstream_benchmark_test.go`
  (end to end).
- Tests: `TestFileStoreSyncBatched`, `TestFileStoreSyncBatchedEncryptionKeyFile`,
  `TestFileStoreSyncBatchedSupersededBySyncAlways`,
  `TestJetStreamSyncInterval/Batched`.
