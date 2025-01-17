<a id="v2.10.24"></a>
# [Release v2.10.24](https://github.com/nats-io/nats-server/releases/tag/v2.10.24) - 2024-12-17

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### CVEs
- Vulnerability check warnings for [CVE-2024-45337](https://github.com/advisories/GHSA-v778-237x-gjrc) are addressed by the dependency update to `x/crypto`, although the NATS Server does not use the affected functionality and is therefore not vulnerable

### Go Version
- 1.23.4

### Dependencies
- golang.org/x/crypto v0.31.0 ([#6246](https://github.com/nats-io/nats-server/issues/6246))
- github.com/nats-io/jwt/v2 v2.7.3 ([#6256](https://github.com/nats-io/nats-server/issues/6256))
- github.com/nats-io/nkeys v0.4.9 ([#6255](https://github.com/nats-io/nats-server/issues/6255))

### Fixed

General
- Request/reply tracking with `allow_responses` permission is now pruned more regularly, fixing performance issues that can get worse over time ([#6064](https://github.com/nats-io/nats-server/issues/6064))

JetStream
- Revert a change introduced in 2.10.23 that could potentially cause a consumer info call to fail if it takes place immediately after the consumer was created in some large or heavily-loaded clustered setups ([#6250](https://github.com/nats-io/nats-server/issues/6250))
- Minor fixes to subject state tracking ([#6244](https://github.com/nats-io/nats-server/issues/6244))
- Minor fixes to `healthz` and healthchecks ([#6247](https://github.com/nats-io/nats-server/issues/6247), [#6248](https://github.com/nats-io/nats-server/issues/6248), [#6232](https://github.com/nats-io/nats-server/issues/6232))
- A calculation used to determine if exceeding limits has been corrected ([#6264](https://github.com/nats-io/nats-server/issues/6264))
- Raft groups will no longer spin when truncating the log fails, i.e. during shutdown ([#6271](https://github.com/nats-io/nats-server/issues/6271))

WebSockets
- A WebSocket close frame will no longer incorrectly include a status code when not needed ([#6260](https://github.com/nats-io/nats-server/issues/6260))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.23...v2.10.24

[Changes][v2.10.24]


<a id="v2.10.23"></a>
# [Release v2.10.23](https://github.com/nats-io/nats-server/releases/tag/v2.10.23) - 2024-12-10

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.23.4 ([#6228](https://github.com/nats-io/nats-server/issues/6228))

### Dependencies

- golang.org/x/crypto v0.30.0 ([#6230](https://github.com/nats-io/nats-server/issues/6230))
- golang.org/x/sys v0.27.0 ([#6229](https://github.com/nats-io/nats-server/issues/6229))
- golang.org/x/time v0.8.0 ([#6105](https://github.com/nats-io/nats-server/issues/6105))
- github.com/nats-io/nkeys v0.4.8 ([#6192](https://github.com/nats-io/nats-server/issues/6192))

### Added

JetStream
- Support for responding to forwarded proposals (for future use, [#6157](https://github.com/nats-io/nats-server/issues/6157))

Windows
- New `ca_certs_match` option has been added in the `tls` block for searching the certificate store for only certificates matching the specified CAs ([#5115](https://github.com/nats-io/nats-server/issues/5115))
- New `cert_match_skip_invalid` option has been added in the `tls` block for ignoring certificates that have expired or are not valid yet ([#6042](https://github.com/nats-io/nats-server/issues/6042))
- The `cert_match_by` option can now be set to `thumbprint`, allowing an SHA1 thumbprint to be specified in `cert_match` ([#6042](https://github.com/nats-io/nats-server/issues/6042), [#6047](https://github.com/nats-io/nats-server/issues/6047))

### Improved

JetStream
- Reduced the number of allocations in consumers from get-next requests and when returning some error codes ([#6039](https://github.com/nats-io/nats-server/issues/6039))
- Metalayer recovery at startup will now more reliably group assets for creation/update/deletion and handle pending consumers more reliably, reducing the chance of ghost consumers and misconfigured streams happening after restarts ([#6066](https://github.com/nats-io/nats-server/issues/6066), [#6069](https://github.com/nats-io/nats-server/issues/6069), [#6088](https://github.com/nats-io/nats-server/issues/6088), [#6092](https://github.com/nats-io/nats-server/issues/6092))
- Creation of filtered consumers is now considerably faster with the addition of a new multi-subject num-pending calculation ([#6089](https://github.com/nats-io/nats-server/issues/6089), [#6112](https://github.com/nats-io/nats-server/issues/6112))
- Consumer backoff should now be correctly respected with multiple in-flight deliveries to clients ([#6104](https://github.com/nats-io/nats-server/issues/6104))
- Add node10 node size to stree, providing better memory utilisation for some subject spaces, particularly those that are primarily numeric or with numeric tokens ([#6106](https://github.com/nats-io/nats-server/issues/6106))
- Some JetStream log lines have been made more consistent ([#6065](https://github.com/nats-io/nats-server/issues/6065))
- File-backed Raft groups will now use the same sync intervals as the filestore, including when sync always is in use ([#6041](https://github.com/nats-io/nats-server/issues/6041))
- Metalayer snapshots will now always be attempted on shutdown ([#6067](https://github.com/nats-io/nats-server/issues/6067))
- Consumers will now detect if an ack is received past the stream last sequence and will no longer register pre-acks from a snapshot if this happens, reducing memory usage ([#6109](https://github.com/nats-io/nats-server/issues/6109))
- Reduced copies and number of allocations when generating headers for republished messages ([#6127](https://github.com/nats-io/nats-server/issues/6127))
- Adjusted the spread of filestore sync timers ([#6128](https://github.com/nats-io/nats-server/issues/6128))
- Reduced the number of allocations in Raft group send queues, improving performance ([#6132](https://github.com/nats-io/nats-server/issues/6132))
- Improvements to Raft append entry handling and log consistency ([#5661](https://github.com/nats-io/nats-server/issues/5661), [#5689](https://github.com/nats-io/nats-server/issues/5689), [#5714](https://github.com/nats-io/nats-server/issues/5714), [#5957](https://github.com/nats-io/nats-server/issues/5957), [#6027](https://github.com/nats-io/nats-server/issues/6027), [#6073](https://github.com/nats-io/nats-server/issues/6073))
- Improvements to Raft stepdown behaviour ([#5666](https://github.com/nats-io/nats-server/issues/5666), [#5344](https://github.com/nats-io/nats-server/issues/5344), [#5717](https://github.com/nats-io/nats-server/issues/5717))
- Improvements to Raft elections and vote handling ([#5671](https://github.com/nats-io/nats-server/issues/5671), [#6056](https://github.com/nats-io/nats-server/issues/6056))
- Improvements to Raft term handling ([#5684](https://github.com/nats-io/nats-server/issues/5684), [#5792](https://github.com/nats-io/nats-server/issues/5792), [#5975](https://github.com/nats-io/nats-server/issues/5975), [#5848](https://github.com/nats-io/nats-server/issues/5848), [#6060](https://github.com/nats-io/nats-server/issues/6060))
- Improvements to Raft catchups ([#5987](https://github.com/nats-io/nats-server/issues/5987), [#6038](https://github.com/nats-io/nats-server/issues/6038), [#6072](https://github.com/nats-io/nats-server/issues/6072))
- Improvements to Raft snapshot handling ([#6053](https://github.com/nats-io/nats-server/issues/6053), [#6055](https://github.com/nats-io/nats-server/issues/6055))
- Reduced the overall metalayer snapshot frequency by increasing the minimum interval and no longer pre-empting consumer deletes ([#6165](https://github.com/nats-io/nats-server/issues/6165))
- Consumer info requests for non-existent consumers will no longer be relayed, reducing overall load on the metaleader ([#6176](https://github.com/nats-io/nats-server/issues/6176))
- The metaleader will now log if it takes a long time to perform a metalayer snapshot ([#6178](https://github.com/nats-io/nats-server/issues/6178))
- Unnecessary client and subject information will no longer be included in the meta snapshots, reducing the size and encoding time ([#6185](https://github.com/nats-io/nats-server/issues/6185))
- Sourcing consumers for R1 streams will now be set up inline when the stream is recovered ([#6219](https://github.com/nats-io/nats-server/issues/6219))
- Introduced additional jitter to the timer for writing stream state, to smooth out sudden spikes in I/O ([#6220](https://github.com/nats-io/nats-server/issues/6220))

### Fixed

General
- Load balancing queue groups from leaf nodes in a cluster ([#6043](https://github.com/nats-io/nats-server/issues/6043))

JetStream
- Invalidate the stream state when a drift between the tracking states has been detected ([#6034](https://github.com/nats-io/nats-server/issues/6034))
- Fixed a panic in the subject tree when checking for full wildcards ([#6049](https://github.com/nats-io/nats-server/issues/6049))
- Snapshot processing should no longer spin when there is no leader ([#6050](https://github.com/nats-io/nats-server/issues/6050))
- Replicated stream message framing can no longer overflow with extremely long subjects, headers or reply subjects ([#6052](https://github.com/nats-io/nats-server/issues/6052))
- Don’t replace the leader’s snapshot when shutting down, potentially causing a desync ([#6053](https://github.com/nats-io/nats-server/issues/6053))
- Consumer start sequence when specifying an optional start time has been fixed ([#6082](https://github.com/nats-io/nats-server/issues/6082))
- Raft snapshots will no longer be incorrectly removed when truncating the log back to applied ([#6055](https://github.com/nats-io/nats-server/issues/6055))
- Raft state will no longer be deleted if creating a stream/consumer failed because the server was shutting down ([#6061](https://github.com/nats-io/nats-server/issues/6061))
- Fixed a panic when shutting down whilst trying to set up the metagroup ([#6075](https://github.com/nats-io/nats-server/issues/6075))
- Raft entries that we cannot be sure were applied during a shutdown will no longer be reported as applied ([#6087](https://github.com/nats-io/nats-server/issues/6087))
- Corrected an off-by-one error in the run-length encoding of interior deletes, which could incorrectly remove an extra message ([#6111](https://github.com/nats-io/nats-server/issues/6111))
- Don’t process duplicate stream assignment responses when the stream is being reassigned due to placement issues ([#6121](https://github.com/nats-io/nats-server/issues/6121))
- Corrected use of the stream mutex when checking interest ([#6122](https://github.com/nats-io/nats-server/issues/6122))
- Raft entries for consumers that we cannot be sure were applied during a shutdown will no longer be reported as applied ([#6133](https://github.com/nats-io/nats-server/issues/6133))
- Consistent state update behavior between file store and memory store, including a fixed integer underflow ([#6147](https://github.com/nats-io/nats-server/issues/6147))
- No longer send a state snapshot when becoming a consumer leader as it may not include all applied state ([#6151](https://github.com/nats-io/nats-server/issues/6151))
- Do not install snapshots on shutdown from outside the monitor goroutines as it may race with upper layer state ([#6153](https://github.com/nats-io/nats-server/issues/6153))
- The consumer `Backoff` configuration option now correctly checks the `MaxDelivery` constraint ([#6154](https://github.com/nats-io/nats-server/issues/6154))
- Consumer check floor will no longer surpass the store ack floor ([#6146](https://github.com/nats-io/nats-server/issues/6146))
- Replicated consumers will no longer update their delivered state until quorum is reached, fixing some drifts that can occur on a leader change ([#6139](https://github.com/nats-io/nats-server/issues/6139))
- Resolved a deadlock when removing the leader from the peer set ([#5912](https://github.com/nats-io/nats-server/issues/5912))
- Don’t delete disk state if a stream or consumer creation fails during shutdown ([#6061](https://github.com/nats-io/nats-server/issues/6061))
- The metalayer will no longer generate and send snapshots when switching leaders, reducing the chance that snapshots can be sent with stale assignments ([#5700](https://github.com/nats-io/nats-server/issues/5700))
- When restarting a Raft group, wait for previous goroutines to shut down, fixing a race condition ([#5832](https://github.com/nats-io/nats-server/issues/5832))
- Correctly empty the Raft snapshots directory for in-memory assets ([#6169](https://github.com/nats-io/nats-server/issues/6169))
- A race condition when accessing the stream assignments has been fixed ([#6173](https://github.com/nats-io/nats-server/issues/6173))
- Subject state will now be correctly cleared when compacting in-memory streams, fixing some potential replica drift issues ([#6187](https://github.com/nats-io/nats-server/issues/6187))
- Stream-level catchups no longer return more than they should ([#6213](https://github.com/nats-io/nats-server/issues/6213))

Leafnodes
- Fixed queue distribution where a leafnode expressed interest on behalf of a gateway in complex setups ([#6126](https://github.com/nats-io/nats-server/issues/6126))
- A number of leafnode interest propagation issues have been fixed, making it possible to distinguish leaf subscriptions from local routed subscriptions ([#6161](https://github.com/nats-io/nats-server/issues/6161))
- Credential files containing CRLF line endings will no longer error ([#6175](https://github.com/nats-io/nats-server/issues/6175))

WebSockets
- Ensure full writes are made when compression is in use ([#6091](https://github.com/nats-io/nats-server/issues/6091))

Windows
- Using the `LocalMachine` certificate store is now possible from a non-administrator user ([#6019](https://github.com/nats-io/nats-server/issues/6019))

Tests
- A number of unit tests have been improved ([#6086](https://github.com/nats-io/nats-server/issues/6086), [#6096](https://github.com/nats-io/nats-server/issues/6096), [#6098](https://github.com/nats-io/nats-server/issues/6098), [#6097](https://github.com/nats-io/nats-server/issues/6097), [#5691](https://github.com/nats-io/nats-server/issues/5691), [#5707](https://github.com/nats-io/nats-server/issues/5707), [#5991](https://github.com/nats-io/nats-server/issues/5991), [#6107](https://github.com/nats-io/nats-server/issues/6107), [#6183](https://github.com/nats-io/nats-server/issues/6183), [#6218](https://github.com/nats-io/nats-server/issues/6218))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.22...v2.10.23


[Changes][v2.10.23]


<a id="v2.10.22"></a>
# [Release v2.10.22](https://github.com/nats-io/nats-server/releases/tag/v2.10.22) - 2024-10-17

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.8

### Dependencies

- golang.org/x/crypto v0.28.0 ([#5971](https://github.com/nats-io/nats-server/issues/5971))
- golang.org/x/sys v0.26.0 ([#5971](https://github.com/nats-io/nats-server/issues/5971))
- golang.org/x/time v0.7.0 ([#5971](https://github.com/nats-io/nats-server/issues/5971))
- go.uber.org/automaxprocs v1.6.0 ([#5944](https://github.com/nats-io/nats-server/issues/5944))
- github.com/klauspost/compress v1.17.11 ([#6002](https://github.com/nats-io/nats-server/issues/6002))

### Added

Config
- A warning will now be logged at startup if the JetStream store directory appears to be in a temporary folder ([#5935](https://github.com/nats-io/nats-server/issues/5935))

### Improved

General
- More efficient searching of sublists for the number of subscriptions ([#5918](https://github.com/nats-io/nats-server/issues/5918))

JetStream
- Improve performance when checking interest and correcting ack state on interest-based or work queue streams ([#5963](https://github.com/nats-io/nats-server/issues/5963))
- Safer default file permissions for JetStream filestore and logs ([#6013](https://github.com/nats-io/nats-server/issues/6013))

### Fixed

JetStream
- Large number of message delete tombstones will no longer result in unusually large message blocks on disk ([#5973](https://github.com/nats-io/nats-server/issues/5973))
- The server will no longer panic when restoring corrupted subject state containing null characters ([#5978](https://github.com/nats-io/nats-server/issues/5978))
- A data race when processing append entries has been fixed ([#5970](https://github.com/nats-io/nats-server/issues/5970))
- Fix a stream desync across replicas that could occur after stalled or failed catch-ups ([#5939](https://github.com/nats-io/nats-server/issues/5939))
- Consumers will no longer race with the filestore when fetching messages, fixing some cases where consumers can get stuck with workqueue streams and max messages per subject limits ([#6003](https://github.com/nats-io/nats-server/issues/6003))
- Pull consumers will now recalculate max delivered when expiring messages, such that the redelivered status does not report incorrectly and cause a stall with a max deliver limit ([#5995](https://github.com/nats-io/nats-server/issues/5995))
- Clustered streams should no longer desync if a catch-up fails due to a loss of leader ([#5986](https://github.com/nats-io/nats-server/issues/5986))
- Fixed a panic that could occur when calculating asset placement in a JetStream cluster ([#5996](https://github.com/nats-io/nats-server/issues/5996))
- Fixed a panic when shutting down a clustered stream ([#6007](https://github.com/nats-io/nats-server/issues/6007))
- Revert earlier PR [#5785](https://github.com/nats-io/nats-server/issues/5785) to restore consumer start sequence clipping, fixing an issue where sourcing/mirroring consumers could skip messages ([#6014](https://github.com/nats-io/nats-server/issues/6014))

Leafnodes
- Load balancing of queue groups over leafnode connections ([#5982](https://github.com/nats-io/nats-server/issues/5982))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.21...v2.10.22

[Changes][v2.10.22]


<a id="v2.10.21"></a>
# [Release v2.10.21](https://github.com/nats-io/nats-server/releases/tag/v2.10.21) - 2024-09-26

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.7

### Dependencies

* golang.org/x/crypto v0.27.0 ([#5869](https://github.com/nats-io/nats-server/issues/5869))
* golang.org/x/sys v0.25.0 ([#5869](https://github.com/nats-io/nats-server/issues/5869))

### Added

Config
- New TLS `min_version` option for configuring the minimum supported TLS version ([#5904](https://github.com/nats-io/nats-server/issues/5904))

### Improved

JetStream
- Global JetStream API queue hard limit for protecting the system ([#5900](https://github.com/nats-io/nats-server/issues/5900), [#5923](https://github.com/nats-io/nats-server/issues/5923))
- Orphaned ephemeral consumer clean-up is now logged at debug level only ([#5917](https://github.com/nats-io/nats-server/issues/5917))

Monitoring
- `statsz` messages are now sent every 10 seconds instead of every 30 seconds ([#5925](https://github.com/nats-io/nats-server/issues/5925))
- Include JetStream pending API request count in `statsz` messages and `jsz` responses for monitoring ([#5923](https://github.com/nats-io/nats-server/issues/5923), [#5926](https://github.com/nats-io/nats-server/issues/5926))

### Fixed

JetStream
- Fix an issue comparing the stream configuration with the updated stream assignment on stream create ([#5854](https://github.com/nats-io/nats-server/issues/5854))
- Improvements to recovering from old or corrupted `index.db` ([#5893](https://github.com/nats-io/nats-server/issues/5893), [#5901](https://github.com/nats-io/nats-server/issues/5901), [#5907](https://github.com/nats-io/nats-server/issues/5907))
- Ensure that consumer replicas and placement are adjusted properly when scaling down a replicated stream ([#5927](https://github.com/nats-io/nats-server/issues/5927))
- Fix a panic that could occur when trying to shut down while the JetStream meta group was in the process of being set up ([#5934](https://github.com/nats-io/nats-server/issues/5934))

Monitoring
- Always update account issuer in `accountsz` ([#5886](https://github.com/nats-io/nats-server/issues/5886))

OCSP
- Fix peer validation on the HTTPS monitoring port when OCSP is enabled ([#5906](https://github.com/nats-io/nats-server/issues/5906))

Config
- Support multiple trusted operators using a config file ([#5896](https://github.com/nats-io/nats-server/issues/5896))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.20...v2.10.21

[Changes][v2.10.21]


<a id="v2.10.20"></a>
# [Release v2.10.20](https://github.com/nats-io/nats-server/releases/tag/v2.10.20) - 2024-08-29

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.6

### Fixed

JetStream
  - Fix regression in KV CAS operations on R=1 replicas introduced in v2.10.19 ([#5841](https://github.com/nats-io/nats-server/issues/5841)) Thanks to [@cbrewster](https://github.com/cbrewster) for the report!

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.19...v2.10.20


[Changes][v2.10.20]


<a id="v2.10.19"></a>
# [Release v2.10.19](https://github.com/nats-io/nats-server/releases/tag/v2.10.19) - 2024-08-27

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.6

### Dependencies
- golang.org/x/crypto v0.26.0 ([#5782](https://github.com/nats-io/nats-server/issues/5782))
- golang.org/x/sys v0.24.0 ([#5782](https://github.com/nats-io/nats-server/issues/5782))
- golang.org/x/time v0.6.0 ([#5751](https://github.com/nats-io/nats-server/issues/5751))

### Improved

General
- Reduced allocations in various code paths that check for subscription interest ([#5736](https://github.com/nats-io/nats-server/issues/5736), [#5744](https://github.com/nats-io/nats-server/issues/5744))
- Subscription matching for gateways and reply tracking has been optimized ([#5735](https://github.com/nats-io/nats-server/issues/5735))
- Client outbound queues now limit the number of flushed vectors to ensure that very large outbound buffers don’t unfairly compete with write deadlines ([#5750](https://github.com/nats-io/nats-server/issues/5750))
- In client and leafnode results cache, populate new entry after pruning ([#5760](https://github.com/nats-io/nats-server/issues/5760))
- Use newly-available generic sorting functions ([#5757](https://github.com/nats-io/nats-server/issues/5757))
- Set a HTTP read timeout on profiling, monitoring and OCSP HTTP servers ([#5790](https://github.com/nats-io/nats-server/issues/5790))
- Improve behavior of rate-limited warning logs ([#5793](https://github.com/nats-io/nats-server/issues/5793))
- Use dedicated queues for the handling of `statsz` and `profilez` system events ([#5816](https://github.com/nats-io/nats-server/issues/5816))

Clustering
- Reduce the chances of implicit routes being duplicated ([#5602](https://github.com/nats-io/nats-server/issues/5602))

JetStream
- Optimize LoadNextMsg for wildcard consumers that are consuming over a large subject space ([#5710](https://github.com/nats-io/nats-server/issues/5710))
- When `sync`/`sync_interval` is set to `always`, metadata files for streams and consumers are now written using `O_SYNC` to guarantee flushes to disk ([#5729](https://github.com/nats-io/nats-server/issues/5729))
- Walking an entire subject tree is now faster and allocates less ([#5734](https://github.com/nats-io/nats-server/issues/5734))
- Try to snapshot stream state when a change in the clustered last failed sequence is detected ([#5812](https://github.com/nats-io/nats-server/issues/5812))
- Message blocks are no longer loaded into memory unnecessarily when checking if we can skip ahead when loading the next message ([#5819](https://github.com/nats-io/nats-server/issues/5819))
- Don’t attempt to re-compact blocks that cannot be compacted, reducing unnecessary CPU usage and disk I/Os ([#5831](https://github.com/nats-io/nats-server/issues/5831))

Monitoring
- Add StreamLeaderOnly filter option to return replica results only for groups for which that node is the leader ([#5704](https://github.com/nats-io/nats-server/issues/5704))
- The `profilez` API endpoint in the system account can now acquire and return CPU profiles ([#5743](https://github.com/nats-io/nats-server/issues/5743))

Miscellaneous
- Various improvements to unit tests ([#5668](https://github.com/nats-io/nats-server/issues/5668), [#5607](https://github.com/nats-io/nats-server/issues/5607), [#5695](https://github.com/nats-io/nats-server/issues/5695), [#5706](https://github.com/nats-io/nats-server/issues/5706), [#5825](https://github.com/nats-io/nats-server/issues/5825), [#5834](https://github.com/nats-io/nats-server/issues/5834))

### Fixed
General
- Fixed a panic when looking up the account for a client ([#5713](https://github.com/nats-io/nats-server/issues/5713))
- The `ClientURL()` function now returns correctly formatted IPv6 host literals ([#5725](https://github.com/nats-io/nats-server/issues/5725))
- Fixed incorrect import cycle warnings when subject mapping is in use ([#5755](https://github.com/nats-io/nats-server/issues/5755))
- A race condition that could cause slow consumers to leave behind subscription interest after the connection has been closed has been fixed ([#5754](https://github.com/nats-io/nats-server/issues/5754))
- Corrected an off-by-one condition when growing to or shrinking from node48 in the subject tree ([#5826](https://github.com/nats-io/nats-server/issues/5826))

JetStream
- Retention issue that could cause messages to be incorrectly removed on a WorkQueuePolicy stream when consumers did not cover the entire subject space ([#5697](https://github.com/nats-io/nats-server/issues/5697))
- Fixed a panic when calling the `raftz` endpoint during shutdown ([#5672](https://github.com/nats-io/nats-server/issues/5672))
- Don’t delete NRG group persistent state on disk when failing to create subscriptions ([#5687](https://github.com/nats-io/nats-server/issues/5687))
- Fixed behavior when checking for the first block that matches a consumer subject filter ([#5709](https://github.com/nats-io/nats-server/issues/5709))
- Reduce the number of compactions made on filestore blocks due to deleted message tombstones ([#5719](https://github.com/nats-io/nats-server/issues/5719))
- Fixed `maximum messages per subject exceeded` unexpected error on streams using a max messages per subject limit of 1 and discard new retention policy ([#5761](https://github.com/nats-io/nats-server/issues/5761))
- Fixed bad meta state on restart that could cause deletion of assets ([#5767](https://github.com/nats-io/nats-server/issues/5767))
- Fixed R1 streams exceeding quota limits ([#5771](https://github.com/nats-io/nats-server/issues/5771))
- Return the correct sequence for a duplicated message on an interest policy stream when there is no interest ([#5818](https://github.com/nats-io/nats-server/issues/5818))
- Fixed setting the consumer start sequence when that sequence does not yet appear in the stream ([#5785](https://github.com/nats-io/nats-server/issues/5785))
- Connection type in scoped signing keys are now honored correctly ([#5789](https://github.com/nats-io/nats-server/issues/5789))
- Expected last sequence per subject logic has now been harmonized across clustered stream leaders and followers, fixing a potential drift ([#5794](https://github.com/nats-io/nats-server/issues/5794))
- Stream snapshots are now always installed correctly on graceful shutdown ([#5809](https://github.com/nats-io/nats-server/issues/5809))
- A data race between consumer and stream updates has been resolved ([#5820](https://github.com/nats-io/nats-server/issues/5820))
- Avoid increasing the cluster last failed sequence when the message was likely deleted ([#5821](https://github.com/nats-io/nats-server/issues/5821))

Leafnodes
- Leafnode connections will now be rejected when the cluster name contains spaces ([#5732](https://github.com/nats-io/nats-server/issues/5732))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.18...v2.10.19





[Changes][v2.10.19]


<a id="v2.10.18"></a>
# [Release v2.10.18](https://github.com/nats-io/nats-server/releases/tag/v2.10.18) - 2024-07-17

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.5

### Dependencies

- github.com/nats-io/jwt v2.5.8 ([#5618](https://github.com/nats-io/nats-server/issues/5618))
- github.com/minio/highwayhash v1.0.3 ([#5627](https://github.com/nats-io/nats-server/issues/5627))
- golang.org/x/crypto v0.25.0 ([#5627](https://github.com/nats-io/nats-server/issues/5627))
- golang.org/x/sys v0.22.0 ([#5627](https://github.com/nats-io/nats-server/issues/5627))

### Improved

Embedded
- Export server function to initiate “lame duck mode” when embedding NATS ([#5660](https://github.com/nats-io/nats-server/issues/5660))

JetStream
- CPU spike during recalculation of first message in the memory store ([#5629](https://github.com/nats-io/nats-server/issues/5629))

### Fixed
JetStream
- Fix duplicate callbacks on full wildcard match ([#5610](https://github.com/nats-io/nats-server/issues/5610))
- Multiple fixes for the filestore per-subject state ([#5616](https://github.com/nats-io/nats-server/issues/5616))
- Fix checkSkipFirstBlock which could return a negative index if the first block in the per-subject index is outdated ([#5630](https://github.com/nats-io/nats-server/issues/5630))
- Don't ack messages if consumer is filtered and they were not applicable ([#5639](https://github.com/nats-io/nats-server/issues/5639), [#5612](https://github.com/nats-io/nats-server/issues/5612), [#5638](https://github.com/nats-io/nats-server/issues/5638))
- Protect against possible panic in the filestore where the stree index is nil ([#5662](https://github.com/nats-io/nats-server/issues/5662))
- Prevent panic when shutting down a server immediately after starting it ([#5663](https://github.com/nats-io/nats-server/issues/5663))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.17...v2.10.18

[Changes][v2.10.18]


<a id="v2.10.17"></a>
# [Release v2.10.17](https://github.com/nats-io/nats-server/releases/tag/v2.10.17) - 2024-06-27

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.22.4 ([#5487](https://github.com/nats-io/nats-server/issues/5487))

### Dependencies
- golang.org/x/sys v0.21.0 ([#5508](https://github.com/nats-io/nats-server/issues/5508))
- golang.org/x/crypto v0.24.0 ([#5509](https://github.com/nats-io/nats-server/issues/5509))
- github.com/klauspost/compress v1.17.9 ([#5538](https://github.com/nats-io/nats-server/issues/5538))
- github.com/nats-io/nats.go v1.36.0 ([#5538](https://github.com/nats-io/nats-server/issues/5538))

### Added
Monitoring
- Experimental `/raftz` monitoring endpoint for retrieving internal Raft group state for diagnostic purposes ([#5530](https://github.com/nats-io/nats-server/issues/5530))

### Improved

Core
- Reorder struct fields in stree for improved memory alignment ([#5517](https://github.com/nats-io/nats-server/issues/5517))

JetStream
- Improve performance of calculating num-pending and interest state of a stream ([#5476](https://github.com/nats-io/nats-server/issues/5476))
- Improve leadership change signaling ([#5504](https://github.com/nats-io/nats-server/issues/5504), [#5505](https://github.com/nats-io/nats-server/issues/5505))
- Improved memory-based stream behavior during server restarts ([#5506](https://github.com/nats-io/nats-server/issues/5506))
- Reset election timer when leaving observer mode enabling quicker leadership hand-off ([#5516](https://github.com/nats-io/nats-server/issues/5516))
- Ensure ack processing is consistent and correct between leader and followers for replicated consumers ([#5524](https://github.com/nats-io/nats-server/issues/5524))
- Use per-subject info to speed up load-last filestore operations with wildcard filters ([#5546](https://github.com/nats-io/nats-server/issues/5546))
- Populate missing per-subject info after skipping blocks when calculating filtered pending ([#5545](https://github.com/nats-io/nats-server/issues/5545))
- Reduced time taken to process consumer deletes when there is a large gap between the consumer ack floor and the stream last sequence ([#5547](https://github.com/nats-io/nats-server/issues/5547))
- No longer retrieve the WAL state unnecessarily when installing Raft snapshots ([#5552](https://github.com/nats-io/nats-server/issues/5552))
- Increased filestore block and per-subject info cache expiry times to help improve performance on sparse streams ([#5568](https://github.com/nats-io/nats-server/issues/5568))
- Reduce allocations in isMatch in filestore/memstore ([#5573](https://github.com/nats-io/nats-server/issues/5573))
- Improved handling of out-of-date first blocks in per-subject info entries ([#5577](https://github.com/nats-io/nats-server/issues/5577))
- Use stree for message block subject indexing instead of hashmaps ([#5559](https://github.com/nats-io/nats-server/issues/5559))
- Avoid loading last message blocks on LoadNextMsg miss ([#5584](https://github.com/nats-io/nats-server/issues/5584))
- Add node48 node size to stree, providing better memory utilisation for some subject spaces ([#5585](https://github.com/nats-io/nats-server/issues/5585))
- Logging message when exceeding JetStream account limits now prints the account ([#5597](https://github.com/nats-io/nats-server/issues/5597))

Monitoring
- Rate-limit statsz updates which reduces load for very large clusters ([#5470](https://github.com/nats-io/nats-server/issues/5470), [#5485](https://github.com/nats-io/nats-server/issues/5485)) Thanks to [@wjordan](https://github.com/wjordan) for the report and contribution!

### Changed

MQTT
- Do not wait for JS responses when disconnecting the session ([#5575](https://github.com/nats-io/nats-server/issues/5575))

### Fixed

Accounts
- Import/export cycle detection ([#5494](https://github.com/nats-io/nats-server/issues/5494)) Thanks to [@semakasyrok](https://github.com/semakasyrok) for the contribution!
- Allow callout users to be revoked ([#5555](https://github.com/nats-io/nats-server/issues/5555), [#5561](https://github.com/nats-io/nats-server/issues/5561))
- Fixed a data race when updating payload limits from JWT claims ([#5593](https://github.com/nats-io/nats-server/issues/5593))

Core
- Allow client kick to also kick leafnode connections ([#5587](https://github.com/nats-io/nats-server/issues/5587))
- Fix imports sometimes not being available for a client after server restarts ([#5588](https://github.com/nats-io/nats-server/issues/5588), [#5589](https://github.com/nats-io/nats-server/issues/5589))

JetStream
- Avoid panic on corrupted TAV file ([#5464](https://github.com/nats-io/nats-server/issues/5464))
- Performance regression in `LoadNextMsg` with very sparse or no messages ([#5475](https://github.com/nats-io/nats-server/issues/5475))
- Stepdown candidate when append-entry is ahead of last log term ([#5481](https://github.com/nats-io/nats-server/issues/5481))
- Fix possible redelivery after successful ack during rollout restarts ([#5482](https://github.com/nats-io/nats-server/issues/5482))
- Fix returning maximum consumers limit reached on some consumer updates ([#5489](https://github.com/nats-io/nats-server/issues/5489))
- Fix last sequence stream reset on server restart ([#5497](https://github.com/nats-io/nats-server/issues/5497))
- Fix data between creating a consumer and determining cluster state ([#5501](https://github.com/nats-io/nats-server/issues/5501))
- Prevent interleaving of setting/unsetting observer states ([#5503](https://github.com/nats-io/nats-server/issues/5503))
- Fix accounting for consumers with a different replication factor than the parent stream ([#5521](https://github.com/nats-io/nats-server/issues/5521))
- Fix potential segfault if `mset.mirror` was nil when calculating the last loaded message ([#5522](https://github.com/nats-io/nats-server/issues/5522))
- Follower stores no longer inherit the redelivered consumer delivered sequence which could break ack gap fill ([#5533](https://github.com/nats-io/nats-server/issues/5533))
- Direct Raft proposals will no longer bypass the internal proposal queue which could cause incorrect interleaving of state ([#5543](https://github.com/nats-io/nats-server/issues/5543))
- Audit streams that capture `$JS.>`, `$JS.API.>`, `$JSC.>` and `$SYS.>` subjects are now only allowed if `NoAck` is set, avoiding potential misconfiguration that could affect the JetStream API, with the exception of the more specific `$JS.EVENT.>` and `$SYS.ACCOUNT.>` as these were allowed before ([#5548](https://github.com/nats-io/nats-server/issues/5548), [#5556](https://github.com/nats-io/nats-server/issues/5556))
- Streams from failed snapshot restores are now cleaned up correctly, fixing potential false positive warnings on `/healthz` after a failed restore ([#5549](https://github.com/nats-io/nats-server/issues/5549))
- Ensure that internal system clients used by Raft groups are always cleaned up correctly, fixing a potential memory leak ([#5566](https://github.com/nats-io/nats-server/issues/5566)) Thanks to [@slice-srinidhis](https://github.com/slice-srinidhis) for the report!
- Fixed a potential panic when updating stream sources on an existing stream ([#5571](https://github.com/nats-io/nats-server/issues/5571))
- Fixed panic when creating a stream with an incorrect mapping destination ([#5570](https://github.com/nats-io/nats-server/issues/5570), [#5571](https://github.com/nats-io/nats-server/issues/5571))
- Fixed returning error when trying to update a stream that has sources with bad subject transforms ([#5574](https://github.com/nats-io/nats-server/issues/5574))
- Fixed a bug that would return “no message found” for last_per_subject ([#5578](https://github.com/nats-io/nats-server/issues/5578))
- Correctly leave Raft observer state after applies were paused, i.e. due to a catch-up in progress ([#5586](https://github.com/nats-io/nats-server/issues/5586))
- JetStream no longer leaks memory when creating and deleting Raft groups ([#5600](https://github.com/nats-io/nats-server/issues/5600))
- Fixed a potential panic in consumer ack queue handling ([#5601](https://github.com/nats-io/nats-server/issues/5601))
- Fixed data race in runAsLeader ([#5604](https://github.com/nats-io/nats-server/issues/5604))

Leafnodes
- Prevent potential message duplication for queue-group subscriptions ([#5519](https://github.com/nats-io/nats-server/issues/5519)) Thanks to [@pcsegal](https://github.com/pcsegal) for the report!

Monitoring
- Ensure consistency of the delivered stream sequence in `/jsz` filtered consumer reporting ([#5528](https://github.com/nats-io/nats-server/issues/5528))

### Chores

Config
- Clarify comment on re-use of config `Options` type for embedded mode ([#5472](https://github.com/nats-io/nats-server/issues/5472)) Thanks to [@robinkb](https://github.com/robinkb) for the report and contribution!

JetStream
- Added additional memory-based Raft tests ([#5515](https://github.com/nats-io/nats-server/issues/5515))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.16...v2.10.17

[Changes][v2.10.17]


<a id="v2.10.16"></a>
# [Release v2.10.16](https://github.com/nats-io/nats-server/releases/tag/v2.10.16) - 2024-05-21

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

> [!WARNING]
> A possible regression may result in a server panic at startup when `tav.idx` files were incorrectly truncated down to zero bytes. You can work around this problem by deleting `tav.idx` files that are zero bytes in length before starting the server. Zero-byte files could exist as a result of a previous server crash before a successful file sync to disk occurred.

### Go Version
- 1.22.3 ([#5438](https://github.com/nats-io/nats-server/issues/5438))

### Dependencies
- github.com/nats-io/jwt/v2 v2.5.6 ([#5328](https://github.com/nats-io/nats-server/issues/5328))
- golang.org/x/sys v0.20.0 ([#5388](https://github.com/nats-io/nats-server/issues/5388))
- golang.org/x/crypto v0.23.0 ([#5413](https://github.com/nats-io/nats-server/issues/5413))

### Added
- Added Left and Right subject mapping operations ([#5337](https://github.com/nats-io/nats-server/issues/5337)) Thanks to [@sspates](https://github.com/sspates) for the contribution!
- Add a `/expvarz` monitoring endpoint ([#5374](https://github.com/nats-io/nats-server/issues/5374))

### Improved

Accounts
- Change `AccountResolver()` to use read lock to prevent contention ([#5351](https://github.com/nats-io/nats-server/issues/5351))
- Improve muxed routes with large subject space ([#5399](https://github.com/nats-io/nats-server/issues/5399))

Gateway
- Outbound may fail to detect stale connection ([#5356](https://github.com/nats-io/nats-server/issues/5356)) Thanks to [@wjordan](https://github.com/wjordan) for the report!

JetStream
- Optimize stream subject matching implementation ([#5316](https://github.com/nats-io/nats-server/issues/5316), [#5324](https://github.com/nats-io/nats-server/issues/5324), [#5329](https://github.com/nats-io/nats-server/issues/5329), [#5342](https://github.com/nats-io/nats-server/issues/5342), [#5353](https://github.com/nats-io/nats-server/issues/5353))
- Improve filestore LoadNextMsg performance ([#5401](https://github.com/nats-io/nats-server/issues/5401))
- Prevent blocking writes on meta state filestore flush ([#5333](https://github.com/nats-io/nats-server/issues/5333))
- Add logging to measure `writeFullState` and `enforceMsgPerSubjectLimit` ([#5340](https://github.com/nats-io/nats-server/issues/5340))
- Do not hold filestore lock on msg block loads when looking up the first sequence for subject ([#5363](https://github.com/nats-io/nats-server/issues/5363))
- Improve various stream sourcing and mirror behaviors and performance ([#5366](https://github.com/nats-io/nats-server/issues/5366), [#5372](https://github.com/nats-io/nats-server/issues/5372), [#5379](https://github.com/nats-io/nats-server/issues/5379), [#5389](https://github.com/nats-io/nats-server/issues/5389))
- Increase the compression threshold for Raft traffic ([#5371](https://github.com/nats-io/nats-server/issues/5371))
- Put a maximum idle flush time for the filestore ([#5370](https://github.com/nats-io/nats-server/issues/5370))
- Updated subject state expiration ([#5377](https://github.com/nats-io/nats-server/issues/5377))
- Simplify writing the full state to index.db ([#5378](https://github.com/nats-io/nats-server/issues/5378))
- Added in separate last subject timestamp to track access ([#5380](https://github.com/nats-io/nats-server/issues/5380))
- Check consumer leader status without locks ([#5386](https://github.com/nats-io/nats-server/issues/5386))
- Various Raft improvements with limited to no state ([#5427](https://github.com/nats-io/nats-server/issues/5427))
- Improved consumer with AckAll performance of first ack with large first stream sequence ([#5446](https://github.com/nats-io/nats-server/issues/5446))
- Various stream catchup improvements ([#5454](https://github.com/nats-io/nats-server/issues/5454))

WebSocket
- Improve generating INFO data to send to clients ([#5405](https://github.com/nats-io/nats-server/issues/5405))

### Fixed

Config
- Fix to properly deal with block scopes in lexer ([#5406](https://github.com/nats-io/nats-server/issues/5406), [#5431](https://github.com/nats-io/nats-server/issues/5431), [#5436](https://github.com/nats-io/nats-server/issues/5436))

JetStream
- Fix potential deadlock if a panic occurs during `calculateSyncRequest` ([#5321](https://github.com/nats-io/nats-server/issues/5321))
- Fix corner cases of subject matching ([#5318](https://github.com/nats-io/nats-server/issues/5318), [#5339](https://github.com/nats-io/nats-server/issues/5339)) Thanks to [@mihaitodor](https://github.com/mihaitodor) for the report!
- Prevent stepping down for old election terms ([#5314](https://github.com/nats-io/nats-server/issues/5314))
- Prevent WAL truncation during catch-up until after peerstate/snapshot check ([#5330](https://github.com/nats-io/nats-server/issues/5330))
- Fix various delivery counter logic ([#5338](https://github.com/nats-io/nats-server/issues/5338), [#5361](https://github.com/nats-io/nats-server/issues/5361))
- Reset election timeout only on granted vote request ([#5315](https://github.com/nats-io/nats-server/issues/5315))
- Ensure stream catchup syncs after server crash and restart ([#5362](https://github.com/nats-io/nats-server/issues/5362))
- Prevent race condition for mirroring a consumer ([#5369](https://github.com/nats-io/nats-server/issues/5369))
- Ensure messages are removed after consumer updates occur on interest-based streams ([#5384](https://github.com/nats-io/nats-server/issues/5384)) Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the report and contribution!
- Cleanup messages on interest stream after consumer interest changes ([#5385](https://github.com/nats-io/nats-server/issues/5385)) Thanks to [@tyler-eon](https://github.com/tyler-eon) for the report and [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the contribution!
- Fix potential redelivery of acked messages during server restarts ([#5419](https://github.com/nats-io/nats-server/issues/5419))
- Hold onto tombstones for previous blocks on compact ([#5426](https://github.com/nats-io/nats-server/issues/5426))
- Fixes for rescaling streams with sources ([#5428](https://github.com/nats-io/nats-server/issues/5428))

WebSocket
- Fix data races during shutdown ([#5398](https://github.com/nats-io/nats-server/issues/5398))

### Chores

- Various test improvements ([#5319](https://github.com/nats-io/nats-server/issues/5319), [#5332](https://github.com/nats-io/nats-server/issues/5332), [#5341](https://github.com/nats-io/nats-server/issues/5341))
- Document field names ([#5359](https://github.com/nats-io/nats-server/issues/5359))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.14...v2.10.16

[Changes][v2.10.16]


<a id="v2.10.14"></a>
# [Release v2.10.14](https://github.com/nats-io/nats-server/releases/tag/v2.10.14) - 2024-04-11

## Changelog

(Note there was no 2.10.13 version 🙂)

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.9 ([#5300](https://github.com/nats-io/nats-server/issues/5300))

### Dependencies
- github.com/nats-io/nats.go v1.34.1 ([#5271](https://github.com/nats-io/nats-server/issues/5271))
- golang.org/x/crypto v0.22.0 ([#5283](https://github.com/nats-io/nats-server/issues/5283))

### Improved

Auth
- Improve clone behavior to prevent unintended references ([#5246](https://github.com/nats-io/nats-server/issues/5246)) Thanks to [Trail Of Bits](https://trailofbits.com/) for the report!
- Apply constant-time evaluation of non-bcrypt passwords ([#5247](https://github.com/nats-io/nats-server/issues/5247)) Thanks to [Trail Of Bits](https://trailofbits.com/) for the report!

JetStream
- Reduce lock contention when looking up stream metadata ([#5223](https://github.com/nats-io/nats-server/issues/5223))
- Optimize matching a subject when applying per subject message limits ([#5228](https://github.com/nats-io/nats-server/issues/5228))
- Optimize waiting queue for pull consumers to reduce excessive memory and GC pressure ([#5233](https://github.com/nats-io/nats-server/issues/5233))
- Improve error handling in filestore to prevent duplicate nonces being used and ignored errors ([#5248](https://github.com/nats-io/nats-server/issues/5248)) Thanks to [Trail Of Bits](https://trailofbits.com/) for the report!
- Improve interest and workqueue state tracking to prevent stranded messages during concurrent consumer acks and stream deletes ([#5270](https://github.com/nats-io/nats-server/issues/5270))
- Introduce store method to push down and optimize multi-filter subject matching used by consumers ([#5274](https://github.com/nats-io/nats-server/issues/5274)) Thanks to [@svenfoo](https://github.com/svenfoo) for the report!
- Various improvements and fixes for clustered interest-based streams and associated consumers ([#5287](https://github.com/nats-io/nats-server/issues/5287))
- Return errors and/or adding logging for rare filestore conditions ([#5298](https://github.com/nats-io/nats-server/issues/5298))
- When explicitly syncing to the filesystem, hold the message block lock to prevent possible downstream corruption ([#5301](https://github.com/nats-io/nats-server/issues/5301), [#5303](https://github.com/nats-io/nats-server/issues/5303))

### Fixed

OS
- Fix for race checkptr panic on macOS/Darwin on Go 1.22 ([#5265](https://github.com/nats-io/nats-server/issues/5265))

Connections
- Address possible memory leak due to connections not be released ([#5244](https://github.com/nats-io/nats-server/issues/5244)) Thanks to [@davidzhao](https://github.com/davidzhao) for the report!

JetStream
- Fix incorrect subject overlapping checks that could lead to multiple consumers or streams bound to the same subjects ([#5224](https://github.com/nats-io/nats-server/issues/5224))
- Improve situations that could result in orphan messages in streams ([#5227](https://github.com/nats-io/nats-server/issues/5227))
- Protect against corrupt message block when doing indexing ([#5238](https://github.com/nats-io/nats-server/issues/5238)) Thanks to [@kylemcc](https://github.com/kylemcc) for the report!
- Fix consumer config check of max deliver when backoff is set ([#5242](https://github.com/nats-io/nats-server/issues/5242))
- Ignore Nats-Expected-* headers from source stream ([#5256](https://github.com/nats-io/nats-server/issues/5256)) Thanks to [@ramonberrutti](https://github.com/ramonberrutti) for the report and contribution!
- Add missing check that could result an extended purge or compact to fail in memory-based streams ([#5264](https://github.com/nats-io/nats-server/issues/5264))
- Fix issue that could result in skipping valid messages when loading them from the filestore ([#5266](https://github.com/nats-io/nats-server/issues/5266))
- Use cluster-scoped lock when processing a leader change ([#5267](https://github.com/nats-io/nats-server/issues/5267))
- Fix missing unlocks in filestore and streams in certain error conditions ([#5276](https://github.com/nats-io/nats-server/issues/5276)) Thanks to [Trail Of Bits](https://trailofbits.com/) for the report!
- Ensure lock is held for the duration of a filestore truncate ([#5279](https://github.com/nats-io/nats-server/issues/5279))
- Fix race condition when checking for the stream interest state ([#5290](https://github.com/nats-io/nats-server/issues/5290))
- Ensure dangling NRG directories are cleaned up when the in-memory stream/consumer are deleted ([#5291](https://github.com/nats-io/nats-server/issues/5291))
- Perform a standard stream purge when ack floor is higher than the last known state ([#5293](https://github.com/nats-io/nats-server/issues/5293))
- Handle concurrent creation of workqueue consumers that could result in overlapping interest ([#5295](https://github.com/nats-io/nats-server/issues/5295)) Thanks to [@LautaroJayat](https://github.com/LautaroJayat) for the report!
- Fix possible case of multiple deliveries of the same message that cause the delivery count decreasing ([#5305](https://github.com/nats-io/nats-server/issues/5305))

Monitoring
- Fix JSZ account filtering behavior when requesting stream details ([#5229](https://github.com/nats-io/nats-server/issues/5229))

OCSP
- Fix stapling during gateway reconnect and configuration reload ([#5208](https://github.com/nats-io/nats-server/issues/5208))

### Chores

- Fix incorrect function names in comments ([#5237](https://github.com/nats-io/nats-server/issues/5237), [#5289](https://github.com/nats-io/nats-server/issues/5289)) Thanks to [@depthlending](https://github.com/depthlending) and [@needsure](https://github.com/needsure) for the contributions!
- Improve workqueue stream sourcing tests ([#5112](https://github.com/nats-io/nats-server/issues/5112))
- Speed-up tests and fixup lint reports ([#5258](https://github.com/nats-io/nats-server/issues/5258))
- Improve hardened systemd configuration example ([#5272](https://github.com/nats-io/nats-server/issues/5272))
- Refactor LoadNextMsgMulti store tests to guard against drift ([#5275](https://github.com/nats-io/nats-server/issues/5275), [#5277](https://github.com/nats-io/nats-server/issues/5277))
- Rename orphan messages and replicas test cases ([#5292](https://github.com/nats-io/nats-server/issues/5292))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.12...v2.10.14

[Changes][v2.10.14]


<a id="v2.10.12"></a>
# [Release v2.10.12](https://github.com/nats-io/nats-server/releases/tag/v2.10.12) - 2024-03-12

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.8 ([#5175](https://github.com/nats-io/nats-server/issues/5175))

### Dependencies
- github.com/klauspost/compress v1.17.7 ([#5129](https://github.com/nats-io/nats-server/issues/5129))
- github.com/nats-io/nats.go v1.33.1 ([#5104](https://github.com/nats-io/nats-server/issues/5104))
- golang.org/x/crypto v0.21.0 ([#5199](https://github.com/nats-io/nats-server/issues/5199))
- golang.org/x/sys v0.18.0 ([#5198](https://github.com/nats-io/nats-server/issues/5198))
- github.com/nats-io/jwt/v2 v2.5.5

### Improved
General
- Optimize detecting long subjects with wildcards ([#5102](https://github.com/nats-io/nats-server/issues/5102))
- Refactor `getHeader` to improve bounds checking ([#5135](https://github.com/nats-io/nats-server/issues/5135))

JetStream
- Switch to use `fmt.Appendf` to reduce a few allocations ([#5100](https://github.com/nats-io/nats-server/issues/5100))
- Write Raft peer state and term/vote inline ([#5151](https://github.com/nats-io/nats-server/issues/5151), [#5178](https://github.com/nats-io/nats-server/issues/5178))
- Improve term management when handling Raft vote responses ([#5149](https://github.com/nats-io/nats-server/issues/5149))
- Ensure Raft applied index is only updated by upper layer ([#5152](https://github.com/nats-io/nats-server/issues/5152))
- Add I/O gate for message block `writeAt` syscall ([#5193](https://github.com/nats-io/nats-server/issues/5193))
- Lower the minimum expiry threshold to 250ms ([#5206](https://github.com/nats-io/nats-server/issues/5206))

### Fixed
General
- Fix randomisation for queue subscriptions on 32-bit platforms which could cause a panic ([#5169](https://github.com/nats-io/nats-server/issues/5169)) Thanks to [@jeremylowery](https://github.com/jeremylowery) for the report!
- Stree not matching when partial parts were compared to long fragments ([#5177](https://github.com/nats-io/nats-server/issues/5177))

Gateway
- Fix sending empty reply on gateway RMSG ([#5192](https://github.com/nats-io/nats-server/issues/5192)) Thanks to [@n-holmstedt](https://github.com/n-holmstedt) for the report!

Leafnodes
- Fix loop detection on daisy-chained leafnodes ([#5126](https://github.com/nats-io/nats-server/issues/5126))
- Make sure to not remove account mappings that just had their value changed ([#5132](https://github.com/nats-io/nats-server/issues/5132), [#5103](https://github.com/nats-io/nats-server/issues/5103))

JetStream
- Fix sending `Consumer Deleted` on peer remove ([#5111](https://github.com/nats-io/nats-server/issues/5111), [#5160](https://github.com/nats-io/nats-server/issues/5160))
- Fix memory leak during compaction within memory store ([#5116](https://github.com/nats-io/nats-server/issues/5116)) Thanks to [@stefanLeo](https://github.com/stefanLeo) for the report and contribution!
- Updating consumer config fails to check `OptStartTime` properly ([#5127](https://github.com/nats-io/nats-server/issues/5127)) Thanks to [@thed0ct0r](https://github.com/thed0ct0r) for the contribution!
- Slow ack for snapshots could cause slow consumer and client disconnect ([#5144](https://github.com/nats-io/nats-server/issues/5144))
- Fix for a test flapper with consumer expire frequency change ([#5155](https://github.com/nats-io/nats-server/issues/5155))
- Fix a potential drift that could occur when assigning last sequences to published messages on clustered streams ([#5179](https://github.com/nats-io/nats-server/issues/5179))
- Fix data race when capturing last sequence on clustered streams ([#5182](https://github.com/nats-io/nats-server/issues/5182))
- Fix lock inversion when tracking last sequence failures on clustered streams ([#5183](https://github.com/nats-io/nats-server/issues/5183))
- Revert an earlier change around the Raft handling of stepdowns due to stream move performance ([#5200](https://github.com/nats-io/nats-server/issues/5200))
- Make sure not to commit a replicated ack when the consumer is closed ([#5196](https://github.com/nats-io/nats-server/issues/5196))
- Check the Raft layer stream state once recovery is complete ([#5196](https://github.com/nats-io/nats-server/issues/5196))

OCSP
- Prefer a POST method to the OCSP server, falling back to GET ([#5109](https://github.com/nats-io/nats-server/issues/5109))
- Fixed a race condition that could affect OCSP stapling during server reloads ([#5207](https://github.com/nats-io/nats-server/issues/5207))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.11...v2.10.12

[Changes][v2.10.12]


<a id="v2.9.25"></a>
# [Release v2.9.25](https://github.com/nats-io/nats-server/releases/tag/v2.9.25) - 2024-03-01

## Changelog

### Go Version
- 1.20.14

### Improved

JetStream
- In lame duck mode, shutdown JetStream at the start to signal transfer of leadership if the leader (backported from [#4579](https://github.com/nats-io/nats-server/issues/4579))
- Prevent processing of consumer assignments after JetStream shutdown occurs (backported from [#4625](https://github.com/nats-io/nats-server/issues/4625))

### Fixed

JetStream
- Fix possible stream assignment race condition (backported from [#4589](https://github.com/nats-io/nats-server/issues/4589))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.23...v2.9.24

[Changes][v2.9.25]


<a id="v2.10.11"></a>
# [Release v2.10.11](https://github.com/nats-io/nats-server/releases/tag/v2.10.11) - 2024-02-15

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.7

### Dependencies
- github.com/nats-io/nats.go v1.33.0

### Improved
JetStream
- Optimize replaying messages when they are at the end of the stream ([#5083](https://github.com/nats-io/nats-server/issues/5083)) Thanks to [@david-wakeo](https://github.com/david-wakeo) for the report!
- Optimize replaying messages with start sequence by time when there are large gaps between the time sequence and first sequence ([#5088](https://github.com/nats-io/nats-server/issues/5088)) Thanks again to [@david-wakeo](https://github.com/david-wakeo) for the report!
- Reduce possible contention for NRG step-down ([#4990](https://github.com/nats-io/nats-server/issues/4990))
- Reduce log-level to debug for non-actionable reallocations warnings ([#5085](https://github.com/nats-io/nats-server/issues/5085))
- Improved placement of streams in larger clusters when created in rapid succession ([#5079](https://github.com/nats-io/nats-server/issues/5079)) Thanks to [@kohlisid](https://github.com/kohlisid) for the report!
- Improved filtered consumer retrieval time for large subject space streams with wildcards ([#5089](https://github.com/nats-io/nats-server/issues/5089)) 

### Fixed
JetStream
- Fixed a bug that could cause consumers to not match filtered subjects with wildcards correctly ([#5080](https://github.com/nats-io/nats-server/issues/5080))
- Fixed a bug that could not allow keys/subjects to be found after restarts after extended downtime ([#5054](https://github.com/nats-io/nats-server/issues/5054))
- Fixed a bug that would not properly update all consumer state when a filtered purge of the parent stream was narrower then the consumer’s filtered subject space ([#5075](https://github.com/nats-io/nats-server/issues/5075))

MQTT
- Fixed an issue with retained messages not working properly if the server name had a ‘.’ ([#5048](https://github.com/nats-io/nats-server/issues/5048))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.10...v2.10.11

[Changes][v2.10.11]


<a id="v2.10.10"></a>
# [Release v2.10.10](https://github.com/nats-io/nats-server/releases/tag/v2.10.10) - 2024-02-02

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.6

### Dependencies
- github.com/klauspost/compress v1.17.5
- github.com/nats-io/nats.go v1.32.0

### Added
- Add `ping_interval` cluster route option to configure differently than client connections ([#5029](https://github.com/nats-io/nats-server/issues/5029))

### Improved
JetStream
- Avoid a linear scan of filestore blocks when loading messages for the first time after restart or long period of inactivity ([#4969](https://github.com/nats-io/nats-server/issues/4969))
- NumPending calculations and subject index memory in filestore and memstore ([#4960](https://github.com/nats-io/nats-server/issues/4960), [#4983](https://github.com/nats-io/nats-server/issues/4983))
- Log healthy Raft group status when no longer falling behind ([#4995](https://github.com/nats-io/nats-server/issues/4995))
- Optimize writing full filestore state where there is high subject cardinality ([#5004](https://github.com/nats-io/nats-server/issues/5004))
- Share higher fidelity client info in JetStream advisory messages ([#5019](https://github.com/nats-io/nats-server/issues/5019), [#5026](https://github.com/nats-io/nats-server/issues/5026))
- More thorough gating of blocking disk I/O operations ([#5022](https://github.com/nats-io/nats-server/issues/5022), [#5027](https://github.com/nats-io/nats-server/issues/5027))
- Improved fallback cleanup of consumer Raft directory ([#5028](https://github.com/nats-io/nats-server/issues/5028))

MQTT
- Use different unique names for internal consumers ([#5017](https://github.com/nats-io/nats-server/issues/5017))

### Fixed
Auth callout
- Ensure the server properly binds scoped users ([#5013](https://github.com/nats-io/nats-server/issues/5013)) Thanks to [@dpotapov](https://github.com/dpotapov) for the report!

Logging
- Fix missing variable in consumer skew log line ([#4967](https://github.com/nats-io/nats-server/issues/4967))

JetStream
- Fix data race with stream stop and removal ([#4963](https://github.com/nats-io/nats-server/issues/4963))
- Don't unconditionally send consumer snapshot when becoming leader ([#4965](https://github.com/nats-io/nats-server/issues/4965))
- Fix tracking holes in the message block index when they are at the end of the block, improving filestore cache efficiency ([#4988](https://github.com/nats-io/nats-server/issues/4988))
- Wait for recovery to complete before sending snapshot on stream scale-up from R1 ([#5001](https://github.com/nats-io/nats-server/issues/5001))
- Acking a redelivered msg with more pending should move the ack floor ([#5008](https://github.com/nats-io/nats-server/issues/5008))
- Fix for a panic calculating record length for secure erase followed by compaction ([#5009](https://github.com/nats-io/nats-server/issues/5009))
- Reduce memory growth on interest stream recovery when consumers are ahead of the recovered stream state ([#5011](https://github.com/nats-io/nats-server/issues/5011))
- Raft node responds to VoteRequest with outdated Term ([#5021](https://github.com/nats-io/nats-server/issues/5021))
- Avoid sending meta leader snapshot as normal entry on leader change when not up-to-date ([#5024](https://github.com/nats-io/nats-server/issues/5024))
- Make sure to not restart streams or consumers when they are deleted or immediately after being created ([#5032](https://github.com/nats-io/nats-server/issues/5032))

Shutdown
- Avoid panic when reloading during lame duck mode ([#5012](https://github.com/nats-io/nats-server/issues/5012))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.9...v2.10.10

[Changes][v2.10.10]


<a id="v2.10.9"></a>
# [Release v2.10.9](https://github.com/nats-io/nats-server/releases/tag/v2.10.9) - 2024-01-11

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.6

### Fixed
JetStream
- Guard against potential panic when finding starting sequence for stream sources from 2.9.x servers ([#4950](https://github.com/nats-io/nats-server/issues/4950))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.8...v2.10.9



[Changes][v2.10.9]


<a id="v2.10.8"></a>
# [Release v2.10.8](https://github.com/nats-io/nats-server/releases/tag/v2.10.8) - 2024-01-10

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.6

### Dependencies
- golang.org/x/crypto v0.18.0
- golang.org/x/sys v0.16.0
- github.com/nats-io/nkeys v0.4.7

### Added
TLS
- Add 'certs' option to TLS block for multi-cert support ([#4889](https://github.com/nats-io/nats-server/issues/4889))

### Improved
General
- Random number generation now uses a faster lock-free algorithm ([#4901](https://github.com/nats-io/nats-server/issues/4901))
- no_auth_user is now allowed to be an nkey ([#4938](https://github.com/nats-io/nats-server/issues/4938))

JetStream
- Improve matching efficiency of filter subjects in consumer ([#4864](https://github.com/nats-io/nats-server/issues/4864)) Thanks to [@svenfoo](https://github.com/svenfoo) for the contribution!
- Optimize JetStream metalayer snapshots by reducing allocations and simplifying marshaling ([#4925](https://github.com/nats-io/nats-server/issues/4925))
- Micro-optimization where subject tokenization occurs ([#4880](https://github.com/nats-io/nats-server/issues/4880)) Thanks to [@svenfoo](https://github.com/svenfoo) for the contribution!
- Prevent backing up internal JS API requests in large-scale source and mirror setups ([#4884](https://github.com/nats-io/nats-server/issues/4884))
- Optimize catchups for replicas and mirrors where there are a significant number of interior deletes ([#4929](https://github.com/nats-io/nats-server/issues/4929))
- Reduce lock contention on the stream lock for some operations that could block routes & gateways ([#4933](https://github.com/nats-io/nats-server/issues/4933))
- Do not load all blocks for NumPending when delivery is LastPerSubject ([#4885](https://github.com/nats-io/nats-server/issues/4885))
- Call stream update only if the config has changed ([#4898](https://github.com/nats-io/nats-server/issues/4898))
- Prevent large memory buildup in the apply queue for NRGs during startup ([#4895](https://github.com/nats-io/nats-server/issues/4895))
- Finding the last sourced message for each source of a stream is now much faster ([#4935](https://github.com/nats-io/nats-server/issues/4935))

MQTT
- Retained messages will now be fetched concurrently for a new subscription ([#4835](https://github.com/nats-io/nats-server/issues/4835))

### Fixed
Accounts
- Guard account random number generator with mutex ([#4894](https://github.com/nats-io/nats-server/issues/4894)) Thanks to [@igorrius](https://github.com/igorrius) for the report!

JetStream
- Fix accounting for replicas and tier limits ([#4868](https://github.com/nats-io/nats-server/issues/4868), [#4909](https://github.com/nats-io/nats-server/issues/4909))
- Ensure all filter subjects across consumers are accounted for when purging a stream ([#4873](https://github.com/nats-io/nats-server/issues/4873)) Thanks to [@svenfoo](https://github.com/svenfoo) for the contribution!
- Detect corrupt psim subjects during recovery of index.db ([#4890](https://github.com/nats-io/nats-server/issues/4890))
- Don’t allow writing snapshots to disk before recovery has completed ([#4927](https://github.com/nats-io/nats-server/issues/4927))
- Reduce memory usage during purge operations by flushing cache ([#4905](https://github.com/nats-io/nats-server/issues/4905))
- Return an “Account not enabled” error when trying to access JetStream via the system account ([#4910](https://github.com/nats-io/nats-server/issues/4910))
- Reduce the number of blocks loaded into memory when doing linear scans ([#4916](https://github.com/nats-io/nats-server/issues/4916))

Leafnodes
- Mapping updates on reload for the global account are now propagated to leafnodes correctly ([#4937](https://github.com/nats-io/nats-server/issues/4937))
- Leafnode authorization now supports nkeys ([#4940](https://github.com/nats-io/nats-server/issues/4940))

MQTT
- Fixed an out-of-date error message on unsupported characters in MQTT topics ([#4903](https://github.com/nats-io/nats-server/issues/4903))

OCSP
- Default to Unknown status instead of Good for unknown status assertions ([#4917](https://github.com/nats-io/nats-server/issues/4917))
- Fixed OCSP Stapling not resuming for gateways on reload after certs change ([#4943](https://github.com/nats-io/nats-server/issues/4943))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.7...v2.10.8


[Changes][v2.10.8]


<a id="v2.10.7"></a>
# [Release v2.10.7](https://github.com/nats-io/nats-server/releases/tag/v2.10.7) - 2023-12-06

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.5

### Dependencies
- github.com/klauspost/compress v1.17.4
- golang.org/x/crypto v0.16.0
- golang.org/x/sys v0.15.0
- golang.org/x/time v0.5.0

### Improved
JetStream
- Increase minimum interval for full index.db state writes to reduce contention for high-speed ingest in large streams ([#4858](https://github.com/nats-io/nats-server/issues/4858))

### Fixed
JetStream
- Corruption of per-subject tracking on recover of bad or missing index.db ([#4851](https://github.com/nats-io/nats-server/issues/4851)) Thanks to [@oscarwcl](https://github.com/oscarwcl) for the report!
- Prevent GetSeqFromTime panic in memstore if the last sequence was deleted ([#4853](https://github.com/nats-io/nats-server/issues/4853)) Thanks to [@andreib1](https://github.com/andreib1) for the report!
- Protect against bad consumer state and high-memory usage during rollback ([#4857](https://github.com/nats-io/nats-server/issues/4857))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.6...v2.10.7



[Changes][v2.10.7]


<a id="v2.10.6"></a>
# [Release v2.10.6](https://github.com/nats-io/nats-server/releases/tag/v2.10.6) - 2023-12-01

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.4

### Dependencies
- github.com/klauspost/compress v1.17.3

### Improved

JetStream
- Added in internal filestore state checks on write and recover ([#4804](https://github.com/nats-io/nats-server/issues/4804))
- Reduce memory usage for streams with a large subject space ([#4806](https://github.com/nats-io/nats-server/issues/4806))
- Only gather subject filters if we need them ([#4820](https://github.com/nats-io/nats-server/issues/4820)) Thanks to [@svenfoo](https://github.com/svenfoo) for the contribution!
- Add pre-check when expected-last-subject-sequence header is present ([#4827](https://github.com/nats-io/nats-server/issues/4827))
- Avoid resetting WAL in RAFT layer if we already processed the message ([#4830](https://github.com/nats-io/nats-server/issues/4830))

Monitoring
- Remove `ocsp_peer_cache` from `varz` response when not applicable ([#4829](https://github.com/nats-io/nats-server/issues/4829))

### Fixed

JetStream
- Only drop firstSeq under DiscardOld policy ([#4802](https://github.com/nats-io/nats-server/issues/4802)) Thanks to [@davidmcote](https://github.com/davidmcote) for the report and contribution!
- Do not allow consumers to be updated if they have been deleted ([#4818](https://github.com/nats-io/nats-server/issues/4818)) Thanks to [@matevzmihalic](https://github.com/matevzmihalic) for the report!
- Fix potential race when starting the consumer monitor ([#4828](https://github.com/nats-io/nats-server/issues/4828))
- Fix race condition in debug print ([#4833](https://github.com/nats-io/nats-server/issues/4833))

MQTT
- Fix typo in README ([#4791](https://github.com/nats-io/nats-server/issues/4791)) Thanks to [@testwill](https://github.com/testwill) for the contribution!
- Improved large number of MQTT clients on reconnect with retain messages and larger scoped subscriptions ([#4810](https://github.com/nats-io/nats-server/issues/4810))

WebSockets
- Fix potential data race in overlapping re-use of buffers ([#4811](https://github.com/nats-io/nats-server/issues/4811)) Thanks to [@oscarwcl](https://github.com/oscarwcl) for the report!

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.5...v2.10.6


[Changes][v2.10.6]


<a id="v2.10.5"></a>
# [Release v2.10.5](https://github.com/nats-io/nats-server/releases/tag/v2.10.5) - 2023-11-09

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.4

### Dependencies
- golang.org/x/crypto v0.15.0
- golang.org/x/sys v0.14.0
- golang.org/x/time v0.4.0
- github.com/nats-io/jwt/v2 v2.5.3

### Improved
General
- Remove places where using `time.After` could cause GC pressure ([#4756](https://github.com/nats-io/nats-server/issues/4756))

JetStream
- Remove unused Observer const, add unit test to check observer applies ([#4727](https://github.com/nats-io/nats-server/issues/4727))
- Throttle writeFullState from separate goroutine ([#4731](https://github.com/nats-io/nats-server/issues/4731))
- Reduce memory usage with lots of subjects in filestore ([#4742](https://github.com/nats-io/nats-server/issues/4742))
- Resiliency when doing lots of conditional updates to a KV and restarting servers ([#4764](https://github.com/nats-io/nats-server/issues/4764))
- ​​General stability and consistency improvements for clustered streams with failure offsets during server restarts ([#4777](https://github.com/nats-io/nats-server/issues/4777))
- Improve code comments for Raft subsystem ([#4724](https://github.com/nats-io/nats-server/issues/4724))
- Optimize linear scan when looking by comparing the first seq in a block ([#4780](https://github.com/nats-io/nats-server/issues/4780))
- Move filestore cleanup to separate goroutine to make non-blocking ([#4782](https://github.com/nats-io/nats-server/issues/4782))
- Move deletion of filestore files to separate goroutine to make non-blocking ([#4783](https://github.com/nats-io/nats-server/issues/4783))

Monitoring
- Better check for standalone mode when determining to send statsz ([#4757](https://github.com/nats-io/nats-server/issues/4757))

MQTT
- Add "clean" flag in trace message ([#4740](https://github.com/nats-io/nats-server/issues/4740))

WebSocket
- ​​Check for `/leafnode` suffix path on leaf WebSocket connection ([#4774](https://github.com/nats-io/nats-server/issues/4774))

### Fixed
Accounts
- Fix panic in JWT permissions template handling ([#4730](https://github.com/nats-io/nats-server/issues/4730))

Leafnode
- Fix subpath concatenation used for WebSocket remote connect URL ([#4770](https://github.com/nats-io/nats-server/issues/4770)) Thanks to [@yoadey](https://github.com/yoadey) for the report!

JetStream
- Remove the state check in the runAs loops except for runAsLeader ([#4725](https://github.com/nats-io/nats-server/issues/4725))
- Make sure to properly remove meta files for filestore after conversion from 2.9.x to 2.10.x ([#4733](https://github.com/nats-io/nats-server/issues/4733))
- Make sure we check limits when scaling up a stream ([#4738](https://github.com/nats-io/nats-server/issues/4738))
- Improve estimation on full state allocations to avoid reallocations in filestore ([#4743](https://github.com/nats-io/nats-server/issues/4743))
- Make access to message block first and last sequence consistently use atomics ([#4744](https://github.com/nats-io/nats-server/issues/4744))
- Fix `DiscardNew` exceed bytes calculation ([#4772](https://github.com/nats-io/nats-server/issues/4772)) Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the contribution! Thanks to [@davidmcote](https://github.com/davidmcote) for the report!
- Fix data race and possible panic when compacting ([#4773](https://github.com/nats-io/nats-server/issues/4773), [#4776](https://github.com/nats-io/nats-server/issues/4776))
- Fix panic in `fileStore.Stop()` ([#4779](https://github.com/nats-io/nats-server/issues/4779))

MQTT
- Rapid load-balanced (re-)CONNECT to cluster causes races ([#4734](https://github.com/nats-io/nats-server/issues/4734))
- Potential deadlock between JS API and mqttDeliverMsgCbQoS0 ([#4760](https://github.com/nats-io/nats-server/issues/4760))

WebSocket
- Partial writes may lead to disconnect ([#4755](https://github.com/nats-io/nats-server/issues/4755))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.4...v2.10.5


[Changes][v2.10.5]


<a id="v2.9.24"></a>
# [Release v2.9.24](https://github.com/nats-io/nats-server/releases/tag/v2.9.24) - 2023-11-08

## Changelog

### Go Version
- 1.20.11

### Improved

JetStream
- Stricter management of Raft state, which should improve recovery from a leaderless state ([#4684](https://github.com/nats-io/nats-server/issues/4684) backport via [#4737](https://github.com/nats-io/nats-server/issues/4737))
- Remove unused Observer const, add unit test to check observer applies ([#4727](https://github.com/nats-io/nats-server/issues/4727) backport via [#4737](https://github.com/nats-io/nats-server/issues/4737))

### Fixed

Accounts
- Fix panic in JWT permissions template handling ([#4730](https://github.com/nats-io/nats-server/issues/4730) backport via [#4759](https://github.com/nats-io/nats-server/issues/4759))

WebSocket
- Partial writes may lead to disconnect ([#4755](https://github.com/nats-io/nats-server/issues/4755) backport via [#4759](https://github.com/nats-io/nats-server/issues/4759))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.23...v2.9.24


[Changes][v2.9.24]


<a id="v2.10.4"></a>
# [Release v2.10.4](https://github.com/nats-io/nats-server/releases/tag/v2.10.4) - 2023-10-27

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### CVEs
- [CVE-2023-46129](https://advisories.nats.io/CVE/secnote-2023-02.txt) - nkeys: xkeys seal encryption used fixed key for all encryption

### Go Version
- 1.21.3

### Dependencies
- github.com/nats-io/nats.go v1.31.0
- github.com/nats-io/nkeys v0.4.6
- github.com/klauspost/compress v1.17.2
- golang.org/x/crypto v0.14.0
- golang.org/x/sys v0.13.0

### Added
JetStream
- Report Raft group name in stream and consumer info responses ([#4661](https://github.com/nats-io/nats-server/issues/4661))

MQTT
- Add config options to disable QoS 2 support ([#4705](https://github.com/nats-io/nats-server/issues/4705))

TLS
- Add opt-in TLS handshake first for client connections ([#4642](https://github.com/nats-io/nats-server/issues/4642)) 

### Improved
Dependencies
- Remove unnecessary constraints dependency for `ordered` constraint ([#4709](https://github.com/nats-io/nats-server/issues/4709)) Thanks to [@misterpickypants](https://github.com/misterpickypants) for the contribution!

JetStream
- Add internal pprof labels as metadata to the stream config for improved debuggability ([#4662](https://github.com/nats-io/nats-server/issues/4662))
- Stricter management of Raft state, which should improve recovery from a leaderless state ([#4684](https://github.com/nats-io/nats-server/issues/4684))
- Avoid unnecessary reallocations when writing the full filestore state to disk ([#4687](https://github.com/nats-io/nats-server/issues/4687))
- Improve recovery of blocks that are being updated midway ([#4692](https://github.com/nats-io/nats-server/issues/4692))
- Recycle filestore buffers on rebuild and write out full state prior to snapshotting ([#4699](https://github.com/nats-io/nats-server/issues/4699))
- Extend AckTerm advisory event to support a *reason* ([#4697](https://github.com/nats-io/nats-server/issues/4697))
- Improve time to select skip list and starting sequence number for deliver last by subject ([#4712](https://github.com/nats-io/nats-server/issues/4712), [#4713](https://github.com/nats-io/nats-server/issues/4713)) Thanks to [@StanEgo](https://github.com/StanEgo) for the report!
- Optimize loading messages on last by subject if max messages per subject is one ([#4714](https://github.com/nats-io/nats-server/issues/4714))

MQTT
- No longer require a server name to be set for a standalone server ([#4679](https://github.com/nats-io/nats-server/issues/4679))

Routes
- Remove unnecessary account lookups for pinned accounts ([#4686](https://github.com/nats-io/nats-server/issues/4686))
- Upgrade non-solicited routes if present in config ([#4701](https://github.com/nats-io/nats-server/issues/4701), [#4708](https://github.com/nats-io/nats-server/issues/4708))

Systemd
- Use correct network target to prevent host-dependent race conditions when establishing external connections ([#4676](https://github.com/nats-io/nats-server/issues/4676))

### Fixed
Configuration
- Fix possible panic during configuration reload during a server shutdown ([#4666](https://github.com/nats-io/nats-server/issues/4666))

Exports/imports
- Prevent service import from duplicating MSG as HMSG with a remapped subject ([#4678](https://github.com/nats-io/nats-server/issues/4678)) Thanks to [@izwerg](https://github.com/izwerg) for the report!

JetStream
- Fix panic if store error occurs when requesting consumer info ([#4669](https://github.com/nats-io/nats-server/issues/4669))
- Fix incorrect calculation of num pending with a filtered subject ([#4693](https://github.com/nats-io/nats-server/issues/4693)) Thanks to [@a-h](https://github.com/a-h) for the report!
- Prevent purge of entire stream when targeting a sequence of `1` ([#4698](https://github.com/nats-io/nats-server/issues/4698)) Thanks to [@john-bagatta](https://github.com/john-bagatta) for the report!
- Ensure there is a valid messages queue prior to processing within a mirror ([#4700](https://github.com/nats-io/nats-server/issues/4700))
- Avoid concurrent consumer setLeader calls resulting in chance of multiple leaders ([#4703](https://github.com/nats-io/nats-server/issues/4703))

MQTT
- Fix memory leak for retained messages ([#4665](https://github.com/nats-io/nats-server/issues/4665)) Thanks to [@pricelessrabbit](https://github.com/pricelessrabbit) for the contribution!

Windows
- Ensure signal handler is stopped when shutting down on Windows to prevent goroutine leak ([#4690](https://github.com/nats-io/nats-server/issues/4690))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.3...v2.10.4



[Changes][v2.10.4]


<a id="v2.10.3"></a>
# [Release v2.10.3](https://github.com/nats-io/nats-server/releases/tag/v2.10.3) - 2023-10-12

## Changelog

Refer to the [2.10 Upgrade Guide](https://docs.nats.io/release-notes/whats_new/whats_new_210) for backwards compatibility notes with 2.9.x.

### Go Version
- 1.21.3

### Fixed

JetStream
- Reclaim more space with streams having many interior deletes during compaction with compression enabled ([#4645](https://github.com/nats-io/nats-server/issues/4645))
- Fixed updating a non unique consumer on workqueue stream not returning an error. Thanks to [@mdawar](https://github.com/mdawar) for the contribution ([#4654](https://github.com/nats-io/nats-server/issues/4654))
- Stream / KV lookups fail after decreasing history size ([#4656](https://github.com/nats-io/nats-server/issues/4656))
- Only mark fs as dirty vs full write on mb compaction ([#4657](https://github.com/nats-io/nats-server/issues/4657))

MQTT
- Fix crash in MQTT layer with outgoing PUBREL header ([#4646](https://github.com/nats-io/nats-server/issues/4646))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.2...v2.10.3


[Changes][v2.10.3]


<a id="v2.9.23"></a>
# [Release v2.9.23](https://github.com/nats-io/nats-server/releases/tag/v2.9.23) - 2023-10-12

## Changelog

### Go Version
- 1.20.10

### Fixed

Accounts
- Prevent bypassing authorization block when enabling system account access in accounts block ([#4605](https://github.com/nats-io/nats-server/issues/4605)). Backport from v2.10.2

Leafnodes
- Prevent a leafnode cluster from receiving a message multiple times in a queue subscription ([#4578](https://github.com/nats-io/nats-server/issues/4578)). Backport from v2.10.2

JetStream
- Hold lock when calculating the first message for subject in a message block ([#4531](https://github.com/nats-io/nats-server/issues/4531)). Backport from v2.10.0
- Add self-healing mechanism to detect and delete orphaned Raft groups ([#4647](https://github.com/nats-io/nats-server/issues/4647)). Backport from v2.10.0
- Prevent forward proposals in consumers after scaling down a stream ([#4647](https://github.com/nats-io/nats-server/issues/4647)). Backport from v2.10.0
- Fix race condition during leader failover scenarios resulting in potential duplicate messages being sourced ([#4592](https://github.com/nats-io/nats-server/issues/4592)). Backport from v2.10.2

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.22...v2.9.23


[Changes][v2.9.23]


<a id="v2.10.2"></a>
# [Release v2.10.2](https://github.com/nats-io/nats-server/releases/tag/v2.10.2) - 2023-10-06

## Changelog

### Downgrade compatibility note

2.10.x brings on-disk storage changes which bring significant performance improvements. Upgrade existing server versions will handle the new storage format transparently. However, if a downgrade from 2.10.x occurs, the old version will not understand the format on disk with the exception 2.9.22 and any subsequent patch releases for 2.9. So if you upgrade from 2.9.x to 2.10.0 and then need to downgrade for some reason, it must be back to 2.9.22+ to ensure the stream data can be read correctly.

### Go Version
- 1.21.2

### Dependencies
- github.com/nats-io/nats.go v1.30.2

### Added

Profiling
- Add `prof_block_rate` config option for configuring the block profile ([#4587](https://github.com/nats-io/nats-server/issues/4587))
- Add more pprof labels to consumers, sources, and mirrors ([#4609](https://github.com/nats-io/nats-server/issues/4609))

### Improved

Core
- Reduce contention when pattern matching subjects when the sublist cache is disabled ([#4586](https://github.com/nats-io/nats-server/issues/4586))
- Various service import reply optimizations ([#4591](https://github.com/nats-io/nats-server/issues/4591))
- Remove unnecessary lock on subscription list if cache is disabled ([#4594](https://github.com/nats-io/nats-server/issues/4594))

Docs
- Fix links in various repo markdown files ([#4590](https://github.com/nats-io/nats-server/issues/4590)) Thanks to [@jdhenke](https://github.com/jdhenke) for the contribution!

Leafnodes
- Set S2 writer concurrency to 1 rather than the default of GOMAXPROCS to improve performance ([#4570](https://github.com/nats-io/nats-server/issues/4570))

JetStream
- Make install snapshot errors rate limited when catching up ([#4574](https://github.com/nats-io/nats-server/issues/4574))
- Log a warning on reset if bad stream state is detected ([#4583](https://github.com/nats-io/nats-server/issues/4583))
- Change some contended locks to atomic swap operations ([#4585](https://github.com/nats-io/nats-server/issues/4585))
- Log a warning if filestore recovery fails on the happy path ([#4599](https://github.com/nats-io/nats-server/issues/4599))
- Ensure concurrent stream of the same stream does not return not found ([#4600](https://github.com/nats-io/nats-server/issues/4600))
- Add additional markers for indicating unflushed state ([#4601](https://github.com/nats-io/nats-server/issues/4601))
- Log a warning when subject skew is detected in the filestore ([#4606](https://github.com/nats-io/nats-server/issues/4606))
- Reduce contention for a high number of connections in JetStream enabled account ([#4613](https://github.com/nats-io/nats-server/issues/4613))
- Reduce contention in the consumer info API ([#4615](https://github.com/nats-io/nats-server/issues/4615))
- Reduce contention and increase throughput of replica synchronization ([#4621](https://github.com/nats-io/nats-server/issues/4621))

Systemd
- Update systemd scripts to use SIGUSR2 (lame duck model) for shutdown ([#4603](https://github.com/nats-io/nats-server/issues/4603))

WebSocket
- Minimize memory growth for compressed WebSocket connections ([#4620](https://github.com/nats-io/nats-server/issues/4620))
- Significantly reduce allocations in WebSocket interface ([#4623](https://github.com/nats-io/nats-server/issues/4623))

### Fixed

Accounts
- Fix inversion of lock on startup when setting up the account resolver ([#4588](https://github.com/nats-io/nats-server/issues/4588))
- Prevent bypassing authorization block when enabling system account access in accounts block ([#4605](https://github.com/nats-io/nats-server/issues/4605)) Thanks to [@alexherington](https://github.com/alexherington) for the report!

Leafnodes
- Prevent a leafnode cluster from receiving a message multiple times in a queue subscription ([#4578](https://github.com/nats-io/nats-server/issues/4578)) Thanks to [@pcsegal](https://github.com/pcsegal) for the report!

JetStream
- Fix possible panic due to message block unlock occurring prematurely ([#4571](https://github.com/nats-io/nats-server/issues/4571))
- Guard against an accounting error resulting in a negative message count ([#4575](https://github.com/nats-io/nats-server/issues/4575))
- Skip enabling direct gets if no commits ([#4576](https://github.com/nats-io/nats-server/issues/4576))
- In lame duck mode, shutdown JetStream at the start to signal transfer of leadership if the leader ([#4579](https://github.com/nats-io/nats-server/issues/4579))
- Fix possible stream assignment race condition ([#4589](https://github.com/nats-io/nats-server/issues/4589))
- Fix race condition during leader failover scenarios resulting in potential duplicate messages being sourced ([#4592](https://github.com/nats-io/nats-server/issues/4592))
- Respond with “not found” for consumer info if consumer is closed ([#4610](https://github.com/nats-io/nats-server/issues/4610))
- Prevent processing of consumer assignments after JetStream shutdown occurs ([#4625](https://github.com/nats-io/nats-server/issues/4625))
- Fix possibly lookup misses when MaxMsgsPerSubject=1 leading to excess messages in stream ([#4631](https://github.com/nats-io/nats-server/issues/4631))

MQTT
- Fix PUBREL header incompatibility ([#4616](https://github.com/nats-io/nats-server/issues/4616))

Routes
- Fix potential of pinned accounts not establishing a route on connect ([#4602](https://github.com/nats-io/nats-server/issues/4602))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.1...v2.10.2


[Changes][v2.10.2]


<a id="v2.10.1"></a>
# [Release v2.10.1](https://github.com/nats-io/nats-server/releases/tag/v2.10.1) - 2023-09-20

## Changelog

### Downgrade compatibility note

2.10.x brings on-disk storage changes which bring significant performance improvements. Upgrade existing server versions will handle the new storage format transparently. However, if a downgrade from 2.10.x occurs, the old version will not understand the format on disk with the exception 2.9.22 and any subsequent patch releases for 2.9. So if you upgrade from 2.9.x to 2.10.0 and then need to downgrade for some reason, it must be back to 2.9.22+ to ensure the stream data can be read correctly.

### Go Version
- 1.21.1

### Fixed

Leafnode
- Fix TLS handshake being prevented if remote (leaf) does not have a TLS block configured ([#4565](https://github.com/nats-io/nats-server/issues/4565))

JetStream
- Ensure a single filter in new consumer SubjectFilters or stream SubjectTransforms block uses the extended consumer subject format as it did with SubjectFilter ([#4564](https://github.com/nats-io/nats-server/issues/4564))
- Ensure stream-specified consumer limits are correctly applied in combination with the explicit ack policy ([#4567](https://github.com/nats-io/nats-server/issues/4567))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.10.0...v2.10.1


[Changes][v2.10.1]


<a id="v2.10.0"></a>
# [Release v2.10.0](https://github.com/nats-io/nats-server/releases/tag/v2.10.0) - 2023-09-19

## Changelog

### Downgrade compatibility note

2.10.0 brings on-disk storage changes which bring significant performance improvements. Upgrade existing server versions will handle the new storage format transparently. However, if a downgrade from 2.10.0 occurs, the old version will not understand the format on disk with the exception 2.9.22 and any subsequent patch releases for 2.9. So if you upgrade from 2.9.x to 2.10.0 and then need to downgrade for some reason, it must be back to 2.9.22+ to ensure the stream data can be read correctly.

### Go Version
- 1.21.1

### Dependencies
- github.com/nats-io/nats.go v1.29.0
- github.com/nats-io/jwt/v2 v2.5.2
- github.com/nats-io/nkeys v0.4.5
- github.com/klauspost/compress v1.17.0
- golang.org/x/crypto v0.13.0

### Added

Accounts
- Add `$SYS.REQ.USER.INFO` NATS endpoint for user info ([#3671](https://github.com/nats-io/nats-server/issues/3671))

Auth
- Authorization callout extension for delegating to external auth providers ([#3719](https://github.com/nats-io/nats-server/issues/3719), [#3784](https://github.com/nats-io/nats-server/issues/3784), [#3799](https://github.com/nats-io/nats-server/issues/3799), [#3864](https://github.com/nats-io/nats-server/issues/3864), [#3987](https://github.com/nats-io/nats-server/issues/3987), [#4501](https://github.com/nats-io/nats-server/issues/4501), [#4544](https://github.com/nats-io/nats-server/issues/4544))

Builds
- Add early build support for NetBSD ([#3526](https://github.com/nats-io/nats-server/issues/3526)) Thanks to [@MatthiasPetermann](https://github.com/MatthiasPetermann) for the contribution!
- Add early build support for IBM z/OS ([#4209](https://github.com/nats-io/nats-server/issues/4209)) Thanks to [@v1gnesh](https://github.com/v1gnesh) for the contribution!

Cluster
- Multiple routes and ability to have per-account routes to reduce head-of-line blocking in clustered setups ([#4001](https://github.com/nats-io/nats-server/issues/4001), [#4183](https://github.com/nats-io/nats-server/issues/4183), [#4414](https://github.com/nats-io/nats-server/issues/4414))
- Support for S2 compression of traffic over route connections ([#4115](https://github.com/nats-io/nats-server/issues/4115), [#4137](https://github.com/nats-io/nats-server/issues/4137))

Config
- Reload server config by sending a message in the system account to `$SYS.REQ.SERVER.{server-id}.RELOAD` ([#4307](https://github.com/nats-io/nats-server/issues/4307))

Embedded
- Add `ConnectionDeadline` field to `User` to force server disconnect after deadline ([#3580](https://github.com/nats-io/nats-server/issues/3580), [#3674](https://github.com/nats-io/nats-server/issues/3674))

Leafnode
- Add TLSHandshakeFirst option to perform a TLS handshake before sending connection info ([#4119](https://github.com/nats-io/nats-server/issues/4119))
- Support S2 compression of traffic over leafnode connections where the default now is `s2_auto` to compress relative to the RTT of the hub ([#4167](https://github.com/nats-io/nats-server/issues/4167), [#4230](https://github.com/nats-io/nats-server/issues/4230))
- Allow remotes from same server binding to same hub account ([#4259](https://github.com/nats-io/nats-server/issues/4259))

Logging
- Add `logfile_max_num` server config field to auto-rotate files ([#4548](https://github.com/nats-io/nats-server/issues/4548)) 

JetStream
- Add stream subject transforms ([#3814](https://github.com/nats-io/nats-server/issues/3814), [#3823](https://github.com/nats-io/nats-server/issues/3823), [#3827](https://github.com/nats-io/nats-server/issues/3827), [#4035](https://github.com/nats-io/nats-server/issues/4035), [#4354](https://github.com/nats-io/nats-server/issues/4354), [#4400](https://github.com/nats-io/nats-server/issues/4400), [#4403](https://github.com/nats-io/nats-server/issues/4403), [#4512](https://github.com/nats-io/nats-server/issues/4512))
- Add freeform `metadata` field to stream and consumer configs ([#3797](https://github.com/nats-io/nats-server/issues/3797))
- Add support for consumers filtering on multiple subjects ([#3500](https://github.com/nats-io/nats-server/issues/3500), [#3865](https://github.com/nats-io/nats-server/issues/3865), [#4008](https://github.com/nats-io/nats-server/issues/4008), [#4129](https://github.com/nats-io/nats-server/issues/4129), [#4188](https://github.com/nats-io/nats-server/issues/4188))
- Add original timestamp as header to republished message ([#3933](https://github.com/nats-io/nats-server/issues/3933)) Thanks to [@amorey](https://github.com/amorey) for the contribution!
- Allow republish for mirroring/sourcing streams ([#4010](https://github.com/nats-io/nats-server/issues/4010))
- Add optional S2 stream compression for file store-backed streams ([#4004](https://github.com/nats-io/nats-server/issues/4004), [#4072](https://github.com/nats-io/nats-server/issues/4072))
- Add file store ability to re-encrypt with new encryption keys ([#4296](https://github.com/nats-io/nats-server/issues/4296))
- Add embedded option to disable JetStream ASCII art at startup ([#4261](https://github.com/nats-io/nats-server/issues/4261)) Thanks to [@renevo](https://github.com/renevo) for the contribution!
- Add ability to configure `first_seq` when creating streams ([#4322](https://github.com/nats-io/nats-server/issues/4322), [#4345](https://github.com/nats-io/nats-server/issues/4345))
- Add `sync_internal` option to JetStream config ([#4483](https://github.com/nats-io/nats-server/issues/4483))

Monitoring
- Add `unique_tag` field in `/jsz` and `/varz` endpoints ([#3617](https://github.com/nats-io/nats-server/issues/3617))
- Add `$SYS.REQ.SERVER.PING.IDZ` NATS endpoint for basic server info ([#3663](https://github.com/nats-io/nats-server/issues/3663))
- Add `$SYS.REQ.SERVER.<id>.PROFILEZ` NATS endpoint for requesting debugging profiles ([#3774](https://github.com/nats-io/nats-server/issues/3774))
- Add subscription count to `/statz` endpoint ([#3875](https://github.com/nats-io/nats-server/issues/3875))
- Add Raft query parameter to `/jsz` to include Raft group info ([#3914](https://github.com/nats-io/nats-server/issues/3914))
- Add `slow_consumer_stats` to the `/varz` endpoint ([#4330](https://github.com/nats-io/nats-server/issues/4330))

MQTT
- Add support for QoS2 exactly-once delivery ([#4349](https://github.com/nats-io/nats-server/issues/4349), [#4440](https://github.com/nats-io/nats-server/issues/4440))

Reload
- Match `--signal` PIDs with globular-style expression ([#4370](https://github.com/nats-io/nats-server/issues/4370)) Thanks to [@jevolk](https://github.com/jevolk) for the contribution!

Subject Mapping
- Add ability to remove wildcard tokens in subject transforms ([#4152](https://github.com/nats-io/nats-server/issues/4152))
- Allows cluster filtering in account subject mapping ([#4175](https://github.com/nats-io/nats-server/issues/4175))

System Services
- Add `$SYS.REQ.SERVER.<id>.KICK` NATS endpoint to disconnect a client by `id` or by `name` from the target server ([#4298](https://github.com/nats-io/nats-server/issues/4298))
- Add `$SYS.REQ.SERVER.<id>.LDM` NATS endpoint that sends a “lame duck mode” message to a client by `id` or `name` on the target server ([#4298](https://github.com/nats-io/nats-server/issues/4298))

Windows
- Add NATS_STARTUP_DELAY env for configurable startup time ([#3743](https://github.com/nats-io/nats-server/issues/3743)) Thanks to [@Alberic-Hardis](https://github.com/Alberic-Hardis) for the contribution!

### Improved

Leafnodes
- Add jitter to leafnode reconnections ([#4398](https://github.com/nats-io/nats-server/issues/4398))

Logging
- Add account, stream and consumer name to consumer alignment cleanup warning ([#3666](https://github.com/nats-io/nats-server/issues/3666)) Thanks to [@ch629](https://github.com/ch629) for the contribution!

JetStream
- Significant optimisations and reduced memory impact for replicated streams with a large number of interior deletes (common in large KVs), considerably reducing the amount of CPU and memory required to create stream snapshots and smoothing out publish latencies ([#4070](https://github.com/nats-io/nats-server/issues/4070), [#4071](https://github.com/nats-io/nats-server/issues/4071), [#4075](https://github.com/nats-io/nats-server/issues/4075), [#4284](https://github.com/nats-io/nats-server/issues/4284), [#4520](https://github.com/nats-io/nats-server/issues/4520), [#4553](https://github.com/nats-io/nats-server/issues/4553))
- Improve signaling mechanism for consumers to improve performance and reduce latency ([#3706](https://github.com/nats-io/nats-server/issues/3706))
- Allow edit of Stream RePublish ([#3811](https://github.com/nats-io/nats-server/issues/3811))
- Add batch completed status to pull consumers ([#3822](https://github.com/nats-io/nats-server/issues/3822))
- Improve behavior of stream source consumer creation or config updates on leadership change ([#4009](https://github.com/nats-io/nats-server/issues/4009))
- Record the stream and consumer info timestamps ([#4133](https://github.com/nats-io/nats-server/issues/4133))
- Allow switching between limits and interest retention policies ([#4361](https://github.com/nats-io/nats-server/issues/4361))
- Improve performance of deleting blocks ([#4371](https://github.com/nats-io/nats-server/issues/4371))
- Update the way meta indexing is handled for filestore, significantly reducing time to recover streams at startup ([#4450](https://github.com/nats-io/nats-server/issues/4450), [#4481](https://github.com/nats-io/nats-server/issues/4481))
- Add self-healing mechanism to detect and delete orphaned Raft groups ([#4510](https://github.com/nats-io/nats-server/issues/4510))
- Improve monitoring of consumers that need to be cleaned up ([#4536](https://github.com/nats-io/nats-server/issues/4536))

MQTT
- Optimize retained messages by using KV semantics instead of holding retained messages in memory ([#4199](https://github.com/nats-io/nats-server/issues/4199), [#4228](https://github.com/nats-io/nats-server/issues/4228))
- Support for topics with `.` character ([#4243](https://github.com/nats-io/nats-server/issues/4243)) Thanks to [@petedavis](https://github.com/petedavis) and [@telemac](https://github.com/telemac) for the reports!
- Set the `RETAIN` flag when delivering to new subscriptions and clear the flag in all other conditions ([#4443](https://github.com/nats-io/nats-server/issues/4443))

Profiling
- Annotate CPU and goroutine profiles with additional asset information to assist with debugging ([#4204](https://github.com/nats-io/nats-server/issues/4204))
- Remove unused block profile rate ([#4402](https://github.com/nats-io/nats-server/issues/4402))

Subject Mapping
- Subject transform validation and error reporting ([#4202](https://github.com/nats-io/nats-server/issues/4202))

### Fixed

Accounts
- Fix data race when updating accounts ([#4435](https://github.com/nats-io/nats-server/issues/4435), [#4550](https://github.com/nats-io/nats-server/issues/4550), [#4561](https://github.com/nats-io/nats-server/issues/4561)) Thanks to [@hspaay](https://github.com/hspaay) for the report!

Clients
- Check if client connection name was already set when storing it ([#3824](https://github.com/nats-io/nats-server/issues/3824))

Leafnode
- Data race during validation and setup ([#4194](https://github.com/nats-io/nats-server/issues/4194))

JetStream
- Check for invalid stream name in sources ([#4222](https://github.com/nats-io/nats-server/issues/4222))
- Stream config update idempotency ([#4292](https://github.com/nats-io/nats-server/issues/4292))
- Seqset encode bug that could cause bad stream state snapshots ([#4348](https://github.com/nats-io/nats-server/issues/4348))
- Ensure stream assignment is set when checking replica count and updating retention ([#4391](https://github.com/nats-io/nats-server/issues/4391))
- Hold lock when enforcing message limit on startup ([#4469](https://github.com/nats-io/nats-server/issues/4469))
- Fix filestore data race on hash during snapshots ([#4470](https://github.com/nats-io/nats-server/issues/4470))
- Use write lock for memory store filtered state ([#4498](https://github.com/nats-io/nats-server/issues/4498))
- Fix data race on stream’s clustered filestore sequence ([#4508](https://github.com/nats-io/nats-server/issues/4508))
- Fix possible panic when recalculating the first sequence of a subject ([#4530](https://github.com/nats-io/nats-server/issues/4530)) Thanks to [@aldiesel](https://github.com/aldiesel) for the report!
- Fix leaking timers in stream sources resulting in runaway CPU usage ([#4532](https://github.com/nats-io/nats-server/issues/4532))
- Fix possible panic when consumer is not closed ([#4541](https://github.com/nats-io/nats-server/issues/4541))
- Fix data race when accessing consumer assignment ([#4547](https://github.com/nats-io/nats-server/issues/4547))
- Fix data race when changing stream retention policy ([#4551](https://github.com/nats-io/nats-server/issues/4551))
- Fix data race when loading the next message in memory-based streams ([#4552](https://github.com/nats-io/nats-server/issues/4552))
- Prevent forward proposals in consumers after scaling down a stream ([#4556](https://github.com/nats-io/nats-server/issues/4556))

OSCP
- Fixed local issuer determination for OCSP Staple ([#4355](https://github.com/nats-io/nats-server/issues/4355))

Routes
- Update LastActivity on connect for routes ([#4415](https://github.com/nats-io/nats-server/issues/4415))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.22...v2.10.0



[Changes][v2.10.0]


<a id="v2.9.22"></a>
# [Release v2.9.22](https://github.com/nats-io/nats-server/releases/tag/v2.9.22) - 2023-09-06

## Changelog

### Go Version
- 1.20.8 (updated out-of-cycle since Go 1.19 is now EOL)

### Dependencies
- github.com/nats-io/jwt/v2 v2.5.0
- golang.org/x/crypto v0.12.0
- golang.org/x/sys v0.11.0

### Improved
Monitoring
- CORS Allow-Origin passthrough for monitoring server ([#4423](https://github.com/nats-io/nats-server/issues/4423)) Thanks to [@mdawar](https://github.com/mdawar) for the contribution!

JetStream
- Improve consumer scaling reliability with filters and cluster restart ([#4404](https://github.com/nats-io/nats-server/issues/4404))
- Send event on lame duck mode (LDM) to avoid placing assets on shutting down nodes ([#4405](https://github.com/nats-io/nats-server/issues/4405))
- Skip filestore tombstones if downgrade from 2.10 occurs ([#4452](https://github.com/nats-io/nats-server/issues/4452))
- Adjust delivered and waiting count when consumer message delivery fails ([#4472](https://github.com/nats-io/nats-server/issues/4472))

### Fixed
Config
- Allow empty configs and fix JSON compatibility ([#4394](https://github.com/nats-io/nats-server/issues/4394), [#4418](https://github.com/nats-io/nats-server/issues/4418))
- Remove TLS OCSP debug log on reload ([#4453](https://github.com/nats-io/nats-server/issues/4453))

Monitoring
- Fix Content-Type header when /healthz is not 200 OK ([#4437](https://github.com/nats-io/nats-server/issues/4437)) Thanks to [@mdawar](https://github.com/mdawar) for the contribution!
- Fix server /connz idle time sorting ([#4463](https://github.com/nats-io/nats-server/issues/4463)) Thanks to [@mdawar](https://github.com/mdawar) for the contribution!
- Interface conversion bug which could cause a panic when calling /ipqueuesz endpoint ([#4477](https://github.com/nats-io/nats-server/issues/4477))

Leafnode
- Fix race condition which could affect propagating interest over leafnode connections ([#4464](https://github.com/nats-io/nats-server/issues/4464))

JetStream
- Fix possible deadlock in checking for drift in the usage reporting when storing a message ([#4411](https://github.com/nats-io/nats-server/issues/4411))
- Durable pull consumers could get cleaned up incorrectly on leader change ([#4412](https://github.com/nats-io/nats-server/issues/4412))
- Moving an R1 stream could sometimes lose all messages ([#4413](https://github.com/nats-io/nats-server/issues/4413))
- Prevent peer-remove of an R1 stream which could result in the stream becoming orphaned ([#4420](https://github.com/nats-io/nats-server/issues/4420))
- Ensure consumer ack pending is less than max ack pending on state restore ([#4427](https://github.com/nats-io/nats-server/issues/4427))
- Ensure to reset election timer when catching up ([#4428](https://github.com/nats-io/nats-server/issues/4428)) Thanks to [@yuzhou-nj](https://github.com/yuzhou-nj) for the report!
- Auto step-down Raft leader if an entry is missing on a catchup request ([#4432](https://github.com/nats-io/nats-server/issues/4432))
- Fix PurgeEx with keep having deletes in blocks ([#4431](https://github.com/nats-io/nats-server/issues/4431))
- Update global subject index when message blocks expire ([#4439](https://github.com/nats-io/nats-server/issues/4439))
- Ensure max messages per subject is respected after update ([#4446](https://github.com/nats-io/nats-server/issues/4446)) Thanks to [@anthonyjacques20](https://github.com/anthonyjacques20) for the report!
- Ignore and remove empty message blocks on rebuild ([#4447](https://github.com/nats-io/nats-server/issues/4447))
- Fix possible accounting discrepancy on message write ([#4455](https://github.com/nats-io/nats-server/issues/4455))
- Fix potential message duplication from stream sources when downgrading from 2.10 ([#4454](https://github.com/nats-io/nats-server/issues/4454)) 
- Check for checksum violations for all records before sequence processing ([#4465](https://github.com/nats-io/nats-server/issues/4465))
- Fix message block accounting ([#4473](https://github.com/nats-io/nats-server/issues/4473))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.21...v2.9.22


[Changes][v2.9.22]


<a id="v2.9.21"></a>
# [Release v2.9.21](https://github.com/nats-io/nats-server/releases/tag/v2.9.21) - 2023-08-04

## Changelog

### Go Version
- 1.19.12

### Dependencies
- github.com/klauspost/compress v1.16.7
- github.com/nats-io/nats.go v1.28.0
- go.uber.org/automaxprocs v1.5.3
- golang.org/x/crypto v0.11.0
- golang.org/x/sys v0.10.0

### Added
OCSP
- Add fetch, cache, and verification of client CA's OCSP Response for NATS, WebSocket, and MQTT client mTLS connections ([#4362](https://github.com/nats-io/nats-server/issues/4362), backported from 2.10)
- Add bi-directional fetch, cache, and verification of CA OCSP Response for LEAF connections ([#4362](https://github.com/nats-io/nats-server/issues/4362), backported from 2.10)

See [ADR-38 OCSP Peer Verification](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-38.md)

General
- Add UTC log timestamp option ([#4331](https://github.com/nats-io/nats-server/issues/4331), backported from 2.10)

### Improved
JetStream
- Don't error to server logs if message was deleted for consumer ([#4328](https://github.com/nats-io/nats-server/issues/4328))
- Improve publish performance for zero-interest subjects ([#4359](https://github.com/nats-io/nats-server/issues/4359)) Thanks to [@antlad](https://github.com/antlad) for reporting the issue!
- Sync and reset message rejected count to ensure replicas don’t incorrectly discard messages ([#4365](https://github.com/nats-io/nats-server/issues/4365), [#4366](https://github.com/nats-io/nats-server/issues/4366))

### Fixed
General
- Leaking memory on usage of `getHash()` ([#4329](https://github.com/nats-io/nats-server/issues/4329)) Thanks to [@VuongUranus](https://github.com/VuongUranus) for reporting the issue!
- Server reload with highly active accounts and service imports could cause panic or dataloss ([#4327](https://github.com/nats-io/nats-server/issues/4327))
- Fix detection of an unusable configuration file ([#4358](https://github.com/nats-io/nats-server/issues/4358))
  - **NOTE: as a side effect of this fix, the server will no longer startup with an empty config file**
- Fix a few system service imports going missing after configuration reload ([#4360](https://github.com/nats-io/nats-server/issues/4360))

OCSP
- Fix local-determination of issuer CA at startup ([#4362](https://github.com/nats-io/nats-server/issues/4362))
- Remove constraint that all (super)cluster node peers must be issued by the same CA ([#4362](https://github.com/nats-io/nats-server/issues/4362))

Embedded
- Don't require TLS for in-process client connection ([#4323](https://github.com/nats-io/nats-server/issues/4323))

JetStream
- Fix serializability guarantee for concurrent publish when using expected-last-subject-sequence ([#4319](https://github.com/nats-io/nats-server/issues/4319))
- Report correct consumer count in paged list response ([#4339](https://github.com/nats-io/nats-server/issues/4339))
- Fix not validating single token filtered consumer ([#4338](https://github.com/nats-io/nats-server/issues/4338))
- Fix stream recovery of message block with sequence gaps ([#4344](https://github.com/nats-io/nats-server/issues/4344))
- Fix panic when re-calculating first sequence of SimpleState info ([#4346](https://github.com/nats-io/nats-server/issues/4346))
- Fix stream store accounting drift ([#4357](https://github.com/nats-io/nats-server/issues/4357))  

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.20...v2.9.21

[Changes][v2.9.21]


<a id="v2.9.20"></a>
# [Release v2.9.20](https://github.com/nats-io/nats-server/releases/tag/v2.9.20) - 2023-07-13

## Changelog

### Go Version
- 1.19.11

### Added
Windows
- Backport 2.10 support for native Windows certificate store ([#4268](https://github.com/nats-io/nats-server/issues/4268))

### Improved
Accounts
- Allow advisories to be exported/imported across accounts ([#4302](https://github.com/nats-io/nats-server/issues/4302))

JetStream
- Optimize consumer create time on streams with a large number of blocks ([#4269](https://github.com/nats-io/nats-server/issues/4269))

### Fixed
Gateways
- Protect possible data race when reloading accounts ([#4274](https://github.com/nats-io/nats-server/issues/4274))

Leafnodes
- Prevent zombie subscriptions which could lead to silent data loss when using queue subscriptions ([#4299](https://github.com/nats-io/nats-server/issues/4299)) 

WebSocket
- Prevent reporting `tls_required` when `tls_available` is not set ([#4264](https://github.com/nats-io/nats-server/issues/4264))

JetStream
- Prevent corrupting streams actively being restored during health check ([#4277](https://github.com/nats-io/nats-server/issues/4277)) Thank you [@vitush93](https://github.com/vitush93) for the report!
- Prevent encrypted data attempting to be decrypted with an empty key ([#4301](https://github.com/nats-io/nats-server/issues/4301))

MQTT
- Ensure republished messages from streams are received by MQTT subscriptions ([#4303](https://github.com/nats-io/nats-server/issues/4303))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.19...v2.9.20

[Changes][v2.9.20]


<a id="v2.9.19"></a>
# [Release v2.9.19](https://github.com/nats-io/nats-server/releases/tag/v2.9.19) - 2023-06-20

## Changelog

### Go Version
- 1.19.10

### Improved
JetStream
- Improve resource utilization when creating mirrors on very high-sequence streams ([#4249](https://github.com/nats-io/nats-server/issues/4249))

### Fixed
WebSocket
- Ensure INFO properties are populated based on the WebSocket listener when enabled ([#4255](https://github.com/nats-io/nats-server/issues/4255)) Thanks to [@Envek](https://github.com/Envek) for reporting the issue!

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.18...v2.9.19


[Changes][v2.9.19]


<a id="v2.9.18"></a>
# [Release v2.9.18](https://github.com/nats-io/nats-server/releases/tag/v2.9.18) - 2023-06-13

## Changelog

### Go Version
- 1.19.10

### Dependency Updates
- golang.org/x/crypto v0.9.0 ([#4236](https://github.com/nats-io/nats-server/issues/4236))
- golang.org/x/sys v0.8.0 ([#4236](https://github.com/nats-io/nats-server/issues/4236))
- github.com/nats-io/nats.go v1.27.0 ([#4239](https://github.com/nats-io/nats-server/issues/4239))

### Improved
Monitoring
- Optimize /statsz locking and sending in standalone mode ([#4235](https://github.com/nats-io/nats-server/issues/4235))

JetStream
- Apply ack floor check only for interest-based streams ([#4206](https://github.com/nats-io/nats-server/issues/4206))
- Improved efficiency and reduced CPU usage of the consumer ack floor check, particularly when the stream first sequence is a large number ([#4226](https://github.com/nats-io/nats-server/issues/4226))
- Improve clean-up phase of R1 consumers on server restart for name reuse ([#4216](https://github.com/nats-io/nats-server/issues/4216))
- Optimize “last message lookups” by subject (KV get operations) for small messages ([#4232](https://github.com/nats-io/nats-server/issues/4232)) Thanks to [@jjthiessen](https://github.com/jjthiessen) for reporting the issue!
- Only enable JetStream account updates in clustered mode ([#4233](https://github.com/nats-io/nats-server/issues/4233)) Thanks to [@tpihl](https://github.com/tpihl) for reporting the issue!

### Fixed
General
- Fix a variety of potential panic scenarios ([#4214](https://github.com/nats-io/nats-server/issues/4214)) Thanks to [@Zamony](https://github.com/Zamony) and [@artemseleznev](https://github.com/artemseleznev) for the contribution!

Leadnode
- Daisy chained leafnodes could have unreliable interest propagation ([#4207](https://github.com/nats-io/nats-server/issues/4207))
- Properly distribute requests to queue groups across leafnodes ([#4231](https://github.com/nats-io/nats-server/issues/4231))

JetStream
- Killed server on restart could render encrypted stream unrecoverable ([#4210](https://github.com/nats-io/nats-server/issues/4210)) Thanks to [@BhatheyBalaji](https://github.com/BhatheyBalaji) for the report!
- Fix a few data races detected internal testing ([#4211](https://github.com/nats-io/nats-server/issues/4211))
- Process extended purge operations correctly when being replayed ([#4212](https://github.com/nats-io/nats-server/issues/4212), [#4213](https://github.com/nats-io/nats-server/issues/4213)) Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the report and contribution!

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.17...v2.9.18

[Changes][v2.9.18]


<a id="v2.9.17"></a>
# [Release v2.9.17](https://github.com/nats-io/nats-server/releases/tag/v2.9.17) - 2023-05-18

## Changelog

### Go Version
- 1.19.9

### Dependency Updates
- github.com/klauspost/compress v1.16.5 ([#4088](https://github.com/nats-io/nats-server/issues/4088))

### Improved
Core
- Additional optimizations to outbound queues, reducing memory footprint ([#4084](https://github.com/nats-io/nats-server/issues/4084), [#4093](https://github.com/nats-io/nats-server/issues/4093), [#4139](https://github.com/nats-io/nats-server/issues/4139))
- Use faster flate compression library for WebSocket transport compression ([#4087](https://github.com/nats-io/nats-server/issues/4087))

Leafnodes
- Optimize subscription interest propagation for large leafnode fleet ([#4117](https://github.com/nats-io/nats-server/issues/4117), [#4135](https://github.com/nats-io/nats-server/issues/4135))

Monitoring
- Support sorting by RTT for /connz ([#4157](https://github.com/nats-io/nats-server/issues/4157))

Resolver
- Improve signaling for missing account lookups ([#4151](https://github.com/nats-io/nats-server/issues/4151))

JetStream
- Optimized determining if a stream snapshot is required ([#4074](https://github.com/nats-io/nats-server/issues/4074))
- Run periodic check for consumer “ack floor” drift on leader ([#4086](https://github.com/nats-io/nats-server/issues/4086))
- Optimize leadership transfer during a stream migration ([#4104](https://github.com/nats-io/nats-server/issues/4104))
- Improve how clustered consumer state is hydrated on startup ([#4107](https://github.com/nats-io/nats-server/issues/4107))
- Add operation type to panic messages for improved debugging ([#4108](https://github.com/nats-io/nats-server/issues/4108))
- Improve health check to repair stalled assets periodically ([#4116](https://github.com/nats-io/nats-server/issues/4116), [#4172](https://github.com/nats-io/nats-server/issues/4172))
- Remove unnecessary filestore lock to improve I/O performance ([#4123](https://github.com/nats-io/nats-server/issues/4123))
- Various Raft leadership improvements ([#4126](https://github.com/nats-io/nats-server/issues/4126), [#4142](https://github.com/nats-io/nats-server/issues/4142), [#4143](https://github.com/nats-io/nats-server/issues/4143), [#4145](https://github.com/nats-io/nats-server/issues/4145))
- Improve accuracy of account usage ([#4131](https://github.com/nats-io/nats-server/issues/4131))
- Clean up old Raft groups when streams are reset ([#4177](https://github.com/nats-io/nats-server/issues/4177))

### Fixed
General
- Fix various names in comments ([#4099](https://github.com/nats-io/nats-server/issues/4099)) Thanks to [@cuishuang](https://github.com/cuishuang) for the contribution!
- Fix various typos in comments ([#4169](https://github.com/nats-io/nats-server/issues/4169)) Thanks to [@savion1024](https://github.com/savion1024) for the contribution!
- Update tests to reflect the `server.Start()` call no longer blocks ([#4111](https://github.com/nats-io/nats-server/issues/4111)) Thanks to [@lheiskan](https://github.com/lheiskan) for reporting the issue!
- Fix race condition in config reload with gateway sublist check ([#4127](https://github.com/nats-io/nats-server/issues/4127))
- Track all remote servers in a NATS system with different domains ([#4159](https://github.com/nats-io/nats-server/issues/4159))

Core
- Fix premature closing in WebSocket transport due to outbound queue changes ([#4084](https://github.com/nats-io/nats-server/issues/4084))
- Fix subscription interest for config-based accounts during config reload ([#4130](https://github.com/nats-io/nats-server/issues/4130))
- Use monotonic time for measuring durations internally ([#4132](https://github.com/nats-io/nats-server/issues/4132), [#4154](https://github.com/nats-io/nats-server/issues/4154), [#4163](https://github.com/nats-io/nats-server/issues/4163))

Monitoring
- Service import reporting for /accountz when mapping to local subjects ([#4158](https://github.com/nats-io/nats-server/issues/4158))

JetStream
- Fix formatting of Raft debug log ([#4090](https://github.com/nats-io/nats-server/issues/4090))
- Prevent failure of /healthz in single server mode on failed snapshot restore ([#4100](https://github.com/nats-io/nats-server/issues/4100))
- Ensure a stream Raft node has fully stopped and resources freed ([#4118](https://github.com/nats-io/nats-server/issues/4118))
- Fix case where R1 streams are orphaned and can’t scale up ([#4146](https://github.com/nats-io/nats-server/issues/4146))
- Protect against out of bounds access on usage updates ([#4164](https://github.com/nats-io/nats-server/issues/4164))
- Fix state rebuild where the first block is truncated and missing index info ([#4166](https://github.com/nats-io/nats-server/issues/4166))
- Avoid stale KV reads on server restarted for replicated stores ([#4171](https://github.com/nats-io/nats-server/issues/4171)) Thanks to [@yixinin](https://github.com/yixinin) for reporting the issue!
- Prevent deadlock with usage report for accounts ([#4176](https://github.com/nats-io/nats-server/issues/4176))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.16...v2.9.17


[Changes][v2.9.17]


<a id="v2.9.16"></a>
# [Release v2.9.16](https://github.com/nats-io/nats-server/releases/tag/v2.9.16) - 2023-04-17

## Changelog

### Go Version
- 1.19.8

### Dependency Updates
- github.com/klauspost/compress v1.16.4
- github.com/nats-io/jwt/v2 v2.4.1
- github.com/nats-io/nkeys v0.4.4
- golang.org/x/crypto v0.8.0
- golang.org/x/sys v0.7.0

### Added
Build
- Nightly build of the “main” branch as a Docker image: synadia/nats-server:nightly-main ([#3961](https://github.com/nats-io/nats-server/issues/3961), [#3962](https://github.com/nats-io/nats-server/issues/3962), [#3963](https://github.com/nats-io/nats-server/issues/3963), [#3972](https://github.com/nats-io/nats-server/issues/3972), [#4019](https://github.com/nats-io/nats-server/issues/4019), [#4063](https://github.com/nats-io/nats-server/issues/4063))
- Version control SHA in the Goreleaser build of the server ([#3993](https://github.com/nats-io/nats-server/issues/3993)). Thanks for the report [@jzhoucliqr](https://github.com/jzhoucliqr)!
Monitoring
- Add server name and route remote server name to /routez ([#4054](https://github.com/nats-io/nats-server/issues/4054))

Resolver
- Add “hard_delete” option for stored JWTs ([#3783](https://github.com/nats-io/nats-server/issues/3783)). Thanks for the contribution [@JulienVdG](https://github.com/JulienVdG)!
 
### Improved
JetStream
- Storage and Raft layer improvements to decrease p99 latency variance and memory pressure in high load environments ([#3956](https://github.com/nats-io/nats-server/issues/3956), [#3952](https://github.com/nats-io/nats-server/issues/3952), [#3965](https://github.com/nats-io/nats-server/issues/3965), [#3981](https://github.com/nats-io/nats-server/issues/3981), [#3999](https://github.com/nats-io/nats-server/issues/3999), [#4018](https://github.com/nats-io/nats-server/issues/4018), [#4020](https://github.com/nats-io/nats-server/issues/4020), [#4021](https://github.com/nats-io/nats-server/issues/4021), [#4022](https://github.com/nats-io/nats-server/issues/4022), [#4026](https://github.com/nats-io/nats-server/issues/4026), [#4027](https://github.com/nats-io/nats-server/issues/4027), [#4028](https://github.com/nats-io/nats-server/issues/4028), [#4029](https://github.com/nats-io/nats-server/issues/4029), [#4030](https://github.com/nats-io/nats-server/issues/4030), [#4038](https://github.com/nats-io/nats-server/issues/4038), [#4045](https://github.com/nats-io/nats-server/issues/4045), [#4050](https://github.com/nats-io/nats-server/issues/4050), [#4053](https://github.com/nats-io/nats-server/issues/4053))
- Don’t show Raft warning for node that is closed ([#3968](https://github.com/nats-io/nats-server/issues/3968))
- Use pooled buffer for flushing encrypted message blocks ([#3975](https://github.com/nats-io/nats-server/issues/3975))
- Remove snapshotting of cores and maxprocs ([#3979](https://github.com/nats-io/nats-server/issues/3979))
- Improvements to interest-based streams to optimize when messages are deleted ([#4006](https://github.com/nats-io/nats-server/issues/4006), [#4007](https://github.com/nats-io/nats-server/issues/4007), [#4012](https://github.com/nats-io/nats-server/issues/4012))
- Better handling of concurrent stream and consumer creation of the same name ([#4013](https://github.com/nats-io/nats-server/issues/4013))
- Finer-grain locking during asset checking to reduce contention in the /healthz endpoint ([#4031](https://github.com/nats-io/nats-server/issues/4031))
- Encrypted file stores will now limit block sizes to improve performance ([#4046](https://github.com/nats-io/nats-server/issues/4046))
- Improve performance on storing messages with varying subjects and limits are imposed ([#4048](https://github.com/nats-io/nats-server/issues/4048), [#4057](https://github.com/nats-io/nats-server/issues/4057)). Thanks for the report [@kung-foo](https://github.com/kung-foo)!

### Fixed
Subjects
- Ensure subjects containing percent (%) are escaped ([#4040](https://github.com/nats-io/nats-server/issues/4040))

Accounts
- Fix data race when setting up service import subscriptions ([#4068](https://github.com/nats-io/nats-server/issues/4068))

Leaf
- Fix leaf client connection failing on OCSP setups ([#3964](https://github.com/nats-io/nats-server/issues/3964))
- Fix case when allow/deny permissions on leaf connection could block legitimate interest ([#4032](https://github.com/nats-io/nats-server/issues/4032))

Cluster
- Route disconnect not detected by ping/pong ([#4016](https://github.com/nats-io/nats-server/issues/4016)). Thanks for the contribution [@sandykellagher](https://github.com/sandykellagher)!

JetStream
- Pull consumer not sending timeout error to clients for expired requests ([#3942](https://github.com/nats-io/nats-server/issues/3942))
- Prevent meta leader deadlock during deletion of orphaned streams during server startup ([#3945](https://github.com/nats-io/nats-server/issues/3945))
- Clear ack’ed messages when scaling workqueue or interest-based streams ([#3960](https://github.com/nats-io/nats-server/issues/3960)) Thanks for the report [@Kaarel](https://github.com/Kaarel)!
- Remove messages from interest-based stream on consumer snapshot ([#3970](https://github.com/nats-io/nats-server/issues/3970))
- Fix potential panic in message block buffer pool ([#3978](https://github.com/nats-io/nats-server/issues/3978))
- Fixed an issue with consumer states growing and causing instability ([#3980](https://github.com/nats-io/nats-server/issues/3980))
- Improve handling of out-of-storage condition ([#3985](https://github.com/nats-io/nats-server/issues/3985))
- Address memory leak of unreachable Raft groups when JetStream becomes disabled ([#3986](https://github.com/nats-io/nats-server/issues/3986))
- Prevent Raft leader from being placed on server in lame-duck mode ([#4002](https://github.com/nats-io/nats-server/issues/4002))
- Remove potential race condition on sysRequest ([#4017](https://github.com/nats-io/nats-server/issues/4017))
- Fix FirstSeq not being updated with filestore when purging subject ([#4041](https://github.com/nats-io/nats-server/issues/4041)). Thanks for the contribution [@MauriceVanVeen](https://github.com/MauriceVanVeen)!
- Fix Raft log debug reloading ([#4047](https://github.com/nats-io/nats-server/issues/4047))
- Ensure consumer recovers fully on restart before being eligible for leader ([#4049](https://github.com/nats-io/nats-server/issues/4049))
- Fix incorrect check between stream source/mirror with external streams ([#4052](https://github.com/nats-io/nats-server/issues/4052))
- Fix various conditions during Raft group recovery ([#4056](https://github.com/nats-io/nats-server/issues/4056), [#4058](https://github.com/nats-io/nats-server/issues/4058))

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.15...v2.9.16



[Changes][v2.9.16]


<a id="v2.9.15"></a>
# [Release v2.9.15](https://github.com/nats-io/nats-server/releases/tag/v2.9.15) - 2023-03-02

## Changelog

### Go Version
- 1.19.6: Both the release executables and Docker images are built with this Go release

### Added
- Monitoring
  - Add raft query parameter to /jsz to include group info ([#3915](https://github.com/nats-io/nats-server/issues/3915))
  - Update /leafz to include leaf node remove server name and “spoke” flag ([#3923](https://github.com/nats-io/nats-server/issues/3923), [#3925](https://github.com/nats-io/nats-server/issues/3925))

### Changed
- Lower default value of `jetstream.max_outstanding_catchup` to prevent slow consumers between routes ([#3922](https://github.com/nats-io/nats-server/issues/3922))
  - **Note: The new value is now 64MB from 128MB. This is better optimized for 1 Gbit links, however if your links are 10 Gbit or higher, this value can be set back to 128MB if slow consumers were not previously observed.**
 
### Improved
- Refactor intra-process queue to reduce allocations ([#3894](https://github.com/nats-io/nats-server/issues/3894))
- JetStream
  - Better system stability and recovery from corrupt metadata due to hard forced restarts ([#3934](https://github.com/nats-io/nats-server/issues/3934))
  - Optimize on-disk, per-subject info update ([#3867](https://github.com/nats-io/nats-server/issues/3867))
  - Limit concurrent blocking IO to improve stability under heavy IO loads ([#3867](https://github.com/nats-io/nats-server/issues/3867))
  - Improve message expiry calculation for large numbers of messages ([#3867](https://github.com/nats-io/nats-server/issues/3867))
  - Optimize when and how consumer num pending is calculated, significantly speeding up consumer info requests ([#3877](https://github.com/nats-io/nats-server/issues/3877))
  - Improve parallel consumer creation to prevent dropped messages ([#3880](https://github.com/nats-io/nats-server/issues/3880))
  - Properly warn on consumer state update state failures ([#3892](https://github.com/nats-io/nats-server/issues/3892))
  - Performance of consumer creation for certain configurations ([#3901](https://github.com/nats-io/nats-server/issues/3901))
  - Send current snapshot to followers when becoming meta-leader ([#3904](https://github.com/nats-io/nats-server/issues/3904))
  - Ensure preferred peer during stepdown is healthy ([#3905](https://github.com/nats-io/nats-server/issues/3905))
  - Optimized various store calls on stream state ([#3910](https://github.com/nats-io/nats-server/issues/3910))
  - Various performance and stability under heavy IO loads ([#3922](https://github.com/nats-io/nats-server/issues/3922)) (Thank you [@matkam](https://github.com/matkam) and [@davidzhao](https://github.com/davidzhao) for the report and the test harness!)

### Fixed
- Fix stack overflow panic in reverse entry check when inbox ends with wildcard ([#3862](https://github.com/nats-io/nats-server/issues/3862))
- Check if client connection name was already set when storing, preventing recursive memory growth ([#3886](https://github.com/nats-io/nats-server/issues/3886))
- Fix check for count of wildcard tokens in “partition” subject transform ([#3887](https://github.com/nats-io/nats-server/issues/3887)) (Thank you [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the contribution!)
- Fix panic if service export is nil ([#3917](https://github.com/nats-io/nats-server/issues/3917)) (Thank you [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the report!)
- JetStream
  - Ensure per-subject info is updated when doing stream compact ([#3860](https://github.com/nats-io/nats-server/issues/3860))
  - Ensure account usage is updated in the filestore when extended version purge occurs ([#3876](https://github.com/nats-io/nats-server/issues/3876))
  - Prevent consumer deletes on restart, with non-fatal errors ([#3881](https://github.com/nats-io/nats-server/issues/3881))
  - Do not warn if consumer replicas is zero since it will be inherited from the stream ([#3882](https://github.com/nats-io/nats-server/issues/3882))
  - Named push consumers with inactive thresholds deleted when still active ([#3884](https://github.com/nats-io/nats-server/issues/3884))
  - Prevent spurious “Error storing entry to WAL” log messages ([#3893](https://github.com/nats-io/nats-server/issues/3893), [#3891](https://github.com/nats-io/nats-server/issues/3891))
  - Clean up consumer redelivery state on stream purge ([#3892](https://github.com/nats-io/nats-server/issues/3892))
  - Clean up consumer ack pending if below stream ack floor ([#3892](https://github.com/nats-io/nats-server/issues/3892))
  - Update ack floors on messages being expired ([#3892](https://github.com/nats-io/nats-server/issues/3892))
  - Fix lost ack pending consumer state on partial stream purge ([#3892](https://github.com/nats-io/nats-server/issues/3892))
  - Snapshot and compact the consumer RAFT WAL, even when state changes do not occur, to prevent excessive disk usage ([#3898](https://github.com/nats-io/nats-server/issues/3898))
  - Fix KV accounting errors under heavy concurrent usage ([#3900](https://github.com/nats-io/nats-server/issues/3900))
  - Ensure new replicas respect MaxAge when a stream is scaled up ([#3861](https://github.com/nats-io/nats-server/issues/3861))
  - Snapshots would not compact after being applied ([#3907](https://github.com/nats-io/nats-server/issues/3907))
  - Fix filtered pending state calculation ([#3910](https://github.com/nats-io/nats-server/issues/3910))
  - Recover from a failed truncate on raft WAL ([#3911](https://github.com/nats-io/nats-server/issues/3911))
  - Fix JWT claims update if headers are passed ([#3918](https://github.com/nats-io/nats-server/issues/3918))
- MQTT
  - Prevent use of wildcard characters with topics beginning with $, per the MQTT spec violation 4.7.2-1 ([#3926](https://github.com/nats-io/nats-server/issues/3926)) (Thank you [@dominikh](https://github.com/dominikh) for the report!)

### Dependency Updates
- klauspost/compress - v1.16.0
- nats-io/nats.go - v1.24.0
- golang.org/x/crypto - v0.6.0
- golang.org/x/sys - v0.5.0

### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.14...v2.9.15



[Changes][v2.9.15]


<a id="v2.9.14"></a>
# [Release v2.9.14](https://github.com/nats-io/nats-server/releases/tag/v2.9.14) - 2023-02-06

## Changelog

### Go Version

- 1.19.5: Both the release executables and Docker images are built with this Go release

### Fixed

- JetStream
  - Fix circumstance when an empty snapshot could be written ([#3844](https://github.com/nats-io/nats-server/issues/3844))
  - Fix possible panic and deadlock during a consumer filter subject update ([#3845](https://github.com/nats-io/nats-server/issues/3845))
  - Fix consumer snapshot logic ([#3846](https://github.com/nats-io/nats-server/issues/3846))
 
### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.12...v2.9.14


[Changes][v2.9.14]


<a id="v2.9.12"></a>
# [Release v2.9.12](https://github.com/nats-io/nats-server/releases/tag/v2.9.12) - 2023-02-02

## Changelog

**NOTE: regressions were found in this release. Please skip this and go directly to the [v2.9.14 release](https://github.com/nats-io/nats-server/releases/tag/v2.9.14).**

### Go Version

- 1.19.5: Both the release executables and Docker images are built with this Go release
 
### Added

- OS/Arch
    - Add support for dragonfly BSD ([#3804](https://github.com/nats-io/nats-server/issues/3804))
 
### Improved

- JetStream
    - Use highwayhash to optimize difference tracking for stream, consumer, and cluster snapshots ([#3780](https://github.com/nats-io/nats-server/issues/3780))
    - Add small tolerance in lag for stream and consumer health checks ([#3794](https://github.com/nats-io/nats-server/issues/3794))
    - Various optimizations related to snapshots and memory usage ([#3828](https://github.com/nats-io/nats-server/issues/3828), [#3831](https://github.com/nats-io/nats-server/issues/3831), [#3837](https://github.com/nats-io/nats-server/issues/3837)) Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the collaboration on this issue.

### Fixed

- JetStream
    - Update numCores and maxProcs if altered by container limits ([#3796](https://github.com/nats-io/nats-server/issues/3796))
    - Fix filtered state for all subjects when the first sequences are deleted ([#3801](https://github.com/nats-io/nats-server/issues/3801))
    - Updating a stream to direct gets would fail direct gets ([#3806](https://github.com/nats-io/nats-server/issues/3806))
    - Force consumer replicas to match for interest-based policy streams ([#3817](https://github.com/nats-io/nats-server/issues/3817))
    - Assign signal subscription to consumer when created ([#3808](https://github.com/nats-io/nats-server/issues/3808))
    - Properly process updates on a stream on restart ([#3818](https://github.com/nats-io/nats-server/issues/3818))
    - Select consumer peer(s) from active/online peers only on creation ([#3821](https://github.com/nats-io/nats-server/issues/3821))
    - Sourced streams that do not overlap subjects were incorrectly reported as a cycle ([#3825](https://github.com/nats-io/nats-server/issues/3825))
    - Fix for isGroupLeaderless when JS not available due to shutdown ([#3830](https://github.com/nats-io/nats-server/issues/3830))
    - Deadlock on data loss when holding mb lock ([#3832](https://github.com/nats-io/nats-server/issues/3832))
    - Fix consumer not getting messages after filter update ([#3829](https://github.com/nats-io/nats-server/issues/3829))
    - Fix current consumers not getting messages after purge ([#3838](https://github.com/nats-io/nats-server/issues/3838)) Thanks to [@pcsegal](https://github.com/pcsegal) for the report!
 
### Updated Dependencies

- github.com/klauspost/compress - v1.15.15
- github.com/nats-io/nats.go - v1.23.0
- golang.org/x/time - v0.3.0
 
### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.11...v2.9.12

[Changes][v2.9.12]


<a id="v2.9.11"></a>
# [Release v2.9.11](https://github.com/nats-io/nats-server/releases/tag/v2.9.11) - 2023-01-06

## Changelog

### Go Version

- 1.19.4: Both the release executables and Docker images are built with this Go release

### Improved

- Remove duplicate subscription point to reduce memory usage ([#3768](https://github.com/nats-io/nats-server/issues/3768))
 
### Updated

- Update golang.org/x/crypto dependency to v0.5.0 ([#3761](https://github.com/nats-io/nats-server/issues/3761)). Thanks to [@friedrichwilken](https://github.com/friedrichwilken) for the contribution.
 
### Fixed

- JetStream
    - Allow recovery of stream meta file if not properly flushed prior to restart ([#3752](https://github.com/nats-io/nats-server/issues/3752))
    - Ensure max bytes are honored when message block is not properly flushed ([#3757](https://github.com/nats-io/nats-server/issues/3757)). Thanks to [@containerpope](https://github.com/containerpope) for the report.
    - Ensure new consumers assigned to an offline server are properly restored from the metaleader's snapshot ([#3760](https://github.com/nats-io/nats-server/issues/3760))
    - Set and clear observer state for servers with disconnected leaf nodes ([#3763](https://github.com/nats-io/nats-server/issues/3763))
- Leaf node
    - Fix duplicate messages when leaf node binds an account with service imports ([#3751](https://github.com/nats-io/nats-server/issues/3751)). Thanks to [@asambres-form3](https://github.com/asambres-form3) and [@maxarndt](https://github.com/maxarndt) for the original reports.
- Monitoring
    - Add back existing `HealthzOptions.JSEnabled` field to restore backwards compatibility ([#3744](https://github.com/nats-io/nats-server/issues/3744))
 
### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.10...v2.9.11


[Changes][v2.9.11]


<a id="v2.9.10"></a>
# [Release v2.9.10](https://github.com/nats-io/nats-server/releases/tag/v2.9.10) - 2022-12-20

## Changelog

### Go Version

- 1.19.4: Both the release executables and Docker images are built with this Go release
 
### Improved

- JetStream
    - Improve performance and latency with large number of sparse consumers ([#3710](https://github.com/nats-io/nats-server/issues/3710))
    - Simplify locking for consumer info requests ([#3714](https://github.com/nats-io/nats-server/issues/3714))
 
### Fixed

- JetStream
    - Ensure previously assigned nodes are removed from stream assignment ([#3707](https://github.com/nats-io/nats-server/issues/3707), [#3709](https://github.com/nats-io/nats-server/issues/3709))
    - Bad first sequence and expiration stopped working after server restart ([#3711](https://github.com/nats-io/nats-server/issues/3711))
    - Fix no-quorum issue if leader is restarted with a memory-based clustered stream ([#3713](https://github.com/nats-io/nats-server/issues/3713))
    - Fix dangling message block on compaction/cleanup which could prevent a stream from restoring ([#3717](https://github.com/nats-io/nats-server/issues/3717)) thanks to [@k15r](https://github.com/k15r) for the report
    - Prevent overlapping stream subjects on stream update in non-clustered JetStream ([#3715](https://github.com/nats-io/nats-server/issues/3715))

- Monitoring 
    - `/healthz?js-enabled=true` behavior now properly asserts whether JetStream is enabled if it is expected to be and does not check for health of the assets. ([#3704](https://github.com/nats-io/nats-server/issues/3704))
 
### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.9...v2.9.10


[Changes][v2.9.10]


<a id="v2.9.9"></a>
# [Release v2.9.9](https://github.com/nats-io/nats-server/releases/tag/v2.9.9) - 2022-12-08

## Changelog

### Go Version

- 1.19.4: Both the release executables and Docker images are built with this Go release
 
### Improved

- JetStream
    - Avoid full state snapshot for streams with many deleted items ([#3680](https://github.com/nats-io/nats-server/issues/3680))
    - Refactor to make stream removal from server consistent ([#3691](https://github.com/nats-io/nats-server/issues/3691))

- Help/Usage
    - Display two additional `--reload` signal options, `ldm` and `term` ([#3683](https://github.com/nats-io/nats-server/issues/3683))
 
### Fixed

- Authorization
    - Prevent returning `no_auth_required` when a client tries to connect in operator mode ([#3667](https://github.com/nats-io/nats-server/issues/3667))
        - Any client (Java) that suppressed sending credentials before of this flag could be affected.
        - This only affects the 2.9.8 server version
        
- JetStream
    - Tag policies not honored during stream replica reassignment after a peer is removed ([#3678](https://github.com/nats-io/nats-server/issues/3678))
    - Address issues when concurrent "create" requests for the same stream are issued ([#3679](https://github.com/nats-io/nats-server/issues/3679))
    - Server panic when consumer state was not decoded correctly ([#3688](https://github.com/nats-io/nats-server/issues/3688))
    - Ensure consumers that are deleted on startup are removed from the system ([#3689](https://github.com/nats-io/nats-server/issues/3689))
    - Fixed JetStream remained disabled for reactivated JWT accounts ([#3690](https://github.com/nats-io/nats-server/issues/3690), thank you [@JulienVdG](https://github.com/JulienVdG))
    
- Leafnodes
    - Do not delay PINGs for leaf connections ([#3692](https://github.com/nats-io/nats-server/issues/3692), thank you [@sandykellagher](https://github.com/sandykellagher))
 
### Complete Changes
 
https://github.com/nats-io/nats-server/compare/v2.9.8...v2.9.9

[Changes][v2.9.9]


<a id="v2.9.8"></a>
# [Release v2.9.8](https://github.com/nats-io/nats-server/releases/tag/v2.9.8) - 2022-11-22

## Changelog

### Go Version

- 1.19.3: Both release executables and Docker images are built with this Go release.

### Monitoring:

- JetStream:
  - Server might crash if a pull consumer with an activity threshold is deleted immediately after a message ack ([#3658](https://github.com/nats-io/nats-server/issues/3658)). Thanks to [@jdhenke](https://github.com/jdhenke) for the report.
- Server Info:
  - If `no_auth_user` is set, clear the auth required flag in the server info presented to the client ([#3659](https://github.com/nats-io/nats-server/issues/3659))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.7...v2.9.8

[Changes][v2.9.8]


<a id="v2.9.7"></a>
# [Release v2.9.7](https://github.com/nats-io/nats-server/releases/tag/v2.9.7) - 2022-11-17

## Changelog


### Go Version
- 1.19.3: Both release executables and Docker images are built with this Go release.


### Improved:
- JetStream:
    - Processing of acknowledgments ([#3624](https://github.com/nats-io/nats-server/issues/3624))
    - Stream ingest performance with large numbers of consumers ([#3633](https://github.com/nats-io/nats-server/issues/3633))
    - StreamDetail now includes stream creation for `/healthz` reporting. Thanks to [@raypinto](https://github.com/raypinto) for the contribution ([#3646](https://github.com/nats-io/nats-server/issues/3646))

### Fixed
- JetStream:
    - For "Interest" or "WorkQueue" policy streams, when a consumer was deleted, a stream view could cause high CPU or memory usage. Thanks to [@Kaarel](https://github.com/Kaarel) for the report ([#3610](https://github.com/nats-io/nats-server/issues/3610))
    - Make sure to enforce HA asset limits during peer processing as well as assignment ([#3614](https://github.com/nats-io/nats-server/issues/3614))
    - Account max streams/consumers not always honored ([#3615](https://github.com/nats-io/nats-server/issues/3615))
    - Reduce some warning log messages "append entries" ([#3619](https://github.com/nats-io/nats-server/issues/3619))
    - Logic bug that would prevent some stream messages from being deleted. Thanks to [@Kaarel](https://github.com/Kaarel) for the report ([#3620](https://github.com/nats-io/nats-server/issues/3620), [#3625](https://github.com/nats-io/nats-server/issues/3625))
    - Account removal leaks subscriptions. Thanks to [@JulienVdG](https://github.com/JulienVdG) for the report ([#3627](https://github.com/nats-io/nats-server/issues/3627))
    - Server fails to start on concurrent map read/write ([#3629](https://github.com/nats-io/nats-server/issues/3629), [#3634](https://github.com/nats-io/nats-server/issues/3634))
    - Possible panic on stream info when leader is not yet or elected or loses quorum. Thanks to [@apuckey](https://github.com/apuckey) for the report ([#3631](https://github.com/nats-io/nats-server/issues/3631))
    - Allow any type of ack to suppress auto-cleanup of consumer ([#3635](https://github.com/nats-io/nats-server/issues/3635))
    - Make mirror consumers use filtered version of consumer create ([#3638](https://github.com/nats-io/nats-server/issues/3638))
    - WorkQueue was not correctly rejecting overlapping consumers in some cases. Thanks to [@Kilio22](https://github.com/Kilio22) for the report ([#3640](https://github.com/nats-io/nats-server/issues/3640))
    - Make sure header keys do not have additional prefixes. Thanks to [@osmanovv](https://github.com/osmanovv) for the report ([#3649](https://github.com/nats-io/nats-server/issues/3649))
- Routing:
    - TLS connections to discovered server may fail ([#3611](https://github.com/nats-io/nats-server/issues/3611), [#3613](https://github.com/nats-io/nats-server/issues/3613))
- Weighted subject mappings updates not applied ([#3618](https://github.com/nats-io/nats-server/issues/3618))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.6...v2.9.7



[Changes][v2.9.7]


<a id="v2.9.6"></a>
# [Release v2.9.6](https://github.com/nats-io/nats-server/releases/tag/v2.9.6) - 2022-11-04

## Changelog


### Go Version
- 1.19.3: Both release executables and Docker images are built with this Go release.


### Fixed
- JetStream:
    - Possible panic on some rare cases where a clustered consumer monitor go routine started while the consumer was deleted or scaled down to an R1. Some tracing could have caused a panic ([#3599](https://github.com/nats-io/nats-server/issues/3599))
    - On stream proposal failures, the server would incorrectly warn about high stream lag. Thanks to [@kino71](https://github.com/kino71) for the report ([#3601](https://github.com/nats-io/nats-server/issues/3601))
    - Stream sources with `OptStartTime` would get redelivered messages on server restart. Thanks to [@DavidCockerill](https://github.com/DavidCockerill) for the report ([#3606](https://github.com/nats-io/nats-server/issues/3606))
    - Scaling down a replicated stream (R>1) to R1 while it has no quorum, for instance due to a network partition, would leave the stream in a bad state and there would be a constant report of "No quorum, stalled" for this stream, even after the network partition is resolved ([#3608](https://github.com/nats-io/nats-server/issues/3608))
- LeafNode:
    - Possible duplicate messages in complex setup. Thanks to [@chenchunping](https://github.com/chenchunping) for the report ([#3604](https://github.com/nats-io/nats-server/issues/3604))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.5...v2.9.6



[Changes][v2.9.6]


<a id="v2.9.5"></a>
# [Release v2.9.5](https://github.com/nats-io/nats-server/releases/tag/v2.9.5) - 2022-11-01

## Changelog


### Go Version
- 1.19.3: Both release executables and Docker images are built with this Go release.


### Fixed
- JetStream:
    - Honor `MaxMsgsPerSubject` on stream update ([#3595](https://github.com/nats-io/nats-server/issues/3595))
    - Errors such as "wrong sequence for skipped msg" or "expected sequence does not match store " while processing snapshots of streams with expired messages. Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) and [@sylrfor](https://github.com/sylrfor) the reports  ([#3589](https://github.com/nats-io/nats-server/issues/3589), [#3596](https://github.com/nats-io/nats-server/issues/3596))
- Sublist's cache hit rate would be wrong in presence of multiple accounts. Thanks to [@aopetrov86](https://github.com/aopetrov86) for the report and contribution ([#3591](https://github.com/nats-io/nats-server/issues/3591)) 


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.4...v2.9.5


[Changes][v2.9.5]


<a id="v2.9.4"></a>
# [Release v2.9.4](https://github.com/nats-io/nats-server/releases/tag/v2.9.4) - 2022-10-27

## Changelog


### Go Version
- 1.19.2: Both release executables and Docker images are built with this Go release.


### Fixed
- Configuration Reload:
    - The option `allow_non_tls` would be ignored after a configuration reload. Thanks to [@JulienVdG](https://github.com/JulienVdG) for the report ([#3583](https://github.com/nats-io/nats-server/issues/3583))
- JetStream:
    - Possible deadlock. Thanks to [@ashumkin](https://github.com/ashumkin) for the report and [@neilalexander](https://github.com/neilalexander) for the contribution ([#3555](https://github.com/nats-io/nats-server/issues/3555))
    - Possible panic in disk full situations. Thanks to [@fantashley](https://github.com/fantashley) for the contribution ([#3560](https://github.com/nats-io/nats-server/issues/3560), [#3568](https://github.com/nats-io/nats-server/issues/3568))
    - "First sequence mismatch" after a restart/deployment with streams that have message TTLs. Thanks to [@MauriceVanVeen](https://github.com/MauriceVanVeen) for the report ([#3567](https://github.com/nats-io/nats-server/issues/3567))
    - Update of an R1 consumer would not get a response. The update was accepted by the server, but the client library or NATS CLI would timeout waiting for the response ([#3574](https://github.com/nats-io/nats-server/issues/3574))
    - Update of a consumer's `InactiveThreshold` would not always take effect. Thanks to [@neilalexander](https://github.com/neilalexander) for the contribution ([#3575](https://github.com/nats-io/nats-server/issues/3575))
    - A consumer may not be removed based on `InactiveThreshold` in presence of gateways ([#3575](https://github.com/nats-io/nats-server/issues/3575))
    - Migration of ephemerals on server shutdown was not working and could create "ghost" consumers on servers restart, that is, consumers that would be listed by the meta leader, but getting information about this consumer would fail. Migration will no longer occur, instead, all R1 pull consumers will be notified that the server is shutting down, invalidating the pending requests ([#3576](https://github.com/nats-io/nats-server/issues/3576))
    - Consumers on a `Limits` policy stream could have their replicas changed to R1 ([#3576](https://github.com/nats-io/nats-server/issues/3576))
    - Ensure that RAFT communication is properly stopped when needed, which otherwise could cause server memory usage increase ([#3577](https://github.com/nats-io/nats-server/issues/3577))
    - Adding a warning when the inbound of messages causes a lag with the storage layer. In future release, the producers may be notified through a PubAck failure that the message cannot be accepted ([#3578](https://github.com/nats-io/nats-server/issues/3578))
    - Added pending messages/bytes to pull request errors and status: when the server responds to the client library that a request has timed-out, or server is shutdown, etc..., the response will now include the request pending messages and bytes ([#3572](https://github.com/nats-io/nats-server/issues/3572), [#3587](https://github.com/nats-io/nats-server/issues/3587))
    - More messages than the `max_msgs_per_subject` value could be recovered on server restart following an abnormal server exit ([#3579](https://github.com/nats-io/nats-server/issues/3579), [#3582](https://github.com/nats-io/nats-server/issues/3582))
- Leafnode:
    - Existing subscriptions would be sent to leafnodes even though they violated permissions. The publish side would be doing the right thing by not sending the messages over, but the subscription interest was still sent ([#3585](https://github.com/nats-io/nats-server/issues/3585))
- MQTT:
    - Subjects mapping were not working. Thanks to [@ozon2](https://github.com/ozon2) for the report ([#3552](https://github.com/nats-io/nats-server/issues/3552))
- Routing:
    - An implicit route may not reconnect, regardless of the `ConnectRetries` setting. This can happen in configurations where the `routes[]` block contains only the seed (and not as a name that could resolve to each IP of the cluster). If a route to a discovered server is disconnected, it may not try to reconnect due to the implicit nature of that connection. Thanks to [@wubumihuo](https://github.com/wubumihuo) for the report ([#3573](https://github.com/nats-io/nats-server/issues/3573))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.3...v2.9.4



[Changes][v2.9.4]


<a id="v2.9.3"></a>
# [Release v2.9.3](https://github.com/nats-io/nats-server/releases/tag/v2.9.3) - 2022-10-10

## Changelog


### Go Version
- 1.19.2: Both release executables and Docker images are built with this Go release.


### Fixed
- JetStream:
    - Unresponsiveness (health check failures, routes being blocked) while creating a RAFT group when disk performance if very slow. Thanks to [@TomasVojacek](https://github.com/TomasVojacek) for the report ([#3519](https://github.com/nats-io/nats-server/issues/3519))
    - Purge with additional options may leave some messages in the stream ([#3529](https://github.com/nats-io/nats-server/issues/3529))
    - Prevent stack overflow when an account imports its own export. [CVE-2022-42709](https://cve.report/CVE-2022-42709) ([#3538](https://github.com/nats-io/nats-server/issues/3538))
    - Prevent panic on negative replicas count. [CVE-2022-42708](https://cve.report/CVE-2022-42708) ([#3539](https://github.com/nats-io/nats-server/issues/3539))
    - User-provided ephemeral consumer name was not used in cluster mode, that is, the server would still pick a random name ([#3537](https://github.com/nats-io/nats-server/issues/3537))
- Added missing command line options in the `-help` section. Thanks to [@ariel-zilber](https://github.com/ariel-zilber) for the contributions ([#3523](https://github.com/nats-io/nats-server/issues/3523), [#3527](https://github.com/nats-io/nats-server/issues/3527))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.2...v2.9.3



[Changes][v2.9.3]


<a id="v2.9.2"></a>
# [Release v2.9.2](https://github.com/nats-io/nats-server/releases/tag/v2.9.2) - 2022-09-29

## Changelog


### Go Version
- 1.19.1: Both release executables and Docker images are built with this Go release.

### Improved:
- Fan-out performance degraded between `v2.8.4` and `v2.9.0`. This was mainly due to addition of message count/size accounting per-account. Some code refactoring restored or even increased the performance compared to `v2.8.4` ([#3504](https://github.com/nats-io/nats-server/issues/3504))

### Fixed
- JetStream:
    - Prevent panic processing a consumer assignment. This could happen in rare situations where a stream would catchup and start processing consumer assignments while the stream itself was "stopped", for instance during a cluster-reset event following a sequence mismatch detection, etc... ([#3498](https://github.com/nats-io/nats-server/issues/3498))
    - The FileStore implementation could have held into memory of message blocks for longer (about 5 seconds) than needed. In situations where the stream was filling up quickly, the amount of blocks held in memory could grow enough that even 5 seconds expiration could cause the memory growth to be noticeable, even more so if the garbage collection has no reason to trigger if the overall memory usage is below the host's limit ([#3501](https://github.com/nats-io/nats-server/issues/3501))
    - Scaling a consumer down to a R=1 would work but not send a response to the CLI/application requesting that change. That is, if an application would call `js.UpdateConsumer()` (using the Golang client library) with a replica of 1, the call would timeout but the operation would actually succeed ([#3502](https://github.com/nats-io/nats-server/issues/3502))
    - The consumer names paging had issues and could have returned only the API limit, which for this request is 1024 ([#3510](https://github.com/nats-io/nats-server/issues/3510))
    - Some streams may fail to be recovered if a meta-layer snapshot was done but an upstream source or mirror changed it subjects ([#3508](https://github.com/nats-io/nats-server/issues/3508))
    - Redeliveries for consumers with the "LastPerSubject" delivery policy were not honored. Thanks to [@brentd](https://github.com/brentd) for the report ([#3511](https://github.com/nats-io/nats-server/issues/3511))
- Possible exit of NATS Server running as a Windows service without logging enabled when server would report invalid protocols. The workaround is to enable logging (specify a log file as opposed to have the logging directed to the Windows Event Logs). Thanks to [@BentTranberg](https://github.com/BentTranberg) for the report ([#3497](https://github.com/nats-io/nats-server/issues/3497))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.1...v2.9.2



[Changes][v2.9.2]


<a id="v2.9.1"></a>
# [Release v2.9.1](https://github.com/nats-io/nats-server/releases/tag/v2.9.1) - 2022-09-22

## Changelog


### Go Version
- 1.19.1: Both release executables and Docker images are built with this Go release.


### Added
- JetStream:
    - Ability to remove a server by peer ID instead of server name ([#3473](https://github.com/nats-io/nats-server/issues/3473))
    - Peer ID in the `meta_cluster` of `statsz` or `jsz` monitoring endpoint ([#3476](https://github.com/nats-io/nats-server/issues/3476))
    - Ability to apply a discard new policy per subject. A new JSON field in the stream configuration called `discard_new_per_subject` can now be set (along with discard new policy and max messages per subject > 0) ([#3487](https://github.com/nats-io/nats-server/issues/3487))

### Improved
- JetStream:
    - Optimize acknowledgment handling. Thanks to [@neilalexander](https://github.com/neilalexander) for the report and contribution ([#3468](https://github.com/nats-io/nats-server/issues/3468), [#3472](https://github.com/nats-io/nats-server/issues/3472))

### Updated
- Dependencies ([#3491](https://github.com/nats-io/nats-server/issues/3491))

### Changed
- JetStream:
    - When filtering a source stream, use the new consumer create API subject ([#3478](https://github.com/nats-io/nats-server/issues/3478)) 

### Fixed
- JetStream:
    - Peer randomization when creating consumers group for replicas of 1. Thanks to [@goku321](https://github.com/goku321) for the contribution ([#3470](https://github.com/nats-io/nats-server/issues/3470))
    - Added an error if consumer's `Name` and `Durable` are not equal when sending to the new `$JS.API.CONSUMER.CREATE.%s.%s.%s` subject ([#3471](https://github.com/nats-io/nats-server/issues/3471))
    - Server was not sending a `409` to the client library when a pull request was closed after sending at least a message but could not send more if that would exceed the `max_bytes` pull request limit ([#3474](https://github.com/nats-io/nats-server/issues/3474))
    - Possible panic on peer remove on server shutdown ([#3477](https://github.com/nats-io/nats-server/issues/3477))
    - Filtered consumers may also receive messages on other subjects from the stream. Thanks to [@perestoronin](https://github.com/perestoronin) for the report ([#3486](https://github.com/nats-io/nats-server/issues/3486))
- LeafNode:
    - A server that accepts a leaf connection on the websocket port, and the `websocket{}` block had a `no_auth_user` defined, this user was not being used for the account binding for that leaf node connection ([#3489](https://github.com/nats-io/nats-server/issues/3489))
- Edge condition handling in `{{Split()}}` subject mapping function ([#3463](https://github.com/nats-io/nats-server/issues/3463))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.9.0...v2.9.1



[Changes][v2.9.1]


<a id="v2.9.0"></a>
# [Release v2.9.0](https://github.com/nats-io/nats-server/releases/tag/v2.9.0) - 2022-09-09

## Changelog

IMPORTANT NOTE: Leafnode connections will now be closed if the cluster name is detected to be the same on the "hub" and "spoke", and reconnect attempts will be delayed by 30 seconds. If you are unaware of this misconfiguration you may experience a split network for longer than expected during the upgrade process. See the `CHANGED` section below and more details in pull request [#3232](https://github.com/nats-io/nats-server/issues/3232).


### Go Version
- 1.19.1: Both release executables and Docker images are built with this Go release.


### Added
- JetStream:
    - A new stream configuration field `AllowDirect` allows capable client libraries to have a new API that retrieves a message from any member of the group (leader or replicas). Note that this can lead to non-coherent reads after write since a replica may respond to the request although that replica does not yet have the latest write. The configuration field `MirrorDirect` allow the mirror to be part of the origin's group ([#3158](https://github.com/nats-io/nats-server/issues/3158), [#3221](https://github.com/nats-io/nats-server/issues/3221), [#3238](https://github.com/nats-io/nats-server/issues/3238), [#3247](https://github.com/nats-io/nats-server/issues/3247), [#3252](https://github.com/nats-io/nats-server/issues/3252), [#3325](https://github.com/nats-io/nats-server/issues/3325), [#3329](https://github.com/nats-io/nats-server/issues/3329), [#3358](https://github.com/nats-io/nats-server/issues/3358), [#3380](https://github.com/nats-io/nats-server/issues/3380), [#3392](https://github.com/nats-io/nats-server/issues/3392), [#3441](https://github.com/nats-io/nats-server/issues/3441))
    - Support for `InactiveThreshold` for durable consumers. If the inactivity threshold is specified, a durable that is offline for more than this duration will be removed ([#3190](https://github.com/nats-io/nats-server/issues/3190))
    - Ability for an operator to move streams, and support for tags reload ([#3217](https://github.com/nats-io/nats-server/issues/3217), [#3236](https://github.com/nats-io/nats-server/issues/3236), [#3234](https://github.com/nats-io/nats-server/issues/3234), [#3270](https://github.com/nats-io/nats-server/issues/3270), [#3354](https://github.com/nats-io/nats-server/issues/3354), [#3376](https://github.com/nats-io/nats-server/issues/3376), [#3419](https://github.com/nats-io/nats-server/issues/3419))
    - Support for filter subject in a mirror configuration ([#3227](https://github.com/nats-io/nats-server/issues/3227))
    - Support for consumer replica change. Thanks to [@goku321](https://github.com/goku321) for the report ([#3293](https://github.com/nats-io/nats-server/issues/3293))
    - Support for account purge operation by sending a request to `$JS.API.ACCOUNT.PURGE.<account name>`. Thanks to [@goku321](https://github.com/goku321) and [@sourabhaggrawal](https://github.com/sourabhaggrawal) for the report ([#3319](https://github.com/nats-io/nats-server/issues/3319), [#3378](https://github.com/nats-io/nats-server/issues/3378))
    - Support for AES-GCM cipher encryption for FileStore ([#3371](https://github.com/nats-io/nats-server/issues/3371))
    - Ability to override the default server limit for stream catchup. For instance `jetstream: { max_outstanding_catchup: 32MB }`. This limit is how many bytes in total may be inflight during streams catchup. This can help lower network bandwidth saturation ([#3418](https://github.com/nats-io/nats-server/issues/3418))
    - Pagination for `StreamInfo` requests. Note that not all clients may have the ability to provide the offset at the time of the server release ([#3454](https://github.com/nats-io/nats-server/issues/3454))
- LeafNodes:
    - Support for a `SignatureHandler` in remote configurations. This is applicable to applications embedding the NATS Server. Thanks to [@kung-foo](https://github.com/kung-foo) for the suggestion ([#3335](https://github.com/nats-io/nats-server/issues/3335))
- Monitoring:
   - Account specific in/out messages/bytes and slow consumers statistics in `$SYS.ACCOUNT.%s.SERVER.CONNS` message response ([#3187](https://github.com/nats-io/nats-server/issues/3187))
   - New endpoint `/accstatz` to get specific account statistics (such as number of connections, messages/bytes in/out, etc...) ([#3250](https://github.com/nats-io/nats-server/issues/3250), [#3382](https://github.com/nats-io/nats-server/issues/3382))
   - The `/healthz` endpoint is now also available via the system account under the `$SYS.REQ.SERVER.PING.HEALTHZ` subject ([#3250](https://github.com/nats-io/nats-server/issues/3250))
   - New options for the `/healthz` endpoint: `/healthz?js-enabled=true` to return an error if JetStream is disabled, and `/healthz?js-server-only=true` to skip the check of JetStream accounts, streams and consumers. Thanks to [@mfaizanse](https://github.com/mfaizanse) for the contribution ([#3326](https://github.com/nats-io/nats-server/issues/3326))
   - The `/connz?auth=1` endpoint now includes a `tls_peer_certs` array with subject, subject public key and raw certificate sha256. Thanks to [@RedShift1](https://github.com/RedShift1) for the suggestion ([#3387](https://github.com/nats-io/nats-server/issues/3387))
   - The `/jsz?accounts=true` endpoint will now show non 0 values for `reserved_memory` and `reserved_storage` when applicable ([#3435](https://github.com/nats-io/nats-server/issues/3435))
- MQTT:
   - A new configuration option `consumer_inactive_threshold` allow new QoS1 consumers to be removed if they are offline for more than the specified duration (based on [#3190](https://github.com/nats-io/nats-server/issues/3190)) ([#3193](https://github.com/nats-io/nats-server/issues/3193))
- Use of a library that automatically sets `GOMAXPROCS` to match Linux container CPU quota. Thanks to [@1995parham](https://github.com/1995parham) for the contribution ([#3218](https://github.com/nats-io/nats-server/issues/3218), [#3224](https://github.com/nats-io/nats-server/issues/3224), [#3237](https://github.com/nats-io/nats-server/issues/3237), [#3406](https://github.com/nats-io/nats-server/issues/3406))
- A new server configuration option `DontListen` that triggers the server to accept only "in memory" client connections. This is for embedded use-cases only and is paired with changes made in the client library nats.go. Thanks to [@neilalexander](https://github.com/neilalexander) for the contribution ([#2360](https://github.com/nats-io/nats-server/issues/2360), [#3225](https://github.com/nats-io/nats-server/issues/3225))
- Support for JWT account option `DisallowBearer` ([#3127](https://github.com/nats-io/nats-server/issues/3127))
- Stubs for WebAssembly. This allows NATS Server to be built under the js/wasm target. Thanks to [@neilalexander](https://github.com/neilalexander) for the contribution ([#2363](https://github.com/nats-io/nats-server/issues/2363))
- Symlink for the deb/rpm packages. Since v2.7.4, the server is installed under `/usr/bin` instead of `/usr/local/bin` as it used to. We now have added symlink to `/usr/local/sbin`. Thanks to [@ismail0352](https://github.com/ismail0352) for the report ([#3242](https://github.com/nats-io/nats-server/issues/3242))
- Templates to scoped signing key user permissions ([#3367](https://github.com/nats-io/nats-server/issues/3367), [#3373](https://github.com/nats-io/nats-server/issues/3373), [#3390](https://github.com/nats-io/nats-server/issues/3390))
- New subject mapping functions: `SplitFromLeft`, `SplitFromRight`, `SliceFromLeft`, `SliceFromRight` and `Split` ([#3305](https://github.com/nats-io/nats-server/issues/3305))
- Building of executable, deb and rpm packages for the `s390x` architecture ([#3458](https://github.com/nats-io/nats-server/issues/3458))

### Changed
- Gateway:
     - Phasing out of the "optimistic" mode whereby a server could send messages to the remote cluster without knowing if there was an interest or not. The remote cluster would reply with a "no interest" protocol. As of v2.9.0, servers that creates a gateway connection to a server of that version (and above) will no longer send messages in optimistic mode since it is assumed that all accounts will be switched to interest-only mode (where the subscription interest map is sent over) ([#3383](https://github.com/nats-io/nats-server/issues/3383))
- JetStream:
    - Stream's `RePublish` configuration field is now a new `RePublish` object (and `SubjectMapping` has been removed) that allows for a new boolean field called `HeadersOnly`. This could be useful for large messages and having republish just be a signaling mechanism ([#3157](https://github.com/nats-io/nats-server/issues/3157)) 
    - When a pull request exceeds the maximum bytes, the error returned will be a "409" now instead of a "408" ([#3172](https://github.com/nats-io/nats-server/issues/3172))
    - Make pull consumers FIFO per message, not per request ([#3241](https://github.com/nats-io/nats-server/issues/3241))
    - Accept `Nats-Expected-Last-Sequence` with a `0` value. The server used to ignore if the sequence was 0, but now it will treat it as a requirement that the stream be empty if the header is present with a value of `0`. Thanks to [@bruth](https://github.com/bruth) for the suggestion ([#3038](https://github.com/nats-io/nats-server/issues/3038))
    - A consumer "Maximum Deliveries" count can now be updated. Thanks to [@abegaj](https://github.com/abegaj) for the contribution ([#3265](https://github.com/nats-io/nats-server/issues/3265))
    - Encryption of meta and RAFT stores ([#3308](https://github.com/nats-io/nats-server/issues/3308))
    - Now return an error if there is an overlap between a source/mirror filter subject and the existing origin stream's subjects ([#3356](https://github.com/nats-io/nats-server/issues/3356))
    - Compression in RAFT and stream catchup traffic when nodes are v2.9.0+. This can reduce network bandwidth ([#3419](https://github.com/nats-io/nats-server/issues/3419))
- LeafNodes:
    - On establishment, the connection will now be closed when the same cluster name is detected on the "hub" and "spoke" side ([#3232](https://github.com/nats-io/nats-server/issues/3232))
- Default to essential client information. We did default to full sharing for an export from the system account, the main one being JetStream ([#3220](https://github.com/nats-io/nats-server/issues/3220))
- The queue group named `_sys_` is now reserved and an application attempting to use it would get a permission violation ([#3246](https://github.com/nats-io/nats-server/issues/3246))

### Improved
- JetStream:
    - Server under heavy load and low on resources like file descriptors ([#3168](https://github.com/nats-io/nats-server/issues/3168))
    - Performance of wildcard filtered consumer with stream with many subjects ([#3184](https://github.com/nats-io/nats-server/issues/3184))
    - Better distribution in placement of streams in a cluster when no "max bytes" is set ([#3194](https://github.com/nats-io/nats-server/issues/3194))
    - When the user adds a stream that already exists with a different configuration, the error description returned to the application was "stream name already in use", it will now be "stream name already in use with a different configuration". Also, when the user tries to restore a stream, if the stream was already present, the operation would fail with "stream name already in use", it will now be "stream name already in use, cannot restore". Thanks to [@AndrewDK](https://github.com/AndrewDK) for the suggestion ([#3280](https://github.com/nats-io/nats-server/issues/3280))
    - Speed of storing new keys with a large number of pre-existing keys ([#3320](https://github.com/nats-io/nats-server/issues/3320))
    - Replicas ordering and information regarding unknown replica in stream information API response ([#3347](https://github.com/nats-io/nats-server/issues/3347))
    - Catchup logic ([#3348](https://github.com/nats-io/nats-server/issues/3348))
    - Catchup aborted on requester failure and better flow control ([#3349](https://github.com/nats-io/nats-server/issues/3349))
    - FileStore for large KeyValue streams ([#3351](https://github.com/nats-io/nats-server/issues/3351), [#3353](https://github.com/nats-io/nats-server/issues/3353), [#3366](https://github.com/nats-io/nats-server/issues/3366), [#3401](https://github.com/nats-io/nats-server/issues/3401), [#3413](https://github.com/nats-io/nats-server/issues/3413), [#3416](https://github.com/nats-io/nats-server/issues/3416))
    - Usage of the FileStore implementation for the RAFT logs ([#3377](https://github.com/nats-io/nats-server/issues/3377), [#3394](https://github.com/nats-io/nats-server/issues/3394))
    - General improvements to clustered streams during server restart and KV/Compare-And-Swap use cases ([#3392](https://github.com/nats-io/nats-server/issues/3392))
    - Ability to secure the creation of consumer for libraries sending consumer creation requests to the new subject `$JS.API.CONSUMER.CREATE.<stream>.<subject>.<filter>` ([#3409](https://github.com/nats-io/nats-server/issues/3409))
    - Better accounting for `max-bytes` for pull consumers ([#3456](https://github.com/nats-io/nats-server/issues/3456))
    - Better error description when an asset placement fails ([#3342](https://github.com/nats-io/nats-server/issues/3342), [#3459](https://github.com/nats-io/nats-server/issues/3459))
- Server banner for `Trusted Operators` now displays the `Expires` date as `Never` instead of the epoch time. Thanks [@mschneider82](https://github.com/mschneider82) for the contribution ([#3452](https://github.com/nats-io/nats-server/issues/3452))

### Updated
- Dependencies ([#3153](https://github.com/nats-io/nats-server/issues/3153), [#3263](https://github.com/nats-io/nats-server/issues/3263), [#3286](https://github.com/nats-io/nats-server/issues/3286))
- JetStream:
    - Allow consumer filter subjects to be updated ([#3216](https://github.com/nats-io/nats-server/issues/3216))
    - `AckAll` and `NoAck` are now allowed on pull consumers ([#3442](https://github.com/nats-io/nats-server/issues/3442))

### Fixed
- Configuration Reload:
    - Configuration reload would fail if a `leafnodes` block would contain an `authorization{}` block, even if no modification was done there. Thanks to [@cleaton](https://github.com/cleaton) for the report ([#3204](https://github.com/nats-io/nats-server/issues/3204))
    - Some data races that could also lead to a server panic when processing a subscription and trying to send it to routes ([#3222](https://github.com/nats-io/nats-server/issues/3222))
- Gateway:
    - Possible panic if monitor endpoint is inspected too soon on startup ([#3374](https://github.com/nats-io/nats-server/issues/3374))
    - Possible routing issues with System messages and JetStream when internal subscriptions are created and internal requests are sent through gateways. The replies may sometimes be missed ([#3427](https://github.com/nats-io/nats-server/issues/3427))
- JetStream:
    - Suppress consumer and stream advisories on server restart and any direct stream get message ([#3156](https://github.com/nats-io/nats-server/issues/3156), [#3160](https://github.com/nats-io/nats-server/issues/3160), [#3162](https://github.com/nats-io/nats-server/issues/3162))
    - Possibly fail to retrieve a newly stored message. This would happen when stores and load using "last for subject" were concurrent ([#3159](https://github.com/nats-io/nats-server/issues/3159))
    - When using Republish feature, republish on the republish subject and place original subject in a `Nate-Subject` header - similar to stream direct get messages ([#3169](https://github.com/nats-io/nats-server/issues/3169))
    - Data race with account exported services. Thanks to [@Davincible](https://github.com/Davincible) for the report ([#3189](https://github.com/nats-io/nats-server/issues/3189))
    - Path separators in consumer or stream names prevented restoring the stream. Thanks to [@daudcanugerah](https://github.com/daudcanugerah) for the report ([#3205](https://github.com/nats-io/nats-server/issues/3205))
    - Pull consumer may be incorrectly removed after the `InactiveThreshold` interval ([#3229](https://github.com/nats-io/nats-server/issues/3229))
    - When a pull consumer is stalled for `MaxAckPending`, expire all pull requests that had at least 1 delivered message ([#3241](https://github.com/nats-io/nats-server/issues/3241))
    - Possible bad stream placement when increasing the stream replica's value (scale up) and using `unique_tag` configuration ([#3248](https://github.com/nats-io/nats-server/issues/3248))
    - Stalled stream catchup in endless cycle on EOF error trying to retrieve a catchup message ([#3249](https://github.com/nats-io/nats-server/issues/3249))
    - Servers may be reported as orphaned (considering the node offline) ([#3258](https://github.com/nats-io/nats-server/issues/3258))
    - Updates to a stream mirror configuration were not rejected in standalone mode, as they are in cluster mode ([#3269](https://github.com/nats-io/nats-server/issues/3269))
    - Possible panic when removing a mirror configuration from a stream, which was possible because of the issue above. Thanks to [@chris13524](https://github.com/chris13524) for the report ([#3269](https://github.com/nats-io/nats-server/issues/3269))
    - A stream with `RePublish` configuration would republish any incoming message, regardless of the `RePublish`'s source subject ([#3271](https://github.com/nats-io/nats-server/issues/3271))
    - Raft issue that could cause a follower's log index to be ahead of the leader's by 1 ([#3277](https://github.com/nats-io/nats-server/issues/3277))
    - Possible panic in cluster mode. Thanks to [@vishaltripathi24](https://github.com/vishaltripathi24) for the report ([#3279](https://github.com/nats-io/nats-server/issues/3279))
    - Scale down of a stream was not always waiting for the scale down of its consumers ([#3282](https://github.com/nats-io/nats-server/issues/3282))
    - Short index write could lead to loss of stream sequence for an empty stream ([#3283](https://github.com/nats-io/nats-server/issues/3283))
    - Losing stream sequence on multiple restarts and leader changes ([#3284](https://github.com/nats-io/nats-server/issues/3284))
    - In clustering mode, the "stream names" API could return more than the JS API limit, and "stream infos" list would return that the total of stream is the JS API limit of 256 channels, when in reality it could be more ([#3287](https://github.com/nats-io/nats-server/issues/3287))
    - Scaling up and some RAFT issues ([#3288](https://github.com/nats-io/nats-server/issues/3288))
    - Reject update of a pull consumer `MaxWaiting` configuration since it is not currently supported ([#3302](https://github.com/nats-io/nats-server/issues/3302))
    - Catching up a RAFT follower that involved snapshots ([#3307](https://github.com/nats-io/nats-server/issues/3307))
    - Instability with encrypted systems ([#3314](https://github.com/nats-io/nats-server/issues/3314))
    - Reject stream update with changes to the `RePublish` configuration since it is not currently supported ([#3328](https://github.com/nats-io/nats-server/issues/3328))
    - Internal consumer restart on source filter update ([#3355](https://github.com/nats-io/nats-server/issues/3355))
    - The proper error regarding "subject overlap" was not returned in standalone mode (was correct in clustering mode). Thanks to [@swDevTX](https://github.com/swDevTX) for the report ([#3363](https://github.com/nats-io/nats-server/issues/3363))
    - Messages that had reached the max delivery count may be redelivered after a server or cluster restart ([#3365](https://github.com/nats-io/nats-server/issues/3365))
    - Issue if the source consumer changes the subjects filter ([#3364](https://github.com/nats-io/nats-server/issues/3364))
    - Possible race during an asset scale down that could cause a node to prematurely being considered caught up ([#3381](https://github.com/nats-io/nats-server/issues/3381))
    - Consumer subject validation on recovery which caused delivery of messages to a downstream stream that should not have been delivered ([#3389](https://github.com/nats-io/nats-server/issues/3389))
    - Issues with concurrent use of `Nats-Expected` headers, such as concurrent uses of kv.Create()/kv.Delete() ([#3400](https://github.com/nats-io/nats-server/issues/3400)) 
    - Stream information numbers may be 0 after a cluster restart ([#3411](https://github.com/nats-io/nats-server/issues/3411))
    - Wrong streams returned in some conditions involving wildcard in subjects and filters ([#3423](https://github.com/nats-io/nats-server/issues/3423))
    - Expired/removed accounts were counted toward limits. Thanks to [@JulienVdG](https://github.com/JulienVdG) for the contribution ([#3421](https://github.com/nats-io/nats-server/issues/3421), [#3428](https://github.com/nats-io/nats-server/issues/3428))
    - Don't allow subjects overlap between multiple subjects for a stream (for instance configuration that had subjects `"foo.*", "foo.bar"` would create duplicate messages if produced on `foo.bar` and will now be reported as an invalid configuration ([#3429](https://github.com/nats-io/nats-server/issues/3429))
    - Some nodes in a super cluster may never be reported as `offline` in some situations ([#3433](https://github.com/nats-io/nats-server/issues/3433))
    - Restarted queue subscriptions may not receive messages. Thanks to [@kyle8615](https://github.com/kyle8615) for the report ([#3440](https://github.com/nats-io/nats-server/issues/3440))
    - Durables with replicas of 1 but different from the stream's replicas could possibly be incorrectly migrated on server shutdown ([#3451](https://github.com/nats-io/nats-server/issues/3451))
- MQTT:
    - Possible panic when clients repeatedly connect with a client ID while that is already connected. Thanks to [@gebv](https://github.com/gebv) for the report ([#3315](https://github.com/nats-io/nats-server/issues/3315))
- Delivery of internal system messages to queue subscribers across routes. Thanks to [@rebelf](https://github.com/rebelf) for the report ([#3185](https://github.com/nats-io/nats-server/issues/3185))
- Memory leak when unsubscribing the last queue subscription on a given queue group ([#3338](https://github.com/nats-io/nats-server/issues/3338))
- Issue with services import/export cycles that could cause stack-overflow. Thanks to [@apis](https://github.com/apis) for the report ([#3407](https://github.com/nats-io/nats-server/issues/3407))
- Strings that started with a number and had some of the "bytes" suffix (such as K/G/etc..) but not as suffixes were not parsed properly. Thanks to [@julian-dolce-form3](https://github.com/julian-dolce-form3) for the report ([#3434](https://github.com/nats-io/nats-server/issues/3434), [#3436](https://github.com/nats-io/nats-server/issues/3436))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.8.4...v2.9.0



[Changes][v2.9.0]


<a id="v2.8.4"></a>
# [Release v2.8.4](https://github.com/nats-io/nats-server/releases/tag/v2.8.4) - 2022-05-26

## Changelog



### Go Version
- 1.17.10: Both release executables and Docker images are built with this Go release.


### Fixed
- JetStream:
    - Consumer pending count was not correct when a stream had max messages per subject greater than 1 and a consumer that filtered out part of the stream was created ([#3148](https://github.com/nats-io/nats-server/issues/3148))
    - Spurious 408 status messages sent to pull consumers. Also made some modifications to reduce lock contention between processing of acknowledgments and delivery of messages ([#3149](https://github.com/nats-io/nats-server/issues/3149))
- MQTT:
    - Connections with same client ID but on different domains may have been detected as duplicates ([#3150](https://github.com/nats-io/nats-server/issues/3150))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.8.3...v2.8.4




[Changes][v2.8.4]


<a id="v2.8.3"></a>
# [Release v2.8.3](https://github.com/nats-io/nats-server/releases/tag/v2.8.3) - 2022-05-23

## Changelog



### Go Version
- 1.17.10: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Consumer configuration has now `MaxRequestMaxBytes` as a `int` to limit a pull request to that many bytes ([#3126](https://github.com/nats-io/nats-server/issues/3126))
    - Allow explicit consumer configuration of replica count and memory storage ([#3128](https://github.com/nats-io/nats-server/issues/3128), [#3131](https://github.com/nats-io/nats-server/issues/3131)). **Note:** This feature is not compatible or tested in conjunction with modifying the associated stream's replication factor once created (2.7.3+).
    - **EXPERIMENTAL FEATURE**: Stream configuration has now `RePublish` which is a `SubjectMapping` with a `Source` and `Destination`. This enables lightweight distribution of messages to very large number of NATS subscribers ([#3129](https://github.com/nats-io/nats-server/issues/3129), [#3138](https://github.com/nats-io/nats-server/issues/3138))
- MQTT:
    - New configuration parameters `stream_replicas`, `consumer_replicas` and `consumer_memory_storage`, which allow the override of the replica count for MQTT streams (which are normally auto-determined base on the cluster size, but capped at 3) and also override the consumer replica count and possibly underlying storage (based on feature added in [#3128](https://github.com/nats-io/nats-server/issues/3128)). Thanks to [@ianzhang1988](https://github.com/ianzhang1988) for the feedback ([#3130](https://github.com/nats-io/nats-server/issues/3130))

### Improved
- JetStream:
    - Tweak RAFT election timeouts and heartbeats and also reduce lock contention ([#3132](https://github.com/nats-io/nats-server/issues/3132), [#3135](https://github.com/nats-io/nats-server/issues/3135))
- Monitoring:
    - A route information in `/routez` will now include `start`, `last_activity`, `uptime` and `idle` fields ([#3133](https://github.com/nats-io/nats-server/issues/3133))

### Fixed
- JetStream:
    - Panic due to non loaded message cache during a file compaction ([#3110](https://github.com/nats-io/nats-server/issues/3110))
    - Downstream sourced retention policy streams during server restart have redelivered messages ([#3109](https://github.com/nats-io/nats-server/issues/3109), [#3122](https://github.com/nats-io/nats-server/issues/3122))
    - Possible lockup due to a missing mutex unlock in some conditions ([#3115](https://github.com/nats-io/nats-server/issues/3115))
    - Panic when processing a consumer create in clustered mode. This was introduced in v2.8.2 ([#3117](https://github.com/nats-io/nats-server/issues/3117))
    - Some data races ([#3120](https://github.com/nats-io/nats-server/issues/3120), [#3134](https://github.com/nats-io/nats-server/issues/3134))
    - Panic `runtime error: makeslice: cap out of range` on server restart. Thanks to [@ajlane](https://github.com/ajlane) for the report ([#3121](https://github.com/nats-io/nats-server/issues/3121))
    - A purge with a given subject and a sequence of 1 for a stream with a single message would not work ([#3121](https://github.com/nats-io/nats-server/issues/3121))
    - Some JS API requests handled through routes or gateways may be dropped when there were too many of them at once ([#3142](https://github.com/nats-io/nats-server/issues/3142))
    - Consumer with "deliver new" delivery mode could skip messages in some conditions after a consumer step down or server(s) restart ([#3143](https://github.com/nats-io/nats-server/issues/3143))
- MQTT:
    - Errors deleting consumers will now prevent deletion of the session record, which would otherwise leave orphan consumers. Thanks to [@ianzhang1988](https://github.com/ianzhang1988) for the report ([#3144](https://github.com/nats-io/nats-server/issues/3144))
- Accounts Export/Import isolation with overlap subjects. Thanks to [@JH7](https://github.com/JH7) for the report ([#3112](https://github.com/nats-io/nats-server/issues/3112))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.8.2...v2.8.3




[Changes][v2.8.3]


<a id="v2.8.2"></a>
# [Release v2.8.2](https://github.com/nats-io/nats-server/releases/tag/v2.8.2) - 2022-05-04

## Changelog

### Go Version
- 1.17.9: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - A `JSConsumerDeliveryNakAdvisory` when a message is nak'ed. Thanks to [@Coffeeri](https://github.com/Coffeeri) for the contribution ([#3074](https://github.com/nats-io/nats-server/issues/3074))

### Improved:
- JetStream:
    - KeyValue memory store performance for history of 1 ([#3081](https://github.com/nats-io/nats-server/issues/3081))
    - A check was done twice (per message) for max_msg_per_subject and discard_new policy, which is the case for KV stores ([#3101](https://github.com/nats-io/nats-server/issues/3101))
    - Reduce the number of RAFT warnings to 1 per second (for exact same log line) ([#3100](https://github.com/nats-io/nats-server/issues/3100))
    - Bumped the default file store block size ([#3102](https://github.com/nats-io/nats-server/issues/3102))

### Changed
- JetStream:
    - When reaching the max number of pull requests, the server would evict existing pending requests (with a `408 Request Canceled`) instead of sending a `409 Exceeded MaxWaiting` to the new request ([#3099](https://github.com/nats-io/nats-server/issues/3099)) 

### Fixed
- JetStream:
    - Step-down timing for consumers or streams. When becoming leader a consumer's stream sequence number could be reset ([#3079](https://github.com/nats-io/nats-server/issues/3079))
    - Not able to recover a consumer if a source based stream which housed the consumer filter subject was removed. Also made updates to the `/healthz` behavior ([#3080](https://github.com/nats-io/nats-server/issues/3080))
    - File store compaction would not work well for blocks with small messages and lots of interior deletes. The index file was also written too often ([#3087](https://github.com/nats-io/nats-server/issues/3087))
    - Subject transforms and delivery subjects ([#3088](https://github.com/nats-io/nats-server/issues/3088))
    - Lock inversion, but no known situations that would have triggered it ([#3092](https://github.com/nats-io/nats-server/issues/3092))
    - Possible panic when checking for RAFT group leaderless status ([#3094](https://github.com/nats-io/nats-server/issues/3094))
    - A pull consumer with a MaxWaiting of 1 would not send the notification that the request timed-out ([#3099](https://github.com/nats-io/nats-server/issues/3099))
    - RAFT and clustering issues that may have contributed to repeated warnings about inability to store an entry to WAL and expected first catchup entry to be a snapshot and peer state ([#3100](https://github.com/nats-io/nats-server/issues/3100))
- Permissions:
    - Queue subscription's deny subjects were not always enforced. CVE-2022-29946 ([#3090](https://github.com/nats-io/nats-server/issues/3090))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.8.1...v2.8.2



[Changes][v2.8.2]


<a id="v2.8.1"></a>
# [Release v2.8.1](https://github.com/nats-io/nats-server/releases/tag/v2.8.1) - 2022-04-21

## Changelog



### Go Version
- 1.17.9: Both release executables and Docker images are built with this Go release.

### Changed:
- JetStream:
    - Enforce a minimum of 100ms for "max age" and "duplicates window" settings ([#3056](https://github.com/nats-io/nats-server/issues/3056))

### Updated:
- Dependencies:
    - golang.org/x/crypto due to a CVE scan. However, since this affects `crypto/ssh` that the server is not using, the vulnerability does not impact the NATS Server. Thank you to [@pgvishnuram](https://github.com/pgvishnuram) for the contribution ([#3065](https://github.com/nats-io/nats-server/issues/3065))

### Fixed
- JetStream:
    - Assets migration may fail due to an aggressive internal polling mechanism ([#3059](https://github.com/nats-io/nats-server/issues/3059))
    - Mirror streams would fail to be recovered from an earlier NATS Server versions with an error regarding duplicate window. Thanks to [@yadvlz](https://github.com/yadvlz) and [@BenChen-PKI](https://github.com/BenChen-PKI) for the reports ([#3060](https://github.com/nats-io/nats-server/issues/3060), [#3067](https://github.com/nats-io/nats-server/issues/3067))
    - Mirror and Sources issues, especially in a mixed-mode super cluster layout ([#3061](https://github.com/nats-io/nats-server/issues/3061), [#3066](https://github.com/nats-io/nats-server/issues/3066))
    - A stream with a source may be canceled when the stream was updated. Thanks to [@sargikk](https://github.com/sargikk) for the report ([#3061](https://github.com/nats-io/nats-server/issues/3061))
- LeafNode:
    - Failed to propagate subscription interest after a configuration reload. Thanks to [@LLLLimbo](https://github.com/LLLLimbo) for the report ([#3058](https://github.com/nats-io/nats-server/issues/3058))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.8.0...v2.8.1



[Changes][v2.8.1]


<a id="v2.8.0"></a>
# [Release v2.8.0](https://github.com/nats-io/nats-server/releases/tag/v2.8.0) - 2022-04-18

## Changelog



### Go Version
- 1.17.9: Both release executables and Docker images are built with this Go release.

### Added:
- LeafNode:
    - Support for a `min_version` in the `leafnodes{}` that would reject servers with a lower version. Note that this would work only for servers that are v2.8.0 and above ([#3013](https://github.com/nats-io/nats-server/issues/3013))
- Monitoring:
    - Server version in monitoring landing page ([#2928](https://github.com/nats-io/nats-server/issues/2928))
    - Logging to `/healthz` endpoint when failure occurs. Thanks to [@samuel-form3](https://github.com/samuel-form3) for the contribution ([#2976](https://github.com/nats-io/nats-server/issues/2976))
    - MQTT and Websocket blocks in the `/varz` endpoint ([#2996](https://github.com/nats-io/nats-server/issues/2996))
- JetStream:
    - Consumer check added to `healthz` endpoint ([#2927](https://github.com/nats-io/nats-server/issues/2927))
    - Max stream bytes checks ([#2970](https://github.com/nats-io/nats-server/issues/2970))
    - Ability to limit a consumer's MaxAckPending value ([#2982](https://github.com/nats-io/nats-server/issues/2982))
    - Allow streams and consumers to migrate between clusters. This feature is considered "beta" ([#3001](https://github.com/nats-io/nats-server/issues/3001), [#3036](https://github.com/nats-io/nats-server/issues/3036), [#3041](https://github.com/nats-io/nats-server/issues/3041), [#3043](https://github.com/nats-io/nats-server/issues/3043), [#3047](https://github.com/nats-io/nats-server/issues/3047))
    - New `unique_tag` option in `jetstream{}` configuration block to prevent placing a stream in the same availability zone twice ([#3011](https://github.com/nats-io/nats-server/issues/3011))
    - Stream `Alternates` field in `StreamInfo` response. They provide a priority list of mirrors and the source in relation to where the request originated ([#3023](https://github.com/nats-io/nats-server/issues/3023))
- Deterministic subject tokens to partition mapping ([#2890](https://github.com/nats-io/nats-server/issues/2890))

### Changed:
- Gateway:
    - Duplicate server names are now detected across a super-cluster. Server names ought to be unique, which is critical when JetStream is used ([#2923](https://github.com/nats-io/nats-server/issues/2923))
- JetStream:
    - Processing of consumers acknowledgments are now done outside of the socket read go routine. This helps reducing occurrences of "Readloop" warnings for route connections when there was a lot of pending acknowledgements to process per consumer. The redeliveries are also possibly postponed (by chunk of 100ms) to favor processing of acknowledgements ([#2898](https://github.com/nats-io/nats-server/issues/2898))
    - Lower default consumer's "Maximum Ack Pending" from 20,000 to 1,000. This affects only applications/NATS CLI that do not set an explicit value. If you notice a performance degradation and your system can handle very well a higher value, then the parameter should be explicitly configured to a higher value when creating the consumer/subscription ([#2972](https://github.com/nats-io/nats-server/issues/2972))
- Duplicate user names in `authorization{}` and `accounts{}` blocks are now detected and will fail the start of the server. Thanks to [@smlx](https://github.com/smlx) for the report ([#2943](https://github.com/nats-io/nats-server/issues/2943))

### Improved
- Configuration:
    - Skip exact duplicate URLS for routes, gateways or leaf nodes. Duplicate route URLs could cause issues for JetStream cases since it may prevent electing a leader ([#2930](https://github.com/nats-io/nats-server/issues/2930))
- Logging:
    - Limiting rate of some identical warnings ([#2994](https://github.com/nats-io/nats-server/issues/2994))
- JetStream:
    - Behavior of "list" and "delete" operations for offline streams has been improved (responses no longer hanging or failing) ([#2925](https://github.com/nats-io/nats-server/issues/2925))
    - When a consumer had a backoff duration list, the server could check for redeliveries more frequently than it should have. The redelivery timing was still honored though ([#2948](https://github.com/nats-io/nats-server/issues/2948))
    - Ensures the cluster information in `/jsz` monitoring endpoint is sent from the leader only ([#2932](https://github.com/nats-io/nats-server/issues/2932), [#2983](https://github.com/nats-io/nats-server/issues/2983), [#2984](https://github.com/nats-io/nats-server/issues/2984))
    - Memory pooling and management ([#2960](https://github.com/nats-io/nats-server/issues/2960))
    - Consumer snapshot logic and disk usage in clustered mode. Thanks to [@phho](https://github.com/phho) for the report ([#2973](https://github.com/nats-io/nats-server/issues/2973))
    - Performance of ordered consumers (and stream catchup) with longer RTTs ([#2975](https://github.com/nats-io/nats-server/issues/2975))
    - Performance for streams containing multiple subjects and consumer with a filter. Thanks to [@samuel-form3](https://github.com/samuel-form3) for the report ([#3008](https://github.com/nats-io/nats-server/issues/3008))
    - Reduction of unnecessary leader elections ([#3035](https://github.com/nats-io/nats-server/issues/3035))
    - On recovery, the server will now print the filenames for which bad checksums were detected ([#3053](https://github.com/nats-io/nats-server/issues/3053))

### Fixed
- JetStream:
    - Consumer state handling, for instance a consumer with "DeliverNew" deliver policy could receive old messages after a server restart in some cases ([#2927](https://github.com/nats-io/nats-server/issues/2927))
    - Removal of an external source stream was not working properly. Thanks to [@kylebernhardy](https://github.com/kylebernhardy) for the report ([#2938](https://github.com/nats-io/nats-server/issues/2938))
    - Possible panic on leadership change notices ([#2939](https://github.com/nats-io/nats-server/issues/2939))
    - Possible deadlock during consumer leadership change. Thanks to [@wchajl](https://github.com/wchajl) for the report ([#2951](https://github.com/nats-io/nats-server/issues/2951))
    - Scaling up a stream was not replicating existing messages ([#2958](https://github.com/nats-io/nats-server/issues/2958))
    - Heavy contention in file store could result in underflow and panic. Thanks to [@whynowy](https://github.com/whynowy) for the report ([#2959](https://github.com/nats-io/nats-server/issues/2959))
    - Consumer sample frequency not updated during a consumer update. Thanks to [@boris-ilijic](https://github.com/boris-ilijic) for the report and contribution ([#2966](https://github.com/nats-io/nats-server/issues/2966))
    - Some limit issues on update ([#2945](https://github.com/nats-io/nats-server/issues/2945))
    - Memory based replicated consumers could possibly stop working after snapshots and server restart. The `$SYS` folder could also being seen as growing in size. Thanks to [@phho](https://github.com/phho) and [@MilkyWay-core](https://github.com/MilkyWay-core) for the reports ([#2973](https://github.com/nats-io/nats-server/issues/2973))
    - Possible panic (due to data races) when processing pull consumer requests. Thanks to [@phho](https://github.com/phho) for the report ([#2977](https://github.com/nats-io/nats-server/issues/2977))
    - Account stream imports were not removed on configuration reload. Thanks to [@alirezaDavid](https://github.com/alirezaDavid) for the report ([#2978](https://github.com/nats-io/nats-server/issues/2978))
    - Sealed streams would not recover on server restart ([#2991](https://github.com/nats-io/nats-server/issues/2991))
    - Possible panic on server shutdown trying to migrate ephemeral consumers ([#2999](https://github.com/nats-io/nats-server/issues/2999))
    - A "next message" request for a pull consumer, when going over a gateway but down to a Leafnode could fail ([#3016](https://github.com/nats-io/nats-server/issues/3016))
    - Consumer deliver subject incorrect when imported and crossing route or gateway ([#3017](https://github.com/nats-io/nats-server/issues/3017), [#3025](https://github.com/nats-io/nats-server/issues/3025))
    - RAFT layer for stability and leader election ([#3020](https://github.com/nats-io/nats-server/issues/3020))
    - Memory stream potentially delivering duplicates during a node restart. Thanks to [@aksdb](https://github.com/aksdb) for the report ([#3020](https://github.com/nats-io/nats-server/issues/3020))
    - A stream could become leader when it should not, causing messages to be lost ([#3029](https://github.com/nats-io/nats-server/issues/3029))
    - A stream catchup could stall because the server sending data could fail during the process but still send an indication that the other server that catchup did complete ([#3029](https://github.com/nats-io/nats-server/issues/3029), [#3040](https://github.com/nats-io/nats-server/issues/3040))
    - Route could be blocked when processing an API call while an asset was recovering a big file ([#3035](https://github.com/nats-io/nats-server/issues/3035))
    - Assets (streams or consumers) state could be removed if they had been recreated after being initially removed ([#3039](https://github.com/nats-io/nats-server/issues/3039))
    - When running on mixed-mode, a JetStream export could be removed on JWT update ([#3044](https://github.com/nats-io/nats-server/issues/3044))
    - Possible panic on cluster step-down of a consumer ([#3045](https://github.com/nats-io/nats-server/issues/3045))
    - Some limit enforcement issues and prevent loops in cyclic source stream configurations ([#3046](https://github.com/nats-io/nats-server/issues/3046))
    - Some stream source issues, including missing messages and possible stall ([#3052](https://github.com/nats-io/nats-server/issues/3052))
    - On configuration reload, JetStream would be disabled if it was enabled only from the command line options. Thanks to [@xieyuschen](https://github.com/xieyuschen) for the contribution ([#3050](https://github.com/nats-io/nats-server/issues/3050))
- Leafnode:
    - Interest propagation issue when crossing accounts and the leaf connection is recreated. This could also manifest with JetStream since internally there are subscriptions that do cross accounts. Thanks to [@LLLLimbo](https://github.com/LLLLimbo) and [@JH7](https://github.com/JH7) for the report ([#3031](https://github.com/nats-io/nats-server/issues/3031))
- Monitoring:
    - `reserved_memory` and/or `reserved_storage` in the `jetstream{}` block of the `/varz` endpoint could show incorrect huge number due to a `unint64` underflow ([#2907](https://github.com/nats-io/nats-server/issues/2907))
    - `verify_and_map` in the `tls{}` block would prevent inspecting the monitoring page when using the secure `https` port. Thanks to [@rsoberano-ld](https://github.com/rsoberano-ld) for the report ([#2981](https://github.com/nats-io/nats-server/issues/2981))
    - Possible deadlock when inspecting the `/jsz` endpoint ([#3029](https://github.com/nats-io/nats-server/issues/3029))
- Miscellaneous:
    - Client connection occasionally incorrectly assigned to the global account. This happened when the configuration was incorrectly referencing the same user in `authorization{}` and `accounts{}`. Thanks to [@smlx](https://github.com/smlx) for the report ([#2943](https://github.com/nats-io/nats-server/issues/2943))
    - The NATS account resolver, while synchronizing all JWTs, would not validate the nkey(s) or jwt(s) received via the system account (CVE-2022-28357) ([#2985](https://github.com/nats-io/nats-server/issues/2985))
    - Reject messages from application that have an invalid reply subject (contains the `$JS.ACK` prefix) ([#3026](https://github.com/nats-io/nats-server/issues/3026))
    - Allow server to run as system user on Windows. Thanks to [@LaurensVergote](https://github.com/LaurensVergote) for the contribution ([#3022](https://github.com/nats-io/nats-server/issues/3022))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.7.4...v2.8.0




[Changes][v2.8.0]


<a id="v2.7.4"></a>
# [Release v2.7.4](https://github.com/nats-io/nats-server/releases/tag/v2.7.4) - 2022-03-09

## Changelog

## Note about .deb/.rpm packages

We use [Goreleaser](https://github.com/goreleaser/goreleaser) to build our assets, and it seems that it changed the default install from `/usr/local/bin` to `/usr/bin`. See Goreleaser's change [here](https://github.com/goreleaser/goreleaser/pull/2910). We are sorry for the inconvenience this may cause.

### Go Version
- 1.17.8: Both release executables and Docker images are built with this Go release.

### Improved
- JetStream:
    - Better startup logging to help debug RAFT log directories to streams/consumers ([#2897](https://github.com/nats-io/nats-server/issues/2897))

### Fixed
- JetStream:
    - The consumers count when getting stream information could be wrong in clustered mode ([#2896](https://github.com/nats-io/nats-server/issues/2896))
    - Never used clustered and filtered consumers consume storage under `$SYS`. Thanks to [@nayanparegi](https://github.com/nayanparegi) and [@aksdb](https://github.com/aksdb) for the reports ([#2899](https://github.com/nats-io/nats-server/issues/2899), [#2914](https://github.com/nats-io/nats-server/issues/2914))
    - Stream not recovered on restart with "deleted message", "checksum" or "no message cache" errors ([#2900](https://github.com/nats-io/nats-server/issues/2900))
    - Schema violations in the NATS CLI tool caused by large number overflow when "active" field for Sources and Mirrors was computed and there had been no contact yet ([#2903](https://github.com/nats-io/nats-server/issues/2903))
    - Some Stream advisories were missing ([#2887](https://github.com/nats-io/nats-server/issues/2887))
    - Inconsistent durable consumer state after stream peer removal ([#2904](https://github.com/nats-io/nats-server/issues/2904))
    - Scaling up and down for streams and consumers ([#2905](https://github.com/nats-io/nats-server/issues/2905))
    - Validate files' path when restoring stream from a snapshot/backup (CVE-2022-26652). Thanks to Yiming Xiang for the report ([#2917](https://github.com/nats-io/nats-server/issues/2917))
- Monitoring:
    - Panic on non 64-bit platforms due to an unaligned 64-bit atomic operation. Thanks to [@mlorenz-tug](https://github.com/mlorenz-tug) for the report ([#2915](https://github.com/nats-io/nats-server/issues/2915))
- LeafNode:
    - Queue subscription interest could be suppressed in complex situations causing messages to not flow from a LeafNode server to the rest of the (super)cluster ([#2901](https://github.com/nats-io/nats-server/issues/2901))
- Fixed some lock inversions ([#2911](https://github.com/nats-io/nats-server/issues/2911))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.7.3...v2.7.4




[Changes][v2.7.4]


<a id="v2.7.3"></a>
# [Release v2.7.3](https://github.com/nats-io/nats-server/releases/tag/v2.7.3) - 2022-02-24

## Changelog


### Go Version
- 1.17.7: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Allow replica updates during stream update ([#2867](https://github.com/nats-io/nats-server/issues/2867))
    - Allow stream placement by tags ([#2871](https://github.com/nats-io/nats-server/issues/2871))

### Updated
- Dependencies:
    - `github.com/klauspost/compress`: v1.13.4 -> v1.14.4
    - `github.com/minio/highwayhash` : v1.0.1  -> v1.0.2

### Improved
- JetStream:
    - Sparse consumers replay time. This is when a stream has multiple subjects and a consumer filters the stream to a small and spread out list of messages ([#2848](https://github.com/nats-io/nats-server/issues/2848), [#2863](https://github.com/nats-io/nats-server/issues/2863))
    - Small improvements to send performance when sending to a full stream ([#2877](https://github.com/nats-io/nats-server/issues/2877))

### Fixed
- Gateway:
    - Connection could fail with 'Authorization Violation' and parser error due to an initial `PING` possibly sent prior to the `CONNECT` protocol. Also, the server accepting a connection was not starting the authentication timer, which responsibility is to close the connection if the `CONNECT` protocol is not received within the gateway's authentication timeout. It can be configured with `authorization{ timeout: 5.0 }` in the `gateway{}` block, but if not set will default to 2 seconds. Thanks to [@jimenj1970](https://github.com/jimenj1970) for the report ([#2881](https://github.com/nats-io/nats-server/issues/2881))
- JetStream:
    - Flow control stall under specific conditions of messages size ([#2847](https://github.com/nats-io/nats-server/issues/2847))
    - A stream name is tied to its identity and cannot be changed on restore ([#2857](https://github.com/nats-io/nats-server/issues/2857))
    - The consumer information's state in response to a consumer create request could return inaccurate information, specially regarding number of pending messages ([#2858](https://github.com/nats-io/nats-server/issues/2858))
    - Remove "fss" files from a snapshot when a block is removed. Thanks to [@nekufa](https://github.com/nekufa) for the report ([#2859](https://github.com/nats-io/nats-server/issues/2859))
    - Prevent a panic when stream restore fails ([#2864](https://github.com/nats-io/nats-server/issues/2864)
    - Interest policy and staggered filtered consumers could fail to remove messages. Thanks to [@lukasGemela](https://github.com/lukasGemela) for the report ([#2875](https://github.com/nats-io/nats-server/issues/2875))
    - Remove "key" files when removing message blocks and encryption is used. Thanks to [@exename](https://github.com/exename) for the report ([#2878](https://github.com/nats-io/nats-server/issues/2878))
    - Reduce logging for internal message exchange ([#2879](https://github.com/nats-io/nats-server/issues/2879))
    - Rare possible re-use of internal RAFT inbox that could cause a node to receive a message on the wrong inbox, possibly leading to a panic. Servers would likely have to be started at the exact same time for that to happen ([#2879](https://github.com/nats-io/nats-server/issues/2879))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.7.2...v2.7.3



[Changes][v2.7.3]


<a id="v2.7.2"></a>
# [Release v2.7.2](https://github.com/nats-io/nats-server/releases/tag/v2.7.2) - 2022-02-04

## Changelog



### Go Version
- 1.17.6: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Ability to get per-subject details via `StreamInfo` responses. The request needs to include a `subjects_filter:<subject>` for the server to include the list. The `StreamInfo` structure now has `NumSubjects` that will always be set, but optionally `Subjects` which contains the list of distincts subjects present in the stream with the count of messages per subject ([#2833](https://github.com/nats-io/nats-server/issues/2833))

### Removed
- Dynamic account behaviors. The server option `AllowNewAccounts` and function `NewAccountsAllowed()` have been removed. Note that the option could only be set for applications embedding the NATS Server since configuration parsing was not parsing this option ([#2840](https://github.com/nats-io/nats-server/issues/2840))

### Fixed
- JetStream:
    - Adding streams may fail with "insufficient resources" in some cases ([#2824](https://github.com/nats-io/nats-server/issues/2824))
    - Possible panic when attempting to update a push consumer by removing the deliver subject ([#2829](https://github.com/nats-io/nats-server/issues/2829))
    - Consumer updates were not updated on disk and would be reverted after a server restart. Thanks to [@oliverpool](https://github.com/oliverpool) for the report ([#2830](https://github.com/nats-io/nats-server/issues/2830))
    - Behavior of a stream when `MaxMsgsPerSubject` is set along with `DiscardNew` discard policy. Unless the stream is reaching a limit, old messages will be removed on a per-subject basis ([#2831](https://github.com/nats-io/nats-server/issues/2831))
    - A pull request `no_wait:true` without expiration was not considering redeliveries ([#2832](https://github.com/nats-io/nats-server/issues/2832))
    - `BackOff` redeliveries would always use the first delay from the list if the consumer's sequence was not matching the stream sequence ([#2834](https://github.com/nats-io/nats-server/issues/2834))
    - Under certain scenarios, the number of pending messages (unprocessed messages reported by the NATS CLI) for a consumer could appear to get stuck at 0 ([#2835](https://github.com/nats-io/nats-server/issues/2835)) 
    - When a consumer had no filtered subject and was attached to an interest policy retention stream, the server could incorrectly drop messages ([#2838](https://github.com/nats-io/nats-server/issues/2838))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.7.1...v2.7.2




[Changes][v2.7.2]


<a id="v2.7.1"></a>
# [Release v2.7.1](https://github.com/nats-io/nats-server/releases/tag/v2.7.1) - 2022-01-25

## Changelog



### Go Version
- 1.17.6: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Support for a delay when `Nak`'ing a message ([#2812](https://github.com/nats-io/nats-server/issues/2812))
    - Support for a backoff list of times in the consumer configuration ([#2812](https://github.com/nats-io/nats-server/issues/2812))
- Monitoring:
    - Replication lag metrics added to the `jsz` endpoint. Thanks to [@mattstep](https://github.com/mattstep) for the contribution ([#2791](https://github.com/nats-io/nats-server/issues/2791))
    - `/healthz` endpoint which will return 200 with `{status: ok}` if and only if all configured ports are opened and, if JetStream is configured, there is contact with the meta leader and is current and all streams are up to date, otherwise returns 503 with `{ "status": "unavailable", "error": "DESCRIPTION" }` body ([#2815](https://github.com/nats-io/nats-server/issues/2815))

### Fixed
- JetStream:
    - Pull consumers have been reworked ([#2813](https://github.com/nats-io/nats-server/issues/2813))
    - A stream's state number of deleted messages could be negative ([#2814](https://github.com/nats-io/nats-server/issues/2814))
    - A message's "in progress" request may not be honored in case of consumer's leadership change ([#2812](https://github.com/nats-io/nats-server/issues/2812))
- Git commit hash was not present in v2.7.0 ([#2790](https://github.com/nats-io/nats-server/issues/2790))
- Possible deadlock during Client or Leaf JWT authentication ([#2803](https://github.com/nats-io/nats-server/issues/2803))
- Do not emit warning about plaintext password when only using accounts with NKey and setting `system_account` ([#2811](https://github.com/nats-io/nats-server/issues/2811))
- JWT based user/activation token revocation and granularity. Thanks to [@shurya-kumar](https://github.com/shurya-kumar) for the report ([#2816](https://github.com/nats-io/nats-server/issues/2816))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.7.0...v2.7.1



[Changes][v2.7.1]


<a id="v2.7.0"></a>
# [Release v2.7.0](https://github.com/nats-io/nats-server/releases/tag/v2.7.0) - 2022-01-14

## Changelog


### _Notice for JetStream Users_ ###

See **important** [note](https://github.com/nats-io/nats-server/pull/2693#issuecomment-996212582) if using LeafNode regarding domains.


### Go Version
- 1.17.6: Both release executables and Docker images are built with this Go release.

### Added
- Configuration:
    - Ability to configure account limits (`max_connections`, `max_subscriptions`, `max_payload`, `max_leafnodes`) in server configuration file ([#2755](https://github.com/nats-io/nats-server/issues/2755))
- JetStream:
    - Overflow placement for streams. A stream can now be placed in the closest cluster from the origin request if it can be placed there ([#2771](https://github.com/nats-io/nats-server/issues/2771), [#2779](https://github.com/nats-io/nats-server/issues/2779))
    - Support for ephemeral Pull consumers (client libraries will need to be updated to allow those) ([#2776](https://github.com/nats-io/nats-server/issues/2776))
    - New consumer configuration options ([#2776](https://github.com/nats-io/nats-server/issues/2776)):
        - For Pull Consumers:
            - `MaxRequestBatch` to limit the batch size any client can request
            - `MaxRequestExpires` to limit the expiration any client can request
        - For ephemeral consumers :
            - `InactiveThreshold` duration that instructs the server to cleanup ephemeral consumers that are inactive for that long
    - Ability to configure `max_file_store` and `max_memory_store` in the `jetstream{}` block as strings with the following suffixes `K`, `M`, `G` and `T`, for instance: `max_file_store: "256M"`. Thanks to [@hooksie1](https://github.com/hooksie1) for the contribution ([#2777](https://github.com/nats-io/nats-server/issues/2777))
    - Support for the JWT field `MaxBytesRequired`, which defines a per-account maximum bytes for assets ([#2779](https://github.com/nats-io/nats-server/issues/2779))
- MQTT:
    - Support for websocket protocol. MQTT clients must connect to the opened websocket port and add `/mqtt` to the URL path. Thanks to [@Etran-H](https://github.com/Etran-H) for the suggestion ([#2735](https://github.com/nats-io/nats-server/issues/2735))
- TLS:
    - Ability to rate-limit the clients connections by adding the `connection_rate_limit: <number of connections per seconds>` in the `tls{}` top-level block. Thanks to [@julius-welink](https://github.com/julius-welink) for the contribution ([#2573](https://github.com/nats-io/nats-server/issues/2573))

### Improved
- JetStream:
    - MemStore is improved in the presence of a lot of interior delete messages, which could happen for instance with the use of KV store ([#2752](https://github.com/nats-io/nats-server/issues/2752))
    - Pull consumers behavior: more resilient, better at waiting request management, and the ability to determine and watch interest cross account ([#2776](https://github.com/nats-io/nats-server/issues/2776))
    - In clustering mode, with streams with replication factor greater than 1, a server will now use much less memory. For instance: 1,000 streams, with 10 consumers each would use about 15GB of memory, with this change memory usage would go down to about 1.5GB ([#2775](https://github.com/nats-io/nats-server/issues/2775))
- Monitoring:
    - Make the HTTP(s) monitoring endpoints available before starting JetStream to be able to monitor whether it is available during startup and recovery of streams. This could help with health probes ([#2782](https://github.com/nats-io/nats-server/issues/2782))
- Websocket:
    - Added client IP from `X-Forwarded-For` header. Thanks to [@byazrail](https://github.com/byazrail) for the suggestion ([#2734](https://github.com/nats-io/nats-server/issues/2734), [#2769](https://github.com/nats-io/nats-server/issues/2769))

### Changed
- JetStream:
    - JetStream API traffic will now be denied across leaf nodes, unless the system account is shared and the availability domain is identical ([#2693](https://github.com/nats-io/nats-server/issues/2693))

### Updated
- Some dependencies ([#2727](https://github.com/nats-io/nats-server/issues/2727))

### Fixed
- JetStream:
    - Possible panic "could not decode consumer snapshot". Thanks to [@raoptimus](https://github.com/raoptimus) for the report ([#2738](https://github.com/nats-io/nats-server/issues/2738))
    - Added missing entries to stream and consumer list requests ([#2739](https://github.com/nats-io/nats-server/issues/2739))
    - Interest across gateways that would prevent a push consumer from receiving messages. Thanks to [@ZMing316](https://github.com/ZMing316) for the report ([#2750](https://github.com/nats-io/nats-server/issues/2750))
    - Stream could "stall" after printing warnings such as "AppendEntry failed to be placed on internal channel" ([#2751](https://github.com/nats-io/nats-server/issues/2751))
    - Stream (with `WorkQueue` retention policy) could have its first/last sequences reset to 0 after a non-clean server restart, causing pull subscriptions to fail getting newly published messages ([#2753](https://github.com/nats-io/nats-server/issues/2753))
    - Stall of consumers after getting a "partial cache" error message in the server log. Thanks to [@rino-pupkin](https://github.com/rino-pupkin), [@raoptimus](https://github.com/raoptimus), [@broken-ufa](https://github.com/broken-ufa) and [@abishai](https://github.com/abishai) for the report ([#2761](https://github.com/nats-io/nats-server/issues/2761))
    - Large number of ephemeral consumers could exhaust Go runtime's maximum threads. Thanks to [@rh2048](https://github.com/rh2048), [@OpenGuidou](https://github.com/OpenGuidou) for the report ([#2764](https://github.com/nats-io/nats-server/issues/2764))
    - Restarting a cluster with lots of streams/consumers could cause routes to break with "write deadline" in some situations ([#2780](https://github.com/nats-io/nats-server/issues/2780))
    - Changes to inter-process communications could solve some issues caused by inability to send messages to internal Go channels ([#2775](https://github.com/nats-io/nats-server/issues/2775), [#2780](https://github.com/nats-io/nats-server/issues/2780))
    - A consumer's number of pending messages (or "Unprocessed Messages" in NATS cli) could be wrong when the consumer was created with inflight messages. Stepping down of the consumer leader would solve the discrepancy ([#2776](https://github.com/nats-io/nats-server/issues/2776))
    - Possible file store deadlock in when trying to rebuild some state ([#2781](https://github.com/nats-io/nats-server/issues/2781))
- Monitoring:
    - Possible panic when inspecting the `/jsz` endpoint. Thanks to [@rh2048](https://github.com/rh2048) for the report ([#2743](https://github.com/nats-io/nats-server/issues/2743))
- TLS:
    - When setting `verify_and_map` to `true`, if a connection connects with a certificate with an ID matching an existing user, but that user's `allowed_connection_types` is specified and does not have the type of the connection trying to connect, the server would panic ([#2747](https://github.com/nats-io/nats-server/issues/2747))
- Check for `no_auth_user` should be done only when no authentication at all is provided by the connection ([#2744](https://github.com/nats-io/nats-server/issues/2744))
- System account issue where the wrong structure was updated ([#2757](https://github.com/nats-io/nats-server/issues/2757))
- Broken link to monitoring documentation. Thanks to [@kfabryczny](https://github.com/kfabryczny) for the contribution ([#2766](https://github.com/nats-io/nats-server/issues/2766))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.6...v2.7.0




[Changes][v2.7.0]


<a id="v2.6.6"></a>
# [Release v2.6.6](https://github.com/nats-io/nats-server/releases/tag/v2.6.6) - 2021-12-02

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.10: Both release executables and Docker images are built with this Go release.

### Changed
- Profiler:
    - Moved the start of the profiler earlier in the start sequence so it is possible to get some insight should a start take a long time (for instance recovering JetStream datastore) ([#2723](https://github.com/nats-io/nats-server/issues/2723))
- Added `LEAFNODE_WS` in `allowed_connection_types` to allow or prevent Leafnode over websocket. Previously, if `allowed_connection_types` was specified and contained `LEAFNODE`, this user could be used to create a Leafnode connection over websocket. The existing `WEBSOCKET` connection type is used for regular client applications ([#2707](https://github.com/nats-io/nats-server/issues/2707))

### Fixed
- JetStream:
    - Slowness with large interior deletes ([#2711](https://github.com/nats-io/nats-server/issues/2711))
    - Possible panic when removing a source from a stream. Thanks to [@kylebernhardy](https://github.com/kylebernhardy) for the report ([#2712](https://github.com/nats-io/nats-server/issues/2712))
    - Reject invalid subjects in stream configuration, and attempt to fix them on recovery ([#2717](https://github.com/nats-io/nats-server/issues/2717), [#2721](https://github.com/nats-io/nats-server/issues/2721))
    - Prevent stream update to add subjects to mirror and remove them on recovery ([#2718](https://github.com/nats-io/nats-server/issues/2718), [#2721](https://github.com/nats-io/nats-server/issues/2721))
    - Application may receive corrupted headers when receiving from a consumer configured with "MetaOnly" option ([#2719](https://github.com/nats-io/nats-server/issues/2719))
    - Aligned internal timeout to 4 seconds for stream and consumer list requests ([#2722](https://github.com/nats-io/nats-server/issues/2722))
    - Incomplete error when consumer list in cluster mode failed ([#2725](https://github.com/nats-io/nats-server/issues/2725))
- Leafnode:
    - When using `allowed_connection_types`, it was not possible to restrict a Leafnode connection over websocket ([#2707](https://github.com/nats-io/nats-server/issues/2707))
    - Reaching the account connections limit left an outgoing Leafnode connection in a bad state ([#2715](https://github.com/nats-io/nats-server/issues/2715))
- Monitoring:
    - TLS configuration changes would not take effect after a configuration reload ([#2714](https://github.com/nats-io/nats-server/issues/2714))
- Documentation:
    - Links in the README. Thanks to [@Shoothzj](https://github.com/Shoothzj) for the contribution ([#2724](https://github.com/nats-io/nats-server/issues/2724))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.5...v2.6.6



[Changes][v2.6.6]


<a id="v2.6.5"></a>
# [Release v2.6.5](https://github.com/nats-io/nats-server/releases/tag/v2.6.5) - 2021-11-19

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.10: Both release executables and Docker images are built with this Go release.

### Added
- Server option `AlwaysEnableNonce` to force the server to send a nonce in the INFO block, regardless if the client connects with a NKey or not. Since its primarily useful to embedded scenarios there is no corresponding option in configuration file ([#2696](https://github.com/nats-io/nats-server/issues/2696), [#2699](https://github.com/nats-io/nats-server/issues/2699))

### Fixed
- JetStream:
    - The stream list API request was not using the optional "subject" filter ([#2695](https://github.com/nats-io/nats-server/issues/2695))
    - Followers may keep trying to reset streams, due to either snapshots not containing any actionable data or when stream had lot of failures by design (such as KV abstraction) ([#2702](https://github.com/nats-io/nats-server/issues/2702))
    - Memstore: setting max messages per subject to 1 would not be properly enforced ([#2704](https://github.com/nats-io/nats-server/issues/2704))
- OCSP:
    - Allow intermediates, or designated responders, to sign responses on behalf of the CA ([#2692](https://github.com/nats-io/nats-server/issues/2692))
- A slow consumer could cause the publisher to block. Thanks to [@cpiotr](https://github.com/cpiotr), [@GuanYin108](https://github.com/GuanYin108) and [@lukedodd](https://github.com/lukedodd) for the reports ([#2684](https://github.com/nats-io/nats-server/issues/2684))
- Possible panic due to latency tracking when more than one replies are sent ([#2688](https://github.com/nats-io/nats-server/issues/2688))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.4...v2.6.5




[Changes][v2.6.5]


<a id="v2.6.4"></a>
# [Release v2.6.4](https://github.com/nats-io/nats-server/releases/tag/v2.6.4) - 2021-11-04

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.10: Both release executables and Docker images are built with this Go release.

### Improved
- JetStream:
    - Allow system account to respond with "JetStream not enabled", which makes interactions with NATS cli better ([#2671](https://github.com/nats-io/nats-server/issues/2671))
    - Allow certain consumer attributes to be updated. Thanks to [@andreib1](https://github.com/andreib1) for the suggestion ([#2674](https://github.com/nats-io/nats-server/issues/2674))
- Websocket:
    - Add client IP in websocket upgrade error messages ([#2660](https://github.com/nats-io/nats-server/issues/2660))

### Fixed
- JetStream:
    - Messages not expiring properly after server restart ([#2665](https://github.com/nats-io/nats-server/issues/2665))
    - Conditional failures for stream messages could cause stream resets ([#2668](https://github.com/nats-io/nats-server/issues/2668))
    - Duplicate stream create returned wrong response type ([#2669](https://github.com/nats-io/nats-server/issues/2669))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.3...v2.6.4



[Changes][v2.6.4]


<a id="v2.6.3"></a>
# [Release v2.6.3](https://github.com/nats-io/nats-server/releases/tag/v2.6.3) - 2021-10-28

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.9: Both release executables and Docker images are built with this Go release.

### Fixed
- Gateways:
    - Each server within a cluster gossips its gateway URL, however, when two servers create route to each other and one route is dropped as a duplicate, a server may lose the peer's gateway URL and therefore would not be able to gossip it to inbound gateway connections ([#2653](https://github.com/nats-io/nats-server/issues/2653))
- JetStream:
    - Purging would, in some cases, not honor the amount of messages to keep after purge ([#2623](https://github.com/nats-io/nats-server/issues/2623))
    - Panic caused by the Unlock of unlocked RWMutex, which would happen through non client connections when the limit of JetStream API in-flight requests was reached. Thanks to [@minight](https://github.com/minight) for the report ([#2645](https://github.com/nats-io/nats-server/issues/2645))
    - Cluster becomes inconsistent with constant "catchup for stream stalled" warnings. Thanks to [@aksdb](https://github.com/aksdb) for the report ([#2648](https://github.com/nats-io/nats-server/issues/2648))
    - Ensure that stream's sealed, deny delete, deny purge and allow rollup booleans in stream config are always set (for languages that may not set to false when decoding JSON when absent) and set the deny delete and purge to true and allow rollup to false when a stream is sealed ([#2650](https://github.com/nats-io/nats-server/issues/2650))
    - Redelivered state of consumers was not properly decoded from file ([#2652](https://github.com/nats-io/nats-server/issues/2652))
- LeafNode:
    - Apparent memory leak on "Authorization Violation" errors. Thanks to [@DavidSimner](https://github.com/DavidSimner) for the contribution ([#2637](https://github.com/nats-io/nats-server/issues/2637))
- Monitoring:
    - The `/varz` endpoint may still show gateway URLs of servers that have been removed from the remote cluster ([#2647](https://github.com/nats-io/nats-server/issues/2647))
    - The `/varz` endpoint would not show the configured URLs of a remote gateway block if that remote gateway was the one of the server being inspected ([#2653](https://github.com/nats-io/nats-server/issues/2653))
- OCSP:
    - Only last of LeafNode and Gateway remotes were being updated. Thanks to [@guodongli-google](https://github.com/guodongli-google) for the report ([#2632](https://github.com/nats-io/nats-server/issues/2632))
- TLS:
    - If a connection is closed before the specified TLS timeout, the connection object would be retained in the go runtime due to the TLS timeout timer not being stopped, which could be seen as a memory leak, until that timer fired ([#2638](https://github.com/nats-io/nats-server/issues/2638))
- Redact URLs before logging. Thanks to [@fowlerp-qlik](https://github.com/fowlerp-qlik) for the report ([#2643](https://github.com/nats-io/nats-server/issues/2643))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.2...v2.6.3



[Changes][v2.6.3]


<a id="v2.6.2"></a>
# [Release v2.6.2](https://github.com/nats-io/nats-server/releases/tag/v2.6.2) - 2021-10-12

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.9: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Ability to seal a stream through a stream update ([#2584](https://github.com/nats-io/nats-server/issues/2584))
    - Ability to do roll ups ([#2585](https://github.com/nats-io/nats-server/issues/2585), [#2593](https://github.com/nats-io/nats-server/issues/2593), [#2600](https://github.com/nats-io/nats-server/issues/2600), [#2601](https://github.com/nats-io/nats-server/issues/2601))
    - Allow consumers to request only headers to be delivered with `HeadersOnly` consumer configuration ([#2596](https://github.com/nats-io/nats-server/issues/2596))
    - New stream configuration properties to limit delete/purge/rollups ([#2595](https://github.com/nats-io/nats-server/issues/2595))

### Updated
- JWT library dependency to v2.1.0

### Improved
- MQTT:
    - The client ID will now be part of the client log statements. Thanks to [@imranrazakhan](https://github.com/imranrazakhan) for the report ([#2598](https://github.com/nats-io/nats-server/issues/2598))

### Fixed
- JetStream:
    - Adjust cluster size on cold start in mixed mode ([#2579](https://github.com/nats-io/nats-server/issues/2579))
    - Issue with the "get message" audit event ([#2583](https://github.com/nats-io/nats-server/issues/2583))
    - Some data races ([#2590](https://github.com/nats-io/nats-server/issues/2590), [#2619](https://github.com/nats-io/nats-server/issues/2619))
    - More robust checking for decoding append entries ([#2606](https://github.com/nats-io/nats-server/issues/2606))
    - Issues with RAFT around snapshots and recovery ([#2615](https://github.com/nats-io/nats-server/issues/2615))
    - Starting a consumer with "deliver last per subject" with a filtered subject same as the stream's subject would possibly return wrong `NumPending` value ([#2616](https://github.com/nats-io/nats-server/issues/2616))
    - Purging filtered stream resets all stream consumer offsets. Thanks to [@minight](https://github.com/minight) for the report ([#2616](https://github.com/nats-io/nats-server/issues/2616), [#2617](https://github.com/nats-io/nats-server/issues/2617))
    - Honor max memory and max file settings of 0. Thanks to [@biskit](https://github.com/biskit) for the report ([#2618](https://github.com/nats-io/nats-server/issues/2618))
- Websocket:
    - Issue with compression and Safari browsers using the experimental feature `NSURLSession WebSocket`. Thanks to [@byazrail](https://github.com/byazrail) for the report ([#2613](https://github.com/nats-io/nats-server/issues/2613))
- Handling of message headers in some system requests ([#2580](https://github.com/nats-io/nats-server/issues/2580))
- Go doc fixes and some code cleanup. Thanks to [@dtest11](https://github.com/dtest11) for the contributions ([#2610](https://github.com/nats-io/nats-server/issues/2610), [#2614](https://github.com/nats-io/nats-server/issues/2614))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.1...v2.6.2


[Changes][v2.6.2]


<a id="v2.6.1"></a>
# [Release v2.6.1](https://github.com/nats-io/nats-server/releases/tag/v2.6.1) - 2021-09-24

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.8: Both release executables and Docker images are built with this Go release.

### Fixed
- JetStream:
    - Deadlock when using stream mirrors with source with non limit retention, which could manifest with increase memory usage ([#2561](https://github.com/nats-io/nats-server/issues/2561))
    - Possible panic due when getting a slice of a message block cache buffer ([#2565](https://github.com/nats-io/nats-server/issues/2565))
    - Internal "direct consumers" used for mirroring were incorrectly subject to the "max consumers" limit ([#2564](https://github.com/nats-io/nats-server/issues/2564))
    - Storage limit of 0 (which means disabled) was not always enforced ([#2563](https://github.com/nats-io/nats-server/issues/2563))
    - Handling when exceeding account resources ([#2569](https://github.com/nats-io/nats-server/issues/2569))
    - Internal connections were counted toward the account's max connections (client or leafnode) ([#2568](https://github.com/nats-io/nats-server/issues/2568))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.0...v2.6.1


[Changes][v2.6.1]


<a id="v2.6.0"></a>
# [Release v2.6.0](https://github.com/nats-io/nats-server/releases/tag/v2.6.0) - 2021-09-21

## Changelog


**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) if upgrading from a version prior to `v2.5.0`.


### Go Version
- 1.16.8: Both release executables and Docker images are built with this Go release.

### Added
- Monitoring:
    - JetStream's reserved memory and memory used from accounts with reservations in `/jsz` and `/varz` endpoints ([#2539](https://github.com/nats-io/nats-server/issues/2539), [#2555](https://github.com/nats-io/nats-server/issues/2555))
- Hardened `systemd` service. Thanks to [@c0deaddict](https://github.com/c0deaddict) for the contribution ([#2318](https://github.com/nats-io/nats-server/issues/2318)) 

### Changed
- JetStream:
    - The server store message headers with a uint 16, which limits to 64KB, however, it was not rejecting incoming messages with headers bigger than that but then was failing its recovery ([#2526](https://github.com/nats-io/nats-server/issues/2526))
    - The server was accepting consumer creation requests with flow control enabled but without heartbeats, which could lead to the consumer stalling if the library missed the flow control message. The server will now reject such consumer creation request ([#2533](https://github.com/nats-io/nats-server/issues/2533))
    - Trying to add a stream with a configuration that matched exactly the one of an existing stream would report an error in cluster mode, not in standalone mode. The cluster mode will now behave as standalone mode and treat the stream creation as idempotent ([#2535](https://github.com/nats-io/nats-server/issues/2535))
    - The server will now reject negative duplicate window values ([#2546](https://github.com/nats-io/nats-server/issues/2546))
- The server will now fail to start if a `token` is specified in `cluster{authorization{}}` or `gateway{authorization{}}`. Routes and Gateways have never supported `token` authentication, however, it was silently ignored. Thanks to [@antong](https://github.com/antong) for the report ([#2518](https://github.com/nats-io/nats-server/issues/2518))

### Fixed
- JetStream:
    - Filestore compaction issue that could to message lookup to fail, possibly stalling consumers ([#2522](https://github.com/nats-io/nats-server/issues/2522))
    - Reject messages with headers greater than 64KB ([#2526](https://github.com/nats-io/nats-server/issues/2526))
    - Condition where assets could be created in multiple leafnode domains ([#2529](https://github.com/nats-io/nats-server/issues/2529))
    - Reject consumer creation requests with a flow control but no heartbeat ([#2533](https://github.com/nats-io/nats-server/issues/2533))
    - Stream creation request was idempotent in standalone mode, but not in clustered mode ([#2535](https://github.com/nats-io/nats-server/issues/2535))
    - In clustered mode, getting consumer information or a just created consumer could sometimes return a "consumer not found" error ([#2537](https://github.com/nats-io/nats-server/issues/2537))
    - Make large pull subscribe batch requests expire more efficiently ([#2538](https://github.com/nats-io/nats-server/issues/2538))
    - Stabilize filestore implementation. Thanks to [@izarraga](https://github.com/izarraga) for the report ([#2545](https://github.com/nats-io/nats-server/issues/2545))
    - Reject duplicate window with negative value ([#2546](https://github.com/nats-io/nats-server/issues/2546))
    - Memory store memory leak when running in cluster mode. Thanks to [@anjmao](https://github.com/anjmao) for the report ([#2550](https://github.com/nats-io/nats-server/issues/2550))
    - Some issues with RAFT WAL repair ([#2549](https://github.com/nats-io/nats-server/issues/2549))
    - A consumer on mirror stream would fail to be recovered if its stream was recovered before the mirrored stream. Thanks to [@tbeets](https://github.com/tbeets) for the report ([#2554](https://github.com/nats-io/nats-server/issues/2554))
- Websocket:
    - Possible panic when decoding CLOSE frames. Thanks to [@byazrail](https://github.com/byazrail) for the report ([#2519](https://github.com/nats-io/nats-server/issues/2519))
- Memory leak when using Queue Subscriptions. Thanks to [@andrey-shalamov](https://github.com/andrey-shalamov) for the report and fix! ([#2517](https://github.com/nats-io/nats-server/issues/2517))
- Fail configuration that would have `token` specified in `cluster{}` or `gateway{}`'s `authorization{}` blocks. Authentication token were never supported there but the misconfiguration was silently ignored. Thanks to [@antong](https://github.com/antong) for the report ([#2518](https://github.com/nats-io/nats-server/issues/2518))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.6.0...v2.5.0



[Changes][v2.6.0]


<a id="v2.5.0"></a>
# [Release v2.5.0](https://github.com/nats-io/nats-server/releases/tag/v2.5.0) - 2021-09-09

## Changelog

**_Notice for JetStream Users_**

See important [note](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) if upgrading from a version prior to `v2.4.0`.

**_Notice for MQTT Users_**

See note in the `Improved` and `Changed` section.


### Go Version
- 1.16.8: Both release executables and Docker images are built with this Go release.

### Added
- MQTT/Monitoring:
    - `MQTTClient` in the `/connz` connections report and system events CONNECT and DISCONNECT. Ability to select on `mqtt_client`. Thanks to [@carr123](https://github.com/carr123) and [@imranrazakhan](https://github.com/imranrazakhan) for the suggestions ([#2507](https://github.com/nats-io/nats-server/issues/2507))

### Improved
- MQTT:
    - Sessions are now all stored inside a single stream, as opposed to individual streams, reducing resources usage ([#2501](https://github.com/nats-io/nats-server/issues/2501))

### Changed
- JetStream:
    - Using `Nats-Expected-Last-Subject-Sequence` header with a value of `0` now means that the server will reject the store command if there were messages on this subject ([#2506](https://github.com/nats-io/nats-server/issues/2506))
- MQTT: 
    - Due to improvement described above, when an MQTT client connects for the first time after an upgrade to this server version, the server will migrate all individual `$MQTT_sess_<xxxx>` streams to a new `$MQTT_sess` stream for the user's account ([#2501](https://github.com/nats-io/nats-server/issues/2501))

### Fixed
- JetStream:
    - Possible deadlock due to lock inversion ([#2479](https://github.com/nats-io/nats-server/issues/2479))
    - Possible consumer stall. Thanks to [@carr123](https://github.com/carr123) for the report ([#2480](https://github.com/nats-io/nats-server/issues/2480))
    - Don't send `408` status when pull request expires. This has no visible impact for users, but library implementers may want to be aware of it ([#2482](https://github.com/nats-io/nats-server/issues/2482))
    - During peer removal, try to remap any stream or consumer assets ([#2493](https://github.com/nats-io/nats-server/issues/2493))
    - Issues with remapping of stream and/or consumer assets during peer removal ([#2493](https://github.com/nats-io/nats-server/issues/2493))
    - Issue that could lead to perceived message loss ([#2490](https://github.com/nats-io/nats-server/issues/2490))
    - Message cleanup for interest stream and `AckNone` consumers in clustered mode ([#2499](https://github.com/nats-io/nats-server/issues/2499))
    - Suppress duplicates on JS deny all for system account ([#2502](https://github.com/nats-io/nats-server/issues/2502))
    - Consumers stopped receiving messages. Thanks to [@anjmao](https://github.com/anjmao), [@izarraga](https://github.com/izarraga) and [@tigrato](https://github.com/tigrato) for the report ([#2505](https://github.com/nats-io/nats-server/issues/2505))
- Handle `SIGTERM` on Windows platform ([#2481](https://github.com/nats-io/nats-server/issues/2481))
- Account resolver TLS connection may fail with `x509: certificate signed by unknown authority` errors. Thanks to [@Ryner51](https://github.com/Ryner51), [@ronaldslc](https://github.com/ronaldslc) for the report ([#2483](https://github.com/nats-io/nats-server/issues/2483))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.4.0...v2.5.0



[Changes][v2.5.0]


<a id="v2.4.0"></a>
# [Release v2.4.0](https://github.com/nats-io/nats-server/releases/tag/v2.4.0) - 2021-08-26

## Changelog

**_Notice for JetStream Users_**

With the latest release of the NATS server we have fixed bugs around queue subscriptions and have restricted undesired behavior that could be confusing or introduce data loss by unintended/undefined behavior of client applications.  If you are using queue subscriptions on a JetStream Push Consumer or have created multiple push subscriptions on the same consumer, you may be affected and need to upgrade your client version along with the server version.  We’ve detailed the behavior with different client versions below.

With a NATS Server **prior** to v2.4.0 and client libraries **prior** to these versions: NATS C client v3.1.0, Go client v1.12.0, Java client 2.12.0-SNAPSHOT, NATS.js v2.2.0, NATS.ws v1.3.0, NATS.deno v1.2.0, NATS .NET 0.14.0-pre2:

- It was possible to create multiple non-queue subscription instances for the same JetStream durable consumer. This is not correct since each instance will receive the same copy of a message and acknowledgment is therefore meaningless since the first instance to acknowledge the message will prevent other instances to control if/when a message should be acknowledged.
- Similar to the first issue, it was possible to create many different queue groups for one single JetStream consumer. 
- For queue subscriptions, if no consumer nor durable name was provided, the libraries would create ephemeral JetStream consumers, which meant that each member of the same group would receive the same message than the other members, which was not the expected behavior. Users assumed that 2 members subscribing to “foo” with the queue group named “bar” would load-balance the consumption of messages from the stream/consumer.
- It was possible to create a queue subscription on a JetStream consumer configured with heartbeat and/or flow control. This does not make sense because by definition, queue members would receive some (randomly distributed) messages, so the library would think that heartbeat are missed, and flow control would also be disrupted.

If above client libraries are not updated to the latest but the NATS server is upgraded to v2.4.0:

- It is still possible to create multiple non-queue subscription instances for the same JetStream durable consumer. Since the check is performed by the library (with the help of a new field called `PushBound` in the consumer information object set by the server), this mis-behavior is still possible.
- Queue subscriptions will not receive any message. This is because the server now has a new field `DeliverGroup` in the consumer configuration, which won’t be set for existing JetStream consumers and by the older libraries, and detects interest (and starts delivering) only when a subscription on the deliver subject for a queue subscription matching the “deliver group” name is found. Since the JetStream consumer is thought to be a non-deliver-group consumer, the opposite happens: the server detects an core NATS *queue* subscription on the “deliver subject”, therefore does not trigger delivery on the JetStream consumer’s “deliver subject”.

The 2 other issues are still present because those checks are done in the updated libraries.

If the above client libraries are update to the latest version, but the NATS Server is still to version prior to v2.4.0 (that is, up to v2.3.4):

- It is still possible to create multiple non-queue subscription instances for the same JetStream durable consumer. This is because the JetStream consumer’s information retrieved by the library will not have the `PushBound` boolean set by the server, therefore will not be able to alert the user that they are trying to create multiple subscription instances for the same JetStream consumer.
- Queue subscriptions will fail because the consumer information returned will not contain the `DeliverGroup` field. The error will be likely to the effect that the user tries to create a queue subscription to a non-queue JetStream consumer. Note that if the application creates a queue subscription for a non-yet created JetStream consumer, then this call will succeed, however, adding new members or restarting the application with the now existing JetStream consumer will fail.
- Creating queue subscriptions without a named consumer/durable will now result in the library using the queue name as the durable name.
- Trying to create a queue subscription with a consumer configuration that has heartbeat and/or flow control will now return an error message.

For completeness, using the latest client libraries and NATS Server v2.4.0:

- Trying to start multiple non-queue subscriptions instances for the same JetStream consumer will now return an error to the effect that the user is trying to create a “duplicate subscription”. That is, there is already an active subscription on that JetStream consumer.
It is now only possible to create a queue group for a JetStream consumer created for that group. The `DeliverGroup` field will be set by the library or need to be provided when creating the consumer externally.
- Trying to create a queue subscription without a durable nor consumer name results in the library creating/using the queue group as the JetStream consumer’s durable name.
- Trying to create a queue subscription with a consumer configuration that has heartbeat and/or flow control will now return an error message.

Note that if the server v2.4.0 recovers existing JetStream consumers that were created prior to v2.4.0 (and with older libraries), none of them will have a `DeliverGroup`, so none of them can be used for queue subscriptions. They will have to be recreated.


### Go Version
- 1.16.7: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Domain to the content of a `PubAck` protocol ([#2432](https://github.com/nats-io/nats-server/issues/2432), [#2434](https://github.com/nats-io/nats-server/issues/2434))
    - `PushBound` boolean in `ConsumerInfo` to indicate that a push consumer is already bound to an active subscription ([#2438](https://github.com/nats-io/nats-server/issues/2438))
    - `DeliverGroup` string in `ConsumerConfig` to specify which deliver group (or queue group name) the consumer is created for ([#2438](https://github.com/nats-io/nats-server/issues/2438))
    - Warning log statement in situations where catchup for a stream resulted in an error ([#2444](https://github.com/nats-io/nats-server/issues/2444))
- Monitoring:
    - Ability for normal accounts to access scoped `connz` information ([#2437](https://github.com/nats-io/nats-server/issues/2437))
- Operator option `resolver_pinned_accounts` to ensure user are signed by certain accounts ([#2461](https://github.com/nats-io/nats-server/issues/2461))

### Changed
- JetStream:
    - `ConsumerInfo`'s `Delivered` and `AckFloor` are now `SequenceInfo` instead of `SequencePair`. `SequenceInfo` contains `Last` which represents the last active time (in UTC) ([#2462](https://github.com/nats-io/nats-server/issues/2462))
    - Delivery of messages for consumers will now be subject to proper group information. Older clients may fail to receive messages on a queue subscription if the library has not been updated to specify the `DeliverGroup` ([#2438](https://github.com/nats-io/nats-server/issues/2438))
 
### Improved
- Make error message actionable when adding operator and leaf nodes. Thanks to [@alsuren](https://github.com/alsuren) for the contribution ([#2449](https://github.com/nats-io/nats-server/issues/2449))
- JetStream:
    - File utilization when using a Jetstream stream as a KeyValue store ([#2456](https://github.com/nats-io/nats-server/issues/2456))
    - Encryption at rest with message expiration on server restart ([#2467](https://github.com/nats-io/nats-server/issues/2467))
    - Enable global account on non JetStream servers in mixed mode ([#2473](https://github.com/nats-io/nats-server/issues/2473))

### Fixed
- JetStream:
    - Stream delete can fail for non empty directory ([#2418](https://github.com/nats-io/nats-server/issues/2418))
    - Possible panic for concurrent stream remove and consumer create ([#2419](https://github.com/nats-io/nats-server/issues/2419))
    - Simplified flow control and avoid stalls due to message loss ([#2425](https://github.com/nats-io/nats-server/issues/2425))
    - Consumer info `max_msgs_per_subject` defaults to 0, but should be -1 ([#2426](https://github.com/nats-io/nats-server/issues/2426))
    - Creating a consumer with a `max_waiting` value and a deliver subject was returning the wrong error message ([#2427](https://github.com/nats-io/nats-server/issues/2427))
    - Assign default to `max_ack_pending` when `AckExplicit` or `AckAll` ([#2428](https://github.com/nats-io/nats-server/issues/2428))
    - Subscriptions for internal clients for JetStream consumers were not properly removed, resulting in possible subscriptions leak/high memory usage ([#2439](https://github.com/nats-io/nats-server/issues/2439))
    - Expiration of messages during a server restart could lead to clients reporting errors after reconnect when trying to send new messages ([#2452](https://github.com/nats-io/nats-server/issues/2452))
    - Added additional checks for failures during filestore encryption ([#2453](https://github.com/nats-io/nats-server/issues/2453))
    - Processing of a publish ACK could cause a server panic ([#2460](https://github.com/nats-io/nats-server/issues/2460))
    - Consumer's number of pending messages for multiple matches and merging ([#2464](https://github.com/nats-io/nats-server/issues/2464))
    - Deadlock due to lock inversion when creating a RAFT group. Thanks to [@lokiwins](https://github.com/lokiwins) for the report ([#2471](https://github.com/nats-io/nats-server/issues/2471))
- Leafnode:
    - User authorization issue when JetStream is involved. Thanks to [@wchajl](https://github.com/wchajl) for the report ([#2430](https://github.com/nats-io/nats-server/issues/2430))
    - A remote websocket connection with `wss://` scheme but no `tls{}` configuration block would be attempted as non TLS connection, resulting on an "invalid websocket connection" in the log of the server attempting to create the remote connection ([#2442](https://github.com/nats-io/nats-server/issues/2442))
    - Wrong permission checks prevented messages flow. Thanks to [@rbboulton](https://github.com/rbboulton) for the report ([#2455](https://github.com/nats-io/nats-server/issues/2455), [#2470](https://github.com/nats-io/nats-server/issues/2470))
    - Daisy chained subject propagation issue. Thanks to [@KimonHoffmann](https://github.com/KimonHoffmann) for the report ([#2468](https://github.com/nats-io/nats-server/issues/2468))
- Possible subscription leak with the use of "AutoUnsubscribe" ([#2421](https://github.com/nats-io/nats-server/issues/2421))
- Prevent JWT claim updates from removing system imports ([#2450](https://github.com/nats-io/nats-server/issues/2450), [#2451](https://github.com/nats-io/nats-server/issues/2451))
- Error print when adding back existing system imports ([#2466](https://github.com/nats-io/nats-server/issues/2466))
- Build on OpenBSD-6.9. Thanks to [@miraculli](https://github.com/miraculli) for the contribution ([#2472](https://github.com/nats-io/nats-server/issues/2472))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.3.4...v2.4.0



[Changes][v2.4.0]


<a id="v2.3.4"></a>
# [Release v2.3.4](https://github.com/nats-io/nats-server/releases/tag/v2.3.4) - 2021-08-04

## Changelog

### Go Version
- 1.16.6: Both release executables and Docker images are built with this Go release.

### Changed:
- Server will now reject a `max_payload` that is set higher than `max_pending` since no message would be able to be delivered to subscriptions. The server will also warn if the value is set above 8MB (and may enforce this limit in the future) ([#2407](https://github.com/nats-io/nats-server/issues/2407), [#2413](https://github.com/nats-io/nats-server/issues/2413))

### Fixed
- JetStream:
    - A deadlock could happen when the server was removing messages. Thanks to [@rwrz](https://github.com/rwrz) for the report ([#2404](https://github.com/nats-io/nats-server/issues/2404))
    - Various fixes and improvements to clustered Filestore consumer stores ([#2406](https://github.com/nats-io/nats-server/issues/2406))
    - Leafnodes with same domain and shared system account should behave like flat Jetstream network ([#2410](https://github.com/nats-io/nats-server/issues/2410))
    - With stream with multi-subject or wildcard and `max_msgs_per_subject` set, if a consumer had `deliver_last_per_subject` set, the initial pending would be 1 higher than it actually was ([#2412](https://github.com/nats-io/nats-server/issues/2412))
    - Memory store would sometimes incorrectly select the proper starting sequence ([#2412](https://github.com/nats-io/nats-server/issues/2412))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.3.3...v2.3.4



[Changes][v2.3.4]


<a id="v2.3.3"></a>
# [Release v2.3.3](https://github.com/nats-io/nats-server/releases/tag/v2.3.3) - 2021-08-02

## Changelog


### Go Version
- 1.16.6: Both release executables and Docker images are built with this Go release.

### Added
- `ReloadOptions` API to support configuration reload without use of configuration file for embedded cases. Thanks to [@taigrr](https://github.com/taigrr) for the contribution ([#2341](https://github.com/nats-io/nats-server/issues/2341))
- `Kind` and `ClientType` to account CONNECT/DISCONNECT events. Thanks to [@mullerch](https://github.com/mullerch) for the report ([#2351](https://github.com/nats-io/nats-server/issues/2351))
- JetStream:
    - Streams and consumers now have a `Description` property ([#2377](https://github.com/nats-io/nats-server/issues/2377))
    - New `DeliverLastPerSubject` delivery policy ([#2381](https://github.com/nats-io/nats-server/issues/2381), [#2390](https://github.com/nats-io/nats-server/issues/2390))

### Changed
- Default account fetch timeout to be smaller than client timeout to increase probability of getting the `Authorization Violation` error instead of a `i/o timeout` error when credentials of an account has not yet been pushed to the account server ([#2365](https://github.com/nats-io/nats-server/issues/2365))
- Executable symbol table no longer stripped. Thanks to [@yzhao1012](https://github.com/yzhao1012) and [@justicezyx](https://github.com/justicezyx) for the contribution ([#2383](https://github.com/nats-io/nats-server/issues/2383))

### Improved
- TLS timeout in configuration file parsing now accept units, such as "2s" for 2 seconds ([#2364](https://github.com/nats-io/nats-server/issues/2364))
- JetStream:
    - Server restart time with many expired messages ([#2387](https://github.com/nats-io/nats-server/issues/2387))

### Fixed
- JetStream:
    - A data race on JetStream shutdown ([#2353](https://github.com/nats-io/nats-server/issues/2353))
    - In clustered mode, the maximum consumers limit was not always applied for ephemeral consumers ([#2354](https://github.com/nats-io/nats-server/issues/2354))
    - Consumer's `NumPending` may be stuck at 1 ([#2357](https://github.com/nats-io/nats-server/issues/2357))
    - Removed a stack print that may appear in some cases since v2.3.1 ([#2362](https://github.com/nats-io/nats-server/issues/2362))
    - Possible subscription leak when processing service imports and processing of pull subscribers ([#2373](https://github.com/nats-io/nats-server/issues/2373))
    - Unique server name requirement across domains ([#2378](https://github.com/nats-io/nats-server/issues/2378))
    - A clustered consumer on an interest retention policy could cause the server to panic when the consumer was being deleted ([#2382](https://github.com/nats-io/nats-server/issues/2382))
    - Allow non-JS leafnode(s) to access a HUB transparently ([#2393](https://github.com/nats-io/nats-server/issues/2393))
    - A stream with un-acknowledged messages would not redeliver new un-acknowledged messages following a purge. Thanks to [@sloveridge](https://github.com/sloveridge) for the report ([#2394](https://github.com/nats-io/nats-server/issues/2394))
- Subscription on a subject that is not a subset of a wildcard import. Thanks to [@DamianoChini](https://github.com/DamianoChini) for the report ([#2369](https://github.com/nats-io/nats-server/issues/2369))
- OCSP issue in embedded cases when the TLS configuration did not set the certificate Leaf ([#2376](https://github.com/nats-io/nats-server/issues/2376))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.3.2...v2.3.3



[Changes][v2.3.3]


<a id="v2.3.2"></a>
# [Release v2.3.2](https://github.com/nats-io/nats-server/releases/tag/v2.3.2) - 2021-07-06

## Changelog


### Go Version
- 1.16.5: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Error codes for consumers creation errors ([#2345](https://github.com/nats-io/nats-server/issues/2345))

### Changed
- JetStream:
    - Creating an ephemeral consumer if there is not yet interest will no longer fail, that is, a JetStream consumer can be created prior to low level NATS subscription on the delivery subject ([#2347](https://github.com/nats-io/nats-server/issues/2347))

### Fixed
- JetStream:
    - Updates to a multi-subject tacked stream ([#2334](https://github.com/nats-io/nats-server/issues/2334))
    - Possible publish timeout due to server sending messages to consumers on a slow connection ([#2337](https://github.com/nats-io/nats-server/issues/2337))
    - Possible message corruption ([#2344](https://github.com/nats-io/nats-server/issues/2344))
    - Peer info reports had a large last active values
- Headers handling in system services ([#2338](https://github.com/nats-io/nats-server/issues/2338), [#2348](https://github.com/nats-io/nats-server/issues/2348))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.3.1...v2.3.2



[Changes][v2.3.2]


<a id="v2.3.1"></a>
# [Release v2.3.1](https://github.com/nats-io/nats-server/releases/tag/v2.3.1) - 2021-06-29

## Changelog


### Go Version
- 1.16.5: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Ability to get a stream last message by subject ([#2313](https://github.com/nats-io/nats-server/issues/2313))
    - Ability to match based on last expected sequence per subject ([#2322](https://github.com/nats-io/nats-server/issues/2322))    

### Improved
- JetStream:
    - Large number of R1 consumers per stream ([#2324](https://github.com/nats-io/nats-server/issues/2324), [#2326](https://github.com/nats-io/nats-server/issues/2326))

### Fixed
- JetStream:
    - Max consumers was not enforced when set on a stream ([#2316](https://github.com/nats-io/nats-server/issues/2316))
    - Clustered streams can become broken with sequence mismatch state on low level store failures ([#2317](https://github.com/nats-io/nats-server/issues/2317))
    - Do not log at `[ERR]` level some "normal" store failures (such as max messages, bytes, etc..) ([#2321](https://github.com/nats-io/nats-server/issues/2321))
- Race when generated random service reply subject ([#2325](https://github.com/nats-io/nats-server/issues/2325))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.3.0...v2.3.1



[Changes][v2.3.1]


<a id="v2.3.0"></a>
# [Release v2.3.0](https://github.com/nats-io/nats-server/releases/tag/v2.3.0) - 2021-06-23

## Changelog


### Go Version
- 1.16.5: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - Richer API errors. JetStream errors now contain an `ErrCode` that uniquely describes the error. Thanks to [@jon-whit](https://github.com/jon-whit) and others for the suggestion ([#2168](https://github.com/nats-io/nats-server/issues/2168), [#2255](https://github.com/nats-io/nats-server/issues/2255), [#2266](https://github.com/nats-io/nats-server/issues/2266))
    - Ability to send more advanced Stream purge requests ([#2296](https://github.com/nats-io/nats-server/issues/2296), [#2297](https://github.com/nats-io/nats-server/issues/2297), [#2303](https://github.com/nats-io/nats-server/issues/2303), [#2306](https://github.com/nats-io/nats-server/issues/2306))
    - Stream can now be configured with a per-subject message limit ([#2284](https://github.com/nats-io/nats-server/issues/2284))
    - Encryption at rest ([#2302](https://github.com/nats-io/nats-server/issues/2302))
- Monitoring:
    - JetStream information into `statsz` ([#2269](https://github.com/nats-io/nats-server/issues/2269), [#2276](https://github.com/nats-io/nats-server/issues/2276))
- OCSP support ([#2240](https://github.com/nats-io/nats-server/issues/2240), [#2263](https://github.com/nats-io/nats-server/issues/2263), [#2277](https://github.com/nats-io/nats-server/issues/2277))

### Changed
- CPU and memory usage report on macOS (removed dependency on `ps`) ([#2260](https://github.com/nats-io/nats-server/issues/2260))
- Throttle the number of `maximum subscriptions exceeded` log statements per account, to 1 every 2 seconds ([#2304](https://github.com/nats-io/nats-server/issues/2304))

### Improved
- JetStream:
    - Setting initial pending and selecting starting sequence number of streams with multiple subjects ([#2284](https://github.com/nats-io/nats-server/issues/2284))
    - Filestore memory usage ([#2306](https://github.com/nats-io/nats-server/issues/2306))

### Fixed
- Gateways:
    - Handling of subject rewrites for subjects to a globally routed subject ([#2275](https://github.com/nats-io/nats-server/issues/2275))
    - Message headers were lost (passed in the message payload) from a response across a Gateway and through a route ([#2278](https://github.com/nats-io/nats-server/issues/2278))
- JetStream:
    - Better support for multiple domains where the hub is JetStream enabled but the hub account is not, and the Leafnode is ([#2261](https://github.com/nats-io/nats-server/issues/2261))
    - Orphaned consumers on sourced or mirrored streams keep trying to create new ones ([#2279](https://github.com/nats-io/nats-server/issues/2279))
    - CPU spikes in some catch-up situations ([#2280](https://github.com/nats-io/nats-server/issues/2280))
    - Dynamic account limits would be applied based on single server limits ([#2281](https://github.com/nats-io/nats-server/issues/2281))
    - Error description missing in some requests ([#2293](https://github.com/nats-io/nats-server/issues/2293), [#2294](https://github.com/nats-io/nats-server/issues/2294), [#2295](https://github.com/nats-io/nats-server/issues/2295))
- LeafNode:
    - Hanging connection when account can't be found ([#2267](https://github.com/nats-io/nats-server/issues/2267), [#2288](https://github.com/nats-io/nats-server/issues/2288))
    - Configuration reload could fail even if there were no changes to the Leafnode configuration ([#2274](https://github.com/nats-io/nats-server/issues/2274))
    - Service export interest was not propagated correctly ([#2288](https://github.com/nats-io/nats-server/issues/2288))
- MQTT:
    - Panic when subjects cross accounts with import/export. Thanks to [@mullerch](https://github.com/mullerch) for the report ([#2268](https://github.com/nats-io/nats-server/issues/2268))
- Websocket:
    - Compression/Decompression issues with continuation frames. Thanks to [@luan007](https://github.com/luan007) for the report ([#2300](https://github.com/nats-io/nats-server/issues/2300))
- Clients disconnected on configuration reload when only `$SYS` account is configured ([#2301](https://github.com/nats-io/nats-server/issues/2301))
- Failed route TLS handshake would leave the failed connection's lock in a locked state ([#2305](https://github.com/nats-io/nats-server/issues/2305))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.6...v2.3.0



[Changes][v2.3.0]


<a id="v2.2.6"></a>
# [Release v2.2.6](https://github.com/nats-io/nats-server/releases/tag/v2.2.6) - 2021-05-24

## Changelog


### Go Version
- 1.16.4: Both release executables and Docker images are built with this Go release.

### Changed
- `pinned_certs` is now also checked by servers initiating connections, not only when accepting them. Furthermore, connections whose certificate is no longer present in the `pinned_cert` list after a configuration reload, will be closed ([#2247](https://github.com/nats-io/nats-server/issues/2247))

### Fixed
- JetStream:    
    - Possible message corruption with inbound messages that would have an existing header ([#2241](https://github.com/nats-io/nats-server/issues/2241))
    - In cluster mode and with replicas greater than 1, after a valid "duplicate" error was returned (when using the `Nats-Msg-Id` header), the server would fail subsequent publish calls of non duplicate messages. It would take several attempts before the message would be accepted without error. Thanks to [@krisdaniels](https://github.com/krisdaniels) for the report ([#2245](https://github.com/nats-io/nats-server/issues/2245))
    - Messages would not be removed from the stream with `WorkQueuePolicy` and replicas greater than 1. Thanks to [@danpoland](https://github.com/danpoland) for the report ([#2246](https://github.com/nats-io/nats-server/issues/2246))
    - When using domains, cross domain transfers would stop working after updating the account JWT ([#2248](https://github.com/nats-io/nats-server/issues/2248))
    - The monitoring endpoint `/varz` was showing total account usage instead of server usage. Thanks to [@cjbottaro](https://github.com/cjbottaro) for the report ([#2249](https://github.com/nats-io/nats-server/issues/2249))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.5...v2.2.6



[Changes][v2.2.6]


<a id="v2.2.5"></a>
# [Release v2.2.5](https://github.com/nats-io/nats-server/releases/tag/v2.2.5) - 2021-05-20

## Changelog


### Go Version
- 1.16.4: Both release executables and Docker images are built with this Go release.

### Added
- `pinned_certs` configuration in TLS blocks, which contains "fingerprint" of accepted certificates. If a connection presents a certificate with a fingerprint that is not in this list, the connection will be rejected ([#2233](https://github.com/nats-io/nats-server/issues/2233))

### Fixed
- JetStream:    
    - Bad redelivered values on consumer state should not cause a panic ([#2223](https://github.com/nats-io/nats-server/issues/2223))
    - Restoring snapshots would require access to `/tmp` directory, which is not available in docker images ([#2227](https://github.com/nats-io/nats-server/issues/2227))
    - Ensure that removal of a peer is replicated ([#2231](https://github.com/nats-io/nats-server/issues/2231))
    - Reject an invalid API prefix for source or mirror is used ([#2237](https://github.com/nats-io/nats-server/issues/2237))
- MQTT:
    - Reduce replicas value when creating session streams if some servers in the cluster are not running ([#2226](https://github.com/nats-io/nats-server/issues/2226))
- Monitoring:
    - Always initialize httpReqStats, which allows users embedding NATS Server to use the NATS server http handlers in their own http server, without producing a panic. Thanks to [@BlizzTom](https://github.com/BlizzTom) for the contribution ([#2224](https://github.com/nats-io/nats-server/issues/2224))
- Under double import scenarios, the server could possibly map to the wrong subject ([#2225](https://github.com/nats-io/nats-server/issues/2225))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.4...v2.2.5



[Changes][v2.2.5]


<a id="v2.2.4"></a>
# [Release v2.2.4](https://github.com/nats-io/nats-server/releases/tag/v2.2.4) - 2021-05-13

## Changelog


### Go Version
- 1.16.4: Both release executables and Docker images are built with this Go release.

### Added
- JetStream:
    - The information about an upstream stream source will now have an optional External stream information, which will allow to know the API Prefix ([#2218](https://github.com/nats-io/nats-server/issues/2218))

### Changed
- `GetOpts()` from `ClientAuthentication` interface will now returned `*ClientOpts` (instead of `*clientOpts` which was internal) ([#2189](https://github.com/nats-io/nats-server/issues/2189)) 

### Fixed
- JetStream:    
    - Server was not checking for invalid de-duplication window specified in a stream mirror ([#2204](https://github.com/nats-io/nats-server/issues/2204)) 
    - A store directory on disk without `jetstream` could appear to lose assets on restart ([#2206](https://github.com/nats-io/nats-server/issues/2206), [#2216](https://github.com/nats-io/nats-server/issues/2216))
    - Source stream does not import from another stream if that stream name is not unique within the importing stream sources ([#2209](https://github.com/nats-io/nats-server/issues/2209))
    - Stream create (and others) responses do not return when the Leafnode is a cluster ([#2212](https://github.com/nats-io/nats-server/issues/2212))
    - Single instance shows direct consumers when it shouldn't ([#2214](https://github.com/nats-io/nats-server/issues/2214))
- Websocket:
    - Specifying `same_origin` or `allowed_origins` would prevent non web clients (that may not have the `Origin` header present) to connect, for instance Leafnodes. Thanks to [@wutkemtt](https://github.com/wutkemtt) for the report ([#2211](https://github.com/nats-io/nats-server/issues/2211))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.3...v2.2.4




[Changes][v2.2.4]


<a id="v2.2.3"></a>
# [Release v2.2.3](https://github.com/nats-io/nats-server/releases/tag/v2.2.3) - 2021-05-07

## Changelog


### Go Version
- 1.16.4: Both release executables and Docker images are built with this Go release.

### Security
- TLS default (secure) ciphers were not selected when configuring TLS from the command line as opposed to from the configuration file. Thanks to [@DavidSimner](https://github.com/DavidSimner) for the report. See [CVE-2021-32026](https://advisories.nats.io/CVE/CVE-2021-32026.txt) ([#2167](https://github.com/nats-io/nats-server/issues/2167))

### Added
- JetStream:
    - Support for multiple JetStream domains across Leafnodes. A new field called `domain` (a string) can be specified in the `jetstream{}` block ([#2171](https://github.com/nats-io/nats-server/issues/2171), [#2186](https://github.com/nats-io/nats-server/issues/2186), [#2190](https://github.com/nats-io/nats-server/issues/2190), [#2194](https://github.com/nats-io/nats-server/issues/2194))
- LeafNode:
    - `dont_randomize` configuration under a remote leaf configuration to restore original behavior that was no randomizing the list of URLs ([#2156](https://github.com/nats-io/nats-server/issues/2156))
- Monitoring:
    - LeafNodes deny exports and imports in `/varz` ([#2159](https://github.com/nats-io/nats-server/issues/2159))

### Changed
- Server is now trying to send data from the producer's network loop only when both producers and consumers are user connections. Thanks to [@shkim-will](https://github.com/shkim-will) for the contribution ([#2093](https://github.com/nats-io/nats-server/issues/2093))
- LeafNode:
    - Randomize remote URLs list by default. Thanks to [@RudeDude](https://github.com/RudeDude) for the suggestion ([#2156](https://github.com/nats-io/nats-server/issues/2156))
- MQTT:
    - In order to support use of MQTT in some more complex setups, the server must enforce that its `server_name` configuration be explicitly defined ([#2178](https://github.com/nats-io/nats-server/issues/2178))

### Improved
- JetStream: stability for concurrent compact, purge, expiration and persisting of messages ([#2180](https://github.com/nats-io/nats-server/issues/2180))

### Fixed
- Panic on startup when using a NATS Resolver without having configured a system account. The server will now report the error instead of panic'ing ([#2162](https://github.com/nats-io/nats-server/issues/2162))
- JetStream:
    - Pull based message delivery could drop responses in a super cluster configuration ([#2166](https://github.com/nats-io/nats-server/issues/2166))
    - Under heavy load, a leader change could warn about not processing entry responses ([#2173](https://github.com/nats-io/nats-server/issues/2173))
    - Stream bytes limit setting failed when account used dynamic limits. Also, file store implementation was not honoring block size ([#2183](https://github.com/nats-io/nats-server/issues/2183))
    - Mirror/Source streams from work queues which could cause a deadlock on Interest policy streams ([#2187](https://github.com/nats-io/nats-server/issues/2187))
    - Raft groups could continuously spin trying to catchup ([#2191](https://github.com/nats-io/nats-server/issues/2191))
    - Check for more unwanted characters for the stream/consumer names, namely ` `, `\r`, `\n`, `\t` and `\f` in addition to existing `.`, `*` and `>` ([#2195](https://github.com/nats-io/nats-server/issues/2195))
- LeafNode:
    - A message loop could occur if a Leafnode, which has several members of a queue group, reconnects to a different server in a remote cluster. Thanks to [@RudeDude](https://github.com/RudeDude) for the report ([#2163](https://github.com/nats-io/nats-server/issues/2163))
- Monitoring:
    - The http endpoint `/varz` would report increased subscriptions count every time it was inspected, even if no new subscription was added. Thanks to [@cjbottaro](https://github.com/cjbottaro) and [@harrisa1](https://github.com/harrisa1) for the report ([#2172](https://github.com/nats-io/nats-server/issues/2172))
- MQTT:
    - JetStream assets would not be placed in the local LeafNode cluster ([#2164](https://github.com/nats-io/nats-server/issues/2164))
    - A server would be forced to have JetStream enabled locally, which is not required if it is part of a cluster and JetStream is available in that cluster ([#2164](https://github.com/nats-io/nats-server/issues/2164), [#2178](https://github.com/nats-io/nats-server/issues/2178))
    - Several issues including connection timeouts, unexpected memory usage in QoS1 high publish message rate, etc... ([#2178](https://github.com/nats-io/nats-server/issues/2178))
    - Retained message in cluster mode may not be delivered to starting matching subscription ([#2179](https://github.com/nats-io/nats-server/issues/2179))
- The `User.Username` was not used when a custom authenticator was calling `RegisterUser` ([#2165](https://github.com/nats-io/nats-server/issues/2165))
- Error parsing operator JWT on Windows ([#2181](https://github.com/nats-io/nats-server/issues/2181))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.2...v2.2.3



[Changes][v2.2.3]


<a id="v2.2.2"></a>
# [Release v2.2.2](https://github.com/nats-io/nats-server/releases/tag/v2.2.2) - 2021-04-22

## Changelog


### Go Version
- 1.16.3: Both release executables and Docker images are built with this Go release.

### Added
- `Kind()` to the `ClientAuthentication` interface so that applications can know what type of connection they are dealing with ([#2084](https://github.com/nats-io/nats-server/issues/2084))

### Improved
- Some code cleanup. Thanks to [@alexpantyukhin](https://github.com/alexpantyukhin) for the contributions ([#2064](https://github.com/nats-io/nats-server/issues/2064), [#2065](https://github.com/nats-io/nats-server/issues/2065))
- JetStream:
    - Startup for filtered consumers on large streams ([#2075](https://github.com/nats-io/nats-server/issues/2075))
    - When running in mixed mode (some of clustered servers having JetStream enabled, some not) ([#2095](https://github.com/nats-io/nats-server/issues/2095))
    - Setup with a remote Leafnode cluster extending a cluster or super-cluster and the JetStream domain. The Leafnode will not be elected as a leader and placement will by default be in the Leafnode's cluster ([#2108](https://github.com/nats-io/nats-server/issues/2108))
- MQTT:
    - Error message when clients try to connect using Websocket protocol, which is currently not supported. Thanks to [@LLLLimbo](https://github.com/LLLLimbo) for the report ([#2151](https://github.com/nats-io/nats-server/issues/2151))

### Changed
- JetStream:
    - The StreamInfo response contained an array of sequences of deleted messages. It will now return the number of deleted messages and the request needs to set boolean `deleted_details` to `true` to get back the array of sequences of deleted messages ([#2109](https://github.com/nats-io/nats-server/issues/2109))

### Fixed
- JetStream:
    - Report the possible account loading failure when creating a stream ([#2076](https://github.com/nats-io/nats-server/issues/2076))
    - Possible panic when a mirror was removed or its configuration changes ([#2078](https://github.com/nats-io/nats-server/issues/2078))
    - Possible panic and file corruption during a file store compact ([#2080](https://github.com/nats-io/nats-server/issues/2080))
    - Stream expired messages were not removed from consumer pending ack list ([#2085](https://github.com/nats-io/nats-server/issues/2085))
    - Memory store should take length of message header into consideration to check for max bytes, similar to the file store implementation. Thanks to [@alexpantyukhin](https://github.com/alexpantyukhin) for the contribution ([#2086](https://github.com/nats-io/nats-server/issues/2086))
    - Issue with cached messages when server exits abruptly. Thanks to [@GuangchaoDeng](https://github.com/GuangchaoDeng) for the report ([#2099](https://github.com/nats-io/nats-server/issues/2099), [#2104](https://github.com/nats-io/nats-server/issues/2104))
    - Messages not properly removed from a stream with interest retention when a pull consumer was deleted. Thanks to [@GuangchaoDeng](https://github.com/GuangchaoDeng) for the report ([#2105](https://github.com/nats-io/nats-server/issues/2105))
    - Mirrors failed when upstream messages had expired ([#2110](https://github.com/nats-io/nats-server/issues/2110))
    - Make sure to stop unneeded retries for mirror consumers ([#2113](https://github.com/nats-io/nats-server/issues/2113))
    - Subscription leak on failure when creating source consumers ([#2118](https://github.com/nats-io/nats-server/issues/2118))
    - Files handles not closed on store close. Only impacting tests or applications embedding the server ([#2121](https://github.com/nats-io/nats-server/issues/2121))
    - Inability to add some nodes to the group if they were not known prior to the meta group leader being elected ([#2119](https://github.com/nats-io/nats-server/issues/2119))
    - General updates and stability improvements ([#2131](https://github.com/nats-io/nats-server/issues/2131))
    - Prevent possible stall when shutting down a high traffic server or stream ([#2146](https://github.com/nats-io/nats-server/issues/2146))
    - Errors deleting streams on Windows ([#2152](https://github.com/nats-io/nats-server/issues/2152))
- LeafNode:
    - Incorrect loop detection when cluster of leaf nodes reconnect to a server in another cluster ([#2066](https://github.com/nats-io/nats-server/issues/2066))
    - Subscriptions not properly removed during a route disconnect and information not properly forwarded to leaf nodes, resulting in possible unnecessary message flow ([#2066](https://github.com/nats-io/nats-server/issues/2066))
    - Possible failure for a solicited leaf node connection to authenticate in extremely rare timing conditions ([#2088](https://github.com/nats-io/nats-server/issues/2088))
    - Permission negotiation between two servers that could result in authorization failures causing connection to be closed ([#2091](https://github.com/nats-io/nats-server/issues/2091), [#2101](https://github.com/nats-io/nats-server/issues/2101))
    - Loss of subscription interest or closed connection could cause incorrect suppression of interest in a local cluster ([#2124](https://github.com/nats-io/nats-server/issues/2124))
    - Possible panic due to concurrent access of unlocked map when permissions are set on a leaf node ([#2136](https://github.com/nats-io/nats-server/issues/2136))
- Websocket:
    - TLS configuration changes were not reflected after a configuration reload ([#2072](https://github.com/nats-io/nats-server/issues/2072))
- Monitoring:
    - Ensure `/varz` subscriptions count is for all accounts ([#2074](https://github.com/nats-io/nats-server/issues/2074))
- Issue with concurrent fetching of an account that could result in message flow disruption ([#2067](https://github.com/nats-io/nats-server/issues/2067))
- On TERM signal, the server would exit with code `0`, while it should have been `1`  ([#2103](https://github.com/nats-io/nats-server/issues/2103))
- `GetTLSConnectionState()` was not using proper locking, resulting on some DATA RACE reports ([#2122](https://github.com/nats-io/nats-server/issues/2122))
- Do not propagate service import interest across gateways and routes ([#2123](https://github.com/nats-io/nats-server/issues/2123))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.1...v2.2.2



[Changes][v2.2.2]


<a id="v2.2.1"></a>
# [Release v2.2.1](https://github.com/nats-io/nats-server/releases/tag/v2.2.1) - 2021-04-02

## Changelog


### Go Version
- 1.16.3: Both release executables and Docker images are built with this Go release.

### Added
- Ability to set a timeout to NATS resolver ([#2057](https://github.com/nats-io/nats-server/issues/2057))

### Changed
- `JetStreamVarz` fields from structures to pointers so they can be omitted if empty. This is may affect users that embed the NATS Server in their code ([#2009](https://github.com/nats-io/nats-server/issues/2009))

### Improved
- Error log statement when an account registration fails ([#2016](https://github.com/nats-io/nats-server/issues/2016))
- JetStream:
    - Durable consumers performance with Replicas > 1 ([#2039](https://github.com/nats-io/nats-server/issues/2039), [#2049](https://github.com/nats-io/nats-server/issues/2049))
    - Report error when mirror/sources stream prefix overlaps/collides with stream subjects ([#2041](https://github.com/nats-io/nats-server/issues/2041))

### Fixed
- JetStream:
    - Possible panic when consumers are stopped ([#2008](https://github.com/nats-io/nats-server/issues/2008))
    - Panic on 32bit systems due to unaligned 64-bit atomic operations. Thanks to [@GuangchaoDeng](https://github.com/GuangchaoDeng) for the report ([#2012](https://github.com/nats-io/nats-server/issues/2012))
    - Check for filter subject correctness of an upstream stream's mirror or source ([#2013](https://github.com/nats-io/nats-server/issues/2013))
    - Reduce memory pressure and protect against some nil dereferences ([#2015](https://github.com/nats-io/nats-server/issues/2015))
    - Mirror streams were not properly retrying after failures to create their internal consumer ([#2017](https://github.com/nats-io/nats-server/issues/2017))
    - Prevent suppression of idle heartbeats for a filtered consumer on a busy stream ([#2018](https://github.com/nats-io/nats-server/issues/2018))
    - Some updates for direct consumers (used for mirrors and sources streams) ([#2021](https://github.com/nats-io/nats-server/issues/2021))
    - Possible double adds under reload or restart scenarios ([#2023](https://github.com/nats-io/nats-server/issues/2023))
    - In operator mode, JetStream accounts were not all loaded on startup ([#2024](https://github.com/nats-io/nats-server/issues/2024))
    - Consumer interest dropping and coming back across gateways ([#2024](https://github.com/nats-io/nats-server/issues/2024))
    - Leaked subscriptions when retrying to create streams' source consumers ([#2024](https://github.com/nats-io/nats-server/issues/2024))
    - Idle heartbeats were unnecessarily sent when a consumer was known to be active ([#2024](https://github.com/nats-io/nats-server/issues/2024))
    - Performance degradation for mirrors and sources in presence of gaps ([#2025](https://github.com/nats-io/nats-server/issues/2025))
    - Reworked sources and mirrors on missed data ([#2026](https://github.com/nats-io/nats-server/issues/2026))
    - Reduce sliding window for direct consumers and catchup stream windows ([#2027](https://github.com/nats-io/nats-server/issues/2027))
    - Flow control with multiple sources streams ([#2028](https://github.com/nats-io/nats-server/issues/2028))
    - Chaining of sources and mirrors with filtered consumers ([#2028](https://github.com/nats-io/nats-server/issues/2028))
    - General stability improvements ([#2033](https://github.com/nats-io/nats-server/issues/2033))
    - Possible deadlock ([#2034](https://github.com/nats-io/nats-server/issues/2034))
    - Panic when WAL was corrupted ([#2045](https://github.com/nats-io/nats-server/issues/2045))
    - Prevent bad stream updates from deleting the stream ([#2045](https://github.com/nats-io/nats-server/issues/2045))
    - When a request to get a message fails, returns code 404, instead of 500 ([#2053](https://github.com/nats-io/nats-server/issues/2053))
    - Possible deadlock caused by an account lookup failure when processing a consumer assignment ([#2054](https://github.com/nats-io/nats-server/issues/2054))
    - Consumer state (ack floor/pending or number of pending messages) could be skewed after server restarts ([#2058](https://github.com/nats-io/nats-server/issues/2058))
- LeafNode:
    - `verify_and_map` was not honored ([#2038](https://github.com/nats-io/nats-server/issues/2038))
    - When using Websocket connections, in some cases corruption could prevent messages to flow properly between nodes ([#2040](https://github.com/nats-io/nats-server/issues/2040))
    - Subscriptions leak for subscriptions when hitting the "auto-unsubscribe" limit ([#2059](https://github.com/nats-io/nats-server/issues/2059))
- MQTT:
    - Fix a possible subscription leak in setup failure conditions ([#2061](https://github.com/nats-io/nats-server/issues/2061))
- Websocket:
    - Possible empty frames sent to webbrowser clients ([#2040](https://github.com/nats-io/nats-server/issues/2040))
- Account connection events were not sent when using custom authentication ([#2020](https://github.com/nats-io/nats-server/issues/2020))
- Disconnect clients for account JWT that has been disabled ([#2048](https://github.com/nats-io/nats-server/issues/2048))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.2.0...v2.2.1


[Changes][v2.2.1]


<a id="v2.2.0"></a>
# [Release v2.2.0](https://github.com/nats-io/nats-server/releases/tag/v2.2.0) - 2021-03-15

## Changelog


### Go Version
- 1.16.2: Both release executables and Docker images are built with this Go release.

### Added
- JetStream, our new persistence offering (https://docs.nats.io/jetstream/jetstream)
- Websocket support (https://docs.nats.io/nats-server/configuration/websocket) ([#1309](https://github.com/nats-io/nats-server/issues/1309))
    - Websocket Leafnode connections ([#1858](https://github.com/nats-io/nats-server/issues/1858))
    - Cookie JWT authentication for Websocket. Thanks to #pas2k for the contribution ([#1477](https://github.com/nats-io/nats-server/issues/1477))
- MQTT Support (https://docs.nats.io/nats-server/configuration/mqtt) ([#1754](https://github.com/nats-io/nats-server/issues/1754))
    - Allow BearerToken as MQTT authentication method. Thanks to [@angiglesias](https://github.com/angiglesias) for the contribution ([#1840](https://github.com/nats-io/nats-server/issues/1840))
- Monitoring:
    - New Endpoint: `jsz` for JetStream ([#1881](https://github.com/nats-io/nats-server/issues/1881))
    - New Endpoint `/accountz` ([#1611](https://github.com/nats-io/nats-server/issues/1611))
    - Value of GOMAXPROCS in `/varz` endpoint ([#1304](https://github.com/nats-io/nats-server/issues/1304))
    - Ability to include subscription details in monitoring responses ([#1318](https://github.com/nats-io/nats-server/issues/1318))
    - Endpoints now available via system services ([#1362](https://github.com/nats-io/nats-server/issues/1362))
    - Base path for monitoring endpoints. Thanks to [@guilherme-santos](https://github.com/guilherme-santos) for the contribution ([#1392](https://github.com/nats-io/nats-server/issues/1392))
    - Filtering by account for `/leafz` and exposing this as per account subject ([#1612](https://github.com/nats-io/nats-server/issues/1612))
    - Support for tags and filter PING monitoring requests by tags ([#1832](https://github.com/nats-io/nats-server/issues/1832))
    - JWT/IssuerKey/NameTag/Tags to monitoring and event endpoints ([#1830](https://github.com/nats-io/nats-server/issues/1830))
    - `tls_required`, `tls_verify` and `tls_timeout` to Cluster/Gateway/Leafnode sections under `/varz` ([#1854](https://github.com/nats-io/nats-server/issues/1854))
    - Operator JWT to `/varz`  ([#1862](https://github.com/nats-io/nats-server/issues/1862))
    - `system_account` to `/varz` ([#1898](https://github.com/nats-io/nats-server/issues/1898))
- Options
    - `lame_duck_grace_period` ([#1460](https://github.com/nats-io/nats-server/issues/1460))
    - `sys_trace` or `--sys_trace` command line to trace the system account ([#1295](https://github.com/nats-io/nats-server/issues/1295))
    - `resolver_tls` to specify TLS configuration for account resolver. Thanks to [@JnMik](https://github.com/JnMik) for the report ([#1272](https://github.com/nats-io/nats-server/issues/1272))
    - `allowed_connection_types` to restrict which type connections (STANDARD, WEBSOCKET, etc..) can authenticate with a specific user ([#1594](https://github.com/nats-io/nats-server/issues/1594))
    - `verify_cert_and_check_known_urls` to tie subject ALT name to URL in configuration ([#1727](https://github.com/nats-io/nats-server/issues/1727))
    - `account_token_position` to simplify the securing of imports without requiring a token ([#1874](https://github.com/nats-io/nats-server/issues/1874))
- Support for JWT BearerToken ([#1226](https://github.com/nats-io/nats-server/issues/1226))
- Accounts default permissions ([#1398](https://github.com/nats-io/nats-server/issues/1398))
- Printing of the configuration file being used in the startup banner. Thanks to [@rmoriz](https://github.com/rmoriz) for the report ([#1473](https://github.com/nats-io/nats-server/issues/1473))
- Checks for CIDR blocks and connect time ranges specified in JWTs ([#1567](https://github.com/nats-io/nats-server/issues/1567))
- Support for route hostname resolution. Thanks to [@israellot](https://github.com/israellot) for the report ([#1590](https://github.com/nats-io/nats-server/issues/1590))
- Account name checks for Leafnodes in operator mode ([#1739](https://github.com/nats-io/nats-server/issues/1739))
- User JWT payload and subscriber limits ([#1570](https://github.com/nats-io/nats-server/issues/1570))
- Ability to use JWT latency sampling properties "headers" and "share" ([#1776](https://github.com/nats-io/nats-server/issues/1776))
- Support for wildcard services and import remapping by JWT ([#1790](https://github.com/nats-io/nats-server/issues/1790))
- Support for JWT export response threshold ([#1793](https://github.com/nats-io/nats-server/issues/1793))
- Enforcement and usage of scoped signing keys ([#1805](https://github.com/nats-io/nats-server/issues/1805))
- Support for StrictSigningKeyUsage ([#1845](https://github.com/nats-io/nats-server/issues/1845))
- Support for JWT based account mappings ([#1897](https://github.com/nats-io/nats-server/issues/1897))
- Build for mips64le platform. Thanks to [@duchuanLX](https://github.com/duchuanLX) for the contribution ([#1885](https://github.com/nats-io/nats-server/issues/1885))

### Changed
- `nats.io` resources from HTTP to HTTPS. Thanks to [@DavidSimner](https://github.com/DavidSimner) for the contribution ([#1596](https://github.com/nats-io/nats-server/issues/1596))
- Default TLS and Authentication timeouts, to 2 seconds and TLS timeout + 1 second respectively ([#1633](https://github.com/nats-io/nats-server/issues/1633))
- Gateways:
    - Connections now always send PINGs (the server otherwise will sometime suppress PINGs) ([#1692](https://github.com/nats-io/nats-server/issues/1692))
    - Log statements regarding Interest-only mode switch is now `DBG` instead of `INF` ([#2002](https://github.com/nats-io/nats-server/issues/2002))
- Enforce `max_control_line` for client connections only. The enforcement was previously happening only in case of handling of a partial protocol ([#1850](https://github.com/nats-io/nats-server/issues/1850))

### Improved
- Better support for distinguishedNameMatch in TLS authentication ([#1577](https://github.com/nats-io/nats-server/issues/1577))

### Updated
- Various dependencies, notably JWT and NKeys ([#2004](https://github.com/nats-io/nats-server/issues/2004))

### Fixed
- Log file size limit not honored after re-open signal ([#1438](https://github.com/nats-io/nats-server/issues/1438))
- Leafnode issues
    - Unsubscribe may not be propagated correctly ([#1455](https://github.com/nats-io/nats-server/issues/1455))
    - TLSMap authentication override ([#1470](https://github.com/nats-io/nats-server/issues/1470))
    - Solicit failure race could leave the connection registered ([#1475](https://github.com/nats-io/nats-server/issues/1475))
    - Loop detection may prevent early reconnect ([#1607](https://github.com/nats-io/nats-server/issues/1607))
    - Possible panic when server accepts TLS Leafnode connection ([#1652](https://github.com/nats-io/nats-server/issues/1652))
    - Duplicate queue messages in complex routing setup ([#1725](https://github.com/nats-io/nats-server/issues/1725))
    - Reject duplicate remote ([#1738](https://github.com/nats-io/nats-server/issues/1738))
    - Route parser error. Thanks to [@wuddl6](https://github.com/wuddl6) for the report ([#1745](https://github.com/nats-io/nats-server/issues/1745))
    - Configuration reload for remote TLS configurations ([#1771](https://github.com/nats-io/nats-server/issues/1771))
    - Connection issues if scheme was not `tls://` in some instances ([#1846](https://github.com/nats-io/nats-server/issues/1846))
- Gateway issues:
    - Implicit reconnection ([#1785](https://github.com/nats-io/nats-server/issues/1785))
    - Implicit connection not using global username/password. Thanks to [@DavidSimner](https://github.com/DavidSimner) for the report ([#1915](https://github.com/nats-io/nats-server/issues/1915))
    - System account incorrect tracking of gateways routed replies ([#1749](https://github.com/nats-io/nats-server/issues/1749))
    - Configuration reload for remote TLS configurations ([#1771](https://github.com/nats-io/nats-server/issues/1771))
- Connection name in log statement for some IPv6 addresses ([#1506](https://github.com/nats-io/nats-server/issues/1506))
- Handling of real duplicate subscriptions (same subscription ID sent by clients) ([#1507](https://github.com/nats-io/nats-server/issues/1507))
- Handling of gossiped URLs ([#1517](https://github.com/nats-io/nats-server/issues/1517))
- Queue subscriptions not able to receive system events ([#1530](https://github.com/nats-io/nats-server/issues/1530))
- JWT:
    - Revocation checks ([#1632](https://github.com/nats-io/nats-server/issues/1632), [#1645](https://github.com/nats-io/nats-server/issues/1645))
    - Validation of private imports (tokens) did return a warning instead of an error ([#2004](https://github.com/nats-io/nats-server/issues/2004))
- Detect service import cycles ([#1731](https://github.com/nats-io/nats-server/issues/1731))
- Syslog warning trace as a "INF" instead of "WRN". Thanks to [@paoloteti](https://github.com/paoloteti) for the contribution ([#1788](https://github.com/nats-io/nats-server/issues/1788))
- Monitoring endpoint `/connz` may report incorrect user. Thanks to [@nqd](https://github.com/nqd) for the report ([#1800](https://github.com/nats-io/nats-server/issues/1800))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.9...v2.2.0



[Changes][v2.2.0]


<a id="v2.1.9"></a>
# [Release v2.1.9](https://github.com/nats-io/nats-server/releases/tag/v2.1.9) - 2020-11-02

## Changelog


### Go Version
- 1.14.10: Both release executables and Docker images are built with this Go release.

### Fixed
- Possible panic if server receives a maliciously crafted JWT [CVE-2020-26521](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-26521) ([#1624](https://github.com/nats-io/nats-server/issues/1624))
- User and claims activation revocation checks [CVE-2020-26892](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-26892) ([#1632](https://github.com/nats-io/nats-server/issues/1632), [#1635](https://github.com/nats-io/nats-server/issues/1635), [#1645](https://github.com/nats-io/nats-server/issues/1645))
- Panic on shutdown while accepting TLS client connections ([`6900905b0a`](https://github.com/nats-io/nats-server/commit/6900905b0ac41b1d080db610438d4037c654aced))
- Added defensive code for handling of Leafnode connections ([`d99d0eb069`](https://github.com/nats-io/nats-server/commit/d99d0eb06983c3fc07617412019e03ecb1d68a38))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.8...v2.1.9



[Changes][v2.1.9]


<a id="v2.1.8"></a>
# [Release v2.1.8](https://github.com/nats-io/nats-server/releases/tag/v2.1.8) - 2020-09-03

## Changelog


### Go Version
- 1.14.8: Both release executables and Docker images are built with this Go release.

### Fixed
- Allow response permissions to work across accounts ([#1487](https://github.com/nats-io/nats-server/issues/1487))
- Race condition during implicit Gateway reconnection ([#1412](https://github.com/nats-io/nats-server/issues/1412))
- Possible stall on shutdown with leafnode setup. Thanks to [@HeavyHorst](https://github.com/HeavyHorst) for the report ([#1414](https://github.com/nats-io/nats-server/issues/1414))
- Possible removal of interest on queue subs with leaf nodes ([#1424](https://github.com/nats-io/nats-server/issues/1424))
- Unsubscribe may not be propagated through a leaf node ([#1455](https://github.com/nats-io/nats-server/issues/1455))
- LeafNode solicit failure race could leave conn registered ([#1475](https://github.com/nats-io/nats-server/issues/1475))
- Handling or real duplicate subscription ([#1507](https://github.com/nats-io/nats-server/issues/1507))
- Log file size limit not honored after re-open signal ([#1438](https://github.com/nats-io/nats-server/issues/1438))
- Connection name in log statement for some IPv6 addresses ([#1506](https://github.com/nats-io/nats-server/issues/1506))
- Better support for distinguishedNameMatch in TLS Auth. Thanks to [@nagukothapalli](https://github.com/nagukothapalli) for the report ([#1577](https://github.com/nats-io/nats-server/issues/1577))
- Error when importing an account results in an error ([#1578](https://github.com/nats-io/nats-server/issues/1578))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.7...v2.1.8



[Changes][v2.1.8]


<a id="v2.1.7"></a>
# [Release v2.1.7](https://github.com/nats-io/nats-server/releases/tag/v2.1.7) - 2020-05-14


## Changelog


### Go Version
- 1.13.10: Both release executables and Docker images are built with this Go release.

### Added
- Monitoring endpoints available via system services ([#1362](https://github.com/nats-io/nats-server/issues/1362))
- Configuration `no_auth_user` allows to refer to a configured user/account when no credentials are provided ([#1363](https://github.com/nats-io/nats-server/issues/1363))
- Support to match domainComponent (DC) in RDNSequence with TLS authentication ([#1386](https://github.com/nats-io/nats-server/issues/1386))
- Configuration `http_base_path` for monitoring endpoints. Thanks to [@guilherme-santos](https://github.com/guilherme-santos) for the contribution ([#1392](https://github.com/nats-io/nats-server/issues/1392))

### Improved
- Added close reason in the connection close statement ([#1348](https://github.com/nats-io/nats-server/issues/1348))

### Fixed
- Switch gateways to interest-only mode for Leafnode accounts ([#1327](https://github.com/nats-io/nats-server/issues/1327))
- Leafnode loop detection fixes ([#1331](https://github.com/nats-io/nats-server/issues/1331), [#1338](https://github.com/nats-io/nats-server/issues/1338))
- Service reply interest propagation in some Leafnode scenario ([#1334](https://github.com/nats-io/nats-server/issues/1334))
- Inconsistent subscription propagation behavior across accounts and Leafnodes ([#1335](https://github.com/nats-io/nats-server/issues/1335))
- Service across account and Leafnodes ([#1337](https://github.com/nats-io/nats-server/issues/1337))
- Service responses not delivered after Leafnode restart ([#1345](https://github.com/nats-io/nats-server/issues/1345))
- Update remote gateway URLs when node goes away in cluster ([#1352](https://github.com/nats-io/nats-server/issues/1352))
- Monitoring endpoint `/subsz` support for accounts ([#1377](https://github.com/nats-io/nats-server/issues/1377))
- Validate options on configuration reload ([#1381](https://github.com/nats-io/nats-server/issues/1381))
- Closed connection early in connect handshake may linger in the server (including monitoring `/connz`) ([#1385](https://github.com/nats-io/nats-server/issues/1385))
- Account unnecessarily reloaded in some cases during configuration reload ([#1387](https://github.com/nats-io/nats-server/issues/1387))
- `default_permissions` was not applied to NKey users ([#1391](https://github.com/nats-io/nats-server/issues/1391))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.6...v2.1.7

[Changes][v2.1.7]


<a id="v2.1.6"></a>
# [Release v2.1.6](https://github.com/nats-io/nats-server/releases/tag/v2.1.6) - 2020-03-31

## Changelog

### Go Version
- 1.13.9: Both release executables and Docker images are built with this Go release.

### Added
- Ability to specify TLS configuration for the account resolver. Thanks to [@JnMik](https://github.com/JnMik) for the report ([#1272](https://github.com/nats-io/nats-server/issues/1272)):
```
resolver_tls {
  cert_file: ...
  key_file: ...
  ca_file: ...
}
```
- Client IP (`client_ip`) in the server's INFO sent to clients. Client libraries may expose that in the future ([#1293](https://github.com/nats-io/nats-server/issues/1293))
- Option `trace_verbose` and command line parameters `-VV` and `-DVV` to increase trace and debug verbosity. By default system account messages will not be traced unless this option is enabled ([#1295](https://github.com/nats-io/nats-server/issues/1295))
- Value of `GOMAXPROCS` in `/varz` monitoring output ([#1304](https://github.com/nats-io/nats-server/issues/1304))
- Option to include subscription details in monitoring endpoints `/routez` and `/connz`. For instance `/connz?subs=detail` will now return not only the subjects of the subscription, but the queue name (if applicable) and some other details ([#1318](https://github.com/nats-io/nats-server/issues/1318))

### Improved
- Recover from panics during configuration parsing and instead issue errors ([#1274](https://github.com/nats-io/nats-server/issues/1274))
- Parse `ping_interval` as a duration. If not a duration, falls back to interpret as the number of seconds ([#1281](https://github.com/nats-io/nats-server/issues/1281))
- Error trace in case protocol exceeds the max control line value ([#1286](https://github.com/nats-io/nats-server/issues/1286))
- TLS version 1.3 and cipher names in log/monitoring. Thanks to [@burner-account](https://github.com/burner-account) for the report ([#1316](https://github.com/nats-io/nats-server/issues/1316))

### Updated
- Include port on the "Connected leafnode" `INF` notice in the server log ([#1303](https://github.com/nats-io/nats-server/issues/1303))
- Some dependencies. This covers the golang crypto package [CVE](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-9283). **Note that the CVE mentions the ssh package, which NATS Server does not use, so it should not be affected**. Thanks to [@KauzClay](https://github.com/KauzClay) for the contribution ([#1320](https://github.com/nats-io/nats-server/issues/1320))

### Fixed
- Server did not exit after receiving the lame duck mode signal. This is a regression introduced in v2.1.2 ([#1276](https://github.com/nats-io/nats-server/issues/1276))
- Use configured ping interval for clients that have not yet sent the `CONNECT` protocol. When the `CONNECT` protocol is received, a ping will be sent to the client in a short period of time to establish the initial `TTL` for this client ([#1289](https://github.com/nats-io/nats-server/issues/1289))
- A configuration producing a warning causes `-DV` to be ignored ([#1291](https://github.com/nats-io/nats-server/issues/1291))
- Incorrect buffer reuse in case of partial connection write ([#1298](https://github.com/nats-io/nats-server/issues/1298))
- Configuration reload of debug/trace option was not applied to existing clients ([#1300](https://github.com/nats-io/nats-server/issues/1300))
- Loop detection for LeafNodes ([#1308](https://github.com/nats-io/nats-server/issues/1308))
- Use account resolver URL from the operator JWT if one is specified. Note that if one is explicitly configured with the "resolver" option, it will take precedence ([#1318](https://github.com/nats-io/nats-server/issues/1318))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.4...v2.1.6


[Changes][v2.1.6]


<a id="v2.1.4"></a>
# [Release v2.1.4](https://github.com/nats-io/nats-server/releases/tag/v2.1.4) - 2020-01-30

## Changelog


### Go Version
- 1.13.7: Both release executables and Docker images are built with this Go release.

### Added
- `LogSizeLimit` option to automatically rotate logs. Thanks to [@xzzh999](https://github.com/xzzh999) for the report ([#1202](https://github.com/nats-io/nats-server/issues/1202))

### Updated
- Handling of slow consumer for non client connections ([#1233](https://github.com/nats-io/nats-server/issues/1233))

### Fixed
- Prevent "Account no-interest" for account that has service reply subscription ([#1204](https://github.com/nats-io/nats-server/issues/1204))
- Closing of Gateway or Route TLS connection may hang ([#1209](https://github.com/nats-io/nats-server/issues/1209))
- Messages to queue subscriptions are not distributed evenly. Thanks to [@harrisa1](https://github.com/harrisa1) for the report ([#1215](https://github.com/nats-io/nats-server/issues/1215))
- Allow multiple stream imports on the same subject ([#1220](https://github.com/nats-io/nats-server/issues/1220))
- Do not check URL account resolver reachability on configuration reload ([#1239](https://github.com/nats-io/nats-server/issues/1239))
- More than expected switch to Interest-Only mode for given account ([#1242](https://github.com/nats-io/nats-server/issues/1242))
- Possible panic when handling bad subjects ([#1249](https://github.com/nats-io/nats-server/issues/1249))
- Display of connections IPv6 addresses ([#1260](https://github.com/nats-io/nats-server/issues/1260))
- LeafNode TLS issues with mixed IP/Hostnames. Thanks to [@rbboulton](https://github.com/rbboulton) for the report ([#1261](https://github.com/nats-io/nats-server/issues/1261), [#1264](https://github.com/nats-io/nats-server/issues/1264))
- Fail and report if LeafNode attempt to connect to wrong listen port ([#1265](https://github.com/nats-io/nats-server/issues/1265))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.2...v2.1.4



[Changes][v2.1.4]


<a id="v2.1.2"></a>
# [Release v2.1.2](https://github.com/nats-io/nats-server/releases/tag/v2.1.2) - 2019-11-18


## Changelog


### Go Version
- 1.12.13: Both release executables and Docker images are built with this Go release.

### Added
- QueueSubscribe permissions ([#1143](https://github.com/nats-io/nats-server/issues/1143))
- Use of single/multiple users for authentication of Leafnodes ([#1147](https://github.com/nats-io/nats-server/issues/1147), [#1168](https://github.com/nats-io/nats-server/issues/1168))
- `~` support for Leafnode credentials ([#1148](https://github.com/nats-io/nats-server/issues/1148))
- Account support in `/connz` ([#1154](https://github.com/nats-io/nats-server/issues/1154))
- `server_name` configuration option to help better identify a server through `/varz` or system events ([#1158](https://github.com/nats-io/nats-server/issues/1158), [#1166](https://github.com/nats-io/nats-server/issues/1166))

### Updated
- In monitor home page, the help link now points to the monitoring page in our new documentation website ([#1169](https://github.com/nats-io/nats-server/issues/1169))
- Handling of replies (including service replies) across Gateways ([#1183](https://github.com/nats-io/nats-server/issues/1183), [#1184](https://github.com/nats-io/nats-server/issues/1184), [#1190](https://github.com/nats-io/nats-server/issues/1190), [#1195](https://github.com/nats-io/nats-server/issues/1195))
- Server performs actual shutdown procedure (closing client connections, etc..) when signaled to exit ([#1186](https://github.com/nats-io/nats-server/issues/1186))

### Fixed
- Reject duplicate service import "to" subject ([#1140](https://github.com/nats-io/nats-server/issues/1140))
- String trim in function getting the process name on Windows. Thanks to [@beautytiger](https://github.com/beautytiger) for the contribution ([#1157](https://github.com/nats-io/nats-server/issues/1157))
- Panic when incorrectly using a wildcard for a stream import prefix. Thanks to [@lucj](https://github.com/lucj) for the report ([#1160](https://github.com/nats-io/nats-server/issues/1160))
- Explicit gateway not using discovered URLs ([#1165](https://github.com/nats-io/nats-server/issues/1165))
- Leafnode loop detection ([#1170](https://github.com/nats-io/nats-server/issues/1170), [#1172](https://github.com/nats-io/nats-server/issues/1172))
- Prevent server from sending a PING to measure RTT until the client has finished the connect process ([#1175](https://github.com/nats-io/nats-server/issues/1175))
- Requestor RTT was often reported as 0 when tracking latency ([#1179](https://github.com/nats-io/nats-server/issues/1179))
- Leaking of service imports and subscriptions on routes ([#1185](https://github.com/nats-io/nats-server/issues/1185))
- Possible panic when processing route subscription interest ([#1189](https://github.com/nats-io/nats-server/issues/1189))
- Some account locking issues and race that could cause clients to not receive messages ([#1191](https://github.com/nats-io/nats-server/issues/1191))
- Server was fetching unknown account when tracking remote connections ([#1192](https://github.com/nats-io/nats-server/issues/1192))
- Handling of missing account when processing a remote latency update ([#1194](https://github.com/nats-io/nats-server/issues/1194))
- Ability to daisy chain Leafnode servers ([#1196](https://github.com/nats-io/nats-server/issues/1196))
- Handling of split buffers for Leafnodes. Thanks to Bfox for the report ([#1198](https://github.com/nats-io/nats-server/issues/1198), [#1199](https://github.com/nats-io/nats-server/issues/1199))

### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.1.0...v2.1.2


[Changes][v2.1.2]


<a id="v2.1.0"></a>
# [Release v2.1.0](https://github.com/nats-io/nats-server/releases/tag/v2.1.0) - 2019-09-20


## Changelog


### Go Version
- 1.12.9: Both release executables and Docker images are built with this Go release.

### Added
- RTT in `/routez` details ([#1101](https://github.com/nats-io/nats-server/issues/1101))
- New `/leafz` monitoring endpoint ([#1108](https://github.com/nats-io/nats-server/issues/1108))
- Latency tracking for exported services (BETA) ([#1111](https://github.com/nats-io/nats-server/issues/1111), [#1112](https://github.com/nats-io/nats-server/issues/1112), [#1122](https://github.com/nats-io/nats-server/issues/1122), [#1125](https://github.com/nats-io/nats-server/issues/1125), [#1130](https://github.com/nats-io/nats-server/issues/1130), [#1132](https://github.com/nats-io/nats-server/issues/1132), [#1136](https://github.com/nats-io/nats-server/issues/1136), [#1137](https://github.com/nats-io/nats-server/issues/1137))
- System level services for debugging (BETA). Exported services to the system account for debugging of blackbox systems. Ability to get the number fo subscribers for a given subject and optionally queue group ([#1127](https://github.com/nats-io/nats-server/issues/1127))

### Fixed
- Some typos in code. Thanks to [@beautytiger](https://github.com/beautytiger) for the contribution ([#1105](https://github.com/nats-io/nats-server/issues/1105))
- Some Leafnode issues ([#1106](https://github.com/nats-io/nats-server/issues/1106))
- Issue when there is a circular dependency in account server import ([#1119](https://github.com/nats-io/nats-server/issues/1119))
- MaxPending configured to more than 2GB. Thanks to [@cv711](https://github.com/cv711) for the report ([#1121](https://github.com/nats-io/nats-server/issues/1121))
- Some internal locking issues related to accounts lookup and updates ([#1126](https://github.com/nats-io/nats-server/issues/1126), [#1131](https://github.com/nats-io/nats-server/issues/1131))
- Ability to pass to the command line `-cluster nets://<host>:-1` for a random port, which is used in some NATS libraries for testing. This was broken due to changes in Golang ([#1128](https://github.com/nats-io/nats-server/issues/1128))
- Ensure server uses default if ResponsesPermissions's values are set to 0 ([#1135](https://github.com/nats-io/nats-server/issues/1135))

### Improved


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.0.4...v2.1.0


[Changes][v2.1.0]


<a id="v2.0.4"></a>
# [Release v2.0.4](https://github.com/nats-io/nats-server/releases/tag/v2.0.4) - 2019-08-15


## Changelog


### Go Version
- 1.12.8: Both release executables and Docker images are built with this Go release.

### Added
- Use of GoReleaser. Thanks to [@caarlos0](https://github.com/caarlos0) for this tool! ([#1095](https://github.com/nats-io/nats-server/issues/1095))
- Deb and RPM packages ([#1095](https://github.com/nats-io/nats-server/issues/1095))
- Publish permissions based on reply subjects of received messages ([#1081](https://github.com/nats-io/nats-server/issues/1081))
- Support for user and activation token revocation ([#1086](https://github.com/nats-io/nats-server/issues/1086))
- Leafnode connections to `/varz` ([#1088](https://github.com/nats-io/nats-server/issues/1088))
- Ability to cross account import services to return streams as well as singletons ([#1090](https://github.com/nats-io/nats-server/issues/1090))
- Support for service response types ([#1091](https://github.com/nats-io/nats-server/issues/1091), [#1093](https://github.com/nats-io/nats-server/issues/1093))

### Fixed
- Leafnode user JWT with signer fails to authenticate ([#1078](https://github.com/nats-io/nats-server/issues/1078))
- Leaked subscriptions from queue group across routes. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1079](https://github.com/nats-io/nats-server/issues/1079))
- Shadow subscriptions can be leaked on stream import and connection close ([#1090](https://github.com/nats-io/nats-server/issues/1090))
- Connection could be closed twice resulting in duplicate reconnect mainly affecting Gateways and Leafnodes ([#1092](https://github.com/nats-io/nats-server/issues/1092))
- Some typos in code. Thanks to [@ethan-daocloud](https://github.com/ethan-daocloud) for the contribution ([#1098](https://github.com/nats-io/nats-server/issues/1098))

### Improved
- Reduce memory usage on routes. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1087](https://github.com/nats-io/nats-server/issues/1087))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.0.2...v2.0.4


[Changes][v2.0.4]


<a id="v2.0.2"></a>
# [Release v2.0.2](https://github.com/nats-io/nats-server/releases/tag/v2.0.2) - 2019-07-15

## Changelog


### Go Version
- 1.11.12: Both release executables and Docker images are built with this Go release.


### Changed
- Default TLS Timeout bumped to 2 seconds (as opposed to 0.5s) ([#1042](https://github.com/nats-io/nats-server/issues/1042))

### Added 
- Support to extend leafnodes remote TLS timeout ([#1042](https://github.com/nats-io/nats-server/issues/1042))
- Allow operator to be inline JWT ([#1045](https://github.com/nats-io/nats-server/issues/1045))
- Made ReadOperatorJWT public for embedded use cases. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1052](https://github.com/nats-io/nats-server/issues/1052))
- Ability to disable sublist cache globally for all accounts. Thanks to [@azrle](https://github.com/azrle) for the report ([#1055](https://github.com/nats-io/nats-server/issues/1055))
- Ability to set a limit to the trace of the payload of a message. Thanks to [@andyxning](https://github.com/andyxning) for the contribution ([#1057](https://github.com/nats-io/nats-server/issues/1057))

### Improved
- Add default port (7422) for Leafnode remote connections ([#1049](https://github.com/nats-io/nats-server/issues/1049))
- Reduce server PINGs when data is flowing ([#1048](https://github.com/nats-io/nats-server/issues/1048))
- Allow remotes leafnode to specify an array of URLs. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1069](https://github.com/nats-io/nats-server/issues/1069))

### Fixed
- Messages not distributed evenly when sourced from leafnode ([#1040](https://github.com/nats-io/nats-server/issues/1040))
- Help link in top level monitoring ([#1043](https://github.com/nats-io/nats-server/issues/1043))
- Check of max payload could be bypassed if size overruns an int 32. Note that the client would first have to be authorized to connect. This fix is for CVE-2019-13126. Thanks to Aviv Sasson and Ariel Zelivansky from Twistlock for the security report ([#1053](https://github.com/nats-io/nats-server/issues/1053))
- Sending to client libraries an updated MaxPayload through INFO protocol when a bound account's MaxPayload is not the same as the server the client is connected to ([#1059](https://github.com/nats-io/nats-server/issues/1059))
- Routing of responses across leafnodes ([#1060](https://github.com/nats-io/nats-server/issues/1060))
- Subscriptions were not propagated correctly upon new leafnode joining the network. Thanks to [@antmanler](https://github.com/antmanler) for the report and fix! ([#1067](https://github.com/nats-io/nats-server/issues/1067))
- Prevent multiple solicited leafnodes from forming cycles. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1070](https://github.com/nats-io/nats-server/issues/1070))
- Report possible error starting the monitoring port. Thanks to [@andyxning](https://github.com/andyxning) for the contribution ([#1064](https://github.com/nats-io/nats-server/issues/1064))
- Allow use of `insecure` for remote leafnode and gateways again. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#1071](https://github.com/nats-io/nats-server/issues/1071), [#1073](https://github.com/nats-io/nats-server/issues/1073))
- Report authorization error and use TLS hostname for IPs on leafnodes ([#1072](https://github.com/nats-io/nats-server/issues/1072))
- Leafnode URLs may be missing in INFO protocol sent to Leafnodes connections ([#1074](https://github.com/nats-io/nats-server/issues/1074))
- Server now read pending data on closed connection to be able to report error (for instance in case of an authorization error sent by remote server) ([#1075](https://github.com/nats-io/nats-server/issues/1075))


### Complete Changes

https://github.com/nats-io/nats-server/compare/v2.0.0...v2.0.2



[Changes][v2.0.2]


<a id="v2.0.0"></a>
# [Release v2.0.0](https://github.com/nats-io/nats-server/releases/tag/v2.0.0) - 2019-06-05

## Changelog


### Go Version
- 1.11.10: Both release executables and Docker images are built with this Go release.

### Backward incompatibility
- The routing protocol has been dramatically improved and adds support for accounts and multi-tenancy. The new protocol is not backward compatible with servers <2.0.0.
- For users embedding NATS Server and using `Varz()` to get server statistics should be aware of some changes described in [#989](https://github.com/nats-io/nats-server/issues/989)

### Changed
- Repository and server name have changed: gnatsd becomes nats-server ([#985](https://github.com/nats-io/nats-server/issues/985))
- With go.mod, users embedding NATS Server should change their import path to include `/v2`. For instance:
```
import (

    natsd     "github.com/nats-io/nats-server/v2/server" 
)
```
- Cluster permissions moved out of cluster's authorization section ([#747](https://github.com/nats-io/nats-server/issues/747))
- The utility mkpasswd.go file was moved to its own directory `util/mkpasswd` to enable `go get` to install this tool ([#996](https://github.com/nats-io/nats-server/issues/996))

### Added 
- NKey support ([#743](https://github.com/nats-io/nats-server/issues/743))
- Accounts support ([#755](https://github.com/nats-io/nats-server/issues/755))
- JWT Support ([#804](https://github.com/nats-io/nats-server/issues/804))
- Gateways ([#808](https://github.com/nats-io/nats-server/issues/808))
- Leaf Nodes ([#928](https://github.com/nats-io/nats-server/issues/928))
- System events ([#823](https://github.com/nats-io/nats-server/issues/823))
- Support of TLS certificate subject for users authentication ([#896](https://github.com/nats-io/nats-server/issues/896), [#909](https://github.com/nats-io/nats-server/issues/909))
- Support of SANs in TLS certificate for user permissions. Thanks to [@twrobel3](https://github.com/twrobel3) for the report ([#966](https://github.com/nats-io/nats-server/issues/966))
- Ability to disable TLS server name verification for routes. Thanks to [@softkot](https://github.com/softkot) for the contribution ([#921](https://github.com/nats-io/nats-server/issues/921))
- Ability to explicitly set server name for TLS in Gateways. Thanks to [@danielsdeleo](https://github.com/danielsdeleo) for the contribution ([#922](https://github.com/nats-io/nats-server/issues/922))
- Configuration check with `-t` command line parameter ([#745](https://github.com/nats-io/nats-server/issues/745))
- Support for route permissions configuration reload ([#753](https://github.com/nats-io/nats-server/issues/753))
- Lame duck mode ([#780](https://github.com/nats-io/nats-server/issues/780))
- Support for path as argument to `--signal`. Thanks to [@pires](https://github.com/pires) for the contribution ([#838](https://github.com/nats-io/nats-server/issues/838))
- Expose connection remote address in `ClientAuthentication`. Thanks to [@ripienaar](https://github.com/ripienaar) for the contribution ([#837](https://github.com/nats-io/nats-server/issues/837))
- `ntp.service` dependency to the systemd service file. Thanks to [@andyxning](https://github.com/andyxning) for the contribution ([#880](https://github.com/nats-io/nats-server/issues/880))
- Configuration parameter to select the frequency at which failed route, gateways and leaf nodes connections are reported. Thanks to [@santo74](https://github.com/santo74) for the feedback ([#1000](https://github.com/nats-io/nats-server/issues/1000), [#1001](https://github.com/nats-io/nats-server/issues/1001))
- List or route URLs in `cluster{}` from `/varz` endpoint ([#1012](https://github.com/nats-io/nats-server/issues/1012))
- Ability to ignore top-level unknown configuration field ([#1024](https://github.com/nats-io/nats-server/issues/1024))

### Improved
- New route protocol ([#786](https://github.com/nats-io/nats-server/issues/786))
- Fan in/out scenarios ([#876](https://github.com/nats-io/nats-server/issues/876))
- Various optimizations ([#897](https://github.com/nats-io/nats-server/issues/897))
- Utility `mkpasswd`'s help output. Thanks to [@andyxning](https://github.com/andyxning) for the contribution ([#881](https://github.com/nats-io/nats-server/issues/881))
- You can now have unquoted strings that start with number ([#893](https://github.com/nats-io/nats-server/issues/893))
- Use of https for README's links. Thanks to [@huynq0911](https://github.com/huynq0911) for the contribution ([#914](https://github.com/nats-io/nats-server/issues/914))
- Warning on plaintext password in configuration and redact them from log statements ([#743](https://github.com/nats-io/nats-server/issues/743), [#776](https://github.com/nats-io/nats-server/issues/776))

### Fixed
- Misleading "Slow Consumer" error message during a TLS Handshake ([#836](https://github.com/nats-io/nats-server/issues/836))
- Report "Slow Consumer" only for clients that do complete the connect process ([#861](https://github.com/nats-io/nats-server/issues/861))
- Configuration reload of boolean flags. Thanks to [@sazo](https://github.com/sazo) for the report ([#879](https://github.com/nats-io/nats-server/issues/879))
- Runaway process when parsing a configuration file with missing a `}` or `)` ([#887](https://github.com/nats-io/nats-server/issues/887)) 
- Don't allow overruns for message payloads. Thanks to [@valichek](https://github.com/valichek) for the report ([#889](https://github.com/nats-io/nats-server/issues/889))
- Possible delays in delivering messages ([#895](https://github.com/nats-io/nats-server/issues/895))
- Possible slow consumer when routes exchange their subscriptions list ([#912](https://github.com/nats-io/nats-server/issues/912))
- Protocol Parser type safety. Thanks to [@nmiculinic](https://github.com/nmiculinic) for the contribution ([#908](https://github.com/nats-io/nats-server/issues/908))
- Use of custom authentication with configuration reload. Thanks to [@Will2817](https://github.com/Will2817) for the report ([#924](https://github.com/nats-io/nats-server/issues/924))
- Issue with utility `mkpasswd` on Windows platform. Thanks to [@Ryner51](https://github.com/Ryner51) for the report ([#935](https://github.com/nats-io/nats-server/issues/935))
- Some typos. Thanks to [@huynq0911](https://github.com/huynq0911), [@JensRantil](https://github.com/JensRantil) for their contributions.
- Changes to Varz content and fixed race conditions ([#989](https://github.com/nats-io/nats-server/issues/989))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.4.1...v2.0.0



[Changes][v2.0.0]


<a id="v1.4.1"></a>
# [Release v1.4.1](https://github.com/nats-io/nats-server/releases/tag/v1.4.1) - 2019-02-07

## Changelog


### Go Version
- 1.11.5: Both release executables and Docker images are built with this Go release.

### Fixed
- Possible delay in flushing data ([#901](https://github.com/nats-io/nats-server/issues/901))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.4.0...v1.4.1


[Changes][v1.4.1]


<a id="v1.4.0"></a>
# [Release v1.4.0](https://github.com/nats-io/nats-server/releases/tag/v1.4.0) - 2019-01-15

## Changelog


### Go Version
- 1.11.4: Both release executables and Docker images are built with this Go release.

### Added
- Warning if passwords are stored in plain text in config. Redact password from `CONNECT` trace.
- Support for cluster permissions reload ([#753](https://github.com/nats-io/nats-server/issues/753))
- Support a path as argument to `--signal`. Thanks to [@pires](https://github.com/pires) for the contribution ([#838](https://github.com/nats-io/nats-server/issues/838))
- `RemoteAddress()` to the `CustomClientAuthentication` interface. Thanks to [@ripienaar](https://github.com/ripienaar) for the contribution ([#837](https://github.com/nats-io/nats-server/issues/837))

### Changed
- Cluster permissions moved out of cluster's authorization ([#747](https://github.com/nats-io/nats-server/issues/747))

### Fixed
- Ports file on Windows ([#733](https://github.com/nats-io/nats-server/issues/733))
- Memory usage for failed TLS connections ([#872](https://github.com/nats-io/nats-server/issues/872))
- Issue with configuration reload for some boolean parameters, such as `logtime`. Thanks to [@sazo](https://github.com/sazo) for the report ([#874](https://github.com/nats-io/nats-server/issues/874), [#879](https://github.com/nats-io/nats-server/issues/879))
- Reduced risk of slow consumer in fan in scenarios ([#876](https://github.com/nats-io/nats-server/issues/876))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.3.0...v1.4.0


[Changes][v1.4.0]


<a id="v1.3.0"></a>
# [Release v1.3.0](https://github.com/nats-io/nats-server/releases/tag/v1.3.0) - 2018-08-30

## Changelog


### Go Version
- 1.11: Both release executables and Docker images are built with this Go release.

### Added
- Allow/Deny permissions ([#725](https://github.com/nats-io/nats-server/issues/725), [#727](https://github.com/nats-io/nats-server/issues/727)). It is now possible to specify deny permissions
  for subjects. For instance:
```
authorization {
    myUserPerms = {
      publish = {
        allow = "*.*"
        deny = ["SYS.*", "bar.baz", "foo.*"]
      }
      subscribe = {
        allow = "foo.*"
        deny = "foo.baz"
      }
    }

    users = [
        {user: myUser, password: pwd, permissions: $myUserPerms}
    ]
}

```
means that user `myUser` is allowed to publish to subjects with 2 tokens (`allow = "*.*"`) but not to subjects matching `SYS.*`, `bar.baz` or `foo.*`. It can subscribe to subjects matching `foo.*` but not `foo.baz`.
  

### Improved
- Scalability with high cardinality of subscriptions. Thanks to [@gwik](https://github.com/gwik) for the report ([#726](https://github.com/nats-io/nats-server/issues/726), [#729](https://github.com/nats-io/nats-server/issues/729))

### Fixed
- Unexpected `Authorization Error` during configuration reload ([#270](https://github.com/nats-io/nats-server/issues/270))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.2.0...v1.3.0


[Changes][v1.3.0]


<a id="v1.2.0"></a>
# [Release v1.2.0](https://github.com/nats-io/nats-server/releases/tag/v1.2.0) - 2018-07-05

## Changelog


### Go Version
- 1.10.3: Both release executables and Docker images are built with this Go release.

### Added
- License scan status. Thanks to [@xizhao](https://github.com/xizhao) ([#658](https://github.com/nats-io/nats-server/issues/658))
- PSE file for OpenBSD. Thanks to [@gabeguz](https://github.com/gabeguz) ([#661](https://github.com/nats-io/nats-server/issues/661))
- Best practices badge ([#679](https://github.com/nats-io/nats-server/issues/679))
- Governance and Maintainers files ([#703](https://github.com/nats-io/nats-server/issues/703))
- New `rtt` field in `/connz` monitoring endpoint. This measures the time between the server sent a PING to a client and the time it got the PONG back ([#683](https://github.com/nats-io/nats-server/issues/683))
- Ability to filter `/connz` by client ID (CID). Example: `http://localhost:8222/connz?cid=100`. Note that the CID is now sent back to the client. Client libraries have not yet been updated to report this to the application ([#687](https://github.com/nats-io/nats-server/issues/687))
- Tracking of closed connections and reason for their closing. To filter closed connections, use `?state=closed` as in `http://localhost:8222/connz?state=closed`. Possible values for `state` are `open`, `closed`, `all`. The result now includes a new field `reason` that gives a reason why the connection was closed, for instance `reason: Client` means that the client closed the connection ([#692](https://github.com/nats-io/nats-server/issues/692)). For example:
```
    {
      "cid": 2,
      "ip": "::1",
      "port": 63065,
      "start": "2018-07-05T11:00:16.09747933-06:00",
      "last_activity": "2018-07-05T11:00:21.441585272-06:00",
      "stop": "2018-07-05T11:00:23.445200428-06:00",
      "reason": "Slow Consumer (Write Deadline)",
      "uptime": "7s",
      "idle": "2s",
      "pending_bytes": 8265,
      "in_msgs": 0,
      "out_msgs": 4178,
      "in_bytes": 0,
      "out_bytes": 534784,
      "subscriptions": 1,
      "lang": "go",
      "version": "1.5.0"
    },
```
- New `/connz?sort=` sort options: `start`, `stop` and `reason` ([#705](https://github.com/nats-io/nats-server/issues/705))
- Support for "echo" feature. By default, if a connection subscribes on `foo` and publishes on `foo`, it will receive those messages. To prevent this, the client's `CONNECT` protocol includes a new boolean field named `echo` that should be set to `false`. The server is also sending a new `int` field `proto` in the `INFO` protocol to the client, allowing the client library to decide if it can use the "echo" feature or not. Client libraries have not yet been updated to take advantage of this feature  ([#698](https://github.com/nats-io/nats-server/issues/698))
- Ability to specify a maximum number of subscriptions (per connection) ([#707](https://github.com/nats-io/nats-server/issues/707))
- Ability to get details about a subscription with `/subsz?subs=1` ([#707](https://github.com/nats-io/nats-server/issues/707)). For instance: `http://localhost:8222/subsz?subs=1` may return:
```
"subscriptions_list": [
    {
      "subject": "foo",
      "sid": "1",
      "msgs": 1000,
      "cid": 2
    }
  ]
```
- Ability to test for matching subscriptions on a given subject ([#707](https://github.com/nats-io/nats-server/issues/707)). For instance: `http://localhost:8222/subsz?subs=1&test=foo` would return the `subscriptions_list` with subscriptions matching the test subject. If no match is found, the `subscriptions_list` is not present.

### Improved
- Authorization and Authentication documentation in README ([#673](https://github.com/nats-io/nats-server/issues/673), [#678](https://github.com/nats-io/nats-server/issues/678))
- Big performance improvement for fan-out situations (one message to many subscriptions). Special thanks to [@ripienaar](https://github.com/ripienaar) that has been helping test the solution at scale ([#680](https://github.com/nats-io/nats-server/issues/680)).

### Fixed
- Display of cluster port when configured as random ([#657](https://github.com/nats-io/nats-server/issues/657))
- Inability to remove a route from configuration (with configuration reload) if the remote server is not running ([#682](https://github.com/nats-io/nats-server/issues/682))
- Do not send more than one error when client sends invalid protocol. Thanks to [@danielwertheim](https://github.com/danielwertheim) for the report ([#684](https://github.com/nats-io/nats-server/issues/684))
- Possible truncation of the subscription interest list when route connects ([#680](https://github.com/nats-io/nats-server/issues/680))
- Route behavior in high fan-out (slow consumer) ([#680](https://github.com/nats-io/nats-server/issues/680))

### Updated
- Build requirements in the README (now require Go 1.9+ to build from source) and default cipher suites ([#689](https://github.com/nats-io/nats-server/issues/689))
- Elevate TLS statements from DBG to ERR. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#690](https://github.com/nats-io/nats-server/issues/690))


https://github.com/nats-io/gnatsd/compare/v1.1.0...v1.2.0


[Changes][v1.2.0]


<a id="v1.1.0"></a>
# [Release v1.1.0](https://github.com/nats-io/nats-server/releases/tag/v1.1.0) - 2018-03-23

## Changelog


### Go Version
- 1.9.4: Both release executables and Docker images are built with this Go release.

### Added
- Monitoring endpoint functions (Varz(), etc...) for those embedding NATS Server in their application. Thanks to [@ripienaar](https://github.com/ripienaar) for the report ([#615](https://github.com/nats-io/nats-server/issues/615))

### Improved
- Better attempt at delivering messages to queue subscriptions. Thanks to [@vkhorozvs](https://github.com/vkhorozvs) ([#638](https://github.com/nats-io/nats-server/issues/638))
- Compatibility with JSON in configuration parser ([#653](https://github.com/nats-io/nats-server/issues/653))

### Fixed
- Cluster topology change possibly not sent to clients ([#631](https://github.com/nats-io/nats-server/issues/631), [#634](https://github.com/nats-io/nats-server/issues/634))
- Race between delivering messages to queue subscriptions and subscriptions being unsubscribed ([#641](https://github.com/nats-io/nats-server/issues/641))
- Close log file on log re-open signal. Thanks to [@acadiant](https://github.com/acadiant) for the report ([#644](https://github.com/nats-io/nats-server/issues/644))

### Changed
- Moved to Apache 2.0 License following move to CNCF ([#650](https://github.com/nats-io/nats-server/issues/650))

### Removed
- `ssl_required` field in INFO protocol. This was deprecated some time ago. The correct field to use is TLSRequired ([#655](https://github.com/nats-io/nats-server/issues/655))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.0.6...v1.1.0



[Changes][v1.1.0]


<a id="v1.0.6"></a>
# [Release v1.0.6](https://github.com/nats-io/nats-server/releases/tag/v1.0.6) - 2018-02-28

## Changelog


### Go Version
- 1.9.4: Both release executables and Docker images are built with this Go release.


### Added
- ARM32v6 Release build ([#629](https://github.com/nats-io/nats-server/issues/629))
- Server ID to `/connz` and `/routez` ([#598](https://github.com/nats-io/nats-server/issues/598))
- Server notifies clients when server join/leave/rejoins cluster. Thanks to [@madgrenadier](https://github.com/madgrenadier) for feedback ([#606](https://github.com/nats-io/nats-server/issues/606), [#626](https://github.com/nats-io/nats-server/issues/626))
- Client and Cluster Advertise address ([#608](https://github.com/nats-io/nats-server/issues/608))
- GitCommit of release is printed in the banner ([#624](https://github.com/nats-io/nats-server/issues/624))


### Improved
- Various internal optimizations ([#580](https://github.com/nats-io/nats-server/issues/580), [#585](https://github.com/nats-io/nats-server/issues/585))


### Changed
- Log level for route errors were elevated from debug to error ([#611](https://github.com/nats-io/nats-server/issues/611))


### Fixed
- Profiling and Monitoring timeout issues ([#603](https://github.com/nats-io/nats-server/issues/603))
- `/connz` could be delayed for TLS clients still in TLS handshake ([#604](https://github.com/nats-io/nats-server/issues/604))


### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.0.4...v1.0.6

[Changes][v1.0.6]


<a id="v1.0.4"></a>
# [Release v1.0.4](https://github.com/nats-io/nats-server/releases/tag/v1.0.4) - 2018-02-20

## Changelog


### Go Version
- 1.8.3: Both release executables and Docker images are built with this Go release.

### Added
- ARM64v8 Release build ([#567](https://github.com/nats-io/nats-server/issues/567))
- Systemd unit file. Thanks to [@RedShift1](https://github.com/RedShift1) ([#587](https://github.com/nats-io/nats-server/issues/587))
- Custom Authentication. Thanks to [@cdevienne](https://github.com/cdevienne) ([#576](https://github.com/nats-io/nats-server/issues/576))

### Fixed
- Subscriptions not closed in the cluster when using auto unsubscribe. Thanks to [@ingosus](https://github.com/ingosus) ([#551](https://github.com/nats-io/nats-server/issues/551))
- Use of `*` and `>` in subjects as literals. Thanks to [@s921102002](https://github.com/s921102002) for the report ([#561](https://github.com/nats-io/nats-server/issues/561))
- Clarification about token usage. Thanks to [@sarbash](https://github.com/sarbash) ([#563](https://github.com/nats-io/nats-server/issues/563))
- Sublist insert and remove with wildcards in literals ([#573](https://github.com/nats-io/nats-server/issues/573))
- Override from command line not always working ([#578](https://github.com/nats-io/nats-server/issues/578))
- Name of selected new cipher suites ([#592](https://github.com/nats-io/nats-server/issues/592))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.0.2...v1.0.4



[Changes][v1.0.4]


<a id="v1.0.2"></a>
# [Release v1.0.2](https://github.com/nats-io/nats-server/releases/tag/v1.0.2) - 2017-07-19

## Changelog

### Go Version
- 1.7.6

### Fixed
- Windows Docker Images failure to start with error: “The service process could not connect to the service controller” ([#544](https://github.com/nats-io/nats-server/issues/544))
- Unnecessary attempt to reconnect a route. When server B connected to A and B was then shutdown, A would try to reconnect to B once, although the route was implicit ([#547](https://github.com/nats-io/nats-server/issues/547))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v1.0.0...v1.0.2


[Changes][v1.0.2]


<a id="v1.0.0"></a>
# [Release v1.0.0](https://github.com/nats-io/nats-server/releases/tag/v1.0.0) - 2017-07-12

## Changelog

Great community effort! Special thanks to Elton Stoneman ([@sixeyed](https://github.com/sixeyed)) for his help in verifying that the Windows Docker images build properly!

### Go Version
- 1.7.6

### Added
- Ability to configure number of connect retries for implicit routes. Thanks to [@joelanford](https://github.com/joelanford) for reporting an issue leading to this PR ([#409](https://github.com/nats-io/nats-server/issues/409))
- `GetTLSConnectionState()` in `ClientAuth` interface. This returns the TLS `ConnectionState` if TLS is enabled. Thanks to [@cdevienne](https://github.com/cdevienne) ([#385](https://github.com/nats-io/nats-server/issues/385))
- Windows Event logging ([#413](https://github.com/nats-io/nats-server/issues/413))
- `curve_preference` parameter to specify the preferred order ([#412](https://github.com/nats-io/nats-server/issues/412))
- ChaCha cipher ([#415](https://github.com/nats-io/nats-server/issues/415))
- `write_deadline` parameter to make the deadline when flushing messages to clients configurable ([#421](https://github.com/nats-io/nats-server/issues/421), [#488](https://github.com/nats-io/nats-server/issues/488))
- Reject clients connecting to route’s listen port ([#424](https://github.com/nats-io/nats-server/issues/424))
- Support for authentication token in configuration file. Thanks to [@qrpike](https://github.com/qrpike) for the report ([#465](https://github.com/nats-io/nats-server/issues/465))
- Ability to get the server’s HTTP Handler ([#481](https://github.com/nats-io/nats-server/issues/481))
- `MonitorAddr()` and `ClusterAddr()` return the monitoring an route listener respectively ([#512](https://github.com/nats-io/nats-server/issues/512))
- Configuration Reload ([#499](https://github.com/nats-io/nats-server/issues/499), [#503](https://github.com/nats-io/nats-server/issues/503), [#506](https://github.com/nats-io/nats-server/issues/506), [#515](https://github.com/nats-io/nats-server/issues/515), [#519](https://github.com/nats-io/nats-server/issues/519), [#523](https://github.com/nats-io/nats-server/issues/523), [#524](https://github.com/nats-io/nats-server/issues/524))

### Removed
- Global logger ([#501](https://github.com/nats-io/nats-server/issues/501))
- Auth package and `Server.SetClientAuthMethod()` and `Server.SetRouteAuthMethod()` ([#474](https://github.com/nats-io/nats-server/issues/474))

### Improved
- Parsing of subscription IDs for routes. Thanks to [@miraclesu](https://github.com/miraclesu) ([#478](https://github.com/nats-io/nats-server/issues/478))
- Handling of escaped substrings. Thanks to [@ericpromislow](https://github.com/ericpromislow) ([#482](https://github.com/nats-io/nats-server/issues/482))

### Fixed
- Possible data races with /varz and /subsz monitoring endpoints ([#445](https://github.com/nats-io/nats-server/issues/445))
- PINGs not sent to TLS Connections (clients or routes). Thanks to [@stefanschneider](https://github.com/stefanschneider) for the report ([#459](https://github.com/nats-io/nats-server/issues/459))
- Not all executables statically linked when release issued. Thanks to [@leifg](https://github.com/leifg) for the report ([#471](https://github.com/nats-io/nats-server/issues/471))
- Check for negative offset and/or limit when processing /connz monitoring endpoint ([#492](https://github.com/nats-io/nats-server/issues/492))
- Authorization timeout and TLS. Prevents errors such as `tls: oversized record received` ([#493](https://github.com/nats-io/nats-server/issues/493))
- Unexpected behavior when providing both HTTP and HTTPS monitoring ports ([#497](https://github.com/nats-io/nats-server/issues/497))
- HTTP Server not shutdown on server shutdown if HTTP server is started manually ([#498](https://github.com/nats-io/nats-server/issues/498))
- Possible DATA RACE when polling the routez endpoint and a route disconnects ([#540](https://github.com/nats-io/nats-server/issues/540))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v0.9.6...v1.0.0


[Changes][v1.0.0]


<a id="v0.9.6"></a>
# [Release v0.9.6](https://github.com/nats-io/nats-server/releases/tag/v0.9.6) - 2016-12-15

## Changelog

## Go Version
- 1.7.4

### Added
- Support for Go 1.7
- [Staticcheck](https://github.com/dominikh/go-staticcheck) verifications in Travis
- Ability to configure ping_interval and ping_max in configuration file ([#372](https://github.com/nats-io/nats-server/issues/372))
- Support for integer suffixes, e.g. 1k 8mb, etc.. ([#377](https://github.com/nats-io/nats-server/issues/377))
- Support for ‘include’ to configuration files ([#378](https://github.com/nats-io/nats-server/issues/378))
- Ability to make server re-open log file using SIGUSR1 - for log rotation ([#379](https://github.com/nats-io/nats-server/issues/379),[#382](https://github.com/nats-io/nats-server/issues/382))
- Support for `max_connections` parameter ([#387](https://github.com/nats-io/nats-server/issues/387), [#389](https://github.com/nats-io/nats-server/issues/389))

### Updated
- Docker build to use go 1.7.4
- Generate syslog tag based on executable/link name ([#362](https://github.com/nats-io/nats-server/issues/362))

### Fixed
- Client certificate was required but not verified even when configuration parameter `verify` was set to true ([#337](https://github.com/nats-io/nats-server/issues/337))
- Non-ASCII quotes in monitoring’s HTML output ([#353](https://github.com/nats-io/nats-server/issues/353))
- It was possible to configure `max_connections` in previous releases, but it had no effect ([#387](https://github.com/nats-io/nats-server/issues/387), [#389](https://github.com/nats-io/nats-server/issues/389))
- Non flags in command line parameters would cause the server to ignore remaining flags but not report an error ([#393](https://github.com/nats-io/nats-server/issues/393))

### Removed
- `MaxPending` option and `max_pending_size`, `max_pending` configuration parameters. Those were never used ([#383](https://github.com/nats-io/nats-server/issues/383))

### Complete Changes

https://github.com/nats-io/gnatsd/compare/v0.9.4...v0.9.6


[Changes][v0.9.6]


<a id="v0.9.4"></a>
# [Release v0.9.4](https://github.com/nats-io/nats-server/releases/tag/v0.9.4) - 2016-08-18

## Changelog

### Go Version
- 1.6.3

### Added
- Option to disable advertisement of cluster IP addresses to clients.

### Changed
- HTTP host, if not specified, now defaults to client’s host (`-a` command line or `Options.Host`)

### Fixed
- When server listens to all interfaces (0.0.0.0 or ::), it now advertises only global IPs instead of all resolved IPs (which included link local IPs).
- You can now specify an IPv6 address int the `-cluster` URL.
- Server could panic when user would poll Varz from monitoring port concurrently with other endpoints (Varz or others). 
- Data race with unsubscribe and connection close

### Complete changes

https://github.com/nats-io/gnatsd/compare/v0.9.2...v0.9.4


[Changes][v0.9.4]


<a id="v0.9.2"></a>
# [Release v0.9.2](https://github.com/nats-io/nats-server/releases/tag/v0.9.2) - 2016-08-08

## Changelog

### Go Version
- 1.6.3

### Added
- Multi-user and authorization
- Cluster discovery pushed to clients

### Updated
- Dockerfile to use go 1.6.3
- Clustering section in README

### Improved
- Route performance for larger messages

### Fixed
- Route/Cluster command line parameters/Options override
- Protocol parser to prevent memory attacks

### Complete changes

https://github.com/nats-io/gnatsd/compare/v0.8.1...v0.9.2


[Changes][v0.9.2]


<a id="v0.8.1"></a>
# [Release v0.8.1](https://github.com/nats-io/nats-server/releases/tag/v0.8.1) - 2016-06-09

## Changelog

### Go Version
- 1.6.2

Bug fix release.


[Changes][v0.8.1]


<a id="v0.8.0"></a>
# [Release v0.8.0](https://github.com/nats-io/nats-server/releases/tag/v0.8.0) - 2016-05-09

## Changelog

### Go Version
- 1.6.2

### Changes
- [Cluster Auto-Discovery](https://github.com/nats-io/gnatsd/blob/master/README.md#clustering) via seed hosts. 
- [Monitoring](https://github.com/nats-io/gnatsd/blob/master/README.md#monitoring) updates for **varz** and **connz**.
- Performance improvements in publish, subscribe and queue subscriptions.
- New automated release process.
- Vendor support and upgrade to Go 1.6.2 for supported release builds.
- Official support for Windows.
- Removed original hash and hashmap code, new 3 layer sublist cache design.
- Various bug fixes.
- Complete details - https://github.com/nats-io/gnatsd/compare/v0.7.2...v0.8.0


[Changes][v0.8.0]


<a id="v0.7.2"></a>
# [Release v0.7.2](https://github.com/nats-io/nats-server/releases/tag/v0.7.2) - 2015-12-09



[Changes][v0.7.2]


<a id="v0.7.0"></a>
# [Release v0.7.0](https://github.com/nats-io/nats-server/releases/tag/v0.7.0) - 2015-12-04



[Changes][v0.7.0]


<a id="v0.6.8"></a>
# [Release v0.6.8](https://github.com/nats-io/nats-server/releases/tag/v0.6.8) - 2015-09-18



[Changes][v0.6.8]


<a id="v0.6.6"></a>
# [Release v0.6.6](https://github.com/nats-io/nats-server/releases/tag/v0.6.6) - 2015-08-23

I recut this release with Go1.4.2 since there are issues with Go1.5.


[Changes][v0.6.6]


<a id="v0.6.4"></a>
# [Release v0.6.4](https://github.com/nats-io/nats-server/releases/tag/v0.6.4) - 2015-08-08



[Changes][v0.6.4]


<a id="v0.6.2"></a>
# [Release v0.6.2](https://github.com/nats-io/nats-server/releases/tag/v0.6.2) - 2015-08-08



[Changes][v0.6.2]


<a id="v0.6.0"></a>
# [Release v0.6.0](https://github.com/nats-io/nats-server/releases/tag/v0.6.0) - 2015-06-16



[Changes][v0.6.0]


<a id="v0.5.6"></a>
# [Release v0.5.6](https://github.com/nats-io/nats-server/releases/tag/v0.5.6) - 2014-09-10



[Changes][v0.5.6]


<a id="v0.5.4"></a>
# [Release v0.5.4](https://github.com/nats-io/nats-server/releases/tag/v0.5.4) - 2014-07-26



[Changes][v0.5.4]


<a id="v0.5.2"></a>
# [Release v0.5.2](https://github.com/nats-io/nats-server/releases/tag/v0.5.2) - 2014-06-17



[Changes][v0.5.2]


<a id="v0.5.1"></a>
# [Release v0.5.1](https://github.com/nats-io/nats-server/releases/tag/v0.5.1) - 2014-06-17



[Changes][v0.5.1]


[v2.10.24]: https://github.com/nats-io/nats-server/compare/v2.10.23...v2.10.24
[v2.10.23]: https://github.com/nats-io/nats-server/compare/v2.10.22...v2.10.23
[v2.10.22]: https://github.com/nats-io/nats-server/compare/v2.10.21...v2.10.22
[v2.10.21]: https://github.com/nats-io/nats-server/compare/v2.10.20...v2.10.21
[v2.10.20]: https://github.com/nats-io/nats-server/compare/v2.10.19...v2.10.20
[v2.10.19]: https://github.com/nats-io/nats-server/compare/v2.10.18...v2.10.19
[v2.10.18]: https://github.com/nats-io/nats-server/compare/v2.10.17...v2.10.18
[v2.10.17]: https://github.com/nats-io/nats-server/compare/v2.10.16...v2.10.17
[v2.10.16]: https://github.com/nats-io/nats-server/compare/v2.10.14...v2.10.16
[v2.10.14]: https://github.com/nats-io/nats-server/compare/v2.10.12...v2.10.14
[v2.10.12]: https://github.com/nats-io/nats-server/compare/v2.9.25...v2.10.12
[v2.9.25]: https://github.com/nats-io/nats-server/compare/v2.10.11...v2.9.25
[v2.10.11]: https://github.com/nats-io/nats-server/compare/v2.10.10...v2.10.11
[v2.10.10]: https://github.com/nats-io/nats-server/compare/v2.10.9...v2.10.10
[v2.10.9]: https://github.com/nats-io/nats-server/compare/v2.10.8...v2.10.9
[v2.10.8]: https://github.com/nats-io/nats-server/compare/v2.10.7...v2.10.8
[v2.10.7]: https://github.com/nats-io/nats-server/compare/v2.10.6...v2.10.7
[v2.10.6]: https://github.com/nats-io/nats-server/compare/v2.10.5...v2.10.6
[v2.10.5]: https://github.com/nats-io/nats-server/compare/v2.9.24...v2.10.5
[v2.9.24]: https://github.com/nats-io/nats-server/compare/v2.10.4...v2.9.24
[v2.10.4]: https://github.com/nats-io/nats-server/compare/v2.10.3...v2.10.4
[v2.10.3]: https://github.com/nats-io/nats-server/compare/v2.9.23...v2.10.3
[v2.9.23]: https://github.com/nats-io/nats-server/compare/v2.10.2...v2.9.23
[v2.10.2]: https://github.com/nats-io/nats-server/compare/v2.10.1...v2.10.2
[v2.10.1]: https://github.com/nats-io/nats-server/compare/v2.10.0...v2.10.1
[v2.10.0]: https://github.com/nats-io/nats-server/compare/v2.9.22...v2.10.0
[v2.9.22]: https://github.com/nats-io/nats-server/compare/v2.9.21...v2.9.22
[v2.9.21]: https://github.com/nats-io/nats-server/compare/v2.9.20...v2.9.21
[v2.9.20]: https://github.com/nats-io/nats-server/compare/v2.9.19...v2.9.20
[v2.9.19]: https://github.com/nats-io/nats-server/compare/v2.9.18...v2.9.19
[v2.9.18]: https://github.com/nats-io/nats-server/compare/v2.9.17...v2.9.18
[v2.9.17]: https://github.com/nats-io/nats-server/compare/v2.9.16...v2.9.17
[v2.9.16]: https://github.com/nats-io/nats-server/compare/v2.9.15...v2.9.16
[v2.9.15]: https://github.com/nats-io/nats-server/compare/v2.9.14...v2.9.15
[v2.9.14]: https://github.com/nats-io/nats-server/compare/v2.9.12...v2.9.14
[v2.9.12]: https://github.com/nats-io/nats-server/compare/v2.9.11...v2.9.12
[v2.9.11]: https://github.com/nats-io/nats-server/compare/v2.9.10...v2.9.11
[v2.9.10]: https://github.com/nats-io/nats-server/compare/v2.9.9...v2.9.10
[v2.9.9]: https://github.com/nats-io/nats-server/compare/v2.9.8...v2.9.9
[v2.9.8]: https://github.com/nats-io/nats-server/compare/v2.9.7...v2.9.8
[v2.9.7]: https://github.com/nats-io/nats-server/compare/v2.9.6...v2.9.7
[v2.9.6]: https://github.com/nats-io/nats-server/compare/v2.9.5...v2.9.6
[v2.9.5]: https://github.com/nats-io/nats-server/compare/v2.9.4...v2.9.5
[v2.9.4]: https://github.com/nats-io/nats-server/compare/v2.9.3...v2.9.4
[v2.9.3]: https://github.com/nats-io/nats-server/compare/v2.9.2...v2.9.3
[v2.9.2]: https://github.com/nats-io/nats-server/compare/v2.9.1...v2.9.2
[v2.9.1]: https://github.com/nats-io/nats-server/compare/v2.9.0...v2.9.1
[v2.9.0]: https://github.com/nats-io/nats-server/compare/v2.8.4...v2.9.0
[v2.8.4]: https://github.com/nats-io/nats-server/compare/v2.8.3...v2.8.4
[v2.8.3]: https://github.com/nats-io/nats-server/compare/v2.8.2...v2.8.3
[v2.8.2]: https://github.com/nats-io/nats-server/compare/v2.8.1...v2.8.2
[v2.8.1]: https://github.com/nats-io/nats-server/compare/v2.8.0...v2.8.1
[v2.8.0]: https://github.com/nats-io/nats-server/compare/v2.7.4...v2.8.0
[v2.7.4]: https://github.com/nats-io/nats-server/compare/v2.7.3...v2.7.4
[v2.7.3]: https://github.com/nats-io/nats-server/compare/v2.7.2...v2.7.3
[v2.7.2]: https://github.com/nats-io/nats-server/compare/v2.7.1...v2.7.2
[v2.7.1]: https://github.com/nats-io/nats-server/compare/v2.7.0...v2.7.1
[v2.7.0]: https://github.com/nats-io/nats-server/compare/v2.6.6...v2.7.0
[v2.6.6]: https://github.com/nats-io/nats-server/compare/v2.6.5...v2.6.6
[v2.6.5]: https://github.com/nats-io/nats-server/compare/v2.6.4...v2.6.5
[v2.6.4]: https://github.com/nats-io/nats-server/compare/v2.6.3...v2.6.4
[v2.6.3]: https://github.com/nats-io/nats-server/compare/v2.6.2...v2.6.3
[v2.6.2]: https://github.com/nats-io/nats-server/compare/v2.6.1...v2.6.2
[v2.6.1]: https://github.com/nats-io/nats-server/compare/v2.6.0...v2.6.1
[v2.6.0]: https://github.com/nats-io/nats-server/compare/v2.5.0...v2.6.0
[v2.5.0]: https://github.com/nats-io/nats-server/compare/v2.4.0...v2.5.0
[v2.4.0]: https://github.com/nats-io/nats-server/compare/v2.3.4...v2.4.0
[v2.3.4]: https://github.com/nats-io/nats-server/compare/v2.3.3...v2.3.4
[v2.3.3]: https://github.com/nats-io/nats-server/compare/v2.3.2...v2.3.3
[v2.3.2]: https://github.com/nats-io/nats-server/compare/v2.3.1...v2.3.2
[v2.3.1]: https://github.com/nats-io/nats-server/compare/v2.3.0...v2.3.1
[v2.3.0]: https://github.com/nats-io/nats-server/compare/v2.2.6...v2.3.0
[v2.2.6]: https://github.com/nats-io/nats-server/compare/v2.2.5...v2.2.6
[v2.2.5]: https://github.com/nats-io/nats-server/compare/v2.2.4...v2.2.5
[v2.2.4]: https://github.com/nats-io/nats-server/compare/v2.2.3...v2.2.4
[v2.2.3]: https://github.com/nats-io/nats-server/compare/v2.2.2...v2.2.3
[v2.2.2]: https://github.com/nats-io/nats-server/compare/v2.2.1...v2.2.2
[v2.2.1]: https://github.com/nats-io/nats-server/compare/v2.2.0...v2.2.1
[v2.2.0]: https://github.com/nats-io/nats-server/compare/v2.1.9...v2.2.0
[v2.1.9]: https://github.com/nats-io/nats-server/compare/v2.1.8...v2.1.9
[v2.1.8]: https://github.com/nats-io/nats-server/compare/v2.1.7...v2.1.8
[v2.1.7]: https://github.com/nats-io/nats-server/compare/v2.1.6...v2.1.7
[v2.1.6]: https://github.com/nats-io/nats-server/compare/v2.1.4...v2.1.6
[v2.1.4]: https://github.com/nats-io/nats-server/compare/v2.1.2...v2.1.4
[v2.1.2]: https://github.com/nats-io/nats-server/compare/v2.1.0...v2.1.2
[v2.1.0]: https://github.com/nats-io/nats-server/compare/v2.0.4...v2.1.0
[v2.0.4]: https://github.com/nats-io/nats-server/compare/v2.0.2...v2.0.4
[v2.0.2]: https://github.com/nats-io/nats-server/compare/v2.0.0...v2.0.2
[v2.0.0]: https://github.com/nats-io/nats-server/compare/v1.4.1...v2.0.0
[v1.4.1]: https://github.com/nats-io/nats-server/compare/v1.4.0...v1.4.1
[v1.4.0]: https://github.com/nats-io/nats-server/compare/v1.3.0...v1.4.0
[v1.3.0]: https://github.com/nats-io/nats-server/compare/v1.2.0...v1.3.0
[v1.2.0]: https://github.com/nats-io/nats-server/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/nats-io/nats-server/compare/v1.0.6...v1.1.0
[v1.0.6]: https://github.com/nats-io/nats-server/compare/v1.0.4...v1.0.6
[v1.0.4]: https://github.com/nats-io/nats-server/compare/v1.0.2...v1.0.4
[v1.0.2]: https://github.com/nats-io/nats-server/compare/v1.0.0...v1.0.2
[v1.0.0]: https://github.com/nats-io/nats-server/compare/v0.9.6...v1.0.0
[v0.9.6]: https://github.com/nats-io/nats-server/compare/v0.9.4...v0.9.6
[v0.9.4]: https://github.com/nats-io/nats-server/compare/v0.9.2...v0.9.4
[v0.9.2]: https://github.com/nats-io/nats-server/compare/v0.8.1...v0.9.2
[v0.8.1]: https://github.com/nats-io/nats-server/compare/v0.8.0...v0.8.1
[v0.8.0]: https://github.com/nats-io/nats-server/compare/v0.7.2...v0.8.0
[v0.7.2]: https://github.com/nats-io/nats-server/compare/v0.7.0...v0.7.2
[v0.7.0]: https://github.com/nats-io/nats-server/compare/v0.6.8...v0.7.0
[v0.6.8]: https://github.com/nats-io/nats-server/compare/v0.6.6...v0.6.8
[v0.6.6]: https://github.com/nats-io/nats-server/compare/v0.6.4...v0.6.6
[v0.6.4]: https://github.com/nats-io/nats-server/compare/v0.6.2...v0.6.4
[v0.6.2]: https://github.com/nats-io/nats-server/compare/v0.6.0...v0.6.2
[v0.6.0]: https://github.com/nats-io/nats-server/compare/v0.5.6...v0.6.0
[v0.5.6]: https://github.com/nats-io/nats-server/compare/v0.5.4...v0.5.6
[v0.5.4]: https://github.com/nats-io/nats-server/compare/v0.5.2...v0.5.4
[v0.5.2]: https://github.com/nats-io/nats-server/compare/v0.5.1...v0.5.2
[v0.5.1]: https://github.com/nats-io/nats-server/tree/v0.5.1

<!-- Generated by https://github.com/rhysd/changelog-from-release v3.8.1 -->
