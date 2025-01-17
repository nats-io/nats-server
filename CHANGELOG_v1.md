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


[v1.4.1]: https://github.com/nats-io/nats-server/compare/v1.4.0...v1.4.1
[v1.4.0]: https://github.com/nats-io/nats-server/compare/v1.3.0...v1.4.0
[v1.3.0]: https://github.com/nats-io/nats-server/compare/v1.2.0...v1.3.0
[v1.2.0]: https://github.com/nats-io/nats-server/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/nats-io/nats-server/compare/v1.0.6...v1.1.0
[v1.0.6]: https://github.com/nats-io/nats-server/compare/v1.0.4...v1.0.6
[v1.0.4]: https://github.com/nats-io/nats-server/compare/v1.0.2...v1.0.4
[v1.0.2]: https://github.com/nats-io/nats-server/compare/v1.0.0...v1.0.2
[v1.0.0]: https://github.com/nats-io/nats-server/tree/v1.0.0

<!-- Generated by https://github.com/rhysd/changelog-from-release v3.8.1 -->
