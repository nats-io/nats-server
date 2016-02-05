# NATS server

[![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Build][Build-Status-Image]][Build-Status-Url] [![Release][Release-Image]][Release-Url] [![Coverage][Coverage-Image]][Coverage-Url]

A High Performance [NATS](https://nats.io) Server written in [Go.](http://golang.org)

**Note**: The `master` branch may be in an *unstable or even in a broken state* during development. Please use [releases][github-release] instead of the `master` branch in order to get stable binaries.

## Getting Started

The best way to get the nats server is to use one of the pre-built release binaries which are available for OSX, Linux (x86-64/ARM), and Docker. Instructions for using these binaries are on the [GitHub releases page][github-release].
You can also connect to a public server that is running at our demo site: [nats://demo.nats.io:4222](nats://demo.nats.io:4222), and a secure version at [nats://demo.nats.io:4443](nats://demo.nats.io:4443).

Of course you can build the latest version of gnatsd from the `master` branch. The master branch will always build and pass tests, but may not work correctly in your environment.
You will first need [*Go*](https://golang.org/) installed on your machine (version 1.4+ is required) to build gnatsd.

### Running

The nats server is lightweight and very performant. Starting one with no arguments will give you a server with sane default settings.

```sh
> gnatsd
[35572] 2016/01/10 08:54:16.178798 [INF] Starting gnatsd version 0.7.3
[35572] 2016/01/10 08:54:16.178940 [INF] Listening for client connections on 0.0.0.0:4222
[35572] 2016/01/10 08:54:16.179115 [INF] gnatsd is ready
```

The server will be started and listening for client connections on port 4222 from all interfaces. The logs will be displayed to stdout
as shown above. There are a large range of supported clients that can be found at [https://nats.io/clients](https://nats.io/download).
The server uses a text based protocol, so interacting with it can be as simple as using telnet.

```sh
> telnet localhost 4222
Trying ::1...
Connected to localhost.
Escape character is '^]'.
INFO {"server_id":"3d13165236fe601c863b6714e819fc36","version":"0.7.3","go":"go1.5.2","host":"0.0.0.0","port":4222, ...}
SUB foo 1
+OK
PUB foo 11
Hello World
+OK
MSG foo 1 11
Hello World
```

More information on the protocol can be found at [http://nats.io/documentation/internals/nats-protocol](http://nats.io/documentation/internals/nats-protocol/).

## Configuring

gnatsd accepts command line arguments to control its behavior. An example  configuration file is listed below. Note that
command line arguments will override those items in the configuration file.


```
Server Options:
    -a, --addr HOST                  Bind to HOST address (default: 0.0.0.0)
    -p, --port PORT                  Use PORT for clients (default: 4222)
    -P, --pid FILE                   File to store PID
    -m, --http_port PORT             Use HTTP PORT for monitoring
    -ms,--https_port PORT            Use HTTPS PORT for monitoring
    -c, --config FILE                Configuration File

Logging Options:
    -l, --log FILE                   File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Enable syslog as log method.
    -r, --remote_syslog              Syslog server addr (udp://localhost:514).
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -DV                              Debug and Trace

Authorization Options:
        --user user                  User required for connections
        --pass password              Password required for connections

TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert FILE               Server certificate file
        --tlskey FILE                Private key for server certificate
        --tlsverify                  Enable TLS, very client certificates
        --tlscacert FILE             Client certificate CA for verification

Cluster Options:
        --routes [rurl-1, rurl-2]    Routes to solicit and connect
        --cluster [cluster url]      Cluster URL for solicited routes

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
```

## Sample Configuration File

```

port: 4242      # port to listen for client connections
net: apcera.me  # net interface to listen

http_port: 8222 # HTTP monitoring port

# Authorization for client connections
authorization {
  user:     derek
  password: T0pS3cr3t
  timeout:  1
}

# Cluster definition

cluster {

  host: '127.0.0.1'  # host/net interface
  port: 4244         # port for inbound route connections

  # Authorization for route connections
  authorization {
    user: route_user
    password: T0pS3cr3tT00!
    timeout: 0.5
  }

  # Routes are actively solicited and connected to from this server.
  # Other servers can connect to us if they supply the correct credentials
  # in their routes definitions from above.

  routes = [
    nats-route://user1:pass1@127.0.0.1:4245
    nats-route://user2:pass2@127.0.0.1:4246
  ]
}

# logging options
debug:   false
trace:   true
logtime: false
log_file: "/tmp/gnatsd.log"

# pid file
pid_file: "/tmp/gnatsd.pid"

# Some system overides

# max_connections
max_connections: 100

# maximum protocol control line
max_control_line: 512

# maximum payload
max_payload: 65536

# slow consumer threshold
max_pending_size: 10000000

```

## Securing NATS

### TLS

As of Release 0.7.0, the server can use modern TLS semantics for client connections, route connections, and the HTTPS monitoring port.
The server requires TLS version 1.2, and sets preferences for modern cipher suites that avoid those known with vunerabilities. The
server's preferences when building with Go1.5 are as follows.

```go
func defaultCipherSuites() []uint16 {
	return []uint16{
		// The SHA384 versions are only in Go1.5
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	}
}
```

Generating self signed certs and intermediary certificate authotities is beyond the scope here, but this document can be helpful in addition to Google Search:
<a href="https://docs.docker.com/engine/articles/https/" target="_blank">https://docs.docker.com/engine/articles/https/</a>

The server **requires** a certificate and private key. Optionally the server can require that clients need to present certificates, and the server can be configured
with a CA authority to verify the client certificates.

```
# Simple TLS config file

port: 4443
net: apcera.me # net interface

tls {
  cert_file:  "./configs/certs/server-cert.pem"
  key_file:   "./configs/certs/server-key.pem"
  timeout:    0.5
}

authorization {
  user:     derek
  password: s3cr3t!
  timeout:  1
}
```

If requiring client certificates as well, simply change the TLS section as follows.

```
tls {
  cert_file: "./configs/certs/server-cert.pem"
  key_file:  "./configs/certs/server-key.pem"
  ca_file:   "./configs/certs/ca.pem"
  verify:    true
}
```

When setting up clusters, all servers in the cluster, if using TLS, will both verify the connecting endpoints and the server responses. So certificates are checked in
both directions. Certificates can be configured only for the server's cluster identity, keeping client and server certificates separate from cluster formation.

```
cluster {
  host: '127.0.0.1'
  port: 4244

  tls {
    # Route cert
    cert_file: "./configs/certs/srva-cert.pem"
    # Private key
    key_file:  "./configs/certs/srva-key.pem"
    # Optional certificate authority verifying connected routes
    # Required when we have self-signed CA, etc.
    ca_file:   "./configs/certs/ca.pem"
  }
  # Routes are actively solicited and connected to from this server.
  # Other servers can connect to us if they supply the correct credentials
  # in their routes definitions from above.
  routes = [
    nats-route://127.0.0.1:4246
  ]
}
```
The server can be run using command line arguments to enable TLS functionality.

```
--tls                        Enable TLS, do not verify clients (default: false)
--tlscert FILE               Server certificate file
--tlskey FILE                Private key for server certificate
--tlsverify                  Enable TLS, very client certificates
--tlscacert FILE             Client certificate CA for verification
```

Examples using the test certicates which are self signed for localhost and 127.0.0.1.
```bash
gnatsd --tls --tlscert=./test/configs/certs/server-cert.pem --tlskey=./test/configs/certs/server-key.pem

[15139] 2015/12/03 12:37:18.963635 [INF] Starting gnatsd version 0.7.0
[15139] 2015/12/03 12:37:18.963710 [INF] Listening for client connections on 0.0.0.0:4222
[15139] 2015/12/03 12:37:18.963874 [INF] TLS required for client connections
[15139] 2015/12/03 12:37:18.963878 [INF] gnatsd is ready
```

Notice that the log  indicates that the client connections will be required to use TLS. If you run the server in Debug mode with -D or -DV, the logs will show the cipher suite selection for each connected client.
```
[15146] 2015/12/03 12:38:37.733139 [DBG] ::1:63330 - cid:1 - Starting TLS client connection handshake
[15146] 2015/12/03 12:38:37.751948 [DBG] ::1:63330 - cid:1 - TLS handshake complete
[15146] 2015/12/03 12:38:37.751959 [DBG] ::1:63330 - cid:1 - TLS version 1.2, cipher suite TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
```


If you want the server to enforce and require client certificates as well via the command line, utilize this example.

```
gnatsd --tlsverify --tlscert=./test/configs/certs/server-cert.pem --tlskey=./test/configs/certs/server-key.pem --tlscacert=./test/configs/certs/ca.pem
```


### Bcrypt

In addition to TLS functionality, the server now also supports bcrypt for passwords and tokens. This is transparent and you can simply replace the plaintext password in the configuration with the bcrypt hash, the server will automatically utilize bcrypt as needed.

There is a utility bundled under /util/mkpasswd. By default with no arguments it will generate a secure password and the associated hash. This can be used for a password or a token in the configuration. If you already have a password selected, you can supply that on stdin with the -p flag.

```bash
> $GOPATH/bin/mkpasswd
pass: #IclkRPHUpsTmACWzmIGXr
bcrypt hash: $2a$11$3kIDaCxw.Glsl1.u5nKa6eUnNDLV5HV9tIuUp7EHhMt6Nm9myW1aS
```

Add into the server configuration file's authorization section.
```
  authorization {
    user: derek
    password: $2a$11$3kIDaCxw.Glsl1.u5nKa6eUnNDLV5HV9tIuUp7EHhMt6Nm9myW1aS
  }
```

## Monitoring

If the monitoring port is enabled, the server will run a lightweight http server that has several endpoints defined, **[/varz, /connz, /routez, /subsz]**. All endpoints return a JSON object.

To test, run '``gnatsd -m 8222``', then go to <a href="http://localhost:8222/" target="_blank">http://localhost:8222/</a>

### /varz

<a href="http://localhost:8222/varz" target="_blank">http://localhost:8222/varz</a> reports various general statistics.

```json
{
  "server_id": "ec933edcd2bd86bcf71d555fc8b4fb2c",
  "version": "0.6.6",
  "go": "go1.5.0",
  "host": "0.0.0.0",
  "port": 4222,
  "auth_required": false,
  "ssl_required": false,
  "max_payload": 1048576,
  "max_connections": 65536,
  "ping_interval": 120000000000,
  "ping_max": 2,
  "http_port": 8222,
  "ssl_timeout": 0.5,
  "max_control_line": 1024,
  "max_pending_size": 10485760,
  "max_payload": 1048576,
  "start": "2015-07-14T13:29:26.426805508-07:00",
  "now": "2015-07-14T13:30:59.349179963-07:00",
  "uptime": "1m33s",
  "mem": 8445952,
  "cores": 4,
  "cpu": 0,
  "connections": 39,
  "total_connections": 122,
  "routes": 0,
  "remotes": 0,
  "in_msgs": 100000,
  "out_msgs": 100000,
  "in_bytes": 1600000,
  "out_bytes": 1600000,
  "slow_consumers": 0,
  "http_req_stats": {
    "/": 8,
    "/connz": 2,
    "/routez": 0,
    "/subsz": 0,
    "/varz": 8
  }
}
```

### /connz

<a href="http://localhost:8222/connz" target="_blank">http://localhost:8222/connz</a> reports more detailed information on current connections. It uses a paging mechanism which defaults to 1024 connections.
You can control these via url arguments (limit and offset), e.g. <a href="http://localhost:8222/connz?limit=1&offset=1" target="_blank">http://localhost:8222/connz?limit=1&offset=1</a>.
You can also report detailed subscription information on a per connection basis using subs=1, e.g. <a href="http://localhost:8222/connz?limit=1&offset=1&subs=1" target="_blank">http://localhost:8222/connz?limit=1&offset=1&subs=1</a>.

```json
{
  "now": "2016-01-10T08:17:29.970134607-08:00",
  "num_connections": 2,
  "offset": 0,
  "limit": 1024,
  "connections": [
    {
      "cid": 571,
      "ip": "127.0.0.1",
      "port": 61572,
      "start": "2016-01-10T08:15:07.970134607-08:00",
      "uptime": "2m22s",
      "pending_bytes": 0,
      "in_msgs": 0,
      "out_msgs": 0,
      "in_bytes": 0,
      "out_bytes": 0,
      "subscriptions": 1,
      "lang": "go",
      "version": "1.0.9",
      "subscriptions_list": [
        "hello.world"
      ]
    },
    {
      "cid": 574,
      "ip": "127.0.0.1",
      "port": 61577,
      "start": "2016-01-10T08:17:09.970134607-08:00",
      "uptime": "22m22s",
      "pending_bytes": 0,
      "in_msgs": 0,
      "out_msgs": 0,
      "in_bytes": 0,
      "out_bytes": 0,
      "subscriptions": 1,
      "lang": "ruby",
      "version": "0.5.0",
      "subscriptions_list": [
        "hello.world"
      ]
    }
  ]
}
```

### /routez

<a href="http://localhost:8222/routez" target="_blank">http://localhost:8222/routez</a> reports information on active routes for a cluster. Routes are expected to be low, so there is no paging mechanism currently with this endpoint. It does support the subs arg line /connz, e.g. <a href="http://localhost:8222/routez?subs=1" target="_blank">http://localhost:8222/routez?subs=1</a>

```json
{
  "now": "2015-07-14T13:30:59.349179963-07:00",
  "num_routes": 1,
  "routes": [
    {
      "rid": 1,
      "remote_id": "de475c0041418afc799bccf0fdd61b47",
      "did_solicit": true,
      "ip": "127.0.0.1",
      "port": 61791,
      "pending_size": 0,
      "in_msgs": 0,
      "out_msgs": 0,
      "in_bytes": 0,
      "out_bytes": 0,
      "subscriptions": 0
    }
  ]
}
```

### /subsz

<a href="http://localhost:8222/subsz" target="_blank">http://localhost:8222/subsz</a> reports detailed information about the current subscriptions and the routing data structure.

```json
{
  "num_subscriptions": 3,
  "num_cache": 0,
  "num_inserts": 572,
  "num_removes": 569,
  "num_matches": 200000,
  "cache_hit_rate": 0.99999,
  "max_fanout": 0,
  "avg_fanout": 0,
  "stats_time": "2015-07-14T12:55:25.564818051-07:00"
}
```

Monitoring endpoints support JSONP for CORS so you can easily create single page
web applications for monitoring. Simply pass `callback` query parameter to any
endpoint. For example; `http://localhost:8222/connz?callback=cb`

```javascript
// JQuery example

$.getJSON('http://localhost:8222/connz?callback=?', function(data) {
  console.log(data);
});

```

## Building

This code currently requires at _least_ version 1.4 of Go, but we encourage
the use of the latest stable release.  We will be moving to requiring at _least_ 1.5
in the near future. Go is still young and improving
rapidly, new releases provide performance improvements and fixes.  Information
on installation, including pre-built binaries, is available at
<http://golang.org/doc/install>.  Stable branches of operating system
packagers provided by your OS vendor may not be sufficient.

Run `go version` to see the version of Go which you have installed.

Run `go test ./...` to run the unit regression tests.

Run `go install ./...` to build and install `gnatsd`, `mkpasswd`, `nats-pub` and `nats-sub` to $GOPATH/bin.

You can invoke `$GOPATH/bin/gnatsd &`, with no options and no configuration file, to start a server 
with acceptable standalone defaults (no authentication, no clustering). And start a subscriber via 
`$GOPATH/bin/nats-sub foo &` and publish a message on the `foo` channel like `$GOPATH/bin/nats-pub foo hello`.

Run `go help` for more guidance, and visit <http://golang.org/> for tutorials,
presentations, references and more.


## Client libraries

Here is a sample of client language bindings for NATS. For a complete and updated list, please visit <https://nats.io/download>.

- [Go](https://github.com/nats-io/nats)
- [Node.js](https://github.com/nats-io/node-nats)
- [Java](https://github.com/nats-io/jnats)
- [Spring](https://github.com/cloudfoundry-community/java-nats)
- [C/C++](https://github.com/nats-io/cnats)
- [C#/.NET](https://github.com/nats-io/csnats)
- [Ruby](https://github.com/nats-io/ruby-nats)
- [Lua](https://github.com/DawnAngel/lua-nats)
- [PHP](https://github.com/repejota/phpnats)
- [Python](https://github.com/mcuadros/pynats)
- [Scala](https://github.com/tyagihas/scala_nats)
- [Haskell](https://github.com/ondrap/nats-queue)
- [Rust](https://github.com/jedisct1/rust-nats)
- [NGINX](https://github.com/nats-io/nginx-nats)

## License

(The MIT License)

Copyright (c) 2012-2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

[License-Url]: http://opensource.org/licenses/MIT
[License-Image]: https://img.shields.io/npm/l/express.svg
[Build-Status-Url]: http://travis-ci.org/nats-io/gnatsd
[Build-Status-Image]: https://travis-ci.org/nats-io/gnatsd.svg?branch=master
[Release-Url]: https://github.com/nats-io/gnatsd/releases/tag/v0.7.2
[Release-image]: http://img.shields.io/badge/release-v0.7.2-1eb0fc.svg
[Coverage-Url]: https://coveralls.io/r/nats-io/gnatsd?branch=master
[Coverage-image]: https://img.shields.io/coveralls/nats-io/gnatsd.svg
[ReportCard-Url]: http://goreportcard.com/report/nats-io/gnatsd
[ReportCard-Image]: http://goreportcard.com/badge/nats-io/gnatsd
[github-release]: https://github.com/nats-io/gnatsd/releases/
