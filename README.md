##<img src="logos/nats-server.png" width="300">
[![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Build][Build-Status-Image]][Build-Status-Url] [![Release][Release-Image]][Release-Url] [![Coverage][Coverage-Image]][Coverage-Url]

A High Performance [NATS](https://nats.io) Server written in [Go.](http://golang.org)

**Note**: The `master` branch may be in an *unstable or even in a broken state* during development. Please use [releases][github-release] instead of the `master` branch in order to get stable binaries.

## Getting Started

The best way to get the NATS server is to use one of the pre-built release binaries which are available for OSX, Linux (x86-64/ARM), Windows, and Docker. Instructions for using these binaries are on the [GitHub releases page][github-release].
You can also connect to a public server that is running at our demo site: [nats://demo.nats.io:4222](nats://demo.nats.io:4222), and a secure version at [nats://demo.nats.io:4443](nats://demo.nats.io:4443).

Of course you can build the latest version of the server from the `master` branch. The master branch will always build and pass tests, but may not work correctly in your environment.
You will first need [*Go*](https://golang.org/) installed on your machine (version 1.4+ is required) to build the NATS server.

### Running

The NATS server is lightweight and very performant. Starting one with no arguments will give you a server with sane default settings.

```sh
> ./gnatsd
[2842] 2016/04/26 13:21:20.379640 [INF] Starting nats-server version 0.8.0
[2842] 2016/04/26 13:21:20.379745 [INF] Listening for client connections on 0.0.0.0:4222
[2842] 2016/04/26 13:21:20.379865 [INF] Server is ready
```

The server will be started and listening for client connections on port 4222 (the default) from all available interfaces. The logs will be displayed to stdout
as shown above. There are a large range of supported clients that can be found at [https://nats.io/clients](https://nats.io/download).
The server uses a text based protocol, so interacting with it can be as simple as using telnet.

```sh
> telnet demo.nats.io 4222
Trying 107.170.221.32...
Connected to demo.nats.io.
Escape character is '^]'.
INFO {"server_id":"kG19DsXX1UVeSyEjhl3RFw","version":"0.8.0.beta","go":"go1.6.2","host":"0.0.0.0","port":4222, ...}
SUB foo 1
+OK
PUB foo 11
Hello World
+OK
MSG foo 1 11
Hello World
```

More information on the NATS protocol can be found at [http://nats.io/documentation/internals/nats-protocol](http://nats.io/documentation/internals/nats-protocol/).

## Configuring

The server accepts command line arguments to control its behavior. An example  configuration file is listed below. Note that
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
        --help_tls                   TLS help
```

## Sample Configuration File

```

port: 4242      # port to listen for client connections
net: apcera.me  # net interface to listen

http_port: 8222 # HTTP monitoring port

# Authorization for client connections
authorization {
  user:     derek
  # ./util/mkpassword -p T0pS3cr3t
  password: $2a$11$W2zko751KUvVy59mUTWmpOdWjpEm5qhcCZRd05GjI/sSOT.xtiHyG
  timeout:  1
}

# Cluster definition

cluster {

  host: '127.0.0.1'  # host/net interface
  port: 4244         # port for inbound route connections

  # Authorization for route connections
  authorization {
    user: route_user
    # ./util/mkpassword -p T0pS3cr3tT00!
    password: $2a$11$xH8dkGrty1cBNtZjhPeWJewu/YPbSU.rXJWmS6SFilOBXzmZoMk9m
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
log_file: "/tmp/nats-server.log"

# pid file
pid_file: "/tmp/nats-server.pid"

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

## Clustering

Setting up a full mesh cluster is really easy. When running NATS Servers in different hosts, the command line parameters for all servers could be as simple as:

```
gnatsd -cluster nats://$HOSTNAME:$NATS_CLUSTER_PORT -routes://$NATS_SEED_HOST:$NATS_CLUSTER_PORT
```

Even on the host where the "seed" is running, the above command would work. The server would detect an attempt to connect to itself and ignore that. In other words, the same command line could be deployed in several hosts and the full mesh will properly form.

Note that you don't have to connect all servers to the same "seed" server, any server accepting a connection will inform other servers in the mesh about that new server so that they can connect to it. The advantage of the seed approach, is that you can "deploy" the same configuration (as shown above) on all hosts.

Now let's have a look at running a cluster of 3 servers on the same host. We will start with the seed server and use the `-D` command line parameter to produce debug information.

```
gnatsd -p 4222 -cluster nats://localhost:4248 -D
```

Alternatively, you could use a configuration file, let's call it `seed.conf`, with a content similar to this:
```
# Cluster Seed Node

port: 4222
net: 127.0.0.1

http_port: 8222

cluster {
  host: 127.0.0.1
  port: 4248
}
```
and start the server like this:
```
gnatsd -config ./seed.conf -D
```

This will produce an output similar to:
```
[75653] 2016/04/26 15:14:47.339321 [INF] Listening for route connections on localhost:4248
[75653] 2016/04/26 15:14:47.340787 [INF] Listening for client connections on 0.0.0.0:4222
[75653] 2016/04/26 15:14:47.340822 [DBG] server id is xZfu3u7usAPWkuThomoGzM
[75653] 2016/04/26 15:14:47.340825 [INF] server is ready
```

Now let's start two more servers, each one connecting to the seed server.

```
gnatsd -p 5222 -cluster nats://localhost:5248 -routes nats://localhost:4248 -D
```

When running on the same host, we need to pick different ports for the client connections `-p`, and for the port used to accept other routes `-cluster`. Note that `-routes` points to the `-cluster` address of the seed server (`localhost:4248`).

Here is the log produced. See how it connects and registers a route to the seed server (`...GzM`).

```
[75665] 2016/04/26 15:14:59.970014 [INF] Listening for route connections on localhost:5248
[75665] 2016/04/26 15:14:59.971150 [INF] Listening for client connections on 0.0.0.0:5222
[75665] 2016/04/26 15:14:59.971176 [DBG] server id is 53Yi78q96t52QdyyWLKIyE
[75665] 2016/04/26 15:14:59.971179 [INF] server is ready
[75665] 2016/04/26 15:14:59.971199 [DBG] Trying to connect to route on localhost:4248
[75665] 2016/04/26 15:14:59.971551 [DBG] 127.0.0.1:4248 - rid:1 - Route connection created
[75665] 2016/04/26 15:14:59.971559 [DBG] 127.0.0.1:4248 - rid:1 - Route connect msg sent
[75665] 2016/04/26 15:14:59.971720 [DBG] 127.0.0.1:4248 - rid:1 - Registering remote route "xZfu3u7usAPWkuThomoGzM"
[75665] 2016/04/26 15:14:59.971731 [DBG] 127.0.0.1:4248 - rid:1 - Route sent local subscriptions
```

From the seed's server log, we see that the route is indeed accepted:

```
[75653] 2016/04/26 15:14:59.971602 [DBG] 127.0.0.1:52679 - rid:1 - Route connection created
[75653] 2016/04/26 15:14:59.971733 [DBG] 127.0.0.1:52679 - rid:1 - Registering remote route "53Yi78q96t52QdyyWLKIyE"
[75653] 2016/04/26 15:14:59.971739 [DBG] 127.0.0.1:52679 - rid:1 - Route sent local subscriptions
```

Finally, let's start the third server:

```
gnatsd -p 6222 -cluster nats://localhost:6248 -routes nats://localhost:4248 -D
```

Again, notice that we use a different client port and cluster address, but still point to the same seed server at the address `nats://localhost:4248`:

```
[75764] 2016/04/26 15:19:11.528185 [INF] Listening for route connections on localhost:6248
[75764] 2016/04/26 15:19:11.529787 [INF] Listening for client connections on 0.0.0.0:6222
[75764] 2016/04/26 15:19:11.529829 [DBG] server id is IRepas80TBwJByULX1ulAp
[75764] 2016/04/26 15:19:11.529842 [INF] server is ready
[75764] 2016/04/26 15:19:11.529872 [DBG] Trying to connect to route on localhost:4248
[75764] 2016/04/26 15:19:11.530272 [DBG] 127.0.0.1:4248 - rid:1 - Route connection created
[75764] 2016/04/26 15:19:11.530281 [DBG] 127.0.0.1:4248 - rid:1 - Route connect msg sent
[75764] 2016/04/26 15:19:11.530408 [DBG] 127.0.0.1:4248 - rid:1 - Registering remote route "xZfu3u7usAPWkuThomoGzM"
[75764] 2016/04/26 15:19:11.530414 [DBG] 127.0.0.1:4248 - rid:1 - Route sent local subscriptions
[75764] 2016/04/26 15:19:11.530595 [DBG] 127.0.0.1:52727 - rid:2 - Route connection created
[75764] 2016/04/26 15:19:11.530659 [DBG] 127.0.0.1:52727 - rid:2 - Registering remote route "53Yi78q96t52QdyyWLKIyE"
[75764] 2016/04/26 15:19:11.530664 [DBG] 127.0.0.1:52727 - rid:2 - Route sent local subscriptions
```

First a route is created to the seed server (`...GzM`) and after that, a route from `...IyE` - which is the ID of the second server - is accepted.

The log from the seed server shows that it accepted the route from the third server:

```
[75653] 2016/04/26 15:19:11.530308 [DBG] 127.0.0.1:52726 - rid:2 - Route connection created
[75653] 2016/04/26 15:19:11.530384 [DBG] 127.0.0.1:52726 - rid:2 - Registering remote route "IRepas80TBwJByULX1ulAp"
[75653] 2016/04/26 15:19:11.530389 [DBG] 127.0.0.1:52726 - rid:2 - Route sent local subscriptions
```

And the log from the second server shows that it connected to the third.

```
[75665] 2016/04/26 15:19:11.530469 [DBG] Trying to connect to route on 127.0.0.1:6248
[75665] 2016/04/26 15:19:11.530565 [DBG] 127.0.0.1:6248 - rid:2 - Route connection created
[75665] 2016/04/26 15:19:11.530570 [DBG] 127.0.0.1:6248 - rid:2 - Route connect msg sent
[75665] 2016/04/26 15:19:11.530644 [DBG] 127.0.0.1:6248 - rid:2 - Registering remote route "IRepas80TBwJByULX1ulAp"
[75665] 2016/04/26 15:19:11.530650 [DBG] 127.0.0.1:6248 - rid:2 - Route sent local subscriptions
```

At this point, there is a full mesh cluster of NATS servers.


## Securing NATS

### TLS

As of Release 0.7.0, the server can use modern TLS semantics for client connections, route connections, and the HTTPS monitoring port.
The server requires TLS version 1.2, and sets preferences for modern cipher suites that avoid those known with vunerabilities. The
server's preferences when building with Go1.5 are as follows.

```go
func defaultCipherSuites() []uint16 {
	return []uint16{
		// The SHA384 versions are only in Go1.5+
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	}
}
```

Generating self signed certs and intermediary certificate authorities is beyond the scope here, but this document can be helpful in addition to Google Search:
<a href="https://docs.docker.com/engine/articles/https/" target="_blank">https://docs.docker.com/engine/articles/https/</a>

The server **requires** a certificate and private key. Optionally the server can require that clients need to present certificates, and the server can be configured
with a CA authority to verify the client certificates.

```
# Simple TLS config file

net:  127.0.0.1
port: 4443

tls {
  cert_file:  "./configs/certs/server-cert.pem"
  key_file:   "./configs/certs/server-key.pem"
  timeout:    2
}

authorization {
  user:     derek
  password: $2a$11$W2zko751KUvVy59mUTWmpOdWjpEm5qhcCZRd05GjI/sSOT.xtiHyG
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
> ./gnatsd --tls --tlscert=./test/configs/certs/server-cert.pem --tlskey=./test/configs/certs/server-key.pem

[2935] 2016/04/26 13:34:30.685413 [INF] Starting nats-server version 0.8.0.beta
[2935] 2016/04/26 13:34:30.685509 [INF] Listening for client connections on 0.0.0.0:4222
[2935] 2016/04/26 13:34:30.685656 [INF] TLS required for client connections
[2935] 2016/04/26 13:34:30.685660 [INF] Server is ready
```

Notice that the log  indicates that the client connections will be required to use TLS. If you run the server in Debug mode with -D or -DV, the logs will show the cipher suite selection for each connected client.
```
[15146] 2015/12/03 12:38:37.733139 [DBG] ::1:63330 - cid:1 - Starting TLS client connection handshake
[15146] 2015/12/03 12:38:37.751948 [DBG] ::1:63330 - cid:1 - TLS handshake complete
[15146] 2015/12/03 12:38:37.751959 [DBG] ::1:63330 - cid:1 - TLS version 1.2, cipher suite TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
```


If you want the server to enforce and require client certificates as well via the command line, utilize this example.

```
> ./gnatsd --tlsverify --tlscert=./test/configs/certs/server-cert.pem --tlskey=./test/configs/certs/server-key.pem --tlscacert=./test/configs/certs/ca.pem
```


### Bcrypt

In addition to TLS functionality, the server now also supports bcrypt for passwords and tokens. This is transparent and you can simply replace the plaintext password in the configuration with the bcrypt hash, the server will automatically utilize bcrypt as needed.

There is a utility bundled under /util/mkpasswd. By default with no arguments it will generate a secure password and the associated hash. This can be used for a password or a token in the configuration. If you already have a password selected, you can supply that on stdin with the -p flag.

```bash
~/go/src/github.com/nats-io/gnatsd/util> ./mkpasswd
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
  "server_id": "kG19DsXX1UVeSyEjhl3RFw",
  "version": "0.8.0.beta",
  "go": "go1.6.2",
  "host": "0.0.0.0",
  "auth_required": false,
  "ssl_required": false,
  "tls_required": false,
  "tls_verify": false,
  "max_connections": 65536,
  "ping_interval": 120000000000,
  "ping_max": 2,
  "http_port": 8222,
  "https_port": 0,
  "max_control_line": 1024,
  "max_pending_size": 10485760,
  "cluster_port": 0,
  "tls_timeout": 0.5,
  "port": 4222,
  "max_payload": 1048576,
  "start": "2016-04-25T22:09:34.684376796-04:00",
  "now": "2016-04-26T16:39:47.332681784-04:00",
  "uptime": "18h30m12s",
  "mem": 21131264,
  "cores": 8,
  "cpu": 0,
  "connections": 1002,
  "total_connections": 1831,
  "routes": 0,
  "remotes": 0,
  "in_msgs": 5501151,
  "out_msgs": 11001275,
  "in_bytes": 27645750,
  "out_bytes": 55162353,
  "slow_consumers": 0,
  "http_req_stats": {
    "/": 223,
    "/connz": 31,
    "/routez": 1,
    "/subsz": 7,
    "/varz": 35
  }
}
```

### /connz

<a href="http://localhost:8222/connz" target="_blank">http://localhost:8222/connz</a> reports more detailed information on current connections. It uses a paging mechanism which defaults to 1024 connections.
You can control these via url arguments (limit and offset), e.g. <a href="http://localhost:8222/connz?limit=1&offset=1" target="_blank">http://localhost:8222/connz?limit=1&offset=1</a>.
You can also report detailed subscription information on a per connection basis using subs=1, e.g. <a href="http://localhost:8222/connz?limit=1&offset=1&subs=1" target="_blank">http://localhost:8222/connz?limit=1&offset=1&subs=1</a>.

```json
{
  "now": "2016-04-26T16:40:59.926732854-04:00",
  "num_connections": 2,
  "offset": 0,
  "limit": 1024,
  "connections": [
    {
      "cid": 1,
      "ip": "73.170.105.42",
      "port": 46422,
      "start": "2016-04-25T22:09:34.715492478-04:00",
      "last_activity": "2016-04-26T16:40:56.17467432-04:00",
      "uptime": "18h31m25s",
      "idle": "3s",
      "pending_bytes": 0,
      "in_msgs": 1112,
      "out_msgs": 0,
      "in_bytes": 139101,
      "out_bytes": 0,
      "subscriptions": 1,
      "lang": "go",
      "version": "1.1.9"
    },
    {
      "cid": 3,
      "ip": "129.192.191.2",
      "port": 55072,
      "start": "2016-04-25T22:09:36.685630786-04:00",
      "last_activity": "2016-04-26T16:40:56.17467432-04:00",
      "uptime": "18h31m23s",
      "idle": "3s",
      "pending_bytes": 0,
      "in_msgs": 0,
      "out_msgs": 1112,
      "in_bytes": 0,
      "out_bytes": 139101,
      "subscriptions": 1,
      "lang": "go",
      "version": "1.1.9"
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
  "num_subscriptions": 2,
  "num_cache": 1,
  "num_inserts": 3539,
  "num_removes": 3537,
  "num_matches": 103,
  "cache_hit_rate": 0.6504854368932039,
  "max_fanout": 1,
  "avg_fanout": 1
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
rapidly, new releases provide performance improvements and fixes. Information
on installation, including pre-built binaries, is available at
<http://golang.org/doc/install>.  Stable branches of operating system
packagers provided by your OS vendor may not be sufficient.

Note that we now support vendored dependencies, which are fully supported on Go1.6.
For Go1.5, please build with GO15VENDOREXPERIMENT=1.

Run `go version` to see the version of Go which you have installed.

Run `go build` inside the directory to build.

Run `go test ./...` to run the unit regression tests.

A successful build run produces no messages and creates an executable called
`gnatsd` in this directory.  You can invoke that binary, with no options and
no configuration file, to start a server with acceptable standalone defaults
(no authentication, no clustering).

Run `go help` for more guidance, and visit <http://golang.org/> for tutorials,
presentations, references and more.


## Client libraries

Here is a sample of client language bindings for NATS. For a complete and updated list, please visit <https://nats.io/download>.

### Apcera Supported
- [Go](https://github.com/nats-io/nats)
- [Node.js](https://github.com/nats-io/node-nats)
- [Java](https://github.com/nats-io/jnats)
- [C/C++](https://github.com/nats-io/cnats)
- [C#/.NET](https://github.com/nats-io/csnats)
- [Python Asyncio](https://github.com/nats-io/asyncio-nats)
- [Ruby](https://github.com/nats-io/ruby-nats)
- [Elixir](https://github.com/nats-io/elixir-nats)
- [NGINX](https://github.com/nats-io/nginx-nats)

### Community
- [Scala](https://github.com/tyagihas/scala_nats)
- [Arduino](https://github.com/joshglendenning/arduino-nats)
- [Lua](https://github.com/DawnAngel/lua-nats)
- [PHP](https://github.com/repejota/phpnats)
- [Python Twisted](https://github.com/johnwlockwood/txnats)
- [Python](https://github.com/mcuadros/pynats)
- [Haskell](https://github.com/ondrap/nats-queue)
- [Rust](https://github.com/jedisct1/rust-nats)


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
