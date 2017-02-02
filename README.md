##<img src="logos/nats-server.png" width="300">
[![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Build][Build-Status-Image]][Build-Status-Url] [![Release][Release-Image]][Release-Url] [![Coverage][Coverage-Image]][Coverage-Url]

A High Performance [NATS](https://nats.io) Server written in [Go.](http://golang.org)

## Quickstart

If you just want to start using NATS, and you have [installed Go](https://golang.org/doc/install) 1.5+ and set your $GOPATH:

Install and run the NATS server:

```
go get github.com/nats-io/gnatsd
gnatsd -D -V
```

Install the [Go NATS client](https://github.com/nats-io/go-nats/blob/master/README.md):

```
go get github.com/nats-io/go-nats
```

## Installation

You can install the NATS server binary or Docker image, connect to a NATS service, or build the server from source.

### Download

The recommended way to install the NATS server is to [download](http://nats.io/download/) one of the pre-built release binaries which are available for OSX, Linux (x86-64/ARM), Windows, and Docker. Instructions for using these binaries are on the [GitHub releases page][github-release].

### Demo

You can connect to a public NATS server that is running at our demo site: [nats://demo.nats.io:4222](nats://demo.nats.io:4222), and a secure version at [tls://demo.nats.io:4443](nats://demo.nats.io:4443). See the [protocol](#protocol) section for usage.

### Build

You can build the latest version of the server from the `master` branch. The master branch generally should build and pass tests, but may not work correctly in your environment. Note that stable branches of operating system packagers provided by your OS vendor may not be sufficient.

You need [*Go*](http://golang.org/) version 1.5+ [installed](https://golang.org/doc/install) to build the NATS server. We support vendored dependencies, which are fully supported in Go 1.6. For Go 1.5, build with `GO15VENDOREXPERIMENT=1`.

- Run `go version` to verify that you are running Go 1.5+. (Run `go help` for more guidance.)
- Clone the <https://github.com/nats-io/gnatsd> repository.
- Run `go build` inside the `/nats-io/gnatsd` directory. A successful build produces no messages and creates the server executable `gnatsd` in the directory.
- Run `go test ./...` to run the unit regression tests.

## Running

To start the NATS server with default settings (and no authentication or clustering), you can invoke the `gnatsd` binary with no [command line options](#command-line-arguments) or [configuration file](#configuration-file).

```sh
> ./gnatsd
[37274] 2016/12/15 18:33:12.119961 [INF] Starting nats-server version 0.9.6
[37274] 2016/12/15 18:33:12.120060 [INF] Listening for client connections on 0.0.0.0:4222
[37274] 2016/12/15 18:33:12.120154 [INF] Server is ready
```

The server is started and listening for client connections on port 4222 (the default) from all available interfaces. The logs are displayed to stdout as shown above in the server output.

### Clients

The NATS ecosystem provides a large range of supported and community [clients](http://nats.io/documentation/clients/nats-clients/), including Go, Java, Node, and many more. For the complete up-to-date list, visit the [NATS download site](https://nats.io/download).

### Protocol

The NATS server uses a [text based protocol](http://nats.io/documentation/internals/nats-protocol/), so interacting with it can be as simple as using telnet as shown below. See also the [protocol demo](http://nats.io/documentation/internals/nats-protocol-demo/).

```sh
> telnet demo.nats.io 4222
Trying 107.170.221.32...
Connected to demo.nats.io.
Escape character is '^]'.
INFO {"server_id":"kG19DsXX1UVeSyEjhl3RFw","version":"0.9.6","go":"go1.7.4","host":"0.0.0.0","port":4222, ...}
SUB foo 1
+OK
PUB foo 11
Hello World
+OK
MSG foo 1 11
Hello World
```

## Command line arguments

The NATS server accepts command line arguments to control its behavior. Usage is shown below. Note that command line arguments override those items in the [configuration file](#configuration-file).

```
Server Options:
    -a, --addr <host>                Bind to host address (default: 0.0.0.0)
    -p, --port <port>                Use port for clients (default: 4222)
    -P, --pid <file>                 File to store PID
    -m, --http_port <port>           Use port for http monitoring
    -ms,--https_port <port>          Use port for https monitoring
    -c, --config <file>              Configuration file

Logging Options:
    -l, --log <file>                 File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Enable syslog as log method
    -r, --remote_syslog <addr>       Syslog server addr (udp://localhost:514)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -DV                              Debug and trace

Authorization Options:
        --user <user>                User required for connections
        --pass <password>            Password required for connections
        --auth <token>               Authorization token required for connections

TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert <file>             Server certificate file
        --tlskey <file>              Private key for server certificate
        --tlsverify                  Enable TLS, verify client certificates
        --tlscacert <file>           Client certificate CA for verification

Cluster Options:
        --routes <rurl-1, rurl-2>    Routes to solicit and connect
        --cluster <cluster-url>      Cluster URL for solicited routes
        --no_advertise <bool>        Advertise known cluster IPs to clients


Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help
```

## Configuration file

Typically you configure the NATS server using a configuration file, an example of which is shown below. See also the [server configuration file](http://nats.io/documentation/server/gnatsd-config/) documentation for details on the configuration language.

```
listen: localhost:4242 # host/port to listen for client connections

http: localhost:8222 # HTTP monitoring port

# Authorization for client connections
authorization {
  user:     derek
  # ./util/mkpassword -p T0pS3cr3t
  password: $2a$11$W2zko751KUvVy59mUTWmpOdWjpEm5qhcCZRd05GjI/sSOT.xtiHyG
  timeout:  1
}

# Cluster definition

cluster {

  listen: localhost:4244 # host/port for inbound route connections

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
```

## Variables

The NATS sever configuration language supports block-scoped variables that can be used for templating in the configuration file, and specifically to ease setting of group values for [permission fields](#authorization) and [user authentication](#authentication).

Variables can be referenced by the prefix `$`, for example: `$PASSWORD`. Variables can be defined in the configuration file itself or reference environment variables.

Any value in the configuration language can be a variable reference (`key=$VALUE`). Note that the variable identifier (name) is not case sensitive, but is capitalized by convention for readability.

## Clustering

Clustering lets you scale NATS messaging by having multiple NATS servers communicate with each other. Clustering lets messages published to one server be routed and received by a subscriber on another server. See also the [clustered NATS](http://nats.io/documentation/server/gnatsd-cluster/) documentation.

### Full mesh required

In order for clustering to work correctly, all NATS servers must be connected to each other.

NATS servers have a forwarding limit of one hop. This means that each server will **only** forward a message that it has received **from a client** to  all connected servers that expressed interest in the message's published subject. A message received **from** a route will only be distributed to local clients.

### Configuration options

NATS supports running each server in clustered mode. The following command line options are supported:

    --cluster [cluster url]     Cluster URL for solicited routes
    --routes [rurl-1, rurl-2]   Routes to solicit and connect

The `--cluster` flag specifies the NATS URL where the server listens for connections from other servers.

The `--routes` flag specifies the NATS URL for one or more servers in the cluster. When a server connects to a specified route, it will advertise its own cluster URL to other servers. Note that when the `--routes` option is specified a `--cluster` option is also required.

Previous releases required you to build the complete mesh using the `--routes` flag. To define your cluster in the current release, please follow the "Basic example" as described below.

### Basic example

NATS makes building the full mesh easy. Simply designate a server to be a *seed* server. All other servers in the cluster simply specify the *seed* server as its server's routes option as indicated below.

When running NATS Servers in different hosts, the command line parameters for all servers could be as simple as:

```
gnatsd --cluster nats://$HOSTNAME:$NATS_CLUSTER_PORT --routes nats://$NATS_SEED_HOST:$NATS_CLUSTER_PORT
```

Even on the host where the *seed* is running, the above would work as the server would detect an attempt to connect to itself and ignore that. In other words, the same command line could be deployed in several hosts and the full mesh will properly form.

Note that you don't have to connect all servers to the same *seed* server, any server accepting a connection will inform other servers in the mesh about that new server so that they can connect to it. The advantage of the seed approach, is that you can deploy the same configuration to all hosts.

### 3-node example

The following example demonstrates how to run a cluster of 3 servers on the same host. We will start with the seed server and use the `-D` command line parameter to produce debug information.

See also [clustered NATS](http://nats.io/documentation/server/gnatsd-cluster/) for clustered NATS examples using Docker.

```
gnatsd -p 4222 -cluster nats://localhost:4248 -D
```

Alternatively, you could use a configuration file, let's call it `seed.conf`, with a content similar to this:

```
# Cluster Seed Node

listen: 127.0.0.1:4222
http: 8222

cluster {
  listen: 127.0.0.1:4248
}
```

And start the server like this:

```
gnatsd -config ./seed.conf -D
```

This will produce an output similar to:

```
[75653] 2016/04/26 15:14:47.339321 [INF] Listening for route connections on 127.0.0.1:4248
[75653] 2016/04/26 15:14:47.340787 [INF] Listening for client connections on 127.0.0.1:4222
[75653] 2016/04/26 15:14:47.340822 [DBG] server id is xZfu3u7usAPWkuThomoGzM
[75653] 2016/04/26 15:14:47.340825 [INF] server is ready
```

It is also possible to specify the hostname and port independently. At least the port is required. If you leave the hostname off it will bind to all the interfaces ('0.0.0.0').

```
cluster {
  host: 127.0.0.1
  port: 4248
}
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

This section describes how to secure the NATS server, including authentication, authorization, and encryption using TLS and bcrypt.

### Authentication

The NATS server supports single and multi-user/client authentication. See also the [server authentication](http://nats.io/documentation/server/gnatsd-authentication/) documentation.

**Single user authentication**

For single-user authentication, you can start the NATS server with authentication enabled by passing in the required credentials on the command line, or by passing in a token.

```
gnatsd -DV --user foo --pass bar
```

```
gnatsd -DV -auth 'S3Cr3T0k3n!'
```

Clients can connect using:

```
nats://foo:bar@localhost:4222
```

```
nats://S3Cr3T0k3n!@localhost:4222
```

You can also enable single-user authentication and set the credentials in the server configuration file as follows:

```
authorization {
  user:     derek
  password: T0pS3cr3t
  timeout:  1
}
```

**Multi-user authentication**

You can enable multi-user authentication using a NATS server configuration file that defines user credentials (`user` and `password`), and optionally `permissions`, for two or more users. Multi-user authentication leverages [variables](#variables).

```
authorization {
  users = [
    {user: value or $VARIABLE, password: value or $VARIABLE}
    {user: value or $VARIABLE, password: value or $VARIABLE, [permissions: $PERMISSION]}
    ...
  ]
}
```

For example:

```
authorization {
  PASS: abcdefghijklmnopqrstuvwxwz0123456789
  users = [
    {user: alice, password: foo, permissions: $ADMIN}
    {user: bob,   password: bar, permissions: $REQUESTOR}
    {user: joe,   password: $PASS}
  ]
}
```

### Authorization

The NATS server supports authorization using subject-level permissions on a per-user basis. Permission-based authorization is available with [multi-user authentication](#authentication). See also the [Server Authorization](http://nats.io/documentation/server/gnatsd-authorization) documentation.

Each permission grant is an object with two fields: what subject(s) the authenticated user can publish to, and what subject(s) the authenticated user can subscribe to. The parser is generous at understanding what the intent is, so both arrays and singletons are processed. Subjects themselves can contain wildcards. Permissions make use of [variables](#variables).

You set permissions by creating an entry inside of the `authorization` configuration block that conforms to the following syntax:

```
authorization {
  PERMISSION_NAME = {
    publish = "singleton" or ["array", ...]
    subscribe = "singleton" or ["array", ...]
  }
}
```

Here is an example authorization configuration that defines three users, two of whom are assigned explicit permissions.

```
authorization {
  ADMIN = {
    publish = ">"
    subscribe = ">"
  }
  REQUESTOR = {
    publish = ["req.foo", "req.bar"]
    subscribe = "_INBOX.*"
  }
  DEFAULT_PERMISSIONS = {
    publish = "SANDBOX.*"
    subscribe = ["PUBLIC.>", "_INBOX.>"]
  }

  PASS: abcdefghijklmnopqrstuvwxwz0123456789
  users = [
    {user: alice, password: foo, permissions: $ADMIN}
    {user: bob,   password: bar, permissions: $REQUESTOR}
    {user: joe,   password: $PASS}
  ]
}
```

Since Alice is an ADMIN she can publish/subscribe on any subject. We use the wildcard “>” to match any subject.

Bob is REQUESTOR and can publish requests on subjects "req.foo" or "req.bar", and subscribe to anything that is a response ("_INBOX.*").

Joe has no permission grant and therefore inherits the default permission set. You set the inherited default permissions by assigning them to the `default_permissions` entry inside of the `authorization` configuration block.

Note that `_INBOX.*` subscribe permissions must be granted in order to use the request APIs in Apcera supported clients. If an unauthorized client publishes or attempts to subscribe to a subject, the action fails and is logged at the server, and an error message is returned to the client.

### TLS

As of Release 0.7.0, the server can use modern TLS semantics for client connections, route connections, and the HTTPS monitoring port.
The server requires TLS version 1.2, and sets preferences for modern cipher suites that avoid those known with vulnerabilities. The
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
The curve preferences are also re-ordered to provide the most secure
environment available, and are as follows:
```go
func defaultCurvePreferences() []tls.CurveID {
	return []tls.CurveID{
		tls.CurveP521,
		tls.CurveP384,
		tls.CurveP256,
	}
}
```
Generating self signed certs and intermediary certificate authorities is beyond the scope here, but this document can be helpful in addition to Google Search: <a href="https://docs.docker.com/engine/articles/https/" target="_blank">https://docs.docker.com/engine/articles/https/</a>.

The server **requires** a certificate and private key. Optionally the server can require that clients need to present certificates, and the server can be configured with a CA authority to verify the client certificates.

```
# Simple TLS config file

listen: 127.0.0.1:4443

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

When setting up clusters, all servers in the cluster, if using TLS, will both verify the connecting endpoints and the server responses. So certificates are checked in both directions. Certificates can be configured only for the server's cluster identity, keeping client and server certificates separate from cluster formation.

```
cluster {
  listen: 127.0.0.1:4244

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
--tlsverify                  Enable TLS, verify client certificates
--tlscacert FILE             Client certificate CA for verification
```

Examples using the test certificates which are self signed for localhost and 127.0.0.1.

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

If the monitoring port is enabled, the NATS server runs a lightweight HTTP server that has the following endpoints: /varz, /connz, /routez, and /subsz. All endpoints return a JSON object. See [NATS Server monitoring](http://nats.io/documentation/server/gnatsd-monitoring/) for endpoint examples.

To see a demonstration of NATS monitoring, run a command similar to the following for each desired endpoint:

```
curl demo.nats.io:8222/varz
```

To enable the monitoring server, start the NATS server with the monitoring flag `-m` (or `-ms`) and specify the monitoring port.

Monitoring options

    -m, --http_port PORT             HTTP PORT for monitoring
    -ms,--https_port PORT            Use HTTPS PORT for monitoring (requires TLS cert and key)

To enable monitoring via the configuration file, use `host:port` (there is no explicit configuration flag for the monitoring interface).

For example, running the `gnatsd -m 8222` command, you should see that the NATS server starts with the HTTP monitoring port enabled. To view the monitoring home page, go to <a href="http://localhost:8222/" target="_blank">http://localhost:8222/</a>.

```
[83249] 2016/06/23 19:39:35.173557 [INF] Starting nats-server version 0.8.0
[83249] 2016/06/23 19:39:35.173835 [INF] Starting http monitor on 0.0.0.0:8222
[83249] 2016/06/23 19:39:35.175193 [INF] Listening for client connections on 0.0.0.0:4222
[83249] 2016/06/23 19:39:35.175226 [INF] Server is ready
```

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
[License-Image]: https://img.shields.io/badge/License-MIT-blue.svg
[Build-Status-Url]: http://travis-ci.org/nats-io/gnatsd
[Build-Status-Image]: https://travis-ci.org/nats-io/gnatsd.svg?branch=master
[Release-Url]: https://github.com/nats-io/gnatsd/releases/tag/v0.9.6
[Release-image]: http://img.shields.io/badge/release-v0.9.6-1eb0fc.svg
[Coverage-Url]: https://coveralls.io/r/nats-io/gnatsd?branch=master
[Coverage-image]: https://coveralls.io/repos/github/nats-io/gnatsd/badge.svg?branch=master
[ReportCard-Url]: http://goreportcard.com/report/nats-io/gnatsd
[ReportCard-Image]: http://goreportcard.com/badge/github.com/nats-io/gnatsd
[github-release]: https://github.com/nats-io/gnatsd/releases/
