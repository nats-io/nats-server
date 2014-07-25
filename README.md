# Gnatsd [![Build Status](https://travis-ci.org/apcera/gnatsd.svg?branch=master)](http://travis-ci.org/apcera/gnatsd) [![Coverage Status](https://img.shields.io/coveralls/apcera/gnatsd.svg)](https://coveralls.io/r/apcera/gnatsd)

A High Performance [NATS](https://github.com/derekcollison/nats) Server written in [Go.](http://golang.org)

## Usage

```

Server options:
    -a, --addr HOST                  Bind to HOST address (default: 0.0.0.0)
    -p, --port PORT                  Use PORT (default: 4222)
    -P, --pid FILE                   File to store PID
    -m, --http_port PORT             Use HTTP PORT
    -c, --config FILE                Configuration File

Logging options:
    -l, --log FILE                   File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol

Authorization options:
        --user user                  User required for connections
        --pass password              Password required for connections

Common options:
    -h, --help                       Show this message
    -v, --version                    Show version

```

## Configuration

```
# Sample config file

port: 4242
net: apcera.me # net interface

http_port: 8222

authorization {
  user:     derek
  password: T0pS3cr3t
  timeout:  1
}

cluster {
  host: '127.0.0.1'
  port: 4244

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

#pid file
pid_file: "/tmp/gnatsd.pid"
```


## Nats client libraries

There are several client language bindings.
- [Go](https://github.com/apcera/nats)
- [Java](https://github.com/tyagihas/java_nats)
- [Java - Spring](https://github.com/mheath/jnats)
- [Node.js](https://github.com/derekcollison/node_nats)
- [Ruby](https://github.com/derekcollison/nats)


## License

(The MIT License)

Copyright (c) 2012-2014 Apcera Inc.

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
