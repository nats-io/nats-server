# NATS Server WebSocket HTTP Proxy Support Implementation

## Problem Analysis

The NATS server's WebSocket leaf node connections currently do not support HTTP forward proxies because:

1. **Direct TCP Connection**: The current implementation uses `natsDialTimeout("tcp", url, dialTimeout)` to establish direct TCP connections, bypassing proxy infrastructure.

2. **No HTTP CONNECT Tunnel**: Forward HTTP proxies require clients to establish an HTTP CONNECT tunnel before performing the WebSocket upgrade handshake.

3. **Missing Proxy Configuration**: The `RemoteLeafOpts` structure lacks proxy configuration options.

## Solution Implementation

### 1. Enhanced Configuration Structure

Added proxy configuration to `RemoteLeafOpts` in `server/opts.go`:

```go
// HTTP Proxy configuration for WebSocket connections
Proxy struct {
    // URL of the HTTP proxy server (e.g., "http://proxy.example.com:8080")
    URL string `json:"-"`
    // Username for proxy authentication
    Username string `json:"-"`
    // Password for proxy authentication  
    Password string `json:"-"`
    // Connect timeout for proxy connection
    ConnectTimeout time.Duration `json:"-"`
}
```

### 2. HTTP CONNECT Tunnel Implementation

Added two new functions in `server/leafnode.go`:

- `establishHTTPProxyTunnel()`: Establishes an HTTP CONNECT tunnel through the proxy
- `connectThroughProxy()`: Server method that parses proxy configuration and establishes the tunnel

The tunnel establishment process:
1. Connect to proxy server via TCP
2. Send HTTP CONNECT request with target host
3. Handle proxy authentication (Basic Auth)
4. Verify 200 OK response from proxy
5. Return established tunnel connection

### 3. Enhanced Connection Logic

Modified the leaf node connection establishment in `connectToRemoteLeafNode()`:

```go
// Check if this is a WebSocket connection that needs proxy support
isWebSocket := rURL.Scheme == "ws" || rURL.Scheme == "wss"
if isWebSocket && remote.Proxy.URL != "" {
    // Use proxy for WebSocket connections
    conn, err = s.connectThroughProxy(remote, rURL.Host, dialTimeout)
} else {
    // Direct connection (normal case)
    conn, err = natsDialTimeout("tcp", url, dialTimeout)
}
```

### 4. Configuration Parsing

Added proxy configuration parsing in `parseLeafNodes()` function:

```go
case "proxy":
    // Parse proxy configuration
    if proxyConfig, ok := v.(map[string]any); ok {
        for pk, pv := range proxyConfig {
            switch strings.ToLower(pk) {
            case "url":
                remote.Proxy.URL = pv.(string)
            case "username", "user":
                remote.Proxy.Username = pv.(string)
            case "password", "pass":
                remote.Proxy.Password = pv.(string)
            case "connect_timeout", "timeout":
                remote.Proxy.ConnectTimeout = parseDuration("proxy connect_timeout", tk, pv, errors, warnings)
            }
        }
    }
```

### 5. Validation

Added `validateLeafNodeProxyOptions()` function that:
- Validates proxy URL format
- Ensures proxy scheme is HTTP/HTTPS
- Verifies proxy is only used with WebSocket URLs (ws:// or wss://)

## Configuration Example

```conf
leafnodes {
    remotes [
        {
            urls: ["wss://remote-nats-server.example.com/leafnode"]
            
            proxy {
                url: "http://proxy.company.com:8080"
                username: "proxy_user"
                password: "proxy_pass"
                connect_timeout: "30s"
            }
            
            ws_compression: true
            credentials: "/path/to/nats-leaf.creds"
        }
    ]
}
```

## Security Considerations

1. **Authentication**: Supports HTTP Basic Authentication for proxy access
2. **TLS**: Works with both HTTP and HTTPS proxies
3. **Timeouts**: Configurable connection timeouts prevent hanging connections
4. **Validation**: Strict validation ensures proxy is only used with appropriate URLs

## Benefits

1. **Corporate Network Support**: Enables NATS WebSocket leaf nodes in corporate environments with mandatory HTTP proxies
2. **Backwards Compatibility**: Direct connections continue to work unchanged
3. **Flexible Configuration**: Per-remote proxy configuration allows mixed environments
4. **Robust Error Handling**: Clear error messages for troubleshooting proxy issues

## Testing Recommendations

1. Test with various proxy servers (Squid, corporate proxies, etc.)
2. Verify authentication mechanisms
3. Test connection timeouts and error scenarios
4. Ensure fallback to direct connections works properly
5. Test with both ws:// and wss:// URLs through proxies

This implementation provides comprehensive HTTP proxy support for NATS WebSocket leaf node connections while maintaining backwards compatibility and security best practices.
