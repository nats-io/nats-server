package websocket

// ClosedState is the reason client was closed. This will
// be passed into calls to clearConnection, but will only
// be stored in ConnInfo for monitoring.
// Duplicated version in the server ClosedState one.
type ClosedState int

const (
	ClientClosed = ClosedState(iota + 1)
	AuthenticationTimeout
	AuthenticationViolation
	TLSHandshakeError
	SlowConsumerPendingBytes
	SlowConsumerWriteDeadline
	WriteError
	ReadError
	ParseError
	StaleConnection
	ProtocolViolation
	BadClientProtocolVersion
	WrongPort
	MaxAccountConnectionsExceeded
	MaxConnectionsExceeded
	MaxPayloadExceeded
	MaxControlLineExceeded
	MaxSubscriptionsExceeded
	DuplicateRoute
	RouteRemoved
	ServerShutdown
	AuthenticationExpired
	WrongGateway
	MissingAccount
	Revocation
	InternalClient
	MsgHeaderViolation
	NoRespondersRequiresHeaders
	ClusterNameConflict
	DuplicateRemoteLeafnodeConnection
	DuplicateClientID
	DuplicateServerName
	MinimumVersionRequired
)

func (reason ClosedState) String() string {
	switch reason {
	case ClientClosed:
		return "Client Closed"
	case AuthenticationTimeout:
		return "Authentication Timeout"
	case AuthenticationViolation:
		return "Authentication Failure"
	case TLSHandshakeError:
		return "TLS Handshake Failure"
	case SlowConsumerPendingBytes:
		return "Slow Consumer (Pending Bytes)"
	case SlowConsumerWriteDeadline:
		return "Slow Consumer (Write Deadline)"
	case WriteError:
		return "Write Error"
	case ReadError:
		return "Read Error"
	case ParseError:
		return "Parse Error"
	case StaleConnection:
		return "Stale Connection"
	case ProtocolViolation:
		return "Protocol Violation"
	case BadClientProtocolVersion:
		return "Bad Client Protocol Version"
	case WrongPort:
		return "Incorrect Port"
	case MaxConnectionsExceeded:
		return "Maximum Connections Exceeded"
	case MaxAccountConnectionsExceeded:
		return "Maximum Account Connections Exceeded"
	case MaxPayloadExceeded:
		return "Maximum Message Payload Exceeded"
	case MaxControlLineExceeded:
		return "Maximum Control Line Exceeded"
	case MaxSubscriptionsExceeded:
		return "Maximum Subscriptions Exceeded"
	case DuplicateRoute:
		return "Duplicate Route"
	case RouteRemoved:
		return "Route Removed"
	case ServerShutdown:
		return "Server Shutdown"
	case AuthenticationExpired:
		return "Authentication Expired"
	case WrongGateway:
		return "Wrong Gateway"
	case MissingAccount:
		return "Missing Account"
	case Revocation:
		return "Credentials Revoked"
	case InternalClient:
		return "Internal Client"
	case MsgHeaderViolation:
		return "Message Header Violation"
	case NoRespondersRequiresHeaders:
		return "No Responders Requires Headers"
	case ClusterNameConflict:
		return "Cluster Name Conflict"
	case DuplicateRemoteLeafnodeConnection:
		return "Duplicate Remote LeafNode Connection"
	case DuplicateClientID:
		return "Duplicate Client ID"
	case DuplicateServerName:
		return "Duplicate Server Name"
	case MinimumVersionRequired:
		return "Minimum Version Required"
	}

	return "Unknown State"
}
