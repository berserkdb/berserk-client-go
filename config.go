package berserk

import (
	"strings"
	"time"
)

// DefaultGRPCPathPrefix is the path the gateway mounts its gRPC proxy
// under. Clients reaching Berserk through the gateway prepend it to every
// method; set Config.GRPCPathPrefix to "" to connect directly to a query
// service (in-cluster / dev).
const DefaultGRPCPathPrefix = "/api/grpc"

// Config holds client configuration for connecting to a Berserk gateway.
type Config struct {
	// Endpoint is the gateway address (e.g., "https://berserk.example.com"
	// or "http://localhost:9500").
	Endpoint string

	// Token is the bearer token sent as `authorization: Bearer` on every
	// call — a CLI access token or a service-principal token. The gateway
	// rejects unauthenticated calls and injects the trusted identity after
	// authenticating the caller.
	Token string

	// GRPCPathPrefix is the path prefix the gateway mounts the gRPC
	// surface under. Defaults to "/api/grpc"; set to "" for a direct
	// in-cluster query service.
	GRPCPathPrefix string

	// Timeout is the maximum time for a complete request.
	Timeout time.Duration

	// ConnectTimeout is the connection timeout.
	ConnectTimeout time.Duration

	// Database to resolve unqualified table names against. Sent on every
	// ExecuteQueryRequest as `database.name`. Defaults to "default".
	Database string
}

// DefaultConfig returns a Config with sensible defaults for the given
// gateway endpoint.
func DefaultConfig(endpoint string) Config {
	return Config{
		Endpoint:       endpoint,
		GRPCPathPrefix: DefaultGRPCPathPrefix,
		Timeout:        30 * time.Second,
		ConnectTimeout: 10 * time.Second,
		Database:       "default",
	}
}

// NormalizedEndpoint ensures the endpoint has an HTTP scheme.
func (c Config) NormalizedEndpoint() string {
	if strings.HasPrefix(c.Endpoint, "http://") || strings.HasPrefix(c.Endpoint, "https://") {
		return c.Endpoint
	}
	return "http://" + c.Endpoint
}

// GRPCTarget returns the endpoint stripped of any HTTP scheme prefix, suitable for gRPC Dial.
func (c Config) GRPCTarget() string {
	ep := c.Endpoint
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}

// useTLS reports whether the gRPC connection should use transport
// security, derived from an https endpoint.
func (c Config) useTLS() bool {
	return strings.HasPrefix(c.Endpoint, "https://")
}

// DatabaseOrDefault returns the configured database, or "default".
func (c Config) DatabaseOrDefault() string {
	if c.Database == "" {
		return "default"
	}
	return c.Database
}
