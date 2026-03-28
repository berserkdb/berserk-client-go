package berserk

import (
	"strings"
	"time"
)

// Config holds client configuration for connecting to a Berserk query service.
type Config struct {
	// Endpoint is the query service address (e.g., "localhost:9510" for gRPC, "http://localhost:9510" for HTTP).
	Endpoint string

	// Username sent as x-bzrk-username header.
	Username string

	// Timeout is the maximum time for a complete request.
	Timeout time.Duration

	// ConnectTimeout is the connection timeout.
	ConnectTimeout time.Duration

	// ClientName sent as x-bzrk-client-name header.
	ClientName string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(endpoint string) Config {
	return Config{
		Endpoint:       endpoint,
		Timeout:        30 * time.Second,
		ConnectTimeout: 10 * time.Second,
		ClientName:     "berserk-client-go",
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
