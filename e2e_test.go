// End-to-end tests against a live Berserk cluster.
// Set BERSERK_ENDPOINT to run (e.g., BERSERK_ENDPOINT=http://localhost:9510).
//
// To run through a gateway (the authenticated public edge) instead of
// directly against the query service:
//
//	BERSERK_TOKEN        bearer token (gateway device flow / service token)
//	BERSERK_GRPC_PREFIX  path prefix the gateway mounts gRPC under
//	                     (defaults to /api/grpc; set "" for direct mode)
package berserk

import (
	"context"
	"os"
	"testing"
	"time"
)

// testConfig builds a Config from the environment, skipping the test when
// no endpoint is set. BERSERK_GRPC_PREFIX overrides the default gateway
// prefix when present (set it to "" to talk to a query service directly).
func testConfig(t *testing.T) Config {
	t.Helper()
	ep := os.Getenv("BERSERK_ENDPOINT")
	if ep == "" {
		t.Skip("BERSERK_ENDPOINT not set, skipping e2e test")
	}
	cfg := DefaultConfig(ep)
	cfg.Token = os.Getenv("BERSERK_TOKEN")
	if prefix, ok := os.LookupEnv("BERSERK_GRPC_PREFIX"); ok {
		cfg.GRPCPathPrefix = prefix
	}
	return cfg
}

func TestE2E_GRPC_SimpleQuery(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	resp, err := client.Query(ctx, "print v = 1", "", "", "UTC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(resp.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(resp.Tables))
	}
	table := resp.Tables[0]
	if len(table.Columns) != 1 || table.Columns[0].Name != "v" {
		t.Fatalf("unexpected columns: %+v", table.Columns)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(table.Rows))
	}
}

func TestE2E_GRPC_InvalidQuery(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	_, err = client.Query(ctx, "this is not valid kql!!!", "", "", "UTC")
	if err == nil {
		t.Fatal("expected error for invalid query")
	}
}

func TestE2E_GRPC_MultiColumn(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	resp, err := client.Query(ctx, `print a = 1, b = "hello", c = true`, "", "", "UTC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	table := resp.Tables[0]
	if len(table.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(table.Columns))
	}
	if table.Columns[0].Name != "a" || table.Columns[1].Name != "b" || table.Columns[2].Name != "c" {
		t.Fatalf("unexpected column names: %+v", table.Columns)
	}
}

func TestE2E_HTTP_SimpleQuery(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewHTTPClient(cfg)
	resp, err := client.Query(ctx, "print v = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(resp.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(resp.Tables))
	}
	if len(resp.Tables[0].Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Tables[0].Rows))
	}
}

func TestE2E_HTTP_InvalidQuery(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewHTTPClient(cfg)
	_, err := client.Query(ctx, "this is not valid kql!!!")
	if err == nil {
		t.Fatal("expected error for invalid query")
	}
}
