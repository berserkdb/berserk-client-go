package berserk

import (
	"context"
	"os"
	"testing"
	"time"
)

func endpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("BERSERK_ENDPOINT")
	if ep == "" {
		t.Skip("BERSERK_ENDPOINT not set, skipping e2e test")
	}
	return ep
}

func TestE2E_GRPC_SimpleQuery(t *testing.T) {
	ep := endpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, DefaultConfig(ep))
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
	ep := endpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, DefaultConfig(ep))
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
	ep := endpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, DefaultConfig(ep))
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
	ep := endpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewHTTPClient(DefaultConfig(ep))
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
	ep := endpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewHTTPClient(DefaultConfig(ep))
	_, err := client.Query(ctx, "this is not valid kql!!!")
	if err == nil {
		t.Fatal("expected error for invalid query")
	}
}
