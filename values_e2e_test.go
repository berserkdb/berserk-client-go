// Validates that every BqlValue oneof arm in proto/dynamic_value.proto
// decodes through the real GRPCClient against a live cluster — one `print`
// query produces a column per value type, and each decoded cell is
// asserted. Mirrors values_e2e in the rust/node/python clients.
//
// Configure via the same env as e2e_test.go (BERSERK_ENDPOINT, and for the
// gateway BERSERK_TOKEN / BERSERK_GRPC_PREFIX).
package berserk

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestE2E_GRPC_ValueTypes(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewGRPCClient(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	const guid = "74be27de-1e4e-49d9-b579-fe0b331d3642"
	// The server emits datetimes as nanoseconds since the Unix epoch and
	// timespans as 100ns ticks. Go's int64 holds both exactly (unlike the
	// JS client, which loses sub-microsecond datetime precision).
	dtUnixNanos := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC).UnixNano()
	const ts1hTicks int64 = 3600 * 10_000_000

	// One column per BqlValue oneof arm, plus in-oneof default values
	// (false / 0 / "") whose proto3 oneof presence must stay
	// distinguishable from null.
	query := `print b = true,
  f = false,
  i = toint(42),
  l = tolong(1234567890123),
  z = tolong(0),
  r = 3.14,
  s = "hello",
  es = "",
  dt = todatetime("2024-01-15T10:30:00Z"),
  ts = 1h,
  g = toguid("` + guid + `"),
  arr = dynamic([1, "two", true]),
  bag = dynamic({"a": 1, "nested": {"c": false}}),
  n = toint("not-a-number")`

	cases := []struct {
		name     string
		wantType ColumnType
		wantVal  Value
	}{
		{"b", ColumnTypeBool, true},
		{"f", ColumnTypeBool, false},
		{"i", ColumnTypeInt, int64(42)},
		{"l", ColumnTypeLong, int64(1234567890123)},
		{"z", ColumnTypeLong, int64(0)},
		{"r", ColumnTypeReal, 3.14},
		{"s", ColumnTypeString, "hello"},
		{"es", ColumnTypeString, ""},
		{"dt", ColumnTypeDatetime, dtUnixNanos},
		{"ts", ColumnTypeTimespan, ts1hTicks},
		// The proto enum has COLUMN_TYPE_GUID, but the engine reports
		// guid-typed expressions as string columns (values arrive on the
		// string arm). Flip to ColumnTypeGuid if the server ever emits it.
		{"g", ColumnTypeString, guid},
		{"arr", ColumnTypeDynamic, []Value{int64(1), "two", true}},
		{"bag", ColumnTypeDynamic, map[string]Value{"a": int64(1), "nested": map[string]Value{"c": false}}},
		{"n", ColumnTypeInt, nil},
	}

	resp, err := client.Query(ctx, query, "", "", "UTC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Tables) == 0 {
		t.Fatal("no result table")
	}
	table := resp.Tables[0]
	if len(table.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(table.Rows))
	}
	row := table.Rows[0]

	colIndex := make(map[string]int, len(table.Columns))
	for i, col := range table.Columns {
		colIndex[col.Name] = i
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx, ok := colIndex[tc.name]
			if !ok {
				t.Fatalf("column %q missing from schema", tc.name)
			}
			if got := table.Columns[idx].Type; got != tc.wantType {
				t.Errorf("column type = %q, want %q", got, tc.wantType)
			}
			if got := row[idx]; !reflect.DeepEqual(got, tc.wantVal) {
				t.Errorf("decoded value = %#v, want %#v", got, tc.wantVal)
			}
		})
	}
}
