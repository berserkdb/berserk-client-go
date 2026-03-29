package berserk

import (
	"context"
	"fmt"
	"io"

	querypb "github.com/berserkdb/berserk-client-go/proto/querypb"
	berserkpb "github.com/berserkdb/berserk-client-go/proto/berserkpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPCClient is a gRPC client for the Berserk query service.
type GRPCClient struct {
	config Config
	conn   *grpc.ClientConn
	client querypb.QueryServiceClient
}

// NewGRPCClient creates a new gRPC client.
func NewGRPCClient(ctx context.Context, config Config) (*GRPCClient, error) {
	target := config.GRPCTarget()
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("berserk: failed to connect: %w", err)
	}

	return &GRPCClient{
		config: config,
		conn:   conn,
		client: querypb.NewQueryServiceClient(conn),
	}, nil
}

// Query executes a query and collects all results.
func (c *GRPCClient) Query(ctx context.Context, query string, since, until, timezone string) (*QueryResponse, error) {
	if timezone == "" {
		timezone = "UTC"
	}

	md := metadata.New(nil)
	if c.config.Username != "" {
		md.Set("x-bzrk-username", c.config.Username)
	}
	if c.config.ClientName != "" {
		md.Set("x-bzrk-client-name", c.config.ClientName)
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	if c.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()
	}

	stream, err := c.client.ExecuteQuery(ctx, &querypb.ExecuteQueryRequest{
		Query:    query,
		Since:    since,
		Until:    until,
		Timezone: timezone,
	})
	if err != nil {
		return nil, fmt.Errorf("berserk: execute query: %w", err)
	}

	var (
		tables          []Table
		currentName     string
		currentColumns  []Column
		currentRows     [][]Value
		hasSchema       bool
		stats           *ExecutionStats
		warnings        []QueryWarning
		partialFailures []PartialFailure
		visualization   *VisualizationMetadata
	)

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("berserk: stream error: %w", err)
		}

		switch p := frame.Payload.(type) {
		case *querypb.ExecuteQueryResultFrame_Schema:
			if hasSchema {
				tables = append(tables, Table{
					Name:    currentName,
					Columns: currentColumns,
					Rows:    currentRows,
				})
				currentRows = nil
			}
			hasSchema = true
			currentName = p.Schema.Name
			currentColumns = make([]Column, len(p.Schema.Columns))
			for i, col := range p.Schema.Columns {
				currentColumns[i] = Column{
					Name: col.Name,
					Type: convertColumnType(querypb.ColumnType(col.Type)),
				}
			}

		case *querypb.ExecuteQueryResultFrame_Batch:
			for _, row := range p.Batch.Rows {
				values := make([]Value, len(row.Values))
				for i, v := range row.Values {
					values[i] = convertValue(v)
				}
				currentRows = append(currentRows, values)
			}

		case *querypb.ExecuteQueryResultFrame_Progress:
			stats = &ExecutionStats{
				RowsProcessed:      p.Progress.RowsProcessed,
				ChunksTotal:        p.Progress.ChunksTotal,
				ChunksScanned:      p.Progress.ChunksScanned,
				QueryTimeNanos:     p.Progress.QueryTimeNanos,
				ChunkScanTimeNanos: p.Progress.ChunkScanTimeNanos,
			}

		case *querypb.ExecuteQueryResultFrame_Error:
			return nil, fmt.Errorf("berserk: query error [%s]: %s", p.Error.Code, p.Error.Message)

		case *querypb.ExecuteQueryResultFrame_Metadata:
			for _, pf := range p.Metadata.PartialFailures {
				partialFailures = append(partialFailures, PartialFailure{
					SegmentIDs: pf.SegmentIds,
					Message:    pf.Message,
				})
			}
			for _, w := range p.Metadata.Warnings {
				warnings = append(warnings, QueryWarning{
					Kind:    w.Kind,
					Message: w.Message,
				})
			}
			if p.Metadata.Visualization != nil && p.Metadata.Visualization.VisualizationType != nil {
				visualization = &VisualizationMetadata{
					VisualizationType: *p.Metadata.Visualization.VisualizationType,
					Properties:        p.Metadata.Visualization.Properties,
				}
			}

		case *querypb.ExecuteQueryResultFrame_Done:
			goto done
		}
	}

done:
	if hasSchema {
		tables = append(tables, Table{
			Name:    currentName,
			Columns: currentColumns,
			Rows:    currentRows,
		})
	}

	return &QueryResponse{
		Tables:          tables,
		Stats:           stats,
		Warnings:        warnings,
		PartialFailures: partialFailures,
		Visualization:   visualization,
	}, nil
}

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func convertColumnType(ct querypb.ColumnType) ColumnType {
	switch ct {
	case querypb.ColumnType_COLUMN_TYPE_BOOL:
		return ColumnTypeBool
	case querypb.ColumnType_COLUMN_TYPE_INT:
		return ColumnTypeInt
	case querypb.ColumnType_COLUMN_TYPE_LONG:
		return ColumnTypeLong
	case querypb.ColumnType_COLUMN_TYPE_REAL:
		return ColumnTypeReal
	case querypb.ColumnType_COLUMN_TYPE_STRING:
		return ColumnTypeString
	case querypb.ColumnType_COLUMN_TYPE_DATETIME:
		return ColumnTypeDatetime
	case querypb.ColumnType_COLUMN_TYPE_TIMESPAN:
		return ColumnTypeTimespan
	case querypb.ColumnType_COLUMN_TYPE_GUID:
		return ColumnTypeGuid
	default:
		return ColumnTypeDynamic
	}
}

func convertValue(dyn *berserkpb.BqlValue) Value {
	if dyn == nil {
		return nil
	}
	switch v := dyn.Value.(type) {
	case *berserkpb.BqlValue_NullValue:
		return nil
	case *berserkpb.BqlValue_BoolValue:
		return v.BoolValue
	case *berserkpb.BqlValue_IntValue:
		return int64(v.IntValue)
	case *berserkpb.BqlValue_LongValue:
		return v.LongValue
	case *berserkpb.BqlValue_RealValue:
		return v.RealValue
	case *berserkpb.BqlValue_StringValue:
		return v.StringValue
	case *berserkpb.BqlValue_DatetimeValue:
		return int64(v.DatetimeValue)
	case *berserkpb.BqlValue_TimespanValue:
		return int64(v.TimespanValue)
	case *berserkpb.BqlValue_ArrayValue:
		if v.ArrayValue == nil {
			return []Value{}
		}
		result := make([]Value, len(v.ArrayValue.Values))
		for i, elem := range v.ArrayValue.Values {
			result[i] = convertValue(elem)
		}
		return result
	case *berserkpb.BqlValue_BagValue:
		if v.BagValue == nil {
			return map[string]Value{}
		}
		result := make(map[string]Value, len(v.BagValue.Properties))
		for k, val := range v.BagValue.Properties {
			result[k] = convertValue(val)
		}
		return result
	default:
		return nil
	}
}
