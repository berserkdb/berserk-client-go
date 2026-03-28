package berserk

// QueryResponse holds the complete result of a query.
type QueryResponse struct {
	Tables          []Table                `json:"tables"`
	Stats           *ExecutionStats        `json:"stats,omitempty"`
	Warnings        []QueryWarning         `json:"warnings,omitempty"`
	PartialFailures []PartialFailure       `json:"partial_failures,omitempty"`
	Visualization   *VisualizationMetadata `json:"visualization,omitempty"`
}

// Table is a result table with schema and rows.
type Table struct {
	Name    string    `json:"name"`
	Columns []Column  `json:"columns"`
	Rows    [][]Value `json:"rows"`
}

// Column describes a column in a result table.
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// ColumnType represents the data type of a column.
type ColumnType string

const (
	ColumnTypeBool     ColumnType = "bool"
	ColumnTypeInt      ColumnType = "int"
	ColumnTypeLong     ColumnType = "long"
	ColumnTypeReal     ColumnType = "real"
	ColumnTypeString   ColumnType = "string"
	ColumnTypeDatetime ColumnType = "datetime"
	ColumnTypeTimespan ColumnType = "timespan"
	ColumnTypeGuid     ColumnType = "guid"
	ColumnTypeDynamic  ColumnType = "dynamic"
)

// Value is a dynamic value from query results.
// It can be nil, bool, int64, float64, string, []Value, or map[string]Value.
type Value interface{}

// ExecutionStats holds query execution statistics.
type ExecutionStats struct {
	RowsProcessed      uint64 `json:"rows_processed"`
	ChunksTotal        uint64 `json:"chunks_total"`
	ChunksScanned      uint64 `json:"chunks_scanned"`
	QueryTimeNanos     uint64 `json:"query_time_nanos"`
	ChunkScanTimeNanos uint64 `json:"chunk_scan_time_nanos"`
}

// QueryWarning is a warning from query execution.
type QueryWarning struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// PartialFailure describes segments that couldn't be read.
type PartialFailure struct {
	SegmentIDs []string `json:"segment_ids"`
	Message    string   `json:"message"`
}

// VisualizationMetadata from the render operator.
type VisualizationMetadata struct {
	VisualizationType string            `json:"visualization_type"`
	Properties        map[string]string `json:"properties"`
}
