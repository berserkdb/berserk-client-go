package berserk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// HTTPClient is an HTTP client for the Berserk ADX v2 REST endpoint.
type HTTPClient struct {
	config     Config
	httpClient *http.Client
}

// NewHTTPClient creates a new HTTP client.
func NewHTTPClient(config Config) *HTTPClient {
	return &HTTPClient{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

type kustoV2Request struct {
	CSL string `json:"csl"`
}

type v2Frame struct {
	FrameType string            `json:"FrameType"`
	TableKind string            `json:"TableKind"`
	TableName string            `json:"TableName"`
	Columns   []v2Column        `json:"Columns"`
	Rows      []json.RawMessage `json:"Rows"`
	HasErrors bool              `json:"HasErrors"`
}

type v2Column struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

// Query executes a query via the ADX v2 REST endpoint.
func (c *HTTPClient) Query(ctx context.Context, query string) (*QueryResponse, error) {
	endpoint := c.config.NormalizedEndpoint()
	url := endpoint + "/v2/rest/query"

	body, err := json.Marshal(kustoV2Request{CSL: query})
	if err != nil {
		return nil, fmt.Errorf("berserk: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("berserk: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.config.Username != "" {
		req.Header.Set("x-bzrk-username", c.config.Username)
	}
	if c.config.ClientName != "" {
		req.Header.Set("x-bzrk-client-name", c.config.ClientName)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("berserk: HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBody []byte
		errBody, _ = io.ReadAll(resp.Body)
		return nil, fmt.Errorf("berserk: HTTP %d: %s", resp.StatusCode, string(errBody))
	}

	var frames []v2Frame
	if err := json.NewDecoder(resp.Body).Decode(&frames); err != nil {
		return nil, fmt.Errorf("berserk: decode response: %w", err)
	}

	var tables []Table
	hasErrors := false

	for _, frame := range frames {
		if frame.FrameType == "DataTable" && frame.TableKind == "PrimaryResult" {
			columns := make([]Column, len(frame.Columns))
			for i, col := range frame.Columns {
				columns[i] = Column{
					Name: col.ColumnName,
					Type: parseColumnType(col.ColumnType),
				}
			}

			var rows [][]Value
			for _, rawRow := range frame.Rows {
				var jsonRow []interface{}
				if err := json.Unmarshal(rawRow, &jsonRow); err != nil {
					continue
				}
				row := make([]Value, len(jsonRow))
				for i, v := range jsonRow {
					row[i] = v
				}
				rows = append(rows, row)
			}

			tables = append(tables, Table{
				Name:    frame.TableName,
				Columns: columns,
				Rows:    rows,
			})
		} else if frame.FrameType == "DataSetCompletion" {
			hasErrors = frame.HasErrors
		}
	}

	if hasErrors {
		return nil, fmt.Errorf("berserk: query completed with errors")
	}

	return &QueryResponse{Tables: tables}, nil
}

func parseColumnType(s string) ColumnType {
	switch s {
	case "bool":
		return ColumnTypeBool
	case "int":
		return ColumnTypeInt
	case "long":
		return ColumnTypeLong
	case "real", "double":
		return ColumnTypeReal
	case "string":
		return ColumnTypeString
	case "datetime":
		return ColumnTypeDatetime
	case "timespan":
		return ColumnTypeTimespan
	case "guid", "uuid":
		return ColumnTypeGuid
	default:
		return ColumnTypeDynamic
	}
}
