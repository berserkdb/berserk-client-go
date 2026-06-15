# berserk-client-go

Go client library for the [Berserk](https://berserk.dev) observability platform.

Clients connect to the **gateway** — the authenticated public edge — using a
bearer token (a CLI access token from the device flow, or a service-principal
token). The gateway authenticates the call and injects the trusted identity
before forwarding to the query service. The gRPC surface is mounted under
`/api/grpc`; the client applies that prefix by default (set
`config.GRPCPathPrefix = ""` to connect directly to a query service in dev).

## Installation

```bash
go get github.com/berserkdb/berserk-client-go
```

## Quick Start

### gRPC

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	berserk "github.com/berserkdb/berserk-client-go"
)

func main() {
	ctx := context.Background()
	config := berserk.DefaultConfig("https://berserk.example.com")
	config.Token = os.Getenv("BERSERK_TOKEN")
	client, err := berserk.NewGRPCClient(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	resp, err := client.Query(ctx, "Logs | where severity == 'error' | take 10", "", "", "UTC")
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range resp.Tables {
		fmt.Printf("Table: %s (%d rows)\n", table.Name, len(table.Rows))
	}
}
```

### HTTP (ADX v2)

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	berserk "github.com/berserkdb/berserk-client-go"
)

func main() {
	ctx := context.Background()
	config := berserk.DefaultConfig("https://berserk.example.com")
	config.Token = os.Getenv("BERSERK_TOKEN")
	client := berserk.NewHTTPClient(config)

	resp, err := client.Query(ctx, "print v = 1")
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range resp.Tables {
		fmt.Printf("Table: %s (%d rows)\n", table.Name, len(table.Rows))
	}
}
```

## Proto Code Generation

Proto stubs are checked in. To regenerate (output paths come from each
proto's `go_package`, so use module mode):

```bash
protoc -Iproto \
  --go_out=. --go_opt=module=github.com/berserkdb/berserk-client-go \
  --go-grpc_out=. --go-grpc_opt=module=github.com/berserkdb/berserk-client-go \
  query.proto common_api.proto dynamic_value.proto
```

## License

Apache-2.0
