# berserk-client-go

Go client library for the [Berserk](https://berserk.dev) observability platform.

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

	berserk "github.com/berserkdb/berserk-client-go"
)

func main() {
	ctx := context.Background()
	client, err := berserk.NewGRPCClient(ctx, berserk.DefaultConfig("localhost:9510"))
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

	berserk "github.com/berserkdb/berserk-client-go"
)

func main() {
	ctx := context.Background()
	client := berserk.NewHTTPClient(berserk.DefaultConfig("http://localhost:9510"))

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

Proto stubs are checked in. To regenerate:

```bash
protoc --go_out=. --go-grpc_out=. -Iproto proto/*.proto
```

## License

Apache-2.0
