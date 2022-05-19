# Stata

Pure Go, Simple, Time-Series Counters for analytics

- aims to support all popular databases

# Install

1. Install library

```bash
go get github.com/fletcherist/stata
```

2. Select storage for your database

- [redis](https://github.com/redis/redis)
- [foundationdb](https://github.com/apple/foundationdb)
- [memory](https://github.com/fletcherist/stata/blob/main/memory.go)
- you can easily implement your own storage backend

```bash
# foundationdb db
go get github.com/fletcherist/stata/fdb
# redis
go get github.com/fletcherist/stata/redis
```

3. Working example

```go
package main

import (
	"fmt"

	"github.com/fletcherist/stata"
)

func main() {
	// create storage where counters would be stored
	storage := stata.NewMemoryStorage()
	// create client with storage
	stataClient := stata.New(&stata.Config{Storage: storage})
	counter := stataClient.Event("simple-counter", stata.EventConfig{
		Bins: []stata.Bin{ // time-series intervals for storing counters
			stata.Bins.Minute, // counts number of events for every minute
		},
	})
	// to test counter lets increment it's value 3 times
	counter.Inc()
	counter.Inc()
	counter.Inc()
	// now lets count how many events we've got for that minute (3 events)
	value, _ := stataClient.Get(stata.Key{
		Name: counter.Name,
		Bin:  stata.Bins.Minute,
	})
	// prints "3"
	fmt.Println(value)
}
```

#### install json datasource

[Grafana JSON Datasource](https://github.com/simPod/GrafanaJsonDatasource)

```bash
grafana-cli plugins install simpod-json-datasource
```
