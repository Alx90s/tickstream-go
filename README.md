# tickstream (Go)

Realtime CME futures, options & Level 2 — one key, one line.

```bash
go get github.com/Alx90s/tickstream-go
```

## Stream ticks

```go
package main

import (
    "fmt"
    ts "github.com/Alx90s/tickstream-go"
)

func main() {
    s := ts.New("sk_live_…")
    ticks, _ := s.Subscribe("ES", "NQ")
    for t := range ticks {
        fmt.Println(t.Symbol, t.Price, t.Size, t.Side)
    }
}
```

`s.Book("ES")` and `s.Options("ES", "SPX")` stream Level 2 and options the same
way. Connections auto-reconnect and answer heartbeats.

## REST

```go
api := ts.NewClient("sk_live_…")
q, _ := api.Quote("ES")
syms, _ := api.Symbols()
```

Get your API key at https://tick-stream.xyz. Docs: https://tick-stream.xyz/docs.
Point at a local gateway with `TICKSTREAM_WS_URL` / `TICKSTREAM_API_URL`.
