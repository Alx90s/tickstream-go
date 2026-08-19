# tickstream-go

CME futures, options and dealer-gamma data over REST and WebSocket.

**21 endpoints, 12 option-history request types, 4 streaming channels** — the complete API. Coverage is asserted by
`sdk/check-coverage.mjs` against a manifest generated from the gateway's own router, and every
call in the table below is exercised against production before release.

## Install

```sh
go get github.com/Alx90s/tickstream-go
```

## Quickstart

```go
ts := tickstream.New("")                // "" reads TICKSTREAM_API_KEY
q, err := ts.Quote(ctx, "NQ")

ticks, errs := ts.Stream(ctx, tickstream.Ticks, "NQ", "ES")
for t := range ticks {
    fmt.Println(t.Symbol, t.Price)
}
```

## Everything you can call

| what | call |
| --- | --- |
| quote | `ts.Quote(ctx, "NQ")` |
| symbols | `ts.Symbols(ctx)` |
| recent ticks | `ts.Ticks(ctx, "NQ", start, end, limit)` |
| deep ticks | `ts.HistoryTicks(ctx, "NQ", start, end, limit)` |
| deep L2 book | `ts.HistoryBook(ctx, "NQ", start, end, limit)` |
| legacy chain archive | `ts.HistoryOptions(ctx, "QQQ", "archive")` |
| live chain | `ts.OptionsChain(ctx, "QQQ")` |
| option history (12 types) | `ts.OptionsRequest(ctx, "eod", tickstream.Params{…})` |
| dealer gamma, live | `ts.GEX(ctx, "NQ", nil)` |
| dealer gamma, past | `ts.GEX(ctx, "NQ", Params{"date": "2026-07-15"})` |
| participant flow | `ts.Participants(ctx, "NQ")` |
| CFTC positioning | `ts.COT(ctx, "NQ", 8)` |
| algo catalogue / record | `ts.Algos(ctx) · ts.AlgoTrack(ctx, id)` |
| orders, positions, fills | `ts.ExecPositions(ctx)` |
| place / close / protect | `ts.ExecOrder(ctx, body)` |
| stream | `ticks, errs := ts.Stream(ctx, tickstream.Ticks, "NQ")` |

## Three things that will bite you otherwise

**1. `ticks()` without `start` returns one hour.** Not seven days — one hour. The window
reaches back seven days on any plan and years with an archive plan, but you have to ask:
pass `start`. This is the single most common integration surprise.

**2. Timestamps come in two units.** Range arguments are unix *seconds*; tick rows are
stamped in *microseconds*. They differ by a factor of a million, and comparing them returns
nothing rather than raising — so this SDK converts for you rather than documenting it and
hoping.

**3. Two opposite symbol conventions.** `gex()` and `participants()` take the **futures or
stock** symbol (`NQ`, `ES`, `AAPL`) and map onto the deep ETF surface internally. `options` and
`history.options` take the **ETF or index root** (`QQQ`, `SPY`, `SPX`). Passing `QQQ` to `gex()`
is a `400 unsupported_symbol`.

## Errors

Every failure carries the API's machine-readable `code`, which is stable and worth branching
on. A `403` whose code ends in `_required` means the endpoint works and your key does not hold
that package — distinct from an invalid key, without parsing prose.

## Entitlements per endpoint

| endpoint | needs | parameters |
| --- | --- | --- |
| `GET /v1/algos` | included | — |
| `GET /v1/algos/:id/events` | included | — |
| `GET /v1/algos/:id/signal` | included | — |
| `GET /v1/algos/:id/track` | included | — |
| `GET /v1/cot` | included | symbol, weeks |
| `POST /v1/exec/close` | execution | — |
| `GET /v1/exec/fills` | included | — |
| `POST /v1/exec/order` | execution | — |
| `GET /v1/exec/orders` | included | — |
| `GET /v1/exec/positions` | included | — |
| `POST /v1/exec/protect` | execution | — |
| `GET /v1/gex` | gex | underlying, symbol, weight, dte, at, date |
| `GET /v1/history/book` | data:nq-ticks | end, limit, start, symbol |
| `GET /v1/history/options` | data:options-data | end, limit, source, start, underlying |
| `GET /v1/history/ticks` | data:nq-ticks | end, limit, start, symbol |
| `GET /v1/options` | options_stream | underlying |
| `GET /v1/options/:req` | options | — |
| `GET /v1/participants` | gex | underlying |
| `GET /v1/quote` | included | symbol |
| `GET /v1/symbols` | included | — |
| `GET /v1/ticks` | included | end, start, symbol |

`included` means every plan that can reach the API at all. `data:*` is an archive window,
`gex`/`options`/`execution` are the product packages — see <https://tick-stream.xyz/pricing>.

## Streaming, and why silence is not a bug

Every frame carries a `type`, **ticks included**. A filter that skips anything with a `type`
therefore drops all the data and leaves a socket that looks connected and delivers nothing —
this SDK handles that. Error frames are never swallowed either: a refused symbol is exactly
the failure that reads as a quiet market.

Index levels (SPX, VIX, NDX, RUT) are quotes, not trade prints: they update on change, roughly
every two seconds. A flat VIX genuinely sends nothing. A liquid future going quiet for minutes
is worth reporting.

## Docs

<https://tick-stream.xyz/docs/sdks> · machine-readable: <https://tick-stream.xyz/llms-full.txt>
