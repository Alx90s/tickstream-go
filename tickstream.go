// Package tickstream is a client for CME futures, options and dealer-gamma data.
//
//	ts := tickstream.New("")           // "" reads TICKSTREAM_API_KEY
//	q, err := ts.Quote(ctx, "NQ")
//
//	ticks, errs := ts.Stream(ctx, tickstream.Ticks, "NQ", "ES")
//	for t := range ticks { fmt.Println(t.Symbol, t.Price) }
//
// The 0.1.0 release exposed three endpoints and no WebSocket at all, against an API with
// twenty-one. The endpoint list here is checked against sdk/api-manifest.json, which is
// generated from the gateway's own router.
//
// Two conventions to know before you build:
//
//   - Timestamps. REST range arguments are unix SECONDS. Tick rows come back stamped in
//     MICROSECONDS (Ts). The two differ by a factor of a million and mixing them returns
//     nothing rather than erroring — use Time() on a tick.
//   - Open interest is daily. OCC publishes it once per day, industry-wide. Intraday gamma
//     moves because spot and implied vol move over a fixed strike ladder, not because OI ticks.
package tickstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	Version  = "1.0.0"
	RESTBase = "https://api.tick-stream.xyz/v1"
	WSBase   = "wss://stream.tick-stream.xyz/v1"
)

// Channel is a WebSocket channel the gateway accepts.
type Channel string

const (
	Ticks   Channel = "ticks"
	Book    Channel = "book"
	L3      Channel = "l3"
	Options Channel = "options"
)

// OptionRequests is the /v1/options/{request} family, ThetaData-backed.
var OptionRequests = []string{
	"eod", "greeks", "greeks_first_order", "greeks_history", "greeks_second_order",
	"greeks_third_order", "ohlc", "oi", "quote", "trade", "trade_greeks", "trade_quote",
}

// Error is what the API returned. Code is stable and worth branching on; Message is for
// humans. A 403 carries Requires, naming the missing entitlement, so "you did not buy
// this" is distinguishable from "your key is wrong" without parsing prose.
type Error struct {
	Status   int    `json:"status"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	Requires string `json:"requires,omitempty"`
}

func (e *Error) Error() string {
	if e.Code != "" {
		return e.Code + ": " + e.Message
	}
	return e.Message
}

// Gated reports whether this error is the API correctly refusing a package the key does
// not hold, rather than a fault.
func (e *Error) Gated() bool {
	return e.Status == 403 && strings.HasSuffix(e.Code, "_required")
}

type Client struct {
	APIKey   string
	RESTBase string
	WSBase   string
	HTTP     *http.Client
	Retries  int
}

// New returns a client. An empty key reads TICKSTREAM_API_KEY.
func New(apiKey string) *Client {
	if apiKey == "" {
		apiKey = os.Getenv("TICKSTREAM_API_KEY")
	}
	if apiKey != "" && !strings.HasPrefix(apiKey, "sk_live_") && !strings.HasPrefix(apiKey, "sk_test_") {
		// Not fatal — keys may change shape — but a placeholder left in an env var is the
		// single most common cause of a support thread about "free tier".
		fmt.Fprintf(os.Stderr, "tickstream: warning — key %.12s does not look like an API key\n", apiKey)
	}
	return &Client{
		APIKey:   apiKey,
		RESTBase: RESTBase,
		WSBase:   WSBase,
		HTTP:     &http.Client{Timeout: 30 * time.Second},
		Retries:  2,
	}
}

// ── REST ──

type Params map[string]any

func (p Params) query() url.Values {
	v := url.Values{}
	for k, val := range p {
		if val == nil {
			continue
		}
		switch t := val.(type) {
		case string:
			if t != "" {
				v.Set(k, t)
			}
		// Zero is "unset", not a value. Go has no optional int without a pointer, so a
		// caller leaving `end` alone sends 0 — and the API answers `bad_range: start must
		// be before end`, because epoch 0 is 1970. Found by the live smoke test; a
		// compile-only check would have shipped it. Pass a real epoch to mean a real time.
		case int:
			if t != 0 {
				v.Set(k, strconv.Itoa(t))
			}
		case int64:
			if t != 0 {
				v.Set(k, strconv.FormatInt(t, 10))
			}
		case time.Time:
			v.Set(k, strconv.FormatInt(t.Unix(), 10))
		default:
			v.Set(k, fmt.Sprint(t))
		}
	}
	return v
}

func (c *Client) do(ctx context.Context, method, path string, p Params, body any, out any) error {
	if c.APIKey == "" {
		return errors.New("tickstream: pass an API key or set TICKSTREAM_API_KEY")
	}
	u := c.RESTBase + path
	if q := p.query(); len(q) > 0 {
		u += "?" + q.Encode()
	}
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = strings.NewReader(string(b))
	}

	var last error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u, payload)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("User-Agent", "tickstream-go/"+Version)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.HTTP.Do(req)
		if err != nil {
			last = err
			if attempt < c.Retries {
				time.Sleep(time.Duration(500*(attempt+1)) * time.Millisecond)
				continue
			}
			return &Error{Code: "network_error", Message: err.Error()}
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			// 5xx is worth retrying; a 4xx is an answer and retrying it is rude.
			if resp.StatusCode >= 500 && attempt < c.Retries {
				last = fmt.Errorf("upstream %d", resp.StatusCode)
				time.Sleep(time.Duration(500*(attempt+1)) * time.Millisecond)
				continue
			}
			var env struct{ Error Error `json:"error"` }
			apiErr := &Error{Status: resp.StatusCode, Code: fmt.Sprintf("http_%d", resp.StatusCode)}
			if json.Unmarshal(raw, &env) == nil && env.Error.Code != "" {
				apiErr.Code, apiErr.Message, apiErr.Requires = env.Error.Code, env.Error.Message, env.Error.Requires
			} else {
				apiErr.Message = string(raw)
			}
			return apiErr
		}
		if out != nil && len(raw) > 0 {
			return json.Unmarshal(raw, out)
		}
		return nil
	}
	return &Error{Code: "network_error", Message: fmt.Sprint(last)}
}

// Get performs an arbitrary GET, for an endpoint this SDK has no typed method for yet.
func (c *Client) Get(ctx context.Context, path string, p Params, out any) error {
	return c.do(ctx, http.MethodGet, path, p, nil, out)
}

// Post performs an arbitrary POST.
func (c *Client) Post(ctx context.Context, path string, body any, out any) error {
	return c.do(ctx, http.MethodPost, path, nil, body, out)
}

// ── snapshots ──

type Quote struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Bid    float64 `json:"bid"`
	Ask    float64 `json:"ask"`
	Ts     int64   `json:"ts"`
}

// Quote returns the last price, with bid/ask where the feed carries them. Index levels
// (SPX, VIX) arrive without bid/ask and update on change, roughly every two seconds.
func (c *Client) Quote(ctx context.Context, symbol string) (*Quote, error) {
	var q Quote
	return &q, c.Get(ctx, "/quote", Params{"symbol": symbol}, &q)
}

// Symbols lists the streamable roots. A symbol absent from this list delivers no ticks.
func (c *Client) Symbols(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/symbols", nil, &out)
}

// Ticks returns recent ticks. WITHOUT start this returns ONE HOUR — the default that has
// cost more than one integration a day of debugging. The window reaches back seven days;
// deeper history is HistoryTicks.
func (c *Client) Ticks(ctx context.Context, symbol string, start, end int64, limit int) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/ticks", Params{"symbol": symbol, "start": start, "end": end, "limit": limit}, &out)
}

// GEX returns dealer gamma: walls, zero-gamma flip, per-strike exposure, DEX/vanna/charm.
//
// at (an instant, unix seconds) or date ("YYYY-MM-DD", resolved to 15:59 ET) evaluate a
// PAST surface with the same computation as live. Historical reads are open-interest
// weighted: weight="vol" is refused there rather than answered with the day's total volume,
// which would be lookahead. Check spotAgeSecs on historical responses.
//
// Takes the FUTURES or STOCK symbol (NQ, ES, AAPL), not the ETF proxy — QQQ is a 400.
func (c *Client) GEX(ctx context.Context, underlying string, p Params) (map[string]any, error) {
	if p == nil {
		p = Params{}
	}
	p["underlying"] = underlying
	out := map[string]any{}
	return out, c.Get(ctx, "/gex", p, &out)
}

// Participants returns customer versus market-maker flow, sweeps in their own bucket.
// Futures or stock symbol, same convention as GEX.
func (c *Client) Participants(ctx context.Context, underlying string) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/participants", Params{"underlying": underlying}, &out)
}

// COT returns CFTC Commitments of Traders, weekly.
func (c *Client) COT(ctx context.Context, symbol string, weeks int) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/cot", Params{"symbol": symbol, "weeks": weeks}, &out)
}

// ── history: depth is your plan's window; a request past it is clamped, not refused, and
// the response says so in history_clamped ──

func (c *Client) HistoryTicks(ctx context.Context, symbol string, start, end int64, limit int) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/history/ticks", Params{"symbol": symbol, "start": start, "end": end, "limit": limit}, &out)
}

func (c *Client) HistoryBook(ctx context.Context, symbol string, start, end int64, limit int) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/history/book", Params{"symbol": symbol, "start": start, "end": end, "limit": limit}, &out)
}

// HistoryOptions reads the legacy chain archive. Equity and index underlyings moved to the
// options package and answer 301 here; pass source="archive" to read the old store.
func (c *Client) HistoryOptions(ctx context.Context, underlying, source string) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/history/options", Params{"underlying": underlying, "source": source}, &out)
}

// ── options: takes the ETF/index root (QQQ, SPY, SPX) — the opposite of GEX ──

// OptionsChain returns the live full-chain snapshot with greeks.
func (c *Client) OptionsChain(ctx context.Context, underlying string) ([]map[string]any, error) {
	var out []map[string]any
	return out, c.Get(ctx, "/options", Params{"underlying": underlying}, &out)
}

// OptionsRequest calls one of OptionRequests, e.g. OptionsRequest(ctx, "eod", Params{...}).
func (c *Client) OptionsRequest(ctx context.Context, kind string, p Params) (map[string]any, error) {
	ok := false
	for _, k := range OptionRequests {
		if k == kind {
			ok = true
			break
		}
	}
	if !ok {
		return nil, fmt.Errorf("unknown option request %q; expected one of %s", kind, strings.Join(OptionRequests, ", "))
	}
	out := map[string]any{}
	return out, c.Get(ctx, "/options/"+kind, p, &out)
}

// ── algos ──

func (c *Client) Algos(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/algos", nil, &out)
}

func (c *Client) AlgoTrack(ctx context.Context, id string) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/algos/"+id+"/track", nil, &out)
}

func (c *Client) AlgoSignal(ctx context.Context, id string) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/algos/"+id+"/signal", nil, &out)
}

func (c *Client) AlgoEvents(ctx context.Context, id string) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/algos/"+id+"/events", nil, &out)
}

// ── execution: the read side is safe to poll; the write side moves real money. ExecClose
// and ExecProtect act on positions the API opened and deliberately cannot flatten a
// position your platform or an algo sleeve opened. ──

func (c *Client) ExecOrders(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/exec/orders", nil, &out)
}

func (c *Client) ExecPositions(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/exec/positions", nil, &out)
}

func (c *Client) ExecFills(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Get(ctx, "/exec/fills", nil, &out)
}

func (c *Client) ExecOrder(ctx context.Context, body any) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Post(ctx, "/exec/order", body, &out)
}

func (c *Client) ExecClose(ctx context.Context, body any) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Post(ctx, "/exec/close", body, &out)
}

func (c *Client) ExecProtect(ctx context.Context, body any) (map[string]any, error) {
	out := map[string]any{}
	return out, c.Post(ctx, "/exec/protect", body, &out)
}

// ── streaming ──

// Tick is one message off the ticks channel.
type Tick struct {
	Type   string  `json:"type"`
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Size   int64   `json:"size"`
	Side   string  `json:"side"`
	Exch   string  `json:"exch"`
	Ts     int64   `json:"ts"`
	Tsu    int64   `json:"tsu"`
	Raw    json.RawMessage `json:"-"`
}

// Time returns the tick's timestamp, in whichever unit it arrived. Ts is seconds on the
// socket and microseconds in the stores; guessing wrong is off by 1e6, which presents as an
// empty result rather than as a unit bug.
func (t Tick) Time() time.Time {
	v := t.Ts
	if t.Tsu > 0 {
		return time.Unix(t.Tsu/1_000_000, (t.Tsu%1_000_000)*1000).UTC()
	}
	if v > 1e14 {
		return time.Unix(v/1_000_000, 0).UTC()
	}
	return time.Unix(v, 0).UTC()
}

// Stream opens the WebSocket and delivers messages until ctx is cancelled. It reconnects
// with backoff and re-subscribes, because the alternative is a loop that looks alive and
// delivers nothing.
//
// Every frame carries a type, ticks included — so a naive "skip anything with a type"
// filter drops all the data and the stream looks silent. That is handled here: pings and
// the welcome frame are swallowed, data is delivered, and error frames go to the errs
// channel rather than being hidden (a refused symbol is exactly the failure that reads as a
// quiet market). Both channels are closed when ctx ends.
func (c *Client) Stream(ctx context.Context, channel Channel, symbols ...string) (<-chan Tick, <-chan error) {
	out := make(chan Tick, 256)
	errs := make(chan error, 8)

	go func() {
		defer close(out)
		defer close(errs)
		delay := time.Second

		for {
			if ctx.Err() != nil {
				return
			}
			u := c.WSBase + "?key=" + url.QueryEscape(c.APIKey)
			if channel == Ticks && len(symbols) > 0 {
				u += "&symbols=" + strings.Join(upper(symbols), ",")
			}
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, u, nil)
			if err != nil {
				select {
				case errs <- fmt.Errorf("dial: %w", err):
				default:
				}
				if !sleepCtx(ctx, delay) {
					return
				}
				delay = min(delay*2, 30*time.Second)
				continue
			}
			delay = time.Second

			if channel != Ticks && len(symbols) > 0 {
				_ = conn.WriteJSON(map[string]any{
					"action": "subscribe", "channel": string(channel), "symbols": upper(symbols),
				})
			}

			for {
				_, raw, err := conn.ReadMessage()
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					break
				}
				// The gateway sends either one object or an array of them.
				var many []json.RawMessage
				if json.Unmarshal(raw, &many) != nil {
					many = []json.RawMessage{raw}
				}
				for _, item := range many {
					var t Tick
					if json.Unmarshal(item, &t) != nil {
						continue
					}
					switch t.Type {
					case "ping", "pong", "welcome", "subscribed":
						continue
					case "error":
						var e struct{ Error Error `json:"error"` }
						_ = json.Unmarshal(item, &e)
						select {
						case errs <- &e.Error:
						default:
						}
						continue
					}
					t.Raw = item
					select {
					case out <- t:
					case <-ctx.Done():
						conn.Close()
						return
					}
				}
			}
			conn.Close()
			if !sleepCtx(ctx, delay) {
				return
			}
			delay = min(delay*2, 30*time.Second)
		}
	}()

	return out, errs
}

func upper(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToUpper(s)
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
