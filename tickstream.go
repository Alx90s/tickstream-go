// Package tickstream — realtime CME futures, options & Level 2. One key, one line.
//
//	import ts "github.com/Alx90s/tickstream-go"
//
//	s := ts.New("sk_live_…")
//	ticks, _ := s.Subscribe("ES", "NQ")
//	for t := range ticks {
//	    fmt.Println(t.Symbol, t.Price, t.Size)
//	}
package tickstream

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultWS  = "wss://stream.tick-stream.xyz/v1"
	defaultAPI = "https://api.tick-stream.xyz/v1"
)

// Tick is a single trade print.
type Tick struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Size   int64   `json:"size"`
	Side   string  `json:"side"`
	Exch   string  `json:"exch"`
	Ts     int64   `json:"ts"`
}

// Book is a Level 2 order-book snapshot.
type Book struct {
	Symbol    string       `json:"symbol"`
	Bids      [][2]float64 `json:"bids"`
	Asks      [][2]float64 `json:"asks"`
	Imbalance float64      `json:"imbalance"`
	Ts        int64        `json:"ts"`
}

// Option is an option quote with greeks.
type Option struct {
	Symbol string  `json:"symbol"`
	Expiry string  `json:"expiry"`
	Strike float64 `json:"strike"`
	Right  string  `json:"right"`
	Bid    float64 `json:"bid"`
	Ask    float64 `json:"ask"`
	Price  float64 `json:"price"`
	OI     int64   `json:"oi"`
	IV     float64 `json:"iv"`
	Delta  float64 `json:"delta"`
	Gamma  float64 `json:"gamma"`
	Theta  float64 `json:"theta"`
	Vega   float64 `json:"vega"`
	Ts     int64   `json:"ts"`
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Stream is a live market-data connection factory.
type Stream struct {
	apiKey string
	url    string
}

// New returns a Stream authenticated with the given API key.
func New(apiKey string) *Stream {
	return &Stream{apiKey: apiKey, url: env("TICKSTREAM_WS_URL", defaultWS)}
}

// Subscribe streams trade ticks for one or more symbols. Range over the channel.
func (s *Stream) Subscribe(symbols ...string) (<-chan Tick, error) {
	return openStream[Tick](s, "ticks", "tick", symbols)
}

// Book streams Level 2 snapshots (Realtime+L2 / Pro plans).
func (s *Stream) Book(symbols ...string) (<-chan Book, error) {
	return openStream[Book](s, "book", "book", symbols)
}

// Options streams option quotes with greeks (Pro plan).
func (s *Stream) Options(symbols ...string) (<-chan Option, error) {
	return openStream[Option](s, "options", "option", symbols)
}

func openStream[T any](s *Stream, channel, want string, symbols []string) (<-chan T, error) {
	out := make(chan T, 1024)
	syms := make([]string, len(symbols))
	for i, x := range symbols {
		syms[i] = strings.ToUpper(x)
	}
	sep := "?"
	if strings.Contains(s.url, "?") {
		sep = "&"
	}
	u := s.url + sep + "key=" + url.QueryEscape(s.apiKey)
	sub, _ := json.Marshal(map[string]any{"op": "subscribe", "channel": channel, "symbols": syms})

	go func() {
		defer close(out)
		for {
			c, _, err := websocket.DefaultDialer.Dial(u, nil)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			_ = c.WriteMessage(websocket.TextMessage, sub)
			for {
				_, data, err := c.ReadMessage()
				if err != nil {
					break
				}
				var head struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(data, &head) != nil {
					continue
				}
				switch head.Type {
				case "ping":
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"op":"pong"}`))
				case want:
					var item T
					if json.Unmarshal(data, &item) == nil {
						out <- item
					}
				}
			}
			_ = c.Close() // dropped — reconnect
		}
	}()
	return out, nil
}

// Client is a REST client for snapshots, reference data and backfill.
type Client struct {
	apiKey string
	base   string
	http   *http.Client
}

// NewClient returns a REST client.
func NewClient(apiKey string) *Client {
	return &Client{apiKey: apiKey, base: strings.TrimRight(env("TICKSTREAM_API_URL", defaultAPI), "/"), http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *Client) get(path string) (map[string]any, error) {
	req, err := http.NewRequest("GET", c.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Quote returns the latest quote for a symbol.
func (c *Client) Quote(symbol string) (map[string]any, error) {
	return c.get("/quote?symbol=" + url.QueryEscape(strings.ToUpper(symbol)))
}

// Symbols lists the available markets.
func (c *Client) Symbols() (map[string]any, error) { return c.get("/symbols") }

// Options returns the option chain for an underlying.
func (c *Client) Options(underlying string) (map[string]any, error) {
	return c.get("/options?underlying=" + url.QueryEscape(strings.ToUpper(underlying)))
}
