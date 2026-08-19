package tickstream

// Live smoke test: does each family of endpoint answer, and does the socket deliver?
// Coverage of the full endpoint list is asserted statically by sdk/check-coverage.mjs; what
// this proves is that the wire format and the WebSocket loop actually work, which no static
// check can see. A 403 ending in _required is a PASS — the key just lacks that package.
//
//   TICKSTREAM_API_KEY=sk_live_… go test -v -run TestLive ./...

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestLive(t *testing.T) {
	key := os.Getenv("TICKSTREAM_API_KEY")
	if key == "" {
		t.Skip("set TICKSTREAM_API_KEY")
	}
	c := New(key)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ok := func(name string, err error) {
		var e *Error
		if errors.As(err, &e) && e.Gated() {
			t.Logf("gated  %s  %s", name, e.Code)
			return
		}
		if err != nil {
			t.Errorf("FAIL   %s  %v", name, err)
			return
		}
		t.Logf("ok     %s", name)
	}

	q, err := c.Quote(ctx, "NQ")
	ok("quote", err)
	if err == nil && q.Price <= 0 {
		t.Errorf("FAIL   quote returned no price")
	}
	_, err = c.Symbols(ctx)
	ok("symbols", err)
	_, err = c.Ticks(ctx, "NQ", time.Now().Unix()-3600, 0, 5)
	ok("ticks", err)
	_, err = c.HistoryTicks(ctx, "NQ", time.Now().Unix()-90*86400, time.Now().Unix()-90*86400+600, 5)
	ok("history.ticks", err)
	_, err = c.HistoryBook(ctx, "NQ", time.Now().Unix()-3*86400, time.Now().Unix()-3*86400+60, 5)
	ok("history.book", err)
	_, err = c.HistoryOptions(ctx, "QQQ", "archive")
	ok("history.options", err)
	_, err = c.OptionsChain(ctx, "QQQ")
	ok("options.chain", err)
	_, err = c.OptionsRequest(ctx, "eod", Params{"underlying": "QQQ", "expiration": "*", "strike": "*", "date": "20260715"})
	ok("options.eod", err)
	// GEX and participants take the FUTURES symbol; the ETF proxy is a 400 here while
	// /v1/options takes exactly the opposite.
	_, err = c.GEX(ctx, "NQ", nil)
	ok("gex", err)
	_, err = c.Participants(ctx, "NQ")
	ok("participants", err)
	_, err = c.COT(ctx, "NQ", 4)
	ok("cot", err)
	_, err = c.Algos(ctx)
	ok("algos", err)
	_, err = c.AlgoTrack(ctx, "gamma-reversal")
	ok("algos.track", err)
	_, err = c.ExecPositions(ctx)
	ok("exec.positions", err)

	if _, err := c.OptionsRequest(ctx, "not_a_request", nil); err == nil {
		t.Error("FAIL   an unknown option request must not reach the wire")
	}

	sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
	defer scancel()
	ticks, errs := c.Stream(sctx, Ticks, "NQ", "ES")
	seen := map[string]int{}
	for n := 0; n < 40; {
		select {
		case tk, open := <-ticks:
			if !open {
				n = 40
				break
			}
			seen[tk.Symbol]++
			n++
			if n == 1 && tk.Time().Year() < 2020 {
				t.Errorf("FAIL   tick timestamp resolved to %v", tk.Time())
			}
		case e := <-errs:
			t.Logf("stream error frame: %v", e)
		case <-sctx.Done():
			n = 40
		}
	}
	if len(seen) == 0 {
		t.Error("FAIL   stream connected but delivered nothing")
	}
	t.Logf("ok     stream %v", seen)
}
