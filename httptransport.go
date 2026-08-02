package sqlitezstd

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// newRangeRoundTripper builds the http.RoundTripper used for HTTP(S)-backed
// databases. It enforces dial/response-header timeouts (so a hung server cannot
// block a query forever), validates that the server actually honored the Range
// request (a 200 to a ranged GET would otherwise be served as frame data —
// silent corruption), and retries transient failures.
func newRangeRoundTripper(o *Options) http.RoundTripper {
	base := o.roundTripper
	if base == nil {
		// A caller-supplied transport owns its own timeouts; the default one
		// enforces dial/response-header timeouts here.
		base = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   o.httpTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   o.httpTimeout,
			ResponseHeaderTimeout: o.httpTimeout,
			ExpectContinueTimeout: time.Second,
		}
	}

	return &retryTransport{
		base:       base,
		maxRetries: o.httpMaxRetries,
		logger:     o.logger,
	}
}

// retryTransport wraps a base RoundTripper with Range validation and bounded
// retry-with-backoff for transient failures. Requests are idempotent GETs
// against an immutable source, so retrying is safe.
type retryTransport struct {
	base       http.RoundTripper
	maxRetries int
	logger     *slog.Logger
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; ; attempt++ {
		resp, err := t.base.RoundTrip(req)

		switch {
		case err != nil:
			lastErr = err
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError:
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("sqlitezstd: http %s for %s", resp.Status, req.URL.Redacted())
		case req.Header.Get("Range") != "" && resp.StatusCode == http.StatusOK:
			// The server returned the full body instead of the requested
			// range. Serving these bytes as frame data would be silent
			// corruption, so the 200 body is always discarded. This can be
			// transient: a CDN in front of the origin (e.g. Cloudflare/R2)
			// occasionally answers a valid range request with a full 200,
			// while retries return 206. Treat it as retryable and only give
			// up — with a clear message — after maxRetries is exhausted.
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("sqlitezstd: server ignored Range header (got %s, want 206 Partial Content) for %s",
				resp.Status, req.URL.Redacted())
		default:
			return resp, nil
		}

		if attempt >= t.maxRetries {
			return nil, fmt.Errorf("sqlitezstd: request failed after %d attempt(s): %w", attempt+1, lastErr)
		}

		if t.logger != nil {
			t.logger.Debug("sqlitezstd: retrying http request",
				"url", req.URL.Redacted(), "attempt", attempt+1, "error", lastErr)
		}

		time.Sleep(backoffDelay(attempt))
	}
}

// backoffDelay returns an exponential backoff capped at 2s: 50ms, 100ms, 200ms…
func backoffDelay(attempt int) time.Duration {
	const (
		base = 50 * time.Millisecond
		max  = 2 * time.Second
	)

	delay := base << attempt
	if delay > max || delay <= 0 {
		delay = max
	}

	return delay
}
