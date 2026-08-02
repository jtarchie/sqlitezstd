package sqlitezstd

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// rangeServer returns a handler that answers the first flakyAttempts requests
// with a full 200 body (as a CDN transiently might) and every request after
// that with a proper 206 Partial Content honoring the Range header. It reports
// how many requests it received via the returned counter.
func rangeServer(body []byte, flakyAttempts int) (http.Handler, *int64) {
	var count int64

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&count, 1)

		if int(n) <= flakyAttempts {
			// Transiently ignore the Range header and return the whole body.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

			return
		}

		// Honor the range: parse "bytes=start-end".
		rng := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		var start, end int
		if _, err := fmt.Sscanf(rng, "%d-%d", &start, &end); err != nil || start < 0 || end >= len(body) || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)

			return
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	})

	return handler, &count
}

func rangeRequest(t *testing.T, url string, start, end int) *http.Request {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	return req
}

// TestRetryTransportRetriesTransient200 verifies that a 200 returned to a Range
// request is treated as transient: the retry loop tries again and succeeds once
// the server starts returning 206 (as happens with a CDN like Cloudflare/R2 in
// front of an origin that reliably serves partial content).
func TestRetryTransportRetriesTransient200(t *testing.T) {
	t.Parallel()

	body := []byte("the quick brown fox jumps over the lazy dog")

	// Fail (200) on the first two attempts, then serve 206.
	handler, count := rangeServer(body, 2)
	server := httptest.NewServer(handler)
	defer server.Close()

	rt := &retryTransport{base: http.DefaultTransport, maxRetries: 3}

	resp, err := rt.RoundTrip(rangeRequest(t, server.URL, 4, 8))
	if err != nil {
		t.Fatalf("expected success after retrying past the 200, got error: %v", err)
	}
	defer resp.Body.Close() //nolint: errcheck

	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %s", resp.Status)
	}

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if want := body[4:9]; string(got) != string(want) {
		t.Fatalf("expected body %q, got %q", want, got)
	}

	if *count != 3 {
		t.Fatalf("expected 3 attempts (2 failing 200s + 1 succeeding 206), got %d", *count)
	}
}

// TestRetryTransportGivesUpOnPersistent200 verifies that a server which returns
// 200 to a Range request on every attempt still fails after maxRetries, and
// that the final error preserves the "server ignored Range header" message so a
// genuinely range-incapable server surfaces a clear cause.
func TestRetryTransportGivesUpOnPersistent200(t *testing.T) {
	t.Parallel()

	body := []byte("the quick brown fox jumps over the lazy dog")

	// Never serve 206 (a very large flaky count means every attempt is a 200).
	handler, count := rangeServer(body, 1<<30)
	server := httptest.NewServer(handler)
	defer server.Close()

	const maxRetries = 2

	rt := &retryTransport{base: http.DefaultTransport, maxRetries: maxRetries}

	resp, err := rt.RoundTrip(rangeRequest(t, server.URL, 4, 8))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected an error when the server ignores Range on every attempt, got success")
	}

	if !strings.Contains(err.Error(), "server ignored Range header") {
		t.Fatalf("expected the final error to mention the ignored Range header, got: %v", err)
	}

	if want := int64(maxRetries + 1); *count != want {
		t.Fatalf("expected %d attempts before giving up, got %d", want, *count)
	}
}
