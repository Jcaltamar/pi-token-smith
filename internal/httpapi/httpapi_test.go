//go:build linux

package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
)

const testToken = "1234567890123456789012345678901234567890123"

type fakeClient struct {
	payload func(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error)
}

func (fakeClient) Health(context.Context) (client.Health, error) {
	return client.Health{Version: 1, Status: "healthy"}, nil
}
func (fakeClient) Info(context.Context) (client.Info, error) { return client.Info{Version: 1}, nil }
func (fakeClient) Search(context.Context, string, int) ([]client.EventReference, error) {
	return []client.EventReference{{ID: "event"}}, nil
}
func (c fakeClient) ReadPayload(ctx context.Context, id string, offset, limit uint64, w io.Writer) (client.PayloadMetadata, error) {
	if c.payload != nil {
		return c.payload(ctx, id, offset, limit, w)
	}
	payload := []byte("payload")
	if limit == 0 {
		limit = uint64(len(payload)) - offset
	}
	n, err := w.Write(payload[offset : offset+limit])
	return payloadMetadata(offset, limit, uint64(n)), err
}

func payloadMetadata(offset, limit, written uint64) client.PayloadMetadata {
	return client.PayloadMetadata{EventID: "event", TotalSize: 7, SHA256: strings.Repeat("a", 64), Offset: offset, Limit: limit, BytesWritten: written}
}

func startHTTPServer(t *testing.T, c Client) *Server {
	t.Helper()
	s, err := New(c, testToken, Options{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func payloadRequest(t *testing.T, s *Server) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, s.URL()+"/v1/events/event/payload?offset=1&limit=3", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	return req
}

func TestServerRejectsUnsafeListenAddresses(t *testing.T) {
	for _, listen := range []string{"0.0.0.0:0", "[::]:0", "192.0.2.1:0", "example.com:0", "127.0.0.1"} {
		t.Run(listen, func(t *testing.T) {
			if _, err := New(fakeClient{}, testToken, Options{Listen: listen}); err == nil {
				t.Fatal("unsafe listen accepted")
			}
		})
	}
	for _, listen := range []string{"127.0.0.1:0", "[::1]:0"} {
		t.Run("allows "+listen, func(t *testing.T) {
			if _, err := New(fakeClient{}, testToken, Options{Listen: listen}); err != nil {
				t.Fatalf("loopback listen rejected: %v", err)
			}
		})
	}
}

func TestPayloadStreamsBytesAndTrailers(t *testing.T) {
	s := startHTTPServer(t, fakeClient{})
	response, err := http.DefaultClient.Do(payloadRequest(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, []byte("ayl")) {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	for key, want := range map[string]string{"X-Payload-Size": "7", "X-Payload-SHA256": strings.Repeat("a", 64), "X-Payload-Offset": "1", "X-Payload-Bytes": "3"} {
		if got := response.Trailer.Get(key); got != want {
			t.Errorf("trailer %s = %q, want %q", key, got, want)
		}
	}
	if got := response.Trailer.Get("X-Payload-Error"); got != "" {
		t.Fatalf("successful payload error trailer = %q", got)
	}
	assertSecurityHeaders(t, response)
}

func TestPayloadZeroBytesCommitsSuccessfulTrailers(t *testing.T) {
	s := startHTTPServer(t, fakeClient{payload: func(_ context.Context, _ string, offset, limit uint64, _ io.Writer) (client.PayloadMetadata, error) {
		return payloadMetadata(offset, limit, 0), nil
	}})
	response, err := http.DefaultClient.Do(payloadRequest(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || len(body) != 0 || response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%q err=%v", response.StatusCode, body, err)
	}
	if got := response.Trailer.Get("X-Payload-Size"); got != "7" {
		t.Fatalf("size trailer = %q", got)
	}
}

func TestPayloadPreStreamErrorsAreJSON(t *testing.T) {
	secret := "token=" + testToken + " socket=/private.sock db=/private.db"
	for _, tt := range []struct {
		name string
		err  error
		want int
		code string
	}{
		{"not found", &client.RPCError{Code: "not_found", Message: secret}, http.StatusNotFound, "not_found"},
		{"daemon unavailable", errors.New(secret), http.StatusServiceUnavailable, "unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := startHTTPServer(t, fakeClient{payload: func(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error) {
				return client.PayloadMetadata{}, tt.err
			}})
			response, err := http.DefaultClient.Do(payloadRequest(t, s))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if response.StatusCode != tt.want || string(body) != `{"code":"`+tt.code+`"}`+"\n" {
				t.Fatalf("status=%d body=%q", response.StatusCode, body)
			}
			if strings.Contains(string(body), testToken) || strings.Contains(string(body), "/private") {
				t.Fatalf("error disclosed sensitive detail: %q", body)
			}
			assertSecurityHeaders(t, response)
		})
	}
}

func TestPayloadMidstreamErrorUsesTrailer(t *testing.T) {
	s := startHTTPServer(t, fakeClient{payload: func(_ context.Context, _ string, offset, limit uint64, w io.Writer) (client.PayloadMetadata, error) {
		if _, err := w.Write([]byte("partial")); err != nil {
			return client.PayloadMetadata{}, err
		}
		return client.PayloadMetadata{}, errors.New("backend failure")
	}})
	response, err := http.DefaultClient.Do(payloadRequest(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "partial" {
		t.Fatalf("status=%d body=%q err=%v", response.StatusCode, body, err)
	}
	if got := response.Trailer.Get("X-Payload-Error"); got != "stream_error" {
		t.Fatalf("error trailer = %q", got)
	}
}

func TestPayloadClientDisconnectCancelsContext(t *testing.T) {
	cancelled := make(chan struct{})
	s := startHTTPServer(t, fakeClient{payload: func(ctx context.Context, _ string, _ uint64, _ uint64, _ io.Writer) (client.PayloadMetadata, error) {
		<-ctx.Done()
		close(cancelled)
		return client.PayloadMetadata{}, ctx.Err()
	}})
	address := strings.TrimPrefix(s.URL(), "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(conn, "GET /v1/events/event/payload?offset=1&limit=3 HTTP/1.1\r\nHost: "+address+"\r\nAuthorization: Bearer "+testToken+"\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("client disconnect did not cancel payload context")
	}
}

func TestPayloadCancellationPropagatesBeforeWrite(t *testing.T) {
	cancelled := make(chan struct{})
	s := startHTTPServer(t, fakeClient{payload: func(ctx context.Context, _ string, _ uint64, _ uint64, _ io.Writer) (client.PayloadMetadata, error) {
		if !errors.Is(ctx.Err(), context.Canceled) {
			return client.PayloadMetadata{}, errors.New("request context was not canceled")
		}
		close(cancelled)
		return client.PayloadMetadata{}, ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := payloadRequest(t, s).WithContext(ctx)
	req.Body = http.NoBody
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", recorder.Code)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler did not receive canceled request context")
	}
}

func TestServerRejectsAmbiguousQueryParameters(t *testing.T) {
	s := startHTTPServer(t, fakeClient{})
	for _, path := range []string{
		"/v1/health?unexpected=value",
		"/v1/info?unexpected=value",
		"/v1/search",
		"/v1/search?q=one&q=two",
		"/v1/search?q=one&%71=two",
		"/v1/search?q=",
		"/v1/search?q=one&limit=1&limit=2",
		"/v1/search?q=one&limit=",
		"/v1/search?q=one&unexpected=value",
		"/v1/events/event/payload?limit=3",
		"/v1/events/event/payload?offset=1",
		"/v1/events/event/payload?offset=1&offset=2&limit=3",
		"/v1/events/event/payload?offset=1&limit=3&limit=4",
		"/v1/events/event/payload?offset=&limit=3",
		"/v1/events/event/payload?offset=1&limit=&extra=value",
	} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, s.URL()+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+testToken)
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d want=%d", response.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestServerRejectsOriginHeaderPresence(t *testing.T) {
	s := startHTTPServer(t, fakeClient{})
	for _, header := range []http.Header{
		{"Origin": {""}},
		{"origin": {"", "https://example.test"}},
		{"ORIGIN": {"https://example.test"}},
	} {
		req, err := http.NewRequest(http.MethodGet, s.URL()+"/v1/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = header
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Body = http.NoBody
		recorder := httptest.NewRecorder()
		s.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("headers=%#v status=%d want=%d", header, recorder.Code, http.StatusForbidden)
		}
	}
}

func TestServerRejectsAuthenticationAndRequestSmugglingInputs(t *testing.T) {
	s := startHTTPServer(t, fakeClient{})
	for _, tt := range []struct {
		name    string
		method  string
		path    string
		auth    []string
		origin  string
		host    string
		body    io.Reader
		chunked bool
		want    int
	}{
		{"duplicate authorization", http.MethodGet, "/v1/health", []string{"Bearer " + testToken, "Bearer " + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"lowercase scheme", http.MethodGet, "/v1/health", []string{"bearer " + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"malformed scheme", http.MethodGet, "/v1/health", []string{"Bearer\t" + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"query token", http.MethodGet, "/v1/health?token=secret", []string{"Bearer " + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"query access token", http.MethodGet, "/v1/health?access_token=secret", []string{"Bearer " + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"query authorization", http.MethodGet, "/v1/health?authorization=secret", []string{"Bearer " + testToken}, "", "", nil, false, http.StatusUnauthorized},
		{"body", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "", "", strings.NewReader("body"), false, http.StatusBadRequest},
		{"chunked body", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "", "", strings.NewReader("body"), true, http.StatusBadRequest},
		{"options", http.MethodOptions, "/v1/health", []string{"Bearer " + testToken}, "", "", nil, false, http.StatusForbidden},
		{"origin", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "null", "", nil, false, http.StatusForbidden},
		{"unsupported method", http.MethodPost, "/v1/health", []string{"Bearer " + testToken}, "", "", nil, false, http.StatusNotFound},
		{"localhost host", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "", "localhost:1", nil, false, http.StatusForbidden},
		{"ipv6 host", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "", "[::1]:1", nil, false, http.StatusForbidden},
		{"confusable host", http.MethodGet, "/v1/health", []string{"Bearer " + testToken}, "", "ⅼocalhost:1", nil, false, http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, s.URL()+tt.path, tt.body)
			if err != nil {
				t.Fatal(err)
			}
			if tt.chunked {
				req.ContentLength = -1
			}
			for _, value := range tt.auth {
				req.Header.Add("Authorization", value)
			}
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if tt.host != "" {
				req.Host = tt.host
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tt.want {
				t.Fatalf("status=%d want=%d", response.StatusCode, tt.want)
			}
			assertSecurityHeaders(t, response)
		})
	}
}

type deadlineWriter struct{ deadlines []time.Time }

func (w *deadlineWriter) Header() http.Header       { return make(http.Header) }
func (*deadlineWriter) WriteHeader(int)             {}
func (*deadlineWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *deadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestPayloadWriterRefreshesDeadlineForEveryWrite(t *testing.T) {
	destination := &deadlineWriter{}
	writer := &payloadWriter{ResponseWriter: destination}
	if _, err := writer.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := writer.Write([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if len(destination.deadlines) != 2 || !destination.deadlines[1].After(destination.deadlines[0]) {
		t.Fatalf("deadlines = %v", destination.deadlines)
	}
}

func assertSecurityHeaders(t *testing.T, response *http.Response) {
	t.Helper()
	if response.Header.Get("Access-Control-Allow-Origin") != "" || response.Header.Get("X-Content-Type-Options") != "nosniff" || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("unsafe headers: %#v", response.Header)
	}
}

func TestPayloadMetadataMatchesRequestedRange(t *testing.T) {
	metadata := payloadMetadata(1, 3, 3)
	if got := strconv.FormatUint(metadata.BytesWritten, 10); got != "3" {
		t.Fatalf("bytes written = %s", got)
	}
}

var _ http.ResponseWriter = (*deadlineWriter)(nil)
