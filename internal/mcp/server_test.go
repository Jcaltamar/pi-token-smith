//go:build linux

package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	mcplib "github.com/mark3labs/mcp-go/mcp"
)

type fakeClient struct {
	health    client.Health
	info      client.Info
	events    []client.EventReference
	metadata  client.EventMetadata
	payload   []byte
	err       error
	read      func(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error)
	readCalls int
}

func (f *fakeClient) Health(ctx context.Context) (client.Health, error) { return f.health, ctx.Err() }
func (f *fakeClient) Info(ctx context.Context) (client.Info, error)     { return f.info, ctx.Err() }
func (f *fakeClient) Search(ctx context.Context, _ string, _ int) ([]client.EventReference, error) {
	return f.events, firstError(ctx, f.err)
}
func (f *fakeClient) EventMetadata(ctx context.Context, _ string) (client.EventMetadata, error) {
	return f.metadata, firstError(ctx, f.err)
}
func (f *fakeClient) ReadPayload(ctx context.Context, id string, offset, limit uint64, dst io.Writer) (client.PayloadMetadata, error) {
	f.readCalls++
	if f.read != nil {
		return f.read(ctx, id, offset, limit, dst)
	}
	if err := firstError(ctx, f.err); err != nil {
		return client.PayloadMetadata{}, err
	}
	data := f.payload[offset:]
	if uint64(len(data)) > limit {
		data = data[:limit]
	}
	n, err := dst.Write(data)
	return client.PayloadMetadata{EventID: "event", TotalSize: uint64(len(f.payload)), SHA256: f.metadata.SHA256, Offset: offset, Limit: limit, BytesWritten: uint64(n)}, err
}
func firstError(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	return ctx.Err()
}

func call(name string, args map[string]any) mcplib.CallToolRequest {
	if args == nil {
		args = map[string]any{}
	}
	raw, _ := json.Marshal(args)
	var normalized map[string]any
	_ = json.Unmarshal(raw, &normalized)
	return mcplib.CallToolRequest{Params: mcplib.CallToolParams{Name: name, Arguments: normalized}}
}

func resultJSON(t *testing.T, result *mcplib.CallToolResult) map[string]any {
	t.Helper()
	if result.IsError {
		t.Fatalf("tool error: %#v", result)
	}
	text := result.Content[0].(mcplib.TextContent).Text
	if strings.Contains(text, "evidence") {
		t.Fatalf("metadata response disclosed evidence: %q", text)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestMetadataToolsDoNotReadEvidence(t *testing.T) {
	fake := &fakeClient{health: client.Health{Version: 1, Status: "healthy"}, info: client.Info{Version: 1}, events: []client.EventReference{{ID: "event", ProjectID: "project", SessionID: "session", Sequence: 2}}, metadata: client.EventMetadata{EventID: "event", TotalSize: 42, SHA256: strings.Repeat("a", 64)}, payload: []byte("raw evidence")}
	s := New(fake, io.Discard)
	for _, request := range []mcplib.CallToolRequest{call("token_smith_status", nil), call("token_smith_search", map[string]any{"query": "needle", "limit": 1}), call("token_smith_get_event", map[string]any{"event_id": "event"})} {
		result, err := s.handle(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		_ = resultJSON(t, result)
	}
	if fake.readCalls != 0 {
		t.Fatalf("metadata tools read payload %d times", fake.readCalls)
	}
}

func TestReadPayloadPreservesExactBytesAndValidatesInputs(t *testing.T) {
	payload := []byte("hello\x00世界")
	fake := &fakeClient{metadata: client.EventMetadata{EventID: "event", TotalSize: uint64(len(payload)), SHA256: strings.Repeat("b", 64)}, payload: payload}
	s := New(fake, io.Discard)
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"utf8", map[string]any{"event_id": "event", "offset": 0, "limit": float64(len(payload)), "encoding": "utf8"}, string(payload)},
		{"base64", map[string]any{"event_id": "event", "offset": 0, "limit": float64(len(payload)), "encoding": "base64"}, base64.StdEncoding.EncodeToString(payload)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := s.handle(context.Background(), call("token_smith_read_payload", tc.args))
			if err != nil {
				t.Fatal(err)
			}
			got := resultJSON(t, result)
			if got["content"] != tc.want || got["encoding"] != tc.args["encoding"] {
				t.Fatalf("result=%#v", got)
			}
		})
	}
	invalid := &fakeClient{metadata: client.EventMetadata{EventID: "event", TotalSize: 1, SHA256: strings.Repeat("c", 64)}, payload: []byte{0xff}}
	result, err := New(invalid, io.Discard).handle(context.Background(), call("token_smith_read_payload", map[string]any{"event_id": "event", "offset": 0, "limit": 1, "encoding": "utf8"}))
	if err != nil || !result.IsError || !strings.Contains(result.Content[0].(mcplib.TextContent).Text, "UTF-8") {
		t.Fatalf("invalid utf8 result=%#v err=%v", result, err)
	}
	for _, args := range []map[string]any{nil, {"event_id": "event", "offset": -1, "limit": 1, "encoding": "base64"}, {"event_id": "event", "offset": 0, "limit": 0, "encoding": "base64"}, {"event_id": "event", "offset": 0, "limit": 65537, "encoding": "base64"}, {"event_id": "event", "offset": 0, "limit": 1, "encoding": "hex"}} {
		result, err := s.handle(context.Background(), call("token_smith_read_payload", args))
		if err != nil || !result.IsError {
			t.Fatalf("args=%#v result=%#v err=%v", args, result, err)
		}
	}
}

func TestArgumentValidationReturnsStableSafeErrors(t *testing.T) {
	fake := &fakeClient{}
	s := New(fake, io.Discard)
	for _, tc := range []struct {
		name    string
		request mcplib.CallToolRequest
	}{
		{"status missing arguments", mcplib.CallToolRequest{Params: mcplib.CallToolParams{Name: "token_smith_status"}}},
		{"status unknown argument", call("token_smith_status", map[string]any{"secret": "do-not-echo"})},
		{"search unknown argument", call("token_smith_search", map[string]any{"query": "needle", "secret": "do-not-echo"})},
		{"search fractional limit", call("token_smith_search", map[string]any{"query": "needle", "limit": 1.5})},
		{"payload negative offset", call("token_smith_read_payload", map[string]any{"event_id": "event", "offset": -1, "limit": 1, "encoding": "base64"})},
		{"payload offset above JSON safe integer", call("token_smith_read_payload", map[string]any{"event_id": "event", "offset": float64(1 << 53), "limit": 1, "encoding": "base64"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := s.handle(context.Background(), tc.request)
			if err != nil || !result.IsError {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			text := result.Content[0].(mcplib.TextContent).Text
			if text != "invalid arguments" || strings.Contains(text, "do-not-echo") {
				t.Fatalf("unsafe argument error %q", text)
			}
		})
	}
}

func TestNumericArgumentsRejectUnsafeValuesAndAcceptJSONSafeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		ok    bool
	}{
		{"NaN", math.NaN(), false},
		{"positive infinity", math.Inf(1), false},
		{"negative infinity", math.Inf(-1), false},
		{"fraction", 1.5, false},
		{"negative", -1, false},
		{"safe integer", float64((1 << 53) - 1), true},
		{"unsafe integer", float64(1 << 53), false},
		{"conversion overflow", math.MaxFloat64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := nonnegativeInt(tc.value)
			if ok != tc.ok {
				t.Fatalf("nonnegativeInt(%v) ok=%v, want %v", tc.value, ok, tc.ok)
			}
		})
	}

	maxOffset := uint64(1<<53 - 1)
	fake := &fakeClient{read: func(_ context.Context, _ string, offset, limit uint64, _ io.Writer) (client.PayloadMetadata, error) {
		return client.PayloadMetadata{EventID: "event", TotalSize: maxOffset, SHA256: strings.Repeat("a", 64), Offset: offset, Limit: limit}, nil
	}}
	result, err := New(fake, io.Discard).handle(context.Background(), call("token_smith_read_payload", map[string]any{"event_id": "event", "offset": float64(maxOffset), "limit": 1, "encoding": "base64"}))
	if err != nil || result.IsError || fake.readCalls != 1 {
		t.Fatalf("safe integer boundary result=%#v err=%v calls=%d", result, err, fake.readCalls)
	}
}

func TestReadPayloadRejectsInconsistentBackendMetadata(t *testing.T) {
	validHash := strings.Repeat("d", 64)
	for _, tc := range []struct {
		name     string
		metadata client.PayloadMetadata
		payload  []byte
	}{
		{"bytes exceed limit", client.PayloadMetadata{EventID: "event", TotalSize: 10, SHA256: validHash, Offset: 0, Limit: 2, BytesWritten: 3}, []byte("abc")},
		{"offset mismatch", client.PayloadMetadata{EventID: "event", TotalSize: 10, SHA256: validHash, Offset: 1, Limit: 2, BytesWritten: 2}, []byte("ab")},
		{"limit mismatch", client.PayloadMetadata{EventID: "event", TotalSize: 10, SHA256: validHash, Offset: 0, Limit: 1, BytesWritten: 2}, []byte("ab")},
		{"event mismatch", client.PayloadMetadata{EventID: "other", TotalSize: 10, SHA256: validHash, Offset: 0, Limit: 2, BytesWritten: 2}, []byte("ab")},
		{"invalid hash", client.PayloadMetadata{EventID: "event", TotalSize: 10, SHA256: "bad", Offset: 0, Limit: 2, BytesWritten: 2}, []byte("ab")},
		{"range exceeds total", client.PayloadMetadata{EventID: "event", TotalSize: 1, SHA256: validHash, Offset: 0, Limit: 2, BytesWritten: 2}, []byte("ab")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{read: func(_ context.Context, _ string, _ uint64, _ uint64, dst io.Writer) (client.PayloadMetadata, error) {
				_, _ = dst.Write(tc.payload)
				return tc.metadata, nil
			}}
			result, err := New(fake, io.Discard).handle(context.Background(), call("token_smith_read_payload", map[string]any{"event_id": "event", "offset": 0, "limit": 2, "encoding": "base64"}))
			if err != nil || !result.IsError || result.Content[0].(mcplib.TextContent).Text != "backend unavailable" {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
}

func TestToolErrorsAreRedactedAndCancellationPropagates(t *testing.T) {
	fake := &fakeClient{err: errors.New("dial /private/daemon.sock evidence-secret")}
	result, err := New(fake, io.Discard).handle(context.Background(), call("token_smith_search", map[string]any{"query": "needle"}))
	if err != nil || !result.IsError {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	text := result.Content[0].(mcplib.TextContent).Text
	if strings.Contains(text, "daemon.sock") || strings.Contains(text, "evidence-secret") {
		t.Fatalf("leaked backend error: %q", text)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = New(&fakeClient{}, io.Discard).handle(ctx, call("token_smith_status", nil))
	if err != nil || !result.IsError || !strings.Contains(result.Content[0].(mcplib.TextContent).Text, "cancelled") {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestJSONRPCInvalidArgumentsAreSafe(t *testing.T) {
	input := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"token_smith_status\"}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"token_smith_status\",\"arguments\":null}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"token_smith_status\",\"arguments\":[]}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"token_smith_status\",\"arguments\":{\"secret\":\"do-not-echo\"}}}\n")
	var output bytes.Buffer
	if err := New(&fakeClient{}, io.Discard).Listen(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n")[1:] {
		if !strings.Contains(line, "invalid arguments") || strings.Contains(line, "backend unavailable") || strings.Contains(line, "do-not-echo") {
			t.Fatalf("unsafe invalid-argument response %q", line)
		}
	}
}

func TestStdioFlowWritesOnlyJSONRPC(t *testing.T) {
	fake := &fakeClient{health: client.Health{Version: 1, Status: "healthy"}, info: client.Info{Version: 1}}
	input := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"token_smith_status\",\"arguments\":{}}}\n")
	var output bytes.Buffer
	if err := New(fake, io.Discard).Listen(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var v map[string]any
		if err := json.Unmarshal([]byte(line), &v); err != nil || v["jsonrpc"] != "2.0" {
			t.Fatalf("line=%q err=%v", line, err)
		}
	}
}
