//go:build linux

package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/protocol"
)

func TestServerCaptureReadSearchAndSequentialFrames(t *testing.T) {
	s := startTestServer(t)
	conn := dialServer(t, s)
	defer conn.Close()
	for index, payload := range [][]byte{nil, []byte("hello, 世界"), bytes.Repeat([]byte("chunk"), 10000)} {
		id := "event-" + string(rune('a'+index))
		appendCapture(t, conn, id, payload)
	}
	response := rpc(t, conn, "search.events", map[string]any{"query":"hello", "limit":10})
	if response.Status != "ok" { t.Fatalf("search = %#v", response) }
	var search struct { Events []struct { ID string `json:"id"` } `json:"events"` }
	if err := json.Unmarshal(response.Body, &search); err != nil || len(search.Events) != 1 { t.Fatalf("search body = %s, %v", response.Body, err) }
	payload := []byte("hello, 世界")
	response = rpc(t, conn, "event.read_payload", map[string]any{"event_id":search.Events[0].ID, "offset":0, "limit":0})
	if response.Status != "ok" { t.Fatalf("read metadata = %#v", response) }
	length, err := protocol.ReadEvidenceHeader(conn)
	if err != nil || length != uint64(len(payload)) { t.Fatalf("evidence header = %d, %v", length, err) }
	got, err := io.ReadAll(io.LimitReader(conn, int64(length)))
	if err != nil || !bytes.Equal(got, payload) { t.Fatalf("read payload = %q, %v", got, err) }
	if health := rpc(t, conn, "system.health", map[string]any{}); health.Status != "ok" { t.Fatalf("following request = %#v", health) }
}

func TestServerReturnsStableErrorsAndPaginates(t *testing.T) {
	s := startTestServer(t)
	conn := dialServer(t, s)
	defer conn.Close()
	for _, tc := range []struct { operation string; version int; code string }{{"unknown", protocol.Version, "unsupported_operation"}, {"system.health", 99, "unsupported_protocol"}} {
		req := request("error-"+tc.operation, tc.operation, map[string]any{})
		req.ProtocolVersion = tc.version
		if err := protocol.Encode(conn, req); err != nil { t.Fatal(err) }
		var response protocol.Response
		if err := protocol.Decode(conn, &response); err != nil || response.Error == nil || response.Error.Code != tc.code { t.Fatalf("error response = %#v, %v", response, err) }
	}
	payload := []byte("0123456789")
	appendCapture(t, conn, "paged", payload)
	response := rpc(t, conn, "event.read_payload", map[string]any{"event_id":"paged", "offset":3, "limit":4})
	if response.Status != "ok" { t.Fatalf("page metadata = %#v", response) }
	length, err := protocol.ReadEvidenceHeader(conn)
	if err != nil || length != 4 { t.Fatalf("page length = %d, %v", length, err) }
	got, err := io.ReadAll(io.LimitReader(conn, int64(length)))
	if err != nil || !bytes.Equal(got, []byte("3456")) { t.Fatalf("page = %q, %v", got, err) }
}

func TestServerRecoversStaleSocket(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	if err := EnsureRuntimeDirectory(paths); err != nil { t.Fatal(err) }
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name:paths.Socket, Net:"unix"})
	if err != nil { t.Fatal(err) }
	if err := stale.Close(); err != nil { t.Fatal(err) }
	s := NewServer(paths)
	if err := s.Start(context.Background()); err != nil { t.Fatalf("Start() stale socket = %v", err) }
	defer s.Close()
	if health := rpc(t, dialServer(t, s), "system.health", map[string]any{}); health.Status != "ok" { t.Fatalf("health = %#v", health) }
}

func TestServerRejectsTruncatedEvidenceAndSecondInstance(t *testing.T) {
	s := startTestServer(t)
	second := NewServer(s.paths)
	if err := second.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) { t.Fatalf("second Start() = %v, want ErrAlreadyRunning", err) }
	conn := dialServer(t, s)
	req := request("truncated", "capture.append", captureBody("truncated", 6))
	if err := protocol.Encode(conn, req); err != nil { t.Fatal(err) }
	if err := protocol.WriteEvidenceHeader(conn, 6); err != nil { t.Fatal(err) }
	if _, err := conn.Write([]byte("short")); err != nil { t.Fatal(err) }
	_ = conn.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) { if s.store != nil { if count, err := s.store.EventCount(context.Background(), "truncated"); err == nil && count == 0 { return } }; time.Sleep(10*time.Millisecond) }
	t.Fatal("truncated event persisted or server did not process closure")
}

func TestServerSocketSafetyAndShutdown(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	paths := mustRuntimePaths(t, root)
	if err := EnsureRuntimeDirectory(paths); err != nil { t.Fatal(err) }
	if err := os.WriteFile(paths.Socket, []byte("keep"), 0o600); err != nil { t.Fatal(err) }
	if err := NewServer(paths).Start(context.Background()); err == nil { t.Fatal("regular socket path accepted") }
	if got, _ := os.ReadFile(paths.Socket); string(got) != "keep" { t.Fatal("regular path changed") }
	if err := os.Remove(paths.Socket); err != nil { t.Fatal(err) }
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil { t.Fatal(err) }
	if err := os.Symlink(target, paths.Socket); err != nil { t.Fatal(err) }
	if err := NewServer(paths).Start(context.Background()); err == nil { t.Fatal("symlink socket path accepted") }
	if got, _ := os.ReadFile(target); string(got) != "target" { t.Fatal("symlink target changed") }

	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime")))
	if err := s.Start(ctx); err != nil { t.Fatal(err) }
	assertMode(t, s.paths.Socket, 0o600)
	cancel()
	if err := s.Close(); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
}

func TestServerInvalidCaptureClosesBeforeFollowingEvidenceIsParsed(t *testing.T) {
	s := startTestServer(t)
	conn := dialServer(t, s)
	defer conn.Close()
	invalid := request("invalid-capture", "capture.append", map[string]any{"event_id": "only"})
	if err := protocol.Encode(conn, invalid); err != nil { t.Fatal(err) }
	// This is a complete control request placed where capture evidence would be.
	if err := protocol.Encode(conn, request("must-not-run", "system.health", map[string]any{})); err != nil { t.Fatal(err) }
	var response protocol.Response
	if err := protocol.Decode(conn, &response); err != nil || response.Status != "error" { t.Fatalf("invalid capture response = %#v, %v", response, err) }
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := protocol.Decode(conn, &response); err == nil { t.Fatal("following bytes were parsed as a request") }
}

func TestServerConnectionLimitAndReadDeadline(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	s := NewServerWithOptions(paths, ServerOptions{MaxConnections: 1, ReadTimeout: 30 * time.Millisecond, WriteTimeout: time.Second})
	if err := s.Start(context.Background()); err != nil { t.Fatal(err) }
	defer s.Close()
	first := dialServer(t, s)
	defer first.Close()
	second := dialServer(t, s)
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	var response protocol.Response
	if err := protocol.Decode(second, &response); err == nil { t.Fatal("connection beyond limit was not rejected") }
	_ = first.SetReadDeadline(time.Now().Add(time.Second))
	if err := protocol.Decode(first, &response); err == nil { t.Fatal("idle connection was not closed after read timeout") }
}

func TestServerCloseWaitsForFailedStartupWithoutRecursion(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	entered := make(chan struct{})
	release := make(chan struct{})
	startupFailure := errors.New("controlled startup failure")
	closeWaiting := make(chan struct{})
	s := NewServerWithOptions(paths, ServerOptions{
		startupFailure: func() error {
			close(entered)
			<-release
			return startupFailure
		},
		closeWaiting: func() { close(closeWaiting) },
	})

	startResult := make(chan error, 1)
	go func() { startResult <- s.Start(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach the controlled failure point")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- s.Close() }()
	select {
	case <-closeWaiting:
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for startup")
	}
	close(release)

	select {
	case err := <-startResult:
		if !errors.Is(err, startupFailure) {
			t.Fatalf("Start() = %v, want controlled failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after controlled failure")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close() = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after failed startup")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	s.mu.Lock()
	state, lock, store, listener, socket := s.state, s.lock, s.store, s.listener, s.socket
	s.mu.Unlock()
	if state != serverClosed || lock != nil || store != nil || listener != nil || socket != nil {
		t.Fatalf("failed startup retained state=%v lock=%v store=%v listener=%v socket=%v", state, lock, store, listener, socket)
	}
	if _, err := os.Lstat(paths.Socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed startup left socket: %v", err)
	}

	replacement := NewServer(paths)
	if err := replacement.Start(context.Background()); err != nil {
		t.Fatalf("replacement Start() = %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatalf("replacement Close() = %v", err)
	}
}

func TestServerIsSingleUseAndRejectsCanceledStart(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	cancelled, cancel := context.WithCancel(context.Background()); cancel()
	s := NewServer(paths)
	if err := s.Start(cancelled); !errors.Is(err, context.Canceled) { t.Fatalf("Start(cancelled) = %v", err) }
	if _, err := os.Stat(paths.Root); !errors.Is(err, os.ErrNotExist) { t.Fatalf("cancelled Start acquired resources: %v", err) }
	s = startTestServer(t)
	if err := s.Start(context.Background()); !errors.Is(err, ErrServerSingleUse) { t.Fatalf("second Start = %v", err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	if err := s.Start(context.Background()); !errors.Is(err, ErrServerSingleUse) { t.Fatalf("Start after Close = %v", err) }
}

func TestServerShutdownPreservesSubstitutedSocketNodes(t *testing.T) {
	for _, tt := range []struct { name string; create func(t *testing.T, path string) }{
		{"regular file", func(t *testing.T, path string) { if err := os.WriteFile(path, []byte("preserve"), 0o600); err != nil { t.Fatal(err) } }},
		{"symlink", func(t *testing.T, path string) { target := filepath.Join(t.TempDir(), "target"); if err := os.WriteFile(target, []byte("preserve"), 0o600); err != nil { t.Fatal(err) }; if err := os.Symlink(target, path); err != nil { t.Fatal(err) } }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := startTestServer(t)
			if err := os.Remove(s.paths.Socket); err != nil { t.Fatal(err) }
			tt.create(t, s.paths.Socket)
			if err := s.Close(); err == nil { t.Fatal("Close() accepted substituted socket") }
			info, err := os.Lstat(s.paths.Socket); if err != nil { t.Fatal(err) }
			if tt.name == "regular file" && !info.Mode().IsRegular() { t.Fatalf("node mode = %v, want regular", info.Mode()) }
			if tt.name == "symlink" && info.Mode()&os.ModeSymlink == 0 { t.Fatalf("node mode = %v, want symlink", info.Mode()) }
		})
	}
}

func TestServerConcurrentClients(t *testing.T) {
	s := startTestServer(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ { wg.Add(1); go func() { defer wg.Done(); c:=dialServer(t,s); defer c.Close(); if response:=rpc(t,c,"system.hello",map[string]any{}); response.Status!="ok" { t.Errorf("hello=%#v",response) } }() }
	wg.Wait()
}

func startTestServer(t *testing.T) *Server { t.Helper(); s:=NewServer(mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))); if err:=s.Start(context.Background()); err!=nil { t.Fatal(err) }; t.Cleanup(func(){ _=s.Close() }); return s }
func dialServer(t *testing.T, s *Server) net.Conn { t.Helper(); c,err:=net.Dial("unix",s.paths.Socket); if err!=nil { t.Fatal(err) }; return c }
func request(id, operation string, body any) protocol.Request { raw,_:=json.Marshal(body); return protocol.Request{ProtocolVersion:protocol.Version,RequestID:id,Operation:operation,SentAt:time.Now().UTC(),Body:raw} }
func rpc(t *testing.T, c net.Conn, operation string, body any) protocol.Response { t.Helper(); if err:=protocol.Encode(c,request(operation+"-id",operation,body));err!=nil{t.Fatal(err)}; var response protocol.Response; if err:=protocol.Decode(c,&response);err!=nil{t.Fatal(err)}; return response }
func captureBody(id string, size int) map[string]any { return map[string]any{"event_id":id,"event_type":"pi_assistant_message_json","project_id":"project","session_id":"session","exchange_id":"exchange","sequence":1,"occurred_at":"2026-01-02T03:04:05Z","encoding":"application/json","payload_size":size} }
func appendCapture(t *testing.T, c net.Conn, id string, payload []byte) { t.Helper(); if err:=protocol.Encode(c,request(id,"capture.append",captureBody(id,len(payload))));err!=nil{t.Fatal(err)}; if _,err:=protocol.WriteEvidence(c,uint64(len(payload)),bytes.NewReader(payload));err!=nil{t.Fatal(err)}; var response protocol.Response; if err:=protocol.Decode(c,&response);err!=nil||response.Status!="ok"{t.Fatalf("append = %#v, %v",response,err)} }
