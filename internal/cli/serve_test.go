//go:build linux

package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
)

type serveClient struct {
	healthCalls int
	healthErr   error
}

func (c *serveClient) Health(context.Context) (client.Health, error) {
	c.healthCalls++
	if c.healthErr != nil {
		return client.Health{}, c.healthErr
	}
	return client.Health{Status: "healthy", Version: 1}, nil
}
func (*serveClient) Info(context.Context) (client.Info, error) { return client.Info{}, nil }
func (*serveClient) Search(context.Context, string, int) ([]client.EventReference, error) {
	return nil, nil
}
func (*serveClient) ReadPayload(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error) {
	return client.PayloadMetadata{}, nil
}
func (*serveClient) Close() error { return nil }

type serveHTTP struct {
	started, closed bool
	closeCalls      int
}

func (s *serveHTTP) Start() error { s.started = true; return nil }
func (*serveHTTP) URL() string    { return "http://127.0.0.1:12345" }
func (s *serveHTTP) Close(context.Context) error {
	s.closed = true
	s.closeCalls++
	return nil
}

func TestServePrintsOnlyBoundAddressAndClosesOnCancellation(t *testing.T) {
	paths, err := daemon.NewRuntimePaths(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := &serveClient{}
	server := &serveHTTP{}
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deps := Dependencies{Stdout: &stdout, Stderr: &stderr, Paths: paths, NewClient: func(string) RPCClient { return client }, NewServer: func(daemon.RuntimePaths) Server { return nil }, LoadHTTPToken: func(daemon.RuntimePaths) (string, error) { return "1234567890123456789012345678901234567890123", nil }, NewHTTPServer: func(RPCClient, string, string) (HTTPServer, error) { return server, nil }}
	if err := Run(ctx, []string{"serve"}, deps); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "http://127.0.0.1:12345\n" || stderr.Len() != 0 || !server.started || !server.closed || server.closeCalls != 1 || client.healthCalls != 1 {
		t.Fatalf("stdout=%q stderr=%q started=%v closed=%v closes=%d health=%d", stdout.String(), stderr.String(), server.started, server.closed, server.closeCalls, client.healthCalls)
	}
}

func TestServeUnavailableDoesNotCreateTokenOrStartHTTP(t *testing.T) {
	paths, err := daemon.NewRuntimePaths(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := &serveClient{healthErr: errors.New("daemon socket unavailable")}
	server := &serveHTTP{}
	var stdout bytes.Buffer
	deps := Dependencies{Stdout: &stdout, Paths: paths, NewClient: func(string) RPCClient { return client }, NewServer: func(daemon.RuntimePaths) Server { return nil }, LoadHTTPToken: func(daemon.RuntimePaths) (string, error) { t.Fatal("token loader called"); return "", nil }, NewHTTPServer: func(RPCClient, string, string) (HTTPServer, error) {
		t.Fatal("HTTP server factory called")
		return nil, nil
	}}
	if err := Run(context.Background(), []string{"serve"}, deps); err == nil || !strings.Contains(err.Error(), "daemon unavailable") {
		t.Fatalf("Run() = %v", err)
	}
	if stdout.Len() != 0 || server.started {
		t.Fatalf("stdout=%q started=%v", stdout.String(), server.started)
	}
}

func TestServeRejectsInvalidListenBeforeStarting(t *testing.T) {
	paths, err := daemon.NewRuntimePaths(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := &serveHTTP{}
	deps := Dependencies{Paths: paths, NewClient: func(string) RPCClient { return &serveClient{} }, NewServer: func(daemon.RuntimePaths) Server { return nil }, LoadHTTPToken: func(daemon.RuntimePaths) (string, error) { return "token", nil }, NewHTTPServer: func(_ RPCClient, _ string, listen string) (HTTPServer, error) {
		if listen != "0.0.0.0:0" {
			t.Fatalf("listen=%q", listen)
		}
		return nil, errors.New("HTTP listen address must be loopback")
	}}
	if err := Run(context.Background(), []string{"serve", "--listen", "0.0.0.0:0"}, deps); err == nil || !strings.Contains(err.Error(), "configure HTTP server") {
		t.Fatalf("Run() = %v", err)
	}
	if server.started {
		t.Fatal("HTTP server started for invalid listen")
	}
}
