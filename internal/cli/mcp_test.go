//go:build linux

package cli

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
)

type mcpClient struct{ closes int }

func (*mcpClient) Health(context.Context) (client.Health, error) { return client.Health{}, nil }
func (*mcpClient) Info(context.Context) (client.Info, error)     { return client.Info{}, nil }
func (*mcpClient) Search(context.Context, string, int) ([]client.EventReference, error) {
	return nil, nil
}
func (*mcpClient) EventMetadata(context.Context, string) (client.EventMetadata, error) {
	return client.EventMetadata{}, nil
}
func (*mcpClient) ReadPayload(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error) {
	return client.PayloadMetadata{}, nil
}
func (c *mcpClient) Close() error { c.closes++; return nil }

type fakeMCPServer struct {
	calls  int
	stdout io.Writer
}

func (s *fakeMCPServer) Listen(_ context.Context, input io.Reader, stdout io.Writer) error {
	s.calls++
	s.stdout = stdout
	_, _ = io.Copy(io.Discard, input)
	return nil
}

func TestMCPUsesExistingDaemonClientAndLeavesStdoutToProtocol(t *testing.T) {
	paths, err := daemon.NewRuntimePaths(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := &mcpClient{}
	mcp := &fakeMCPServer{}
	serverCreated := false
	var stdout, stderr bytes.Buffer
	deps := Dependencies{Stdin: bytes.NewBufferString(""), Stdout: &stdout, Stderr: &stderr, Paths: paths, NewClient: func(string) RPCClient { return client }, NewServer: func(daemon.RuntimePaths) Server { serverCreated = true; return nil }, NewMCPServer: func(RPCClient, io.Writer) MCPServer { return mcp }}
	if err := Run(context.Background(), []string{"mcp"}, deps); err != nil {
		t.Fatal(err)
	}
	if serverCreated || mcp.calls != 1 || client.closes != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("daemon=%v calls=%d closes=%d stdout=%q stderr=%q", serverCreated, mcp.calls, client.closes, stdout.String(), stderr.String())
	}
}

func TestMCPRejectsArguments(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := Run(context.Background(), []string{"mcp", "extra"}, testDeps(&stdout, &stderr, &fakeClient{}))
	if _, ok := err.(*ExitError); !ok || stdout.Len() != 0 {
		t.Fatalf("err=%v stdout=%q", err, stdout.String())
	}
}
