//go:build linux

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
	"github.com/Jcaltamar/pi-token-smith/internal/httpapi"
	mcpserver "github.com/Jcaltamar/pi-token-smith/internal/mcp"
)

// RPCClient is the read-only daemon surface required by the CLI.
type RPCClient interface {
	Health(context.Context) (client.Health, error)
	Info(context.Context) (client.Info, error)
	Search(context.Context, string, int) ([]client.EventReference, error)
	EventMetadata(context.Context, string) (client.EventMetadata, error)
	ReadPayload(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error)
	Close() error
}
type ClientFactory func(string) RPCClient
type Server interface {
	Start(context.Context) error
	Close() error
}
type ServerFactory func(daemon.RuntimePaths) Server
type HTTPServer interface {
	Start() error
	URL() string
	Close(context.Context) error
}
type HTTPServerFactory func(RPCClient, string, string) (HTTPServer, error)
type HTTPTokenLoader func(daemon.RuntimePaths) (string, error)
type MCPServer interface {
	Listen(context.Context, io.Reader, io.Writer) error
}
type MCPServerFactory func(RPCClient, io.Writer) MCPServer

// Dependencies are process boundaries injected for deterministic command tests.
type Dependencies struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
	Paths          daemon.RuntimePaths
	NewClient      ClientFactory
	NewServer      ServerFactory
	LoadHTTPToken  HTTPTokenLoader
	NewHTTPServer  HTTPServerFactory
	NewMCPServer   MCPServerFactory
}

func DefaultDependencies(paths daemon.RuntimePaths) Dependencies {
	return Dependencies{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Paths: paths, NewClient: func(socket string) RPCClient { return client.New(socket, client.Options{}) }, NewServer: func(paths daemon.RuntimePaths) Server { return daemon.NewServer(paths) }, LoadHTTPToken: daemon.LoadOrCreateHTTPToken, NewHTTPServer: func(c RPCClient, token, listen string) (HTTPServer, error) {
		return httpapi.New(c, token, httpapi.Options{Listen: listen})
	}, NewMCPServer: func(c RPCClient, stderr io.Writer) MCPServer { return mcpserver.New(c, stderr) }}
}

type ExitError struct{ Message string }

func (e *ExitError) Error() string { return e.Message }
func usage(message string) error   { return &ExitError{Message: message} }

func Run(ctx context.Context, args []string, deps Dependencies) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if deps.Stdout == nil {
		deps.Stdout = io.Discard
	}
	if deps.Stderr == nil {
		deps.Stderr = io.Discard
	}
	if deps.NewClient == nil || deps.NewServer == nil {
		return errors.New("CLI dependencies are incomplete")
	}
	if len(args) == 0 {
		printUsage(deps.Stderr)
		return usage("command is required")
	}
	switch args[0] {
	case "help", "--help", "-h":
		printUsage(deps.Stdout)
		return nil
	case "daemon":
		return runDaemon(ctx, args[1:], deps)
	case "status":
		return runStatus(ctx, args[1:], deps)
	case "search":
		return runSearch(ctx, args[1:], deps)
	case "inspect":
		return runInspect(ctx, args[1:], deps)
	case "doctor":
		return runDoctor(ctx, args[1:], deps)
	case "serve":
		return runServe(ctx, args[1:], deps)
	case "mcp":
		return runMCP(ctx, args[1:], deps)
	default:
		printUsage(deps.Stderr)
		return usage("unknown command: " + args[0])
	}
}
func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, "usage: pi-token-smith <daemon|status|search|inspect|doctor|serve|mcp> [options]\n")
}
func flags(name string, stderr io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	return f
}
func getClient(deps Dependencies) (RPCClient, error) {
	if deps.Paths.Socket == "" {
		return nil, errors.New("runtime socket is empty")
	}
	return deps.NewClient(deps.Paths.Socket), nil
}
func runDaemon(ctx context.Context, args []string, deps Dependencies) error {
	if len(args) != 0 {
		return usage("daemon accepts no arguments")
	}
	s := deps.NewServer(deps.Paths)
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	<-ctx.Done()
	if err := s.Close(); err != nil {
		return fmt.Errorf("close daemon: %w", err)
	}
	return nil
}
func runStatus(ctx context.Context, args []string, deps Dependencies) error {
	f := flags("status", deps.Stderr)
	jsonOutput := f.Bool("json", false, "output JSON")
	if err := f.Parse(args); err != nil {
		return usage(err.Error())
	}
	if f.NArg() != 0 {
		return usage("status accepts no arguments")
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	health, err := c.Health(ctx)
	if err != nil {
		return fmt.Errorf("daemon health: %w", err)
	}
	if *jsonOutput {
		return json.NewEncoder(deps.Stdout).Encode(health)
	}
	_, err = fmt.Fprintf(deps.Stdout, "%s (protocol %d)\n", health.Status, health.Version)
	return err
}
func runSearch(ctx context.Context, args []string, deps Dependencies) error {
	f := flags("search", deps.Stderr)
	limit := f.Int("limit", 100, "maximum references")
	if err := f.Parse(args); err != nil {
		return usage(err.Error())
	}
	if f.NArg() != 1 || strings.TrimSpace(f.Arg(0)) == "" || *limit < 1 || *limit > 100 {
		return usage("search requires one query and --limit between 1 and 100")
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	events, err := c.Search(ctx, f.Arg(0), *limit)
	if err != nil {
		return fmt.Errorf("search events: %w", err)
	}
	return json.NewEncoder(deps.Stdout).Encode(events)
}
func runInspect(ctx context.Context, args []string, deps Dependencies) error {
	f := flags("inspect", deps.Stderr)
	offset := f.Uint64("offset", 0, "byte offset")
	limit := f.Uint64("limit", 0, "maximum bytes")
	output := f.String("output", "", "write raw bytes to file")
	if err := f.Parse(args); err != nil {
		return usage(err.Error())
	}
	if f.NArg() != 1 || f.Arg(0) == "" {
		return usage("inspect requires one event ID")
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	if *output == "" {
		_, err := c.ReadPayload(ctx, f.Arg(0), *offset, *limit, deps.Stdout)
		if err != nil {
			return fmt.Errorf("inspect payload: %w", err)
		}
		return nil
	}
	return inspectFile(ctx, c, f.Arg(0), *offset, *limit, *output)
}
func inspectFile(ctx context.Context, c RPCClient, id string, offset, limit uint64, target string) error {
	directory := filepath.Dir(target)
	temporary, err := os.CreateTemp(directory, ".pi-token-smith-")
	if err != nil {
		return fmt.Errorf("create inspect output: %w", err)
	}
	name := temporary.Name()
	complete := false
	defer func() {
		_ = temporary.Close()
		if !complete {
			_ = os.Remove(name)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect inspect output: %w", err)
	}
	if _, err := c.ReadPayload(ctx, id, offset, limit, temporary); err != nil {
		return fmt.Errorf("inspect payload: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close inspect output: %w", err)
	}
	if err := os.Rename(name, target); err != nil {
		return fmt.Errorf("publish inspect output: %w", err)
	}
	complete = true
	return nil
}
func runMCP(ctx context.Context, args []string, deps Dependencies) error {
	if len(args) != 0 {
		return usage("mcp accepts no arguments")
	}
	if deps.Stdin == nil || deps.NewMCPServer == nil {
		return errors.New("CLI MCP dependencies are incomplete")
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	return deps.NewMCPServer(c, deps.Stderr).Listen(ctx, deps.Stdin, deps.Stdout)
}

func runServe(ctx context.Context, args []string, deps Dependencies) error {
	if deps.LoadHTTPToken == nil || deps.NewHTTPServer == nil {
		return errors.New("CLI serve dependencies are incomplete")
	}
	f := flags("serve", deps.Stderr)
	listen := f.String("listen", "127.0.0.1:0", "loopback listen address")
	if err := f.Parse(args); err != nil {
		return usage(err.Error())
	}
	if f.NArg() != 0 {
		return usage("serve accepts no arguments")
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.Health(ctx); err != nil {
		return fmt.Errorf("daemon unavailable: %w", err)
	}
	token, err := deps.LoadHTTPToken(deps.Paths)
	if err != nil {
		return fmt.Errorf("load HTTP token: %w", err)
	}
	s, err := deps.NewHTTPServer(c, token, *listen)
	if err != nil {
		return fmt.Errorf("configure HTTP server: %w", err)
	}
	if err := s.Start(); err != nil {
		return fmt.Errorf("start HTTP server: %w", err)
	}
	defer s.Close(context.Background())
	if _, err := fmt.Fprintln(deps.Stdout, s.URL()); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func runDoctor(ctx context.Context, args []string, deps Dependencies) error {
	if len(args) != 0 {
		return usage("doctor accepts no arguments")
	}
	rootInfo, err := os.Lstat(deps.Paths.Root)
	if err != nil {
		return fmt.Errorf("runtime directory: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		_, _ = fmt.Fprintln(deps.Stdout, "runtime_directory: unhealthy")
		return errors.New("runtime directory is not a directory")
	}
	_, _ = fmt.Fprintf(deps.Stdout, "runtime_directory: %s\n", modeStatus(rootInfo.Mode(), 0o700))
	for _, check := range []struct {
		name, path string
		mode       os.FileMode
	}{{"database", deps.Paths.Database, 0o600}, {"socket", deps.Paths.Socket, 0o600}, {"http_token", deps.Paths.HTTPToken, 0o600}} {
		info, statErr := os.Lstat(check.path)
		if statErr != nil {
			_, _ = fmt.Fprintf(deps.Stdout, "%s: missing\n", check.name)
			continue
		}
		_, _ = fmt.Fprintf(deps.Stdout, "%s: %s\n", check.name, modeStatus(info.Mode(), check.mode))
	}
	c, err := getClient(deps)
	if err != nil {
		return err
	}
	defer c.Close()
	health, err := c.Health(ctx)
	if err != nil {
		return fmt.Errorf("daemon health: %w", err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		return fmt.Errorf("daemon info: %w", err)
	}
	_, err = fmt.Fprintf(deps.Stdout, "daemon: %s (protocol %d)\n", health.Status, info.Version)
	return err
}
func modeStatus(actual, want os.FileMode) string {
	if actual.Perm() == want {
		return "ok"
	}
	return "unexpected permissions"
}
