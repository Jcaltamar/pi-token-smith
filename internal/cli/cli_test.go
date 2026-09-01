//go:build linux

package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
)

type fakeClient struct {
	health       client.Health
	events       []client.EventReference
	payload      []byte
	readErr      error
	writeThenErr bool
}

func (f *fakeClient) Health(context.Context) (client.Health, error) { return f.health, nil }
func (f *fakeClient) Info(context.Context) (client.Info, error)     { return client.Info{Version: 1}, nil }
func (f *fakeClient) Search(context.Context, string, int) ([]client.EventReference, error) {
	return f.events, nil
}
func (f *fakeClient) EventMetadata(context.Context, string) (client.EventMetadata, error) {
	return client.EventMetadata{}, nil
}
func (f *fakeClient) ReadPayload(_ context.Context, _ string, _, _ uint64, w io.Writer) (client.PayloadMetadata, error) {
	if f.readErr != nil && !f.writeThenErr {
		return client.PayloadMetadata{}, f.readErr
	}
	n, err := w.Write(f.payload)
	if err != nil {
		return client.PayloadMetadata{BytesWritten: uint64(n)}, err
	}
	if f.readErr != nil {
		return client.PayloadMetadata{BytesWritten: uint64(n)}, f.readErr
	}
	return client.PayloadMetadata{BytesWritten: uint64(n)}, nil
}
func (f *fakeClient) Close() error { return nil }

type cancelledClient struct{ fakeClient }

func (c *cancelledClient) ReadPayload(ctx context.Context, _ string, _, _ uint64, _ io.Writer) (client.PayloadMetadata, error) {
	return client.PayloadMetadata{}, ctx.Err()
}

func testDeps(out, errOut io.Writer, fake RPCClient) Dependencies {
	paths, _ := daemon.NewRuntimePaths(filepath.Join("/tmp", "token-smith-test"))
	return Dependencies{Stdout: out, Stderr: errOut, Paths: paths, NewClient: func(string) RPCClient { return fake }, NewServer: func(daemon.RuntimePaths) Server { return nil }}
}

func TestStatusSearchAndInspectKeepOutputChannelsSeparate(t *testing.T) {
	fake := &fakeClient{health: client.Health{Version: 1, Status: "healthy"}, events: []client.EventReference{{ID: "event"}}, payload: []byte{0, 1, 2}}
	var stdout, stderr bytes.Buffer
	deps := testDeps(&stdout, &stderr, fake)
	if err := Run(context.Background(), []string{"status"}, deps); err != nil || stdout.String() != "healthy (protocol 1)\n" || stderr.Len() != 0 {
		t.Fatalf("status stdout=%q stderr=%q err=%v", stdout.String(), stderr.String(), err)
	}
	stdout.Reset()
	if err := Run(context.Background(), []string{"search", "--limit", "2", "needle"}, deps); err != nil || stdout.String() != "[{\"id\":\"event\",\"project_id\":\"\",\"session_id\":\"\",\"exchange_id\":\"\",\"sequence\":0}]\n" {
		t.Fatalf("search stdout=%q err=%v", stdout.String(), err)
	}
	stdout.Reset()
	if err := Run(context.Background(), []string{"inspect", "event"}, deps); err != nil || !bytes.Equal(stdout.Bytes(), []byte{0, 1, 2}) || stderr.Len() != 0 {
		t.Fatalf("inspect stdout=%v stderr=%q err=%v", stdout.Bytes(), stderr.String(), err)
	}
}

func TestInspectFailedFileOutputLeavesNoArtifact(t *testing.T) {
	target := filepath.Join(t.TempDir(), "evidence.bin")
	fake := &fakeClient{readErr: errors.New("unavailable")}
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), []string{"inspect", "--output", target, "event"}, testDeps(&stdout, &stderr, fake)); err == nil {
		t.Fatal("inspect succeeded")
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed inspect left output: %v", err)
	}
}

func TestDoctorIsReadOnlyAndDoesNotDiscloseRuntimeContents(t *testing.T) {
	root := t.TempDir()
	paths, err := daemon.NewRuntimePaths(root)
	if err != nil {
		t.Fatal(err)
	}
	secretToken, lockMetadata, evidence := "token-secret", "lock-secret", "evidence-secret"
	for _, file := range []struct{ path, contents string }{{paths.Database, evidence}, {paths.HTTPToken, secretToken}, {paths.Lock, lockMetadata}} {
		if err := os.WriteFile(file.path, []byte(file.contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string][]byte{}
	for _, path := range []string{paths.Database, paths.HTTPToken, paths.Lock} {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		before[path] = data
	}
	var stdout, stderr bytes.Buffer
	serverCreated := false
	deps := Dependencies{Stdout: &stdout, Stderr: &stderr, Paths: paths, NewClient: func(string) RPCClient { return &fakeClient{health: client.Health{Version: 1, Status: "healthy"}} }, NewServer: func(daemon.RuntimePaths) Server { serverCreated = true; return nil }}
	if err := Run(context.Background(), []string{"doctor"}, deps); err != nil {
		t.Fatal(err)
	}
	if serverCreated {
		t.Fatal("doctor started or constructed a daemon")
	}
	for path, want := range before {
		got, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("doctor modified %s: %q, %v", path, got, readErr)
		}
	}
	output := stdout.String() + stderr.String()
	for _, forbidden := range []string{root, secretToken, lockMetadata, evidence} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("doctor disclosed %q in %q", forbidden, output)
		}
	}
	stdout.Reset()
	stderr.Reset()
	if err := Run(context.Background(), []string{"doctor", "--json"}, deps); err == nil || stdout.Len() != 0 || strings.Contains(stderr.String(), secretToken) {
		t.Fatalf("doctor --json stdout=%q stderr=%q err=%v", stdout.String(), stderr.String(), err)
	}
}

func TestDoctorRejectsSymlinkRuntimeRootWithoutFollowingIt(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "runtime-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	targetPaths, err := daemon.NewRuntimePaths(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []struct {
		path, contents string
		mode           os.FileMode
	}{
		{targetPaths.Database, "evidence-secret", 0o600},
		{targetPaths.HTTPToken, "token-secret", 0o600},
		{targetPaths.Lock, "lock-secret", 0o600},
	} {
		if err := os.WriteFile(file.path, []byte(file.contents), file.mode); err != nil {
			t.Fatal(err)
		}
	}
	before := make(map[string]struct {
		contents []byte
		mode     os.FileMode
	})
	for _, path := range []string{targetPaths.Database, targetPaths.HTTPToken, targetPaths.Lock} {
		info, statErr := os.Lstat(path)
		contents, readErr := os.ReadFile(path)
		if statErr != nil || readErr != nil {
			t.Fatalf("snapshot %s: %v, %v", path, statErr, readErr)
		}
		before[path] = struct {
			contents []byte
			mode     os.FileMode
		}{contents, info.Mode()}
	}
	paths, err := daemon.NewRuntimePaths(link)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	clientCreated := false
	deps := Dependencies{
		Stdout: &stdout,
		Stderr: &stderr,
		Paths:  paths,
		NewClient: func(string) RPCClient {
			clientCreated = true
			return &fakeClient{}
		},
		NewServer: func(daemon.RuntimePaths) Server { return nil },
	}
	if err := Run(context.Background(), []string{"doctor"}, deps); err == nil {
		t.Fatal("doctor accepted symlink runtime root")
	}
	if clientCreated {
		t.Fatal("doctor created a client for symlink runtime root")
	}
	linkInfo, err := os.Lstat(link)
	if err != nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("runtime root changed: %#v, %v", linkInfo, err)
	}
	for path, want := range before {
		info, statErr := os.Lstat(path)
		contents, readErr := os.ReadFile(path)
		if statErr != nil || readErr != nil || info.Mode() != want.mode || !bytes.Equal(contents, want.contents) {
			t.Fatalf("doctor modified target %s: mode=%v contents=%q errors=%v,%v", path, info.Mode(), contents, statErr, readErr)
		}
	}
	output := stdout.String() + stderr.String()
	if !strings.Contains(output, "runtime_directory: unhealthy") {
		t.Fatalf("doctor did not report unhealthy root: %q", output)
	}
	for _, forbidden := range []string{"evidence-secret", "token-secret", "lock-secret"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("doctor disclosed %q in %q", forbidden, output)
		}
	}
}

func TestDoctorRejectsNonDirectoryRuntimeRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime-file")
	if err := os.WriteFile(root, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths, err := daemon.NewRuntimePaths(root)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	clientCreated := false
	deps := Dependencies{Stdout: &stdout, Stderr: &stderr, Paths: paths, NewClient: func(string) RPCClient { clientCreated = true; return &fakeClient{} }, NewServer: func(daemon.RuntimePaths) Server { return nil }}
	if err := Run(context.Background(), []string{"doctor"}, deps); err == nil {
		t.Fatal("doctor accepted non-directory runtime root")
	}
	if clientCreated {
		t.Fatal("doctor created a client for non-directory runtime root")
	}
	if !strings.Contains(stdout.String()+stderr.String(), "runtime_directory: unhealthy") {
		t.Fatalf("doctor did not report unhealthy root: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestInspectFileIsAtomicAndPreservesExistingDestination(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "evidence.bin")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	partial := &fakeClient{payload: []byte("new"), readErr: errors.New("RPC interrupted"), writeThenErr: true}
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), []string{"inspect", "--output", target, "event"}, testDeps(&stdout, &stderr, partial)); err == nil {
		t.Fatal("inspect succeeded")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "old" {
		t.Fatalf("failed inspect replaced destination: %q, %v", got, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "evidence.bin" {
		t.Fatalf("temporary output remained: %#v, %v", entries, err)
	}

	success := &fakeClient{payload: []byte("complete")}
	if err := Run(context.Background(), []string{"inspect", "--output", target, "event"}, testDeps(&stdout, &stderr, success)); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(target)
	if err != nil || string(got) != "complete" {
		t.Fatalf("successful inspect did not atomically replace destination: %q, %v", got, err)
	}

	cancelledTarget := filepath.Join(root, "cancelled.bin")
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(cancelledCtx, []string{"inspect", "--output", cancelledTarget, "event"}, testDeps(&stdout, &stderr, &cancelledClient{})); err == nil {
		t.Fatal("cancelled inspect succeeded")
	}
	if _, err := os.Stat(cancelledTarget); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled inspect left output: %v", err)
	}
}

func TestArgumentValidationWritesOnlyToStderr(t *testing.T) {
	for _, args := range [][]string{{"status", "extra"}, {"search"}, {"search", "--limit", "0", "query"}, {"inspect"}, {"daemon", "extra"}, {"doctor", "extra"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := Run(context.Background(), args, testDeps(&stdout, &stderr, &fakeClient{}))
			if _, ok := err.(*ExitError); !ok || stdout.Len() != 0 {
				t.Fatalf("args=%q err=%v stdout=%q stderr=%q", args, err, stdout.String(), stderr.String())
			}
		})
	}
}

func TestInvalidCommandReturnsExitError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := Run(context.Background(), []string{"unknown"}, testDeps(&stdout, &stderr, &fakeClient{}))
	if _, ok := err.(*ExitError); !ok || stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
}
