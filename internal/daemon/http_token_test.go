//go:build linux

package daemon

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLoadOrCreateHTTPTokenCreatesPrivateReusableToken(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))

	first, err := LoadOrCreateHTTPToken(paths)
	if err != nil {
		t.Fatalf("LoadOrCreateHTTPToken() = %v", err)
	}
	if len(first) < 43 { // 32 random bytes, base64url without padding.
		t.Fatalf("token too short: %d", len(first))
	}
	info, err := os.Stat(paths.HTTPToken)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token mode = %o, want 600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(paths.HTTPToken)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, []byte(first)) {
		t.Fatal("token file differs from returned token")
	}
	second, err := LoadOrCreateHTTPToken(paths)
	if err != nil {
		t.Fatalf("second LoadOrCreateHTTPToken() = %v", err)
	}
	if second != first {
		t.Fatal("token was not reused")
	}
}

func TestLoadOrCreateHTTPTokenRejectsUnsafeExistingNodes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		create func(*testing.T, RuntimePaths, string)
	}{
		{"symlink", func(t *testing.T, paths RuntimePaths, target string) {
			if err := os.Symlink(target, paths.HTTPToken); err != nil {
				t.Fatal(err)
			}
		}},
		{"directory", func(t *testing.T, paths RuntimePaths, _ string) {
			if err := os.Mkdir(paths.HTTPToken, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{"fifo", func(t *testing.T, paths RuntimePaths, _ string) {
			if err := unix.Mkfifo(paths.HTTPToken, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"malformed", func(t *testing.T, paths RuntimePaths, _ string) {
			if err := os.WriteFile(paths.HTTPToken, []byte("short"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"newline", func(t *testing.T, paths RuntimePaths, _ string) {
			if err := os.WriteFile(paths.HTTPToken, []byte("1234567890123456789012345678901234567890123\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"short base64", func(t *testing.T, paths RuntimePaths, _ string) {
			if err := os.WriteFile(paths.HTTPToken, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			paths := mustRuntimePaths(t, t.TempDir())
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			tt.create(t, paths, target)
			result := make(chan error, 1)
			go func() { _, err := LoadOrCreateHTTPToken(paths); result <- err }()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("unsafe token node accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("unsafe token node blocked")
			}
			if got, err := os.ReadFile(target); err != nil || string(got) != "preserve" {
				t.Fatalf("target changed: %q, %v", got, err)
			}
		})
	}
}

func TestLoadOrCreateHTTPTokenTightensExistingRegularToken(t *testing.T) {
	paths := mustRuntimePaths(t, t.TempDir())
	if err := EnsureRuntimeDirectory(paths); err != nil {
		t.Fatal(err)
	}
	want, err := LoadOrCreateHTTPToken(paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(paths.HTTPToken, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadOrCreateHTTPToken(paths)
	if err != nil || got != want {
		t.Fatalf("LoadOrCreateHTTPToken() = %q, %v", got, err)
	}
	info, err := os.Stat(paths.HTTPToken)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token mode = %v, %v", info.Mode(), err)
	}
}

func TestLoadOrCreateHTTPTokenConcurrentCreatorsConverge(t *testing.T) {
	const (
		attempts = 100
		creators = 16
	)
	for attempt := range attempts {
		paths := mustRuntimePaths(t, t.TempDir())
		results := make(chan string, creators)
		errors := make(chan error, creators)
		var group sync.WaitGroup
		for range creators {
			group.Add(1)
			go func() {
				defer group.Done()
				token, err := LoadOrCreateHTTPToken(paths)
				if err != nil {
					errors <- err
					return
				}
				results <- token
			}()
		}
		group.Wait()
		close(results)
		close(errors)
		for err := range errors {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		var token string
		for got := range results {
			if token == "" {
				token = got
			} else if got != token {
				t.Fatalf("attempt %d: tokens differ", attempt)
			}
		}
		entries, err := os.ReadDir(paths.Root)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != "http.token" {
			t.Fatalf("attempt %d: runtime directory contains %v, want only http.token", attempt, entries)
		}
	}
}
