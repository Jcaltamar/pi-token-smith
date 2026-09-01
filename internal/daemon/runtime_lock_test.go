//go:build linux

package daemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestEnsureRuntimeDirectoryCreatesAndProtectsDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	paths := mustRuntimePaths(t, root)

	if err := EnsureRuntimeDirectory(paths); err != nil {
		t.Fatalf("EnsureRuntimeDirectory() error = %v", err)
	}
	assertMode(t, root, 0o700)

	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	if err := EnsureRuntimeDirectory(paths); err != nil {
		t.Fatalf("EnsureRuntimeDirectory() after chmod error = %v", err)
	}
	assertMode(t, root, 0o700)
}

func TestDefaultRuntimePathsUsesHomeRuntimeDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	paths, err := NewRuntimePaths("")
	if err != nil {
		t.Fatalf("NewRuntimePaths(\"\") error = %v", err)
	}
	wantRoot := filepath.Join(home, ".pi", "agent", "pi-token-smith")
	if paths.Root != wantRoot {
		t.Fatalf("default root = %q, want %q", paths.Root, wantRoot)
	}
	if paths.Lock != filepath.Join(wantRoot, "token-smith.lock") {
		t.Fatalf("default lock = %q, want path beneath default root", paths.Lock)
	}
}

func TestEnsureRuntimeDirectoryRejectsUnsafeRoot(t *testing.T) {
	parent := t.TempDir()
	fileRoot := filepath.Join(parent, "file")
	if err := os.WriteFile(fileRoot, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	tests := []struct {
		name string
		root string
	}{
		{name: "regular file", root: fileRoot},
	}

	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	symlinkRoot := filepath.Join(parent, "symlink")
	if err := os.Symlink(target, symlinkRoot); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	tests = append(tests, struct {
		name string
		root string
	}{name: "symbolic link", root: symlinkRoot})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := EnsureRuntimeDirectory(mustRuntimePaths(t, tt.root))
			if err == nil {
				t.Fatal("EnsureRuntimeDirectory() error = nil, want rejection")
			}
		})
	}

	t.Run("symbolic link leaves target permissions unchanged", func(t *testing.T) {
		if err := os.Chmod(target, 0o755); err != nil {
			t.Fatalf("Chmod() error = %v", err)
		}
		err := EnsureRuntimeDirectory(mustRuntimePaths(t, symlinkRoot))
		if err == nil {
			t.Fatal("EnsureRuntimeDirectory() error = nil, want rejection")
		}
		assertMode(t, target, 0o755)
	})
}

func TestAcquireLockLifecycle(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))

	first, err := AcquireLock(paths)
	if err != nil {
		t.Fatalf("first AcquireLock() error = %v", err)
	}

	second, err := AcquireLock(paths)
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second AcquireLock() error = %v, want ErrAlreadyRunning", err)
	}
	if second != nil {
		t.Fatalf("second AcquireLock() lock = %v, want nil", second)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}

	reacquired, err := AcquireLock(paths)
	if err != nil {
		t.Fatalf("AcquireLock() after release error = %v", err)
	}
	defer func() {
		if err := reacquired.Release(); err != nil {
			t.Errorf("reacquired Release() error = %v", err)
		}
	}()
}

func TestAcquireLockContendsAcrossProcesses(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	command := exec.Command(os.Args[0], "-test.run=^TestLockHeldBySubprocess$")
	command.Env = append(os.Environ(), "PI_TOKEN_SMITH_LOCK_HELPER=1", "PI_TOKEN_SMITH_LOCK_ROOT="+root)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe() error = %v", err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() error = %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waited := false
	defer func() {
		_ = input.Close()
		if !waited {
			_ = command.Wait()
		}
	}()

	ready, err := bufio.NewReader(output).ReadString('\n')
	if err != nil {
		t.Fatalf("read helper readiness: %v", err)
	}
	if ready != "locked\n" {
		t.Fatalf("helper readiness = %q, want %q", ready, "locked\\n")
	}

	lock, err := AcquireLock(mustRuntimePaths(t, root))
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("AcquireLock() error = %v, want ErrAlreadyRunning", err)
	}
	if lock != nil {
		t.Fatalf("AcquireLock() lock = %v, want nil", lock)
	}

	if _, err := input.Write([]byte("release")); err != nil {
		t.Fatalf("write helper release signal: %v", err)
	}
	if err := input.Close(); err != nil {
		t.Fatalf("close helper input: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("helper process error = %v", err)
	}
	waited = true

	lock, err = AcquireLock(mustRuntimePaths(t, root))
	if err != nil {
		t.Fatalf("AcquireLock() after helper exit error = %v", err)
	}
	defer lock.Release()
}

func TestLockHeldBySubprocess(t *testing.T) {
	if os.Getenv("PI_TOKEN_SMITH_LOCK_HELPER") != "1" {
		return
	}

	lock, err := AcquireLock(mustRuntimePaths(t, os.Getenv("PI_TOKEN_SMITH_LOCK_ROOT")))
	if err != nil {
		t.Fatalf("AcquireLock() error = %v", err)
	}
	defer lock.Release()
	fmt.Println("locked")
	_, _ = os.Stdin.Read(make([]byte, 1))
}

func TestAcquireLockReusesStaleFileAndWritesProtectedMetadata(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	if err := EnsureRuntimeDirectory(paths); err != nil {
		t.Fatalf("EnsureRuntimeDirectory() error = %v", err)
	}
	if err := os.WriteFile(paths.Lock, []byte("stale metadata"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	lock, err := AcquireLock(paths)
	if err != nil {
		t.Fatalf("AcquireLock() with stale file error = %v", err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			t.Errorf("Release() error = %v", err)
		}
	}()

	assertMode(t, paths.Lock, 0o600)
	contents, err := os.ReadFile(paths.Lock)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var metadata struct {
		PID        int    `json:"pid"`
		AcquiredAt string `json:"acquired_at"`
		OwnerToken string `json:"owner_token"`
	}
	if err := json.Unmarshal(contents, &metadata); err != nil {
		t.Fatalf("lock metadata is not JSON: %v", err)
	}
	if metadata.PID <= 0 || metadata.AcquiredAt == "" || metadata.OwnerToken == "" {
		t.Fatalf("lock metadata = %#v, want pid, acquisition timestamp, and owner token", metadata)
	}
	if metadata.AcquiredAt[len(metadata.AcquiredAt)-1:] != "Z" {
		t.Fatalf("metadata acquisition timestamp = %q, want UTC RFC 3339 timestamp", metadata.AcquiredAt)
	}
}

func TestAcquireLockRejectsLockSymlinkWithoutMutatingTarget(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	if err := EnsureRuntimeDirectory(paths); err != nil {
		t.Fatalf("EnsureRuntimeDirectory() error = %v", err)
	}

	target := filepath.Join(t.TempDir(), "lock-target")
	original := []byte("do not alter this file")
	if err := os.WriteFile(target, original, 0o644); err != nil {
		t.Fatalf("WriteFile(target) error = %v", err)
	}
	if err := os.Symlink(target, paths.Lock); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	lock, err := AcquireLock(paths)
	if err == nil {
		if lock != nil {
			_ = lock.Release()
		}
		t.Fatal("AcquireLock() error = nil, want lock symlink rejection")
	}
	if lock != nil {
		t.Fatalf("AcquireLock() lock = %v, want nil", lock)
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile(target) error = %v", err)
	}
	if string(contents) != string(original) {
		t.Fatalf("target contents = %q, want %q", contents, original)
	}
	assertMode(t, target, 0o644)
}

func mustRuntimePaths(t *testing.T, root string) RuntimePaths {
	t.Helper()
	paths, err := NewRuntimePaths(root)
	if err != nil {
		t.Fatalf("NewRuntimePaths() error = %v", err)
	}
	return paths
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode for %q = %04o, want %04o", path, got, want)
	}
}
