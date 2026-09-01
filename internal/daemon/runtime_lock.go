//go:build linux

package daemon

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	runtimeDirectoryMode os.FileMode = 0o700
	privateFileMode      os.FileMode = 0o600
)

// ErrAlreadyRunning indicates that another daemon currently owns the runtime lock.
var ErrAlreadyRunning = errors.New("pi-token-smith daemon is already running")

// RuntimePaths identifies all daemon runtime resources beneath one protected root.
type RuntimePaths struct {
	Root      string
	Database  string
	Socket    string
	Lock      string
	HTTPToken string
}

// DefaultRuntimePaths returns paths rooted at ~/.pi/agent/pi-token-smith.
func DefaultRuntimePaths() (RuntimePaths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return RuntimePaths{}, fmt.Errorf("resolve user home directory: %w", err)
	}
	return NewRuntimePaths(filepath.Join(home, ".pi", "agent", "pi-token-smith"))
}

// NewRuntimePaths returns paths rooted at root. An empty root selects the default root.
func NewRuntimePaths(root string) (RuntimePaths, error) {
	if root == "" {
		return DefaultRuntimePaths()
	}

	return RuntimePaths{
		Root:      root,
		Database:  filepath.Join(root, "token-smith.sqlite"),
		Socket:    filepath.Join(root, "token-smith.sock"),
		Lock:      filepath.Join(root, "token-smith.lock"),
		HTTPToken: filepath.Join(root, "http.token"),
	}, nil
}

// EnsureRuntimeDirectory creates paths.Root with private permissions. Its final
// path component is opened without following symlinks, and permissions are set
// through that directory descriptor.
func EnsureRuntimeDirectory(paths RuntimePaths) error {
	if paths.Root == "" {
		return errors.New("runtime root is empty")
	}
	if err := os.MkdirAll(paths.Root, runtimeDirectoryMode); err != nil {
		return fmt.Errorf("create runtime directory: %w", err)
	}

	directory, err := openRuntimeDirectory(paths.Root)
	if err != nil {
		return err
	}
	defer unix.Close(directory)
	if err := unix.Fchmod(directory, uint32(runtimeDirectoryMode)); err != nil {
		return fmt.Errorf("protect runtime directory: %w", err)
	}
	return nil
}

func openRuntimeDirectory(root string) (int, error) {
	directory, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open runtime directory without following symlinks: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(directory, &stat); err != nil {
		_ = unix.Close(directory)
		return -1, fmt.Errorf("inspect runtime directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(directory)
		return -1, fmt.Errorf("runtime root %q is not a directory", root)
	}
	return directory, nil
}

// InstanceLock owns the daemon's process-wide advisory lock until Release.
type InstanceLock struct {
	fd int

	mu       sync.Mutex
	released bool
}

// AcquireLock non-blockingly acquires the daemon's process-wide advisory lock.
// Lock ownership is authoritative; stale metadata is safely replaced only after
// the operating system grants the lock.
func AcquireLock(paths RuntimePaths) (*InstanceLock, error) {
	if err := EnsureRuntimeDirectory(paths); err != nil {
		return nil, err
	}

	directory, err := openRuntimeDirectory(paths.Root)
	if err != nil {
		return nil, err
	}
	defer unix.Close(directory)

	fd, err := unix.Openat(directory, filepath.Base(paths.Lock), unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(privateFileMode))
	if err != nil {
		return nil, fmt.Errorf("open daemon lock without following symlinks: %w", err)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = unix.Close(fd)
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("acquire daemon lock: %w", err)
	}

	if err := writeLockMetadata(fd); err != nil {
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = unix.Close(fd)
		return nil, err
	}
	return &InstanceLock{fd: fd}, nil
}

// Release releases the advisory lock and closes its descriptor. It is safe to
// call more than once and leaves lock metadata in place for diagnostics and safe
// stale-file reuse.
func (l *InstanceLock) Release() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	unlockErr := unix.Flock(l.fd, unix.LOCK_UN)
	closeErr := unix.Close(l.fd)
	l.released = true
	if unlockErr != nil {
		return fmt.Errorf("release daemon lock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close daemon lock: %w", closeErr)
	}
	return nil
}

type lockMetadata struct {
	PID        int       `json:"pid"`
	AcquiredAt time.Time `json:"acquired_at"`
	OwnerToken string    `json:"owner_token"`
}

func writeLockMetadata(fd int) error {
	ownerToken, err := randomOwnerToken()
	if err != nil {
		return fmt.Errorf("generate lock owner token: %w", err)
	}
	if err := unix.Fchmod(fd, uint32(privateFileMode)); err != nil {
		return fmt.Errorf("protect lock file: %w", err)
	}
	if err := unix.Ftruncate(fd, 0); err != nil {
		return fmt.Errorf("truncate lock metadata: %w", err)
	}
	if _, err := unix.Seek(fd, 0, io.SeekStart); err != nil {
		return fmt.Errorf("seek lock metadata: %w", err)
	}
	metadata, err := json.Marshal(lockMetadata{
		PID:        os.Getpid(),
		AcquiredAt: time.Now().UTC(),
		OwnerToken: ownerToken,
	})
	if err != nil {
		return fmt.Errorf("encode lock metadata: %w", err)
	}
	metadata = append(metadata, '\n')
	for len(metadata) > 0 {
		written, err := unix.Write(fd, metadata)
		if err != nil {
			return fmt.Errorf("write lock metadata: %w", err)
		}
		metadata = metadata[written:]
	}
	return nil
}

func randomOwnerToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
