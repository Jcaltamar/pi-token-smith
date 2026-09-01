//go:build linux

package daemon

import (
	"crypto/rand"
	"encoding/base64"
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

const httpTokenBytes = 32

// LoadOrCreateHTTPToken returns the private HTTP bearer token stored beneath the
// protected runtime directory. The token is never suitable for display or logs.
func LoadOrCreateHTTPToken(paths RuntimePaths) (string, error) {
	if filepath.Clean(filepath.Dir(paths.HTTPToken)) != filepath.Clean(paths.Root) || filepath.Base(paths.HTTPToken) != "http.token" {
		return "", errors.New("HTTP token path is outside runtime directory")
	}
	if err := EnsureRuntimeDirectory(paths); err != nil {
		return "", err
	}
	directory, err := openRuntimeDirectory(paths.Root)
	if err != nil {
		return "", err
	}
	defer unix.Close(directory)

	for attempts := 0; attempts < 2; attempts++ {
		token, exists, err := readHTTPTokenAt(directory)
		if err != nil {
			return "", err
		}
		if exists {
			return token, nil
		}
		created, err := createHTTPTokenAt(directory)
		if err == nil {
			return created, nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return "", err
		}
	}
	return "", errors.New("HTTP token creation did not converge")
}

func readHTTPTokenAt(directory int) (string, bool, error) {
	var pathStat unix.Stat_t
	if err := unix.Fstatat(directory, "http.token", &pathStat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
		return "", false, nil
	} else if err != nil {
		return "", false, fmt.Errorf("inspect HTTP token without following symlinks: %w", err)
	}
	// Reject special nodes before open: opening a FIFO for reading can block.
	if pathStat.Mode&unix.S_IFMT != unix.S_IFREG {
		return "", false, errors.New("HTTP token is not a regular file")
	}
	fd, err := unix.Openat(directory, "http.token", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", false, fmt.Errorf("open HTTP token without following symlinks: %w", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return "", false, fmt.Errorf("inspect HTTP token: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return "", false, errors.New("HTTP token is not a regular file")
	}
	if err := unix.Fchmod(fd, uint32(privateFileMode)); err != nil {
		return "", false, fmt.Errorf("protect HTTP token: %w", err)
	}
	if stat.Size < 43 || stat.Size > 512 {
		return "", false, errors.New("HTTP token is malformed")
	}
	data := make([]byte, stat.Size)
	for read := 0; read < len(data); {
		n, err := unix.Read(fd, data[read:])
		if err != nil {
			return "", false, fmt.Errorf("read HTTP token: %w", err)
		}
		if n == 0 {
			return "", false, errors.New("HTTP token is malformed")
		}
		read += n
	}
	token := string(data)
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) < httpTokenBytes || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return "", false, errors.New("HTTP token is malformed")
	}
	return token, true, nil
}

func createHTTPTokenAt(directory int) (string, error) {
	data := make([]byte, httpTokenBytes)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		return "", fmt.Errorf("generate HTTP token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(data)
	temporarySuffix, err := randomOwnerToken()
	if err != nil {
		return "", fmt.Errorf("generate HTTP token temporary name: %w", err)
	}
	temporaryName := ".http.token.tmp-" + temporarySuffix
	fd, err := unix.Openat(directory, temporaryName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(privateFileMode))
	if err != nil {
		return "", err
	}
	defer unix.Close(fd)
	defer unix.Unlinkat(directory, temporaryName, 0)
	if err := unix.Fchmod(fd, uint32(privateFileMode)); err != nil {
		return "", fmt.Errorf("protect HTTP token: %w", err)
	}
	for remaining := []byte(token); len(remaining) > 0; {
		written, err := unix.Write(fd, remaining)
		if err != nil {
			return "", fmt.Errorf("write HTTP token: %w", err)
		}
		if written == 0 {
			return "", errors.New("write HTTP token: no progress")
		}
		remaining = remaining[written:]
	}
	if err := unix.Fsync(fd); err != nil {
		return "", fmt.Errorf("sync HTTP token: %w", err)
	}
	if err := unix.Renameat2(directory, temporaryName, directory, "http.token", unix.RENAME_NOREPLACE); err != nil {
		return "", err
	}
	return token, nil
}

type socketIdentity struct {
	device uint64
	inode  uint64
}

func socketIdentityAt(paths RuntimePaths) (*socketIdentity, error) {
	directory, err := openRuntimeDirectory(paths.Root)
	if err != nil {
		return nil, err
	}
	defer unix.Close(directory)
	var stat unix.Stat_t
	if err := unix.Fstatat(directory, filepath.Base(paths.Socket), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, fmt.Errorf("inspect bound Unix socket: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return nil, errors.New("bound socket path is not a Unix socket")
	}
	return &socketIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
}

// removeStaleSocket atomically quarantines the candidate before inspecting it.
// The runtime directory and daemon lock establish authority over this namespace.
func removeStaleSocket(paths RuntimePaths, expected *socketIdentity) error {
	directory, err := openRuntimeDirectory(paths.Root)
	if err != nil {
		return err
	}
	defer unix.Close(directory)
	name := filepath.Base(paths.Socket)
	var quarantine string
	for i := 0; i < 4; i++ {
		token, tokenErr := randomOwnerToken()
		if tokenErr != nil {
			return fmt.Errorf("generate socket quarantine name: %w", tokenErr)
		}
		quarantine = "." + name + ".quarantine-" + token
		var stat unix.Stat_t
		if err := unix.Fstatat(directory, quarantine, &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
			break
		}
		if i == 3 {
			return errors.New("allocate unique socket quarantine name")
		}
	}
	if err := unix.Renameat(directory, name, directory, quarantine); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("quarantine Unix socket: %w", err)
	}
	var stat unix.Stat_t
	inspectErr := unix.Fstatat(directory, quarantine, &stat, unix.AT_SYMLINK_NOFOLLOW)
	valid := inspectErr == nil && stat.Mode&unix.S_IFMT == unix.S_IFSOCK && (expected == nil || (uint64(stat.Dev) == expected.device && uint64(stat.Ino) == expected.inode))
	if valid {
		if err := unix.Unlinkat(directory, quarantine, 0); err != nil {
			return fmt.Errorf("remove quarantined Unix socket: %w", err)
		}
		return nil
	}
	// Never overwrite a node created after quarantine; preserve the unexpected node.
	if err := unix.Renameat2(directory, quarantine, directory, name, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("unexpected socket node preserved at quarantine after failed restore: %w", err)
	}
	if inspectErr != nil {
		return fmt.Errorf("inspect quarantined socket: %w", inspectErr)
	}
	return errors.New("socket path exists but is not the expected Unix socket")
}
