package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	Version = 1

	// MaxControlFrameLength bounds JSON RPC envelopes. Evidence travels over the
	// data plane and is never included in a control frame.
	MaxControlFrameLength = 1 << 20
	evidenceCopyBufferSize = 32 << 10
)

var ErrControlFrameTooLarge = errors.New("protocol control frame length exceeds safety bound")

// ErrFrameTooLarge is retained for callers that used the original control-frame
// API. New code should use ErrControlFrameTooLarge.
var ErrFrameTooLarge = ErrControlFrameTooLarge

// EvidenceMetadata verifies the exact evidence bytes streamed through the data plane.
type EvidenceMetadata struct {
	ByteCount uint64
	SHA256    string
}

// Request is a versioned RPC request. Body contains one complete JSON value.
type Request struct {
	ProtocolVersion int             `json:"protocol_version"`
	RequestID       string          `json:"request_id"`
	Operation       string          `json:"operation"`
	SentAt          time.Time       `json:"sent_at"`
	Body            json.RawMessage `json:"body"`
}

// Response is a versioned RPC response.
type Response struct {
	ProtocolVersion int             `json:"protocol_version"`
	RequestID       string          `json:"request_id"`
	Status          string          `json:"status"`
	Body            json.RawMessage `json:"body"`
	Error           *ResponseError  `json:"error,omitempty"`
}

// ResponseError provides a stable machine-readable error code and message.
type ResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Encode serializes value as compact JSON and writes it as one complete frame.
func Encode(w io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal protocol message: %w", err)
	}
	return WriteFrame(w, payload)
}

// Decode reads one frame and unmarshals its JSON payload into value.
func Decode(r io.Reader, value any) error {
	payload, err := ReadFrame(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, value); err != nil {
		return fmt.Errorf("unmarshal protocol message: %w", err)
	}
	return nil
}

// WriteFrame writes one bounded control-plane JSON frame. Evidence must use
// WriteEvidence so arbitrary declared data-plane lengths are never allocated.
func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxControlFrameLength {
		return ErrControlFrameTooLarge
	}
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return fmt.Errorf("write protocol frame header: %w", err)
	}
	if err := writeAll(w, payload); err != nil {
		return fmt.Errorf("write protocol frame payload: %w", err)
	}
	return nil
}

// WriteEvidence writes a data-plane length header and streams exactly length
// bytes from evidence. It uses bounded buffering and returns integrity metadata.
func WriteEvidence(w io.Writer, length uint64, evidence io.Reader) (EvidenceMetadata, error) {
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], length)
	if err := writeAll(w, header[:]); err != nil {
		return EvidenceMetadata{}, fmt.Errorf("write evidence header: %w", err)
	}
	metadata, err := streamEvidence(evidence, w, length)
	if err != nil {
		return EvidenceMetadata{}, fmt.Errorf("write evidence payload: %w", err)
	}
	return metadata, nil
}

// ReadEvidence reads a data-plane length header and streams exactly its declared
// payload to destination. It never allocates based on the declared length.
func ReadEvidence(r io.Reader, destination io.Writer) (EvidenceMetadata, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return EvidenceMetadata{}, fmt.Errorf("read evidence header: %w", err)
	}
	metadata, err := streamEvidence(r, destination, binary.BigEndian.Uint64(header[:]))
	if err != nil {
		return EvidenceMetadata{}, fmt.Errorf("read evidence payload: %w", err)
	}
	return metadata, nil
}

func streamEvidence(source io.Reader, destination io.Writer, length uint64) (EvidenceMetadata, error) {
	hash := sha256.New()
	buffer := make([]byte, evidenceCopyBufferSize)
	remaining := length
	for remaining > 0 {
		readSize := uint64(len(buffer))
		if remaining < readSize {
			readSize = remaining
		}
		n, err := source.Read(buffer[:int(readSize)])
		if n > 0 {
			chunk := buffer[:n]
			if writeErr := writeAll(destination, chunk); writeErr != nil {
				return EvidenceMetadata{}, writeErr
			}
			if _, hashErr := hash.Write(chunk); hashErr != nil {
				return EvidenceMetadata{}, hashErr
			}
			remaining -= uint64(n)
		}
		if err != nil {
			if err == io.EOF && remaining > 0 {
				return EvidenceMetadata{}, io.ErrUnexpectedEOF
			}
			if remaining > 0 {
				return EvidenceMetadata{}, err
			}
		}
		if n == 0 {
			return EvidenceMetadata{}, io.ErrNoProgress
		}
	}
	return EvidenceMetadata{ByteCount: length, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writeAll(w io.Writer, remaining []byte) error {
	for len(remaining) > 0 {
		n, err := w.Write(remaining)
		if n < 0 || n > len(remaining) {
			return errors.New("invalid write count")
		}
		remaining = remaining[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// ReadFrame reads one bounded control-plane JSON frame. It rejects lengths
// before allocation; use ReadEvidence for unbounded data-plane evidence.
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read protocol frame header: %w", err)
	}
	length := binary.BigEndian.Uint64(header[:])
	if length > MaxControlFrameLength {
		return nil, ErrControlFrameTooLarge
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read protocol frame payload: %w", err)
	}
	return payload, nil
}
