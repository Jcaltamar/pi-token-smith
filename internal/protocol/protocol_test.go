package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type shortWriter struct {
	bytes.Buffer
	max int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.Buffer.Write(p)
}

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("{\"message\":\"hello\\n世界\"}")
	var wire bytes.Buffer

	if err := WriteFrame(&wire, payload); err != nil {
		t.Fatalf("WriteFrame() error = %v", err)
	}
	if got := binary.BigEndian.Uint64(wire.Bytes()[:8]); got != uint64(len(payload)) {
		t.Fatalf("frame length = %d, want %d", got, len(payload))
	}
	if got := wire.Bytes()[8:]; !bytes.Equal(got, payload) {
		t.Fatalf("frame payload = %q, want %q", got, payload)
	}

	got, err := ReadFrame(&fragmentReader{reader: bytes.NewReader(wire.Bytes()), size: 2})
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadFrame() = %q, want %q", got, payload)
	}
}

func TestWriteFrameCompletesShortWrites(t *testing.T) {
	payload := []byte(`{"ok":true}`)
	writer := &shortWriter{max: 3}

	if err := WriteFrame(writer, payload); err != nil {
		t.Fatalf("WriteFrame() error = %v", err)
	}
	got, err := ReadFrame(bytes.NewReader(writer.Bytes()))
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadFrame() = %q, want %q", got, payload)
	}
}

func TestReadFrameConsecutiveFrames(t *testing.T) {
	var wire bytes.Buffer
	for _, payload := range [][]byte{[]byte(`{"one":1}`), []byte(`{"two":2}`)} {
		if err := WriteFrame(&wire, payload); err != nil {
			t.Fatalf("WriteFrame() error = %v", err)
		}
	}

	for _, want := range [][]byte{[]byte(`{"one":1}`), []byte(`{"two":2}`)} {
		got, err := ReadFrame(&wire)
		if err != nil {
			t.Fatalf("ReadFrame() error = %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadFrame() = %q, want %q", got, want)
		}
	}
}

func TestDecodeRejectsInvalidJSONAndEmptyFrames(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte(`{"unterminated":`)} {
		var wire bytes.Buffer
		if err := WriteFrame(&wire, payload); err != nil {
			t.Fatalf("WriteFrame() error = %v", err)
		}
		var target Request
		if err := Decode(&wire, &target); err == nil {
			t.Fatalf("Decode(%q) error = nil, want error", payload)
		}
	}
}

func TestControlFramesEnforceSafetyBound(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, MaxControlFrameLength)
	var wire bytes.Buffer
	if err := WriteFrame(&wire, payload); err != nil {
		t.Fatalf("WriteFrame() at bound error = %v", err)
	}
	if _, err := ReadFrame(&wire); err != nil {
		t.Fatalf("ReadFrame() at bound error = %v", err)
	}

	tooLarge := bytes.Repeat([]byte{'x'}, MaxControlFrameLength+1)
	if err := WriteFrame(io.Discard, tooLarge); !errors.Is(err, ErrControlFrameTooLarge) {
		t.Fatalf("WriteFrame() above bound error = %v, want ErrControlFrameTooLarge", err)
	}

	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(MaxControlFrameLength+1))
	_, err := ReadFrame(bytes.NewReader(header[:]))
	if !errors.Is(err, ErrControlFrameTooLarge) {
		t.Fatalf("ReadFrame() above bound error = %v, want ErrControlFrameTooLarge", err)
	}
}

// TestEvidenceHeaderSharedProtocolGoldenVector locks the shared Go/TypeScript
// protocol vector: uint64(0x0102030405060708) encodes as eight big-endian bytes.
func TestEvidenceHeaderSharedProtocolGoldenVector(t *testing.T) {
	const declaredLength uint64 = 0x0102030405060708
	want := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	var wire bytes.Buffer

	_, err := WriteEvidence(&wire, declaredLength, bytes.NewReader(nil))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("WriteEvidence() error = %v, want io.ErrUnexpectedEOF", err)
	}
	if got := wire.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("evidence header = %x, want %x", got, want)
	}
}

func TestEvidenceHeaderBoundsFollowingFrame(t *testing.T) {
	var wire bytes.Buffer
	if err := WriteEvidenceHeader(&wire, 3); err != nil { t.Fatalf("WriteEvidenceHeader() error = %v", err) }
	wire.WriteString("one")
	if err := WriteFrame(&wire, []byte(`{"next":true}`)); err != nil { t.Fatalf("WriteFrame() error = %v", err) }
	length, err := ReadEvidenceHeader(&wire)
	if err != nil || length != 3 { t.Fatalf("ReadEvidenceHeader() = %d, %v; want 3, nil", length, err) }
	var evidence bytes.Buffer
	if _, err := io.Copy(&evidence, io.LimitReader(&wire, int64(length))); err != nil { t.Fatalf("copy evidence: %v", err) }
	if evidence.String() != "one" { t.Fatalf("evidence = %q", evidence.String()) }
	frame, err := ReadFrame(&wire)
	if err != nil || !bytes.Equal(frame, []byte(`{"next":true}`)) { t.Fatalf("following frame = %q, %v", frame, err) }
}

func TestEvidenceStreamsExactlyAndHashes(t *testing.T) {
	payload := []byte{0, 10, 240, 159, 140, 141}
	var wire bytes.Buffer
	written, err := WriteEvidence(&wire, uint64(len(payload)), &fragmentReader{reader: bytes.NewReader(payload), size: 2})
	if err != nil {
		t.Fatalf("WriteEvidence() error = %v", err)
	}
	if written.ByteCount != uint64(len(payload)) || written.SHA256 != "f7cf928c740e3e790c5a85843c8a184fa5c65513e249aacfcffe7c2db93bffef" {
		t.Fatalf("WriteEvidence() metadata = %#v", written)
	}
	if got := wire.Bytes()[8:]; !bytes.Equal(got, payload) {
		t.Fatalf("written payload = %v, want %v", got, payload)
	}

	var copied bytes.Buffer
	read, err := ReadEvidence(&fragmentReader{reader: bytes.NewReader(wire.Bytes()), size: 3}, &copied)
	if err != nil {
		t.Fatalf("ReadEvidence() error = %v", err)
	}
	if !bytes.Equal(copied.Bytes(), payload) || read != written {
		t.Fatalf("ReadEvidence() = (%#v, %v), want (%#v, %v)", read, copied.Bytes(), written, payload)
	}
}

func TestEvidenceStreamsRejectTruncationWithoutDeclaredLengthAllocation(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		var wire bytes.Buffer
		_, err := WriteEvidence(&wire, 2, bytes.NewReader([]byte{'x'}))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("WriteEvidence() error = %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("read fragmented truncation", func(t *testing.T) {
		var wire bytes.Buffer
		binary.Write(&wire, binary.BigEndian, uint64(3))
		wire.Write([]byte{'x', 'y'})
		var copied bytes.Buffer
		_, err := ReadEvidence(&fragmentReader{reader: bytes.NewReader(wire.Bytes()), size: 1}, &copied)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadEvidence() error = %v, want io.ErrUnexpectedEOF", err)
		}
		if got := copied.String(); got != "xy" {
			t.Fatalf("ReadEvidence() copied %q, want %q", got, "xy")
		}
	})
	t.Run("read huge declaration", func(t *testing.T) {
		var header [8]byte
		binary.BigEndian.PutUint64(header[:], ^uint64(0))
		var copied bytes.Buffer
		_, err := ReadEvidence(bytes.NewReader(header[:]), &copied)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadEvidence() error = %v, want io.ErrUnexpectedEOF", err)
		}
		if copied.Len() != 0 {
			t.Fatalf("ReadEvidence() wrote %d bytes, want 0", copied.Len())
		}
	})
}

func TestEnvelopeJSON(t *testing.T) {
	sentAt := time.Date(2026, time.April, 13, 0, 0, 0, 0, time.UTC)
	request := Request{ProtocolVersion: Version, RequestID: "request-1", Operation: "capture.append", SentAt: sentAt, Body: []byte(`{"event":"世界"}`)}
	response := Response{ProtocolVersion: Version, RequestID: "request-1", Status: "error", Error: &ResponseError{Code: "invalid_request", Message: "request is invalid"}}

	var requestWire, responseWire bytes.Buffer
	if err := Encode(&requestWire, request); err != nil {
		t.Fatalf("Encode(request) error = %v", err)
	}
	if err := Encode(&responseWire, response); err != nil {
		t.Fatalf("Encode(response) error = %v", err)
	}
	if !strings.Contains(requestWire.String(), `"protocol_version":1`) || !strings.Contains(requestWire.String(), `"sent_at":"2026-04-13T00:00:00Z"`) {
		t.Fatalf("request JSON = %s", requestWire.String())
	}
	if !strings.Contains(responseWire.String(), `"body":null`) || !strings.Contains(responseWire.String(), `"code":"invalid_request"`) {
		t.Fatalf("response JSON = %s", responseWire.String())
	}

	var decoded Request
	if err := Decode(&requestWire, &decoded); err != nil {
		t.Fatalf("Decode(request) error = %v", err)
	}
	if !bytes.Equal(decoded.Body, request.Body) || !decoded.SentAt.Equal(sentAt) {
		t.Fatalf("decoded request = %#v, want body %q and sent_at %v", decoded, request.Body, sentAt)
	}
}

type fragmentReader struct {
	reader io.Reader
	size   int
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	if len(p) > r.size {
		p = p[:r.size]
	}
	return r.reader.Read(p)
}
