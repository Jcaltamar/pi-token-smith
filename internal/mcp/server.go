//go:build linux

// Package mcp exposes Token Smith's daemon-backed MCP stdio interface.
package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"unicode/utf8"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const Version = "0.1.0-dev"

const (
	maxReadPayload = 65536
	maxSearchLimit = 100
	maxJSONInteger = uint64(1<<53 - 1)
)

var errInvalidArguments = errors.New("invalid arguments")

// Client is the daemon RPC surface required by MCP. It intentionally has no storage access.
type Client interface {
	Health(context.Context) (client.Health, error)
	Info(context.Context) (client.Info, error)
	Search(context.Context, string, int) ([]client.EventReference, error)
	EventMetadata(context.Context, string) (client.EventMetadata, error)
	ReadPayload(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error)
}

// Server owns the MCP tool handlers and transports them over injected stdio streams.
type Server struct {
	client Client
	server *server.MCPServer
	stderr io.Writer
}

func New(c Client, stderr io.Writer) *Server {
	if stderr == nil {
		stderr = io.Discard
	}
	s := &Server{client: c, stderr: stderr}
	s.server = server.NewMCPServer("Pi Token Smith", Version)
	s.server.AddTool(mcplib.NewToolWithRawSchema("token_smith_status", "Return daemon health and protocol information without evidence.", rawSchema(`{"type":"object","additionalProperties":false}`)), s.handle)
	s.server.AddTool(mcplib.NewToolWithRawSchema("token_smith_search", "Search event references. Results contain metadata only, never evidence payloads.", rawSchema(`{"type":"object","additionalProperties":false,"properties":{"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["query"]}`)), s.handle)
	s.server.AddTool(mcplib.NewToolWithRawSchema("token_smith_get_event", "Return available event integrity metadata without evidence payloads.", rawSchema(`{"type":"object","additionalProperties":false,"properties":{"event_id":{"type":"string","minLength":1}},"required":["event_id"]}`)), s.handle)
	s.server.AddTool(mcplib.NewToolWithRawSchema("token_smith_read_payload", "Read an explicit bounded raw evidence byte range; utf8 rejects invalid bytes and base64 is lossless.", rawSchema(`{"type":"object","additionalProperties":false,"properties":{"event_id":{"type":"string","minLength":1},"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":65536},"encoding":{"type":"string","enum":["utf8","base64"]}},"required":["event_id","offset","limit","encoding"]}`)), s.handle)
	return s
}

func rawSchema(schema string) json.RawMessage { return json.RawMessage(schema) }

// Listen serves MCP JSON-RPC until EOF or context cancellation. stdout receives protocol messages only.
func (s *Server) Listen(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	stdio := server.NewStdioServer(s.server)
	stdio.SetErrorLogger(log.New(s.stderr, "", 0))
	return stdio.Listen(ctx, stdin, stdout)
}

func (s *Server) handle(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	if err := ctx.Err(); err != nil {
		return toolError(err), nil
	}
	if s.client == nil {
		return toolError(errors.New("backend unavailable")), nil
	}
	switch request.Params.Name {
	case "token_smith_status":
		return s.status(ctx, request)
	case "token_smith_search":
		return s.search(ctx, request)
	case "token_smith_get_event":
		return s.getEvent(ctx, request)
	case "token_smith_read_payload":
		return s.readPayload(ctx, request)
	default:
		return toolError(errors.New("invalid tool request")), nil
	}
}

func (s *Server) status(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	if !emptyArguments(request) {
		return toolError(errInvalidArguments), nil
	}
	health, err := s.client.Health(ctx)
	if err != nil {
		return toolError(err), nil
	}
	info, err := s.client.Info(ctx)
	if err != nil {
		return toolError(err), nil
	}
	return jsonResult(struct {
		Health client.Health `json:"health"`
		Info   client.Info   `json:"info"`
	}{health, info})
}
func (s *Server) search(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	args, ok := strictArguments(request, "query", "limit")
	if !ok {
		return toolError(errInvalidArguments), nil
	}
	query, ok := requiredString(args, "query")
	if !ok {
		return toolError(errInvalidArguments), nil
	}
	limit := maxSearchLimit
	if value, exists := args["limit"]; exists {
		var valid bool
		limit, valid = positiveInt(value, 1, maxSearchLimit)
		if !valid {
			return toolError(errInvalidArguments), nil
		}
	}
	events, err := s.client.Search(ctx, query, limit)
	if err != nil {
		return toolError(err), nil
	}
	return jsonResult(struct {
		Events []client.EventReference `json:"events"`
	}{events})
}
func (s *Server) getEvent(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	args, ok := strictArguments(request, "event_id")
	if !ok {
		return toolError(errInvalidArguments), nil
	}
	id, ok := requiredString(args, "event_id")
	if !ok {
		return toolError(errInvalidArguments), nil
	}
	metadata, err := s.client.EventMetadata(ctx, id)
	if err != nil {
		return toolError(err), nil
	}
	return jsonResult(metadata)
}
func (s *Server) readPayload(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	args, ok := strictArguments(request, "event_id", "offset", "limit", "encoding")
	if !ok {
		return toolError(errInvalidArguments), nil
	}
	id, idOK := requiredString(args, "event_id")
	offset, offsetOK := nonnegativeInt(args["offset"])
	limit, limitOK := positiveInt(args["limit"], 1, maxReadPayload)
	encoding, encodingOK := requiredString(args, "encoding")
	if !idOK || !offsetOK || !limitOK || !encodingOK || (encoding != "utf8" && encoding != "base64") {
		return toolError(errInvalidArguments), nil
	}
	var payload bytes.Buffer
	metadata, err := s.client.ReadPayload(ctx, id, offset, uint64(limit), &payload)
	if err != nil {
		return toolError(err), nil
	}
	data := payload.Bytes()
	if !validPayloadResponse(metadata, id, offset, uint64(limit), data) {
		return toolError(errors.New("backend returned invalid payload metadata")), nil
	}
	content := ""
	if encoding == "utf8" {
		if !utf8.Valid(data) {
			return mcplib.NewToolResultError("payload is not valid UTF-8"), nil
		}
		content = string(data)
	} else {
		content = base64.StdEncoding.EncodeToString(data)
	}
	return jsonResult(struct {
		EventID       string `json:"event_id"`
		TotalSize     uint64 `json:"total_size"`
		SHA256        string `json:"sha256"`
		Offset        uint64 `json:"offset"`
		BytesReturned uint64 `json:"bytes_returned"`
		Encoding      string `json:"encoding"`
		Content       string `json:"content"`
	}{metadata.EventID, metadata.TotalSize, metadata.SHA256, metadata.Offset, metadata.BytesWritten, encoding, content})
}

func emptyArguments(request mcplib.CallToolRequest) bool {
	args := request.GetArguments()
	return args != nil && len(args) == 0
}
func strictArguments(request mcplib.CallToolRequest, allowed ...string) (map[string]any, bool) {
	args := request.GetArguments()
	if args == nil {
		return nil, false
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range args {
		if _, ok := allowedSet[key]; !ok {
			return nil, false
		}
	}
	return args, true
}
func requiredString(args map[string]any, key string) (string, bool) {
	value, ok := args[key].(string)
	return value, ok && value != ""
}
func nonnegativeInt(value any) (uint64, bool) {
	number, ok := value.(float64)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) || number < 0 || number > float64(maxJSONInteger) || math.Trunc(number) != number {
		return 0, false
	}
	return uint64(number), true
}
func positiveInt(value any, min, max int) (int, bool) {
	number, ok := nonnegativeInt(value)
	if !ok || number > uint64(max) || number < uint64(min) {
		return 0, false
	}
	return int(number), true
}

func validPayloadResponse(metadata client.PayloadMetadata, eventID string, offset, limit uint64, data []byte) bool {
	if metadata.EventID != eventID || metadata.Offset != offset || metadata.Limit != limit || metadata.TotalSize < offset || len(metadata.SHA256) != 64 {
		return false
	}
	if _, err := hex.DecodeString(metadata.SHA256); err != nil {
		return false
	}
	expected := metadata.TotalSize - offset
	if expected > limit {
		expected = limit
	}
	return metadata.BytesWritten == uint64(len(data)) && metadata.BytesWritten == expected
}
func jsonResult(value any) (*mcplib.CallToolResult, error) { return mcplib.NewToolResultJSON(value) }
func toolError(err error) *mcplib.CallToolResult {
	if errors.Is(err, errInvalidArguments) {
		return mcplib.NewToolResultError("invalid arguments")
	}
	if errors.Is(err, context.Canceled) {
		return mcplib.NewToolResultError("request cancelled")
	}
	var rpc *client.RPCError
	if errors.As(err, &rpc) && rpc.Code == "not_found" {
		return mcplib.NewToolResultError("event not found")
	}
	return mcplib.NewToolResultError("backend unavailable")
}
