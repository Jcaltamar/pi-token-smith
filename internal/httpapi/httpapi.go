//go:build linux

// Package httpapi exposes the daemon read-only RPC surface over an explicit,
// authenticated loopback HTTP server.
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/client"
)

const maxSearchResults = 100

type Client interface {
	Health(context.Context) (client.Health, error)
	Info(context.Context) (client.Info, error)
	Search(context.Context, string, int) ([]client.EventReference, error)
	ReadPayload(context.Context, string, uint64, uint64, io.Writer) (client.PayloadMetadata, error)
}

type Options struct {
	Listen                                                    string
	ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	MaxHeaderBytes                                            int
}

type Server struct {
	client   Client
	token    string
	options  Options
	listener net.Listener
	http     *http.Server
	mu       sync.Mutex
	closed   bool
}

func New(c Client, token string, options Options) (*Server, error) {
	if c == nil || len(token) < 43 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("HTTP server configuration is invalid")
	}
	if options.Listen == "" {
		options.Listen = "127.0.0.1:0"
	}
	listen, err := resolveLoopbackListen(options.Listen)
	if err != nil {
		return nil, err
	}
	options.Listen = listen
	if options.ReadHeaderTimeout <= 0 {
		options.ReadHeaderTimeout = 5 * time.Second
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = 30 * time.Second
	}
	// Payload writes use per-write progress deadlines, so this is deliberately
	// unset rather than a total response deadline that would truncate evidence.
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = 60 * time.Second
	}
	if options.MaxHeaderBytes <= 0 {
		options.MaxHeaderBytes = 16 << 10
	}
	return &Server{client: c, token: token, options: options}, nil
}

func resolveLoopbackListen(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "", errors.New("HTTP listen address must be loopback")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !ip.IsLoopback() {
			return "", errors.New("HTTP listen address must be loopback")
		}
		return address, nil
	}
	if !strings.EqualFold(host, "localhost") {
		return "", errors.New("HTTP listen address must be loopback")
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return "", errors.New("resolve localhost for HTTP listen: unavailable")
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return "", errors.New("resolve localhost for HTTP listen: non-loopback result")
		}
	}
	// Bind the verified literal, never a hostname that can be rebound later.
	return net.JoinHostPort(ips[0].String(), port), nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.listener != nil {
		return errors.New("HTTP server is closed or started")
	}
	listener, err := net.Listen("tcp", s.options.Listen)
	if err != nil {
		return err
	}
	s.listener = listener
	s.http = &http.Server{Handler: s, ReadHeaderTimeout: s.options.ReadHeaderTimeout, ReadTimeout: s.options.ReadTimeout, WriteTimeout: s.options.WriteTimeout, IdleTimeout: s.options.IdleTimeout, MaxHeaderBytes: s.options.MaxHeaderBytes}
	go func() { _ = s.http.Serve(listener) }()
	return nil
}
func (s *Server) URL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return "http://" + s.listener.Addr().String()
}
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	server := s.http
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodOptions || hasHeaderKey(r.Header, "Origin") {
		s.error(w, http.StatusForbidden, "forbidden")
		return
	}
	if !s.validHost(r.Host) {
		s.error(w, http.StatusForbidden, "forbidden")
		return
	}
	if r.ContentLength != 0 || r.Body == nil {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if r.URL.Query().Has("token") || r.URL.Query().Has("access_token") || r.URL.Query().Has("authorization") {
		s.error(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !s.authorized(r.Header.Values("Authorization")) {
		s.error(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	s.route(w, r)
}
func (s *Server) validHost(host string) bool {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		return false
	}
	actualHost, actualPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return false
	}
	hostName, port, err := net.SplitHostPort(host)
	if err != nil || port != actualPort {
		return false
	}
	return hostName == actualHost
}
func (s *Server) authorized(values []string) bool {
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	candidate := strings.TrimPrefix(values[0], "Bearer ")
	if candidate == "" || strings.ContainsAny(candidate, " \t\r\n") || len(candidate) != len(s.token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(s.token)) == 1
}
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/health":
		if !onlyQueryKeys(r.URL.Query()) {
			s.error(w, http.StatusBadRequest, "invalid_request")
			return
		}
		s.json(w, http.StatusOK, s.mustHealth(r.Context()))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/info":
		if !onlyQueryKeys(r.URL.Query()) {
			s.error(w, http.StatusBadRequest, "invalid_request")
			return
		}
		s.json(w, http.StatusOK, s.mustInfo(r.Context()))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/search":
		s.search(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/events/") && strings.HasSuffix(r.URL.Path, "/payload"):
		s.payload(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/"):
		s.error(w, http.StatusNotFound, "not_found")
	default:
		s.error(w, http.StatusNotFound, "not_found")
	}
}
func (s *Server) mustHealth(ctx context.Context) any {
	value, err := s.client.Health(ctx)
	if err != nil {
		return nil
	}
	return value
}
func (s *Server) mustInfo(ctx context.Context) any {
	value, err := s.client.Info(ctx)
	if err != nil {
		return nil
	}
	return value
}
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if !onlyQueryKeys(query, "q", "limit") {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	q, ok := singleQueryValue(query, "q", true)
	if !ok || strings.TrimSpace(q) == "" {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	limit := 10
	if raw, ok := singleQueryValue(query, "limit", false); !ok {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	} else if raw != "" {
		var err error
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxSearchResults {
			s.error(w, http.StatusBadRequest, "invalid_request")
			return
		}
	}
	events, err := s.client.Search(r.Context(), q, limit)
	if err != nil {
		s.error(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	s.json(w, http.StatusOK, map[string]any{"events": events})
}
func (s *Server) payload(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if !onlyQueryKeys(query, "offset", "limit") {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/events/"), "/payload")
	if id == "" || strings.Contains(id, "/") {
		s.error(w, http.StatusNotFound, "not_found")
		return
	}
	offset, ok := uintQuery(query, "offset")
	if !ok {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	limit, ok := uintQuery(query, "limit")
	if !ok {
		s.error(w, http.StatusBadRequest, "invalid_request")
		return
	}
	stream := &payloadWriter{ResponseWriter: w}
	metadata, err := s.client.ReadPayload(r.Context(), id, offset, limit, stream)
	if err != nil {
		if !stream.started {
			status, code := payloadError(err)
			s.error(w, status, code)
			return
		}
		// A status code cannot change after evidence starts. The declared trailer
		// makes the incomplete stream explicit to HTTP clients after EOF.
		w.Header().Set("X-Payload-Error", "stream_error")
		return
	}
	stream.commit()
	w.Header().Set("X-Payload-Size", strconv.FormatUint(metadata.TotalSize, 10))
	w.Header().Set("X-Payload-SHA256", metadata.SHA256)
	w.Header().Set("X-Payload-Offset", strconv.FormatUint(metadata.Offset, 10))
	w.Header().Set("X-Payload-Bytes", strconv.FormatUint(metadata.BytesWritten, 10))
}

func payloadError(err error) (int, string) {
	var rpcError *client.RPCError
	if errors.As(err, &rpcError) && rpcError.Code == "not_found" {
		return http.StatusNotFound, "not_found"
	}
	return http.StatusServiceUnavailable, "unavailable"
}

type payloadWriter struct {
	http.ResponseWriter
	started bool
}

func (w *payloadWriter) commit() {
	if w.started {
		return
	}
	w.started = true
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Trailer", "X-Payload-Size")
	w.Header().Add("Trailer", "X-Payload-SHA256")
	w.Header().Add("Trailer", "X-Payload-Offset")
	w.Header().Add("Trailer", "X-Payload-Bytes")
	w.Header().Add("Trailer", "X-Payload-Error")
}

func (w *payloadWriter) Write(data []byte) (int, error) {
	if len(data) > 0 {
		w.commit()
	}
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(30 * time.Second))
	return w.ResponseWriter.Write(data)
}

func hasHeaderKey(header http.Header, key string) bool {
	for actualKey := range header {
		if strings.EqualFold(actualKey, key) {
			return true
		}
	}
	return false
}

func onlyQueryKeys(query map[string][]string, allowed ...string) bool {
	for key := range query {
		found := false
		for _, allowedKey := range allowed {
			if key == allowedKey {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func singleQueryValue(query map[string][]string, key string, required bool) (string, bool) {
	values, present := query[key]
	if !present {
		return "", !required
	}
	if len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func uintQuery(query map[string][]string, key string) (uint64, bool) {
	raw, ok := singleQueryValue(query, key, true)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	return value, err == nil
}
func (s *Server) json(w http.ResponseWriter, status int, value any) {
	if value == nil {
		s.error(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (s *Server) error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code})
}
