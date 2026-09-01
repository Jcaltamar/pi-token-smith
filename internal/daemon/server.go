//go:build linux

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/protocol"
	"github.com/Jcaltamar/pi-token-smith/internal/storage"
)

const (
	maxSearchQueryLength = 4096
	maxSearchResults     = 100
	// DefaultMaxConnections bounds daemon file descriptors and goroutines.
	DefaultMaxConnections = 64
	defaultReadTimeout    = 30 * time.Second
	defaultWriteTimeout   = 30 * time.Second
)

var ErrServerSingleUse = errors.New("daemon server is single-use")

type ServerOptions struct {
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	startupFailure func() error
	closeWaiting   func()
}

type serverState uint8
const ( serverNew serverState = iota; serverStarting; serverRunning; serverClosing; serverClosed )

type Server struct {
	paths RuntimePaths
	options ServerOptions
	lock *InstanceLock
	store *storage.Store
	listener *net.UnixListener
	socket *socketIdentity

	mu sync.Mutex
	state serverState
	startDone chan struct{}
	closeDone chan struct{}
	connections map[net.Conn]struct{}
	slots chan struct{}
	acceptWG sync.WaitGroup
	connectionWG sync.WaitGroup
}

func NewServer(paths RuntimePaths) *Server { return NewServerWithOptions(paths, ServerOptions{}) }
func NewServerWithOptions(paths RuntimePaths, options ServerOptions) *Server {
	if options.MaxConnections <= 0 { options.MaxConnections = DefaultMaxConnections }
	if options.ReadTimeout <= 0 { options.ReadTimeout = defaultReadTimeout }
	if options.WriteTimeout <= 0 { options.WriteTimeout = defaultWriteTimeout }
	return &Server{paths: paths, options: options, connections: make(map[net.Conn]struct{}), slots: make(chan struct{}, options.MaxConnections)}
}

// Start starts a single-use server. Read and write timeouts are progress-based:
// every underlying socket read or write receives a fresh deadline.
func (s *Server) Start(ctx context.Context) (err error) {
	if ctx == nil { ctx = context.Background() }
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	if s.state != serverNew { s.mu.Unlock(); return ErrServerSingleUse }
	s.state, s.startDone = serverStarting, make(chan struct{})
	s.mu.Unlock()

	var lock *InstanceLock
	var store *storage.Store
	var listener *net.UnixListener
	var identity *socketIdentity
	defer func() {
		if err != nil {
			if listener != nil { _ = listener.Close(); _ = removeStaleSocket(s.paths, identity) }
			if store != nil { _ = store.Close() }
			if lock != nil { _ = lock.Release() }
			s.mu.Lock()
			s.lock, s.store, s.listener, s.socket = nil, nil, nil, nil
			s.state = serverClosed
			close(s.startDone)
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		close(s.startDone)
		s.mu.Unlock()
	}()

	if err = EnsureRuntimeDirectory(s.paths); err != nil { return err }
	if err = ctx.Err(); err != nil { return err }
	lock, err = AcquireLock(s.paths); if err != nil { return err }
	s.mu.Lock(); s.lock = lock; s.mu.Unlock()
	if err = removeStaleSocket(s.paths, nil); err != nil { return err }
	store, err = storage.Open(ctx, s.paths.Database); if err != nil { return err }
	listener, err = net.ListenUnix("unix", &net.UnixAddr{Name:s.paths.Socket, Net:"unix"})
	if err != nil { return fmt.Errorf("listen Unix socket: %w", err) }
	listener.SetUnlinkOnClose(false)
	if err = os.Chmod(s.paths.Socket, privateFileMode); err != nil { return fmt.Errorf("protect Unix socket: %w", err) }
	identity, err = socketIdentityAt(s.paths); if err != nil { return err }
	if s.options.startupFailure != nil { if err = s.options.startupFailure(); err != nil { return err } }
	s.mu.Lock()
	s.store, s.listener, s.socket, s.state = store, listener, identity, serverRunning
	s.acceptWG.Add(1)
	s.mu.Unlock()
	go s.acceptLoop(listener)
	if done := ctx.Done(); done != nil { go func() { <-done; _ = s.Close() }() }
	return nil
}

func (s *Server) acceptLoop(listener *net.UnixListener) {
	defer s.acceptWG.Done()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil { if errors.Is(err, net.ErrClosed) { return }; continue }
		select { case s.slots <- struct{}{}: default: _ = connection.Close(); continue }
		s.mu.Lock()
		if s.state != serverRunning { s.mu.Unlock(); <-s.slots; _ = connection.Close(); return }
		s.connections[connection] = struct{}{}
		s.connectionWG.Add(1)
		s.mu.Unlock()
		go func(c *net.UnixConn) { defer s.connectionWG.Done(); defer s.dropConnection(c); s.serveConnection(&deadlineConn{UnixConn:c, readTimeout:s.options.ReadTimeout, writeTimeout:s.options.WriteTimeout}) }(connection)
	}
}
func (s *Server) dropConnection(c net.Conn) { _ = c.Close(); s.mu.Lock(); delete(s.connections,c); s.mu.Unlock(); <-s.slots }

type deadlineConn struct { *net.UnixConn; readTimeout, writeTimeout time.Duration }
func (c *deadlineConn) Read(p []byte) (int,error) { if c.readTimeout > 0 { _ = c.SetReadDeadline(time.Now().Add(c.readTimeout)) }; return c.UnixConn.Read(p) }
func (c *deadlineConn) Write(p []byte) (int,error) { if c.writeTimeout > 0 { _ = c.SetWriteDeadline(time.Now().Add(c.writeTimeout)) }; return c.UnixConn.Write(p) }

func (s *Server) serveConnection(connection net.Conn) { for { var request protocol.Request; if err:=protocol.Decode(connection,&request);err!=nil{return}; if s.handleRequest(connection,request){return} } }
func (s *Server) running() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.state == serverRunning }
func (s *Server) handleRequest(connection net.Conn, request protocol.Request) bool {
	if request.ProtocolVersion != protocol.Version { return s.writeError(connection,request.RequestID,"unsupported_protocol","protocol version is not supported") }
	if request.RequestID=="" || request.Operation=="" { return s.writeError(connection,request.RequestID,"invalid_request","request is invalid") }
	switch request.Operation {
	case "system.hello", "system.health": if !s.running() { return true }; return s.writeOK(connection,request.RequestID,map[string]any{"version":protocol.Version,"status":"healthy"})
	case "system.info": if !s.running() { return true }; return s.writeOK(connection,request.RequestID,map[string]any{"version":protocol.Version})
	case "capture.append": return s.captureAppend(connection,request)
	case "event.read_payload": return s.readPayload(connection,request)
	case "search.events": return s.searchEvents(connection,request)
	default: return s.writeError(connection,request.RequestID,"unsupported_operation","operation is not supported")
	}
}

type captureRequest struct { EventID string `json:"event_id"`; EventType string `json:"event_type"`; ProjectID string `json:"project_id"`; SessionID string `json:"session_id"`; ExchangeID string `json:"exchange_id"`; Sequence int64 `json:"sequence"`; OccurredAt time.Time `json:"occurred_at"`; Encoding string `json:"encoding"`; PayloadSize uint64 `json:"payload_size"` }
func (s *Server) captureAppend(connection net.Conn, request protocol.Request) bool {
	var body captureRequest
	if err:=json.Unmarshal(request.Body,&body);err!=nil || !validCapture(body) { _=s.writeError(connection,request.RequestID,"invalid_request","capture request is invalid"); return true }
	declared,err:=protocol.ReadEvidenceHeader(connection)
	if err != nil || declared != body.PayloadSize || declared > uint64(^uint(0)>>1) { _=s.writeError(connection,request.RequestID,"invalid_request","capture evidence is invalid"); return true }
	s.mu.Lock(); store:=s.store; s.mu.Unlock()
	result,err:=store.AppendEvent(context.Background(),storage.Event{ID:body.EventID,EventType:body.EventType,ProjectID:body.ProjectID,SessionID:body.SessionID,ExchangeID:body.ExchangeID,ProtocolVersion:request.ProtocolVersion,Sequence:body.Sequence,OccurredAt:body.OccurredAt,Encoding:body.Encoding},io.LimitReader(connection,int64(declared)),declared)
	if err != nil { _=s.writeStorageError(connection,request.RequestID,err); return true }
	return s.writeOK(connection,request.RequestID,map[string]any{"actual_size":result.ActualSize,"sha256":result.SHA256,"already_present":result.AlreadyPresent})
}
func validCapture(body captureRequest) bool { return body.EventID!=""&&body.EventType!=""&&body.ProjectID!=""&&body.SessionID!=""&&body.Encoding!=""&&body.Sequence>=0&&!body.OccurredAt.IsZero() }
type readPayloadRequest struct { EventID string `json:"event_id"`; Offset uint64 `json:"offset"`; Limit uint64 `json:"limit"` }
func (s *Server) readPayload(connection net.Conn, request protocol.Request) bool { var body readPayloadRequest; if err:=json.Unmarshal(request.Body,&body);err!=nil||body.EventID=="" { return s.writeError(connection,request.RequestID,"invalid_request","payload request is invalid") }; s.mu.Lock(); store:=s.store; s.mu.Unlock(); metadata,err:=store.EventMetadata(context.Background(),body.EventID);if err!=nil{return s.writeStorageError(connection,request.RequestID,err)}; length:=metadata.TotalSize-body.Offset;if body.Offset>=metadata.TotalSize{length=0};if body.Limit!=0&&body.Limit<length{length=body.Limit};if s.writeOK(connection,request.RequestID,map[string]any{"event_id":body.EventID,"total_size":metadata.TotalSize,"sha256":metadata.SHA256,"offset":body.Offset,"limit":body.Limit}){return true};if err:=protocol.WriteEvidenceHeader(connection,length);err!=nil{return true};_,err=store.ReadEventEvidence(context.Background(),body.EventID,body.Offset,body.Limit,connection);return err!=nil }
type searchRequest struct { Query string `json:"query"`; Limit int `json:"limit"` }
func (s *Server) searchEvents(connection net.Conn, request protocol.Request) bool { var body searchRequest;if err:=json.Unmarshal(request.Body,&body);err!=nil||body.Query==""||len(body.Query)>maxSearchQueryLength||body.Limit<0||body.Limit>maxSearchResults{return s.writeError(connection,request.RequestID,"invalid_request","search request is invalid")};if body.Limit==0{body.Limit=maxSearchResults};s.mu.Lock();store:=s.store;s.mu.Unlock();events,err:=store.SearchEvents(context.Background(),body.Query,body.Limit);if err!=nil{return s.writeStorageError(connection,request.RequestID,err)};return s.writeOK(connection,request.RequestID,map[string]any{"events":events}) }
func (s *Server) writeStorageError(c net.Conn,id string,err error) bool {if errors.Is(err,storage.ErrEventNotFound){return s.writeError(c,id,"not_found","event was not found")};if errors.Is(err,storage.ErrEventConflict){return s.writeError(c,id,"conflict","event conflicts with stored evidence")};return s.writeError(c,id,"internal","internal server error")}
func (s *Server) writeOK(c net.Conn,id string,body any) bool {raw,err:=json.Marshal(body);if err!=nil{return true};return protocol.Encode(c,protocol.Response{ProtocolVersion:protocol.Version,RequestID:id,Status:"ok",Body:raw})!=nil}
func (s *Server) writeError(c net.Conn,id,code,message string) bool{return protocol.Encode(c,protocol.Response{ProtocolVersion:protocol.Version,RequestID:id,Status:"error",Error:&protocol.ResponseError{Code:code,Message:message}})!=nil}

// Close is idempotent. It stops admission, waits for all network goroutines, then releases storage, socket, and lock.
func (s *Server) Close() error {
	var listener *net.UnixListener
	var store *storage.Store
	var lock *InstanceLock
	var identity *socketIdentity
	for {
		s.mu.Lock()
		switch s.state {
		case serverStarting:
			done := s.startDone
			waiting := s.options.closeWaiting
			s.mu.Unlock()
			if waiting != nil { waiting() }
			<-done
			continue
		case serverClosed:
			s.mu.Unlock()
			return nil
		case serverClosing:
			done := s.closeDone
			s.mu.Unlock()
			<-done
			return nil
		default:
			s.state = serverClosing
			s.closeDone = make(chan struct{})
			listener, store, lock, identity = s.listener, s.store, s.lock, s.socket
			s.mu.Unlock()
		}
		break
	}
	defer close(s.closeDone)
	var first error
	if listener!=nil { if err:=listener.Close();err!=nil&&!errors.Is(err,net.ErrClosed){first=err} }
	s.acceptWG.Wait()
	s.mu.Lock(); connections:=make([]net.Conn,0,len(s.connections));for c:=range s.connections{connections=append(connections,c)};s.mu.Unlock();for _,c:=range connections{_=c.Close()};s.connectionWG.Wait()
	if store!=nil {if err:=store.Close();err!=nil&&first==nil{first=err}}
	if identity!=nil {if err:=removeStaleSocket(s.paths,identity);err!=nil&&first==nil{first=err}}
	if lock!=nil {if err:=lock.Release();err!=nil&&first==nil{first=err}}
	s.mu.Lock();s.state=serverClosed;s.mu.Unlock();return first
}
