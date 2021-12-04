package remote

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

const (
	// RemoteMetadataKeyClientName specifies the key in gRPC context for client name.
	RemoteMetadataKeyClientName = "logs-client"
)

// Streamer streams logs to remote server.
type Streamer struct {
	Verbose bool

	clientName string
	conn       *grpc.ClientConn

	streamLock sync.Mutex
	stream     logspb.IngressService_IngressStreamClient
}

// NewStreamer creates a Streamer.
func NewStreamer(clientName, serverAddr string, grpcOpts ...grpc.DialOption) (*Streamer, error) {
	conn, err := grpc.Dial(serverAddr, grpcOpts...)
	if err != nil {
		return nil, err
	}
	return &Streamer{
		clientName: clientName,
		conn:       conn,
	}, nil
}

// Close closes the underlying gRPC connection.
func (s *Streamer) Close() error {
	s.conn.Close()
	return nil
}

// StreamLogEntries implements logs.LogStreamer.
func (s *Streamer) StreamLogEntries(ctx context.Context, entries []*logspb.LogEntry) error {
	stream, err := s.ensureIngressStreamClient(ctx)
	if err != nil {
		if s.Verbose {
			return logs.Emergent().Error(err).PrintErr("IngressStream: ")
		}
		return err
	}
	err = stream.Send(&logspb.IngressBatch{Entries: entries, ChunkEnd: true})
	if err != nil && s.Verbose {
		return logs.Emergent().Error(err).PrintErr("Send: ")
	}
	return err
}

func (s *Streamer) ensureIngressStreamClient(ctx context.Context) (logspb.IngressService_IngressStreamClient, error) {
	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	if s.stream != nil {
		return s.stream, nil
	}
	ctx = metadata.AppendToOutgoingContext(ctx, RemoteMetadataKeyClientName, s.clientName)
	stream, err := logspb.NewIngressServiceClient(s.conn).IngressStream(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
		s.streamLock.Lock()
		defer s.streamLock.Unlock()
		if s.stream == stream {
			s.stream = nil
		}
	}()
	s.stream = stream
	return stream, nil
}

// StartStreamInChunk implements ChunkedStreamer.
func (s *Streamer) StartStreamInChunk(ctx context.Context, info logs.ChunkInfo) (logs.ChunkedLogStreamer, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, RemoteMetadataKeyClientName, s.clientName)
	stream, err := logspb.NewIngressServiceClient(s.conn).IngressStream(ctx)
	if err != nil {
		return nil, err
	}
	streamer := &streamer{
		info:   info,
		stream: stream,
		errCh:  make(chan error, 1),
	}
	go streamer.run()
	return streamer, nil
}

type streamer struct {
	info       logs.ChunkInfo
	stream     logspb.IngressService_IngressStreamClient
	entryCount int
	lastNanoTS int64
	errCh      chan error
}

func (s *streamer) StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	if err := s.stream.Send(&logspb.IngressBatch{Entries: []*logspb.LogEntry{entry}, ChunkEnd: s.entryCount+1 == s.info.NumEntries}); err != nil {
		return err
	}
	s.entryCount++
	return nil
}

func (s *streamer) StreamEnd(ctx context.Context) (int64, error) {
	s.stream.CloseSend()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-s.errCh:
	}
	return atomic.LoadInt64(&s.lastNanoTS), err
}

func (s *streamer) run() {
	for {
		msg, err := s.stream.Recv()
		if err != nil {
			s.errCh <- err
			return
		}
		atomic.StoreInt64(&s.lastNanoTS, msg.GetLastNanoTs())
	}
}
