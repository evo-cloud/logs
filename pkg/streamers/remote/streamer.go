package remote

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/evo-cloud/logs"
	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

const (
	// RemoteMetadataKeyClientName specifies the key in gRPC context for client name.
	RemoteMetadataKeyClientName = "logs-client"
)

// Streamer streams logs to remote server.
type Streamer struct {
	clientName string
	conn       *grpc.ClientConn
}

// NewStreamer creates a Streamer.
func NewStreamer(clientName, serverAddr string) (*Streamer, error) {
	conn, err := grpc.Dial(serverAddr)
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
