package server

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/streamers/remote"
)

const (
	maxPendingAcknowledges = 8
)

// LogStore is the abstraction of log storage.
type LogStore interface {
	WriteBatch(ctx context.Context, name string) (BatchWriter, error)
}

// BatchWriter writes logs in batch.
type BatchWriter interface {
	io.Closer
	WriteLogEntry(ctx context.Context, entry *logspb.LogEntry) error
}

// IngressServer implement logz ingress server
type IngressServer struct {
	Store LogStore

	logspb.UnimplementedIngressServiceServer
}

// IngressStream implements IngressService.
func (s *IngressServer) IngressStream(stream logspb.IngressService_IngressStreamServer) error {
	ctx := stream.Context()
	var clientName string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, val := range md.Get(remote.RemoteMetadataKeyClientName) {
			if val != "" {
				clientName = val
				break
			}
		}
	}
	if clientName == "" {
		return status.Error(codes.Unauthenticated, "unauthenticated")
	}

	r := ingressReceiver{
		server:     s,
		stream:     stream,
		clientName: clientName,
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := r.handleMessage(ctx, msg); err != nil {
			return err
		}
	}
}

type ingressReceiver struct {
	server         *IngressServer
	stream         logspb.IngressService_IngressStreamServer
	clientName     string
	receivedNanoTS int64
	ackPending     int
}

func (r *ingressReceiver) handleMessage(ctx context.Context, msg *logspb.IngressBatch) error {
	writer, err := r.server.Store.WriteBatch(ctx, r.clientName)
	if err != nil {
		return err
	}
	defer writer.Close()
	for _, entry := range msg.GetEntries() {
		if err = writer.WriteLogEntry(ctx, entry); err != nil {
			break
		}
		r.receivedNanoTS = entry.GetNanoTs()
		r.ackPending++
	}
	if msg.GetChunkEnd() || r.ackPending > maxPendingAcknowledges || err != nil {
		r.stream.Send(&logspb.IngressEvent{LastNanoTs: r.receivedNanoTS})
		r.ackPending = 0
	}
	return err
}
