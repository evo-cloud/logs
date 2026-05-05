package logs

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"google.golang.org/protobuf/proto"
)

// Emit batch of log entries to remote stream server.
type StreamBatchEmitter struct {
	Verbose bool
	Dialer  StreamDialer

	clientName string
	network    string
	address    string

	connLock sync.Mutex
	conn     net.Conn
}

type StreamDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// NewStreamBatchEmitter creates a StreamBatchEmitter.
func NewStreamBatchEmitter(clientName, network, address string) (*StreamBatchEmitter, error) {
	if network == "" {
		if strings.HasPrefix(address, "/") || strings.HasPrefix(address, "@") {
			network = "unix"
		} else {
			network = "tcp"
		}
	}
	return &StreamBatchEmitter{
		Dialer:     &net.Dialer{},
		clientName: clientName,
		network:    network,
		address:    address,
	}, nil
}

// Close closes the underlying gRPC connection.
func (s *StreamBatchEmitter) Close() error {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	s.close()
	return nil
}

func (s *StreamBatchEmitter) EmitLogEntries(ctx context.Context, entries []*logspb.LogEntry) error {
	s.connLock.Lock()
	defer s.connLock.Unlock()

	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			continue
		}
		if err := s.tryConnect(ctx); err != nil {
			return err
		}
		if err := s.write(data); err == nil {
			continue
		}
		if err := s.tryConnect(ctx); err != nil {
			return err
		}
		if err := s.write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *StreamBatchEmitter) tryConnect(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	conn, err := s.Dialer.DialContext(ctx, s.network, s.address)
	if err != nil {
		if s.Verbose {
			return Emergent().Error(err).PrintErrf("Stream tryConnect %s: ", s.address)
		}
		return err
	}
	s.conn = conn
	return s.write([]byte(s.clientName))
}

func (s *StreamBatchEmitter) close() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (s *StreamBatchEmitter) write(data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := s.conn.Write(lenBuf); err != nil {
		s.close()
		if s.Verbose {
			return Emergent().Error(err).PrintErrf("Stream write %s: ", s.address)
		}
		return err
	}
	if _, err := s.conn.Write(data); err != nil {
		s.close()
		if s.Verbose {
			return Emergent().Error(err).PrintErrf("Stream write %s: ", s.address)
		}
		return err
	}
	return nil
}
