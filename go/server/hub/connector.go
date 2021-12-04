package hub

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// Connector connects the hub and streams logs to the emitter.
type Connector struct {
	Emitter logs.LogEmitter
}

func (c *Connector) DialAndStream(network, addr string) error {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	return c.Stream(conn)
}

func (c *Connector) Stream(r io.Reader) error {
	defer func() {
		if closer, ok := r.(io.Closer); ok {
			closer.Close()
		}
	}()
	var buf bytes.Buffer
	for {
		var size uint32
		if err := binary.Read(r, binary.BigEndian, &size); err != nil {
			return err
		}
		buf.Reset()
		if _, err := io.CopyN(&buf, r, int64(size)); err != nil {
			return err
		}
		entry := &logspb.LogEntry{}
		if err := proto.Unmarshal(buf.Bytes(), entry); err != nil {
			continue
		}
		c.Emitter.EmitLogEntry(entry)
	}
}
