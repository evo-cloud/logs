package hub

import (
	"context"
	"encoding/binary"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
	"github.com/evo-cloud/logs/go/server"
)

// Dispatcher dispatches logs to connected clients.
type Dispatcher struct {
	Emitter logs.LogEmitter

	connsLock sync.RWMutex
	conns     map[*connWriter]struct{}
}

type batchWriter struct {
	*Dispatcher
	conns []*connWriter
}

type connWriter struct {
	lock sync.Mutex
	conn net.Conn
}

func (w *connWriter) write(bufs ...[]byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, buf := range bufs {
		if _, err := w.conn.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) Serve(ln net.Listener) error {
	defer func() {
		d.connsLock.Lock()
		conns := d.conns
		d.conns = nil
		d.connsLock.Unlock()
		for conn := range conns {
			conn.conn.Close()
		}
		ln.Close()
	}()
	ctx := context.Background()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		w := &connWriter{conn: conn}
		d.connsLock.Lock()
		if d.conns == nil {
			d.conns = make(map[*connWriter]struct{})
		}
		d.conns[w] = struct{}{}
		d.connsLock.Unlock()
		go func(conn net.Conn) {
			_, log := logs.StartSpan(ctx, "Serve", logs.Str("remote-addr", conn.RemoteAddr().String()))
			defer log.EndSpan()
			defer func() {
				d.connsLock.Lock()
				delete(d.conns, w)
				d.connsLock.Unlock()
			}()
			var buf [1]byte
			for {
				_, err := conn.Read(buf[:])
				if err != nil {
					return
				}
			}
		}(conn)
	}
}

func (d *Dispatcher) WriteBatch(ctx context.Context, name string) (server.BatchWriter, error) {
	w := &batchWriter{Dispatcher: d}
	d.connsLock.RLock()
	w.conns = make([]*connWriter, 0, len(d.conns))
	for conn := range d.conns {
		w.conns = append(w.conns, conn)
	}
	d.connsLock.RUnlock()
	return w, nil
}

func (d *Dispatcher) WriteStream(ctx context.Context, name string) (logs.LogEmitter, error) {
	return d, nil
}

func (d *Dispatcher) EmitLogEntry(entry *logspb.LogEntry) {
	if emitter := d.Emitter; emitter != nil {
		emitter.EmitLogEntry(entry)
	}
	d.connsLock.RLock()
	n := len(d.conns)
	d.connsLock.RUnlock()
	if n == 0 {
		return
	}

	lenBuf, data, err := encodeLogEntry(entry)
	if err != nil {
		return
	}
	d.connsLock.RLock()
	for conn := range d.conns {
		conn.write(lenBuf, data)
	}
	d.connsLock.RUnlock()
}

func (w *batchWriter) WriteLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	if emitter := w.Emitter; emitter != nil {
		emitter.EmitLogEntry(entry)
	}
	if len(w.conns) == 0 {
		return nil
	}

	lenBuf, data, err := encodeLogEntry(entry)
	if err != nil {
		return err
	}
	for _, conn := range w.conns {
		conn.write(lenBuf, data)
	}
	return nil
}

func (w *batchWriter) Close() error {
	return nil
}

func encodeLogEntry(entry *logspb.LogEntry) (lenBuf, data []byte, err error) {
	if data, err = proto.Marshal(entry); err != nil {
		return
	}
	lenBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	return
}
