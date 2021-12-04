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
	conns     map[net.Conn]struct{}
}

type batchWriter struct {
	*Dispatcher
	conns  []net.Conn
	lenBuf [4]byte
}

func (d *Dispatcher) Serve(ln net.Listener) error {
	defer func() {
		d.connsLock.Lock()
		conns := d.conns
		d.conns = nil
		d.connsLock.Unlock()
		for conn := range conns {
			conn.Close()
		}
		ln.Close()
	}()
	ctx := context.Background()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		d.connsLock.Lock()
		if d.conns == nil {
			d.conns = make(map[net.Conn]struct{})
		}
		d.conns[conn] = struct{}{}
		d.connsLock.Unlock()
		go func(conn net.Conn) {
			_, log := logs.StartSpan(ctx, "Serve", logs.Str("remote-addr", conn.RemoteAddr().String()))
			defer log.EndSpan()
			defer func() {
				d.connsLock.Lock()
				delete(d.conns, conn)
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
	w.conns = make([]net.Conn, 0, len(d.conns))
	for conn := range d.conns {
		w.conns = append(w.conns, conn)
	}
	d.connsLock.RUnlock()
	return w, nil
}

func (w *batchWriter) WriteLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	if emitter := w.Emitter; emitter != nil {
		emitter.EmitLogEntry(entry)
	}
	if len(w.conns) == 0 {
		return nil
	}
	entryPb, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(w.lenBuf[:], uint32(len(entryPb)))
	for _, conn := range w.conns {
		conn.Write(w.lenBuf[:])
		conn.Write(entryPb)
	}
	return nil
}

func (w *batchWriter) Close() error {
	return nil
}
