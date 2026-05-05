package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

const (
	maxMessageSize = 1 << 30 // 1G
)

func StreamLogsServe(ln net.Listener, store LogStore) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go StreamLogs(context.Background(), conn, store)
	}
}

func StreamLogs(ctx context.Context, in io.Reader, store LogStore) error {
	if closer, ok := in.(io.Closer); ok {
		defer closer.Close()
	}

	var (
		buf     []byte
		emitter logs.LogEmitter
	)
	defer func() {
		if closer, ok := emitter.(io.Closer); ok {
			closer.Close()
		}
	}()
	for {
		var sz uint32
		if err := binary.Read(in, binary.BigEndian, &sz); err != nil {
			return err
		}
		if sz == 0 {
			continue
		}
		if sz > maxMessageSize {
			return fmt.Errorf("invalid message header")
		}
		if size := int(sz); size > len(buf) || size < len(buf)/2 {
			buf = make([]byte, sz)
		}
		if n, err := io.ReadFull(in, buf[:sz]); err != nil {
			if n == 0 && errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if emitter == nil {
			var err error
			if emitter, err = store.WriteStream(ctx, string(buf[:sz])); err != nil {
				return err
			}
			continue
		}

		entry := &logspb.LogEntry{}
		if err := proto.Unmarshal(buf[:sz], entry); err != nil {
			logs.Use(ctx).Error(err).PrintErr("Unmarhsal: ")
			continue
		}

		emitter.EmitLogEntry(entry)
	}
}
