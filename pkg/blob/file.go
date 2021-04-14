package blob

import (
	"encoding/binary"
	"errors"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

var (
	// ErrBadRecord indicates a record contains invalid or inconsistent data.
	ErrBadRecord = errors.New("bad record")
)

// RawRecord is a single record in the file.
type RawRecord struct {
	Head []byte
	Body []byte
	Tail []byte
}

// RawRecordSize estimate RawRecord size after entry is encoded.
func RawRecordSize(entry *logspb.LogEntry) int {
	bodySize := proto.Size(entry)
	if rest := bodySize & 3; rest != 0 {
		bodySize += 4 - rest
	}
	return bodySize + 8
}

// EncodeToRawRecord encodes an entry to a RawRecord.
func EncodeToRawRecord(entry *logspb.LogEntry) (*RawRecord, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	bodySize := len(data)
	rec := &RawRecord{Head: make([]byte, 4), Body: data}
	binary.LittleEndian.PutUint32(rec.Head, uint32(bodySize))
	if rest := bodySize & 3; rest != 0 {
		paddings := 4 - rest
		rec.Tail = make([]byte, paddings+4)
		copy(rec.Tail[paddings:], rec.Head)
	} else {
		rec.Tail = rec.Head
	}
	return rec, nil
}
