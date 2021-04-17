package grpc

import (
	"strings"

	"google.golang.org/grpc/stats"
)

func rpcSpanName(info *stats.RPCTagInfo) string {
	return strings.Replace(strings.TrimPrefix(info.FullMethodName, "/"), "/", ".", -1)
}
