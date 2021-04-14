#!/bin/bash

TOP_DIR="$(cd ${BASH_SOURCE[0]%/*}/..; pwd)"
[[ -n "$TOP_DIR" ]] || {
    echo "Unable to locate base directory from ${BASH_SOURCE[0]}" >&2
    exit 1
}

PROTO_SRC_DIR="$TOP_DIR/proto"
PROTO_GEN_DIR="$TOP_DIR/pkg/gen/proto"

rm -fr "$PROTO_GEN_DIR"
mkdir -p "$PROTO_GEN_DIR"

export PATH="$TOP_DIR/_local/bin:$PATH"
for fn in $(find "$PROTO_SRC_DIR" -name '*.proto'); do
    protoc -I "$PROTO_SRC_DIR" -I "$TOP_DIR/_local/include" "$fn" \
        --go_out=paths=source_relative:"$PROTO_GEN_DIR/" \
        --go_grpc_out=paths=source_relative:"$PROTO_GEN_DIR/"
done
