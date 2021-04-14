#!/bin/bash

TOP_DIR="$(cd ${BASH_SOURCE[0]%/*}/..; pwd)"
[[ -n "$TOP_DIR" ]] || {
    echo "Unable to locate base directory from ${BASH_SOURCE[0]}" >&2
    exit 1
}

mkdir -p "$TOP_DIR/_local/bin"
curl -sSLf -o "$TOP_DIR/_local/protoc.zip" https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/protoc-3.14.0-linux-x86_64.zip
unzip -d "$TOP_DIR/_local" -o "$TOP_DIR/_local/protoc.zip"

mkdir -p "$TOP_DIR/_local/src/protobuf-go"
curl -sSLf https://github.com/golang/protobuf/archive/v1.4.2.tar.gz | tar -C "$TOP_DIR/_local/src/protobuf-go" -xz --strip-components=1
(
    cd "$TOP_DIR/_local/src/protobuf-go"
    CGO_ENABLED=0 go build -o "$TOP_DIR/_local/bin/protoc-gen-go" ./protoc-gen-go/
)

mkdir -p "$TOP_DIR/_local/src/protobuf-go-grpc"
curl -sSLf https://github.com/grpc/grpc-go/archive/v1.31.1.tar.gz | tar -C "$TOP_DIR/_local/src/protobuf-go-grpc" -xz --strip-components=1
(
    cd "$TOP_DIR/_local/src/protobuf-go-grpc/cmd/protoc-gen-go-grpc"
    CGO_ENABLED=0 go build -o "$TOP_DIR/_local/bin/protoc-gen-go_grpc" ./
)
