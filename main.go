package main

import (
	"net"
	"net/rpc"
	"strings"

	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"
)

func main() {
	hardlinks := pflag.Bool("hardlinks", true, "Preserve hardlinks")
	folder := pflag.String("folder", ".", "Folder to use as source or target")
	checksum := pflag.Bool("checksum", false, "Checksum files")
	bind := pflag.String("bind", "0.0.0.0:7331", "Address to bind/connect to")

	pflag.Parse()

	var h codec.MsgpackHandle

	if len(pflag.Args()) == 0 {
		panic("Need command argument")
	}

	switch strings.ToLower(pflag.Arg(0)) {
	case "server":
		conn, err := net.Listen("tcp", *bind)
	case "client":
		//RPC Communication (client side)
		conn, err := net.Dial("tcp", *bind)
		rpcCodec := codec.GoRpc.ClientCodec(conn, h) // OR codec.MsgpackSpecRpc...
		client := rpc.NewClientWithCodec(rpcCodec)
	}

	enc := codec.NewEncoder(conn, h)
	dec := codec.NewDecoder(conn, h)

}
