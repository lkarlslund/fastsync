package main

import "net/rpc"

// Wait for the shutdown response to leave the codec before exiting the process.
// Signaling inside the RPC method alone races response serialization.
type shutdownReplyCodec struct {
	rpc.ServerCodec
	replied chan<- struct{}
}

func (c *shutdownReplyCodec) WriteResponse(response *rpc.Response, body any) error {
	err := c.ServerCodec.WriteResponse(response, body)
	if response.ServiceMethod == "Server.Shutdown" && response.Error == "" {
		select {
		case c.replied <- struct{}{}:
		default:
		}
	}
	return err
}
