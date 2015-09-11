package test_helpers

import "net"

type Conn struct {
	net.Conn
	localAddr   net.Addr
}

func (f *Conn) LocalAddr() net.Addr {
	return &net.IPAddr{}
}

func (c *Conn) Close() error {
	return nil
}
