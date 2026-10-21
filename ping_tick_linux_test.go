package nwws

import (
	"net"
	"syscall"
	"testing"
)

// smallSegmentPair is tcpPair with TCP segments of at most 1460 bytes, as
// on Ethernet, instead of loopback's 32–64 KiB.
//
// With loopback's segment size, a sender of small frames to a peer that
// falls behind stalls for 200 ms or more at times: the peer has read
// everything, but the receive window it advertises stays below one
// segment, so the sender keeps its queue unsent, with nothing in flight
// and no retransmission, until its probe timer fires. Observed during
// such stalls in TestPingsDuringDirectWrites: 128 KiB unsent, a send
// window of 29–31 KB against a 32 KiB segment size, and the time limited
// by the receive window growing. Nothing the connection does can get a
// frame through then.
func smallSegmentPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	mss := func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 1460)
		}); err != nil {
			return err
		}
		return serr
	}
	lc := net.ListenConfig{Control: mss}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	d := net.Dialer{Control: mss}
	client, err = d.DialContext(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	closeQuietly(t, client)
	closeQuietly(t, server)
	return server, client
}
