//go:build !linux

package nwws

import (
	"net"
	"testing"
)

// smallSegmentPair is tcpPair; see the Linux version.
func smallSegmentPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	return tcpPair(t)
}
