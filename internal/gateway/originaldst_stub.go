//go:build !linux

package gateway

import (
	"errors"
	"net"
)

func originalDst(conn net.Conn) (string, error) {
	return "", errors.New("transparent proxy original destination lookup is only implemented on linux")
}
