//go:build linux

package gateway

import (
	"encoding/binary"
	"errors"
	"net"
	"syscall"
	"unsafe"
)

const soOriginalDst = 80

func originalDst(conn net.Conn) (string, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return "", errors.New("not a tcp connection")
	}
	raw, err := tcpConn.SyscallConn()
	if err != nil {
		return "", err
	}
	var addr string
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		var sa syscall.RawSockaddrInet4
		size := uint32(syscall.SizeofSockaddrInet4)
		_, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.SOL_IP, soOriginalDst, uintptr(unsafe.Pointer(&sa)), uintptr(unsafe.Pointer(&size)), 0)
		if errno != 0 {
			sockErr = errno
			return
		}
		ip := net.IPv4(sa.Addr[0], sa.Addr[1], sa.Addr[2], sa.Addr[3])
		port := binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:])
		addr = makeAddress(ip.String(), int(port))
	}); err != nil {
		return "", err
	}
	if sockErr != nil {
		return "", sockErr
	}
	return addr, nil
}
