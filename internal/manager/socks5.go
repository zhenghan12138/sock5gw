package manager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

func ConnectSOCKS5(conn net.Conn, target, username, password string) error {
	methods := []byte{0x00}
	if username != "" || password != "" {
		methods = []byte{0x00, 0x02}
	}
	if _, err := conn.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x05 {
		return errors.New("invalid socks version")
	}
	if resp[1] == 0xff {
		return errors.New("socks auth method rejected")
	}
	if resp[1] == 0x02 {
		if len(username) > 255 || len(password) > 255 {
			return errors.New("socks credentials too long")
		}
		req := []byte{0x01, byte(len(username))}
		req = append(req, []byte(username)...)
		req = append(req, byte(len(password)))
		req = append(req, []byte(password)...)
		if _, err := conn.Write(req); err != nil {
			return err
		}
		if _, err := io.ReadFull(conn, resp); err != nil {
			return err
		}
		if resp[1] != 0x00 {
			return errors.New("socks username/password auth failed")
		}
	}
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = append(req, 0x01)
			req = append(req, ip4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return errors.New("target hostname too long")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	var portBuf [2]byte
	binary.BigEndian.PutUint16(portBuf[:], uint16(port))
	req = append(req, portBuf[:]...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[1] != 0x00 {
		return fmt.Errorf("socks connect failed code=%d", head[1])
	}
	var skip int
	switch head[3] {
	case 0x01:
		skip = 4
	case 0x03:
		lenBuf := []byte{0}
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return err
		}
		skip = int(lenBuf[0])
	case 0x04:
		skip = 16
	default:
		return errors.New("invalid socks address type")
	}
	if _, err := io.CopyN(io.Discard, conn, int64(skip+2)); err != nil {
		return err
	}
	return nil
}
