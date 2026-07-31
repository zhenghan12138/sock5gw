package outbound

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type socksStage uint8

const (
	socksStageHandshake socksStage = iota
	socksStageAuth
	socksStageConnect
)

type socksError struct {
	stage socksStage
	msg   string
	err   error
}

func (e *socksError) Error() string {
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

func (e *socksError) Unwrap() error { return e.err }

func isSOCKSAuthError(err error) bool {
	var socksErr *socksError
	return errors.As(err, &socksErr) && socksErr.stage == socksStageAuth
}

type socks5Client struct {
	conn net.Conn
}

func (c socks5Client) handshake(username, password string) error {
	return c.handshakeWithMethodSelection(username, password, nil)
}

func (c socks5Client) handshakeWithMethodSelection(username, password string, onMethodSelection func()) error {
	if err := validateCredentials(username, password); err != nil {
		return &socksError{stage: socksStageAuth, msg: err.Error()}
	}
	methods := []byte{0x00}
	if username != "" || password != "" {
		methods = append(methods, 0x02)
	}
	request := append([]byte{0x05, byte(len(methods))}, methods...)
	if err := writeFull(c.conn, request); err != nil {
		return &socksError{stage: socksStageHandshake, msg: "write SOCKS5 greeting", err: err}
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(c.conn, response); err != nil {
		return &socksError{stage: socksStageHandshake, msg: "read SOCKS5 greeting", err: err}
	}
	if response[0] != 0x05 {
		return &socksError{stage: socksStageHandshake, msg: fmt.Sprintf("invalid SOCKS5 greeting version %d", response[0])}
	}
	if onMethodSelection != nil {
		onMethodSelection()
	}
	switch response[1] {
	case 0x00:
		return nil
	case 0x02:
		if username == "" && password == "" {
			return &socksError{stage: socksStageAuth, msg: "SOCKS5 server selected an authentication method that was not offered"}
		}
		return c.authenticate(username, password)
	case 0xff:
		return &socksError{stage: socksStageAuth, msg: "SOCKS5 authentication methods rejected"}
	default:
		return &socksError{stage: socksStageAuth, msg: fmt.Sprintf("SOCKS5 server selected unsupported authentication method %d", response[1])}
	}
}

func (c socks5Client) authenticate(username, password string) error {
	request := make([]byte, 0, 3+len(username)+len(password))
	request = append(request, 0x01, byte(len(username)))
	request = append(request, username...)
	request = append(request, byte(len(password)))
	request = append(request, password...)
	if err := writeFull(c.conn, request); err != nil {
		return &socksError{stage: socksStageAuth, msg: "write SOCKS5 authentication", err: err}
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(c.conn, response); err != nil {
		return &socksError{stage: socksStageAuth, msg: "read SOCKS5 authentication", err: err}
	}
	if response[0] != 0x01 {
		return &socksError{stage: socksStageAuth, msg: fmt.Sprintf("invalid SOCKS5 authentication version %d", response[0])}
	}
	if response[1] != 0x00 {
		return &socksError{stage: socksStageAuth, msg: "SOCKS5 username/password authentication failed"}
	}
	return nil
}

func (c socks5Client) connect(target string) error {
	host, portText, err := splitAddress(target)
	if err != nil {
		return &socksError{stage: socksStageConnect, msg: "invalid SOCKS5 target", err: err}
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return &socksError{stage: socksStageConnect, msg: "invalid SOCKS5 target port", err: err}
	}
	request := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			request = append(request, 0x01)
			request = append(request, ip4...)
		} else {
			request = append(request, 0x04)
			request = append(request, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return &socksError{stage: socksStageConnect, msg: "SOCKS5 target hostname exceeds 255 bytes"}
		}
		request = append(request, 0x03, byte(len(host)))
		request = append(request, host...)
	}
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(port))
	request = append(request, portBytes[:]...)
	if err := writeFull(c.conn, request); err != nil {
		return &socksError{stage: socksStageConnect, msg: "write SOCKS5 CONNECT", err: err}
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return &socksError{stage: socksStageConnect, msg: "read SOCKS5 CONNECT response", err: err}
	}
	if header[0] != 0x05 {
		return &socksError{stage: socksStageConnect, msg: fmt.Sprintf("invalid SOCKS5 response version %d", header[0])}
	}
	if header[2] != 0x00 {
		return &socksError{stage: socksStageConnect, msg: fmt.Sprintf("invalid SOCKS5 reserved byte %d", header[2])}
	}
	if header[1] != 0x00 {
		return &socksError{stage: socksStageConnect, msg: fmt.Sprintf("SOCKS5 CONNECT failed with code %d", header[1])}
	}

	addressLength := 0
	switch header[3] {
	case 0x01:
		addressLength = net.IPv4len
	case 0x03:
		length := []byte{0}
		if _, err := io.ReadFull(c.conn, length); err != nil {
			return &socksError{stage: socksStageConnect, msg: "read SOCKS5 bound hostname length", err: err}
		}
		if length[0] == 0 {
			return &socksError{stage: socksStageConnect, msg: "invalid empty SOCKS5 bound hostname"}
		}
		addressLength = int(length[0])
	case 0x04:
		addressLength = net.IPv6len
	default:
		return &socksError{stage: socksStageConnect, msg: fmt.Sprintf("invalid SOCKS5 response address type %d", header[3])}
	}
	if _, err := io.CopyN(io.Discard, c.conn, int64(addressLength+2)); err != nil {
		return &socksError{stage: socksStageConnect, msg: "read SOCKS5 bound address", err: err}
	}
	return nil
}

// ConnectSOCKS5 performs a complete greeting, optional username/password
// authentication, and CONNECT exchange on an existing connection.
func ConnectSOCKS5(ctx context.Context, conn net.Conn, target, username, password string) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if conn == nil {
		return errors.New("nil connection")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cleanup, err := bindConnContext(ctx, conn)
	if err != nil {
		return err
	}
	defer cleanup()
	client := socks5Client{conn: conn}
	if err := client.handshake(username, password); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	if err := client.connect(target); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	return nil
}

func bindConnContext(ctx context.Context, conn net.Conn) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	if ctx.Done() == nil {
		return func() {}, nil
	}

	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-stopped
		_ = conn.SetDeadline(time.Time{})
	}, nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		if written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
