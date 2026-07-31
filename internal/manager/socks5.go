package manager

import (
	"context"
	"net"

	"sock5gw/internal/outbound"
)

func ConnectSOCKS5(conn net.Conn, target, username, password string) error {
	return outbound.ConnectSOCKS5(context.Background(), conn, target, username, password)
}
