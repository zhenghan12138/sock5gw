package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/manager"
)

type Manager interface {
	ResolveProxy(clientIP string) (*manager.Proxy, error)
	RegisterConn(clientIP, proxyID string, conn net.Conn)
	UnregisterConn(clientIP, proxyID string, conn net.Conn)
	FakeIPStore() *manager.FakeIPStore
}

type Gateway struct {
	cfg config.Gateway
	mgr Manager
}

func New(cfg config.Gateway, mgr Manager) *Gateway {
	return &Gateway{cfg: cfg, mgr: mgr}
}

func (g *Gateway) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", g.cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	slog.Info("gateway listening", "addr", g.cfg.Listen)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go g.handle(conn)
	}
}

func (g *Gateway) handle(client net.Conn) {
	defer client.Close()
	clientIP := remoteIP(client.RemoteAddr())
	if clientIP == "" {
		return
	}
	target, err := g.targetAddress(client)
	if err != nil {
		slog.Debug("target lookup failed", "client_ip", clientIP, "err", err)
		return
	}
	target = g.rewriteFakeIPTarget(target)
	if g.isBlockedTarget(target) {
		slog.Debug("blocked target", "client_ip", clientIP, "target", target)
		return
	}
	proxy, err := g.mgr.ResolveProxy(clientIP)
	if err != nil {
		slog.Debug("client has no usable lease", "client_ip", clientIP, "err", err)
		return
	}
	upstream, err := manager.DialProxy(context.Background(), *proxy, g.cfg.DialTimeout.Duration)
	if err != nil {
		slog.Warn("proxy dial failed", "client_ip", clientIP, "proxy_id", proxy.ID, "err", err)
		return
	}
	defer upstream.Close()
	_ = upstream.SetDeadline(time.Now().Add(g.cfg.DialTimeout.Duration))
	if err := manager.ConnectSOCKS5(upstream, target, proxy.Username, proxy.Password); err != nil {
		slog.Warn("socks connect failed", "client_ip", clientIP, "proxy_id", proxy.ID, "target", target, "err", err)
		return
	}
	_ = upstream.SetDeadline(time.Time{})

	g.mgr.RegisterConn(clientIP, proxy.ID, client)
	defer g.mgr.UnregisterConn(clientIP, proxy.ID, client)
	relay(client, upstream, g.cfg.IdleTimeout.Duration)
}

func (g *Gateway) targetAddress(conn net.Conn) (string, error) {
	if g.cfg.TransparentProxy {
		return originalDst(conn)
	}
	host, port, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return "", err
	}
	if host == "" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}

func (g *Gateway) rewriteFakeIPTarget(target string) string {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return target
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return target
	}
	name, ok := g.mgr.FakeIPStore().Lookup(ip)
	if !ok {
		return target
	}
	return net.JoinHostPort(name, port)
}

func (g *Gateway) isBlockedTarget(target string) bool {
	_, port, err := net.SplitHostPort(target)
	if err != nil {
		return true
	}
	for _, blocked := range g.cfg.BlockedPorts {
		if blocked == port {
			return true
		}
	}
	return false
}

func remoteIP(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return ""
	}
	return host
}

func relay(a, b net.Conn, idle time.Duration) {
	var once sync.Once
	closeBoth := func() {
		_ = a.Close()
		_ = b.Close()
	}
	if idle > 0 {
		_ = a.SetDeadline(time.Now().Add(idle))
		_ = b.SetDeadline(time.Now().Add(idle))
	}
	copySide := func(dst, src net.Conn) {
		if idle > 0 {
			buf := make([]byte, 32*1024)
			for {
				n, rerr := src.Read(buf)
				if n > 0 {
					_ = src.SetDeadline(time.Now().Add(idle))
					_ = dst.SetDeadline(time.Now().Add(idle))
					if _, werr := dst.Write(buf[:n]); werr != nil {
						break
					}
				}
				if rerr != nil {
					break
				}
			}
		} else {
			_, _ = io.Copy(dst, src)
		}
		once.Do(closeBoth)
	}
	go copySide(a, b)
	copySide(b, a)
}

func parseHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	if strings.TrimSpace(host) == "" {
		return "", 0, errors.New("empty host")
	}
	return host, port, nil
}

func makeAddress(host string, port int) string {
	return net.JoinHostPort(host, fmt.Sprint(port))
}
