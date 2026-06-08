package dnsproxy

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"sock5gw/internal/config"
	"sock5gw/internal/manager"
)

type Server struct {
	cfg  config.DNS
	fake *manager.FakeIPStore
}

func New(cfg config.DNS, fake *manager.FakeIPStore) *Server {
	return &Server{cfg: cfg, fake: fake}
}

func (s *Server) Run(ctx context.Context) error {
	mux := dns.NewServeMux()
	mux.HandleFunc(".", s.handle)

	udp := &dns.Server{Addr: s.cfg.Listen, Net: "udp", Handler: mux}
	tcp := &dns.Server{Addr: s.cfg.Listen, Net: "tcp", Handler: mux}
	errCh := make(chan error, 2)
	go func() {
		slog.Info("dns udp listening", "addr", s.cfg.Listen)
		errCh <- udp.ListenAndServe()
	}()
	go func() {
		slog.Info("dns tcp listening", "addr", s.cfg.Listen)
		errCh <- tcp.ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		_ = udp.Shutdown()
		_ = tcp.Shutdown()
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) handle(w dns.ResponseWriter, r *dns.Msg) {
	resp := new(dns.Msg)
	resp.SetReply(r)
	resp.Authoritative = true
	if len(r.Question) == 0 {
		_ = w.WriteMsg(resp)
		return
	}
	q := r.Question[0]
	if s.blockedType(q.Qtype) {
		resp.Rcode = dns.RcodeSuccess
		_ = w.WriteMsg(resp)
		return
	}
	if q.Qtype == dns.TypeA {
		ip := s.fake.Get(q.Name)
		resp.Answer = append(resp.Answer, &dns.A{
			Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 30},
			A:   ip.To4(),
		})
		_ = w.WriteMsg(resp)
		return
	}
	msg, err := s.exchange(r)
	if err != nil {
		resp.Rcode = dns.RcodeServerFailure
		_ = w.WriteMsg(resp)
		return
	}
	_ = w.WriteMsg(msg)
}

func (s *Server) blockedType(qtype uint16) bool {
	if qtype == dns.TypeAAAA {
		return true
	}
	name := dns.TypeToString[qtype]
	for _, blocked := range s.cfg.BlockedQTyp {
		if strings.EqualFold(blocked, name) {
			return true
		}
	}
	return false
}

func (s *Server) exchange(r *dns.Msg) (*dns.Msg, error) {
	c := &dns.Client{Net: "udp", Timeout: 5 * time.Second}
	upstream := s.cfg.Upstream
	if _, _, err := net.SplitHostPort(upstream); err != nil {
		upstream = net.JoinHostPort(upstream, "53")
	}
	msg, _, err := c.Exchange(r, upstream)
	return msg, err
}
