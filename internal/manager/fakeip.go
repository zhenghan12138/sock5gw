package manager

import (
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
)

type FakeIPStore struct {
	mu       sync.Mutex
	network  *net.IPNet
	base     uint32
	size     uint32
	next     uint32
	nameToIP map[string]net.IP
	ipToName map[string]string
}

func NewFakeIPStore(cidr string) (*FakeIPStore, error) {
	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil, errors.New("fake ip cidr must be IPv4")
	}
	ones, bits := network.Mask.Size()
	if bits != 32 || ones > 30 {
		return nil, errors.New("fake ip cidr is too small")
	}
	size := uint32(1) << uint32(32-ones)
	return &FakeIPStore{
		network:  network,
		base:     binary.BigEndian.Uint32(ip4),
		size:     size,
		next:     1,
		nameToIP: map[string]net.IP{},
		ipToName: map[string]string{},
	}, nil
}

func (s *FakeIPStore) Get(domain string) net.IP {
	s.mu.Lock()
	defer s.mu.Unlock()
	domain = strings.TrimSuffix(strings.ToLower(domain), ".")
	if ip := s.nameToIP[domain]; ip != nil {
		return append(net.IP(nil), ip...)
	}
	offset := s.next
	s.next++
	if s.next >= s.size-1 {
		s.next = 1
	}
	raw := s.base + offset
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, raw)
	s.nameToIP[domain] = ip
	s.ipToName[ip.String()] = domain
	return append(net.IP(nil), ip...)
}

func (s *FakeIPStore) Lookup(ip net.IP) (string, bool) {
	ip4 := ip.To4()
	if ip4 == nil || !s.network.Contains(ip4) {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.ipToName[ip4.String()]
	return name, ok
}
