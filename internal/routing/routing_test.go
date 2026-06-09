package routing

import (
	"os"
	"path/filepath"
	"testing"

	"sock5gw/internal/config"

	routercommon "github.com/v2fly/v2ray-core/v5/app/router/routercommon"
	"google.golang.org/protobuf/proto"
)

func TestMatcherActions(t *testing.T) {
	m, err := New(config.Routing{
		Enabled:       true,
		DefaultAction: ActionProxy,
		Rules: []config.RoutingRule{
			{Type: "domain_suffix", Value: "lan.example", Action: ActionDirect},
			{Type: "domain_exact", Value: "ads.example.com", Action: ActionBlock},
			{Type: "keyword", Value: "force-proxy", Action: ActionProxy},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"www.lan.example:443":   ActionDirect,
		"ads.example.com:443":   ActionBlock,
		"x.force-proxy.test:80": ActionProxy,
		"other.example:443":     ActionProxy,
		"8.8.8.8:443":           ActionProxy,
	}
	for target, want := range cases {
		if got := m.ActionFor(target); got != want {
			t.Fatalf("ActionFor(%q) = %q, want %q", target, got, want)
		}
	}
}

func TestMatcherLoadsGeosite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "geosite.dat")
	list := &routercommon.GeoSiteList{Entry: []*routercommon.GeoSite{
		{
			CountryCode: "cn",
			Domain: []*routercommon.Domain{
				{Type: routercommon.Domain_RootDomain, Value: "example.cn"},
			},
		},
		{
			Code: "category-ads-all",
			Domain: []*routercommon.Domain{
				{Type: routercommon.Domain_Full, Value: "ads.example.com"},
			},
		},
	}}
	data, err := proto.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	m, err := New(config.Routing{
		Enabled:       true,
		GeositePath:   path,
		DefaultAction: ActionProxy,
		Rules: []config.RoutingRule{
			{Type: "geosite", Value: "geosite:cn", Action: ActionDirect},
			{Type: "geosite", Value: "geosite:category-ads-all", Action: ActionBlock},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.ActionFor("www.example.cn:443"); got != ActionDirect {
		t.Fatalf("cn action = %q", got)
	}
	if got := m.ActionFor("ads.example.com:443"); got != ActionBlock {
		t.Fatalf("ads action = %q", got)
	}
}
