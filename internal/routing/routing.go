package routing

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strings"

	"sock5gw/internal/config"

	routercommon "github.com/v2fly/v2ray-core/v5/app/router/routercommon"
	"google.golang.org/protobuf/proto"
)

const (
	ActionDirect = "direct"
	ActionProxy  = "proxy"
	ActionBlock  = "block"
)

type Matcher struct {
	enabled bool
	def     string
	rules   []compiledRule
}

type compiledRule struct {
	action string
	match  domainMatch
}

type domainMatch interface {
	Match(domain string) bool
}

type suffixMatch string
type exactMatch string
type keywordMatch string
type regexMatch struct{ re *regexp.Regexp }

func New(cfg config.Routing) (*Matcher, error) {
	m := &Matcher{
		enabled: cfg.Enabled,
		def:     normalizeAction(cfg.DefaultAction),
	}
	if m.def == "" {
		m.def = ActionProxy
	}
	if !cfg.Enabled {
		return m, nil
	}
	add := func(action, typ, value string) error {
		action = normalizeAction(action)
		if action == "" {
			return errors.New("routing action is required")
		}
		rule, err := compileRule(action, typ, value)
		if err != nil {
			return err
		}
		m.rules = append(m.rules, rule)
		return nil
	}
	for _, value := range cfg.DirectDomains {
		if geositeCode(value) != "" {
			continue
		}
		if err := add(ActionDirect, "domain_suffix", value); err != nil {
			return nil, err
		}
	}
	for _, value := range cfg.ProxyDomains {
		if geositeCode(value) != "" {
			continue
		}
		if err := add(ActionProxy, "domain_suffix", value); err != nil {
			return nil, err
		}
	}
	for _, value := range cfg.BlockDomains {
		if geositeCode(value) != "" {
			continue
		}
		if err := add(ActionBlock, "domain_suffix", value); err != nil {
			return nil, err
		}
	}
	for _, rule := range cfg.Rules {
		if isGeositeRule(rule) {
			continue
		}
		if err := add(rule.Action, rule.Type, rule.Value); err != nil {
			return nil, err
		}
	}
	if cfg.GeositePath != "" {
		if err := m.loadGeositeRules(cfg); err != nil {
			return nil, err
		}
	}
	slog.Info("routing rules loaded", "enabled", m.enabled, "rules", len(m.rules), "default_action", m.def)
	return m, nil
}

func (m *Matcher) ActionFor(target string) string {
	if m == nil || !m.enabled {
		return ActionProxy
	}
	domain := targetDomain(target)
	if domain == "" {
		return m.def
	}
	for _, rule := range m.rules {
		if rule.match.Match(domain) {
			return rule.action
		}
	}
	return m.def
}

func targetDomain(target string) string {
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		host = target
	}
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" || net.ParseIP(host) != nil {
		return ""
	}
	return host
}

func compileRule(action, typ, value string) (compiledRule, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, ".")
	if value == "" {
		return compiledRule{}, errors.New("routing rule value is required")
	}
	switch normalizeRuleType(typ) {
	case "domain_suffix":
		return compiledRule{action: action, match: suffixMatch(value)}, nil
	case "domain_exact":
		return compiledRule{action: action, match: exactMatch(value)}, nil
	case "keyword":
		return compiledRule{action: action, match: keywordMatch(value)}, nil
	case "regex":
		re, err := regexp.Compile(value)
		if err != nil {
			return compiledRule{}, err
		}
		return compiledRule{action: action, match: regexMatch{re: re}}, nil
	default:
		return compiledRule{}, fmt.Errorf("unsupported routing rule type %q", typ)
	}
}

func normalizeRuleType(typ string) string {
	typ = strings.ToLower(strings.TrimSpace(typ))
	switch typ {
	case "", "suffix", "domain", "domain_suffix":
		return "domain_suffix"
	case "full", "exact", "domain_exact":
		return "domain_exact"
	default:
		return typ
	}
}

func normalizeAction(action string) string {
	action = strings.ToLower(strings.TrimSpace(action))
	switch action {
	case "", ActionProxy:
		return ActionProxy
	case ActionDirect, ActionBlock:
		return action
	default:
		return action
	}
}

func (m suffixMatch) Match(domain string) bool {
	suffix := string(m)
	return domain == suffix || strings.HasSuffix(domain, "."+suffix)
}

func (m exactMatch) Match(domain string) bool {
	return domain == string(m)
}

func (m keywordMatch) Match(domain string) bool {
	return strings.Contains(domain, string(m))
}

func (m regexMatch) Match(domain string) bool {
	return m.re.MatchString(domain)
}

func (m *Matcher) loadGeositeRules(cfg config.Routing) error {
	data, err := os.ReadFile(cfg.GeositePath)
	if err != nil {
		return err
	}
	var list routercommon.GeoSiteList
	if err := proto.Unmarshal(data, &list); err != nil {
		return err
	}
	actions := map[string]string{}
	collectGeositeActions(actions, cfg.DirectDomains, ActionDirect)
	collectGeositeActions(actions, cfg.ProxyDomains, ActionProxy)
	collectGeositeActions(actions, cfg.BlockDomains, ActionBlock)
	for _, rule := range cfg.Rules {
		if isGeositeRule(rule) {
			code := geositeCode(rule.Value)
			if code == "" {
				code = geositeCode(rule.Type)
			}
			if code != "" {
				actions[code] = normalizeAction(rule.Action)
			}
		}
	}
	if len(actions) == 0 {
		return nil
	}
	for _, site := range list.Entry {
		code := strings.ToLower(site.GetCountryCode())
		if site.GetCode() != "" {
			code = strings.ToLower(site.GetCode())
		}
		action := actions[code]
		if action == "" {
			continue
		}
		for _, d := range site.GetDomain() {
			rule, err := geositeDomainRule(action, d)
			if err != nil {
				continue
			}
			m.rules = append(m.rules, rule)
		}
	}
	return nil
}

func isGeositeRule(rule config.RoutingRule) bool {
	return strings.EqualFold(strings.TrimSpace(rule.Type), "geosite") || strings.HasPrefix(strings.ToLower(strings.TrimSpace(rule.Value)), "geosite:")
}

func collectGeositeActions(dst map[string]string, values []string, action string) {
	for _, value := range values {
		if code := geositeCode(value); code != "" {
			dst[code] = action
		}
	}
}

func geositeCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "geosite:")
	if value == "" || strings.Contains(value, ".") {
		return ""
	}
	return value
}

func geositeDomainRule(action string, d *routercommon.Domain) (compiledRule, error) {
	value := strings.ToLower(strings.TrimSpace(d.GetValue()))
	if value == "" {
		return compiledRule{}, errors.New("empty geosite domain")
	}
	switch d.GetType() {
	case routercommon.Domain_Full:
		return compiledRule{action: action, match: exactMatch(value)}, nil
	case routercommon.Domain_RootDomain:
		return compiledRule{action: action, match: suffixMatch(value)}, nil
	case routercommon.Domain_Plain:
		return compiledRule{action: action, match: keywordMatch(value)}, nil
	case routercommon.Domain_Regex:
		re, err := regexp.Compile(value)
		if err != nil {
			return compiledRule{}, err
		}
		return compiledRule{action: action, match: regexMatch{re: re}}, nil
	default:
		return compiledRule{}, errors.New("unknown geosite domain type")
	}
}
