package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"sock5gw/internal/config"
	"sock5gw/internal/routing"
)

type applyFrontProxyFunc func(config.FrontProxy, func() error) error
type applyProxyAPIFunc func(config.ProxyAPI, func() error) error

type RuntimeConfig struct {
	mu              sync.RWMutex
	path            string
	routing         config.Routing
	frontProxy      config.FrontProxy
	proxyAPI        config.ProxyAPI
	applyRouting    func(*routing.Matcher)
	applyFrontProxy applyFrontProxyFunc
	applyProxyAPI   applyProxyAPIFunc
}

func NewRuntimeConfig(
	path string,
	cfg *config.Config,
	applyRouting func(*routing.Matcher),
	applyFrontProxy applyFrontProxyFunc,
	proxyAPIFunc ...applyProxyAPIFunc,
) *RuntimeConfig {
	var applyProxyAPI applyProxyAPIFunc
	if len(proxyAPIFunc) > 0 {
		applyProxyAPI = proxyAPIFunc[0]
	}
	return &RuntimeConfig{
		path:            path,
		routing:         cfg.Routing,
		frontProxy:      cfg.FrontProxy,
		proxyAPI:        cfg.ProxyAPI,
		applyRouting:    applyRouting,
		applyFrontProxy: applyFrontProxy,
		applyProxyAPI:   applyProxyAPI,
	}
}

func (c *RuntimeConfig) ProxyAPI() config.ProxyAPI {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.proxyAPI
}

func (c *RuntimeConfig) UpdateProxyAPI(next config.ProxyAPI) error {
	if c == nil {
		return errors.New("runtime config is unavailable")
	}
	if c.applyProxyAPI == nil {
		return errors.New("proxy API runtime update is unavailable")
	}
	next.URL = strings.TrimSpace(next.URL)
	next.CountryParam = strings.TrimSpace(next.CountryParam)
	next.DurationParam = strings.TrimSpace(next.DurationParam)
	if next.CountryParam == "" {
		next.CountryParam = "region"
	}
	if next.DurationParam == "" {
		next.DurationParam = "time"
	}
	if err := config.ValidateProxyAPI(next); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.applyProxyAPI(next, func() error {
		return c.writeSection("proxy_api", next)
	})
	if err != nil {
		return err
	}
	c.proxyAPI = next
	return nil
}

func (c *RuntimeConfig) Routing() config.Routing {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routing
}

func (c *RuntimeConfig) UpdateRouting(next config.Routing) error {
	if c == nil {
		return errors.New("runtime config is unavailable")
	}
	matcher, err := routing.New(next)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.writeSection("routing", next); err != nil {
		return err
	}
	c.routing = next
	if c.applyRouting != nil {
		c.applyRouting(matcher)
	}
	return nil
}

func (c *RuntimeConfig) FrontProxy() config.FrontProxy {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.frontProxy
}

func (c *RuntimeConfig) UpdateFrontProxy(next config.FrontProxy) error {
	if c == nil {
		return errors.New("runtime config is unavailable")
	}
	if c.applyFrontProxy == nil {
		return errors.New("front proxy runtime update is unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.applyFrontProxy(next, func() error {
		return c.writeSection("front_proxy", next)
	})
	if err != nil {
		return err
	}
	c.frontProxy = next
	return nil
}

func (c *RuntimeConfig) writeSection(name string, value any) error {
	if c.path == "" {
		return errors.New("config path is empty")
	}
	data, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var sectionRaw any
	if err := json.Unmarshal(encoded, &sectionRaw); err != nil {
		return err
	}
	raw[name] = sectionRaw
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	out = append(bytes.TrimRight(out, "\n"), '\n')
	return atomicWriteFile(c.path, out)
}

func atomicWriteFile(path string, data []byte) (err error) {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if err = tmp.Chmod(info.Mode().Perm()); err != nil {
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
