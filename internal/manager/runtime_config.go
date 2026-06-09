package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"sync"

	"sock5gw/internal/config"
	"sock5gw/internal/routing"
)

type RuntimeConfig struct {
	mu        sync.RWMutex
	path      string
	routing   config.Routing
	applyFunc func(*routing.Matcher)
}

func NewRuntimeConfig(path string, cfg *config.Config, applyFunc func(*routing.Matcher)) *RuntimeConfig {
	return &RuntimeConfig{
		path:      path,
		routing:   cfg.Routing,
		applyFunc: applyFunc,
	}
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
	if err := c.writeRouting(next); err != nil {
		return err
	}
	c.mu.Lock()
	c.routing = next
	c.mu.Unlock()
	if c.applyFunc != nil {
		c.applyFunc(matcher)
	}
	return nil
}

func (c *RuntimeConfig) writeRouting(next config.Routing) error {
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
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	var routingRaw any
	if err := json.Unmarshal(encoded, &routingRaw); err != nil {
		return err
	}
	raw["routing"] = routingRaw
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	out = append(bytes.TrimRight(out, "\n"), '\n')
	return os.WriteFile(c.path, out, 0644)
}
