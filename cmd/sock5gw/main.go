package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/dnsproxy"
	"sock5gw/internal/gateway"
	"sock5gw/internal/manager"
	"sock5gw/internal/routing"
	"sock5gw/internal/store"
)

func main() {
	configPath := flag.String("config", "config.example.json", "path to JSON config")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	db, err := store.Open(cfg.DBPath)
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	mgr, err := manager.New(cfg, db)
	if err != nil {
		slog.Error("create manager", "err", err)
		os.Exit(1)
	}
	router, err := routing.New(cfg.Routing)
	if err != nil {
		slog.Error("create routing matcher", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dnsSrv := dnsproxy.New(cfg.DNS, mgr.FakeIPStore())
	go func() {
		if err := dnsSrv.Run(ctx); err != nil {
			slog.Error("dns failed", "err", err)
			stop()
		}
	}()

	gw := gateway.New(cfg.Gateway, cfg.DNS, mgr, router)
	runtimeCfg := manager.NewRuntimeConfig(*configPath, cfg, gw.SetRouter)
	api := &http.Server{
		Addr:              cfg.API.Listen,
		Handler:           manager.NewAPI(mgr, cfg.API, runtimeCfg),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("api listening", "addr", cfg.API.Listen)
		if err := api.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("api failed", "err", err)
			stop()
		}
	}()

	go func() {
		if err := gw.Run(ctx); err != nil {
			slog.Error("gateway failed", "err", err)
			stop()
		}
	}()

	mgr.Start(ctx)

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = api.Shutdown(shutdownCtx)
	mgr.CloseClientConnections("")
}
