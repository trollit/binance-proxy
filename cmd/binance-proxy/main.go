package main

import (
	"binance-proxy/internal/handler"
	"binance-proxy/internal/logcache"
	"binance-proxy/internal/service"
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/jessevdk/go-flags"
)

func startProxy(ctx context.Context, logger *slog.Logger, bd *service.BanDetector, port int, class service.Class, disablefakekline bool, alwaysshowforwards bool, errChan chan<- error) {
	mux := http.NewServeMux()
	address := fmt.Sprintf(":%d", port)
	mux.HandleFunc("/", handler.NewHandler(ctx, logger, bd, class, !disablefakekline, alwaysshowforwards))

	// Create an HTTP server with a custom ErrorLog that suppresses repeated lines
	srv := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      75 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog: stdlog.New(
			logcache.NewSuppressingWriter(os.Stderr),
			"", stdlog.LstdFlags,
		),
	}

	logger.Info("websocket proxy starting", "class", class, "port", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("websocket proxy start failed", "class", class, "error", err)
		errChan <- fmt.Errorf("%s proxy failed: %w", class, err)
	}
}

func handleSignal() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for s := range signalChan {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			cancel()
		}
	}
}

type Config struct {
	Verbose            []bool `short:"v" long:"verbose" env:"BPX_VERBOSE" description:"Verbose output (increase with -vv)"`
	SpotAddress        int    `short:"p" long:"port-spot" env:"BPX_PORT_SPOT" description:"Port to which to bind for SPOT markets" default:"8090"`
	FuturesAddress     int    `short:"t" long:"port-futures" env:"BPX_PORT_FUTURES" description:"Port to which to bind for FUTURES markets" default:"8091"`
	DisableFakeKline   bool   `short:"c" long:"disable-fake-candles" env:"BPX_DISABLE_FAKE_CANDLES" description:"Disable generation of fake candles (ohlcv) when sockets have not delivered data yet"`
	DisableSpot        bool   `short:"s" long:"disable-spot" env:"BPX_DISABLE_SPOT" description:"Disable proxying spot markets"`
	DisableFutures     bool   `short:"f" long:"disable-futures" env:"BPX_DISABLE_FUTURES" description:"Disable proxying futures markets"`
	AlwaysShowForwards bool   `short:"a" long:"always-show-forwards" env:"BPX_ALWAYS_SHOW_FORWARDS" description:"Always show requests forwarded via REST even if verbose is disabled"`
}

var (
	config      Config
	parser             = flags.NewParser(&config, flags.Default)
	Version     string = "1.0.4"
	Buildtime   string = "2025-08-11"
	ctx, cancel        = context.WithCancel(context.Background())
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logcache.SetLoggerHook(func(level, msg string) {
		switch level {
		case "warn":
			logger.Warn(msg)
		case "error":
			logger.Error(msg)
		case "info":
			logger.Info(msg)
		default:
			logger.Info(msg)
		}
	})
	logcache.SetWriterHook(func(msg string) {
		// net/http ErrorLog messages typically include trailing newlines
		if len(msg) > 0 && msg[len(msg)-1] == '\n' {
			msg = msg[:len(msg)-1]
		}

		logger.Warn("http request", "msg", msg)
	})

	logger.Info("Binance proxy version", "version", Version, "build", Buildtime)

	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			logger.Error("failed parsing flags", "error", err, "type", flagsErr.Type)
		}
	}
	if config.DisableSpot && config.DisableFutures {
		logger.Error("can't start if both SPOT and FUTURES are disabled!")
		os.Exit(1)
	}

	if !config.DisableFakeKline {
		logger.Info("Fake candles are enabled for faster processing, the feature can be disabled with --disable-fake-candles or -c")
	}

	if config.AlwaysShowForwards {
		logger.Info("Always show forwards is enabled, all API requests, that can't be served from websockets cached will be logged.")
	}

	go handleSignal()

	// Channel to collect errors from proxy goroutines
	errChan := make(chan error, 2) // Buffer for up to 2 proxies
	var proxyCount int

	banDetector := service.NewBanDetector(logger)

	if !config.DisableSpot {
		proxyCount++
		go startProxy(ctx, logger, banDetector, config.SpotAddress, service.SPOT, config.DisableFakeKline, config.AlwaysShowForwards, errChan)
	}

	if !config.DisableFutures {
		proxyCount++
		go startProxy(ctx, logger, banDetector, config.FuturesAddress, service.FUTURES, config.DisableFakeKline, config.AlwaysShowForwards, errChan)
	}

	// Wait for either context cancellation or errors from proxies
	var collectedErrors []error
	done := false

	for !done {
		select {
		case <-ctx.Done():
			logger.Info("SIGINT received, aborting ...")
			done = true
		case err := <-errChan:
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				// If all proxies have failed, exit
				if len(collectedErrors) >= proxyCount {
					done = true
				}
			}
		}
	}

	// Log any collected errors
	if len(collectedErrors) > 0 {
		combinedErr := errors.Join(collectedErrors...)
		logger.Error("Proxy errors occurred", "error", combinedErr)
	}
}
