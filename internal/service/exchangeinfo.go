package service

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/stash86/binance-proxy/internal/tool"
)

type ExchangeInfoSrv struct {
	rw sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	banDetector *BanDetector
	logger      *slog.Logger

	initCtx  context.Context
	initDone context.CancelFunc

	refreshDur   time.Duration
	si           *symbolInterval
	exchangeInfo []byte
}

// HTTP client pool for connection reuse.
var (
	httpClientOnce sync.Once
	httpClient     *http.Client
)

func getHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		transport := &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
			ForceAttemptHTTP2:   true,
		}

		httpClient = &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		}
	})
	return httpClient
}

func NewExchangeInfoSrv(ctx context.Context, logger *slog.Logger, bd *BanDetector, si *symbolInterval) *ExchangeInfoSrv {
	s := &ExchangeInfoSrv{
		banDetector: bd,
		si:          si,
		refreshDur:  60 * time.Second,
		logger:      logger,
	}

	logger.Debug("ExchangeInfoSrv initialization", "class", s.si.Class, "refreshDur", s.refreshDur.Seconds())

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.initCtx, s.initDone = context.WithCancel(context.Background())

	return s
}

func (s *ExchangeInfoSrv) Start() {
	s.reTryRefreshExchangeInfo()

	go func() {
		rTimer := time.NewTimer(s.refreshDur)
		for {
			rTimer.Reset(s.refreshDur)
			select {
			case <-s.ctx.Done():
				rTimer.Stop()
				return
			case <-rTimer.C:
			}

			s.reTryRefreshExchangeInfo()
		}
	}()
}

// Nothing to do.
func (s *ExchangeInfoSrv) Stop() {}

func (s *ExchangeInfoSrv) GetExchangeInfo() []byte {
	<-s.initCtx.Done()
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.exchangeInfo
}

func (s *ExchangeInfoSrv) reTryRefreshExchangeInfo() {
	for d := tool.NewDelayIterator(); ; d.Delay() {
		if s.refreshExchangeInfo() == nil {
			break
		}
	}
}

func (s *ExchangeInfoSrv) refreshExchangeInfo() error {
	// Check if API is banned
	if s.banDetector.IsBanned(s.si.Class) {
		s.logger.Debug("ExchangeInfo refresh skipped due to API ban", "class", s.si.Class)
		return nil // Don't retry during ban
	}

	var url string
	if s.si.Class == SPOT {
		url = "https://api.binance.com/api/v3/exchangeInfo"
		if err := RateWait(s.ctx, s.si.Class, http.MethodGet, "/api/v3/exchangeInfo", nil); err != nil {
			return err
		}
	} else {
		url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
		if err := RateWait(s.ctx, s.si.Class, http.MethodGet, "/fapi/v1/exchangeInfo", nil); err != nil {
			return err
		}
	}

	// Use pooled HTTP client instead of http.Get()
	client := getHTTPClient()
	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, url, nil)
	if err != nil {
		s.logger.Error("ExchangeInfo request creation failed", "class", s.si.Class, "error", err)
		return err
	}

	resp, err := client.Do(req)

	// Check for bans
	if s.banDetector.CheckResponse(s.si.Class, resp, err) {
		if resp != nil {
			resp.Body.Close()
		}
		return err
	}

	if err != nil {
		s.logger.Error("ExchangeInfo refresh failed", "class", s.si.Class, "error", err)
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	if s.exchangeInfo == nil {
		defer s.initDone()
	}

	s.exchangeInfo = data

	s.logger.Debug("ExchangeInfo refreshed successfully", "class", s.si.Class)

	return nil
}
