package service

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Service struct {
	ctx    context.Context
	cancel context.CancelFunc

	logger *slog.Logger

	banDetector     *BanDetector
	class           Class
	exchangeInfoSrv *ExchangeInfoSrv
	klinesSrv       sync.Map // map[symbolInterval]*Klines
	depthSrv        sync.Map // map[symbolInterval]*Depth
	tickerSrv       sync.Map // map[symbolInterval]*Ticker

	lastGetKlines sync.Map // map[symbolInterval]time.Time
	lastGetDepth  sync.Map // map[symbolInterval]time.Time
	lastGetTicker sync.Map // map[symbolInterval]time.Time
}

func NewService(ctx context.Context, logger *slog.Logger, bd *BanDetector, class Class) *Service {
	s := &Service{class: class, logger: logger}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.exchangeInfoSrv = NewExchangeInfoSrv(s.ctx, logger, bd, NewSymbolInterval(s.class, "", ""))
	s.exchangeInfoSrv.Start()

	go func() {
		t := time.NewTimer(time.Second)
		defer t.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.autoRemoveExpired()
				t.Reset(time.Second)
			}
		}
	}()

	return s
}

func (s *Service) autoRemoveExpired() {
	now := time.Now() // Cache time.Now() call

	s.klinesSrv.Range(func(k, v interface{}) bool {
		si := k.(symbolInterval)
		srv := v.(*KlinesSrv)

		if t, ok := s.lastGetKlines.Load(si); ok {
			expiry := 2 * INTERVAL_2_DURATION[si.Interval]
			if now.Sub(t.(time.Time)) > expiry {
				s.logger.Debug("Kline websocket closed after being idle", "class", si.Class, "symbol", si.Symbol, "interval", si.Interval, "duration", expiry.Seconds())
				// Remove from all caches
				s.lastGetKlines.Delete(si)
				s.klinesSrv.Delete(si)
				srv.Stop()
			}
		} else {
			s.lastGetKlines.Store(si, now)
		}
		return true
	})
	s.depthSrv.Range(func(k, v interface{}) bool {
		si := k.(symbolInterval)
		srv := v.(*DepthSrv)

		if t, ok := s.lastGetDepth.Load(si); ok {
			expiry := 2 * time.Minute
			if now.Sub(t.(time.Time)) > expiry {
				s.logger.Debug("Depth websocket closed after being idle", "class", si.Class, "symbol", si.Symbol, "duration", expiry.Seconds())
				s.lastGetDepth.Delete(si)
				s.depthSrv.Delete(si)
				srv.Stop()
			}
		} else {
			s.lastGetDepth.Store(si, now)
		}
		return true
	})
	s.tickerSrv.Range(func(k, v interface{}) bool {
		si := k.(symbolInterval)
		srv := v.(*TickerSrv)

		if t, ok := s.lastGetTicker.Load(si); ok {
			expiry := 2 * time.Minute
			if now.Sub(t.(time.Time)) > expiry {
				s.logger.Debug("Ticker websocket closed after being idle", "class", si.Class, "symbol", si.Symbol, "duration", expiry.Seconds())
				// Remove from all caches
				s.lastGetTicker.Delete(si)
				s.tickerSrv.Delete(si)
				srv.Stop()
			}
		} else {
			s.lastGetTicker.Store(si, now)
		}
		return true
	})
}

func (s *Service) Ticker(symbol string) *Ticker24hr {
	si := NewSymbolInterval(s.class, symbol, "")
	srv, loaded := s.tickerSrv.Load(*si)
	if !loaded {
		if srv, loaded = s.tickerSrv.LoadOrStore(*si, NewTickerSrv(s.ctx, s.logger, si)); !loaded {
			srv.(*TickerSrv).Start()
		}
	}
	s.lastGetTicker.Store(*si, time.Now())

	return srv.(*TickerSrv).GetTicker()
}

func (s *Service) ExchangeInfo() []byte {
	return s.exchangeInfoSrv.GetExchangeInfo()
}

func (s *Service) Klines(symbol, interval string) []*Kline {
	si := NewSymbolInterval(s.class, symbol, interval)
	srv, loaded := s.klinesSrv.Load(*si)
	if !loaded {
		if srv, loaded = s.klinesSrv.LoadOrStore(*si, NewKlinesSrv(s.ctx, s.logger, s.banDetector, si)); !loaded {
			srv.(*KlinesSrv).Start()
		}
	}
	s.lastGetKlines.Store(*si, time.Now())

	return srv.(*KlinesSrv).GetKlines()
}

func (s *Service) Depth(symbol string) *Depth {
	si := NewSymbolInterval(s.class, symbol, "")
	srv, loaded := s.depthSrv.Load(*si)
	if !loaded {
		if srv, loaded = s.depthSrv.LoadOrStore(*si, NewDepthSrv(s.ctx, s.logger, si)); !loaded {
			srv.(*DepthSrv).Start()
		}
	}
	s.lastGetDepth.Store(*si, time.Now())

	return srv.(*DepthSrv).GetDepth()
}
