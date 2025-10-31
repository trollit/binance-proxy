package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	spot "github.com/adshao/go-binance/v2"
	futures "github.com/adshao/go-binance/v2/futures"
	"github.com/stash86/binance-proxy/internal/tool"
)

type DepthSrv struct {
	rw sync.RWMutex

	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
	initCtx  context.Context
	initDone context.CancelFunc

	si    *symbolInterval
	depth *Depth
}

type Depth struct {
	LastUpdateID int64
	Time         int64
	TradeTime    int64
	Bids         []futures.Bid
	Asks         []futures.Ask
}

func NewDepthSrv(ctx context.Context, logger *slog.Logger, si *symbolInterval) *DepthSrv {
	s := &DepthSrv{si: si, logger: logger}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.initCtx, s.initDone = context.WithCancel(context.Background())

	return s
}

func (s *DepthSrv) Start() {
	go func() {
		for d := tool.NewDelayIterator(); ; d.Delay() {
			s.rw.Lock()
			s.depth = nil
			s.rw.Unlock()

			doneC, stopC, err := s.connect()
			if err != nil {
				s.logger.Error("Depth websocket connection error", "class", s.si.Class, "symbol", s.si.Symbol, "error", err)
				continue
			}

			s.logger.Info("Depth websocket connected", "class", s.si.Class, "symbol", s.si.Symbol)

			// Reset the reconnect backoff now that we have a successful connection
			d.Reset()
			select {
			case <-s.ctx.Done():
				stopC <- struct{}{}
				return
			case <-doneC:
			}

			s.logger.Warn("Depth websocket disconnected, trying to reconnect", "class", s.si.Class, "symbol", s.si.Symbol)
		}
	}()
}

func (s *DepthSrv) Stop() {
	s.cancel()
}

func (s *DepthSrv) connect() (doneC, stopC chan struct{}, err error) {
	if s.si.Class == SPOT {
		return spot.WsPartialDepthServe100Ms(s.si.Symbol, "20", s.wsHandler, s.errHandler)
	} else {
		return futures.WsPartialDepthServeWithRate(s.si.Symbol, 20, 100*time.Millisecond, s.wsHandlerFutures, s.errHandler)
	}
}

func (s *DepthSrv) GetDepth() *Depth {
	<-s.initCtx.Done()
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.depth
}

func (s *DepthSrv) wsHandlerFutures(event *futures.WsDepthEvent) {
	s.rw.Lock()
	defer s.rw.Unlock()

	if s.depth == nil {
		defer s.initDone()
	}

	s.depth = &Depth{
		LastUpdateID: event.LastUpdateID,
		Time:         event.Time,
		TradeTime:    event.TransactionTime,
		Bids:         event.Bids,
		Asks:         event.Asks,
	}

	s.logger.Debug("Depth websocket message received", "class", s.si.Class, "symbol", s.si.Symbol, "lastUpdateID", event.LastUpdateID)
}

func (s *DepthSrv) wsHandler(event *spot.WsPartialDepthEvent) {
	s.rw.Lock()
	defer s.rw.Unlock()

	if s.depth == nil {
		defer s.initDone()
	}

	s.depth = &Depth{
		LastUpdateID: event.LastUpdateID,
		Time:         time.Now().UnixNano() / 1e6,
		TradeTime:    time.Now().UnixNano() / 1e6,
		Bids:         event.Bids,
		Asks:         event.Asks,
	}

	s.logger.Debug("Depth websocket message received", "class", s.si.Class, "symbol", s.si.Symbol, "lastUpdateID", event.LastUpdateID)
}

func (s *DepthSrv) errHandler(err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "context canceled"):
		s.logger.Warn("Depth websocket context canceled, will restart connection", "class", s.si.Class, "symbol", s.si.Symbol)
	case strings.Contains(msg, "use of closed network connection"):
		// This commonly indicates a normal remote close/rotation; treat as info/debug to reduce noise
		s.logger.Info("Depth websocket closed by peer; reconnecting", "class", s.si.Class, "symbol", s.si.Symbol)
	default:
		s.logger.Error("Depth websocket connection error", "class", s.si.Class, "symbol", s.si.Symbol, "error", err)
	}
}
