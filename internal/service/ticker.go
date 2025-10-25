package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	spot "github.com/adshao/go-binance/v2"
	"github.com/stash86/binance-proxy/internal/tool"
)

type TickerSrv struct {
	rw sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	logger *slog.Logger

	initCtx  context.Context
	initDone context.CancelFunc

	si         *symbolInterval
	ticker24hr *Ticker24hr
	bookTicker *BookTicker
}

type BookTicker struct {
	Symbol      string `json:"symbol"`
	BidPrice    string `json:"bidPrice"`
	BidQuantity string `json:"bidQty"`
	AskPrice    string `json:"askPrice"`
	AskQuantity string `json:"askQty"`
}

type Ticker24hr struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	PrevClosePrice     string `json:"prevClosePrice"`
	LastPrice          string `json:"lastPrice"`
	LastQty            string `json:"lastQty"`
	BidPrice           string `json:"bidPrice"`
	AskPrice           string `json:"askPrice"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	FirstID            int64  `json:"firstId"`
	LastID             int64  `json:"lastId"`
	Count              int64  `json:"count"`
}

func NewTickerSrv(ctx context.Context, logger *slog.Logger, si *symbolInterval) *TickerSrv {
	logger = logger.With("symbol", si.Symbol, "class", si.Class)

	s := &TickerSrv{si: si, logger: logger}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.initCtx, s.initDone = context.WithCancel(context.Background())

	return s
}

func (s *TickerSrv) Start() {
	go func() {
		for d := tool.NewDelayIterator(); ; d.Delay() {
			s.rw.Lock()
			s.ticker24hr = nil
			s.bookTicker = nil
			s.rw.Unlock()

			ticker24hrDoneC, ticker24hrstopC, err := s.connectTicker24hr()
			if err != nil {
				s.logger.Error("Ticker24hr websocket connection error", "error", err)
				continue
			}

			bookDoneC, bookStopC, err := s.connectTickerBook()
			if err != nil {
				bookStopC <- struct{}{}
				s.logger.Error("BookTicker websocket connection error", "error", err)
				continue
			}

			s.logger.Info("Ticker24hr and BookTicker websocket connected")

			select {
			case <-s.ctx.Done():
				bookStopC <- struct{}{}
				ticker24hrstopC <- struct{}{}
				return
			case <-bookDoneC:
				ticker24hrstopC <- struct{}{}
			case <-ticker24hrDoneC:
				bookStopC <- struct{}{}
			}

			s.logger.Warn("Ticker24hr or BookTicker websocket disconnected, trying to reconnect.")
		}
	}()
}

func (s *TickerSrv) Stop() {
	s.cancel()
}

func (s *TickerSrv) connectTickerBook() (doneC, stopC chan struct{}, err error) {
	return spot.WsBookTickerServe(s.si.Symbol, s.wsHandlerBookTicker, s.errHandler)
}

func (s *TickerSrv) connectTicker24hr() (doneC, stopC chan struct{}, err error) {
	return spot.WsMarketStatServe(s.si.Symbol, s.wsHandlerTicker24hr, s.errHandler)
}

func (s *TickerSrv) GetTicker() *Ticker24hr {
	<-s.initCtx.Done()
	s.rw.RLock()
	defer s.rw.RUnlock()

	bidPrice := s.ticker24hr.BidPrice
	askPrice := s.ticker24hr.AskPrice
	if s.bookTicker != nil {
		bidPrice = s.bookTicker.BidPrice
		askPrice = s.bookTicker.AskPrice
	}

	return &Ticker24hr{
		Symbol:             s.ticker24hr.Symbol,
		PriceChange:        s.ticker24hr.PriceChange,
		PriceChangePercent: s.ticker24hr.PriceChangePercent,
		WeightedAvgPrice:   s.ticker24hr.WeightedAvgPrice,
		PrevClosePrice:     s.ticker24hr.PrevClosePrice,
		LastPrice:          askPrice,
		LastQty:            s.ticker24hr.LastQty,
		BidPrice:           bidPrice,
		AskPrice:           askPrice,
		OpenPrice:          s.ticker24hr.OpenPrice,
		HighPrice:          s.ticker24hr.HighPrice,
		LowPrice:           s.ticker24hr.LowPrice,
		Volume:             s.ticker24hr.Volume,
		QuoteVolume:        s.ticker24hr.QuoteVolume,
		OpenTime:           s.ticker24hr.OpenTime,
		CloseTime:          s.ticker24hr.CloseTime,
		FirstID:            s.ticker24hr.FirstID,
		LastID:             s.ticker24hr.LastID,
		Count:              s.ticker24hr.Count,
	}
}

func (s *TickerSrv) wsHandlerBookTicker(event *spot.WsBookTickerEvent) {
	s.rw.Lock()
	defer s.rw.Unlock()

	s.bookTicker = &BookTicker{
		Symbol:      event.Symbol,
		BidPrice:    event.BestBidPrice,
		BidQuantity: event.BestBidQty,
		AskPrice:    event.BestAskPrice,
		AskQuantity: event.BestAskQty,
	}

	s.logger.Debug("BookTicker websocket message received", "bidPrice", event.BestBidPrice, "askPrice", event.BestAskPrice)
}

func (s *TickerSrv) wsHandlerTicker24hr(event *spot.WsMarketStatEvent) {
	s.rw.Lock()
	defer s.rw.Unlock()

	if s.ticker24hr == nil {
		defer s.initDone()
	}

	s.ticker24hr = &Ticker24hr{
		Symbol:             event.Symbol,
		PriceChange:        event.PriceChange,
		PriceChangePercent: event.PriceChangePercent,
		WeightedAvgPrice:   event.WeightedAvgPrice,
		PrevClosePrice:     event.PrevClosePrice,
		LastPrice:          event.LastPrice,
		LastQty:            event.CloseQty,
		BidPrice:           event.BidPrice,
		AskPrice:           event.AskPrice,
		OpenPrice:          event.OpenPrice,
		HighPrice:          event.HighPrice,
		LowPrice:           event.LowPrice,
		Volume:             event.BaseVolume,
		QuoteVolume:        event.QuoteVolume,
		OpenTime:           event.OpenTime,
		CloseTime:          event.CloseTime,
		FirstID:            event.FirstID,
		LastID:             event.LastID,
		Count:              event.Count,
	}
	s.logger.Debug("Ticker24hr websocket message received", "lastPrice", event.LastPrice)
}

func (s *TickerSrv) errHandler(err error) {
	if strings.Contains(err.Error(), "context canceled") {
		s.logger.Warn("Ticker websocket context canceled, will restart connection.", "error", err)
	} else {
		s.logger.Error("Ticker24hr websocket connection error", "error", err)
	}
}
