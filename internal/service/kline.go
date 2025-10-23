package service

import (
	"binance-proxy/internal/tool"
	"container/list"
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	spot "github.com/adshao/go-binance/v2"
	futures "github.com/adshao/go-binance/v2/futures"
)

type Kline struct {
	OpenTime                 int64
	Open                     string
	High                     string
	Low                      string
	Close                    string
	Volume                   string
	CloseTime                int64
	QuoteAssetVolume         string
	TradeNum                 int64
	TakerBuyBaseAssetVolume  string
	TakerBuyQuoteAssetVolume string
}

type KlinesSrv struct {
	rw sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	logger   *slog.Logger
	initCtx  context.Context
	initDone context.CancelFunc

	banDetector *BanDetector
	si          *symbolInterval
	klinesList  *list.List
	klinesArr   []*Kline
}

func NewKlinesSrv(ctx context.Context, logger *slog.Logger, bd *BanDetector, si *symbolInterval) *KlinesSrv {
	logger = logger.With("symbol", si.Symbol, "interval", si.Interval, "class", si.Class)

	s := &KlinesSrv{logger: logger, si: si, banDetector: bd}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.initCtx, s.initDone = context.WithCancel(context.Background())

	return s
}

func (s *KlinesSrv) Start() {
	go func() {
		for d := tool.NewDelayIterator(); ; d.Delay() {
			s.rw.Lock()
			s.klinesList = nil
			s.rw.Unlock()

			doneC, stopC, err := s.connect()
			if err != nil {
				s.logger.Error("Kline websocket connection error", "error", err)
				continue
			}

			s.logger.Debug("Kline websocket connected")
			select {
			case <-s.ctx.Done():
				stopC <- struct{}{}
				return
			case <-doneC:
			}
			s.logger.Warn("Kline websocket disconnected, trying to reconnect")
		}
	}()
}

func (s *KlinesSrv) Stop() {
	s.cancel()
}

func (s *KlinesSrv) errHandler(err error) {
	if strings.Contains(err.Error(), "context canceled") {
		s.logger.Warn("Kline websocket context canceled, will restart connection.")
	} else {
		s.logger.Error("Kline websocket connection error", "err", err)
	}
}

func (s *KlinesSrv) connect() (doneC, stopC chan struct{}, err error) {
	if s.si.Class == SPOT {
		return spot.WsKlineServe(s.si.Symbol,
			s.si.Interval,
			func(event *spot.WsKlineEvent) { s.wsHandler(event) },
			s.errHandler,
		)
	} else {
		return futures.WsKlineServe(s.si.Symbol,
			s.si.Interval,
			func(event *futures.WsKlineEvent) { s.wsHandler(event) },
			s.errHandler,
		)
	}
}

func (s *KlinesSrv) initKlineData() {
	// Check if API is banned
	if s.banDetector.IsBanned(s.si.Class) {
		s.logger.Debug("Kline initialization skipped due to API ban")

		// Create empty klines list to prevent repeated initialization attempts
		s.klinesList = list.New()
		defer s.initDone()
		return
	}

	var klines interface{}
	var err error
	s.logger.Debug("Kline initialization through REST.")
	for d := tool.NewDelayIterator(); ; d.Delay() {
		// Check ban status before each attempt
		if s.banDetector.IsBanned(s.si.Class) {
			s.logger.Debug("Kline initialization stopped due to API ban")
			s.klinesList = list.New()
			defer s.initDone()
			return
		}

		var resp *http.Response
		if s.si.Class == SPOT {
			RateWait(s.ctx, s.si.Class, http.MethodGet, "/api/v3/klines", url.Values{
				"limit": []string{"1000"},
			})
			client := spot.NewClient("", "")
			klines, err = client.NewKlinesService().
				Symbol(s.si.Symbol).Interval(s.si.Interval).Limit(1000).
				Do(s.ctx)
		} else {
			RateWait(s.ctx, s.si.Class, http.MethodGet, "/fapi/v1/klines", url.Values{
				"limit": []string{"1000"},
			})
			client := futures.NewClient("", "")
			klines, err = client.NewKlinesService().
				Symbol(s.si.Symbol).Interval(s.si.Interval).Limit(1000).
				Do(s.ctx)
		}

		// Check for bans (resp might be nil for SDK calls, so we check err)
		if s.banDetector.CheckResponse(s.si.Class, resp, err) {
			s.logger.Debug("Kline initialization stopped due to detected ban")
			s.klinesList = list.New()
			defer s.initDone()
			return
		}

		if err != nil {
			s.logger.Error("Kline initialization via REST retrying...")
			continue
		}

		s.klinesList = list.New()

		if vi, ok := klines.([]*spot.Kline); ok {
			for _, v := range vi {
				t := &Kline{
					OpenTime:                 v.OpenTime,
					Open:                     v.Open,
					High:                     v.High,
					Low:                      v.Low,
					Close:                    v.Close,
					Volume:                   v.Volume,
					CloseTime:                v.CloseTime,
					QuoteAssetVolume:         v.QuoteAssetVolume,
					TradeNum:                 v.TradeNum,
					TakerBuyBaseAssetVolume:  v.TakerBuyBaseAssetVolume,
					TakerBuyQuoteAssetVolume: v.TakerBuyQuoteAssetVolume,
				}

				s.klinesList.PushBack(t)
			}
		} else if vi, ok := klines.([]*futures.Kline); ok {
			for _, v := range vi {
				t := &Kline{
					OpenTime:                 v.OpenTime,
					Open:                     v.Open,
					High:                     v.High,
					Low:                      v.Low,
					Close:                    v.Close,
					Volume:                   v.Volume,
					CloseTime:                v.CloseTime,
					QuoteAssetVolume:         v.QuoteAssetVolume,
					TradeNum:                 v.TradeNum,
					TakerBuyBaseAssetVolume:  v.TakerBuyBaseAssetVolume,
					TakerBuyQuoteAssetVolume: v.TakerBuyQuoteAssetVolume,
				}

				s.klinesList.PushBack(t)
			}
		}

		defer s.initDone()
		break
	}
}

func (s *KlinesSrv) wsHandler(event interface{}) {

	if s.klinesList == nil {
		s.initKlineData()
	}

	// Merge kline
	var k *Kline
	if vi, ok := event.(*spot.WsKlineEvent); ok {
		k = &Kline{
			OpenTime:                 vi.Kline.StartTime,
			Open:                     vi.Kline.Open,
			High:                     vi.Kline.High,
			Low:                      vi.Kline.Low,
			Close:                    vi.Kline.Close,
			Volume:                   vi.Kline.Volume,
			CloseTime:                vi.Kline.EndTime,
			QuoteAssetVolume:         vi.Kline.QuoteVolume,
			TradeNum:                 vi.Kline.TradeNum,
			TakerBuyBaseAssetVolume:  vi.Kline.ActiveBuyVolume,
			TakerBuyQuoteAssetVolume: vi.Kline.ActiveBuyQuoteVolume,
		}
	} else if vi, ok := event.(*futures.WsKlineEvent); ok {
		k = &Kline{
			OpenTime:                 vi.Kline.StartTime,
			Open:                     vi.Kline.Open,
			High:                     vi.Kline.High,
			Low:                      vi.Kline.Low,
			Close:                    vi.Kline.Close,
			Volume:                   vi.Kline.Volume,
			CloseTime:                vi.Kline.EndTime,
			QuoteAssetVolume:         vi.Kline.QuoteVolume,
			TradeNum:                 vi.Kline.TradeNum,
			TakerBuyBaseAssetVolume:  vi.Kline.ActiveBuyVolume,
			TakerBuyQuoteAssetVolume: vi.Kline.ActiveBuyQuoteVolume,
		}
	}

	s.logger.Debug("Kline websocket message received", "openTime", k.OpenTime)

	if s.klinesList.Back().Value.(*Kline).OpenTime < k.OpenTime {
		s.klinesList.PushBack(k)
	} else if s.klinesList.Back().Value.(*Kline).OpenTime == k.OpenTime {
		s.klinesList.Back().Value = k
	}

	for s.klinesList.Len() > 1000 {
		s.klinesList.Remove(s.klinesList.Front())
	}

	klinesArr := make([]*Kline, s.klinesList.Len())
	i := 0
	for elems := s.klinesList.Front(); elems != nil; elems = elems.Next() {
		klinesArr[i] = elems.Value.(*Kline)
		i++
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	s.klinesArr = klinesArr
}

func (s *KlinesSrv) GetKlines() []*Kline {
	<-s.initCtx.Done()
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.klinesArr
}
