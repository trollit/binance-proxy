package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func (s *Handler) klines(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.With(
		"class", s.class,
		"method", r.Method,
		"uri", r.RequestURI,
		"remote", r.RemoteAddr,
	)

	// Check if API is banned
	if s.banDetector.IsBanned(s.class) {
		logger.Debug("klines request returning empty due to API ban")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Data-Source", "ban-protection")

		_, err := w.Write([]byte("[]"))
		if err != nil {
			s.logger.Error("Failed to write ban protection response", "err", err)
		}

		return
	}

	var fakeKlineTimestampOpen int64 = 0
	symbol := r.URL.Query().Get("symbol")
	interval := r.URL.Query().Get("interval")
	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "500"
	}
	limitInt, err := strconv.Atoi(limit)

	switch {
	case err != nil, limitInt <= 0, limitInt > 1000, r.URL.Query().Get("startTime") != "", r.URL.Query().Get("endTime") != "", symbol == "", interval == "":
		logger.Debug("Proxying via rest api", "symbol", symbol, "interval", interval, "limit", limit)
		s.reverseProxy(w, r)
		return
	}

	data := s.srv.Klines(symbol, interval)
	if data == nil {
		logger.Debug("Proxying via rest api", "symbol", symbol, "interval", interval, "limit", limit)
		s.reverseProxy(w, r)
		return
	}

	dataLen := len(data)
	minLen := dataLen
	if minLen > limitInt {
		minLen = limitInt
	}

	// Pre-allocate with exact length (not just capacity)
	klines := make([]interface{}, minLen)

	// Calculate start index once
	startIdx := dataLen - minLen
	for i := 0; i < minLen; i++ {
		dataIdx := startIdx + i
		klines[i] = []interface{}{
			data[dataIdx].OpenTime,
			data[dataIdx].Open,
			data[dataIdx].High,
			data[dataIdx].Low,
			data[dataIdx].Close,
			data[dataIdx].Volume,
			data[dataIdx].CloseTime,
			data[dataIdx].QuoteAssetVolume,
			data[dataIdx].TradeNum,
			data[dataIdx].TakerBuyBaseAssetVolume,
			data[dataIdx].TakerBuyQuoteAssetVolume,
			"0",
		}
	}

	currentTime := time.Now().UnixNano() / 1e6
	if dataLen > 0 && currentTime > data[dataLen-1].CloseTime {
		fakeKlineTimestampOpen = data[dataLen-1].CloseTime + 1
		logger.Debug("Kline requested for future timestamp", "symbol", symbol, "interval", interval, "timestamp", strconv.FormatInt(fakeKlineTimestampOpen, 10))
	}

	if s.enableFakeKline && dataLen > 0 && currentTime > data[dataLen-1].CloseTime {
		logger.Debug("Kline faking candle for timestamp", "symbol", symbol, "interval", interval, "timestamp", strconv.FormatInt(fakeKlineTimestampOpen, 10))
		lastData := data[dataLen-1]
		fakeKline := []interface{}{
			lastData.CloseTime + 1,
			lastData.Close,
			lastData.Close,
			lastData.Close,
			lastData.Close,
			"0.0",
			lastData.CloseTime + 1 + (lastData.CloseTime - lastData.OpenTime),
			"0.0",
			0,
			"0.0",
			"0.0",
			"0",
		}

		if len(klines) >= minLen {
			klines[len(klines)-1] = fakeKline
		} else {
			klines = append(klines, fakeKline)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Data-Source", "websocket")

	// Use shared buffer pool
	buf := GetBuffer()
	defer PutBuffer(buf)

	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(klines); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		logger.Error("Failed to write response", "err", err)
	}
}
