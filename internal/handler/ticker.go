package handler

import (
	"encoding/json"
	"net/http"
)

func (s *Handler) ticker(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	if symbol == "" {
		s.logger.Debug("Ticker24hr request without symbol, proxying via REST", "class", s.class)
		s.reverseProxy(w, r)
		return
	}

	ticker := s.srv.Ticker(symbol)
	if ticker == nil {
		s.logger.Debug("Ticker24hr data not available, proxying via REST", "class", s.class, "symbol", symbol)
		s.reverseProxy(w, r)
		return
	} else {
		s.logger.Debug("Ticker24hr data available, delivering via websocket cache", "class", s.class, "symbol", symbol)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Data-Source", "websocket")

	buf := GetBuffer()
	defer PutBuffer(buf)

	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(ticker); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Write(buf.Bytes())
}
