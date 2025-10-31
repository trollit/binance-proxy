package service

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"golang.org/x/time/rate"
)

var (
	SpotLimiter    = rate.NewLimiter(20, 1200)
	FuturesLimiter = rate.NewLimiter(40, 2400)
)

func getKlinesWeight(limit int) int {
	switch {
	case limit >= 1 && limit < 100:
		return 1
	case limit >= 100 && limit < 500:
		return 2
	case limit >= 500 && limit <= 1000:
		return 5
	case limit > 1000 && limit <= 1500:
		return 10
	default:
		return 5
	}
}

func getSpotDepthWeight(limit int) int {
	switch {
	case limit >= 5 && limit <= 100:
		return 1
	case limit >= 100 && limit < 500:
		return 2
	case limit == 500:
		return 5
	case limit == 1000:
		return 10
	case limit == 5000:
		return 50
	default:
		return 1
	}
}

func getFuturesDepthWeight(limit int) int {
	switch {
	case limit >= 5 && limit <= 50:
		return 2
	case limit == 100:
		return 5
	case limit == 500:
		return 10
	case limit == 1000:
		return 20
	default:
		return 2
	}
}

func calculateWeight(method, path string, query url.Values) int {
	switch path {
	case "/fapi/v1/klines":
		limitInt, _ := strconv.Atoi(query.Get("limit"))
		return getKlinesWeight(limitInt)
	case "/api/v3/depth":
		limitInt, _ := strconv.Atoi(query.Get("limit"))
		return getSpotDepthWeight(limitInt)
	case "/fapi/v1/depth":
		limitInt, _ := strconv.Atoi(query.Get("limit"))
		return getFuturesDepthWeight(limitInt)
	case "/api/v3/ticker/24hr", "/fapi/v1/ticker/24hr":
		if query.Get("symbol") == "" {
			return 40
		}
		return 1
	case "/api/v3/exchangeInfo", "/fapi/v1/exchangeInfo", "/api/v3/account", "/api/v3/myTrades":
		return 10
	case "/api/v3/order":
		if method == http.MethodGet {
			return 2
		}
		return 1
	case "/fapi/v1/userTrades", "/fapi/v2/account":
		return 5
	default:
		return 1
	}
}

func RateWait(ctx context.Context, class Class, method, path string, query url.Values) error {
	weight := calculateWeight(method, path, query)

	if class == SPOT {
		return SpotLimiter.WaitN(ctx, weight)
	} else {
		return FuturesLimiter.WaitN(ctx, weight)
	}
}
