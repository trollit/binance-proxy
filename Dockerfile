FROM alpine

COPY bin/binance-proxy /binance-proxy

EXPOSE 8090
EXPOSE 8091

ENTRYPOINT ["/binance-proxy"]
