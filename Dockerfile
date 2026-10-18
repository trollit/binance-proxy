FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG BUILDTIME=unknown

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -tags netgo -trimpath \
    -ldflags "-s -w -X main.Version=${VERSION} -X main.Buildtime=${BUILDTIME}" \
    -o /out/binance-proxy ./cmd/binance-proxy

FROM alpine

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /out/binance-proxy /binance-proxy

EXPOSE 8090
EXPOSE 8091

ENTRYPOINT ["/binance-proxy"]
