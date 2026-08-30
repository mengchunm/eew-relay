FROM --platform=$BUILDPLATFORM golang:1.25.13-alpine3.23 AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w -buildid=" \
    -o /out/eew-bark .

FROM alpine:3.23.5

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 1000 eew \
    && adduser -S -D -H -u 1000 -G eew eew

WORKDIR /app
COPY --from=build --chown=eew:eew /out/eew-bark /app/eew-bark
COPY --chown=eew:eew config.example.yaml /app/config.example.yaml

USER 1000:1000
EXPOSE 30010

ENTRYPOINT ["/app/eew-bark"]
CMD ["-config", "/app/config.yaml"]
