FROM alpine/git:2.47.2 AS source

ARG BARK_SERVER_COMMIT=478659ecdd75a38185d7275d154d78e9c2b752b4

RUN git clone --filter=blob:none https://github.com/Finb/bark-server.git /src \
    && cd /src \
    && git checkout --detach "${BARK_SERVER_COMMIT}" \
    && test "$(git rev-parse HEAD)" = "${BARK_SERVER_COMMIT}"

COPY bark-server-v2.3.5-eew.patch /tmp/bark-server.patch
RUN cd /src \
    && git apply --recount --check /tmp/bark-server.patch \
    && git apply --recount /tmp/bark-server.patch

FROM golang:1.25.6-alpine AS builder

RUN apk add --no-cache ca-certificates git
COPY --from=source /src /src
WORKDIR /src

# Upstream's root test suite intentionally requires a real APNs device token.
# Compile and test the patched leaf packages, then compile the main package.
RUN go test ./database ./apns
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -buildid= -X 'main.version=v2.3.5-eew.3' -X 'main.buildDate=reproducible' -X 'main.commitID=478659ecdd75a38185d7275d154d78e9c2b752b4'" \
    -o /out/bark-server .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 1000 bark \
    && adduser -S -D -H -u 1000 -G bark bark

COPY --from=builder /out/bark-server /usr/local/bin/bark-server
COPY --from=source /src/LICENSE /usr/share/licenses/bark-server/LICENSE

USER bark:bark
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/bark-server"]
