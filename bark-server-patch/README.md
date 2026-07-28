# Hardened Bark server image

This directory builds a narrowly patched image from Bark server v2.3.5 at the
pinned upstream commit `478659ecdd75a38185d7275d154d78e9c2b752b4`.

The patch adds:

- a bounded MySQL connection pool and query timeout;
- a read-through device-token cache, updated on registration and deletion;
- HTTP 404 for missing device keys and HTTP 503 for transient database errors;
- a process-wide APNs in-flight request limit.

Build it with:

```sh
docker build -t bark-server:eew-v2.3.5.1 bark-server-patch
```

Runtime controls:

| Environment variable | Default |
| --- | ---: |
| `BARK_SERVER_MYSQL_MAX_OPEN_CONNS` | `24` |
| `BARK_SERVER_MYSQL_MAX_IDLE_CONNS` | `12` |
| `BARK_SERVER_MYSQL_CONN_MAX_IDLE_TIME` | `5m` |
| `BARK_SERVER_MYSQL_CONN_MAX_LIFETIME` | `30m` |
| `BARK_SERVER_MYSQL_QUERY_TIMEOUT` | `5s` |
| `BARK_SERVER_DEVICE_CACHE_TTL` | `10m` |
| `BARK_SERVER_MAX_INFLIGHT_PUSHES` | `256` |

Keep `BARK_SERVER_MYSQL_MAX_OPEN_CONNS` below the database server's connection
limit and reserve capacity for administrative access and any separate key
verification pool. The upstream source and patch remain under Bark server's MIT
license; the upstream license is copied into the runtime image.
