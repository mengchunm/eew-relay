# Fixed geodata image

The public deployment uses an immutable PostGIS image containing the same
preconverted administrative boundary tables as production. The fixed dataset
is `AreaCity-JsSpider-StatsGov 2025.251231.260403` in its original GCJ-02
coordinates.

The large SQL inputs are intentionally excluded from Git. They are only needed
by the release maintainer when assembling the published container image:

```text
data/eew-admin-boundaries.sql.gz
data/eew-admin-boundary-indexes.sql
```

Build the release image with:

```bash
docker build \
  --build-arg EEW_GEODATA_VERSION=2025.251231.260403 \
  -t ghcr.io/mengchunm/eew-relay-geodata:2025.251231.260403 \
  ops/geodata
```

The image initializes the three `eew_admin_boundary*` tables only when
PostgreSQL starts with an empty data directory. Updating the EEW Relay
application image does not reimport or change the fixed boundary data.

Source and license:

- <https://github.com/xiangyuecn/AreaCity-JsSpider-StatsGov>
- MIT License, copyright 2019 xiangyuecn
