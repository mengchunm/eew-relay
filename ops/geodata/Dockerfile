FROM postgis/postgis:16-3.5-alpine

ARG EEW_GEODATA_VERSION=2025.251231.260403

LABEL org.opencontainers.image.title="EEW Relay fixed China administrative boundaries" \
      org.opencontainers.image.description="PostGIS 16 with the fixed EEW Relay administrative boundary dataset" \
      org.opencontainers.image.source="https://github.com/mengchunm/eew-relay" \
      org.opencontainers.image.version="${EEW_GEODATA_VERSION}" \
      org.opencontainers.image.licenses="MIT"

COPY --chmod=0444 AREA_CITY_LICENSE /usr/share/licenses/AreaCity-JsSpider-StatsGov/LICENSE
COPY --chmod=0444 data/eew-admin-boundaries.sql.gz /docker-entrypoint-initdb.d/20-eew-admin-boundaries.sql.gz
COPY --chmod=0444 data/eew-admin-boundary-indexes.sql /docker-entrypoint-initdb.d/21-eew-admin-boundary-indexes.sql

RUN test -s /docker-entrypoint-initdb.d/20-eew-admin-boundaries.sql.gz \
    && test -s /docker-entrypoint-initdb.d/21-eew-admin-boundary-indexes.sql
