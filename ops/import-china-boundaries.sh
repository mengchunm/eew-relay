#!/bin/sh

set -eu
umask 077

DOCKER=${DOCKER:-/usr/bin/docker}
ENV_FILE=${ENV_FILE:-/etc/eew-relay/scale.env}
POSTGRES_CONTAINER=${POSTGRES_CONTAINER:-eew-postgres}
POSTGRES_DB=${POSTGRES_DB:-eew}
POSTGRES_USER=${POSTGRES_USER:-eew}
DOCKER_NETWORK=${DOCKER_NETWORK:-eew-relay}
GDAL_IMAGE=${GDAL_IMAGE:-ghcr.io/osgeo/gdal:alpine-small-latest}
EXTRACT_IMAGE=${EXTRACT_IMAGE:-alpine:3.23}
AREACITY_VERSION=${AREACITY_VERSION:-2025.251231.260403}
AREACITY_URL=${AREACITY_URL:-https://github.com/xiangyuecn/AreaCity-JsSpider-StatsGov/releases/download/2025.251231.260403/ok_geo.csv.7z}
AREACITY_EXPECTED_BYTES=${AREACITY_EXPECTED_BYTES:-16518032}
AREACITY_CSV=${AREACITY_CSV:-}
AREACITY_GEOJSONSEQ_GZ=${AREACITY_GEOJSONSEQ_GZ:-}

postgres_password=$(sed -n 's/^POSTGRES_PASSWORD=//p' "$ENV_FILE" | head -n 1)
case "$postgres_password" in
  '"'*'"') postgres_password=${postgres_password#\"}; postgres_password=${postgres_password%\"} ;;
esac
if [ -z "$postgres_password" ]; then
  printf 'POSTGRES_PASSWORD is missing\n' >&2
  exit 1
fi

workdir=$(mktemp -d /tmp/eew-china-boundaries.XXXXXX)
archive="$workdir/ok_geo.csv.7z"
csv_file="$workdir/ok_geo.csv"
geojsonseq_file="$workdir/areacity.geojsonseq.gz"
geojsonseq_plain="$workdir/areacity.geojsonseq"
cleanup() {
  rm -rf -- "$workdir"
}
trap cleanup EXIT HUP INT TERM

if [ -n "$AREACITY_GEOJSONSEQ_GZ" ]; then
  if [ ! -f "$AREACITY_GEOJSONSEQ_GZ" ]; then
    printf 'AreaCity GeoJSONSeq gzip does not exist: %s\n' "$AREACITY_GEOJSONSEQ_GZ" >&2
    exit 1
  fi
  if [ "$(wc -c <"$AREACITY_GEOJSONSEQ_GZ")" -lt 50000000 ]; then
    printf 'AreaCity GeoJSONSeq gzip is unexpectedly small\n' >&2
    exit 1
  fi
  gzip -t "$AREACITY_GEOJSONSEQ_GZ"
  geojsonseq_file=$AREACITY_GEOJSONSEQ_GZ
elif [ -n "$AREACITY_CSV" ]; then
  if [ ! -f "$AREACITY_CSV" ]; then
    printf 'AreaCity CSV does not exist: %s\n' "$AREACITY_CSV" >&2
    exit 1
  fi
  csv_file=$AREACITY_CSV
else
  curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 15 --max-time 900 \
    -o "$archive" "$AREACITY_URL"
  actual_bytes=$(wc -c <"$archive")
  if [ "$actual_bytes" -ne "$AREACITY_EXPECTED_BYTES" ]; then
    printf 'unexpected AreaCity archive size: expected=%s actual=%s\n' \
      "$AREACITY_EXPECTED_BYTES" "$actual_bytes" >&2
    exit 1
  fi
  "$DOCKER" run --rm -v "$workdir:/work" "$EXTRACT_IMAGE" sh -c \
    'apk add --no-cache 7zip >/dev/null && 7z x -y /work/ok_geo.csv.7z -o/work >/dev/null'
fi

if [ -z "$AREACITY_GEOJSONSEQ_GZ" ]; then
  if [ "$(wc -c <"$csv_file")" -lt 100000000 ]; then
    printf 'AreaCity CSV is unexpectedly small\n' >&2
    exit 1
  fi

  python3 - "$csv_file" "$geojsonseq_file" <<'PY'
import csv
import gzip
import json
import sys

source_path, output_path = sys.argv[1:]
csv.field_size_limit(sys.maxsize)

def parse_ring(value):
    ring = []
    for pair in value.split(','):
        fields = pair.strip().split()
        if len(fields) != 2:
            continue
        point = [round(float(fields[0]), 7), round(float(fields[1]), 7)]
        if not ring or ring[-1] != point:
            ring.append(point)
    if len(ring) >= 3 and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring if len(ring) >= 4 else []

def parse_polygon(value):
    polygons = []
    for part in value.split(';'):
        rings = [parse_ring(ring) for ring in part.split('~')]
        rings = [ring for ring in rings if ring]
        if rings:
            polygons.append(rings)
    return polygons

counts = {1: 0, 2: 0, 3: 0}
with open(source_path, encoding='utf-8-sig', newline='') as source:
    with gzip.open(output_path, 'wt', encoding='utf-8', compresslevel=6) as output:
        for row in csv.DictReader(source):
            polygon = row.get('polygon', '').strip()
            if polygon in ('', 'EMPTY'):
                continue
            coordinates = parse_polygon(polygon)
            if not coordinates:
                continue
            level = int(row['deep']) + 1
            feature = {
                'type': 'Feature',
                'properties': {
                    'area_id': row['id'],
                    'parent_id': row['pid'],
                    'level': level,
                    'name_zh': row['name'].strip(),
                    'path_zh': row['ext_path'].strip(),
                },
                'geometry': {'type': 'MultiPolygon', 'coordinates': coordinates},
            }
            output.write(json.dumps(feature, ensure_ascii=False, separators=(',', ':')))
            output.write('\n')
            counts[level] += 1
if counts[1] < 34 or counts[2] < 350 or counts[3] < 2800:
    raise SystemExit(f'too few AreaCity boundary features: {counts}')
print(f'converted ADM1={counts[1]} ADM2={counts[2]} ADM3={counts[3]}', file=sys.stderr)
PY
fi

"$DOCKER" exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS postgis' >/dev/null

gzip -dc "$geojsonseq_file" >"$geojsonseq_plain"
if [ "$(wc -c <"$geojsonseq_plain")" -lt 100000000 ]; then
  printf 'AreaCity GeoJSONSeq is unexpectedly small after decompression\n' >&2
  exit 1
fi

"$DOCKER" run --rm --network "$DOCKER_NETWORK" \
  -v "$workdir:/import:ro" \
  -e PGPASSWORD="$postgres_password" \
  "$GDAL_IMAGE" ogr2ogr \
    -f PostgreSQL "PG:host=eew-postgres dbname=$POSTGRES_DB user=$POSTGRES_USER" \
    /import/areacity.geojsonseq \
    -if GeoJSONSeq \
    -nln eew_admin_boundary_import \
    -lco GEOMETRY_NAME=geom \
    -nlt PROMOTE_TO_MULTI -dim XY -t_srs EPSG:4326 \
    -overwrite -progress

"$DOCKER" exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 -v source_version="$AREACITY_VERSION" <<'SQL'
BEGIN;

DROP TABLE IF EXISTS eew_admin_boundaries_new;
CREATE TABLE eew_admin_boundaries_new (
  level smallint NOT NULL CHECK (level BETWEEN 1 AND 3),
  gid text NOT NULL,
  parent_gid text,
  name_en text NOT NULL,
  name_zh text NOT NULL,
  boundary_type text,
  area_sq_degrees double precision NOT NULL,
  geom geometry(MultiPolygon, 4326) NOT NULL,
  PRIMARY KEY (level, gid)
);

INSERT INTO eew_admin_boundaries_new
  (level, gid, parent_gid, name_en, name_zh, boundary_type, area_sq_degrees, geom)
SELECT
  level::smallint,
  area_id::text,
  NULLIF(parent_id::text, '0'),
  name_zh,
  name_zh,
  path_zh,
  ST_Area(clean_geom),
  clean_geom
FROM (
  SELECT *,
    ST_Multi(ST_CollectionExtract(
      CASE WHEN ST_IsValid(geom) THEN geom ELSE ST_Buffer(geom, 0) END,
      3
    ))::geometry(MultiPolygon, 4326) AS clean_geom
  FROM eew_admin_boundary_import
) source
WHERE clean_geom IS NOT NULL AND NOT ST_IsEmpty(clean_geom);

CREATE INDEX eew_admin_boundaries_new_geom_gist ON eew_admin_boundaries_new USING gist (geom);
CREATE INDEX eew_admin_boundaries_new_level_idx ON eew_admin_boundaries_new (level);
ANALYZE eew_admin_boundaries_new;

DROP TABLE IF EXISTS eew_admin_boundary_parts_new;
CREATE TABLE eew_admin_boundary_parts_new (
  level smallint NOT NULL,
  gid text NOT NULL,
  name_en text NOT NULL,
  name_zh text NOT NULL,
  area_sq_degrees double precision NOT NULL,
  geom geometry(Polygon, 4326) NOT NULL
);
INSERT INTO eew_admin_boundary_parts_new
  (level, gid, name_en, name_zh, area_sq_degrees, geom)
SELECT
  boundary.level,
  boundary.gid,
  boundary.name_en,
  boundary.name_zh,
  boundary.area_sq_degrees,
  dumped.geom::geometry(Polygon, 4326)
FROM eew_admin_boundaries_new boundary
CROSS JOIN LATERAL ST_Subdivide(boundary.geom, 256) subdivided(geom)
CROSS JOIN LATERAL ST_Dump(subdivided.geom) dumped;
CREATE INDEX eew_admin_boundary_parts_new_geom_gist
  ON eew_admin_boundary_parts_new USING gist (geom);
CREATE INDEX eew_admin_boundary_parts_new_level_gid_idx
  ON eew_admin_boundary_parts_new (level, gid);
ANALYZE eew_admin_boundary_parts_new;

DO $$
DECLARE
  level1_count integer;
  level2_count integer;
  level3_count integer;
  invalid_count integer;
  part_feature_count integer;
  invalid_part_count integer;
BEGIN
  SELECT count(*) INTO level1_count FROM eew_admin_boundaries_new WHERE level = 1;
  SELECT count(*) INTO level2_count FROM eew_admin_boundaries_new WHERE level = 2;
  SELECT count(*) INTO level3_count FROM eew_admin_boundaries_new WHERE level = 3;
  SELECT count(*) INTO invalid_count FROM eew_admin_boundaries_new WHERE NOT ST_IsValid(geom);
  SELECT count(DISTINCT (level, gid)) INTO part_feature_count
    FROM eew_admin_boundary_parts_new;
  SELECT count(*) INTO invalid_part_count
    FROM eew_admin_boundary_parts_new WHERE NOT ST_IsValid(geom);
  IF level1_count < 34 OR level2_count < 350 OR level3_count < 2800
      OR invalid_count <> 0 OR part_feature_count <> level1_count + level2_count + level3_count
      OR invalid_part_count <> 0 THEN
    RAISE EXCEPTION 'boundary validation failed: ADM1=% ADM2=% ADM3=% invalid=% part_features=% invalid_parts=%',
      level1_count, level2_count, level3_count, invalid_count,
      part_feature_count, invalid_part_count;
  END IF;
END $$;

DROP TABLE IF EXISTS eew_admin_boundary_parts;
DROP TABLE IF EXISTS eew_admin_boundaries;
ALTER TABLE eew_admin_boundaries_new RENAME TO eew_admin_boundaries;
ALTER INDEX eew_admin_boundaries_new_geom_gist RENAME TO eew_admin_boundaries_geom_gist;
ALTER INDEX eew_admin_boundaries_new_level_idx RENAME TO eew_admin_boundaries_level_idx;
ALTER TABLE eew_admin_boundary_parts_new RENAME TO eew_admin_boundary_parts;
ALTER INDEX eew_admin_boundary_parts_new_geom_gist RENAME TO eew_admin_boundary_parts_geom_gist;
ALTER INDEX eew_admin_boundary_parts_new_level_gid_idx RENAME TO eew_admin_boundary_parts_level_gid_idx;

DROP TABLE IF EXISTS eew_admin_boundary_metadata;
CREATE TABLE eew_admin_boundary_metadata (
  source text NOT NULL,
  source_version text NOT NULL,
  source_url text NOT NULL,
  coordinate_conversion text NOT NULL,
  imported_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO eew_admin_boundary_metadata
  (source, source_version, source_url, coordinate_conversion)
VALUES
  ('AreaCity-JsSpider-StatsGov', :'source_version',
   'https://github.com/xiangyuecn/AreaCity-JsSpider-StatsGov',
   'AreaCity original GCJ-02; query points converted from WGS84 to GCJ-02');

DROP TABLE eew_admin_boundary_import;
COMMIT;

SELECT level, count(*) AS boundaries FROM eew_admin_boundaries GROUP BY level ORDER BY level;
SELECT pg_size_pretty(pg_total_relation_size('eew_admin_boundaries')) AS total_relation_size;
SQL

printf 'AreaCity China administrative boundaries imported successfully\n'
