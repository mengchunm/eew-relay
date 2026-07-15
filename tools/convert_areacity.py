#!/usr/bin/env python3

import argparse
import csv
import gzip
import json
import sys


def parse_ring(value):
    ring = []
    for pair in value.split(","):
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
    for part in value.split(";"):
        rings = [parse_ring(ring) for ring in part.split("~")]
        rings = [ring for ring in rings if ring]
        if rings:
            polygons.append(rings)
    return polygons


def main():
    parser = argparse.ArgumentParser(description="Convert AreaCity CSV boundaries to gzipped GeoJSON Sequence.")
    parser.add_argument("source_csv")
    parser.add_argument("output_gzip")
    args = parser.parse_args()
    csv.field_size_limit(sys.maxsize)

    counts = {1: 0, 2: 0, 3: 0}
    with open(args.source_csv, encoding="utf-8-sig", newline="") as source:
        with gzip.open(args.output_gzip, "wt", encoding="utf-8", compresslevel=6) as output:
            for row in csv.DictReader(source):
                polygon = row.get("polygon", "").strip()
                if polygon in ("", "EMPTY"):
                    continue
                coordinates = parse_polygon(polygon)
                if not coordinates:
                    continue
                level = int(row["deep"]) + 1
                feature = {
                    "type": "Feature",
                    "properties": {
                        "area_id": row["id"],
                        "parent_id": row["pid"],
                        "level": level,
                        "name_zh": row["name"].strip(),
                        "path_zh": row["ext_path"].strip(),
                    },
                    "geometry": {"type": "MultiPolygon", "coordinates": coordinates},
                }
                output.write(json.dumps(feature, ensure_ascii=False, separators=(",", ":")))
                output.write("\n")
                counts[level] += 1

    if counts[1] < 34 or counts[2] < 350 or counts[3] < 2800:
        raise SystemExit(f"too few boundaries: {counts}")
    print(f"converted ADM1={counts[1]} ADM2={counts[2]} ADM3={counts[3]}")


if __name__ == "__main__":
    main()
