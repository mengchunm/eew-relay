#!/usr/bin/env python3
"""Build the compact runtime Vs30 grid from the GeoSCK 2026 GeoTIFF.

The source map is CC BY 4.0 and is not committed. Download it from:
https://www.seismisite.net/Download/VS30_ChineseMainland_GeoSCK_30arcsec_Version2026.zip

Runtime output is a deterministic gzip stream containing a small binary header
and one-byte Vs30 cells. Each output cell is the median of a 2x2 source block
(one arc-minute); values are quantized in 8 m/s increments and zero is nodata.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import math
import pathlib
import struct
import warnings

import numpy as np
from PIL import Image


MAGIC = b"EEWVS30\x01"
SCALE = 8.0
SOURCE_NODATA_LIMIT = -1e20


def build(source: pathlib.Path, output: pathlib.Path) -> None:
    image = Image.open(source)
    if image.mode != "F":
        raise RuntimeError(f"expected a float GeoTIFF, got {image.mode}")
    source_width, source_height = image.size
    tiepoint = image.tag_v2.get(33922)
    pixel_scale = image.tag_v2.get(33550)
    if not tiepoint or not pixel_scale:
        raise RuntimeError("GeoTIFF is missing tiepoint or pixel-scale metadata")
    origin_lon = float(tiepoint[3])
    origin_lat = float(tiepoint[4])
    source_step = float(pixel_scale[0])
    if abs(source_step - float(pixel_scale[1])) > 1e-9:
        raise RuntimeError("GeoTIFF longitude and latitude steps differ")

    factor = 2
    width = math.ceil(source_width / factor)
    height = math.ceil(source_height / factor)
    cells = bytearray(width * height)
    valid_values: list[float] = []
    for row in range(height):
        top = row * factor
        bottom = min(source_height, top + factor)
        block = np.asarray(image.crop((0, top, source_width, bottom)), dtype=np.float32)
        if block.shape[0] < factor:
            block = np.pad(block, ((0, factor - block.shape[0]), (0, 0)), constant_values=np.nan)
        if block.shape[1] % factor:
            block = np.pad(block, ((0, 0), (0, 1)), constant_values=np.nan)
        block[block < SOURCE_NODATA_LIMIT] = np.nan
        with np.errstate(all="ignore"), warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            reduced = np.nanmedian(block.reshape(factor, width, factor), axis=(0, 2))
        for column, value in enumerate(reduced):
            if not math.isfinite(float(value)) or value < 100 or value > 2040:
                continue
            quantized = max(1, min(255, round(float(value) / SCALE)))
            cells[row * width + column] = quantized
            valid_values.append(float(quantized) * SCALE)

    header = struct.pack(
        "<8sII4d",
        MAGIC,
        width,
        height,
        origin_lon,
        origin_lat,
        source_step * factor,
        SCALE,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as compressed:
            compressed.write(header)
            compressed.write(cells)

    digest = hashlib.sha256(header + cells).hexdigest()
    print(
        f"wrote {output} compressed={output.stat().st_size} raw={len(header) + len(cells)} "
        f"grid={width}x{height} valid={len(valid_values)} "
        f"range={min(valid_values):.0f}-{max(valid_values):.0f} sha256={digest}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=pathlib.Path)
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=pathlib.Path("site_data/geosck_vs30_cn_1arcmin.bin.gz"),
    )
    args = parser.parse_args()
    build(args.source, args.output)


if __name__ == "__main__":
    main()
