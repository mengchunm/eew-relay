# GeoSCK 2026 site-condition data

`geosck_vs30_cn_1arcmin.bin.gz` is a runtime-optimized derivative of the
GeoSCK 2026 mainland-China Vs30 map. The source map combines 7,939 engineering
boreholes, 30-arcsecond topographic slope, and 1:1,500,000 surface geology.

- Authors: Jian Zhou, Yufang Rong, Li Li, Yefei Ren, and Xin Tian
- Paper: *An Additive Framework for Developing a Hybrid VS30 Model:
  Incorporating Geological Information into the Existing SCK Model for an
  Updated VS30 Map of Chinese Mainland* (2026)
- DOI: https://doi.org/10.1016/j.enggeo.2026.108926
- Data: https://www.seismisite.net/
- License: CC BY 4.0

The committed derivative uses one-arc-minute cells, stores the median of each
2x2 source block, quantizes Vs30 in 8 m/s increments, and retains nodata cells.
It is used only for constant-time subscription-location lookup.
