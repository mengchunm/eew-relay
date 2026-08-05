#!/usr/bin/env python3
"""Train and export the lightweight mainland-China intensity model.

The generated Go file contains only static tree nodes. Production never loads
the training data or a Python/ML runtime. Required Python packages:

    numpy lxml scikit-learn

The source dataset is maintained by the National Earthquake Science Data
Center. Exact duplicate rows, invalid/negative operational intensities, zero
motion rows, and samples outside the runtime model domain are removed before
training. Events receive equal total weight so one densely recorded earthquake
cannot dominate the model.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import pathlib
import urllib.request

import numpy as np
from lxml import html
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import GroupKFold


DATA_URL = (
    "https://data.earthquake.cn/datashare/report.shtml?PAGEID=ground_motion_list"
    "&report1_PAGESIZE=40000&minM=4&maxM=10"
)
MODEL_VERSION = "nedc-ground-motion-monotonic-hgb-v1"
MAX_CORRECTION = 0.8


def fetch_rows() -> list[tuple[str, ...]]:
    request = urllib.request.Request(
        DATA_URL,
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://data.earthquake.cn/"},
    )
    root = html.fromstring(urllib.request.urlopen(request, timeout=120).read())
    rows = []
    for tr in root.xpath('//tr[contains(@class,"cls-data-tr-even")]'):
        cells = tuple(" ".join(cell.text_content().split()) for cell in tr.xpath("./td"))
        if len(cells) == 13:
            rows.append(cells)
    if not rows:
        raise RuntimeError("official dataset returned no data rows")
    return rows


def number(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def clean(rows: list[tuple[str, ...]]):
    # Visible columns: time, event lat/lon/depth/M, place, network, station,
    # epicentral distance, instrument intensity, site, PGA and PGV.
    unique_rows = list(dict.fromkeys(rows))
    accepted = []
    for row in unique_rows:
        lat, lon, depth, magnitude = map(number, (row[1], row[2], row[3], row[4]))
        distance, intensity, pga, pgv = map(number, (row[8], row[9], row[11], row[12]))
        values = (lat, lon, depth, magnitude, distance, intensity, pga, pgv)
        if not all(math.isfinite(value) for value in values):
            continue
        if not (4 <= magnitude <= 8 and 0 <= distance <= 700 and 0 <= depth <= 100):
            continue
        if not (0 <= intensity <= 12) or pga <= 0 or pgv < 0:
            continue
        event = f"{row[0]}|{lat:.3f}|{lon:.3f}|{depth:.1f}|{magnitude:.1f}"
        accepted.append((magnitude, math.log(distance + 7), depth, lat, lon, intensity, event))
    if len(accepted) < 10000:
        raise RuntimeError(f"too few clean rows: {len(accepted)}")
    array = np.asarray([item[:6] for item in accepted], dtype=float)
    groups = np.asarray([item[6] for item in accepted], dtype=object)
    features, target = array[:, :5], array[:, 5]
    names, counts = np.unique(groups, return_counts=True)
    count_by_event = dict(zip(names, counts))
    weights = np.asarray([1 / count_by_event[group] for group in groups], dtype=float)
    weights *= len(weights) / weights.sum()
    return features, target, groups, weights


def new_model() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=0.08,
        max_iter=63,
        max_leaf_nodes=15,
        min_samples_leaf=50,
        l2_regularization=3,
        monotonic_cst=[1, -1, -1, 0, 0],
        random_state=1,
    )


def baseline(features: np.ndarray) -> np.ndarray:
    magnitude, log_distance = features[:, 0], features[:, 1]
    return np.maximum(0, 1.363 * magnitude + 2.941 - 1.494 * log_distance)


def bounded(model_values: np.ndarray, base_values: np.ndarray) -> np.ndarray:
    return np.clip(model_values, base_values - MAX_CORRECTION, base_values + MAX_CORRECTION)


def validate(features, target, groups, weights) -> None:
    predictions = np.zeros_like(target)
    for train, test in GroupKFold(5).split(features, target, groups):
        model = new_model()
        model.fit(features[train], target[train], sample_weight=weights[train])
        predictions[test] = bounded(model.predict(features[test]), baseline(features[test]))
    errors = predictions - target
    event_mae = np.average(np.abs(errors), weights=weights)
    print(
        f"clean_records={len(target)} events={len(np.unique(groups))} "
        f"mae={np.mean(np.abs(errors)):.3f} rmse={np.sqrt(np.mean(errors**2)):.3f} "
        f"bias={np.mean(errors):+.3f} event_mae={event_mae:.3f}"
    )


def flatten_model(model: HistGradientBoostingRegressor):
    nodes = []
    roots = []
    for stage in model._predictors:  # Stable generated artifact; never used at runtime.
        predictor = stage[0]
        offset = len(nodes)
        roots.append(offset)
        for node in predictor.nodes:
            leaf = bool(node["is_leaf"])
            nodes.append(
                (
                    -1 if leaf else int(node["feature_idx"]),
                    0 if leaf else offset + int(node["left"]),
                    0 if leaf else offset + int(node["right"]),
                    bool(node["missing_go_to_left"]),
                    0.0 if leaf else float(node["num_threshold"]),
                    float(node["value"]) if leaf else 0.0,
                )
            )
    return float(model._baseline_prediction[0, 0]), roots, nodes


def generated_predict(features: np.ndarray, base_value, roots, nodes) -> np.ndarray:
    output = np.full(len(features), base_value, dtype=float)
    for row_index, row in enumerate(features):
        for root in roots:
            index = root
            while nodes[index][0] >= 0:
                feature, left, right, missing_left, threshold, _ = nodes[index]
                value = row[feature]
                index = left if (math.isnan(value) and missing_left) or value <= threshold else right
            output[row_index] += nodes[index][5]
    return output


def go_float(value: float) -> str:
    return format(value, ".9g")


def export_go(path: pathlib.Path, model, features, target, groups) -> None:
    base_value, roots, nodes = flatten_model(model)
    check = generated_predict(features[:500], base_value, roots, nodes)
    expected = model.predict(features[:500])
    if not np.allclose(check, expected, atol=1e-6):
        raise RuntimeError(f"generated inference mismatch: {np.max(np.abs(check-expected))}")
    digest = hashlib.sha256()
    digest.update(np.ascontiguousarray(features).tobytes())
    digest.update(np.ascontiguousarray(target).tobytes())
    digest.update("\n".join(groups.tolist()).encode())
    lines = [
        "// Code generated by tools/train_intensity_model.py; DO NOT EDIT.",
        "package main",
        "",
        f'const officialIntensityModelVersion = "{MODEL_VERSION}-{digest.hexdigest()[:12]}"',
        f"const officialIntensityModelBase = {go_float(base_value)}",
        f"const officialIntensityModelRecordCount = {len(target)}",
        f"const officialIntensityModelEventCount = {len(np.unique(groups))}",
        f"const officialIntensityModelMinLatitude = {go_float(features[:, 3].min())}",
        f"const officialIntensityModelMaxLatitude = {go_float(features[:, 3].max())}",
        f"const officialIntensityModelMinLongitude = {go_float(features[:, 4].min())}",
        f"const officialIntensityModelMaxLongitude = {go_float(features[:, 4].max())}",
        "",
        "type officialIntensityModelNode struct {",
        "\tFeature int8",
        "\tLeft uint16",
        "\tRight uint16",
        "\tMissingLeft bool",
        "\tThreshold float32",
        "\tValue float32",
        "}",
        "",
        "var officialIntensityModelRoots = [...]uint16{" + ",".join(map(str, roots)) + "}",
        "var officialIntensityModelNodes = [...]officialIntensityModelNode{",
    ]
    for feature, left, right, missing_left, threshold, value in nodes:
        lines.append(
            "\t{Feature:%d,Left:%d,Right:%d,MissingLeft:%s,Threshold:%s,Value:%s},"
            % (feature, left, right, str(missing_left).lower(), go_float(threshold), go_float(value))
        )
    lines.extend(["}", ""])
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {path} trees={len(roots)} nodes={len(nodes)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("intensity_model_generated.go"))
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args()
    features, target, groups, weights = clean(fetch_rows())
    if not args.skip_validation:
        validate(features, target, groups, weights)
    model = new_model()
    model.fit(features, target, sample_weight=weights)
    export_go(args.output, model, features, target, groups)


if __name__ == "__main__":
    main()
