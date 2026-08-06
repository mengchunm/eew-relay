#!/usr/bin/env python3
"""Train and export GB/T 17742-2020 ground-motion models.

The runtime API does not provide local waveforms or measured PGA/PGV.  These
small monotonic models estimate log10(PGA) and log10(PGV) from the fields that
are available when an alert arrives.  Go then applies the national-standard
instrumental-intensity equations.  A separately trained, high-intensity-
weighted calibration head is exported for the optional hybrid mode.

The generated Go artifact contains static tree nodes only; production does not
need Python, scikit-learn, the source dataset, or network access.
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
MODEL_VERSION = "nedc-gbt17742-2020-hgb-v2"
STANDARD_NAME = "GB/T 17742-2020"
CONSISTENCY_LIMIT = 0.5
HYBRID_DIRECT_WEIGHT = 0.75
HYBRID_MAX_CALIBRATION = 0.8


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


def standard_intensity(log10_pga_mps2, log10_pgv_mps):
    acceleration = 3.17 * log10_pga_mps2 + 6.59
    velocity = 3.00 * log10_pgv_mps + 9.77
    combined = np.where(
        (acceleration >= 6.0) & (velocity >= 6.0),
        velocity,
        (acceleration + velocity) / 2,
    )
    return np.clip(combined, 1.0, 12.0)


def clean(rows: list[tuple[str, ...]]):
    # Visible columns: time, event lat/lon/depth/M, place, network, station,
    # epicentral distance, instrumental intensity, site, total PGA and PGV.
    # The visible PGA/PGV values are cm/s² and cm/s and rounded to one decimal.
    accepted = []
    rejected_consistency = 0
    for row in dict.fromkeys(rows):
        lat, lon, depth, magnitude = map(number, (row[1], row[2], row[3], row[4]))
        distance, intensity, pga, pgv = map(number, (row[8], row[9], row[11], row[12]))
        values = (lat, lon, depth, magnitude, distance, intensity, pga, pgv)
        if not all(math.isfinite(value) for value in values):
            continue
        if not (4 <= magnitude <= 8 and 0 <= distance <= 700 and 0 < depth <= 100):
            continue
        if not (1 <= intensity <= 12) or pga <= 0 or pgv <= 0:
            continue
        log_pga = math.log10(pga / 100)  # cm/s² -> m/s²
        log_pgv = math.log10(pgv / 100)  # cm/s  -> m/s
        reconstructed = float(standard_intensity(np.asarray(log_pga), np.asarray(log_pgv)))
        if abs(reconstructed - intensity) > CONSISTENCY_LIMIT:
            rejected_consistency += 1
            continue
        event = f"{row[0]}|{lat:.3f}|{lon:.3f}|{depth:.1f}|{magnitude:.1f}"
        accepted.append(
            (magnitude, math.log(distance + 7), depth, lat, lon, log_pga, log_pgv, intensity, event)
        )
    if len(accepted) < 25000:
        raise RuntimeError(f"too few clean rows: {len(accepted)}")
    array = np.asarray([item[:8] for item in accepted], dtype=float)
    groups = np.asarray([item[8] for item in accepted], dtype=object)
    features = array[:, :5]
    log_pga, log_pgv, intensity = array[:, 5], array[:, 6], array[:, 7]
    names, counts = np.unique(groups, return_counts=True)
    count_by_event = dict(zip(names, counts))
    event_weights = np.asarray([1 / count_by_event[group] for group in groups], dtype=float)
    event_weights *= len(event_weights) / event_weights.sum()
    # Sparse high-intensity observations otherwise have little effect on the
    # objective.  Ramp their weight smoothly to avoid a discontinuity at VI.
    high_factor = 1 + 3 * np.clip((intensity - 4) / 3, 0, 1)
    calibration_weights = event_weights * high_factor
    calibration_weights *= len(calibration_weights) / calibration_weights.sum()
    print(
        f"clean_records={len(intensity)} events={len(names)} "
        f"consistency_rejected={rejected_consistency} intensity_ge_6={np.sum(intensity >= 6)}"
    )
    return features, log_pga, log_pgv, intensity, groups, event_weights, calibration_weights


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


def hybrid_intensity(standard_values, calibration_values):
    blended = (
        (1 - HYBRID_DIRECT_WEIGHT) * standard_values
        + HYBRID_DIRECT_WEIGHT * calibration_values
    )
    return np.clip(
        blended,
        standard_values - HYBRID_MAX_CALIBRATION,
        standard_values + HYBRID_MAX_CALIBRATION,
    )


def metrics(name, prediction, target, event_weights):
    error = prediction - target
    high = target >= 6
    print(
        f"{name}: mae={np.mean(np.abs(error)):.3f} "
        f"event_mae={np.average(np.abs(error), weights=event_weights):.3f} "
        f"bias={np.mean(error):+.3f} "
        f"mae_ge_6={np.mean(np.abs(error[high])):.3f}"
    )


def validate(features, log_pga, log_pgv, intensity, groups, event_weights, calibration_weights):
    predicted_pga = np.zeros_like(log_pga)
    predicted_pgv = np.zeros_like(log_pgv)
    predicted_direct = np.zeros_like(intensity)
    for train, test in GroupKFold(5).split(features, intensity, groups):
        pga_model, pgv_model, direct_model = new_model(), new_model(), new_model()
        pga_model.fit(features[train], log_pga[train], sample_weight=event_weights[train])
        pgv_model.fit(features[train], log_pgv[train], sample_weight=event_weights[train])
        direct_model.fit(
            features[train], intensity[train], sample_weight=calibration_weights[train]
        )
        predicted_pga[test] = pga_model.predict(features[test])
        predicted_pgv[test] = pgv_model.predict(features[test])
        predicted_direct[test] = direct_model.predict(features[test])
    standard = standard_intensity(predicted_pga, predicted_pgv)
    hybrid = hybrid_intensity(standard, predicted_direct)
    metrics("gbt2020", standard, intensity, event_weights)
    metrics("calibration", predicted_direct, intensity, event_weights)
    metrics("hybrid", hybrid, intensity, event_weights)


def flatten_model(model: HistGradientBoostingRegressor):
    nodes = []
    roots = []
    for stage in model._predictors:
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


def generated_predict(features, base_value, roots, nodes):
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


def append_go_model(lines, prefix, model, features):
    base, roots, nodes = flatten_model(model)
    actual = generated_predict(features[:500], base, roots, nodes)
    expected = model.predict(features[:500])
    if not np.allclose(actual, expected, atol=1e-6):
        raise RuntimeError(f"generated {prefix} inference mismatch")
    lines.extend(
        [
            f"const {prefix}Base = {go_float(base)}",
            f"var {prefix}Roots = [...]uint16{{" + ",".join(map(str, roots)) + "}",
            f"var {prefix}Nodes = [...]gbtIntensityModelNode{{",
        ]
    )
    for feature, left, right, missing_left, threshold, value in nodes:
        lines.append(
            "\t{Feature:%d,Left:%d,Right:%d,MissingLeft:%s,Threshold:%s,Value:%s},"
            % (feature, left, right, str(missing_left).lower(), go_float(threshold), go_float(value))
        )
    lines.extend(["}", ""])


def export_go(path, models, features, targets, groups):
    digest = hashlib.sha256()
    digest.update(np.ascontiguousarray(features).tobytes())
    for target in targets:
        digest.update(np.ascontiguousarray(target).tobytes())
    digest.update("\n".join(groups.tolist()).encode())
    version = f"{MODEL_VERSION}-{digest.hexdigest()[:12]}"
    lines = [
        "// Code generated by tools/train_gbt_intensity_model.py; DO NOT EDIT.",
        "package main",
        "",
        f'const gbtIntensityModelVersion = "{version}"',
        f'const gbtIntensityStandardName = "{STANDARD_NAME}"',
        f"const gbtIntensityModelRecordCount = {len(features)}",
        f"const gbtIntensityModelEventCount = {len(np.unique(groups))}",
        f"const gbtIntensityModelMinLatitude = {go_float(features[:, 3].min())}",
        f"const gbtIntensityModelMaxLatitude = {go_float(features[:, 3].max())}",
        f"const gbtIntensityModelMinLongitude = {go_float(features[:, 4].min())}",
        f"const gbtIntensityModelMaxLongitude = {go_float(features[:, 4].max())}",
        f"const gbtHybridDirectWeight = {go_float(HYBRID_DIRECT_WEIGHT)}",
        f"const gbtHybridMaxCalibration = {go_float(HYBRID_MAX_CALIBRATION)}",
        "",
        "type gbtIntensityModelNode struct {",
        "\tFeature int8",
        "\tLeft uint16",
        "\tRight uint16",
        "\tMissingLeft bool",
        "\tThreshold float32",
        "\tValue float32",
        "}",
        "",
    ]
    append_go_model(lines, "gbtPGAModel", models[0], features)
    append_go_model(lines, "gbtPGVModel", models[1], features)
    append_go_model(lines, "gbtCalibrationModel", models[2], features)
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {path} version={version}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=pathlib.Path, default=pathlib.Path("intensity_model_gbt_generated.go")
    )
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args()
    data = clean(fetch_rows())
    features, log_pga, log_pgv, intensity, groups, event_weights, calibration_weights = data
    if not args.skip_validation:
        validate(*data)
    models = [new_model(), new_model(), new_model()]
    models[0].fit(features, log_pga, sample_weight=event_weights)
    models[1].fit(features, log_pgv, sample_weight=event_weights)
    models[2].fit(features, intensity, sample_weight=calibration_weights)
    export_go(args.output, models, features, (log_pga, log_pgv, intensity), groups)


if __name__ == "__main__":
    main()
