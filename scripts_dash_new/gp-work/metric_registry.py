# -*- coding: utf-8 -*-
"""Metric registry and flat metric adapter for Flow Pulse.

This module intentionally sits on top of the existing parsers.  It does not
replace the older nested metric dictionaries yet; it normalizes them into a
stable flat key space so future table/profile changes can be made in one place.
"""
from __future__ import print_function

try:
    from debug_log import debug_log
except Exception:  # pragma: no cover - logging must never break extraction
    def debug_log(*args, **kwargs):
        return None

REPORT_SOURCES = {
    "FE": {
        "fe_runtime": ["runtime.V2.rpt"],
        "fe_area": ["area.*.rpt"],
        "fe_util": ["utilization.*.rpt"],
        "fe_vth": ["cell_usage.summary.*.rpt"],
        "fe_multibit": ["multibit_banking_ratio.*.rpt"],
        "fe_congestion": ["congestion.*.rpt"],
        "fe_qor": ["qor.*.rpt"],
        "fe_power": ["report_power_info.mission.ss*.rpt"],
        "fe_app_options": ["report_app_options.full.*.rpt"],
    },
    "PNR.FC": {
        "stage_runtime": ["{stage}.runtime.rpt"],
        "stage_timing": ["{stage}.qor_group_sum.rpt", "*.qor_group_sum.rpt", "{stage}.qor_sum.rpt", "*.qor_sum.rpt"],
        "stage_area": ["{stage}.sec_get_area.rpt", "*.sec_get_area.rpt"],
        "stage_vth": ["{stage}.sec_vth_use.rpt", "*.sec_vth_use.rpt"],
        "stage_congestion": ["{stage}.grc.rpt", "*.grc.rpt"],
        "stage_power": ["report_power_info.mission.ss*.rpt", "*.power*.rpt"],
        "stage_cts": ["{stage}.cts.qor.final.rpt", "*.cts.qor.final.rpt"],
        "stage_app_options": ["{stage}.env.app_options.full.rpt", "*env.app_options.full.rpt"],
    },
    "PNR.INNOVUS": {
        "stage_runtime": ["{stage}.runtime.rpt"],
        "stage_setup": ["{stage}_p*.summary.gz", "*_p*.summary.gz"],
        "stage_hold": ["{stage}.qor.snap.rpt", "*.qor.snap.rpt"],
        "stage_area": ["{stage}.sec_get_area.rpt", "*.sec_get_area.rpt"],
        "stage_vth": ["{stage}.sec_vth_use.rpt", "*.sec_vth_use.rpt"],
        "stage_congestion": ["{stage}.grc.rpt", "*.grc.rpt"],
        "stage_power": ["report_power_info.mission.ss*.rpt", "*.power*.rpt"],
        "stage_cts": ["{stage}.cts.qor.final.rpt", "*.cts.qor.final.rpt"],
        "stage_app_options": ["{stage}.opt.options.rpt", "*.opt.options.rpt"],
    },
}

TABLE_PROFILES = {
    "fe_block_summary": [
        "mbit.percent", "cgc.percent", "area.instance_count", "area.std_cell_area",
        "gate_count", "vth.area_pct", "timing.r2r_setup", "timing.r2r_hold",
        "logic_depth", "power.total", "runtime.runtime",
    ],
    "qor_summary_fe": [
        "timing.r2r_setup", "timing.r2r_hold", "area.std_cell_area", "gate_count",
        "power.leakage", "power.total", "runtime.runtime",
    ],
    "qor_summary_pnr": [
        "timing.r2r_setup", "timing.setup_total", "timing.r2r_hold", "timing.hold_total",
        "congestion.total", "area.std_cell_count_area", "gate_count", "util.std_cell",
        "util.total", "vth.inst_pct", "vth.area_pct", "clock.skew_latency",
        "clock.repeater_count_area", "runtime.runtime",
    ],
    "be_stage_summary": [
        "timing.r2r_setup", "timing.setup_total", "timing.hold_total", "congestion.total",
        "area.std_cell_count_area", "gate_count", "util.std_cell", "util.total",
        "vth.inst_pct", "vth.area_pct", "clock.skew_latency",
        "clock.repeater_count_area", "runtime.runtime",
    ],
    "latest_outfeed_fe": [
        "timing.r2r_setup", "timing.r2r_hold", "area.std_cell_count_area", "gate_count",
        "congestion.total", "vth.area_pct", "logic_depth", "power.total",
        "runtime.start", "runtime.end", "runtime.runtime",
    ],
    "latest_outfeed_be": [
        "timing.r2r_setup", "timing.setup_total", "timing.hold_total", "congestion.total",
        "area.std_cell_count_area", "gate_count", "util.std_cell", "util.total",
        "vth.inst_pct", "vth.area_pct", "clock.skew_latency",
        "clock.repeater_count_area", "runtime.start", "runtime.end", "runtime.runtime",
    ],
}

METRIC_REGISTRY = {
    "timing.r2r_setup": {"label": "R2R Setup W/T/N", "scope": ["FE", "PNR"], "default": "-"},
    "timing.r2r_hold": {"label": "R2R Hold W/T/N", "scope": ["FE", "PNR"], "default": "-"},
    "timing.setup_total": {"label": "Total Setup W/T/N", "scope": ["PNR"], "default": "-"},
    "timing.hold_total": {"label": "Total Hold W/T/N", "scope": ["PNR"], "default": "-"},
    "area.instance_count": {"label": "Instance Count", "scope": ["FE"], "default": "-"},
    "area.std_cell_area": {"label": "Std Cell Area", "scope": ["FE", "PNR"], "default": "-"},
    "area.std_cell_count": {"label": "Std Cell Count", "scope": ["PNR"], "default": "-"},
    "area.std_cell_count_area": {"label": "Std Cell Count/Area", "scope": ["PNR"], "default": "-"},
    "gate_count": {"label": "Gate Count", "scope": ["FE", "PNR"], "default": "-"},
    "util.std_cell": {"label": "Std Cell/Std Only Util", "scope": ["PNR"], "default": "-"},
    "util.total": {"label": "Total Util", "scope": ["PNR"], "default": "-"},
    "vth.inst_pct": {"label": "VT Inst%", "scope": ["PNR"], "default": "-"},
    "vth.area_pct": {"label": "VT Area%", "scope": ["FE", "PNR"], "default": "-"},
    "mbit.percent": {"label": "MBIT%", "scope": ["FE"], "default": "-"},
    "cgc.percent": {"label": "CG%", "scope": ["FE"], "default": "-"},
    "congestion.total": {"label": "Congestion", "scope": ["FE", "PNR"], "default": "-"},
    "logic_depth": {"label": "Logic Depth", "scope": ["FE", "PNR"], "default": "-"},
    "power.cell_internal": {"label": "Cell Internal Power", "scope": ["FE", "PNR"], "default": "-"},
    "power.net_switching": {"label": "Net Switching Power", "scope": ["FE", "PNR"], "default": "-"},
    "power.total_dynamic": {"label": "Total Dynamic Power", "scope": ["FE", "PNR"], "default": "-"},
    "power.leakage": {"label": "Cell Leakage Power", "scope": ["FE", "PNR"], "default": "-"},
    "power.total": {"label": "Total Power", "scope": ["FE", "PNR"], "default": "-"},
    "clock.skew_latency": {"label": "Skew/Latency", "scope": ["PNR"], "default": "-"},
    "clock.repeater_count_area": {"label": "Clock Repeater Count/Area", "scope": ["PNR"], "default": "-"},
    "runtime.start": {"label": "Start", "scope": ["FE", "PNR"], "default": "-"},
    "runtime.end": {"label": "End", "scope": ["FE", "PNR"], "default": "-"},
    "runtime.runtime": {"label": "Runtime", "scope": ["FE", "PNR"], "default": "-"},
}

METRIC_ALIASES = {
    "r2r_setup": "timing.r2r_setup",
    "setup_r2r": "timing.r2r_setup",
    "r2r_hold": "timing.r2r_hold",
    "hold_r2r": "timing.r2r_hold",
    "setup_total": "timing.setup_total",
    "hold_total": "timing.hold_total",
    "instance_count": "area.instance_count",
    "std_cell_area": "area.std_cell_area",
    "std_area": "area.std_cell_area",
    "std_cell_count": "area.std_cell_count",
    "std_count_area": "area.std_cell_count_area",
    "gate_count": "gate_count",
    "gc": "gate_count",
    "std_cell_util": "util.std_cell",
    "std_util": "util.std_cell",
    "total_util": "util.total",
    "vth_inst": "vth.inst_pct",
    "vth_area": "vth.area_pct",
    "vt_inst": "vth.inst_pct",
    "vt_area": "vth.area_pct",
    "mbit": "mbit.percent",
    "cgc": "cgc.percent",
    "congestion": "congestion.total",
    "cong": "congestion.total",
    "logic_depth": "logic_depth",
    "leakage": "power.leakage",
    "total_power": "power.total",
    "power_total": "power.total",
    "skew_latency": "clock.skew_latency",
    "clock_repeater_count_area": "clock.repeater_count_area",
    "runtime": "runtime.runtime",
    "start": "runtime.start",
    "end": "runtime.end",
}

_MISSING = (None, "")


def canonical_metric_key(key):
    if key in METRIC_REGISTRY:
        return key
    return METRIC_ALIASES.get(key)


def metric_label(key):
    ckey = canonical_metric_key(key) or key
    return METRIC_REGISTRY.get(ckey, {}).get("label", key)


def _clean(value):
    if value in _MISSING:
        return None
    return value


def _join_triplet(value):
    if isinstance(value, (list, tuple)):
        return "/".join(str(v) for v in value)
    return value


def _get(metrics, *path):
    cur = metrics
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return _clean(cur)


def _legacy_value(metrics, ckey):
    if not isinstance(metrics, dict):
        return None
    if ckey == "timing.r2r_setup":
        return _join_triplet(_get(metrics, "setup_r2r") or _get(metrics, "r2r_setup"))
    if ckey == "timing.r2r_hold":
        return _join_triplet(_get(metrics, "hold_r2r") or _get(metrics, "r2r_hold"))
    if ckey == "timing.setup_total":
        return _join_triplet(_get(metrics, "setup_total"))
    if ckey == "timing.hold_total":
        return _join_triplet(_get(metrics, "hold_total"))
    if ckey == "area.instance_count":
        return _get(metrics, "area", "instance_count")
    if ckey == "area.std_cell_area":
        return _get(metrics, "area", "std_cell_area") or _get(metrics, "area", "std_area")
    if ckey == "area.std_cell_count":
        return _get(metrics, "area", "std_cell_count")
    if ckey == "area.std_cell_count_area":
        direct = _get(metrics, "std_cell_count_area")
        if direct is not None:
            return direct
        count = _get(metrics, "area", "std_cell_count")
        area = _get(metrics, "area", "std_cell_area") or _get(metrics, "area", "std_area")
        if count is not None and area is not None:
            return "{}/{}".format(count, area)
        return None
    if ckey == "gate_count":
        return _get(metrics, "gate_count")
    if ckey == "util.std_cell":
        return _get(metrics, "util", "std_cell_only") or _get(metrics, "util", "std_cell")
    if ckey == "util.total":
        return _get(metrics, "util", "total")
    if ckey == "vth.inst_pct":
        return _get(metrics, "vth", "inst_pct") or _get(metrics, "vth_inst")
    if ckey == "vth.area_pct":
        return _get(metrics, "vth", "area_pct") or _get(metrics, "vth_area")
    if ckey == "mbit.percent":
        return _get(metrics, "mbit", "percent") or _get(metrics, "mbit")
    if ckey == "cgc.percent":
        return _get(metrics, "cgc", "percent") or _get(metrics, "cgc")
    if ckey == "congestion.total":
        return _get(metrics, "congestion", "total") or _get(metrics, "congestion")
    if ckey == "logic_depth":
        return _get(metrics, "logic_depth")
    if ckey == "power.cell_internal":
        return _get(metrics, "power", "cell_internal")
    if ckey == "power.net_switching":
        return _get(metrics, "power", "net_switching")
    if ckey == "power.total_dynamic":
        return _get(metrics, "power", "total_dynamic")
    if ckey == "power.leakage":
        return _get(metrics, "power", "leakage")
    if ckey == "power.total":
        return _get(metrics, "power", "total_power") or _get(metrics, "power", "total")
    if ckey == "clock.skew_latency":
        return _get(metrics, "cts", "skew_latency") or _get(metrics, "skew_latency")
    if ckey == "clock.repeater_count_area":
        return _get(metrics, "cts", "clock_repeater_count_area") or _get(metrics, "clock_repeater_count_area")
    if ckey == "runtime.start":
        return _get(metrics, "runtime", "start") or _get(metrics, "start")
    if ckey == "runtime.end":
        return _get(metrics, "runtime", "end") or _get(metrics, "end")
    if ckey == "runtime.runtime":
        return _get(metrics, "runtime", "runtime") or _get(metrics, "runtime")
    return None


def apply_flat_metrics(metrics, scope=None, tool=None):
    if not isinstance(metrics, dict):
        return metrics
    flat = metrics.setdefault("_flat", {})
    for key in METRIC_REGISTRY:
        val = _legacy_value(metrics, key)
        if val not in _MISSING:
            flat[key] = val
    return metrics


def get_metric_value(metrics, key, default="-"):
    if not isinstance(metrics, dict):
        return default
    ckey = canonical_metric_key(key)
    if ckey:
        flat = metrics.get("_flat")
        if isinstance(flat, dict) and ckey in flat:
            val = flat.get(ckey)
            return default if val in _MISSING else val
        val = _legacy_value(metrics, ckey)
        return default if val in _MISSING else val
    val = metrics.get(key, default)
    return default if val in _MISSING else val


def get_metric_profile(name, config_obj=None):
    raw = None
    if config_obj is not None:
        try:
            if config_obj.has_section("METRIC_TABLES") and config_obj.has_option("METRIC_TABLES", name):
                raw = config_obj.get("METRIC_TABLES", name)
        except Exception as exc:
            debug_log("metric_registry: profile_read_failed {}".format(name), exc)
    if raw:
        keys = [x.strip() for x in raw.split(",") if x.strip()]
    else:
        keys = list(TABLE_PROFILES.get(name, []))
    for key in keys:
        if not canonical_metric_key(key):
            debug_log("metric_registry: unknown metric key {} in profile {}".format(key, name))
    return keys
