# metric_extract.py
# Self-contained report parsers -- same logic as summary.py ecosystem.
# No external deps: pure Python re + glob + os.
# All regex from metric.py / prc.py / qor.py in the summary scripts.

import os
import re
import glob


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _find_rpt(rpt_dir, prefix, ext=".rpt"):
    """Find latest report matching prefix.*.rpt (handles BLK+timestamp suffix)."""
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None
    hits = (glob.glob(os.path.join(rpt_dir, prefix + ".*" + ext)) +
            glob.glob(os.path.join(rpt_dir, prefix + ext)))
    return sorted(hits, key=os.path.getmtime)[-1] if hits else None


def _read(path):
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# AREA / UTILIZATION / VTH  (cell_usage.summary.{top}.*.rpt)
# ---------------------------------------------------------------------------

_A = {
    "total_area":       re.compile(r"^Total cell area:\s+(\S+)", re.M),
    "combinational":    re.compile(r"^Combinational area:\s+(\S+)", re.M),
    "reg_area":         re.compile(r"^Noncombinational area:\s+(\S+)", re.M),
    "macro_area":       re.compile(r"^Macro/Black Box area:\s+(\S+)", re.M),
    "buf_area":         re.compile(r"^Buf/Inv area:\s+(\S+)", re.M),
    "total_count":      re.compile(r"^Number of cells:\s+(\S+)", re.M),
    "reg_count":        re.compile(r"^Number of sequential cells:\s+(\S+)", re.M),
    "macro_count":      re.compile(r"^Number of macros/black boxes:\s+(\S+)", re.M),
    "buf_count":        re.compile(r"^Number of buf/inv:\s+(\S+)", re.M),
    "total_util":       re.compile(r"^\s*Total utilization\s*:\s*(\S+)", re.M),
    "std_cell_util":    re.compile(r"^\s*Standard cell only utilization\s*:\s*(\S+)", re.M),
    "memory_util":      re.compile(r"^\s*Memory utilization\s*:\s*(\S+)", re.M),
    "core_area":        re.compile(r"^\s*core_area\s*:\s*(\S+)", re.M),
    "std_cell_area":    re.compile(r"^\s*std_cell_area\s*:\s*(\S+)", re.M),
    "memory_area":      re.compile(r"^\s*Memory\s+area\s*:\s*(\S+)", re.M),
    "mbit":             re.compile(r"^Flip-flop cells banking ratio\s+(\S+)", re.M),
}

_VTH_HDR  = re.compile(r"Cell Usage by Vth", re.I)
_VTH_ROW  = re.compile(
    r"^\s*(\S+)\s+\|\s+([\d.]+)%\s+\|\s+(\d+)\s+\|\s+\S+\s+([\d.]+)%\s+(\d+)", re.M)


def parse_cell_usage(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}
    for key, pat in _A.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()

    # VTH distribution
    if _VTH_HDR.search(txt):
        vth = {}
        for row in _VTH_ROW.finditer(txt):
            vth_name = row.group(1).split("_")[0]  # LVT / HVT / RVT
            vth[vth_name] = {
                "inst_pct": row.group(2),
                "inst_cnt": row.group(3),
                "area_pct": row.group(4),
                "area_cnt": row.group(5),
            }
        if vth:
            m["vth"] = vth
    return m


# ---------------------------------------------------------------------------
# CGC  (check_timing.{top}.*.rpt  or  qor.{top}.*.rpt)
# ---------------------------------------------------------------------------

_CGC_RE     = re.compile(
    r"Number of Gated registers\s+\|\s*(\d+)\s+\((\S+)\)", re.M)
_TOOLCGC_RE = re.compile(
    r"Number of Tool-Inserted Gated registers\s+\|\s*(\d+)\s+\((\S+)\)", re.M)


def parse_cgc(path):
    txt = _read(path)
    m = {}
    hit = _CGC_RE.search(txt)
    if hit:
        m["cgc_count"] = hit.group(1)
        m["cgc_ratio"] = hit.group(2)
    hit = _TOOLCGC_RE.search(txt)
    if hit:
        m["tool_cgc_count"] = hit.group(1)
        m["tool_cgc_ratio"] = hit.group(2)
    return m


# ---------------------------------------------------------------------------
# CONGESTION  (clock_gating_info.mission.rpt or congestion.{top}.*.rpt)
# ---------------------------------------------------------------------------

_CONG_BOTH = re.compile(
    r"Both\s+Dirs\s*\|?\s*[\d.]+\s*\|\s*[\d.]+\s*\|\s*([\d.]+)\s*%", re.M)
_CONG_H    = re.compile(
    r"^H\s+routing\s*\|?\s*[\d.]+\s*\|\s*[\d.]+\s*\|\s*([\d.]+)\s*%", re.M)
_CONG_V    = re.compile(
    r"^V\s+routing\s*\|?\s*[\d.]+\s*\|\s*[\d.]+\s*\|\s*([\d.]+)\s*%", re.M)


def parse_congestion(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}
    hit = _CONG_BOTH.search(txt)
    if hit: m["cong_both"] = hit.group(1) + "%"
    hit = _CONG_H.search(txt)
    if hit: m["cong_h"]    = hit.group(1) + "%"
    hit = _CONG_V.search(txt)
    if hit: m["cong_v"]    = hit.group(1) + "%"
    return m


# ---------------------------------------------------------------------------
# QOR TIMING  (qor.{top}.*.rpt  or  {stage}.qor.rpt)
# Parses per-scenario WNS/TNS/NVP for setup and hold
# Logic mirrors qor.py in the summary ecosystem
# ---------------------------------------------------------------------------

_SCN_RE    = re.compile(r"^Scenario\s*['\s:]+(\S+?)[']\s*$", re.M)
_GRP_RE    = re.compile(r"^Timing Path Group\s*['\s:]+(\S+?)[']\s*$", re.M)
_WNS_S_RE  = re.compile(r"Worst\s+Negative\s+Slack\s*:\s*([-\d.]+)", re.M)
_WNS_H_RE  = re.compile(r"Worst\s+Hold\s+Violation\s*:\s*([-\d.]+)", re.M)
_TNS_RE    = re.compile(r"Total\s+Negative\s+Slack\s*:\s*([-\d.]+)", re.M)
_NVP_RE    = re.compile(r"Number\s+of\s+Violating\s+(?:Paths|Points)\s*:\s*(\d+)", re.M)
_SETUP_SEC = re.compile(r"^Setup violations\s*$", re.M)
_HOLD_SEC  = re.compile(r"^Hold violations\s*$", re.M)
_NO_VIO    = re.compile(r"No setup violations found", re.M)
_TOOLV_RE  = re.compile(r"^Version:\s*(\S+)", re.M)

# Global timing (report_global_timing section)
_GSETUP_RE = re.compile(r"Setup violations\s*$.*?REG2REG.*?WNS\s*:\s*([-\d.]+)", re.S)
_GHOLD_RE  = re.compile(r"Hold violations\s*$.*?REG2REG.*?WNS\s*:\s*([-\d.]+)", re.S)


def parse_qor_rpt(path):
    """Parse qor report. Returns dict with:
      tool_version, timing{scenario: {grp/type: {wns,tns,nvp}}},
      r2r_setup, r2r_hold, cgc_ratio, mbit"""
    txt = _read(path)
    if not txt:
        return {}
    m = {}

    hit = _TOOLV_RE.search(txt)
    if hit: m["tool_version"] = hit.group(1)

    # CGC / MBIT from qor.rpt
    cgc = parse_cgc(path)
    m.update(cgc)
    mbit_hit = re.search(r"Flip-flop cells banking ratio\s+([\d.]+)", txt, re.M)
    if mbit_hit: m["mbit"] = mbit_hit.group(1)

    # Parse timing blocks
    # Strategy: split by "Scenario" lines, parse each block
    scenarios = {}
    lines = txt.splitlines()
    n = len(lines)
    i = 0
    cur_scn  = "default"
    cur_grp  = "REG2REG"
    cur_type = "setup"   # setup | hold

    while i < n:
        line = lines[i].rstrip()

        # Scenario header
        sm = re.match(r"^Scenario\s+'([^']+)'", line)
        if sm:
            cur_scn = sm.group(1)
            if cur_scn not in scenarios:
                scenarios[cur_scn] = {}
            i += 1; continue

        # Section type
        if re.match(r"^Setup violations\s*$", line):
            cur_type = "setup"; i += 1; continue
        if re.match(r"^Hold violations\s*$", line):
            cur_type = "hold"; i += 1; continue

        # No violation
        if re.match(r"No setup violations found", line):
            key = f"{cur_grp}/{cur_type}"
            scenarios.setdefault(cur_scn, {})[key] = {
                "wns": "0.000", "tns": "0.000", "nvp": "0"}
            i += 1; continue

        # Timing Path Group
        gm = re.match(r"^Timing Path Group\s+'([^']+)'", line)
        if gm:
            cur_grp = gm.group(1)
            # Scan ahead up to 12 lines for WNS/TNS/NVP
            wns = tns = nvp = "-"
            for j in range(i + 1, min(i + 12, n)):
                l2 = lines[j]
                if re.search(r"Worst Negative Slack", l2):
                    hit2 = re.search(r"([-\d.]+)\s*$", l2)
                    if hit2: wns = hit2.group(1)
                if re.search(r"Worst Hold Violation", l2):
                    hit2 = re.search(r"([-\d.]+)\s*$", l2)
                    if hit2: wns = hit2.group(1)
                if re.search(r"Total Negative Slack", l2):
                    hit2 = re.search(r"([-\d.]+)\s*$", l2)
                    if hit2: tns = hit2.group(1)
                if re.search(r"Number of Violating", l2):
                    hit2 = re.search(r"(\d+)\s*$", l2)
                    if hit2: nvp = hit2.group(1)
            key = f"{cur_grp}/{cur_type}"
            scenarios.setdefault(cur_scn, {})[key] = {
                "wns": wns, "tns": tns, "nvp": nvp}
            i += 1; continue

        i += 1

    if scenarios:
        m["timing"] = scenarios
        # Extract summary R2R setup/hold (worst across all scenarios)
        all_wns_s = []
        all_wns_h = []
        for scn_data in scenarios.values():
            for key, td in scn_data.items():
                try:
                    v = float(td["wns"])
                    if "setup" in key: all_wns_s.append(v)
                    if "hold"  in key: all_wns_h.append(v)
                except Exception:
                    pass
        if all_wns_s: m["r2r_wns_setup"] = f"{min(all_wns_s):.4f}"
        if all_wns_h: m["r2r_wns_hold"]  = f"{min(all_wns_h):.4f}"

        # TNS summary
        all_tns = []
        for scn_data in scenarios.values():
            for key, td in scn_data.items():
                if "setup" in key:
                    try: all_tns.append(float(td["tns"]))
                    except Exception: pass
        if all_tns: m["r2r_tns_setup"] = f"{sum(all_tns):.4f}"

    return m


# ---------------------------------------------------------------------------
# DRC  (compile_opt.log  or  {stage}.physical_all_sum)
# ---------------------------------------------------------------------------

def parse_drc_from_log(log_path):
    txt = _read(log_path)
    if not txt: return "-"
    count = sum(1 for l in txt.splitlines() if l.startswith("Error:"))
    return str(count)


def parse_pnr_drc(rpt_dir, stage):
    """Parse physical DRC from stage reports."""
    m = {}
    # Try multiple file patterns
    for prefix in (f"{stage}.physical_all_sum", f"{stage}.drc",
                   "physical_all_sum", "drc"):
        path = _find_rpt(rpt_dir, prefix) or _find_rpt(rpt_dir, prefix, "")
        txt  = _read(path)
        if not txt: continue
        hit = re.search(r"Total number of DRCs\s*=\s*(\d+)", txt)
        if hit: m["drc_count"] = hit.group(1)
        hit = re.search(r"Short\s*[s]?\s*=\s*(\d+)", txt, re.I)
        if hit: m["shorts"] = hit.group(1)
        if m: break
    return m


# ---------------------------------------------------------------------------
# POWER  (report_power_info.ff.rpt  /  report_power_info.ss.rpt)
# ---------------------------------------------------------------------------

_DPOWER_RE = re.compile(
    r"Total Dynamic Power\s*=\s*([\d.]+)\s*\((\w+)\)", re.M)
_LPOWER_RE = re.compile(
    r"Cell Leakage Power\s*=\s*([\d.]+)\s*\((\w+)\)", re.M)


def _to_mw(val, unit):
    try:
        v = float(val)
        return v * {"uW": 1e-3, "nW": 1e-6, "mW": 1.0}.get(unit, 1.0)
    except Exception:
        return None


def parse_power(rpt_dir):
    m = {}
    for fname, dkey, lkey in [
            ("report_power_info.ff", "dynamic_mw", None),
            ("report_power_info.ss", None, "leakage_mw")]:
        path = _find_rpt(rpt_dir, fname)
        txt  = _read(path)
        if not txt: continue
        if dkey:
            hit = _DPOWER_RE.search(txt)
            if hit:
                v = _to_mw(hit.group(1), hit.group(2))
                if v is not None: m[dkey] = f"{v:.3f} mW"
        if lkey:
            hit = _LPOWER_RE.search(txt)
            if hit:
                v = _to_mw(hit.group(1), hit.group(2))
                if v is not None: m[lkey] = f"{v:.3f} mW"
    return m


# ---------------------------------------------------------------------------
# SAIF ANNOTATION  (report_power_info.ff.rpt)
# ---------------------------------------------------------------------------

def parse_saif(rpt_dir):
    for fname in ("report_power_info.ff", "report_power_info"):
        path = _find_rpt(rpt_dir, fname)
        txt  = _read(path)
        if not txt: continue
        hit = re.search(r"seq\s*:\s*([\d.]+)%", txt, re.I)
        if hit: return {"saif_seq": hit.group(1) + "%"}
        hit = re.search(r"port\s*:\s*([\d.]+)%", txt, re.I)
        if hit: return {"saif_port": hit.group(1) + "%"}
    return {}


# ---------------------------------------------------------------------------
# RUNTIME  (already in workers.py -- just re-exported here for completeness)
# ---------------------------------------------------------------------------

def parse_runtime(rpt_dir):
    """Extract total runtime string from runtime.V2.rpt."""
    path = os.path.join(rpt_dir, "..", "reports", "runtime.V2.rpt")
    if not os.path.exists(path):
        path = os.path.join(rpt_dir, "reports", "runtime.V2.rpt")
    txt = _read(path)
    if not txt: return "-"
    hit = re.search(
        r"TimeStamp\s*:\s*TOTAL\b.*?Total\s*:\s*(\d+h:\d+m:\d+s)", txt, re.S)
    return hit.group(1) if hit else "-"


# ---------------------------------------------------------------------------
# MAIN: extract_fe_metrics(run_dir, block_name)
# ---------------------------------------------------------------------------

def extract_fe_metrics(run_dir, block_name):
    """Extract all FE/synth metrics for a completed run.
    run_dir  = absolute path to the *-FE directory
    Returns flat dict of all metrics."""
    rpt_dir = os.path.join(run_dir, "reports")
    result  = {}

    # -- Area / Util / VTH  (cell_usage.summary.BLK.*.rpt) --
    cu = _find_rpt(rpt_dir, "cell_usage.summary")
    if cu:
        result.update(parse_cell_usage(cu))

    # -- Timing QoR  (qor.BLK.*.rpt) --
    qr = _find_rpt(rpt_dir, "qor")
    if qr:
        result.update(parse_qor_rpt(qr))

    # -- CGC ratio  (check_timing.BLK.*.rpt) --
    ct = _find_rpt(rpt_dir, "check_timing")
    if ct:
        result.update(parse_cgc(ct))

    # -- Congestion  (clock_gating_info.mission.rpt) --
    cg = os.path.join(rpt_dir, "clock_gating_info.mission.rpt")
    if not os.path.exists(cg):
        cg = _find_rpt(rpt_dir, "congestion")
    cong = parse_congestion(cg) if cg else {}
    result.update(cong)

    # -- DRC from log --
    log = os.path.join(run_dir, "logs", "compile_opt.log")
    result["drc_errors"] = parse_drc_from_log(log)

    # -- Power --
    result.update(parse_power(rpt_dir))

    # -- SAIF --
    result.update(parse_saif(rpt_dir))

    return result


# ---------------------------------------------------------------------------
# MAIN: extract_pnr_stage_metrics(run_dir, stage_name, source)
# ---------------------------------------------------------------------------

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    """Extract metrics for one PNR stage.
    run_dir    = BE run directory
    stage_name = e.g. place_opt, route_opt, clock_opt_psyn
    source     = WS or OUTFEED"""
    result = {}

    # Report dir path differs by source
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)

    # -- Timing QoR  ({stage}.qor.rpt) --
    qr = _find_rpt(rpt_dir, f"{stage_name}.qor")
    if not qr:
        qr = _find_rpt(rpt_dir, "qor")
    if qr:
        result.update(parse_qor_rpt(qr))

    # -- Global timing summary  ({stage}.qor_sum.rpt) --
    qsum = _find_rpt(rpt_dir, f"{stage_name}.qor_sum")
    if qsum:
        qs = parse_qor_rpt(qsum)
        # prefix keys so they don't overwrite per-scenario data
        for k, v in qs.items():
            result[f"global_{k}"] = v

    # -- DRC physical --
    result.update(parse_pnr_drc(rpt_dir, stage_name))

    # -- Area  ({stage}.qor.rpt also has area in PNR) --
    # qor_area from cell utilization in pnr qor
    cu = _find_rpt(rpt_dir, f"{stage_name}.physical_all_sum")
    if cu:
        txt = _read(cu)
        hit = re.search(r"Total\s+cell\s+area\s*:\s*([\d.]+)", txt)
        if hit: result["total_area"] = hit.group(1)
        hit = re.search(r"Standard\s+cell\s+only.*?:\s*([\d.]+)", txt, re.S)
        if hit: result["std_cell_area"] = hit.group(1)

    return result
