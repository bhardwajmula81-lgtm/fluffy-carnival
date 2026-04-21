# -*- coding: ascii -*-
# metric_extract.py
# Exact file paths and parsers from file.py and prc.py (summary scripts).
# All file names verified from file.py SynFileDb lines 100-116.
# All parse functions verified from prc.py.

import os
import re
import glob


# ---------------------------------------------------------------------------
# self.top computation (from DirDb.__init__ lines 56-57):
#   WS:      top = path.split('/')[-3]   e.g. BLK_ISP2
#   OUTFEED: top = path.split('/')[-4]   e.g. BLK_ISP2
# rpt_dir = run_dir/reports
# ---------------------------------------------------------------------------

def _get_top(run_dir, source="WS"):
    parts = run_dir.rstrip('/').split('/')
    if source == "OUTFEED":
        return parts[-4] if len(parts) >= 4 else "UNKNOWN"
    else:
        return parts[-3] if len(parts) >= 3 else "UNKNOWN"


def _rpt_dir(run_dir):
    return os.path.join(run_dir, "reports")


# SynFileDb.gen_path logic (lines 176-188 file.py):
# if rpt_dir is None: rpt = os.path.join(self.rpt_dir, str)
# else: rpt = os.path.join(rpt_dir, str)
# try: return glob(rpt)[0]  (all=False returns first match)
def _find(rpt_dir, pattern):
    """glob for pattern in rpt_dir. Returns latest match or None."""
    hits = glob.glob(os.path.join(rpt_dir, pattern))
    return sorted(hits, key=os.path.getmtime)[-1] if hits else None


def _read(path):
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.readlines()
    except Exception:
        return []


def _reads(path):
    return ''.join(_read(path))


# ---------------------------------------------------------------------------
# EXACT FILE PATHS from SynFileDb (file.py lines 100-116)
# gen_path(f"prefix.{top}.*.rpt") -> glob in rpt_dir
# ---------------------------------------------------------------------------

def syn_files(run_dir, source="WS"):
    """Returns dict of all report file paths for a synth run.
    Matches exactly SynFileDb.self.files in file.py."""
    top = _get_top(run_dir, source)
    rd  = _rpt_dir(run_dir)
    return {
        # area metrics (NOT cell_usage -- separate file!)
        "area":        _find(rd, f"area.{top}.*.rpt"),
        # CGC ratio -- FIXED name, no timestamp glob
        "cgc":         _find(rd, "clock_gating_info.mission.rpt")
                       or os.path.join(rd, "clock_gating_info.mission.rpt"),
        # congestion overflow
        "congestion":  _find(rd, f"congestion.{top}.*.rpt"),
        # MBIT flip-flop banking -- SEPARATE file
        "mbit":        _find(rd, f"multibit_banking_ratio.{top}.*.rpt"),
        # timing WNS/TNS/NVP
        "qor":         _find(rd, f"qor.{top}.*.rpt"),
        # utilization -- SEPARATE file (NOT cell_usage)
        "utilization": _find(rd, f"utilization.{top}.*.rpt"),
        # VTH distribution -- cell_usage.summary IS the VTH file
        "vth":         _find(rd, f"cell_usage.summary.{top}.*.rpt"),
        # check_timing for CGC
        "chk_timing":  _find(rd, f"check_timing.{top}.*.rpt"),
        # power -- ff=leakage, ss=dynamic
        "leakage":     _find(rd, "report_power_info.mission.ff*.rpt"),
        "dynamic":     _find(rd, "report_power_info.mission.ss*.rpt"),
        # runtime
        "runtime":     os.path.join(rd, "runtime.V2.rpt"),
        # saif annotation
        "saif":        os.path.join(rd, "report_activity_summary.rpt"),
    }


# ---------------------------------------------------------------------------
# EXACT FILE PATHS from PnrFileDb (file.py lines 212-224)
# WS:      rpt_dir = run_dir/reports/stage/
# OUTFEED: rpt_dir = run_dir/stage/reports/stage/
# ---------------------------------------------------------------------------

def pnr_files(run_dir, stage, source="WS"):
    """Returns dict of all report file paths for a PNR stage.
    Matches exactly PnrFileDb.self.files in file.py."""
    if source == "WS":
        rd = os.path.join(run_dir, "reports", stage)
    else:
        rd = os.path.join(run_dir, stage, "reports", stage)
    return {
        "qor":         os.path.join(rd, f"{stage}.qor.rpt"),
        "qor_global":  os.path.join(rd, f"{stage}.qor_sum.rpt"),
        "congestion":  os.path.join(rd, f"{stage}.grc.rpt"),
        "utilization": os.path.join(rd, f"{stage}.sec_get_areas.rpt"),
        "vth":         os.path.join(rd, f"{stage}.sec_vth_use.rpt"),
        "drc":         os.path.join(rd, f"{stage}.physical_all.sum"),
        "runtime":     os.path.join(rd, f"{stage}.runtime.rpt"),
        "baseline":    os.path.join(rd, f"{stage}.env_is_label.rpt"),
        "cts":         os.path.join(rd, "clock_opt_cts.cts.skew.qor.final"),
        # log from DirDb lines 88-93 (WS: run_dir/stage/logs, OUTFEED: run_dir/logs)
        "log":         os.path.join(run_dir, stage, "logs", f"{stage}.log")
                       if source == "WS"
                       else os.path.join(run_dir, "logs", f"{stage}.log"),
    }


# ---------------------------------------------------------------------------
# prc.py get_drc() -- exact copy
# Reads reversed lines, finds DRC-SUMMARY section
# ---------------------------------------------------------------------------

def get_drc(file_path):
    """Exact copy of prc.py Procedure.get_drc()"""
    short_txt = "-"
    drc_txt   = "-"
    lines = _read(file_path)
    if not lines:
        return f"{drc_txt}/ {short_txt}"
    for l in reversed(lines):
        shorts = re.search(r"^\s+Short\s+:\s+(\d+)", l)
        drc    = re.search(r"^Total number of DRCs\s*=\s*(\d+)", l)
        if shorts:
            short_txt = shorts.group(1)
            continue
        if drc:
            drc_txt = drc.group(1)
            continue
        if re.search(r"^DRC-SUMMARY", l):
            break
    return f"{drc_txt}/ {short_txt}"


# ---------------------------------------------------------------------------
# prc.py get_cgc_ratio() -- exact copy
# Reads clock_gating_info.mission.rpt (FIXED filename)
# Returns e.g. "99.97%/ 0.03%" or just "99.97%"
# ---------------------------------------------------------------------------

def get_cgc_ratio(file_path):
    """Exact copy of prc.py Procedure.get_cgc_ratio()"""
    lines = _read(file_path)
    if not lines:
        return "NA"
    txt  = "NA"
    data = lines[-100:]
    for l in data:
        cgc = re.search(
            r"Number of Gated registers\s+\|\s+\d+\s+\|\s+\d+\s+\|\s+\((\S+)\)",
            l)
        tool_cgc = re.search(
            r"Number of Tool-Inserted Gated registers\s+\|\s+\d+\s+\|\s+\d+\s+\|\s+\((\S+)\)",
            l)
        if cgc:
            txt = cgc.group(1)
        if tool_cgc:
            txt = f"{txt}/ {tool_cgc.group(1)}"
            break
    return txt


# ---------------------------------------------------------------------------
# prc.py get_mbit_ratio() -- exact copy
# Reads multibit_banking_ratio.{top}.*.rpt
# ---------------------------------------------------------------------------

def get_mbit_ratio(file_path):
    """Exact copy of prc.py Procedure.get_mbit_ratio()"""
    lines = _read(file_path)
    for line in lines:
        m = re.search(
            r"^Flip-flop cells banking ratio\s+\S+\s+:\s+(\S+)", line)
        if m:
            return m.group(1)
    return "-"


# ---------------------------------------------------------------------------
# prc.py get_runtime() -- exact copy
# Reads runtime.V2.rpt, returns total runtime string
# ---------------------------------------------------------------------------

def get_runtime(file_path):
    """Exact copy of prc.py Procedure.get_runtime()"""
    lines = _read(file_path)
    for line in reversed(lines):
        if re.search(r"TimeStamp : TOTAL", line):
            return line.split()[-2]
        if re.search(r"total (?:place_opt|route_opt|clock_opt_psyn)", line):
            return line.split()[-2]
    return "-"


# ---------------------------------------------------------------------------
# prc.py get_vth_group() -- exact copy
# Reads cell_usage.summary.{top}.*.rpt
# Returns dict: {"LVT": "31.91%", "RVT": "68.09%", ...}
# ---------------------------------------------------------------------------

def get_vth_group(file_path):
    """Exact copy of prc.py Procedure.get_vth_group()"""
    lut = {}
    lines = _read(file_path)
    if not lines:
        return lut
    content = lines
    for i, line in enumerate(content):
        if re.match(r"^\s*$", line):
            continue
        if re.search(r"^Cell Count Report", content[i]):
            i += 5
            while i < len(content) and not re.search(r"^Total", content[i]):
                m = re.search(
                    r"^(\S+)\s+\d+\s+\((\S+)\)", content[i])
                if m:
                    lut[m.group(1)] = m.group(2)
                i += 1
            if re.search(r"^Total", content[i] if i < len(content) else ""):
                break
    return lut


# ---------------------------------------------------------------------------
# prc.py sort_vth_groups() -- groups LVT_G, LVT_LL etc into LVT, RVT, HVT
# Returns dict: {"LVT_total": "31.91%", "RVT_total": "68.09%", ...}
# ---------------------------------------------------------------------------

def sort_vth_groups(vth_lut):
    """Exact copy of prc.py Procedure.sort_vth_groups()"""
    total = {}
    for vth_type, vth_val in vth_lut.items():
        vth_name = vth_type.split('_')[0]  # LVT_G -> LVT, RVT_LL -> RVT
        try:
            vth_val_f = float(str(vth_val).rstrip('%'))
        except Exception:
            continue
        if vth_name in total:
            total[vth_name] += vth_val_f
        else:
            total[vth_name] = vth_val_f
    # Convert to 2 decimal places with %
    return {f"{k}_total": f"{v:.2f}%" for k, v in total.items()}


# ---------------------------------------------------------------------------
# Area metrics from area.{top}.*.rpt
# Uses metric.py patterns (area section)
# ---------------------------------------------------------------------------

def parse_area(file_path):
    """Parse area.{top}.*.rpt -- metric.py area patterns."""
    txt = _reads(file_path)
    if not txt:
        return {}
    m = {}
    pats = {
        "combinational_area": re.compile(
            r"^Combinational area:\s+(\S+)", re.M),
        "reg_area":           re.compile(
            r"^Noncombinational area:\s+(\S+)", re.M),
        "macro_area":         re.compile(
            r"^Macro/Black Box area:\s+(\S+)", re.M),
        "buf_area":           re.compile(
            r"^\*?Buf/Inv area:\s+(\S+)", re.M),
        "total_area":         re.compile(
            r"^Total cell area:\s+(\S+)", re.M),
        "total_count":        re.compile(
            r"^Number of cells:\s+(\S+)", re.M),
        "reg_count":          re.compile(
            r"^Number of sequential cells:\s+(\S+)", re.M),
        "macro_count":        re.compile(
            r"^Number of macros/black boxes:\s+(\S+)", re.M),
        "buf_count":          re.compile(
            r"^Number of buf/inv:\s+(\S+)", re.M),
    }
    for key, pat in pats.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()
    return m


# ---------------------------------------------------------------------------
# Utilization from utilization.{top}.*.rpt
# Uses metric.py utilization patterns
# ---------------------------------------------------------------------------

def parse_utilization(file_path):
    """Parse utilization.{top}.*.rpt -- metric.py utilization patterns."""
    txt = _reads(file_path)
    if not txt:
        return {}
    m = {}
    pats = {
        "total_util":         re.compile(
            r"^\s*Total utilization\s*:\s*(\S+)", re.M),
        "memory_util":        re.compile(
            r"^\s*Memory utilization\s*:\s*(\S+)", re.M),
        "std_cell_only_util": re.compile(
            r"^\s*Standard cell only utilization\s*:\s*(\S+)", re.M),
        "std_cell_util":      re.compile(
            r"^\s*Stdcell utilization\s*:\s*(\S+)", re.M),
        "core_area":          re.compile(
            r"^\s*core_area\s*:\s*(\S+)", re.M),
    }
    for key, pat in pats.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()
    # Memory area from utilization table 3rd column
    mem = re.search(
        r"^\s*Memory utilization\s*:\s*\S+\s+\S+\s+(\S+)", txt, re.M)
    if mem:
        m["memory_area"] = mem.group(1)
    # Std cell area from utilization
    sc = re.search(
        r"^\s*Std\s*[Cc]ell\s+[Aa]rea\s*:\s*(\S+)", txt, re.M)
    if sc:
        m["std_cell_area"] = sc.group(1)
    return m


# ---------------------------------------------------------------------------
# Congestion from congestion.{top}.*.rpt
# metric.py congestion patterns
# ---------------------------------------------------------------------------

def parse_congestion(file_path):
    """Parse congestion.{top}.*.rpt -- metric.py congestion patterns."""
    txt = _reads(file_path)
    if not txt:
        return {}
    m = {}
    pats = {
        "overall": re.compile(
            r"^Both Dirs\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
        "H":       re.compile(
            r"^H routing\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
        "V":       re.compile(
            r"^V routing\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
    }
    for key, pat in pats.items():
        hit = pat.search(txt)
        if hit:
            val = hit.group(1)
            pct = re.search(r"(\d+\.\d+)%", val)
            m[key] = (pct.group(1) + "%") if pct else val
    return m


# ---------------------------------------------------------------------------
# Power from report_power_info.mission.ff*.rpt (leakage)
#         and report_power_info.mission.ss*.rpt (dynamic)
# prc.py get_power_numbers()
# ---------------------------------------------------------------------------

def parse_power(leakage_path, dynamic_path):
    """Parse power from ff (leakage) and ss (dynamic) mission reports."""
    result = {}
    for path, key in [(leakage_path, "leakage_mw"),
                      (dynamic_path,  "dynamic_mw")]:
        lines = _read(path)
        for line in lines:
            if re.search(r"Total Dynamic Power", line):
                val = line.strip().split("=")[-1].split("(")[0].strip()
                if val and val != "N/A":
                    try:
                        parts = val.split()
                        num   = float(parts[0])
                        unit  = parts[1] if len(parts) > 1 else "mW"
                        conv  = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0,
                                 "W": 1000.0}
                        result[key] = f"{num * conv.get(unit, 1.0):.4f}"
                    except Exception:
                        result[key] = val
                break
            if re.search(r"Cell Leakage Power", line):
                val = line.strip().split("=")[-1].strip()
                if val and val != "N/A":
                    try:
                        parts = val.split()
                        num   = float(parts[0])
                        unit  = parts[1] if len(parts) > 1 else "mW"
                        conv  = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0,
                                 "W": 1000.0}
                        result[key] = f"{num * conv.get(unit, 1.0):.4f}"
                    except Exception:
                        result[key] = val
                break
    return result


# ---------------------------------------------------------------------------
# QOR timing from qor.{top}.*.rpt
# qor.py Qor.parse_qor() -- exact logic
# ---------------------------------------------------------------------------

def parse_qor_rpt(file_path):
    """Parse qor.{top}.*.rpt -- qor.py Qor.parse_qor() logic."""
    lines = _read(file_path)
    if not lines:
        return {}
    m = {}

    # Tool version
    for line in lines:
        tv = re.search(r"^Version:\s*(\S+)", line)
        if tv:
            m["tool_version"] = tv.group(1)
            break

    scenarios = {}
    n = len(lines)
    i = 0
    cur_scenario = "default"
    cur_type     = "setup"

    SCENARIO_RE  = re.compile(r"^Scenario\s*[:'\"]+\s*(\S+?)['\"]?\s*$")
    SETUP_RE     = re.compile(r"^Setup violations\s*$")
    HOLD_RE      = re.compile(r"^Hold violations\s*$")
    GRP_RE       = re.compile(
        r"^Timing Path Group\s*[:'\"]+\s*(\S+?)['\"]?\s*$")
    WNS_SETUP    = re.compile(r"^Worst Negative Slack\s*:\s*(\S+)")
    WNS_HOLD     = re.compile(r"^Worst Hold Violation\s*:\s*(\S+)")
    TNS_RE       = re.compile(r"Total Negative Slack\s*:\s*(\S+)")
    NVP_RE       = re.compile(r"Number of Violating Paths\s*:\s*(\S+)")
    NO_VIOL      = re.compile(r"^No setup violations found")

    while i < n:
        line = lines[i].rstrip()
        sm = SCENARIO_RE.match(line)
        if sm:
            cur_scenario = sm.group(1)
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            i += 1; continue
        if SETUP_RE.match(line):
            cur_type = "setup"; i += 1; continue
        if HOLD_RE.match(line):
            cur_type = "hold"; i += 1; continue
        gm = GRP_RE.match(line)
        if gm:
            grp = gm.group(1)
            wns = tns = nvp = "-"
            for j in range(i + 1, min(i + 12, n)):
                l2 = lines[j]
                w = WNS_SETUP.match(l2) or WNS_HOLD.match(l2)
                if w:
                    wns = w.group(1)
                t = TNS_RE.search(l2)
                if t:
                    tns = t.group(1)
                v = NVP_RE.search(l2)
                if v:
                    nvp = v.group(1)
            key = f"{grp}/{cur_type}"
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": wns, "tns": tns, "nvp": nvp}
            i += 1; continue
        if NO_VIOL.match(line):
            key = f"REG2REG/{cur_type}"
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": "0.0000", "tns": "0.0000", "nvp": "0"}
            i += 1; continue
        i += 1

    m["timing"] = scenarios

    # Extract R2R setup/hold strings for Image 1 display
    r2r_setup = r2r_hold = None
    worst_wns = None

    for sce, grps in scenarios.items():
        for grp_key, vals in grps.items():
            if "REG2REG" not in grp_key:
                continue
            wns = vals.get("wns", "-")
            tns = vals.get("tns", "-")
            nvp = vals.get("nvp", "-")
            ts  = f"{wns}/{tns}/{nvp}"
            if "setup" in grp_key and r2r_setup is None:
                r2r_setup = ts
                try:
                    wv = float(wns)
                    if worst_wns is None or wv < worst_wns:
                        worst_wns = wv
                        m["worst_scenario"] = sce
                        m["worst_wns"] = wns
                        m["worst_tns"] = tns
                        m["worst_nvp"] = nvp
                except Exception:
                    pass
            if "hold" in grp_key and r2r_hold is None:
                r2r_hold = ts

    m["r2r_setup"] = r2r_setup or "-"
    m["r2r_hold"]  = r2r_hold  or "-"
    return m


# ---------------------------------------------------------------------------
# MAIN ENTRY POINTS
# ---------------------------------------------------------------------------

def extract_fe_metrics(run_dir, block_name):
    """Extract all FE synth metrics for a completed run.
    Called on-demand only (MetricWorker) -- never during scan."""
    source = ("OUTFEED" if "outfeed" in run_dir.lower() else "WS")
    files  = syn_files(run_dir, source)
    result = {
        "block":    block_name,
        "run_dir":  run_dir,
        "run_name": os.path.basename(run_dir),
        "source":   source,
    }

    # 1. AREA  (area.{top}.*.rpt)
    result["area"] = parse_area(files["area"]) if files["area"] else {}

    # 2. UTILIZATION  (utilization.{top}.*.rpt)
    result["util"] = parse_utilization(files["utilization"]) \
                     if files["utilization"] else {}

    # 3. VTH  (cell_usage.summary.{top}.*.rpt)
    if files["vth"]:
        vth_raw = get_vth_group(files["vth"])
        result["vth_raw"]    = vth_raw
        result["vth_totals"] = sort_vth_groups(vth_raw)
    else:
        result["vth_raw"]    = {}
        result["vth_totals"] = {}

    # 4. MBIT  (multibit_banking_ratio.{top}.*.rpt)
    result["mbit"] = get_mbit_ratio(files["mbit"]) if files["mbit"] else "-"

    # 5. CGC   (clock_gating_info.mission.rpt -- fixed name)
    cgc_path = files["cgc"]
    if cgc_path and os.path.exists(cgc_path):
        result["cgc"] = get_cgc_ratio(cgc_path)
    else:
        # fallback to check_timing glob
        cgc_path2 = files.get("chk_timing")
        result["cgc"] = get_cgc_ratio(cgc_path2) if cgc_path2 else "NA"

    # 6. CONGESTION  (congestion.{top}.*.rpt)
    result["congestion"] = parse_congestion(files["congestion"]) \
                           if files["congestion"] else {}

    # 7. TIMING  (qor.{top}.*.rpt)
    if files["qor"]:
        qor = parse_qor_rpt(files["qor"])
        result["timing_raw"]     = qor
        result["r2r_setup"]      = qor.get("r2r_setup", "-")
        result["r2r_hold"]       = qor.get("r2r_hold",  "-")
        result["worst_scenario"] = qor.get("worst_scenario", "-")
        result["worst_wns"]      = qor.get("worst_wns", "-")
        result["worst_tns"]      = qor.get("worst_tns", "-")
        result["worst_nvp"]      = qor.get("worst_nvp", "-")
    else:
        result["r2r_setup"] = "-"
        result["r2r_hold"]  = "-"

    # 8. POWER  (ff=leakage, ss=dynamic)
    result["power"] = parse_power(files["leakage"], files["dynamic"])

    # 9. RUNTIME  (runtime.V2.rpt)
    result["runtime"] = get_runtime(files["runtime"])

    # 10. FILES FOUND (for debug)
    result["_files"] = {k: v for k, v in files.items() if v}

    return result


def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    """Extract PNR stage metrics on demand."""
    files  = pnr_files(run_dir, stage_name, source)
    result = {"stage": stage_name, "run_dir": run_dir}

    # Timing
    if os.path.exists(files["qor"]):
        qor = parse_qor_rpt(files["qor"])
        result["timing_raw"] = qor
        result["r2r_setup"]  = qor.get("r2r_setup", "-")
        result["r2r_hold"]   = qor.get("r2r_hold",  "-")
        result["worst_wns"]  = qor.get("worst_wns", "-")
        result["worst_tns"]  = qor.get("worst_tns", "-")
        result["worst_nvp"]  = qor.get("worst_nvp", "-")

    # DRC  ({stage}.physical_all.sum -- note: .sum not .rpt)
    result["drc"] = get_drc(files["drc"]) if os.path.exists(
        files["drc"]) else "-"

    # Runtime
    result["runtime"] = get_runtime(files["runtime"]) if os.path.exists(
        files["runtime"]) else "-"

    result["_files"] = files
    return result


# ---------------------------------------------------------------------------
# STANDALONE DEBUG
# Usage: python3.6 metric_extract.py /path/to/run-FE BLK_ISP2
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3.6 metric_extract.py /path/to/run-FE [BLOCK_NAME]")
        sys.exit(1)

    run_dir    = sys.argv[1].rstrip('/')
    block_name = sys.argv[2] if len(sys.argv) > 2 else "UNKNOWN"
    source     = "OUTFEED" if "outfeed" in run_dir.lower() else "WS"
    top        = _get_top(run_dir, source)
    rd         = _rpt_dir(run_dir)

    print("=" * 70)
    print(f"run_dir  : {run_dir}")
    print(f"source   : {source}")
    print(f"top      : {top}  (self.top in SynFileDb)")
    print(f"rpt_dir  : {rd}")
    print("=" * 70)

    if not os.path.isdir(rd):
        print(f"ERROR: rpt_dir not found: {rd}")
        sys.exit(1)

    files = syn_files(run_dir, source)
    print("\nFILE PATH CHECK (from SynFileDb file.py lines 100-116):")
    labels = {
        "area":        "area.{top}.*.rpt",
        "cgc":         "clock_gating_info.mission.rpt (FIXED)",
        "congestion":  "congestion.{top}.*.rpt",
        "mbit":        "multibit_banking_ratio.{top}.*.rpt",
        "qor":         "qor.{top}.*.rpt",
        "utilization": "utilization.{top}.*.rpt",
        "vth":         "cell_usage.summary.{top}.*.rpt",
        "chk_timing":  "check_timing.{top}.*.rpt",
        "leakage":     "report_power_info.mission.ff*.rpt",
        "dynamic":     "report_power_info.mission.ss*.rpt",
        "runtime":     "runtime.V2.rpt",
        "saif":        "report_activity_summary.rpt",
    }
    for key, desc in labels.items():
        path = files.get(key)
        if path and os.path.exists(path):
            status = "FOUND: " + os.path.basename(path)
        elif path:
            status = "MISSING: " + os.path.basename(path)
        else:
            status = "NOT FOUND (no match for pattern)"
        print(f"  {key:15s} [{desc}]")
        print(f"             -> {status}")

    print("\n" + "=" * 70)
    print("EXTRACTED METRICS:")
    metrics = extract_fe_metrics(run_dir, block_name)

    print(f"\n  runtime   : {metrics.get('runtime', '-')}")
    print(f"  r2r_setup : {metrics.get('r2r_setup', '-')}")
    print(f"  r2r_hold  : {metrics.get('r2r_hold', '-')}")
    print(f"  mbit      : {metrics.get('mbit', '-')}")
    print(f"  cgc       : {metrics.get('cgc', '-')}")

    area = metrics.get("area", {})
    print(f"\n  AREA:")
    for k in ["total_area","combinational_area","reg_area",
              "macro_area","buf_area","total_count"]:
        print(f"    {k:25s} : {area.get(k, '(not found)')}")

    util = metrics.get("util", {})
    print(f"\n  UTILIZATION:")
    for k in ["total_util","std_cell_only_util","memory_util",
              "std_cell_area","memory_area"]:
        print(f"    {k:25s} : {util.get(k, '(not found)')}")

    print(f"\n  VTH (raw lut from Cell Count Report):")
    for k, v in metrics.get("vth_raw", {}).items():
        print(f"    {k:15s} : {v}")
    print(f"  VTH (totals grouped):")
    for k, v in metrics.get("vth_totals", {}).items():
        print(f"    {k:15s} : {v}")

    cong = metrics.get("congestion", {})
    print(f"\n  CONGESTION:")
    print(f"    overall : {cong.get('overall', '(not found)')}")
    print(f"    H       : {cong.get('H', '(not found)')}")
    print(f"    V       : {cong.get('V', '(not found)')}")

    pwr = metrics.get("power", {})
    print(f"\n  POWER:")
    print(f"    leakage_mw  : {pwr.get('leakage_mw', '(not found)')}")
    print(f"    dynamic_mw  : {pwr.get('dynamic_mw', '(not found)')}")
