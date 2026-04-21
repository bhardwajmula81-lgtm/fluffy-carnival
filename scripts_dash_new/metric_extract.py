# -*- coding: ascii -*-
# metric_extract.py
# Exact regex patterns from metric.py, prc.py, implementation.py in summary scripts.
# Self-contained -- no external deps. Pure Python re/glob/os.

import os
import re
import glob


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _find_rpt(rpt_dir, prefix):
    """Find latest report matching prefix.*.rpt
    Handles: cell_usage.summary.BLK_ISP2.20260416_1628.rpt
             qor.BLK_ISP2.20260416_1628.rpt
             report_power_info.mission.ssp_*.compile_opt.*.rpt
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None
    hits = glob.glob(os.path.join(rpt_dir, prefix + ".*"))
    hits = [h for h in hits
            if not h.endswith(".log") and not h.endswith(".gz")]
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
# AREA + UTILIZATION
# File: cell_usage.summary.{BLOCK}.{TIMESTAMP}.rpt
# Exact patterns from metric.py
# ---------------------------------------------------------------------------

def parse_cell_usage(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}

    # Area patterns -- exact from metric.py
    area_pats = {
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
    for key, pat in area_pats.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()

    # Utilization -- exact from metric.py
    util_pats = {
        "total_util":         re.compile(
            r"^\s*Total utilization\s*:\s*(\S+)", re.M),
        "memory_util":        re.compile(
            r"^\s*Memory utilization\s*:\s*(\S+)", re.M),
        "std_cell_only_util": re.compile(
            r"^\s*Standard cell only utilization\s*:\s*(\S+)", re.M),
        "core_area":          re.compile(
            r"^\s*core_area\s*:\s*(\S+)", re.M),
        "std_cell_util":      re.compile(
            r"^\s*Stdcell utilization\s*:\s*(\S+)", re.M),
    }
    for key, pat in util_pats.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()

    # Memory area (from utilization table: "Memory utilization : X  X  AREA")
    mem_hit = re.search(
        r"^\s*Memory utilization\s*:\s*\S+\s+\S+\s+(\S+)", txt, re.M)
    if mem_hit:
        m["memory_area"] = mem_hit.group(1)

    # MBIT -- exact from metric.py
    # Format: "Flip-flop cells banking ratio  X : Y%"
    mbit_hit = re.search(
        r"^Flip-flop cells banking ratio\s+\S+\s+:\s+(\S+)", txt, re.M)
    if mbit_hit:
        m["mbit"] = mbit_hit.group(1).strip()

    # VTH -- exact from prc.py get_syn_vth_data()
    # Reads lines after "Cell Usage by Vth" header
    # Format: LVT_G | 21.21% | N | 22.62% | ...
    vth = {"inst": {}, "area": {}}
    lines = txt.splitlines()
    vth_start = -1
    for i, line in enumerate(lines):
        if "Cell Usage by Vth" in line:
            vth_start = i
            break
    if vth_start >= 0:
        # prc.py reads f.readlines()[5:15] after the header line
        vth_lines = lines[vth_start + 1: vth_start + 20]
        for line in vth_lines:
            # strip and split after removing pipes
            lst = line.strip().replace("|", "").split()
            if len(lst) >= 5:
                try:
                    inst_pct = lst[1]  # e.g. "21.21%"
                    area_pct = lst[3]  # e.g. "22.62%"
                    if float(inst_pct.rstrip("%")) > 0:
                        vth_name = lst[0].split("_")[0]  # LVT, HVT, RVT
                        vth["inst"][vth_name] = inst_pct
                        vth["area"][vth_name] = area_pct
                except Exception:
                    pass
    if vth["inst"]:
        m["vth"] = vth

    # Std Cell Area shown in Image 1 = combinational (std cells only, no macros)
    # From output.py it uses area.result['std_cell_area'] which maps to
    # the utilization table "Std Cell Area" line
    sc_area = re.search(
        r"^\s*Std[. ]+[Cc]ell\s+[Aa]rea\s*:\s*(\S+)", txt, re.M)
    if sc_area:
        m["std_cell_area"] = sc_area.group(1)
    elif "combinational_area" in m:
        # fallback: use combinational area as std cell area
        m["std_cell_area"] = m["combinational_area"]

    # Gate count -- from gate count section or prj_cfg
    gc_hit = re.search(r"Gate\s+[Cc]ount\s*:\s*(\d+)", txt, re.M)
    if gc_hit:
        m["gate_count"] = gc_hit.group(1)

    # Instance count
    inst_hit = re.search(r"^Number of cells:\s+(\S+)", txt, re.M)
    if inst_hit:
        m["instance_count"] = inst_hit.group(1)

    return m


# ---------------------------------------------------------------------------
# CONGESTION
# File: congestion.{BLOCK}.{TIMESTAMP}.rpt
# Exact patterns from metric.py
# ---------------------------------------------------------------------------

def parse_congestion(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}
    # Exact from metric.py:
    # 'overall': r'^Both Dirs\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+\(\S+\))'
    # 'H':       r'^H routing\s*\|\s*...'
    # 'V':       r'^V routing\s*\|\s*...'
    pats = {
        "overall": re.compile(
            r"^Both Dirs\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
        "H": re.compile(
            r"^H routing\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
        "V": re.compile(
            r"^V routing\s*\|\s*\d+\.\d+\s*\|\s*\d+\.\d+\s*\|\s*(\S+)",
            re.M),
    }
    for key, pat in pats.items():
        hit = pat.search(txt)
        if hit:
            # Value may be like "0.2002%(14)" -- extract just the % part
            val = hit.group(1)
            pct = re.search(r"(\d+\.\d+)%", val)
            m[key] = pct.group(1) + "%" if pct else val
    return m


# ---------------------------------------------------------------------------
# CGC RATIO
# File: check_timing.{BLOCK}.{TIMESTAMP}.rpt  (or qor.rpt)
# From prc.py get_cgc_ratio()
# ---------------------------------------------------------------------------

def parse_cgc(path):
    txt = _read(path)
    if not txt:
        return {}
    # prc.py searches last 100 lines
    data = txt.splitlines()[-100:]
    for line in data:
        cgc = re.search(
            r"Number of Gated registers\s+\|\s+\d+\s+\|\s+\d+\s+\|\s+(\S+\(\S+\))",
            line)
        if cgc:
            return {"cgc": cgc.group(1)}
        tool_cgc = re.search(
            r"Number of Tool-Inserted Gated registers\s+\|\s+\d+\s+\|\s+\d+\s+\|\s+(\S+\(\S+\))",
            line)
        if tool_cgc:
            return {"cgc": tool_cgc.group(1)}
    # Simpler fallback -- just a percentage
    hit = re.search(r"Gated registers.*?(\d+\.\d+)%", txt, re.M)
    if hit:
        return {"cgc": hit.group(1) + "%"}
    return {}


# ---------------------------------------------------------------------------
# POWER
# File: report_power_info.mission.{SCENARIO}.compile_opt.{TIMESTAMP}.rpt
# From prc.py get_power_numbers()
# ---------------------------------------------------------------------------

def parse_power(rpt_dir):
    # Find mission power report (exact pattern from user)
    hits = glob.glob(
        os.path.join(rpt_dir, "report_power_info.mission.*.rpt"))
    if not hits:
        hits = glob.glob(
            os.path.join(rpt_dir, "report_power_info.*.rpt"))
    if not hits:
        return {}
    path = sorted(hits, key=os.path.getmtime)[-1]
    txt  = _read(path)
    if not txt:
        return {}
    m = {}

    # prc.py get_power_numbers() reads forward for Total Dynamic Power
    # then Cell Leakage Power
    for line in txt.splitlines():
        if re.search(r"Total Dynamic Power", line):
            val = line.strip().split("=")[-1].split("(")[0].strip()
            if val != "N/A":
                m["dynamic_raw"] = val
                try:
                    parts = val.split()
                    num  = float(parts[0])
                    unit = parts[1] if len(parts) > 1 else "mW"
                    conv = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0}
                    m["dynamic_mw"] = f"{num * conv.get(unit, 1.0):.4f}"
                except Exception:
                    m["dynamic_mw"] = val
            break
    for line in txt.splitlines():
        if re.search(r"Cell Leakage Power", line):
            val = line.strip().split("=")[-1].strip()
            if val != "N/A":
                m["leakage_raw"] = val
                try:
                    parts = val.split()
                    num  = float(parts[0])
                    unit = parts[1] if len(parts) > 1 else "mW"
                    conv = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0}
                    m["leakage_mw"] = f"{num * conv.get(unit, 1.0):.4f}"
                except Exception:
                    m["leakage_mw"] = val
            break

    return m


# ---------------------------------------------------------------------------
# DRC from compile_opt.log
# From procedure.py Util.count_error_in_file -> grep -c "^Error" file
# ---------------------------------------------------------------------------

def parse_drc_from_log(log_path):
    count = 0
    txt = _read(log_path)
    if not txt:
        return "-"
    for line in txt.splitlines():
        if line.startswith("Error:"):
            count += 1
    return str(count)


# ---------------------------------------------------------------------------
# QOR TIMING
# File: qor.{BLOCK}.{TIMESTAMP}.rpt
# From qor.py Qor.parse_qor()
# ---------------------------------------------------------------------------

def parse_qor_rpt(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}

    # Tool version
    tv = re.search(r"^Version:\s*(\S+)", txt, re.M)
    if tv:
        m["tool_version"] = tv.group(1)

    # Parse timing: scenario -> path_group -> {wns, tns, nvp}
    # From qor.py parse_qor() -- reads scenario blocks line by line
    lines = txt.splitlines()
    n = len(lines)
    i = 0
    scenarios = {}
    cur_scenario = "default"
    cur_type     = "setup"

    SCENARIO_RE   = re.compile(r"^Scenario\s*[:'\"]+\s*(\S+?)['\"]?\s*$")
    SETUP_VIO_RE  = re.compile(r"^Setup violations\s*$")
    HOLD_VIO_RE   = re.compile(r"^Hold violations\s*$")
    GRP_RE        = re.compile(
        r"^Timing Path Group\s*[:'\"]+\s*(\S+?)['\"]?\s*$")
    WNS_SETUP_RE  = re.compile(r"^Worst Negative Slack\s*:\s*(\S+)")
    WNS_HOLD_RE   = re.compile(r"^Worst Hold Violation\s*:\s*(\S+)")
    TNS_RE        = re.compile(r"Total Negative Slack\s*:\s*(\S+)")
    NVP_RE        = re.compile(r"Number of Violating Paths\s*:\s*(\S+)")
    NO_VIOL_RE    = re.compile(r"^No setup violations found")
    GLOBAL_SET_RE = re.compile(r"^report_global_timing\s+setup")
    GLOBAL_HLD_RE = re.compile(r"^report_global_timing\s+hold")

    while i < n:
        line = lines[i].rstrip()

        sm = SCENARIO_RE.match(line)
        if sm:
            cur_scenario = sm.group(1)
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            i += 1; continue

        if SETUP_VIO_RE.match(line):
            cur_type = "setup"; i += 1; continue
        if HOLD_VIO_RE.match(line):
            cur_type = "hold"; i += 1; continue
        if GLOBAL_SET_RE.match(line):
            cur_type = "setup"; i += 1; continue
        if GLOBAL_HLD_RE.match(line):
            cur_type = "hold"; i += 1; continue

        gm = GRP_RE.match(line)
        if gm:
            grp = gm.group(1)
            wns = tns = nvp = "-"
            # qor.py reads next ~9 lines for hold, ~9 lines for setup
            for j in range(i + 1, min(i + 12, n)):
                l = lines[j]
                w = WNS_SETUP_RE.match(l) or WNS_HOLD_RE.match(l)
                if w:
                    wns = w.group(1)
                t = TNS_RE.search(l)
                if t:
                    tns = t.group(1)
                v = NVP_RE.search(l)
                if v:
                    nvp = v.group(1)
            key = grp + "/" + cur_type
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": wns, "tns": tns, "nvp": nvp}
            i += 1; continue

        if NO_VIOL_RE.match(line):
            key = "REG2REG/" + cur_type
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": "0.0000", "tns": "0.0000", "nvp": "0"}
            i += 1; continue

        i += 1

    m["timing"] = scenarios

    # Extract R2R setup/hold string for Image 1 display
    # From output.py: compare_qor(qor_instances, 'setup') for R2R
    r2r_setup = r2r_hold = None
    worst_setup_wns = None

    for sce, grps in scenarios.items():
        for grp_key, vals in grps.items():
            if "REG2REG" in grp_key:
                wns = vals.get("wns", "-")
                tns = vals.get("tns", "-")
                nvp = vals.get("nvp", "-")
                timing_str = wns + "/" + tns + "/" + nvp
                if "setup" in grp_key:
                    if r2r_setup is None:
                        r2r_setup = timing_str
                    try:
                        wv = float(wns)
                        if worst_setup_wns is None or wv < worst_setup_wns:
                            worst_setup_wns = wv
                            m["worst_scenario"] = sce
                            m["worst_wns"] = wns
                            m["worst_tns"] = tns
                            m["worst_nvp"] = nvp
                    except Exception:
                        pass
                elif "hold" in grp_key and r2r_hold is None:
                    r2r_hold = timing_str

    m["r2r_setup"] = r2r_setup or "-"
    m["r2r_hold"]  = r2r_hold  or "-"
    return m


# ---------------------------------------------------------------------------
# MAIN ENTRY POINTS
# ---------------------------------------------------------------------------

def extract_fe_metrics(run_dir, block_name):
    """Extract all FE metrics for one completed run. Called on-demand only."""
    rpt_dir = os.path.join(run_dir, "reports")
    result  = {
        "block":    block_name,
        "run_dir":  run_dir,
        "run_name": os.path.basename(run_dir),
    }

    # 1. Area + util + VTH + MBIT
    cu_path = _find_rpt(rpt_dir, "cell_usage.summary")
    area    = parse_cell_usage(cu_path) if cu_path else {}
    result["area"] = area

    # 2. Timing
    qor_path = _find_rpt(rpt_dir, "qor")
    if qor_path:
        qor = parse_qor_rpt(qor_path)
        result["timing_raw"]     = qor
        result["r2r_setup"]      = qor.get("r2r_setup", "-")
        result["r2r_hold"]       = qor.get("r2r_hold",  "-")
        result["worst_scenario"] = qor.get("worst_scenario", "-")
        result["worst_wns"]      = qor.get("worst_wns", "-")
        result["worst_tns"]      = qor.get("worst_tns", "-")
        result["worst_nvp"]      = qor.get("worst_nvp", "-")
        # MBIT from qor if not in cell_usage
        if "mbit" in qor and "mbit" not in area:
            result["mbit"] = qor["mbit"]
        else:
            result["mbit"] = area.get("mbit", "-")
    else:
        result["r2r_setup"]  = "-"
        result["r2r_hold"]   = "-"
        result["mbit"]       = area.get("mbit", "-")

    # 3. CGC from check_timing report
    cgc_path = _find_rpt(rpt_dir, "check_timing")
    if not cgc_path:
        cgc_path = _find_rpt(rpt_dir, "qor")
    cgc_data = parse_cgc(cgc_path) if cgc_path else {}
    result["cgc"] = cgc_data.get("cgc", "-")

    # 4. Congestion
    cong_path = _find_rpt(rpt_dir, "congestion")
    if not cong_path:
        cong_path = _find_rpt(rpt_dir, "clock_gating_info.mission")
    result["congestion"] = parse_congestion(cong_path) if cong_path else {}

    # 5. Power (mission scenario report)
    result["power"] = parse_power(rpt_dir)

    # 6. DRC from compile log
    log = os.path.join(run_dir, "logs", "compile_opt.log")
    result["drc_errors"] = parse_drc_from_log(log)

    # 7. Runtime already in tree -- pass through if provided
    result["runtime"] = "-"

    return result


def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    """Extract PNR stage metrics on demand."""
    result = {"stage": stage_name, "run_dir": run_dir}
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)

    qor_path = _find_rpt(rpt_dir, stage_name + ".qor")
    if not qor_path:
        qor_path = _find_rpt(rpt_dir, "qor")
    if qor_path:
        qor = parse_qor_rpt(qor_path)
        result["timing_raw"] = qor
        result["r2r_setup"]  = qor.get("r2r_setup", "-")
        result["r2r_hold"]   = qor.get("r2r_hold",  "-")
        result["worst_wns"]  = qor.get("worst_wns", "-")
        result["worst_tns"]  = qor.get("worst_tns", "-")
        result["worst_nvp"]  = qor.get("worst_nvp", "-")

    # DRC
    drc_path = _find_rpt(rpt_dir, stage_name + ".physical_all_sum")
    if not drc_path:
        drc_path = _find_rpt(rpt_dir, stage_name + ".drc")
    if drc_path:
        txt  = _read(drc_path)
        drc  = {}
        hit  = re.search(r"Total number of DRCs\s*=\s*(\d+)", txt)
        if hit:
            drc["drc_count"] = hit.group(1)
        hit2 = re.search(r"Short\s*:\s*(\d+)", txt)
        if hit2:
            drc["shorts"] = hit2.group(1)
        result["drc"] = drc

    return result


# ---------------------------------------------------------------------------
# STANDALONE DEBUG -- run this file directly to diagnose a run:
# python3.6 metric_extract.py /path/to/run-FE BLK_ISP2
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3.6 metric_extract.py /path/to/run-FE [BLOCK_NAME]")
        sys.exit(1)

    run_dir    = sys.argv[1]
    block_name = sys.argv[2] if len(sys.argv) > 2 else "UNKNOWN"
    rpt_dir    = os.path.join(run_dir, "reports")

    print("=" * 60)
    print(f"run_dir : {run_dir}")
    print(f"rpt_dir : {rpt_dir}")
    print(f"block   : {block_name}")
    print("=" * 60)

    # Check rpt_dir exists
    if not os.path.isdir(rpt_dir):
        print(f"ERROR: rpt_dir does not exist: {rpt_dir}")
        sys.exit(1)

    # Show all .rpt files found
    all_rpts = glob.glob(os.path.join(rpt_dir, "*.rpt"))
    print(f"\nAll .rpt files in rpt_dir ({len(all_rpts)} found):")
    for r in sorted(all_rpts):
        print(f"  {os.path.basename(r)}")

    print()

    # Check each expected file
    checks = [
        ("cell_usage.summary", "AREA/UTIL/VTH/MBIT"),
        ("qor",                "TIMING WNS/TNS/NVP"),
        ("check_timing",       "CGC RATIO"),
        ("congestion",         "CONGESTION"),
        ("clock_gating_info.mission", "CONGESTION (fallback)"),
        ("report_power_info.mission", "POWER"),
    ]
    found_files = {}
    for prefix, desc in checks:
        hit = _find_rpt(rpt_dir, prefix)
        status = f"FOUND: {os.path.basename(hit)}" if hit else "NOT FOUND"
        print(f"  [{desc:30s}] {status}")
        if hit:
            found_files[prefix] = hit

    log = os.path.join(run_dir, "logs", "compile_opt.log")
    log_status = "FOUND" if os.path.exists(log) else "NOT FOUND"
    print(f"  [{'DRC from compile_opt.log':30s}] {log_status}")
    print()

    # Actually extract and show results
    print("EXTRACTING METRICS...")
    metrics = extract_fe_metrics(run_dir, block_name)

    area  = metrics.get("area", {})
    cong  = metrics.get("congestion", {})
    power = metrics.get("power", {})

    print()
    print("AREA:")
    for k in ["total_area","std_cell_area","combinational_area",
              "reg_area","macro_area","memory_area","buf_area",
              "total_count","instance_count","gate_count",
              "total_util","std_cell_only_util","mbit"]:
        v = area.get(k, "(not found)")
        print(f"  {k:30s} = {v}")

    print()
    print("VTH:")
    vth = area.get("vth", {})
    if vth:
        for vtype in sorted(vth.get("inst", {}).keys()):
            inst = vth["inst"].get(vtype, "-")
            ar   = vth.get("area", {}).get(vtype, "-")
            print(f"  {vtype}: inst={inst}  area={ar}")
    else:
        print("  (not found)")

    print()
    print("TIMING:")
    print(f"  r2r_setup = {metrics.get('r2r_setup', '(not found)')}")
    print(f"  r2r_hold  = {metrics.get('r2r_hold',  '(not found)')}")
    print(f"  worst_wns = {metrics.get('worst_wns', '(not found)')}")
    print(f"  worst_tns = {metrics.get('worst_tns', '(not found)')}")
    print(f"  worst_nvp = {metrics.get('worst_nvp', '(not found)')}")
    timing = metrics.get("timing_raw", {}).get("timing", {})
    if timing:
        for sce, grps in sorted(timing.items())[:3]:
            for grp, vals in sorted(grps.items()):
                print(f"    {sce} | {grp}: "
                      f"WNS={vals.get('wns','-')} "
                      f"TNS={vals.get('tns','-')} "
                      f"NVP={vals.get('nvp','-')}")

    print()
    print("CGC:")
    print(f"  cgc = {metrics.get('cgc', '(not found)')}")

    print()
    print("CONGESTION:")
    print(f"  overall = {cong.get('overall', '(not found)')}")
    print(f"  H       = {cong.get('H', '(not found)')}")
    print(f"  V       = {cong.get('V', '(not found)')}")

    print()
    print("POWER:")
    print(f"  dynamic_mw = {power.get('dynamic_mw', '(not found)')}")
    print(f"  leakage_mw = {power.get('leakage_mw', '(not found)')}")

    print()
    print("DRC:")
    print(f"  drc_errors = {metrics.get('drc_errors', '(not found)')}")
