# -*- coding: ascii -*-
# metric_extract.py
# Self-contained QoR metric parsers.
# Reads same report files as summary.py -- no external dependencies.
# Report naming: {prefix}.{BLOCK}.{TIMESTAMP}.rpt

import os
import re
import glob


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _find_rpt(rpt_dir, prefix, ext=".rpt"):
    """Find latest report matching prefix.*.rpt in rpt_dir.
    Handles: cell_usage.summary.BLK_ISP2.20260416_1628.rpt
             qor.BLK_ISP2.20260416_1628.rpt
             report_power_info.mission.ssp_*.compile_opt.*.rpt
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None
    # Try with explicit ext
    hits = glob.glob(os.path.join(rpt_dir, prefix + ".*" + ext))
    if not hits:
        # Try without assuming extension position
        hits = glob.glob(os.path.join(rpt_dir, prefix + ".*"))
        hits = [h for h in hits if not h.endswith(".log")]
    return sorted(hits, key=os.path.getmtime)[-1] if hits else None


def _read(path):
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""


def _dash(val):
    """Return '-' if val is empty/None, else val as string."""
    if val is None or str(val).strip() in ("", "-"):
        return "-"
    return str(val).strip()


# ---------------------------------------------------------------------------
# AREA / UTILIZATION / VTH
# File: cell_usage.summary.{BLOCK}.{TIMESTAMP}.rpt
# ---------------------------------------------------------------------------

def parse_cell_usage(path):
    """Returns dict with area, util, vth keys matching Image 1 fields."""
    txt = _read(path)
    if not txt:
        return {}
    m = {}

    pats = {
        "total_area":            re.compile(r"^Total cell area:\s+(\S+)", re.M),
        "std_cell_area":         re.compile(r"^Combinational area:\s+(\S+)", re.M),
        "noncomb_area":          re.compile(r"^Noncombinational area:\s+(\S+)", re.M),
        "macro_area":            re.compile(r"^Macro/Black Box area:\s+(\S+)", re.M),
        "buf_area":              re.compile(r"^Buf/Inv area:\s+(\S+)", re.M),
        "total_count":           re.compile(r"^Number of cells:\s+(\S+)", re.M),
        "total_util":            re.compile(
            r"^\s*Total utilization\s*:\s*(\S+)", re.M),
        "std_cell_only_util":    re.compile(
            r"^\s*Standard cell only utilization\s*:\s*(\S+)", re.M),
        "memory_util":           re.compile(
            r"^\s*Memory utilization\s*:\s*(\S+)", re.M),
        "mbit":                  re.compile(
            r"^Flip-flop cells banking ratio\s+(\S+)", re.M),
    }
    for key, pat in pats.items():
        hit = pat.search(txt)
        if hit:
            m[key] = hit.group(1).strip()

    # Instance count and Gate count from utilization section
    inst_hit = re.search(r"^\s*(\d+)\s+instances", txt, re.M)
    if inst_hit:
        m["instance_count"] = inst_hit.group(1)

    # Std Cell Area = combinational + noncombinational (shown as "Std Cell Area" in HTML)
    try:
        sc = float(m.get("std_cell_area", 0)) + float(m.get("noncomb_area", 0))
        m["std_cell_area_total"] = f"{sc:.4f}"
    except Exception:
        pass

    # Memory area
    mem_hit = re.search(r"^Memory utilization\s*:\s*\S+\s+\S+\s+(\S+)", txt, re.M)
    if mem_hit:
        m["memory_area"] = mem_hit.group(1)

    # VTH distribution
    vth = {}
    # Look for Cell Usage by Vth section
    vth_section = re.search(r"Cell Usage by Vth(.*?)(?=^\s*$|\Z)",
                             txt, re.M | re.S)
    if vth_section:
        vth_block = vth_section.group(1)
        # Pattern: VTH_TYPE | inst_pct | count | area | area_pct | ...
        vth_row = re.compile(
            r"^\s*(\S+)\s+\|\s+(\S+)\s+\|\s+\d+\s+\|\s+\S+\s+(\S+)",
            re.M)
        for row in vth_row.finditer(vth_block):
            vtype = row.group(1).split("_")[0]  # LVT, HVT, RVT etc
            inst_pct = row.group(2)
            area_pct = row.group(3)
            vth[vtype] = {"inst": inst_pct, "area": area_pct}
    if vth:
        m["vth"] = vth

    # CGC from check_timing or qor (set separately)
    return m


# ---------------------------------------------------------------------------
# QOR / TIMING
# File: qor.{BLOCK}.{TIMESTAMP}.rpt
# ---------------------------------------------------------------------------

def parse_qor_rpt(path):
    """Returns dict with timing per scenario and misc QoR values.
    Matches Image 1: R2R Setup WNS/TNS/NVP, R2R Hold WNS/TNS/NVP."""
    txt = _read(path)
    if not txt:
        return {}
    m = {}

    # Tool version
    tv = re.search(r"^Version:\s*(\S+)", txt, re.M)
    if tv:
        m["tool_version"] = tv.group(1)

    # CGC ratio
    cgc = re.search(
        r"Number of Gated registers\s+\|?\s*\|\s*(\d+\s*\(\s*\S+\))", txt, re.M)
    if cgc:
        m["cgc"] = cgc.group(1).strip()
    # Simpler CGC %
    cgc2 = re.search(r"Number of Gated registers.*?(\d+\.\d+)%", txt, re.M)
    if cgc2:
        m["cgc_pct"] = cgc2.group(1)

    # MBIT from qor.rpt
    mbit = re.search(r"Flip-flop cells banking ratio\s+(\S+)", txt, re.M)
    if mbit:
        m["mbit"] = mbit.group(1)

    # Parse timing blocks: scenario -> path_group -> {wns, tns, nvp}
    scenarios = {}
    lines = txt.splitlines()
    n = len(lines)
    i = 0
    cur_scenario = "default"
    cur_type = "setup"

    # Regex patterns
    scenario_re   = re.compile(r"^Scenario\s*[:\s]+['\"]?(\S+?)['\"]?\s*$")
    setup_sec_re  = re.compile(r"^Setup violations\s*$")
    hold_sec_re   = re.compile(r"^Hold violations\s*$")
    grp_re        = re.compile(r"^Timing Path Group\s*[:\s]+['\"]?(\S+?)['\"]?\s*$")
    wns_setup_re  = re.compile(r"^Worst Negative Slack\s*:\s*(\S+)")
    wns_hold_re   = re.compile(r"^Worst Hold Violation\s*:\s*(\S+)")
    tns_re        = re.compile(r"Total Negative Slack\s*:\s*(\S+)")
    nvp_re        = re.compile(r"Number of Violating Paths\s*:\s*(\S+)")
    no_viol_re    = re.compile(r"^No setup violations found")
    global_set_re = re.compile(r"^report_global_timing\s+setup")
    global_hld_re = re.compile(r"^report_global_timing\s+hold")

    while i < n:
        line = lines[i].rstrip()

        sm = scenario_re.match(line)
        if sm:
            cur_scenario = sm.group(1)
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            i += 1; continue

        if setup_sec_re.match(line):
            cur_type = "setup"; i += 1; continue
        if hold_sec_re.match(line):
            cur_type = "hold"; i += 1; continue

        gm = grp_re.match(line)
        if gm:
            grp = gm.group(1)
            wns = tns = nvp = "-"
            for j in range(i + 1, min(i + 15, n)):
                l = lines[j]
                w = wns_setup_re.match(l) or wns_hold_re.match(l)
                if w:
                    wns = w.group(1)
                t = tns_re.search(l)
                if t:
                    tns = t.group(1)
                v = nvp_re.search(l)
                if v:
                    nvp = v.group(1)
            key = grp + "/" + cur_type
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": wns, "tns": tns, "nvp": nvp}
            i += 1; continue

        if no_viol_re.match(line):
            grp = "REG2REG"
            key = grp + "/" + cur_type
            if cur_scenario not in scenarios:
                scenarios[cur_scenario] = {}
            scenarios[cur_scenario][key] = {
                "wns": "0.0000", "tns": "0.0000", "nvp": "0"}
            i += 1; continue

        # Global timing (REG2REG, IN2REG etc) for summary report
        if global_set_re.match(line) or global_hld_re.match(line):
            g_type = "setup" if "setup" in line else "hold"
            cur_type = g_type

        i += 1

    if scenarios:
        m["timing"] = scenarios

    # Extract R2R setup/hold for Image 1 display
    # Look for REG2REG in all scenarios
    r2r_setup = r2r_hold = None
    for sce, grps in scenarios.items():
        for grp_key, vals in grps.items():
            if "REG2REG" in grp_key and "setup" in grp_key:
                w = vals.get("wns", "-")
                t = vals.get("tns", "-")
                v = vals.get("nvp", "-")
                r2r_setup = f"{w}/{t}/{v}"
            if "REG2REG" in grp_key and "hold" in grp_key:
                w = vals.get("wns", "-")
                t = vals.get("tns", "-")
                v = vals.get("nvp", "-")
                r2r_hold = f"{w}/{t}/{v}"
    if r2r_setup:
        m["r2r_setup"] = r2r_setup
    if r2r_hold:
        m["r2r_hold"] = r2r_hold

    # Worst scenario timing (for Image 2 summary table)
    worst_setup_wns = None
    for sce, grps in scenarios.items():
        for grp_key, vals in grps.items():
            if "REG2REG" in grp_key and "setup" in grp_key:
                try:
                    wv = float(vals.get("wns", "0"))
                    if worst_setup_wns is None or wv < worst_setup_wns:
                        worst_setup_wns = wv
                        m["worst_scenario"] = sce
                        m["worst_wns"] = vals.get("wns", "-")
                        m["worst_tns"] = vals.get("tns", "-")
                        m["worst_nvp"] = vals.get("nvp", "-")
                except Exception:
                    pass

    return m


# ---------------------------------------------------------------------------
# CONGESTION
# File: congestion.{BLOCK}.{TIMESTAMP}.rpt  OR
#       clock_gating_info.mission.rpt
# ---------------------------------------------------------------------------

def parse_congestion(path):
    txt = _read(path)
    if not txt:
        return {}
    m = {}
    both = re.search(r"Both Dirs.*?(\d+\.\d+)%.*?(\d+\.\d+)%.*?(\d+\.\d+)%",
                     txt, re.S)
    if both:
        m["cong_both"] = both.group(1)
        m["cong_v"]    = both.group(2)
        m["cong_h"]    = both.group(3)
    # Alt pattern
    h = re.search(r"H routing.*?(\d+\.\d+)%", txt)
    v = re.search(r"V routing.*?(\d+\.\d+)%", txt)
    if h and "cong_h" not in m:
        m["cong_h"] = h.group(1)
    if v and "cong_v" not in m:
        m["cong_v"] = v.group(1)
    return m


# ---------------------------------------------------------------------------
# POWER
# File: report_power_info.mission.{SCENARIO}.compile_opt.{TIMESTAMP}.rpt
# ---------------------------------------------------------------------------

def parse_power(rpt_dir):
    """Find power report -- handles the mission scenario naming."""
    m = {}
    # Pattern: report_power_info.mission.*.compile_opt.*.rpt
    hits = glob.glob(os.path.join(
        rpt_dir, "report_power_info.mission.*.rpt"))
    if not hits:
        hits = glob.glob(os.path.join(
            rpt_dir, "report_power_info.*.rpt"))
    if not hits:
        return m
    # Use most recently modified
    path = sorted(hits, key=os.path.getmtime)[-1]
    txt = _read(path)
    if not txt:
        return m

    dyn = re.search(
        r"Total Dynamic Power\s*=\s*(\S+)\s*\((\w+)\)", txt)
    if dyn:
        try:
            val  = float(dyn.group(1))
            unit = dyn.group(2)
            conv = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0}
            m["dynamic_mw"] = f"{val * conv.get(unit, 1.0):.4f}"
        except Exception:
            m["dynamic_mw"] = dyn.group(1)

    leak = re.search(r"Cell Leakage Power\s*=\s*(\S+)\s*\((\w+)\)", txt)
    if leak:
        try:
            val  = float(leak.group(1))
            unit = leak.group(2)
            conv = {"uW": 1e-3, "nW": 1e-6, "mW": 1.0}
            m["leakage_mw"] = f"{val * conv.get(unit, 1.0):.4f}"
        except Exception:
            m["leakage_mw"] = leak.group(1)

    # Scenario name from filename
    fname = os.path.basename(path)
    sce_m = re.search(r"report_power_info\.mission\.(.+?)\.compile_opt", fname)
    if sce_m:
        m["power_scenario"] = sce_m.group(1)
    return m


# ---------------------------------------------------------------------------
# DRC
# ---------------------------------------------------------------------------

def parse_drc_from_log(log_path):
    count = 0
    txt = _read(log_path)
    if not txt:
        return "-"
    for line in txt.splitlines():
        if line.startswith("Error:"):
            count += 1
    return str(count) if count > 0 else "0"


def parse_pnr_drc(rpt_dir, stage):
    path = _find_rpt(rpt_dir, stage + ".physical_all_sum")
    if not path:
        path = _find_rpt(rpt_dir, stage + ".drc")
    txt = _read(path)
    if not txt:
        return {}
    m = {}
    hit = re.search(r"Total number of DRCs\s*=\s*(\d+)", txt)
    if hit:
        m["drc_count"] = hit.group(1)
    hit = re.search(r"Short\s*=?\s*(\d+)", txt)
    if hit:
        m["shorts"] = hit.group(1)
    return m


# ---------------------------------------------------------------------------
# MAIN ENTRY POINTS
# ---------------------------------------------------------------------------

def extract_fe_metrics(run_dir, block_name):
    """Extract all FE synthesis metrics for a completed run.
    Called on-demand (MetricWorker) -- NOT during scan."""
    rpt_dir = os.path.join(run_dir, "reports")
    result  = {
        "block":   block_name,
        "run_dir": run_dir,
        "run_name": os.path.basename(run_dir),
    }

    # Area + util + VTH
    cu_path = _find_rpt(rpt_dir, "cell_usage.summary")
    if cu_path:
        area_data = parse_cell_usage(cu_path)
        result["area"] = area_data
    else:
        result["area"] = {}

    # Timing (qor.{BLOCK}.{TIMESTAMP}.rpt)
    qor_path = _find_rpt(rpt_dir, "qor")
    if qor_path:
        qor_data = parse_qor_rpt(qor_path)
        result["timing_raw"] = qor_data
        # R2R for display
        result["r2r_setup"] = qor_data.get("r2r_setup", "-")
        result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")
        result["mbit"]      = qor_data.get("mbit", result["area"].get("mbit", "-"))
        result["cgc_pct"]   = qor_data.get("cgc_pct", "-")
        result["worst_scenario"] = qor_data.get("worst_scenario", "-")
        result["worst_wns"]      = qor_data.get("worst_wns", "-")
        result["worst_tns"]      = qor_data.get("worst_tns", "-")
        result["worst_nvp"]      = qor_data.get("worst_nvp", "-")
    else:
        result["r2r_setup"] = "-"
        result["r2r_hold"]  = "-"
        result["mbit"]      = "-"
        result["cgc_pct"]   = "-"

    # Congestion
    cong_path = _find_rpt(rpt_dir, "congestion")
    if not cong_path:
        cong_path = _find_rpt(rpt_dir, "clock_gating_info.mission")
    if cong_path:
        result["congestion"] = parse_congestion(cong_path)
    else:
        result["congestion"] = {}

    # Power (mission scenario report)
    result["power"] = parse_power(rpt_dir)

    # DRC from compile log
    log = os.path.join(run_dir, "logs", "compile_opt.log")
    result["drc_errors"] = parse_drc_from_log(log)

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
        qor_data = parse_qor_rpt(qor_path)
        result["timing_raw"] = qor_data
        result["r2r_setup"]  = qor_data.get("r2r_setup", "-")
        result["r2r_hold"]   = qor_data.get("r2r_hold",  "-")

    result["drc"] = parse_pnr_drc(rpt_dir, stage_name)

    # Area from qor_area report
    qa_path = _find_rpt(rpt_dir, stage_name + ".qor_sum")
    if qa_path:
        result["area"] = parse_qor_rpt(qa_path)

    return result
