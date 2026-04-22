# -*- coding: ascii -*-
# metric_extract_v2.py
# Fully native QoR metric parser. Replaces external summary.py calls.
# UI-Synchronized: Keys exactly match PyQt5 expected dictionary bindings.

import os
import re
import glob

# ===========================================================================
# AGGRESSIVE FILE DISCOVERY HELPER
# ===========================================================================
def _find_rpt(rpt_dir, search_string):
    """
    Hunts down a report file using multi-depth searching.
    Guarantees file is found whether in base dir or hidden in subfolders.
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    # Check 4 different pathing variations to defeat NFS hiding
    patterns = [
        os.path.join(rpt_dir, f"{search_string}.*.rpt"),
        os.path.join(rpt_dir, f"*{search_string}*.rpt"),
        os.path.join(rpt_dir, "**", f"{search_string}.*.rpt"),
        os.path.join(rpt_dir, "**", f"*{search_string}*.rpt")
    ]
    
    hits = []
    for p in patterns:
        hits.extend(glob.glob(p, recursive=True))

    valid_files = [f for f in set(hits) if os.path.isfile(f) and not f.endswith(".log")]

    if not valid_files:
        return None

    # Return the newest file
    return sorted(valid_files, key=os.path.getmtime)[-1]

# ===========================================================================
# ADVANCED PARSERS
# ===========================================================================
def parse_qor_rpt(filepath):
    result = {
        "runtime": "-", "r2r_setup": "-", "setup_wns": "-", 
        "r2r_hold": "-", "hold_wns": "-", "setup_tns": "-", "hold_tns": "-",
        "mbit": "-", "tool_version": "-",
        "scenarios": {"setup": {}, "hold": {}}
    }
    if not filepath or not os.path.exists(filepath): 
        return result

    current_scen = "default"
    current_pg = "default"
    current_mode = "setup"

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Global Matchers
                m_rt = re.search(r'Design\s*(?:\(Setup\))?\s*WNS:\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rt and result["r2r_setup"] == "-": 
                    result["r2r_setup"] = m_rt.group(1)
                    result["setup_wns"] = m_rt.group(1)

                m_rh = re.search(r'Design\s*\(Hold\)\s*WNS:\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rh and result["r2r_hold"] == "-": 
                    result["r2r_hold"] = m_rh.group(1)
                    result["hold_wns"] = m_rh.group(1)

                m_v = re.search(r'Version:\s+(\S+)', line, re.IGNORECASE)
                if m_v: result["tool_version"] = m_v.group(1)

                # Scenario Mode Tracking
                m_scen = re.search(r'Scenarios?:\s*(\S+)', line, re.IGNORECASE)
                if m_scen: 
                    current_scen = m_scen.group(1)
                    current_mode = "setup" # Reset mode for new scenario

                m_pg = re.search(r'Timing Path Group\s+\'?([^\'\s]+)\'?', line, re.IGNORECASE)
                if m_pg: current_pg = m_pg.group(1)

                if re.search(r'hold', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE): 
                    current_mode = "hold"
                elif re.search(r'setup', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE): 
                    current_mode = "setup"

                if current_scen not in result["scenarios"][current_mode]:
                    result["scenarios"][current_mode][current_scen] = {}
                if current_pg not in result["scenarios"][current_mode][current_scen]:
                    result["scenarios"][current_mode][current_scen][current_pg] = {'wns': '-', 'tns': '-', 'nvp': '-', 'levels': '-'}

                # Dictionary Population
                m_wns = re.search(r'(?:WNS|Worst\s+[sS]etup\s+Violation|Worst\s+[hH]old\s+Violation|Critical\s+Path\s+Slack)\s*[:\|]\s*([-\.\d]+)', line)
                if m_wns and "Design" not in line:
                    result["scenarios"][current_mode][current_scen][current_pg]['wns'] = m_wns.group(1)

                m_tns = re.search(r'(?:TNS|Total\s+Negative\s+Slack|Total\s+[sS]etup\s+Violation|Total\s+[hH]old\s+Violation)\s*[:\|]\s*([-\.\d]+)', line)
                if m_tns and "Design" not in line:
                    result["scenarios"][current_mode][current_scen][current_pg]['tns'] = m_tns.group(1)

                m_nvp = re.search(r'(?:No\.\s+of\s+)?Violating Paths\s*[:\|]\s*([-\.\d]+)', line)
                if m_nvp:
                    result["scenarios"][current_mode][current_scen][current_pg]['nvp'] = m_nvp.group(1)

                m_lvl = re.search(r'Levels of Logic\s*[:\|]\s*([-\.\d]+)', line)
                if m_lvl:
                    result["scenarios"][current_mode][current_scen][current_pg]['levels'] = m_lvl.group(1)

                m_mbit = re.search(r'MBIT Ratio\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mbit: result["mbit"] = m_mbit.group(1)
    except: 
        pass

    return result

def parse_latency_skew(filepath):
    result = {"max_latency": "-", "global_skew": "-"}
    if not filepath or not os.path.exists(filepath): 
        return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.read().splitlines()
            for i, line in enumerate(lines):
                if re.search(r'\s*Mode:\s*(mission|func|test)', line, re.IGNORECASE):
                    if i + 1 < len(lines):
                        parts = lines[i+1].split()
                        if len(parts) >= 9:
                            result["max_latency"] = parts[7]
                            result["global_skew"] = parts[8]
                            break
    except: 
        pass
    return result

def parse_area(filepath):
    # Expanded keys to ensure the UI finds what it needs
    result = {
        "total_area": "-", "combinational_area": "-", "reg_area": "-",
        "macro_area": "-", "buf_area": "-", "comb_area": "-", "seq_area": "-",
        "buf_inv_area": "-", "total_count": "-"
    }
    if not filepath or not os.path.exists(filepath): 
        return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total cell area\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot and result["total_area"] == "-": result["total_area"] = m_tot.group(1)

                m_comb = re.search(r'Combinational\s*(?:cell)?\s*area\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_comb and result["combinational_area"] == "-": 
                    result["combinational_area"] = m_comb.group(1)
                    result["comb_area"] = m_comb.group(1)

                m_reg = re.search(r'Noncombinational\s*(?:cell)?\s*area\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_reg and result["reg_area"] == "-": 
                    result["reg_area"] = m_reg.group(1)
                    result["seq_area"] = m_reg.group(1)

                m_mac = re.search(r'Macro/Black\s+Box\s+area\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mac and result["macro_area"] == "-": result["macro_area"] = m_mac.group(1)

                m_buf = re.search(r'Buf/Inv\s+area\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_buf and result["buf_area"] == "-": 
                    result["buf_area"] = m_buf.group(1)
                    result["buf_inv_area"] = m_buf.group(1)
    except: 
        pass
    return result

def parse_vth_from_cell_usage(filepath):
    result = {"vth_raw": {}, "vth_totals": {}, "total_cells": 0}
    if not filepath or not os.path.exists(filepath): 
        return result

    in_target_block = False
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if "total cells:" in line.lower() or "total standard cells:" in line.lower():
                    result["vth_raw"].clear()
                    result["total_cells"] = 0
                    in_target_block = True
                    continue

                if in_target_block:
                    m_vt = re.search(r'^\s*(LVT|RVT|SLVT|ULVT|SVT|HVT)\b', line, re.IGNORECASE)
                    m_cnt = re.search(r'\s+(\d+)\s+[\d\.]+\s*%', line)
                    
                    if m_vt and m_cnt:
                        vt_type = m_vt.group(1).upper()
                        count = int(m_cnt.group(1))
                        result["vth_raw"][vt_type] = count
                        result["total_cells"] += count
                    elif not line.strip() and result["total_cells"] > 0:
                        in_target_block = False

        if result["total_cells"] > 0:
            for k, v in result["vth_raw"].items():
                pct = (v / result["total_cells"]) * 100
                result["vth_totals"][k] = f"{v} ({pct:.2f}%)"
    except: 
        pass
    return result

def parse_power(filepath):
    result = {"total_power": "-", "leakage_power": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total Power\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_power"] = m_tot.group(1)

                m_leak = re.search(r'Cell Leakage Power\s*[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_leak: result["leakage_power"] = m_leak.group(1)
    except: pass
    return result

def parse_congestion(filepath):
    result = {"max_h": "-", "max_v": "-", "over_90_h": "-", "over_90_v": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_mh = re.search(r'Max H routing congestion\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mh: result["max_h"] = m_mh.group(1)
                m_mv = re.search(r'Max V routing congestion\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mv: result["max_v"] = m_mv.group(1)
    except: pass
    return result

def parse_utilization(filepath):
    # Renamed UI keys to match expected dashboard format
    result = {"total_util": "-", "std_cell_only_util": "-", "memory_util": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total Utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_util"] = m_tot.group(1)
                m_std = re.search(r'Standard cell only utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_std: result["std_cell_only_util"] = m_std.group(1)
                m_mem = re.search(r'Memory utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mem: result["memory_util"] = m_mem.group(1)
    except: pass
    return result

def parse_clock_gating(filepath):
    if not filepath or not os.path.exists(filepath): return "-"
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m = re.search(r'CGC Ratio\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m: return m.group(1)
    except: pass
    return "-"

def parse_drc_from_log(filepath):
    errors = "0"
    if not filepath or not os.path.exists(filepath): return "-"
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_drc = re.search(r'Total\s+DRC\s+errors\s*:\s*(\d+)', line, re.IGNORECASE)
                if m_drc: errors = m_drc.group(1)
    except: pass
    return errors

# ===========================================================================
# MAIN EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")

    # Timing
    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    # Area
    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    # VTH (UI expects this to be updated into the ROOT, not nested inside "vth")
    cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    result.update(vth_data) 
    
    # UI expects total_count to be inside the Area dictionary
    if "total_cells" in vth_data:
        result["area"]["total_count"] = vth_data["total_cells"]

    # Power
    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    # Congestion
    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    # Clock
    clk_path = _find_rpt(rpt_dir, "clock")
    result["clock"] = parse_latency_skew(clk_path)

    # Utilization (UI expects "util")
    util_path = _find_rpt(rpt_dir, "utilization")
    result["util"] = parse_utilization(util_path)

    # CGC (UI expects "cgc")
    cgc_path = _find_rpt(rpt_dir, "clock_gating")
    result["cgc"] = parse_clock_gating(cgc_path)

    # DRC
    log = os.path.join(run_dir, "logs", "compile_opt.log")
    result["drc_errors"] = parse_drc_from_log(log)

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    result = {"stage": stage_name, "run_dir": run_dir}
    
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
        log = os.path.join(run_dir, "logs", f"{stage_name}.log")
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)
        log = os.path.join(run_dir, stage_name, "logs", f"{stage_name}.log")

    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    result.update(vth_data)
    if "total_cells" in vth_data:
        result["area"]["total_count"] = vth_data["total_cells"]

    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    clk_path = _find_rpt(rpt_dir, "clock")
    result["clock"] = parse_latency_skew(clk_path)

    util_path = _find_rpt(rpt_dir, "utilization")
    result["util"] = parse_utilization(util_path)

    cgc_path = _find_rpt(rpt_dir, "clock_gating")
    result["cgc"] = parse_clock_gating(cgc_path)

    result["drc_errors"] = parse_drc_from_log(log)

    return result