# -*- coding: ascii -*-
# metric_extract_v2.py
# Fully native QoR metric parser. Replaces external summary.py calls.
# Extracts advanced scenario timing, Vth distributions, clock latency, and area.

import os
import re
import glob

# ===========================================================================
# FILE DISCOVERY HELPER
# ===========================================================================
def _find_rpt(rpt_dir, search_string):
    """
    Hunts down a report file robustly.
    If search_string has a '*', it uses it exactly.
    Otherwise, it assumes the project naming convention: {prefix}.*.rpt
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    # Primary search
    if "*" in search_string:
        pattern = os.path.join(rpt_dir, search_string)
    else:
        pattern = os.path.join(rpt_dir, f"{search_string}.*.rpt")
        
    hits = glob.glob(pattern)
    
    # Fallback search (find the string anywhere in the file name)
    if not hits and "*" not in search_string:
        pattern = os.path.join(rpt_dir, f"*{search_string}*")
        hits = glob.glob(pattern)

    # Ignore logs
    valid_files = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]

    if not valid_files:
        return None

    # Return the newest file if there are multiples
    return sorted(valid_files, key=os.path.getmtime)[-1]

# ===========================================================================
# ADVANCED PARSERS
# ===========================================================================
def parse_qor_rpt(filepath):
    """
    Extracts global WNS/TNS AND advanced Scenario/Path Group nested dictionaries.
    Accounts for changing vocabulary (Critical Path Slack vs WNS).
    """
    result = {
        "runtime": "-", "r2r_setup": "-", "r2r_hold": "-",
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
                # 1. Global TreeView Matchers (Accounts for missing "Setup" word)
                m_rt = re.search(r'Design\s*(?:\(Setup\))?\s*WNS:\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rt: result["r2r_setup"] = m_rt.group(1)

                m_rh = re.search(r'Design\s*\(Hold\)\s*WNS:\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rh: result["r2r_hold"] = m_rh.group(1)

                m_v = re.search(r'Version:\s+(\S+)', line, re.IGNORECASE)
                if m_v: result["tool_version"] = m_v.group(1)

                # 2. Advanced Scenario / Path Group Matchers
                m_scen = re.search(r'Scenarios?:\s*(\S+)', line, re.IGNORECASE)
                if m_scen: current_scen = m_scen.group(1)

                m_pg = re.search(r'Timing Path Group\s+\'?([^\'\s]+)\'?', line, re.IGNORECASE)
                if m_pg: current_pg = m_pg.group(1)

                # Track context (Setup vs Hold blocks)
                if re.search(r'hold', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE): 
                    current_mode = "hold"
                elif re.search(r'setup', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE): 
                    current_mode = "setup"

                # Ensure dictionary structure exists
                if current_scen not in result["scenarios"][current_mode]:
                    result["scenarios"][current_mode][current_scen] = {}
                if current_pg not in result["scenarios"][current_mode][current_scen]:
                    result["scenarios"][current_mode][current_scen][current_pg] = {'wns': '-', 'tns': '-', 'nvp': '-', 'levels': '-'}

                # 3. Extract Values (Robust Vocabulary)
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

                # Misc QoR Matchers
                m_mbit = re.search(r'MBIT Ratio\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mbit: result["mbit"] = m_mbit.group(1)
    except: 
        pass

    return result

def parse_latency_skew(filepath):
    """Finds Max Latency and Global Skew from clock reports."""
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
    """Extracts Area Metrics natively."""
    result = {
        "total_area": "-", "combinational_area": "-", "reg_area": "-",
        "macro_area": "-", "buf_area": "-"
    }
    if not filepath or not os.path.exists(filepath): 
        return result
        
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total cell area:\s*([-\.\d]+)', line)
                if m_tot: result["total_area"] = m_tot.group(1)

                m_comb = re.search(r'Combinational area:\s*([-\.\d]+)', line)
                if m_comb: result["combinational_area"] = m_comb.group(1)

                m_reg = re.search(r'Noncombinational area:\s*([-\.\d]+)', line)
                if m_reg: result["reg_area"] = m_reg.group(1)

                m_mac = re.search(r'Macro/Black Box area:\s*([-\.\d]+)', line)
                if m_mac: result["macro_area"] = m_mac.group(1)

                m_buf = re.search(r'Buf/Inv area:\s*([-\.\d]+)', line)
                if m_buf: result["buf_area"] = m_buf.group(1)
    except: 
        pass
    return result

def parse_vth_from_cell_usage(filepath):
    """Calculates VT distributions securely (avoids double counting)."""
    result = {"vth_raw": {}, "vth_totals": {}, "total_cells": 0}
    if not filepath or not os.path.exists(filepath): 
        return result

    in_target_block = False
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Prevent double counting: reset dict when we hit a table header
                if "Total cells:" in line or "Total standard cells:" in line:
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
                        # Use direct assignment to overwrite duplicates if any slip through
                        result["vth_raw"][vt_type] = count
                        result["total_cells"] += count
                    elif not line.strip() and result["total_cells"] > 0:
                        # Stop parsing once the block is done
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
    """Parses utilization percentages, accounting for | or : delimiters."""
    result = {"total_util": "-", "std_cell_util": "-", "memory_util": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total Utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_util"] = m_tot.group(1)
                m_std = re.search(r'Standard cell only utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_std: result["std_cell_util"] = m_std.group(1)
                m_mem = re.search(r'Memory utilization\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mem: result["memory_util"] = m_mem.group(1)
    except: pass
    return result

def parse_clock_gating(filepath):
    """Extracts CGC Ratio securely."""
    if not filepath or not os.path.exists(filepath): return "-"
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m = re.search(r'CGC Ratio\s*[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m: return m.group(1)
    except: pass
    return "-"

def parse_drc_from_log(filepath):
    """Finds maximum DRC errors directly from the compile/PNR log."""
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
# MAIN EXTRACTION WRAPPERS (Called by workers.py)
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    """Pulls all Native data for a Synthesis (FE) Run. Accepts source to prevent crashes."""
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")

    # Timing & Scenarios
    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    # Area & VTH
    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    cell_path = _find_rpt(rpt_dir, "cell_usage")
    result["vth"] = parse_vth_from_cell_usage(cell_path)

    # Power & Congestion
    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    # Clock & Utilization Additions
    clk_path = _find_rpt(rpt_dir, "clock")
    result["clock"] = parse_latency_skew(clk_path)

    util_path = _find_rpt(rpt_dir, "utilization")
    result["utilization"] = parse_utilization(util_path)

    cgc_path = _find_rpt(rpt_dir, "clock_gating")
    result["cgc_pct"] = parse_clock_gating(cgc_path)

    # DRC 
    log = os.path.join(run_dir, "logs", "compile_opt.log")
    result["drc_errors"] = parse_drc_from_log(log)

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    """Pulls all Native data for a specific PNR Stage."""
    result = {"stage": stage_name, "run_dir": run_dir}
    
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
        log = os.path.join(run_dir, "logs", f"{stage_name}.log")
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)
        log = os.path.join(run_dir, stage_name, "logs", f"{stage_name}.log")

    # Timing & Scenarios
    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    # Area & VTH
    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    cell_path = _find_rpt(rpt_dir, "cell_usage")
    result["vth"] = parse_vth_from_cell_usage(cell_path)

    # Power & Congestion
    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    # Clock & Utilization Additions
    clk_path = _find_rpt(rpt_dir, "clock")
    result["clock"] = parse_latency_skew(clk_path)

    util_path = _find_rpt(rpt_dir, "utilization")
    result["utilization"] = parse_utilization(util_path)

    cgc_path = _find_rpt(rpt_dir, "clock_gating")
    result["cgc_pct"] = parse_clock_gating(cgc_path)

    result["drc_errors"] = parse_drc_from_log(log)

    return result