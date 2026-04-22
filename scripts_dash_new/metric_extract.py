# -*- coding: ascii -*-
# metric_extract_v2.py
# Pure extraction. NO deep globbing. Exact nested dictionary structure restored.

import os
import re
import glob

# ===========================================================================
# STRICT FILE DISCOVERY (No deep globbing, exact folder only)
# ===========================================================================
def _find_rpt(rpt_dir, prefix):
    """
    Looks ONLY in the provided rpt_dir. 
    Matches exactly the pattern from your images: prefix.BLOCK.TIMESTAMP.rpt
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    # Matches files like area.BLK_ISP2.20260416_1633.rpt
    pattern = os.path.join(rpt_dir, f"{prefix}.*.rpt")
    hits = glob.glob(pattern)
    
    # Fallback just in case it doesn't have .rpt extension
    if not hits:
        pattern = os.path.join(rpt_dir, f"{prefix}*")
        hits = glob.glob(pattern)

    valid_files = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]

    if not valid_files:
        return None

    return sorted(valid_files, key=os.path.getmtime)[-1]

# ===========================================================================
# ADVANCED PARSERS
# ===========================================================================
def parse_qor_rpt(filepath):
    result = {
        "r2r_setup": "-", "r2r_hold": "-", "mbit": "-", "tool_version": "-",
        "scenarios": {"setup": {}, "hold": {}}
    }
    if not filepath or not os.path.exists(filepath): return result

    current_scen, current_pg, current_mode = "default", "default", "setup"

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_rt = re.search(r'Design.*?(?:\(Setup\))?.*?WNS.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rt and result["r2r_setup"] == "-": result["r2r_setup"] = m_rt.group(1)

                m_rh = re.search(r'Design.*?(?:\(Hold\)).*?WNS.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_rh and result["r2r_hold"] == "-": result["r2r_hold"] = m_rh.group(1)

                m_v = re.search(r'Version.*?[:|=]\s+(\S+)', line, re.IGNORECASE)
                if m_v: result["tool_version"] = m_v.group(1)

                m_scen = re.search(r'Scenarios?.*?[:|=]\s*(\S+)', line, re.IGNORECASE)
                if m_scen: 
                    current_scen = m_scen.group(1)
                    current_mode = "setup"

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

                m_wns = re.search(r'(?:WNS|Worst\s+[sS]etup\s+Violation|Worst\s+[hH]old\s+Violation|Critical\s+Path\s+Slack).*?[:\|]\s*([-\.\d]+)', line)
                if m_wns and "Design" not in line:
                    result["scenarios"][current_mode][current_scen][current_pg]['wns'] = m_wns.group(1)

                m_tns = re.search(r'(?:TNS|Total\s+Negative\s+Slack|Total\s+[sS]etup\s+Violation|Total\s+[hH]old\s+Violation).*?[:\|]\s*([-\.\d]+)', line)
                if m_tns and "Design" not in line:
                    result["scenarios"][current_mode][current_scen][current_pg]['tns'] = m_tns.group(1)

                m_nvp = re.search(r'(?:No\.\s+of\s+)?Violating Paths.*?[:\|]\s*([-\.\d]+)', line)
                if m_nvp:
                    result["scenarios"][current_mode][current_scen][current_pg]['nvp'] = m_nvp.group(1)

                m_lvl = re.search(r'Levels of Logic.*?[:\|]\s*([-\.\d]+)', line)
                if m_lvl:
                    result["scenarios"][current_mode][current_scen][current_pg]['levels'] = m_lvl.group(1)

                m_mbit = re.search(r'MBIT Ratio.*?[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mbit: result["mbit"] = m_mbit.group(1)
    except: pass
    return result

def parse_area(filepath):
    result = {
        "total_area": "-", "combinational_area": "-", "reg_area": "-",
        "macro_area": "-", "buf_area": "-", "total_count": "-"
    }
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total cell area.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot and result["total_area"] == "-": result["total_area"] = m_tot.group(1)

                m_comb = re.search(r'Combinational.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_comb and result["combinational_area"] == "-": result["combinational_area"] = m_comb.group(1)

                m_reg = re.search(r'Noncombinational.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_reg and result["reg_area"] == "-": result["reg_area"] = m_reg.group(1)

                m_mac = re.search(r'Macro/Black\s+Box.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mac and result["macro_area"] == "-": result["macro_area"] = m_mac.group(1)

                m_buf = re.search(r'Buf/Inv.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_buf and result["buf_area"] == "-": result["buf_area"] = m_buf.group(1)
    except: pass
    return result

def parse_vth_from_cell_usage(filepath):
    result = {"vth_raw": {}, "vth_totals": {}, "total_cells": 0}
    if not filepath or not os.path.exists(filepath): return result

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
    except: pass
    return result

def parse_power(filepath):
    result = {"total_power": "-", "leakage_power": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total Power.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_power"] = m_tot.group(1)
                m_leak = re.search(r'Cell Leakage Power.*?[:|=]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_leak: result["leakage_power"] = m_leak.group(1)
    except: pass
    return result

def parse_utilization(filepath):
    result = {"total_util": "-", "std_cell_only_util": "-", "memory_util": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total Utilization.*?[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_util"] = m_tot.group(1)
                m_std = re.search(r'Standard cell only.*?[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_std: result["std_cell_only_util"] = m_std.group(1)
                m_mem = re.search(r'Memory utilization.*?[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m_mem: result["memory_util"] = m_mem.group(1)
    except: pass
    return result

def parse_clock_gating(filepath):
    if not filepath or not os.path.exists(filepath): return "-"
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m = re.search(r'CGC Ratio.*?[:\|]\s*([-\.\d]+)', line, re.IGNORECASE)
                if m: return m.group(1)
    except: pass
    return "-"

# ===========================================================================
# MAIN EXTRACTION WRAPPERS (Strictly Nested output)
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")

    # 1. Timing (QoR)
    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    # 2. Area
    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    # 3. Cell Usage / VTH (Matches exactly your image: cell_usage.summary)
    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    if not cell_path: cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    
    # UI requires these exact keys for VTH pie charts
    result["vth_raw"] = vth_data.get("vth_raw", {})
    result["vth_totals"] = vth_data.get("vth_totals", {})
    if vth_data.get("total_cells", 0) > 0:
        result["area"]["total_count"] = str(vth_data["total_cells"])

    # 4. Power
    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    # 5. Utilization
    util_path = _find_rpt(rpt_dir, "utilization")
    result["util"] = parse_utilization(util_path)

    # 6. CGC (Matches exactly your image: clock_gating_info)
    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    result = {"stage": stage_name, "run_dir": run_dir}
    
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)

    qor_path = _find_rpt(rpt_dir, "qor")
    result.update(parse_qor_rpt(qor_path))

    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    if not cell_path: cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    
    result["vth_raw"] = vth_data.get("vth_raw", {})
    result["vth_totals"] = vth_data.get("vth_totals", {})
    if vth_data.get("total_cells", 0) > 0:
        result["area"]["total_count"] = str(vth_data["total_cells"])

    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    util_path = _find_rpt(rpt_dir, "utilization")
    result["util"] = parse_utilization(util_path)

    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)

    return result
