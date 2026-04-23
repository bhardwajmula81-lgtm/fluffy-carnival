# -*- coding: ascii -*-
# metric_extract_v2.py
# Pure local directory extraction. No recursive globbing.
# Bulletproof Regex: Ignores all delimiters (| or :) and directly grabs numbers.

import os
import re
import glob

# ===========================================================================
# STRICT FILE DISCOVERY 
# ===========================================================================
def _find_rpt(rpt_dir, prefix):
    """
    Looks ONLY in the provided rpt_dir. 
    Matches exactly the pattern: prefix.*.rpt (e.g., utilization.BLK_ISP2.123.rpt)
    """
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    pattern = os.path.join(rpt_dir, f"{prefix}.*.rpt")
    hits = glob.glob(pattern)
    
    if not hits:
        pattern = os.path.join(rpt_dir, f"{prefix}*")
        hits = glob.glob(pattern)

    valid_files = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]

    if not valid_files:
        return None

    return sorted(valid_files, key=os.path.getmtime)[-1]

# ===========================================================================
# ADVANCED PARSERS (Bulletproof Regex)
# ===========================================================================
def parse_qor_rpt(filepath):
    result = {
        "r2r_setup": "-", "r2r_hold": "-", "mbit": "-", "tool_version": "-",
        "scenarios": {"setup": {}, "hold": {}}
    }
    if not filepath or not os.path.exists(filepath): return result

    current_mode = "setup"
    in_reg2reg = False
    
    s_wns, s_tns, s_nvp = "-", "-", "-"
    h_wns, h_tns, h_nvp = "-", "-", "-"

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if re.search(r'Design.*?(?:\(Hold\)).*?WNS', line, re.IGNORECASE) or (re.search(r'hold', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE)):
                    current_mode = "hold"
                elif re.search(r'Design.*?WNS', line, re.IGNORECASE) or (re.search(r'setup', line, re.IGNORECASE) and re.search(r'violation|slack', line, re.IGNORECASE)):
                    if "Hold" not in line:
                        current_mode = "setup"
                
                if re.search(r'Timing Path Group\s+\'?REG2REG\'?', line, re.IGNORECASE):
                    in_reg2reg = True
                elif re.search(r'Timing Path Group', line, re.IGNORECASE):
                    in_reg2reg = False
                
                if in_reg2reg:
                    # [^\d\-]+ means "skip anything that isn't a number or a minus sign"
                    m_wns = re.search(r'(?:Critical\s+Path\s+Slack|Worst.*?Violation)[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                    if m_wns:
                        if current_mode == "setup": s_wns = m_wns.group(1)
                        else: h_wns = m_wns.group(1)
                        
                    m_tns = re.search(r'(?:Total\s+Negative\s+Slack|Total.*?Violation)[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                    if m_tns:
                        if current_mode == "setup": s_tns = m_tns.group(1)
                        else: h_tns = m_tns.group(1)
                        
                    m_nvp = re.search(r'(?:No\.\s+of\s+)?Violating Paths[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                    if m_nvp:
                        if current_mode == "setup": s_nvp = m_nvp.group(1)
                        else: h_nvp = m_nvp.group(1)

                m_v = re.search(r'Version[^\d]+([a-zA-Z0-9\.\-]+)', line, re.IGNORECASE)
                if m_v: result["tool_version"] = m_v.group(1)

                m_mbit = re.search(r'MBIT Ratio[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mbit: result["mbit"] = m_mbit.group(1)
                
        if s_wns != "-" or s_tns != "-" or s_nvp != "-":
            result["r2r_setup"] = f"{s_wns}/{s_tns}/{s_nvp}"
        if h_wns != "-" or h_tns != "-" or h_nvp != "-":
            result["r2r_hold"] = f"{h_wns}/{h_tns}/{h_nvp}"

    except: pass
    return result

def parse_area(filepath):
    result = {
        "total_area": "-", "combinational_area": "-", "reg_area": "-",
        "macro_area": "-", "buf_area": "-"
    }
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_tot = re.search(r'Total cell area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_tot and result["total_area"] == "-": result["total_area"] = m_tot.group(1)

                m_comb = re.search(r'Combinational area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_comb and result["combinational_area"] == "-": result["combinational_area"] = m_comb.group(1)

                m_reg = re.search(r'Noncombinational area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_reg and result["reg_area"] == "-": result["reg_area"] = m_reg.group(1)

                m_mac = re.search(r'Macro/Black\s+Box area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mac and result["macro_area"] == "-": result["macro_area"] = m_mac.group(1)

                m_buf = re.search(r'Buf/Inv area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_buf and result["buf_area"] == "-": result["buf_area"] = m_buf.group(1)
    except: pass
    return result

def parse_utilization(filepath):
    result = {
        "total_util": "-", "std_cell_only_util": "-", "memory_util": "-", 
        "std_cell_area": "-", "memory_area": "-", "total_cells_exclude_io": "-"
    }
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Percentage rows
                m_tot = re.search(r'Total Utilization[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_tot and "%" in line: result["total_util"] = m_tot.group(1)
                
                m_std_pct = re.search(r'Standard cell only.*?utilization[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_std_pct and "%" in line: result["std_cell_only_util"] = m_std_pct.group(1)
                
                m_mem_pct = re.search(r'Memory utilization[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mem_pct and "%" in line: result["memory_util"] = m_mem_pct.group(1)

                # Area rows (Grabs everything exactly from the screenshot)
                m_std_area = re.search(r'Standard cell.*?area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_std_area and "%" not in line: result["std_cell_area"] = m_std_area.group(1)

                m_mem_area = re.search(r'Memory area[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mem_area and "%" not in line: result["memory_area"] = m_mem_area.group(1)
                
                # Instance Count from Table
                m_cells = re.search(r'Total cells \(exclude IO\)[^\d\-]+(\d+)', line, re.IGNORECASE)
                if m_cells: result["total_cells_exclude_io"] = m_cells.group(1)
    except: pass
    return result

def parse_vth_from_cell_usage(filepath):
    result = {"vth_raw": {}, "vth_totals": {}, "total_cells": 0, "instance_count": "-"}
    if not filepath or not os.path.exists(filepath): return result

    in_target_block = False
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_inst = re.search(r'Instance count[^\d]+(\d+)', line, re.IGNORECASE)
                if m_inst: result["instance_count"] = m_inst.group(1)

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
                m_tot = re.search(r'Total Power[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_tot: result["total_power"] = m_tot.group(1)
                m_leak = re.search(r'Cell Leakage Power[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_leak: result["leakage_power"] = m_leak.group(1)
    except: pass
    return result

def parse_clock_gating(filepath):
    if not filepath or not os.path.exists(filepath): return "-"
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m = re.search(r'CGC Ratio[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m: return m.group(1)
    except: pass
    return "-"

def parse_congestion(filepath):
    result = {"max_h": "-", "max_v": "-"}
    if not filepath or not os.path.exists(filepath): return result
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m_mh = re.search(r'Max H routing congestion[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mh: result["max_h"] = m_mh.group(1)
                m_mv = re.search(r'Max V routing congestion[^\d\-]+([-\.\d]+)', line, re.IGNORECASE)
                if m_mv: result["max_v"] = m_mv.group(1)
    except: pass
    return result

# ===========================================================================
# MAIN EXTRACTION WRAPPERS (Fully Mapped to UI)
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")

    qor_path = _find_rpt(rpt_dir, "qor")
    qor_data = parse_qor_rpt(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")
    result["mbit"] = qor_data.get("mbit", "-")
    result["tool_version"] = qor_data.get("tool_version", "-")
    result["scenarios"] = qor_data.get("scenarios", {"setup": {}, "hold": {}})

    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)

    area_path = _find_rpt(rpt_dir, "area")
    area_data = parse_area(area_path)
    result["area"] = area_data 

    # UTILIZATION (Injects Area & Counts directly into the 'area' dictionary)
    util_path = _find_rpt(rpt_dir, "utilization")
    util_data = parse_utilization(util_path)
    result["util"] = util_data

    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    
    if util_data.get("total_cells_exclude_io", "-") != "-":
        result["area"]["total_count"] = util_data["total_cells_exclude_io"]
    else:
        result["area"]["total_count"] = "-"

    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    if not cell_path: cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    
    result["vth_raw"] = vth_data.get("vth_raw", {})
    result["vth_totals"] = vth_data.get("vth_totals", {})

    # Fallback to cell_usage only if utilization failed
    if result["area"]["total_count"] == "-":
        if vth_data.get("instance_count", "-") != "-":
            result["area"]["total_count"] = vth_data["instance_count"]
        elif vth_data.get("total_cells", 0) > 0:
            result["area"]["total_count"] = str(vth_data["total_cells"])

    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    result = {"stage": stage_name, "run_dir": run_dir}
    
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)

    qor_path = _find_rpt(rpt_dir, "qor")
    qor_data = parse_qor_rpt(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")
    result["mbit"] = qor_data.get("mbit", "-")
    result["tool_version"] = qor_data.get("tool_version", "-")
    result["scenarios"] = qor_data.get("scenarios", {"setup": {}, "hold": {}})

    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)

    area_path = _find_rpt(rpt_dir, "area")
    area_data = parse_area(area_path)
    result["area"] = area_data

    util_path = _find_rpt(rpt_dir, "utilization")
    util_data = parse_utilization(util_path)
    result["util"] = util_data

    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    
    if util_data.get("total_cells_exclude_io", "-") != "-":
        result["area"]["total_count"] = util_data["total_cells_exclude_io"]
    else:
        result["area"]["total_count"] = "-"

    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    if not cell_path: cell_path = _find_rpt(rpt_dir, "cell_usage")
    vth_data = parse_vth_from_cell_usage(cell_path)
    
    result["vth_raw"] = vth_data.get("vth_raw", {})
    result["vth_totals"] = vth_data.get("vth_totals", {})

    if result["area"]["total_count"] == "-":
        if vth_data.get("instance_count", "-") != "-":
            result["area"]["total_count"] = vth_data["instance_count"]
        elif vth_data.get("total_cells", 0) > 0:
            result["area"]["total_count"] = str(vth_data["total_cells"])

    power_path = _find_rpt(rpt_dir, "report_power_info")
    result["power"] = parse_power(power_path)

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    return result
