# -*- coding: ascii -*-
# metric_extract_v2.py
# Uses the user's custom, tested regex logic for exact parsing.
# Formatted to return nested dictionaries matching the PyQt5 Dashboard.

import os
import re
import glob

# ===========================================================================
# FILE DISCOVERY HELPER
# ===========================================================================
def _find_rpt(rpt_dir, prefix):
    """Finds the latest report file matching the prefix in the directory."""
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    # Matches files like area.*.rpt or utilization.*.rpt
    pattern = os.path.join(rpt_dir, f"{prefix}*")
    hits = glob.glob(pattern)
    
    valid_files = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]

    if not valid_files:
        return None

    # Return the most recently modified file
    return max(valid_files, key=os.path.getmtime)

# ===========================================================================
# CUSTOM PARSERS (Adapted from user script)
# ===========================================================================
def parse_area(file_path):
    result = {"total_count": "-", "total_area": "-"}
    if not file_path or not os.path.exists(file_path): return result
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            cells = re.search(r"Number of cells:\s+(\d+)", content)
            area = re.search(r"Total cell area:\s+([\d.]+)", content)
            
            if cells: result["total_count"] = cells.group(1)
            if area: result["total_area"] = area.group(1)
    except: pass
    return result

def parse_utilization(file_path):
    result = {"std_cell_area": "-", "memory_area": "-", "macro_area": "-"}
    if not file_path or not os.path.exists(file_path): return result
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            std_cell = re.search(r"^\s*std_cell\(\+headbuf\+epbuf\)\s+\d+\s+([\d.]+)", content, re.MULTILINE)
            memory = re.search(r"^\s*memory_cell\s+\d+\s+([\d.]+)", content, re.MULTILINE)
            macro = re.search(r"^\s*macro_cell\s+\d+\s+([\d.]+)", content, re.MULTILINE)
            
            if std_cell: result["std_cell_area"] = std_cell.group(1)
            if memory: result["memory_area"] = memory.group(1)
            if macro: result["macro_area"] = macro.group(1)
    except: pass
    return result

def parse_cell_usage(file_path):
    result = {"vth_totals": {}}
    if not file_path or not os.path.exists(file_path): return result
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lvt_inst, rvt_inst = 0.0, 0.0
            lvt_keys = ["LVT", "LVT_LLP", "LVT_L30L34"]
            rvt_keys = ["RVT", "RVT_LLP", "RVT_L30L34"]
            
            in_all_cells = False

            for line in f:
                if "1-1. For all Cells" in line:
                    in_all_cells = True
                elif "1-2." in line:
                    break 
                
                if in_all_cells and "|" in line:
                    cols = [c.strip() for c in line.split('|')]
                    if len(cols) >= 6:
                        vth_type = cols[1]
                        try:
                            inst_pct = float(cols[3].replace('%', ''))
                            if vth_type in lvt_keys: lvt_inst += inst_pct
                            elif vth_type in rvt_keys: rvt_inst += inst_pct
                        except ValueError: pass
            
            # Formatted exactly as expected by your dashboard
            result["vth_totals"]["LVT*"] = f"{lvt_inst:.2f}%"
            result["vth_totals"]["RVT*"] = f"{rvt_inst:.2f}%"
    except: pass
    return result

def parse_qor(file_path):
    result = {"r2r_setup": "-", "r2r_hold": "-"}
    if not file_path or not os.path.exists(file_path): return result
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
            def get_r2r_data(section_name):
                section = re.search(f"{section_name}.*?(?=Report :|$)", content, re.DOTALL)
                if not section: return "-"
                sec_text = section.group(0)
                
                wns = re.search(r"WNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
                tns = re.search(r"TNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
                num = re.search(r"NUM\s+([-\d.]+)\s+([-\d.]+)", sec_text)
                
                if wns and tns and num:
                    return f"{wns.group(2)}/{tns.group(2)}/{num.group(2)}"
                return "-"

            result["r2r_setup"] = get_r2r_data("Setup violations")
            result["r2r_hold"] = get_r2r_data("Hold violations")
    except: pass
    return result

def parse_clock_gating(file_path):
    if not file_path or not os.path.exists(file_path): return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            match = re.search(r"Number of Gated registers\s+\|\s+\d+\s+\(([\d.]+)%\)", content)
            if match: return match.group(1)
    except: pass
    return "-"

def parse_multibit(file_path):
    if not file_path or not os.path.exists(file_path): return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            match = re.search(r"Flip-flop cells banking ratio \(\(C\)/\s*\(\s*A\s*\+\s*C\s*\)\):\s+([\d.]+)%", content)
            if match: return match.group(1)
    except: pass
    return "-"

def parse_congestion(file_path):
    result = {"max_h": "-", "max_v": "-", "both": "-"}
    if not file_path or not os.path.exists(file_path): return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            both = re.search(r"Both Dirs\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            h_route = re.search(r"H routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            v_route = re.search(r"V routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            
            if both: result["both"] = both.group(1)
            if h_route: result["max_h"] = h_route.group(1)
            if v_route: result["max_v"] = v_route.group(1)
    except: pass
    return result

# ===========================================================================
# MAIN DASHBOARD EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    """Called by the dashboard for FE runs."""
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")

    # QoR (Setup/Hold)
    qor_path = _find_rpt(rpt_dir, "qor")
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")

    # CGC & MBIT
    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)
    
    mbit_path = _find_rpt(rpt_dir, "multibit_banking_ratio")
    result["mbit"] = parse_multibit(mbit_path)

    # Area & Instance Count
    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    # Utilization (Std cell, memory, macro area)
    util_path = _find_rpt(rpt_dir, "utilization")
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    result["area"]["macro_area"] = util_data.get("macro_area", "-")

    # Vth Usage
    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    vth_data = parse_cell_usage(cell_path)
    result["vth_totals"] = vth_data.get("vth_totals", {})

    # Congestion
    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    # Empty stubs to prevent UI errors if it looks for these
    result["power"] = {"total_power": "-", "leakage_power": "-"}
    result["tool_version"] = "-"

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    """Called by the dashboard for BE runs."""
    result = {"stage": stage_name, "run_dir": run_dir}
    
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports", stage_name)

    qor_path = _find_rpt(rpt_dir, "qor")
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")

    cgc_path = _find_rpt(rpt_dir, "clock_gating_info")
    result["cgc"] = parse_clock_gating(cgc_path)
    
    mbit_path = _find_rpt(rpt_dir, "multibit_banking_ratio")
    result["mbit"] = parse_multibit(mbit_path)

    area_path = _find_rpt(rpt_dir, "area")
    result["area"] = parse_area(area_path)
    
    util_path = _find_rpt(rpt_dir, "utilization")
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    result["area"]["macro_area"] = util_data.get("macro_area", "-")

    cell_path = _find_rpt(rpt_dir, "cell_usage.summary")
    vth_data = parse_cell_usage(cell_path)
    result["vth_totals"] = vth_data.get("vth_totals", {})

    cong_path = _find_rpt(rpt_dir, "congestion")
    result["congestion"] = parse_congestion(cong_path)

    result["power"] = {"total_power": "-", "leakage_power": "-"}
    result["tool_version"] = "-"

    return result
