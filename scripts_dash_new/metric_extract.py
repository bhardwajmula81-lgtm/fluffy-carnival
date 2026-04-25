# -*- coding: ascii -*-
# metric_extract_v2.py
# Restored 'vth' dictionary structure for main.py UI.
# Bulletproof File Finder: Tries {block} first, safely falls back to generic glob.

import os
import re
import glob

# ===========================================================================
# PATH PARSING & DISCOVERY HELPERS
# ===========================================================================
def _get_block_name(run_dir):
    parts = os.path.normpath(run_dir).split(os.sep)
    for i, part in enumerate(parts):
        if part in ["fc", "innovus"] and i > 0:
            return parts[i-1]
    if len(parts) >= 3:
        return parts[-3]
    return "*" 

def _find_rpt(rpt_dir, patterns):
    """Loops through a list of patterns (Strict -> Fallback) and returns the first hit."""
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None

    for pat in patterns:
        pattern = os.path.join(rpt_dir, pat)
        hits = glob.glob(pattern)
        valid_files = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]
        
        if valid_files:
            return max(valid_files, key=os.path.getmtime)
            
    return None

# ===========================================================================
# CUSTOM PARSERS
# ===========================================================================
def parse_area(file_path):
    result = {"total_count": "-", "instance_count": "-", "total_area": "-"}
    if not file_path or not os.path.exists(file_path): return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            cells = re.search(r"Number of cells:\s+(\S+)", content)
            area = re.search(r"Total cell area:\s+(\S+)", content)
            if cells: 
                result["total_count"] = cells.group(1)
                result["instance_count"] = cells.group(1) 
            if area: 
                result["total_area"] = area.group(1)
    except: pass
    return result

def parse_utilization(file_path):
    # Added multiple keys to ensure UI catches it
    result = {"std_cell_area": "-", "memory_area": "-", "macro_area": "-", "std_util_str": "-/-", "std_util": "-/-"}
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
            
            std_util = re.search(r"Standard cell utilization\s*:\s*([\d.]+)", content)
            std_only = re.search(r"Standard cell only utilization\s*:\s*([\d.]+)", content)
            
            s_val = std_util.group(1) if std_util else "-"
            o_val = std_only.group(1) if std_only else "-"
            if s_val != "-" or o_val != "-":
                combo = f"{s_val}%/{o_val}%"
                result["std_util_str"] = combo
                result["std_util"] = combo
    except: pass
    return result

def parse_cell_usage(file_path):
    result = {"lvt_rvt_inst": "-/-", "lvt_rvt_area": "-/-"}
    if not file_path or not os.path.exists(file_path): return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lvt_inst, rvt_inst, lvt_area, rvt_area = 0.0, 0.0, 0.0, 0.0
            lvt_keys = ["LVT", "LVT_LLP", "LVT_L30L34"]
            rvt_keys = ["RVT", "RVT_LLP", "RVT_L30L34"]
            in_all_cells = False

            for line in f:
                if "1-1. For all Cells" in line: in_all_cells = True
                elif "1-2." in line: break 
                
                if in_all_cells and "|" in line:
                    cols = [c.strip() for c in line.split('|')]
                    if len(cols) >= 6:
                        vth_type = cols[1]
                        try:
                            inst_pct = float(cols[3].replace('%', ''))
                            area_pct = float(cols[5].replace('%', ''))
                            if vth_type in lvt_keys: 
                                lvt_inst += inst_pct
                                lvt_area += area_pct
                            elif vth_type in rvt_keys: 
                                rvt_inst += inst_pct
                                rvt_area += area_pct
                        except ValueError: pass
            
            result["lvt_rvt_inst"] = f"{lvt_inst:.2f}%/{rvt_inst:.2f}%"
            result["lvt_rvt_area"] = f"{lvt_area:.2f}%/{rvt_area:.2f}%"
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
                if wns and tns and num: return f"{wns.group(2)}/{tns.group(2)}/{num.group(2)}"
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
            if match: return f"{match.group(1)}%"
    except: pass
    return "-"

def parse_multibit(file_path):
    if not file_path or not os.path.exists(file_path): return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            match = re.search(r"Flip-flop cells banking ratio \(\(C\)/\s*\(\s*A\s*\+\s*C\s*\)\):\s+([\d.]+)%", content)
            if match: return f"{match.group(1)}%"
    except: pass
    return "-"

def parse_congestion(file_path):
    result = {"cong_both": "-"} 
    if not file_path or not os.path.exists(file_path): return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            both = re.search(r"Both Dirs\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            h_route = re.search(r"H routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            v_route = re.search(r"V routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%", content)
            if both and h_route and v_route:
                result["cong_both"] = f"{both.group(1)}%/{v_route.group(1)}%/{h_route.group(1)}%"
    except: pass
    return result

# ===========================================================================
# MAIN DASHBOARD EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS"):
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")
    
    block = _get_block_name(run_dir)

    # Note the list format: Tries exact block name first, then safe fallback wildcard
    qor_path = _find_rpt(rpt_dir, [f"qor.{block}.*.rpt", "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")

    cgc_path = _find_rpt(rpt_dir, [f"clock_gating_info.{block}.*.rpt", "clock_gating_info.mission.rpt", "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)
    
    mbit_path = _find_rpt(rpt_dir, [f"multibit_banking_ratio.{block}.*.rpt", "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    area_path = _find_rpt(rpt_dir, [f"area.{block}.*.rpt", "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {}
    result["area"]["total_area"] = area_data.get("total_area", "-")
    result["area"]["instance_count"] = area_data.get("instance_count", "-")
    
    util_path = _find_rpt(rpt_dir, [f"utilization.{block}.*.rpt", "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    
    # Passing util data explicitly
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    result["area"]["macro_area"] = util_data.get("macro_area", "-")
    result["util"] = util_data  # Wrapped in dict for main.py UI safety
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    # RESTORED TO DICTIONARY FOR VTH
    cell_path = _find_rpt(rpt_dir, [f"cell_usage.summary.{block}.*.rpt", "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)  

    cong_path = _find_rpt(rpt_dir, [f"congestion.{block}.*.rpt", "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)  

    return result

def extract_pnr_stage_metrics(run_dir, stage_name, source="WS"):
    result = {"stage": stage_name, "run_dir": run_dir}
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:  # OUTFEED: run_dir/stage_name/reports/
        rpt_dir = os.path.join(run_dir, stage_name, "reports")

    block = _get_block_name(run_dir)

    qor_path = _find_rpt(rpt_dir, [f"qor.{block}.*.rpt", "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"] = qor_data.get("r2r_hold", "-")

    cgc_path = _find_rpt(rpt_dir, [f"clock_gating_info.{block}.*.rpt", "clock_gating_info.mission.rpt", "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)
    
    mbit_path = _find_rpt(rpt_dir, [f"multibit_banking_ratio.{block}.*.rpt", "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    area_path = _find_rpt(rpt_dir, [f"area.{block}.*.rpt", "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {}
    result["area"]["total_area"] = area_data.get("total_area", "-")
    result["area"]["instance_count"] = area_data.get("instance_count", "-")
    
    util_path = _find_rpt(rpt_dir, [f"utilization.{block}.*.rpt", "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"] = util_data.get("memory_area", "-")
    result["area"]["macro_area"] = util_data.get("macro_area", "-")
    result["util"] = util_data 
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    # RESTORED TO DICTIONARY FOR VTH
    cell_path = _find_rpt(rpt_dir, [f"cell_usage.summary.{block}.*.rpt", "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)

    cong_path = _find_rpt(rpt_dir, [f"congestion.{block}.*.rpt", "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)

    return result
