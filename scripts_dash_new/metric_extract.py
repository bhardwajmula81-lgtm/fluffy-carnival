# -*- coding: ascii -*-
# metric_extract.py
# QoR metric extraction for Singularity PD dashboard.
# Parsers validated against user's standalone script.py output.

import os
import re
import glob

# ===========================================================================
# PATH PARSING & DISCOVERY HELPERS
# ===========================================================================
def _get_block_name(run_dir):
    """
    Extract block name from run directory path.
    Looks for 'fc' or 'innovus' in path and returns the parent directory.
    Example: .../BLK_ISP1/fc/run -> "BLK_ISP1"
    """
    parts = os.path.normpath(run_dir).split(os.sep)
    for i, part in enumerate(parts):
        if part in ["fc", "innovus"] and i > 0:
            return parts[i - 1]
    if len(parts) >= 3:
        return parts[-3]
    return "*"


def _find_rpt(rpt_dir, patterns):
    """Try each pattern in order (strict -> fallback). Return most recent match."""
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None
    for pat in patterns:
        hits = glob.glob(os.path.join(rpt_dir, pat))
        valid = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]
        if valid:
            return max(valid, key=os.path.getmtime)
    return None


# ===========================================================================
# PARSERS  (validated against standalone script.py)
# ===========================================================================
def parse_area(file_path):
    result = {"total_count": "-", "instance_count": "-", "total_area": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            cells = re.search(r"Number of cells:\s+(\d+)", content)
            area  = re.search(r"Total cell area:\s+([\d.]+)", content)
            if cells:
                result["total_count"]    = cells.group(1)
                result["instance_count"] = cells.group(1)
            if area:
                result["total_area"] = area.group(1)
    except Exception:
        pass
    return result


def parse_utilization(file_path):
    result = {
        "std_cell_area": "-", "memory_area": "-", "macro_area": "-",
        "std_util_str": "-/-", "std_util": "-/-",
    }
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            std_cell = re.search(
                r"^\s*std_cell\(\+headbuf\+epbuf\)\s+\d+\s+([\d.]+)",
                content, re.MULTILINE)
            memory = re.search(
                r"^\s*memory_cell\s+\d+\s+([\d.]+)",
                content, re.MULTILINE)
            macro = re.search(
                r"^\s*macro_cell\s+\d+\s+([\d.]+)",
                content, re.MULTILINE)
            if std_cell: result["std_cell_area"] = std_cell.group(1)
            if memory:   result["memory_area"]   = memory.group(1)
            if macro:    result["macro_area"]    = macro.group(1)

            std_util = re.search(r"Standard cell utilization\s*:\s*([\d.]+)", content)
            std_only = re.search(r"Standard cell only utilization\s*:\s*([\d.]+)", content)
            s_val = std_util.group(1) if std_util else "-"
            o_val = std_only.group(1) if std_only else "-"
            if s_val != "-" or o_val != "-":
                combo = "{}%/{}%".format(s_val, o_val)
                result["std_util_str"] = combo
                result["std_util"]     = combo
    except Exception:
        pass
    return result


def parse_cell_usage(file_path):
    """
    Column-based parser (section-aware).
    Table format: | VTH_TYPE | count | INST% | area_um2 | AREA% |
    cols[1]=type  cols[3]=inst%  cols[5]=area%
    """
    result = {"lvt_rvt_inst": "-/-", "lvt_rvt_area": "-/-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lvt_inst, rvt_inst, lvt_area, rvt_area = 0.0, 0.0, 0.0, 0.0
            lvt_keys = ["LVT", "LVT_LLP", "LVT_L30L34"]
            rvt_keys = ["RVT", "RVT_LLP", "RVT_L30L34"]
            in_all_cells = False
            for line in f:
                if "1-1. For all Cells" in line:
                    in_all_cells = True
                elif "1-2." in line:
                    break
                if in_all_cells and "|" in line:
                    cols = [c.strip() for c in line.split("|")]
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
                        except ValueError:
                            pass
            result["lvt_rvt_inst"] = "{:.2f}%/{:.2f}%".format(lvt_inst, rvt_inst)
            result["lvt_rvt_area"] = "{:.2f}%/{:.2f}%".format(lvt_area, rvt_area)
    except Exception:
        pass
    return result


def parse_qor(file_path):
    """
    Extract reg->reg WNS/TNS/FEPs for Setup and Hold sections.
    Regex captures second column (reg->reg) from WNS/TNS/NUM rows.
    """
    result = {"r2r_setup": "-", "r2r_hold": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        def get_r2r_data(section_name):
            section = re.search(
                section_name + r".*?(?=Report :|$)", content, re.DOTALL)
            if not section:
                return "-"
            sec_text = section.group(0)
            wns = re.search(r"WNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            tns = re.search(r"TNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            num = re.search(r"NUM\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            if wns and tns and num:
                return "{}/{}/{}".format(
                    wns.group(2), tns.group(2), num.group(2))
            return "-"

        result["r2r_setup"] = get_r2r_data("Setup violations")
        result["r2r_hold"]  = get_r2r_data("Hold violations")
    except Exception:
        pass
    return result


def parse_clock_gating(file_path):
    """File format: Number of Gated registers | 1234 (56.78%)"""
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            match = re.search(
                r"Number of Gated registers\s+\|\s+\d+\s+\(([\d.]+)%\)",
                content)
            if match:
                return "{}%".format(match.group(1))
    except Exception:
        pass
    return "-"


def parse_multibit(file_path):
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            match = re.search(
                r"Flip-flop cells banking ratio \(\(C\)/\s*\(\s*A\s*\+\s*C\s*\)\):\s+([\d.]+)%",
                content)
            if match:
                return "{}%".format(match.group(1))
    except Exception:
        pass
    return "-"


def parse_congestion(file_path):
    """Returns combined Both/V/H string, e.g. '0.50%/0.30%/0.20%'"""
    result = {"cong_both": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        both    = re.search(
            r"Both Dirs\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%",
            content)
        h_route = re.search(
            r"H routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%",
            content)
        v_route = re.search(
            r"V routing\s+\|\s+[\d.]+\s+\|\s+\([\s\d.]+%\)\s+\|\s+([\d.]+)%",
            content)
        if both and h_route and v_route:
            result["cong_both"] = "{}%/{}%/{}%".format(
                both.group(1), v_route.group(1), h_route.group(1))
        elif both:
            result["cong_both"] = "{}%".format(both.group(1))
    except Exception:
        pass
    return result


def parse_power(file_path):
    """Extract Cell Leakage Power from power report."""
    result = {"leakage": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            m = re.search(
                r"Cell Leakage Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)",
                content)
            if m:
                try:
                    val  = float(m.group(1))
                    unit = m.group(2)
                    result["leakage"] = "{:g} {}".format(val, unit)
                except ValueError:
                    result["leakage"] = "{} {}".format(m.group(1), m.group(2))
    except Exception:
        pass
    return result


# ===========================================================================
# MAIN EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS", block=None):
    """
    Extract all QoR metrics for a FE run.
    block: block name like "BLK_ISP1" -- passed from dashboard, falls back
           to path-derived value if not supplied.
    """
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")
    b = block or _get_block_name(run_dir)

    # ---- Timing ----
    qor_path = _find_rpt(rpt_dir, [
        "qor.{}.*.rpt".format(b), "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")

    # ---- Area ----
    area_path = _find_rpt(rpt_dir, [
        "area.{}.*.rpt".format(b), "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {
        "total_area":     area_data.get("total_area",     "-"),
        "instance_count": area_data.get("instance_count", "-"),
    }

    # ---- Utilization ----
    util_path = _find_rpt(rpt_dir, [
        "utilization.{}.*.rpt".format(b), "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"]   = util_data.get("memory_area",   "-")
    result["area"]["macro_area"]    = util_data.get("macro_area",    "-")
    result["util"]         = util_data
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    # ---- Cell Usage (LVT/RVT) ----
    cell_path = _find_rpt(rpt_dir, [
        "cell_usage.summary.{}.*.rpt".format(b),
        "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)

    # ---- Clock Gating ----
    cgc_path = _find_rpt(rpt_dir, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)

    # ---- Multi-bit ----
    mbit_path = _find_rpt(rpt_dir, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    # ---- Congestion ----
    cong_path = _find_rpt(rpt_dir, [
        "congestion.{}.*.rpt".format(b), "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)

    # ---- Power ----
    pwr_path = _find_rpt(rpt_dir, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"])
    result["power"] = parse_power(pwr_path)

    return result


def extract_pnr_stage_metrics(run_dir, stage_name, source="WS", block=None):
    """
    Extract QoR metrics for a single PNR stage.
    block: block name like "BLK_ISP1" -- passed from dashboard.
    """
    result = {"stage": stage_name, "run_dir": run_dir}
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:   # OUTFEED: {run_dir}/{stage_name}/reports/
        rpt_dir = os.path.join(run_dir, stage_name, "reports")
    b = block or _get_block_name(run_dir)

    # ---- Timing ----
    qor_path = _find_rpt(rpt_dir, [
        "qor.{}.*.rpt".format(b), "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")

    # ---- Area ----
    area_path = _find_rpt(rpt_dir, [
        "area.{}.*.rpt".format(b), "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {
        "total_area":     area_data.get("total_area",     "-"),
        "instance_count": area_data.get("instance_count", "-"),
    }

    # ---- Utilization ----
    util_path = _find_rpt(rpt_dir, [
        "utilization.{}.*.rpt".format(b), "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"]   = util_data.get("memory_area",   "-")
    result["area"]["macro_area"]    = util_data.get("macro_area",    "-")
    result["util"]         = util_data
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    # ---- Cell Usage (LVT/RVT) ----
    cell_path = _find_rpt(rpt_dir, [
        "cell_usage.summary.{}.*.rpt".format(b),
        "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)

    # ---- Clock Gating ----
    cgc_path = _find_rpt(rpt_dir, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)

    # ---- Multi-bit ----
    mbit_path = _find_rpt(rpt_dir, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    # ---- Congestion ----
    cong_path = _find_rpt(rpt_dir, [
        "congestion.{}.*.rpt".format(b), "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)

    # ---- Power ----
    pwr_path = _find_rpt(rpt_dir, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"])
    result["power"] = parse_power(pwr_path)

    return result
