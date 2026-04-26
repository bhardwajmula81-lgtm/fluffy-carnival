# -*- coding: ascii -*-
# metric_extract.py
# QoR metric extraction for Singularity PD dashboard.
# Parsers validated against user standalone script output.

import os
import re
import glob

# ===========================================================================
# PATH HELPERS
# ===========================================================================
def _get_block_name(run_dir):
    parts = os.path.normpath(run_dir).split(os.sep)
    for i, part in enumerate(parts):
        if part in ["fc", "innovus"] and i > 0:
            return parts[i - 1]
    if len(parts) >= 3:
        return parts[-3]
    return "*"


def _find_rpt(rpt_dir, patterns):
    """Try each glob pattern in order; return most-recent file or None."""
    if not rpt_dir or not os.path.isdir(rpt_dir):
        return None
    for pat in patterns:
        hits = glob.glob(os.path.join(rpt_dir, pat))
        valid = [f for f in hits if os.path.isfile(f) and not f.endswith(".log")]
        if valid:
            return max(valid, key=os.path.getmtime)
    return None


# ===========================================================================
# PARSERS
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
                r"^\s*memory_cell\s+\d+\s+([\d.]+)", content, re.MULTILINE)
            macro  = re.search(
                r"^\s*macro_cell\s+\d+\s+([\d.]+)",  content, re.MULTILINE)
            if std_cell: result["std_cell_area"] = std_cell.group(1)
            if memory:   result["memory_area"]   = memory.group(1)
            if macro:    result["macro_area"]    = macro.group(1)

            std_util = re.search(r"Standard cell utilization\s*:\s*([\d.]+)", content)
            std_only = re.search(r"Standard cell only utilization\s*:\s*([\d.]+)", content)
            s = std_util.group(1) if std_util else "-"
            o = std_only.group(1) if std_only else "-"
            if s != "-" or o != "-":
                combo = "{}%/{}%".format(s, o)
                result["std_util_str"] = combo
                result["std_util"]     = combo
    except Exception:
        pass
    return result


def parse_cell_usage(file_path):
    """
    Section-aware column parser.
    Table: | VthType | count | Inst% | area_um2 | Area% |
           cols[1]    cols[2]  cols[3]  cols[4]    cols[5]
    Groups by prefix: LVT*, RVT*, HVT*
    Returns lvt_rvt_hvt_inst/area as "X%/Y%/Z%" strings.
    """
    result = {
        "lvt_rvt_hvt_inst": "-/-/-",
        "lvt_rvt_hvt_area": "-/-/-",
        "lvt_rvt_inst":     "-/-",    # backward-compat alias
        "lvt_rvt_area":     "-/-",
    }
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lvt_i = rvt_i = hvt_i = 0.0
            lvt_a = rvt_a = hvt_a = 0.0
            in_all_cells = False
            for line in f:
                if "1-1. For all Cells" in line:
                    in_all_cells = True
                elif "1-2." in line:
                    break
                if not in_all_cells or "|" not in line:
                    continue
                cols = [c.strip() for c in line.split("|")]
                if len(cols) < 6:
                    continue
                vth = cols[1]
                try:
                    ip = float(cols[3].replace('%', ''))
                    ap = float(cols[5].replace('%', ''))
                    if   vth.startswith("LVT"): lvt_i += ip; lvt_a += ap
                    elif vth.startswith("RVT"): rvt_i += ip; rvt_a += ap
                    elif vth.startswith("HVT"): hvt_i += ip; hvt_a += ap
                except ValueError:
                    pass
        inst_str = "{:.2f}%/{:.2f}%/{:.2f}%".format(lvt_i, rvt_i, hvt_i)
        area_str = "{:.2f}%/{:.2f}%/{:.2f}%".format(lvt_a, rvt_a, hvt_a)
        result["lvt_rvt_hvt_inst"] = inst_str
        result["lvt_rvt_hvt_area"] = area_str
        # backward-compat: first two fields only
        result["lvt_rvt_inst"] = "{:.2f}%/{:.2f}%".format(lvt_i, rvt_i)
        result["lvt_rvt_area"] = "{:.2f}%/{:.2f}%".format(lvt_a, rvt_a)
    except Exception:
        pass
    return result


def parse_qor(file_path):
    """
    Extract reg->reg WNS/TNS/FEPs for Setup and Hold.
    Second column (group(2)) = reg->reg value.
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
            sec = section.group(0)
            wns = re.search(r"WNS\s+([-\d.]+)\s+([-\d.]+)", sec)
            tns = re.search(r"TNS\s+([-\d.]+)\s+([-\d.]+)", sec)
            num = re.search(r"NUM\s+([-\d.]+)\s+([-\d.]+)", sec)
            if wns and tns and num:
                return "{}/{}/{}".format(wns.group(2), tns.group(2), num.group(2))
            return "-"

        result["r2r_setup"] = get_r2r_data("Setup violations")
        result["r2r_hold"]  = get_r2r_data("Hold violations")
    except Exception:
        pass
    return result


def parse_clock_gating(file_path):
    """Format: Number of Gated registers | 1234 (56.78%)"""
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            m = re.search(
                r"Number of Gated registers\s+\|\s+\d+\s+\(([\d.]+)%\)", content)
            if m:
                return "{}%".format(m.group(1))
    except Exception:
        pass
    return "-"


def parse_multibit(file_path):
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            m = re.search(
                r"Flip-flop cells banking ratio \(\(C\)/\s*\(\s*A\s*\+\s*C\s*\)\):\s+([\d.]+)%",
                content)
            if m:
                return "{}%".format(m.group(1))
    except Exception:
        pass
    return "-"


def parse_congestion(file_path):
    """
    Real file format (from screenshot):
      Both Dirs |  56154 |  142 |  38832   ( 0.2002%) |  1
      H routing |  53817 |  142 |  37696   ( 0.3886%) |  1
      V routing |   2337 |  100 |   1136   ( 0.0117%) |  1
    Returns "Both%/H%/V%" e.g. "0.2002%/0.3886%/0.0117%"
    """
    result = {"cong_both": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        # Matches: "Both Dirs | total | max | count   ( X.XXXX%) |"
        _pat = (r"{}\s+\|\s+[\d.]+\s+\|\s+[\d.]+\s+\|"
                r"\s+[\d.]+\s+\(\s*([\d.]+)%\)")
        both    = re.search(_pat.format("Both Dirs"), content)
        h_route = re.search(_pat.format("H routing"), content)
        v_route = re.search(_pat.format("V routing"), content)
        if both and h_route and v_route:
            result["cong_both"] = "{}%/{}%/{}%".format(
                both.group(1), h_route.group(1), v_route.group(1))
        elif both:
            result["cong_both"] = "{}%".format(both.group(1))
    except Exception:
        pass
    return result


def parse_power(file_path):
    """Extract Cell Leakage Power = X.XX uW from power report."""
    result = {"leakage": "-"}
    if not file_path or not os.path.exists(file_path):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            m = re.search(
                r"Cell Leakage Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)", content)
            if m:
                try:
                    result["leakage"] = "{:g} {}".format(float(m.group(1)), m.group(2))
                except ValueError:
                    result["leakage"] = "{} {}".format(m.group(1), m.group(2))
    except Exception:
        pass
    return result


def parse_fe_runtime(file_path):
    """Extract total runtime from reports/runtime.V2.rpt."""
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(r"Total\s*:\s*(\d+)h:(\d+)m:(\d+)s", line)
                    if rt:
                        return "{:02}h:{:02}m:{:02}s".format(
                            int(rt.group(1)), int(rt.group(2)), int(rt.group(3)))
    except Exception:
        pass
    return "-"


def parse_logic_depth(file_path):
    """
    Extract max Logic Depth from report_logic_depth.summary.*.rpt.
    Each scenario has 'All Path Groups ... MAX_LEVEL'. Returns max.
    """
    if not file_path or not os.path.exists(file_path):
        return "-"
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        # Line: "All Path Groups  n/a  n/a  1374073  51"
        vals = re.findall(
            r"All Path Groups\s+\S+\s+\S+\s+\d+\s+(\d+)", content)
        if vals:
            return str(max(int(v) for v in vals))
    except Exception:
        pass
    return "-"


# ===========================================================================
# MAIN EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS", block=None):
    """
    Extract all QoR metrics for a FE run.
    block: e.g. "BLK_ISP1" -- passed from dashboard tree.
    """
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")
    b = block or _get_block_name(run_dir)

    # Timing
    qor_path = _find_rpt(rpt_dir, ["qor.{}.*.rpt".format(b), "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")

    # Area
    area_path = _find_rpt(rpt_dir, ["area.{}.*.rpt".format(b), "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {
        "total_area":     area_data.get("total_area",     "-"),
        "instance_count": area_data.get("instance_count", "-"),
    }

    # Utilization
    util_path = _find_rpt(rpt_dir, [
        "utilization.{}.*.rpt".format(b), "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"]   = util_data.get("memory_area",   "-")
    result["area"]["macro_area"]    = util_data.get("macro_area",    "-")
    result["util"]         = util_data
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    # Cell Usage (LVT/RVT/HVT)
    cell_path = _find_rpt(rpt_dir, [
        "cell_usage.summary.{}.*.rpt".format(b),
        "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)

    # Clock Gating
    cgc_path = _find_rpt(rpt_dir, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)

    # Multi-bit
    mbit_path = _find_rpt(rpt_dir, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    # Congestion
    cong_path = _find_rpt(rpt_dir, [
        "congestion.{}.*.rpt".format(b), "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)

    # Power
    pwr_path = _find_rpt(rpt_dir, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"])
    result["power"] = parse_power(pwr_path)

    # Runtime
    result["runtime"] = parse_fe_runtime(
        os.path.join(run_dir, "reports", "runtime.V2.rpt"))

    # Logic Depth
    ld_path = _find_rpt(rpt_dir, ["report_logic_depth.summary.*.rpt"])
    result["logic_depth"] = parse_logic_depth(ld_path)

    # Report file paths — for double-click "open in gvim" from dialogs
    _rt_path = os.path.join(run_dir, "reports", "runtime.V2.rpt")
    result["_paths"] = {
        "r2r_setup":     qor_path,
        "r2r_hold":      qor_path,
        "logic_depth":   ld_path,
        "cgc":           cgc_path,
        "mbit":          mbit_path,
        "congestion":    cong_path,
        "leakage":       pwr_path,
        "area":          area_path,
        "total_area":    area_path,
        "instance_count": area_path,
        "std_cell_area": util_path,
        "memory_area":   util_path,
        "macro_area":    util_path,
        "vth":           cell_path,
        "runtime":       _rt_path if os.path.exists(_rt_path) else None,
    }

    return result


def extract_pnr_stage_metrics(run_dir, stage_name, source="WS", block=None):
    """Extract QoR metrics for a single PNR stage."""
    result = {"stage": stage_name, "run_dir": run_dir}
    if source == "WS":
        rpt_dir = os.path.join(run_dir, "reports", stage_name)
    else:
        rpt_dir = os.path.join(run_dir, stage_name, "reports")
    b = block or _get_block_name(run_dir)

    qor_path = _find_rpt(rpt_dir, ["qor.{}.*.rpt".format(b), "qor.*.rpt"])
    qor_data = parse_qor(qor_path)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")

    area_path = _find_rpt(rpt_dir, ["area.{}.*.rpt".format(b), "area.*.rpt"])
    area_data = parse_area(area_path)
    result["area"] = {
        "total_area":     area_data.get("total_area",     "-"),
        "instance_count": area_data.get("instance_count", "-"),
    }

    util_path = _find_rpt(rpt_dir, [
        "utilization.{}.*.rpt".format(b), "utilization.*.rpt"])
    util_data = parse_utilization(util_path)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"]   = util_data.get("memory_area",   "-")
    result["area"]["macro_area"]    = util_data.get("macro_area",    "-")
    result["util"]         = util_data
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    cell_path = _find_rpt(rpt_dir, [
        "cell_usage.summary.{}.*.rpt".format(b),
        "cell_usage.summary.*.rpt"])
    result["vth"] = parse_cell_usage(cell_path)

    cgc_path = _find_rpt(rpt_dir, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"])
    result["cgc"] = parse_clock_gating(cgc_path)

    mbit_path = _find_rpt(rpt_dir, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"])
    result["mbit"] = parse_multibit(mbit_path)

    cong_path = _find_rpt(rpt_dir, [
        "congestion.{}.*.rpt".format(b), "congestion.*.rpt"])
    result["congestion"] = parse_congestion(cong_path)

    pwr_path = _find_rpt(rpt_dir, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"])
    result["power"] = parse_power(pwr_path)

    ld_path = _find_rpt(rpt_dir, ["report_logic_depth.summary.*.rpt"])
    result["logic_depth"] = parse_logic_depth(ld_path)

    result["_paths"] = {
        "r2r_setup":     qor_path,
        "r2r_hold":      qor_path,
        "logic_depth":   ld_path,
        "cgc":           cgc_path,
        "mbit":          mbit_path,
        "congestion":    cong_path,
        "leakage":       pwr_path,
        "area":          area_path,
        "total_area":    area_path,
        "instance_count": area_path,
        "std_cell_area": util_path,
        "memory_area":   util_path,
        "macro_area":    util_path,
        "vth":           cell_path,
    }

    return result
