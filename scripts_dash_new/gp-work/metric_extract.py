# -*- coding: utf-8 -*-
# metric_extract.py
# QoR metric extraction for Singularity PD dashboard.
# Parsers validated against user standalone script output.

import os
import re
import fnmatch
import gzip

try:
    from debug_log import debug_log
except Exception:
    def debug_log(context, exc=None):
        pass

try:
    from metric_registry import apply_flat_metrics
except Exception:
    def apply_flat_metrics(metrics, scope=None, tool=None):
        return metrics

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


class _ReportFinder(object):
    """Per-extraction report finder with cached directory listings."""

    def __init__(self):
        self._dir_cache = {}
        self._preferred_stage_dirs = {}

    def _entries(self, rpt_dir):
        if not rpt_dir:
            return []
        rpt_dir = os.path.normpath(rpt_dir)
        if rpt_dir in self._dir_cache:
            return self._dir_cache[rpt_dir]
        entries = []
        try:
            for entry in os.scandir(rpt_dir):
                try:
                    if entry.is_file() and not entry.name.endswith(".log"):
                        entries.append((entry.name, entry.path, entry.stat().st_mtime))
                except Exception as e:
                    debug_log("metric_extract: report entry stat failed {}".format(rpt_dir), e)
        except (FileNotFoundError, NotADirectoryError):
            entries = []
        except Exception as e:
            debug_log("metric_extract: report directory scan failed {}".format(rpt_dir), e)
            entries = []
        self._dir_cache[rpt_dir] = entries
        return entries

    def find(self, rpt_dir, patterns):
        """Try patterns in order; return most-recent matching file or None."""
        entries = self._entries(rpt_dir)
        if not entries:
            return None
        for pat in patterns:
            valid = [(path, mtime) for name, path, mtime in entries
                     if fnmatch.fnmatch(name, pat)]
            if valid:
                return max(valid, key=lambda x: x[1])[0]
        return None

    def find_stage(self, run_dir, stage_name, source, patterns, stage_path=None):
        key = (os.path.normpath(run_dir or ""), stage_name, source,
               os.path.normpath(stage_path or ""))
        dirs = list(_stage_report_dirs(run_dir, stage_name, source, stage_path))
        preferred = self._preferred_stage_dirs.get(key)
        if preferred in dirs:
            dirs.remove(preferred)
            dirs.insert(0, preferred)
        for d in dirs:
            hit = self.find(d, patterns)
            if hit:
                self._preferred_stage_dirs[key] = d
                return hit
        return None


def _find_rpt(rpt_dir, patterns, finder=None):
    finder = finder or _ReportFinder()
    return finder.find(rpt_dir, patterns)


def _cancelled(cancel_check):
    try:
        return bool(cancel_check and cancel_check())
    except Exception:
        return False


# ===========================================================================
# PARSERS
# ===========================================================================
def parse_area(file_path, verified=False):
    result = {"total_count": "-", "instance_count": "-", "total_area": "-"}
    if not file_path or ((not verified) and not os.path.exists(file_path)):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if result["total_count"] == "-":
                    cells = re.search(r"Number of cells:\s+(\d+)", line)
                    if cells:
                        result["total_count"] = cells.group(1)
                        result["instance_count"] = cells.group(1)
                if result["total_area"] == "-":
                    area = re.search(r"Total cell area:\s+([\d.]+)", line)
                    if area:
                        result["total_area"] = area.group(1)
                if (result["total_count"] != "-"
                        and result["total_area"] != "-"):
                    break
    except Exception as e:
        debug_log("metric_extract: parse_area failed {}".format(file_path), e)
    return result


def parse_utilization(file_path, verified=False):
    result = {
        "std_cell_area": "-", "memory_area": "-", "macro_area": "-",
        "std_util_str": "-/-", "std_util": "-/-",
    }
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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
    except Exception as e:
        debug_log("metric_extract: parse_utilization failed {}".format(file_path), e)
    return result


def parse_cell_usage(file_path, verified=False):
    """
    Section-aware VT parser for FE reports.
    Preserves legacy LVT/RVT/HVT keys and also returns dynamic VT labels.
    """
    result = {
        "lvt_rvt_hvt_inst": "-/-/-",
        "lvt_rvt_hvt_area": "-/-/-",
        "lvt_rvt_inst":     "-/-",
        "lvt_rvt_area":     "-/-",
        "vt_labels":        "",
        "vt_inst":          "",
        "vt_area":          "",
    }
    if not file_path or ((not verified) and not os.path.exists(file_path)):
        return result
    try:
        groups = {}
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
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
                vth = cols[1].upper()
                m = re.match(r"([A-Z]*VT)", vth)
                if not m:
                    continue
                key = m.group(1)
                try:
                    ip = float(cols[3].replace('%', ''))
                    ap = float(cols[5].replace('%', ''))
                except ValueError:
                    continue
                vals = groups.setdefault(key, [0.0, 0.0])
                vals[0] += ip
                vals[1] += ap
        preferred = ["UHVT", "HVT", "RVT", "SVT", "LVT", "SLVT", "ULVT", "ELVT"]
        order = [g for g in preferred if g in groups]
        order.extend(sorted(g for g in groups if g not in order))
        if order:
            result["vt_labels"] = "/".join(g + "*" for g in order)
            result["vt_inst"] = "/".join("{:.2f}%".format(groups[g][0]) for g in order)
            result["vt_area"] = "/".join("{:.2f}%".format(groups[g][1]) for g in order)
        lvt_i, lvt_a = groups.get("LVT", [0.0, 0.0])
        rvt_i, rvt_a = groups.get("RVT", [0.0, 0.0])
        hvt_i, hvt_a = groups.get("HVT", [0.0, 0.0])
        result["lvt_rvt_hvt_inst"] = "{:.2f}%/{:.2f}%/{:.2f}%".format(lvt_i, rvt_i, hvt_i)
        result["lvt_rvt_hvt_area"] = "{:.2f}%/{:.2f}%/{:.2f}%".format(lvt_a, rvt_a, hvt_a)
        result["lvt_rvt_inst"] = "{:.2f}%/{:.2f}%".format(lvt_i, rvt_i)
        result["lvt_rvt_area"] = "{:.2f}%/{:.2f}%".format(lvt_a, rvt_a)
    except Exception as e:
        debug_log("metric_extract: parse_cell_usage failed {}".format(file_path), e)
    return result


def parse_qor(file_path, verified=False):
    """
    Extract reg->reg WNS/TNS/FEPs for Setup and Hold.
    Second column (group(2)) = reg->reg value.
    """
    result = {"r2r_setup": "-", "r2r_hold": "-"}
    if not file_path or ((not verified) and not os.path.exists(file_path)):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        def get_r2r_data(section_name):
            low_content = content.lower()
            if (section_name.lower().startswith("setup")
                    and "no setup violations found" in low_content):
                return "0/0/0"
            if (section_name.lower().startswith("hold")
                    and "no hold violations found" in low_content):
                return "0/0/0"
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
    except Exception as e:
        debug_log("metric_extract: parse_qor failed {}".format(file_path), e)
    return result


def parse_clock_gating(file_path, verified=False):
    """Format: Number of Gated registers | 1234 (56.78%)"""
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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


def parse_multibit(file_path, verified=False):
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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


def parse_congestion(file_path, verified=False):
    """
    Real file format (from screenshot):
      Both Dirs |  56154 |  142 |  38832   ( 0.2002%) |  1
      H routing |  53817 |  142 |  37696   ( 0.3886%) |  1
      V routing |   2337 |  100 |   1136   ( 0.0117%) |  1
    Returns "Both%/H%/V%" e.g. "0.2002%/0.3886%/0.0117%"
    """
    result = {"cong_both": "-"}
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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


def parse_power(file_path, verified=False):
    """Extract power values from report_power_info*.rpt."""
    result = {
        "cell_internal": "-",
        "net_switching": "-",
        "total_dynamic": "-",
        "leakage": "-",
        "total_power": "-",
    }
    if not file_path or ((not verified) and not os.path.exists(file_path)):
        return result
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                checks = [
                    ("cell_internal", r"Cell Internal Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)"),
                    ("net_switching", r"Net Switching Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)"),
                    ("total_dynamic", r"Total Dynamic Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)"),
                    ("leakage", r"Cell Leakage Power\s*=\s*([-\d.eE+]+)\s*([a-zA-Z]+)"),
                    ("total_power", r"\bTotal\s+Power\s*[=:]\s*([-\d.eE+]+)\s*([a-zA-Z]+)"),
                ]
                for key, pat in checks:
                    if result.get(key) == "-":
                        m = re.search(pat, line, re.I)
                        if m:
                            result[key] = "{} {}".format(m.group(1), m.group(2))

                stripped = line.strip()
                if (result.get("total_power") == "-" and
                        stripped.lower().startswith("total ") and
                        "=" not in stripped and ":" not in stripped):
                    pairs = re.findall(r"([-\d.eE+]+)\s*([a-zA-Z]+)", stripped)
                    if len(pairs) >= 4:
                        num, unit = pairs[-1]
                        result["total_power"] = "{} {}".format(num, unit)

                if all(result.get(k) != "-" for k in ("leakage", "total_power")):
                    if result.get("cell_internal") != "-" and result.get("net_switching") != "-":
                        break
    except Exception as e:
        debug_log("metric_extract: parse_power failed {}".format(file_path), e)
    return result


def parse_fe_runtime(file_path, verified=False):
    """Extract total runtime from reports/runtime.V2.rpt."""
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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


def parse_logic_depth(file_path, verified=False):
    """
    Extract max Logic Depth from report_logic_depth.summary.*.rpt.
    Each scenario has 'All Path Groups ... MAX_LEVEL'. Returns max.
    """
    if not file_path or ((not verified) and not os.path.exists(file_path)):
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


def _stage_report_dirs(run_dir, stage_name, source="WS", stage_path=None):
    dirs = [
        os.path.join(stage_path or "", "reports"),
        os.path.join(stage_path or "", "reports", stage_name),
        stage_path or "",
        os.path.join(run_dir, "reports", stage_name),
        os.path.join(run_dir, stage_name, "reports", stage_name),
        os.path.join(run_dir, stage_name, "reports"),
        os.path.join(run_dir, "reports"),
    ]
    out = []
    for d in dirs:
        if d and d not in out:
            out.append(d)
    return out


def _find_stage_rpt(run_dir, stage_name, source, patterns, stage_path=None,
                    finder=None):
    finder = finder or _ReportFinder()
    return finder.find_stage(run_dir, stage_name, source, patterns,
                             stage_path=stage_path)


def _read_stage_text(path):
    if not path or not os.path.exists(path):
        return ""
    try:
        if path.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                return f.read()
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        debug_log("metric_extract: read stage text failed {}".format(path), e)
        return ""


def _trip(a, b, c):
    return "{}/{}/{}".format(a, b, c)


def _parse_stage_fc_timing(text, section_name):
    low = text.lower()
    if section_name.lower().startswith("setup") and "no setup violations found" in low:
        return ("0/0/0", "0/0/0")
    if section_name.lower().startswith("hold") and "no hold violations found" in low:
        return ("0/0/0", "0/0/0")
    m = re.search(section_name + r".*?(?=\n\s*(?:Setup violations|Hold violations|END_CMD|Report :|$))",
                  text, re.S | re.I)
    if not m:
        return ("-", "-")
    sec = m.group(0)
    wns = re.search(r"^\s*WNS\s+(.+)$", sec, re.M)
    tns = re.search(r"^\s*TNS\s+(.+)$", sec, re.M)
    num = re.search(r"^\s*(?:NUM|FEP|NVE)\s+(.+)$", sec, re.M)
    if not (wns and tns and num):
        return ("-", "-")
    wv = wns.group(1).split()
    tv = tns.group(1).split()
    nv = num.group(1).split()
    if len(wv) < 2 or len(tv) < 2 or len(nv) < 2:
        return ("-", "-")
    return (_trip(wv[0], tv[0], nv[0]), _trip(wv[1], tv[1], nv[1]))


def _parse_stage_fc_group_timing(text, section_name):
    low = text.lower()
    if section_name.lower().startswith("setup") and "no setup violations found" in low:
        return ("0/0/0", "0/0/0")
    if section_name.lower().startswith("hold") and "no hold violations found" in low:
        return ("0/0/0", "0/0/0")
    m = re.search(section_name + r".*?(?=\n\s*(?:Setup violations|Hold violations|END_CMD|Report :|$))",
                  text, re.S | re.I)
    if not m:
        return ("-", "-")
    sec = m.group(0)
    for line in sec.splitlines():
        if "|" not in line:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 2:
            continue
        total_nums = re.findall(r"[-+]?\d+(?:\.\d+)?", cells[0])
        r2r_nums = re.findall(r"[-+]?\d+(?:\.\d+)?", cells[1])
        if len(total_nums) >= 3 and len(r2r_nums) >= 3:
            return (
                _trip(total_nums[0], total_nums[1], total_nums[2]),
                _trip(r2r_nums[0], r2r_nums[1], r2r_nums[2]))
    return ("-", "-")


def _parse_stage_innovus_setup(text):
    header = None
    rows = {}
    for line in text.splitlines():
        if "|" not in line:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if not cells:
            continue
        if cells[0].lower().startswith("setup mode"):
            header = [c.lower() for c in cells]
        elif header and cells[0].lower().startswith("wns"):
            rows["wns"] = cells
        elif header and cells[0].lower().startswith("tns"):
            rows["tns"] = cells
        elif header and cells[0].lower().startswith("violating"):
            rows["num"] = cells
            break
    if not header or not all(k in rows for k in ("wns", "tns", "num")):
        return ("-", "-")
    try:
        all_i = header.index("all")
        r2r_i = header.index("reg2reg")
        return (
            _trip(rows["wns"][all_i], rows["tns"][all_i], rows["num"][all_i]),
            _trip(rows["wns"][r2r_i], rows["tns"][r2r_i], rows["num"][r2r_i]))
    except Exception:
        return ("-", "-")


def _parse_stage_innovus_hold(text):
    m = re.search(r"#\s*HOLD.*?View\s*:\s*ALL\s+([-\d.]+)\s+([-\d.]+)\s+(\d+)",
                  text, re.S | re.I)
    return _trip(m.group(1), m.group(2), m.group(3)) if m else "-"


def _parse_stage_grc(text):
    m = re.search(
        r"Overflow:\s*\S+\s*=\s*\S+\s*\(([^)]*H)\)\s*\+\s*\S+\s*\(([^)]*V)\)",
        text, re.I)
    if m:
        return "{} + {}".format(m.group(1).strip(), m.group(2).strip())
    h = re.search(r"H\s+routing.*?\(\s*([\d.]+%)\s*\)", text, re.I)
    v = re.search(r"V\s+routing.*?\(\s*([\d.]+%)\s*\)", text, re.I)
    if h and v:
        return "{} H + {} V".format(h.group(1), v.group(1))
    return "-"


def _parse_stage_area(text):
    out = {}
    m = re.search(r"^\s*std_cell\(\+headbuf\+epbuf\)\s+(\d+)\s+([\d.]+)",
                  text, re.M)
    if m:
        out["std_cell_count"] = m.group(1)
        out["std_cell_area"] = m.group(2)
        out["std_cell_count_area"] = "{}/{}".format(m.group(1), m.group(2))
    m = re.search(r"Standard\s+cell\s+only\s+utilization\s*:\s*([\d.]+)%", text, re.I)
    if m:
        out["std_cell_only_util"] = m.group(1) + "%"
    m = re.search(r"^\s*Total\s+utilization\s*:\s*([\d.]+)%", text, re.I | re.M)
    if m:
        out["total_util"] = m.group(1) + "%"
    return out


def _parse_stage_vth(text):
    m = re.search(r"##\s*Logic cells only(.*?)(?=##\s*Total cells|$)", text, re.S | re.I)
    if not m:
        return {}
    groups = {}
    for line in m.group(1).splitlines():
        r = re.search(
            r"^\s*([A-Za-z][A-Za-z0-9]*_\d+)\s+[-+\d.]+\s+\(\s*([\d.]+)\s*%\s*\)\s+[-+\d.]+\s+\(\s*([\d.]+)\s*%\s*\)",
            line)
        if not r:
            continue
        g = r.group(1).split("_", 1)[0].upper()
        vals = groups.setdefault(g, [0.0, 0.0])
        vals[0] += float(r.group(2))
        vals[1] += float(r.group(3))
    if not groups:
        return {}
    preferred = ["UHVT", "HVT", "RVT", "SVT", "LVT", "SLVT", "ULVT", "ELVT"]
    order = [g for g in preferred if g in groups]
    order.extend(sorted(g for g in groups if g not in order))
    return {
        "stage_vt_label": "/".join(g + "*" for g in order),
        "stage_vt_inst": "/".join("{:.2f}%".format(groups[g][0]) for g in order),
        "stage_vt_area": "/".join("{:.2f}%".format(groups[g][1]) for g in order),
    }


def _parse_stage_cts(text):
    for line in text.splitlines():
        if not re.search(r"\bAll\s+Clocks\b", line, re.I):
            continue
        nums = re.findall(r"[-+]?\d+(?:\.\d+)?", line)
        if len(nums) >= 7:
            # Columns after "All Clocks":
            # Sinks, Levels, Clock Repeater Count, Clock Repeater Area,
            # Clock Stdcell Area, Max Latency, Global Skew, ...
            return {
                "skew_latency": "{}/{}".format(nums[6], nums[5]),
                "clock_repeater_count_area": "{}/{}".format(nums[2], nums[3]),
            }
    return {}


def _parse_stage_runtime(text):
    runtime = "-"
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        if not parts[0].lower().startswith("total_"):
            continue
        # Runtime files use columns similar to:
        # STAGE TIMESTAMP CPUTIME REALTIME MEMORY
        for tok in reversed(parts):
            if re.match(r"\d+d\.\d+h\.\d+m\.\d+s", tok):
                runtime = tok
                break
            if re.match(r"\d+d:\d+h:\d+m:\d+s", tok):
                runtime = tok
                break
            if re.match(r"\d+h:\d+m:\d+s", tok):
                runtime = tok
                break
        if runtime != "-":
            break
    return runtime


# ===========================================================================
# MAIN EXTRACTION WRAPPERS
# ===========================================================================
def extract_fe_metrics(run_dir, source="WS", block=None, cancel_check=None):
    """
    Extract all QoR metrics for a FE run.
    block: e.g. "BLK_ISP1" -- passed from dashboard tree.
    """
    result = {"run_dir": run_dir, "run_type": "FE"}
    rpt_dir = os.path.join(run_dir, "reports")
    b = block or _get_block_name(run_dir)
    finder = _ReportFinder()

    # Timing
    qor_path = _find_rpt(
        rpt_dir, ["qor.{}.*.rpt".format(b), "qor.*.rpt"], finder)
    qor_data = parse_qor(qor_path, verified=True)
    result["r2r_setup"] = qor_data.get("r2r_setup", "-")
    result["r2r_hold"]  = qor_data.get("r2r_hold",  "-")

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Area
    area_path = _find_rpt(
        rpt_dir, ["area.{}.*.rpt".format(b), "area.*.rpt"], finder)
    area_data = parse_area(area_path, verified=True)
    result["area"] = {
        "total_area":     area_data.get("total_area",     "-"),
        "instance_count": area_data.get("instance_count", "-"),
    }

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Utilization
    util_path = _find_rpt(rpt_dir, [
        "utilization.{}.*.rpt".format(b), "utilization.*.rpt"], finder)
    util_data = parse_utilization(util_path, verified=True)
    result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
    result["area"]["memory_area"]   = util_data.get("memory_area",   "-")
    result["area"]["macro_area"]    = util_data.get("macro_area",    "-")
    result["util"]         = util_data
    result["std_util_str"] = util_data.get("std_util_str", "-/-")

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Cell Usage (LVT/RVT/HVT)
    cell_path = _find_rpt(rpt_dir, [
        "cell_usage.summary.{}.*.rpt".format(b),
        "cell_usage.summary.*.rpt"], finder)
    result["vth"] = parse_cell_usage(cell_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Clock Gating
    cgc_path = _find_rpt(rpt_dir, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"], finder)
    result["cgc"] = parse_clock_gating(cgc_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Multi-bit
    mbit_path = _find_rpt(rpt_dir, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"], finder)
    result["mbit"] = parse_multibit(mbit_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Congestion
    cong_path = _find_rpt(rpt_dir, [
        "congestion.{}.*.rpt".format(b), "congestion.*.rpt"], finder)
    result["congestion"] = parse_congestion(cong_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Power
    pwr_path = _find_rpt(rpt_dir, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"], finder)
    result["power"] = parse_power(pwr_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Runtime
    result["runtime"] = parse_fe_runtime(
        os.path.join(run_dir, "reports", "runtime.V2.rpt"))

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # Logic Depth
    ld_path = _find_rpt(
        rpt_dir, ["report_logic_depth.summary.*.rpt"], finder)
    result["logic_depth"] = parse_logic_depth(ld_path, verified=True)

    # Report file paths - for double-click "open in gvim" from dialogs
    _rt_path = os.path.join(run_dir, "reports", "runtime.V2.rpt")
    result["_paths"] = {
        "r2r_setup":     qor_path,
        "r2r_hold":      qor_path,
        "logic_depth":   ld_path,
        "cgc":           cgc_path,
        "mbit":          mbit_path,
        "congestion":    cong_path,
        "leakage":       pwr_path,
        "total_power":   pwr_path,
        "power.cell_internal": pwr_path,
        "power.net_switching": pwr_path,
        "power.total_dynamic": pwr_path,
        "power.leakage": pwr_path,
        "power.total":   pwr_path,
        "area":          area_path,
        "total_area":    area_path,
        "instance_count": area_path,
        "std_cell_area": util_path,
        "memory_area":   util_path,
        "macro_area":    util_path,
        "vth":           cell_path,
        "runtime":       _rt_path if os.path.exists(_rt_path) else None,
    }

    apply_flat_metrics(result, scope="FE")
    return result


def extract_pnr_stage_metrics(run_dir, stage_name, source="WS", block=None,
                              stage_path=None, cancel_check=None):
    """Extract QoR metrics for a single PNR stage."""
    result = {"stage": stage_name, "run_dir": run_dir}
    b = block or _get_block_name(run_dir)
    finder = _ReportFinder()

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    # New BE stage reports are stage-local and tool-specific.
    # Keep old FE-like fallbacks for compatibility with older runs.
    qor_sum = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.qor_group_sum.rpt".format(stage_name), "*.qor_group_sum.rpt"],
        stage_path=stage_path, finder=finder)
    use_group_sum = bool(qor_sum)
    if not qor_sum:
        qor_sum = _find_stage_rpt(
            run_dir, stage_name, source,
            ["{}.qor_sum.rpt".format(stage_name), "*.qor_sum.rpt"],
            stage_path=stage_path, finder=finder)
    qor_path = qor_sum
    if qor_sum:
        text = _read_stage_text(qor_sum)
        if use_group_sum:
            setup_total, setup_r2r = _parse_stage_fc_group_timing(text, "Setup violations")
            hold_total, hold_r2r = _parse_stage_fc_group_timing(text, "Hold violations")
        else:
            setup_total, setup_r2r = _parse_stage_fc_timing(text, "Setup violations")
            hold_total, hold_r2r = _parse_stage_fc_timing(text, "Hold violations")
        result["setup_total"] = setup_total
        result["setup_r2r"] = setup_r2r
        result["hold_total"] = hold_total
        result["hold_r2r"] = hold_r2r
        result["r2r_setup"] = setup_r2r
        result["r2r_hold"] = hold_r2r
    else:
        setup_path = _find_stage_rpt(
            run_dir, stage_name, source,
            ["{}_p*.summary.gz".format(stage_name),
             "{}_p*.summary".format(stage_name),
             "*_p*.summary.gz"],
            stage_path=stage_path, finder=finder)
        if setup_path:
            text = _read_stage_text(setup_path)
            setup_total, setup_r2r = _parse_stage_innovus_setup(text)
            result["setup_total"] = setup_total
            result["setup_r2r"] = setup_r2r
            result["r2r_setup"] = setup_r2r
            qor_path = setup_path
        hold_path = _find_stage_rpt(
            run_dir, stage_name, source,
            ["{}.qor.snap.rpt".format(stage_name), "*.qor.snap.rpt"],
            stage_path=stage_path, finder=finder)
        if hold_path:
            hold_all = _parse_stage_innovus_hold(_read_stage_text(hold_path))
            result["hold_all"] = hold_all
            result["r2r_hold"] = hold_all
            if not qor_path:
                qor_path = hold_path

    if "r2r_setup" not in result:
        old_qor = _find_stage_rpt(
            run_dir, stage_name, source,
            ["qor.{}.*.rpt".format(b), "qor.*.rpt"],
            stage_path=stage_path, finder=finder)
        qor_data = parse_qor(old_qor, verified=True)
        result["r2r_setup"] = qor_data.get("r2r_setup", "-")
        result["r2r_hold"] = qor_data.get("r2r_hold", "-")
        qor_path = old_qor

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    area_path = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.sec_get_area.rpt".format(stage_name), "*.sec_get_area.rpt"],
        stage_path=stage_path, finder=finder)
    stage_area = _parse_stage_area(_read_stage_text(area_path)) if area_path else {}
    if not stage_area:
        area_path = _find_stage_rpt(
            run_dir, stage_name, source,
            ["area.{}.*.rpt".format(b), "area.*.rpt"],
            stage_path=stage_path, finder=finder)
        stage_area = parse_area(area_path, verified=True)
    result["area"] = {
        "total_area":     stage_area.get("total_area",     "-"),
        "instance_count": stage_area.get(
            "instance_count", stage_area.get("std_cell_count", "-")),
        "std_cell_area":  stage_area.get("std_cell_area", "-"),
    }
    result["std_cell_count_area"] = stage_area.get("std_cell_count_area", "-")
    result["std_cell_only_util"] = stage_area.get("std_cell_only_util", "-")
    result["total_util"] = stage_area.get("total_util", "-")
    util_path = area_path
    util_data = {}
    if result["area"].get("std_cell_area", "-") == "-":
        util_path = _find_stage_rpt(
            run_dir, stage_name, source,
            ["utilization.{}.*.rpt".format(b), "utilization.*.rpt"],
            stage_path=stage_path, finder=finder)
        util_data = parse_utilization(util_path, verified=True)
        result["area"]["std_cell_area"] = util_data.get("std_cell_area", "-")
        result["area"]["memory_area"] = util_data.get("memory_area", "-")
        result["area"]["macro_area"] = util_data.get("macro_area", "-")
        result["std_util_str"] = util_data.get("std_util_str", "-/-")
        result["util"] = util_data
    else:
        result["std_util_str"] = "{}/{}".format(
            result["total_util"], result["std_cell_only_util"])
        result["util"] = {
            "std_cell_area": result["area"].get("std_cell_area", "-"),
            "std_util": result["std_util_str"],
            "std_util_str": result["std_util_str"],
        }

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    cell_path = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.sec_vth_use.rpt".format(stage_name), "*.sec_vth_use.rpt"],
        stage_path=stage_path, finder=finder)
    vth_data = _parse_stage_vth(_read_stage_text(cell_path)) if cell_path else {}
    if not vth_data:
        cell_path = _find_stage_rpt(
            run_dir, stage_name, source,
            ["cell_usage.summary.{}.*.rpt".format(b),
             "cell_usage.summary.*.rpt"],
            stage_path=stage_path, finder=finder)
        vth_data = parse_cell_usage(cell_path, verified=True)
    result["vth"] = vth_data

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    cgc_path = _find_stage_rpt(run_dir, stage_name, source, [
        "clock_gating_info.mission.rpt",
        "clock_gating_info.{}.*.rpt".format(b),
        "clock_gating_info*.rpt"], stage_path=stage_path, finder=finder)
    result["cgc"] = parse_clock_gating(cgc_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    mbit_path = _find_stage_rpt(run_dir, stage_name, source, [
        "multibit_banking_ratio.{}.*.rpt".format(b),
        "multibit_banking_ratio.*.rpt"], stage_path=stage_path, finder=finder)
    result["mbit"] = parse_multibit(mbit_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    cong_path = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.grc.rpt".format(stage_name), "*.grc.rpt",
         "congestion.{}.*.rpt".format(b), "congestion.*.rpt"],
        stage_path=stage_path, finder=finder)
    if cong_path and os.path.basename(cong_path).endswith(".grc.rpt"):
        result["congestion"] = {"cong_both": _parse_stage_grc(_read_stage_text(cong_path))}
    else:
        result["congestion"] = parse_congestion(cong_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    pwr_path = _find_stage_rpt(run_dir, stage_name, source, [
        "report_power_info.mission.ss*.rpt", "report_power*.rpt"],
        stage_path=stage_path, finder=finder)
    result["power"] = parse_power(pwr_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    ld_path = _find_stage_rpt(
        run_dir, stage_name, source, ["report_logic_depth.summary.*.rpt"],
        stage_path=stage_path, finder=finder)
    result["logic_depth"] = parse_logic_depth(ld_path, verified=True)

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    cts_path = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.cts.qor.final.rpt".format(stage_name), "*.cts.qor.final.rpt"],
        stage_path=stage_path, finder=finder)
    if cts_path:
        result.update(_parse_stage_cts(_read_stage_text(cts_path)))
        result["cts_report"] = cts_path

    if _cancelled(cancel_check):
        result["_cancelled"] = True
        return result

    runtime_path = _find_stage_rpt(
        run_dir, stage_name, source,
        ["{}.runtime.rpt".format(stage_name), "*.runtime.rpt"],
        stage_path=stage_path, finder=finder)
    result["runtime"] = _parse_stage_runtime(
        _read_stage_text(runtime_path)) if runtime_path else "-"

    result["_paths"] = {
        "r2r_setup":     qor_path,
        "r2r_hold":      qor_path,
        "logic_depth":   ld_path,
        "cgc":           cgc_path,
        "mbit":          mbit_path,
        "congestion":    cong_path,
        "leakage":       pwr_path,
        "total_power":   pwr_path,
        "power.cell_internal": pwr_path,
        "power.net_switching": pwr_path,
        "power.total_dynamic": pwr_path,
        "power.leakage": pwr_path,
        "power.total":   pwr_path,
        "area":          area_path,
        "total_area":    area_path,
        "instance_count": area_path,
        "std_cell_area": util_path,
        "memory_area":   util_path,
        "macro_area":    util_path,
        "vth":           cell_path,
        "skew_latency":  cts_path,
        "runtime":       runtime_path,
    }

    apply_flat_metrics(result, scope="PNR")
    return result
