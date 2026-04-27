import os
import re
import glob
import pwd
import subprocess
import concurrent.futures
import threading
import datetime
import getpass

from PyQt5.QtCore import QThread, pyqtSignal

try:
    from metric_extract import extract_fe_metrics, extract_pnr_stage_metrics
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False


def _format_size_bytes(total_size):
    if not total_size or total_size <= 0:
        return "N/A"
    for unit in ['K', 'M', 'G']:
        total_size /= 1024.0
        if total_size < 1024.0:
            return "{:.1f}{}".format(total_size, unit)
    return "{:.1f}T".format(total_size)


def _du_size(path, timeout_sec=180):
    """Fast filesystem size using system du. Falls back to None on timeout/error."""
    if not path or not os.path.exists(path):
        return "N/A"
    try:
        out = subprocess.check_output(
            ['du', '-sk', path], stderr=subprocess.DEVNULL,
            timeout=timeout_sec)
        line = out.decode('utf-8', errors='ignore').splitlines()[0]
        kb = int(line.split()[0])
        return _format_size_bytes(kb * 1024)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Lazy constant resolution -- these are defined in main.py at module level
# and available in the process globals by the time workers are started.
# ---------------------------------------------------------------------------
def _g(name, default=""):
    import builtins
    return getattr(builtins, name,
           globals().get(name, default))

def _BASE_WS_FE():   return _g("BASE_WS_FE_DIR")
def _BASE_WS_BE():   return _g("BASE_WS_BE_DIR")
def _BASE_OUTFEED(): return _g("BASE_OUTFEED_DIR")
def _BASE_IR():      return _g("BASE_IR_DIR", "")
def _PROJECT():      return _g("PROJECT_PREFIX", "S5K2P5SP")
def _PNR_TOOLS():    return _g("PNR_TOOL_NAMES", "fc innovus")
def _bool_cfg(name, default=False):
    val = _g(name, default)
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")
def _SCAN_IR_ON_START():      return _bool_cfg("SCAN_IR_ON_START", False)
def _SCAN_OWNER_ON_START():   return _bool_cfg("SCAN_OWNER_ON_START", False)
def _SCAN_SIGNOFF_ON_START(): return _bool_cfg("SCAN_SIGNOFF_ON_START", False)
def _SIGNOFF_BG_WORKERS():
    try:
        return max(1, int(_g("SIGNOFF_BG_WORKERS", 6)))
    except Exception:
        return 6
def _BLOCKS():
    """Return frozenset of allowed block names, or empty frozenset (= scan all)."""
    b = _g("BLOCKS", set())
    if isinstance(b, (set, frozenset)):
        return frozenset(b)
    # If injected as a string (edge case), parse it
    return frozenset(s.strip() for s in str(b).split(',') if s.strip())


# ---------------------------------------------------------------------------
# Path / file utilities (self-contained copies so workers.py has no deps)
# ---------------------------------------------------------------------------
_path_cache      = {}
_path_cache_lock = threading.Lock()

def cached_exists(path):
    with _path_cache_lock:
        if path in _path_cache:
            return _path_cache[path]
    result = os.path.exists(path)
    with _path_cache_lock:
        _path_cache[path] = result
    return result

def clear_path_cache():
    with _path_cache_lock:
        _path_cache.clear()

def prefetch_path_cache(paths):
    unique = [p for p in set(paths) if p]
    if not unique:
        return
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(30, len(unique))) as ex:
        results = list(ex.map(os.path.exists, unique))
    with _path_cache_lock:
        for p, r in zip(unique, results):
            _path_cache[p] = r

def get_owner(path):
    if not path or not cached_exists(path):
        return "Unknown"
    try:
        return pwd.getpwuid(os.stat(path).st_uid).pw_name
    except Exception:
        return "Unknown"

def normalize_rtl(rtl_str):
    pfx = _PROJECT()
    if rtl_str and rtl_str.startswith("EVT"):
        return f"{pfx}_{rtl_str}"
    return rtl_str

def get_milestone_label(rtl_str):
    """Returns milestone string or None."""
    _map = _g("_MILESTONE_MAP_GLOBAL", None)
    if _map:
        for tag, label in _map.items():
            if tag in rtl_str:
                return label
    if "_ML1_" in rtl_str: return "INITIAL RELEASE"
    if "_ML2_" in rtl_str: return "PRE-SVP"
    if "_ML3_" in rtl_str: return "SVP"
    if "_ML4_" in rtl_str: return "FFN"
    return None

def get_dynamic_evt_path(rtl_tag, block_name):
    m = re.search(r"(EVT\d+_ML\d+_DEV\d+)", str(rtl_tag))
    if not m:
        return ""
    return os.path.join(_BASE_OUTFEED(), block_name, m.group(1))

def get_outfeed_evt_base(run_dir):
    """Return {BASE_OUTFEED}/{BLK}/{EVT} for an OUTFEED fc/innovus run."""
    parts = os.path.normpath(run_dir).split(os.sep)
    for i, part in enumerate(parts):
        if part in ("fc", "innovus") and i >= 1:
            return os.sep.join(parts[:i])
    return os.path.dirname(run_dir)

def extract_rtl(run_dir):
    f = glob.glob(os.path.join(
        run_dir, "reports", "dump_variables.user_defined.*.rpt"))
    if not f:
        return "Unknown"
    try:
        with open(f[0], "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                m = re.search('\\s*all\\s*=\\s*"(.*?)"', line)
                if m and m.group(1).strip():   # guard: skip empty captures
                    return normalize_rtl(m.group(1))
    except Exception:
        pass
    return "Unknown"

def format_log_date(date_str):
    m = re.search(
        r"([A-Z][a-z]{2})\s+([A-Z][a-z]{2})\s+(\d+)\s+"
        r"(\d{2}:\d{2}:\d{2})\s+(\d{4})", str(date_str))
    if m:
        return (f"{m.group(1)} {m.group(2)} {m.group(3)}, "
                f"{m.group(5)} - {m.group(4)}")
    return str(date_str).strip()

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A",
         "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not cached_exists(file_path):
        return d
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "TOTAL_START" in line and "Load :" in line:
                    d["start"] = format_log_date(
                        line.split("Load :")[-1].strip())
                m = re.search(r"TimeStamp\s*:\s*(\S+)", line)
                if m and m.group(1) not in ("TOTAL", "TOTAL_START"):
                    d["last_stage"] = m.group(1)
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(
                        r"Total\s*:\s*(\d+)h:(\d+)m:(\d+)s", line)
                    if rt:
                        d["runtime"] = (f"{int(rt.group(1)):02}h:"
                                        f"{int(rt.group(2)):02}m:"
                                        f"{int(rt.group(3)):02}s")
                    if "Load :" in line:
                        d["end"] = format_log_date(
                            line.split("Load :")[-1].strip())
    except Exception:
        pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "-", "end": "-",
         "runtime": "-", "last_stage": "-"}
    if not file_path or not cached_exists(file_path):
        return d
    months = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    try:
        first_ts = last_ts = final_time_str = None
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                ts = re.search(
                    r"(\d{4})-(\d{2})-(\d{2})[ _](\d{2})-(\d{2})", line)
                tm = re.findall(
                    r"(\d+)d:(\d+)h:(\d+)m:(\d+)s", line)
                if ts and not first_ts:
                    first_ts = ts
                if ts:
                    last_ts = ts
                if ts and tm:
                    if not first_ts:
                        first_ts = ts
                    t = tm[1] if len(tm) > 1 else tm[0]
                    d2, h2, mn, sc = map(int, t)
                    final_time_str = (f"{d2*24+h2:02}h:"
                                      f"{mn:02}m:{sc:02}s")
        if first_ts:
            y, mo, dy, H, M = first_ts.groups()
            d["start"] = (f"{months[int(mo)-1]} {int(dy):02d}, "
                           f"{y} - {H}:{M}")
        if last_ts:
            y, mo, dy, H, M = last_ts.groups()
            d["end"] = (f"{months[int(mo)-1]} {int(dy):02d}, "
                         f"{y} - {H}:{M}")
        if final_time_str:
            d["runtime"] = final_time_str
    except Exception:
        pass
    return d

def get_fm_info(report_path):
    if not report_path or not cached_exists(report_path):
        return "N/A"
    try:
        with open(report_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "No failing compare points" in line:
                    return "PASS"
                m = re.search(r"(\d+)\s+Failing compare points", line)
                if m:
                    return f"{m.group(1)} FAILS"
    except Exception:
        pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not cached_exists(report_path):
        return "N/A"
    try:
        with open(report_path, "r", encoding="utf-8", errors="ignore") as f:
            in_summary = False
            for line in f:
                if "Management Summary" in line:
                    in_summary = True
                    continue
                if in_summary and line.strip().startswith("Total"):
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        return f"Error: {parts[1]}, Warning: {parts[2]}"
                    break
    except Exception:
        pass
    return "Not Found"
# ===========================================================================
# BatchSizeWorker -- calculates folder sizes for multiple items in background
# ===========================================================================
class BatchSizeWorker(QThread):
    # Batch signal: emits list[(item_id, size_str)] every 50 results
    # instead of one signal per item — prevents flooding the main-thread event queue.
    sizes_batch_ready = pyqtSignal(list)
    # Keep old signal for backward-compat with any direct callers
    size_calculated   = pyqtSignal(str, str)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self._is_cancelled = False

    def run(self):
        max_w = min(8, max(2, (os.cpu_count() or 4)))
        batch = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self.get_size, path): item_id
                       for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled:
                    break
                item_id = futures[future]
                try:
                    size_str = future.result()
                except Exception:
                    size_str = "N/A"
                batch.append((item_id, size_str))
                # Emit in chunks of 50 — ~10 signal deliveries vs 500
                if len(batch) >= 50:
                    self.sizes_batch_ready.emit(batch)
                    batch = []
        if batch and not self._is_cancelled:
            self.sizes_batch_ready.emit(batch)

    def get_size(self, path):
        fast = _du_size(path, timeout_sec=180)
        if fast is not None:
            return fast
        if not path or not os.path.exists(path):
            return "N/A"
        total_size = 0
        try:
            for entry in os.scandir(path):
                if self._is_cancelled:
                    return "N/A"
                try:
                    if entry.is_file(follow_symlinks=False):
                        total_size += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        total_size += self._calc_dir(entry.path)
                except Exception:
                    continue
        except Exception:
            return "N/A"
        return _format_size_bytes(total_size)

    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True


# ===========================================================================
# SignoffStatusWorker -- low-impact background FE FM/VSLP scan
# ===========================================================================
class SignoffStatusWorker(QThread):
    batch_ready = pyqtSignal(list)

    def __init__(self, runs):
        super().__init__()
        self.runs = list(runs or [])
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True

    def run(self):
        max_w = min(_SIGNOFF_BG_WORKERS(), len(self.runs))
        if max_w <= 0:
            return
        batch = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self._scan_one, r): r for r in self.runs}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled:
                    break
                try:
                    row = future.result()
                except Exception:
                    row = None
                if row:
                    batch.append(row)
                if len(batch) >= 25:
                    self.batch_ready.emit(batch)
                    batch = []
        if batch and not self._is_cancelled:
            self.batch_ready.emit(batch)

    def _scan_one(self, run):
        if self._is_cancelled:
            return None
        row = {
            "path": run.get("path", ""),
            "owner": get_owner(run.get("path", "")),
        }
        if run.get("run_type") == "FE":
            row["st_n"] = get_fm_info(run.get("fm_n_path", ""))
            row["st_u"] = get_fm_info(run.get("fm_u_path", ""))
            row["vslp_status"] = get_vslp_info(run.get("vslp_rpt_path", ""))
        return row


# ===========================================================================
# SingleSizeWorker -- calculates folder size for one item on demand
# ===========================================================================
class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)

    def __init__(self, item, path):
        super().__init__()
        self.item = item
        self.path = path
        self._is_cancelled = False

    def run(self):
        if self._is_cancelled or not self.path or not os.path.exists(self.path):
            self.result.emit(self.item, "N/A")
            return
        fast = _du_size(self.path, timeout_sec=180)
        if fast is not None:
            if not self._is_cancelled:
                self.result.emit(self.item, fast)
            return
        total_size = 0
        try:
            for entry in os.scandir(self.path):
                if self._is_cancelled:
                    self.result.emit(self.item, "N/A")
                    return
                try:
                    if entry.is_file(follow_symlinks=False):
                        total_size += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        total_size += self._calc_dir(entry.path)
                except Exception:
                    continue
            self.result.emit(self.item, _format_size_bytes(total_size))
        except Exception:
            if not self._is_cancelled:
                self.result.emit(self.item, "N/A")

    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True


# ===========================================================================
# DiskScannerWorker -- scans workspace/outfeed disk usage in background
# ===========================================================================
class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def _get_batch_dir_info(self, paths):
        results = []
        if not paths:
            return results
        try:
            cmd    = ['du', '-sk'] + paths
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=300).decode('utf-8', errors='ignore')
            for line in output.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    sz_kb     = int(parts[0])
                    full_path = parts[1]
                    try:
                        owner = pwd.getpwuid(os.stat(full_path).st_uid).pw_name
                    except:
                        owner = "Unknown"
                    results.append((owner, sz_kb, full_path))
        except:
            pass
        return results

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}

        # OUTFEED: outfeed/{BLOCK}/EVT*/fc/* and innovus/*
        outfeed_targets = glob.glob(os.path.join(_BASE_OUTFEED(), "*", "EVT*", "fc", "*"))
        outfeed_targets.extend(glob.glob(os.path.join(_BASE_OUTFEED(), "*", "EVT*", "innovus", "*")))
        if not outfeed_targets:
            outfeed_targets = glob.glob(os.path.join(_BASE_OUTFEED(), "*"))

        targets_map = {
            "WS (FE)": glob.glob(os.path.join(_BASE_WS_FE(), "*")),
            "WS (BE)": glob.glob(os.path.join(_BASE_WS_BE(), "*")),
            "OUTFEED":  outfeed_targets,
        }

        tasks = []
        for cat, paths in targets_map.items():
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                chunk = valid_paths[i:i + 50]
                tasks.append((cat, chunk))

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                cat = future_to_cat[future]
                try:
                    batch_results = future.result()
                    for owner, sz_kb, full_path in batch_results:
                        if sz_kb > 0:
                            gb_sz = sz_kb / (1024 ** 2)
                            if gb_sz > 0.01:
                                if owner not in results[cat]:
                                    results[cat][owner] = {"total": 0, "dirs": []}
                                results[cat][owner]["total"] += gb_sz
                                results[cat][owner]["dirs"].append((full_path, gb_sz))
                except:
                    pass

        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)

        self.finished_scan.emit(results)


# ===========================================================================
# ScannerWorker -- main workspace/outfeed scanner
# FIX 5: IR scan runs in PARALLEL with workspace scans via concurrent.futures
# ===========================================================================
def _find_report(rpt_dir, prefix, ext='.rpt'):
    """Find report matching prefix.BLOCKNAME.TIMESTAMP.rpt using glob.
    Report names like: check_timing.BLK_ISP2.20260416_1628.rpt
    Returns the most recently modified match, or None."""
    hits = glob.glob(os.path.join(rpt_dir, f"{prefix}.*{ext}"))
    if hits:
        return sorted(hits, key=os.path.getmtime)[-1]
    return None


class ScannerWorker(QThread):
    finished        = pyqtSignal(dict, dict, dict, dict)
    progress_update = pyqtSignal(int, int)
    status_update   = pyqtSignal(str)

    # -----------------------------------------------------------------------
    # IR directory scanner -- called as a parallel future inside run()
    # -----------------------------------------------------------------------
    def scan_ir_dir(self):
        ir_data    = {}
        target_lef = f"{_PROJECT()}.lef.list"
        ir_dirs    = _BASE_IR().split()

        for ir_base in ir_dirs:
            if not os.path.exists(ir_base):
                continue
            for root_dir, dirs, files in os.walk(ir_base):
                for f_name in files:
                    if not f_name.startswith("redhawk.log"):
                        continue
                    log_path = os.path.join(root_dir, f_name)

                    run_be_name = step_name = None
                    static_val  = dynamic_val = "-"
                    in_static   = in_dynamic  = False
                    static_lines, dynamic_lines = [], []

                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m:
                                        run_be_name = m.group(1)
                                        step_name   = m.group(2)

                                if "Worst Static IR Drop:" in line:
                                    in_static = True; in_dynamic = False
                                    static_lines.append(line.rstrip())
                                    continue
                                if "Worst Dynamic Voltage Drop:" in line:
                                    in_dynamic = True; in_static = False
                                    dynamic_lines.append(line.rstrip())
                                    continue

                                if in_static:
                                    if line.startswith("****") or line.startswith("Finish"):
                                        in_static = False
                                    elif line.strip():
                                        static_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and static_val == "-":
                                                static_val = parts[1]

                                if in_dynamic:
                                    if line.startswith("****") or line.startswith("Finish"):
                                        in_dynamic = False
                                    elif line.strip():
                                        dynamic_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and dynamic_val == "-":
                                                dynamic_val = parts[1]

                        if run_be_name and step_name:
                            key = f"{run_be_name}/{step_name}"
                            if key not in ir_data:
                                ir_data[key] = {
                                    "static": "-", "dynamic": "-",
                                    "log": log_path,
                                    "static_table": "", "dynamic_table": ""
                                }
                            if static_val  != "-": ir_data[key]["static"]  = static_val
                            if dynamic_val != "-": ir_data[key]["dynamic"] = dynamic_val
                            if static_lines:  ir_data[key]["static_table"]  = "\n".join(static_lines)
                            if dynamic_lines: ir_data[key]["dynamic_table"] = "\n".join(dynamic_lines)
                    except:
                        pass

        return ir_data

    # -----------------------------------------------------------------------
    # Workspace discovery helper
    # -----------------------------------------------------------------------
    def _scan_single_workspace(self, ws_base, ws_name, tools_to_scan):
        tasks           = []
        releases_found  = {}
        ws_path         = os.path.join(ws_base, ws_name)
        if not os.path.isdir(ws_path):
            return tasks, releases_found

        current_rtl = "Unknown"
        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            try:
                with open(sf, 'r', encoding='utf-8', errors='ignore') as f:
                    lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                    for l in set(lbls):
                        current_rtl = normalize_rtl(l)
                        if current_rtl not in releases_found:
                            releases_found[current_rtl] = []
                        releases_found[current_rtl].append(ws_path)
            except:
                pass

        for ent_path in glob.glob(os.path.join(ws_path, "IMPLEMENTATION", "*", "SOC", "*")):
            ent_name = os.path.basename(ent_path)
            if ws_base == _BASE_WS_FE():
                for rd in glob.glob(os.path.join(ent_path, "fc", "*-FE")):
                    tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "FE", None))
            if "fc" in tools_to_scan:
                for pat in ["*-BE", "EVT*_ML*_DEV*_*_*-BE"]:
                    for rd in glob.glob(os.path.join(ent_path, "fc", pat)):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))
            if "innovus" in tools_to_scan:
                # Catch all innovus run dirs -- not just EVT* named ones
                # TOP runs (S5K2P5SP SOC level) may have different naming
                for rd in glob.glob(os.path.join(ent_path, "innovus", "*")):
                    if os.path.isdir(rd) and not os.path.basename(rd).startswith('.'):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

        return tasks, releases_found

    # -----------------------------------------------------------------------
    # Main run -- FIX 5: IR scan launched in parallel with workspace scans
    # -----------------------------------------------------------------------
    def run(self):
        clear_path_cache()
        self.status_update.emit("Discovering Workspaces...")

        ws_data    = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data   = {"releases": {}, "blocks": set(), "all_runs": []}
        scan_stats = {"ws": 0, "outfeed": 0, "blocks": {}, "fc": 0, "innovus": 0}

        tasks          = []
        tools_to_scan  = _PNR_TOOLS().split()

        # --- Workspace discovery (parallel) ---
        disc_max_w = min(20, (os.cpu_count() or 4) * 4)
        disc_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=disc_max_w) as disc_ex:
            for ws_base in [_BASE_WS_FE(), _BASE_WS_BE()]:
                if not os.path.exists(ws_base):
                    continue
                try:
                    ws_names = os.listdir(ws_base)
                except:
                    continue
                for ws_name in ws_names:
                    disc_futures.append(
                        disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan)
                    )

            for future in concurrent.futures.as_completed(disc_futures):
                try:
                    new_tasks, new_releases = future.result()
                    tasks.extend(new_tasks)
                    for rtl, paths in new_releases.items():
                        for p in paths:
                            self._map_release(ws_data, rtl, p)
                except:
                    pass

        # --- Outfeed discovery ---
        self.status_update.emit("Discovering OUTFEED directories...")
        if os.path.exists(_BASE_OUTFEED()):
            for ent_name in os.listdir(_BASE_OUTFEED()):
                ent_path = os.path.join(_BASE_OUTFEED(), ent_name)
                if not os.path.isdir(ent_path):
                    continue

                # Expected outfeed structure:
                # outfeed/{BLOCK}/{EVT_LABEL}/fc/{run}/{run}-FE
                # outfeed/{BLOCK}/{EVT_LABEL}/fc/{run}-BE
                # outfeed/{BLOCK}/{EVT_LABEL}/innovus/{run}[-BE]
                evt_dirs_a = glob.glob(os.path.join(ent_path, "EVT*"))

                if evt_dirs_a:
                    for evt_dir in evt_dirs_a:
                        phys_evt = os.path.basename(evt_dir)
                        blk_name = ent_name
                        for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                            tasks.append((blk_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                        if "fc" in tools_to_scan:
                            for rd in glob.glob(os.path.join(evt_dir, "fc", "*-BE")):
                                tasks.append((blk_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                        if "innovus" in tools_to_scan:
                            for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                                if os.path.isdir(rd):
                                    tasks.append((blk_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        # --- Prefetch path cache ---
        paths_to_prefetch = []
        for t in tasks:
            rd = t[1]
            paths_to_prefetch.append(os.path.join(rd, "pass/compile_opt.pass"))
            paths_to_prefetch.append(os.path.join(rd, "logs/compile_opt.log"))
            paths_to_prefetch.append(os.path.join(rd, "reports/runtime.V2.rpt"))
        self.status_update.emit("Prefetching file metadata...")
        prefetch_path_cache(paths_to_prefetch)

        # --- Process runs + IR scan in PARALLEL ---
        total_tasks     = len(tasks)
        completed_tasks = 0
        max_w           = min(40, (os.cpu_count() or 4) * 6)

        self.status_update.emit("Processing run data...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:

            ir_future = (executor.submit(self.scan_ir_dir)
                         if _SCAN_IR_ON_START() else None)

            future_to_task = {executor.submit(self._thread_process_run, t): t for t in tasks}

            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        if result["source"] == "WS":
                            ws_data["blocks"].add(result["block"])
                            ws_data["all_runs"].append(result)
                            scan_stats["ws"] += 1
                            if result["run_type"] == "BE":
                                self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            scan_stats["outfeed"] += 1
                            self._map_release(out_data, result["rtl"], result["path"])

                        blk = result["block"]
                        if blk not in scan_stats["blocks"]:
                            scan_stats["blocks"][blk] = 0
                        scan_stats["blocks"][blk] += 1

                        if "/fc/" in result["path"]:
                            scan_stats["fc"] += 1
                        elif "/innovus/" in result["path"]:
                            scan_stats["innovus"] += 1
                except:
                    pass

                completed_tasks += 1
                # Throttle UI updates -- emit every 20 tasks to avoid flooding event loop
                if completed_tasks % 20 == 0 or completed_tasks == total_tasks:
                    self.progress_update.emit(completed_tasks, total_tasks)
                    self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")

            if ir_future:
                try:
                    ir_data = ir_future.result()
                except:
                    ir_data = {}
            else:
                ir_data = {}

        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

    # -----------------------------------------------------------------------
    # Per-task run processor (called in thread pool)
    # -----------------------------------------------------------------------
    def _thread_process_run(self, task_tuple):
        b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
        if source == "OUTFEED":
            rtl = self._resolve_outfeed_rtl(rd, phys_evt)
        else:
            per_run_rtl = extract_rtl(rd)
            rtl = per_run_rtl if (per_run_rtl and per_run_rtl != "Unknown") else base_rtl
            if rtl == "Unknown":
                rtl = base_rtl
        return self._process_run(b_name, rd, parent_path, rtl, source, run_type)

    def _resolve_outfeed_rtl(self, rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl):
            rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif not rtl or rtl == "Unknown":   # also catches empty-string result
            rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    def _process_run(self, b_name, rd, parent_path, rtl, source, run_type):
        r_name       = os.path.basename(rd)
        clean_run    = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)

        # Defensive fallback for malformed or manually supplied OUTFEED paths.
        # Normal scanning uses outfeed/{BLOCK}/{EVT_LABEL}/..., so b_name is known.
        if b_name == "UNKNOWN" and source == "OUTFEED":
            rpt_dir = os.path.join(rd, "reports")
            cu_hits = glob.glob(os.path.join(rpt_dir, "cell_usage.summary.*.rpt"))
            if cu_hits:
                # Filename: cell_usage.summary.BLK_ISP.20260416_1633.rpt
                fname = os.path.basename(cu_hits[0])
                parts = fname.split(".")
                # parts[2] is the block name (BLK_ISP)
                if len(parts) >= 4 and parts[2].startswith("BLK"):
                    b_name = parts[2]
            # Fallback: derive from run name prefix before milestone tag
            if b_name == "UNKNOWN":
                m_blk = re.search(r"(BLK_[A-Z0-9]+)", r_name.upper())
                if m_blk:
                    b_name = m_blk.group(1)

        # BLOCKS filter: if a whitelist is configured, skip blocks not in it
        _allowed_blocks = _BLOCKS()
        if _allowed_blocks and b_name not in _allowed_blocks:
            return None

        evt_base     = get_dynamic_evt_path(rtl, b_name)
        owner        = get_owner(rd) if _SCAN_OWNER_ON_START() else "Unknown"

        fm_n     = os.path.join(evt_base, "fm",   clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm",   clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre",   "reports", "report_lp.rpt")
        if run_type == "FE":
            info = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))
        else:
            info = {"start": "-", "end": "-",
                    "runtime": "-", "last_stage": "-"}

        is_comp   = True if source == "OUTFEED" else cached_exists(os.path.join(rd, "pass/compile_opt.pass"))
        fe_status = "RUNNING"

        if run_type == "FE":
            if is_comp:
                fe_status = "COMPLETED"
            else:
                log_file = os.path.join(rd, "logs/compile_opt.log")
                if not cached_exists(log_file):
                    fe_status = "NOT STARTED"
                else:
                    fe_status = "RUNNING"
                    try:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as lf:
                            for line in lf:
                                if "Stack trace for crashing thread" in line:
                                    fe_status = "FATAL ERROR"; break
                                if "Information: Process terminated by interrupt. (INT-4)" in line:
                                    fe_status = "INTERRUPTED"; break
                    except:
                        pass

        stages = []
        if run_type == "BE":
            is_innovus_run = "/innovus/" in rd.replace("\\", "/")
            if source == "WS" and is_innovus_run:
                search_glob = os.path.join(rd, "reports", "*")
            elif source == "WS":
                search_glob = os.path.join(rd, "outputs", "*")
            else:
                search_glob = os.path.join(rd, "*")
            for s_dir in glob.glob(search_glob):
                if not os.path.isdir(s_dir):
                    continue
                step_name = os.path.basename(s_dir)
                if step_name in ["logs", "pass", "fail", "outputs"]:
                    continue
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]:
                    continue

                is_fc = "/innovus/" not in rd.replace("\\", "/")
                # Build path candidates WITHOUT any os.path.exists() / glob during scan.
                # NFS stat calls here would block the scan worker for hundreds of ms per stage.
                # All path resolution is deferred to StageDetailWorker (background thread).
                if source == "WS":
                    stage_path = (os.path.join(rd, "outputs", step_name)
                                  if is_fc else os.path.join(rd, "reports", step_name))
                    if is_fc:
                        log = os.path.join(rd, step_name, "logs", f"{step_name}.log")
                        rpt_cands = [os.path.join(rd, "reports", step_name,
                                                   f"{step_name}.runtime.rpt")]
                    else:
                        log       = os.path.join(rd, "logs", f"{step_name}.log")
                        rpt_cands = [os.path.join(rd, "reports", step_name,
                                                   f"{step_name}.runtime.rpt")]
                else:
                    # OUTFEED: s_dir = rd/step_name
                    log        = os.path.join(s_dir, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, step_name)
                    if is_fc:
                        rpt_cands = [os.path.join(s_dir, "reports", step_name,
                                                   f"{step_name}.runtime.rpt")]
                    else:
                        rpt_cands = [os.path.join(s_dir, "reports", step_name,
                                                   f"{step_name}.runtime.rpt")]

                # FM/VSLP base + dir variants stored for lazy resolution in StageDetailWorker
                evt_base_stage = get_outfeed_evt_base(rd) if source == "OUTFEED" else evt_base
                sta_rpt  = os.path.join(evt_base_stage, "pt", r_name, step_name,
                                        "reports", "sta", "summary", "summary.rpt")
                qor_path = rd if rd.endswith("/") else rd + "/"

                stages.append({
                    "name":          step_name,
                    "rpt":           rpt_cands[0],   # primary (used as fallback)
                    "_rpt_cands":    rpt_cands,       # resolved lazily in StageDetailWorker
                    "log":           log,
                    # All deferred — filled by StageDetailWorker on expand
                    "info":          {"start": "-", "end": "-",
                                      "runtime": "-", "last_stage": "-"},
                    "st_n":          "-",
                    "st_u":          "-",
                    "vslp_status":   "-",
                    "fm_u_path":     "",
                    "fm_n_path":     "",
                    "vslp_rpt_path": "",
                    # Parameters for lazy FM/VSLP resolution (no NFS calls at scan time)
                    "_fm_base":      evt_base_stage,
                    "_fm_dirs":      [r_name, clean_be_run],
                    "_fm_step":      step_name,
                    "sta_rpt_path":  sta_rpt,
                    "qor_path":      qor_path,
                    "stage_path":    stage_path,
                    "_lazy":         True,
                })

        # Metrics are NOT extracted during scan (too slow).
        # They are extracted on-demand when user clicks "Show QoR Summary".
        # See MetricWorker in this file.

        return {
            "block":        b_name,
            "path":         rd,
            "parent":       parent_path,
            "rtl":          rtl,
            "r_name":       r_name,
            "run_type":     run_type,
            "stages":       stages,
            "source":       source,
            "owner":        owner,
            "is_comp":      is_comp,
            "fe_status":    fe_status,
            "st_n":         get_fm_info(fm_n) if _SCAN_SIGNOFF_ON_START() else "N/A",
            "st_u":         get_fm_info(fm_u) if _SCAN_SIGNOFF_ON_START() else "N/A",
            "vslp_status":  get_vslp_info(vslp_rpt) if _SCAN_SIGNOFF_ON_START() else "N/A",
            "info":         info,
            "fm_n_path":    fm_n,
            "fm_u_path":    fm_u,
            "vslp_rpt_path": vslp_rpt,
        }

    def _map_release(self, data_obj, rtl_str, path):
        if rtl_str not in data_obj["releases"]:
            data_obj["releases"][rtl_str] = []
        if path not in data_obj["releases"][rtl_str]:
            data_obj["releases"][rtl_str].append(path)
        base = re.sub(r'_syn\d+$', '', rtl_str)
        if base != rtl_str:
            if base not in data_obj["releases"]:
                data_obj["releases"][base] = []
            if path not in data_obj["releases"][base]:
                data_obj["releases"][base].append(path)


# ===========================================================================
# StageDetailWorker -- loads timing/FM/VSLP for all stages of ONE BE run
# Fired when user expands a BE run node. Deferred so scan stays fast.
# ===========================================================================
class StageDetailWorker(QThread):
    finished = pyqtSignal(object, list)   # (be_item_ref, enriched_stages)

    def __init__(self, be_run, be_item):
        super().__init__()
        self.be_run  = be_run
        self.be_item = be_item

    def run(self):
        enriched = []
        for s in self.be_run.get("stages", []):
            if not s.get("_lazy"):
                enriched.append(s)
                continue
            s2 = dict(s)

            # --- Runtime rpt: try candidates in order, pick first existing ---
            rpt_file = s["rpt"]
            for cand in s.get("_rpt_cands", [rpt_file]):
                if cached_exists(cand):
                    rpt_file = cand
                    break
            s2["info"] = parse_pnr_runtime_rpt(rpt_file)

            # --- FM paths: glob at expand time (deferred from scan) ---
            fm_base = s.get("_fm_base", "")
            step    = s.get("_fm_step", s["name"])
            fm_u_path = fm_n_path = ""
            if fm_base:
                for be_dir in s.get("_fm_dirs", []):
                    blk = self.be_run.get("block", "")
                    u_exact = os.path.join(
                        fm_base, "fm", be_dir, step, "n2upf_func", "reports",
                        "{}_n2upf_func.failpoint.rpt".format(blk))
                    n_exact = os.path.join(
                        fm_base, "fm", be_dir, step, "n2n_func", "reports",
                        "{}_n2n_func.failpoint.rpt".format(blk))
                    u_hits = [u_exact] if cached_exists(u_exact) else glob.glob(os.path.join(
                        fm_base, "fm", be_dir, step, "n2upf_func", "reports", "*.failpoint.rpt"))
                    n_hits = [n_exact] if cached_exists(n_exact) else glob.glob(os.path.join(
                        fm_base, "fm", be_dir, step, "n2n_func", "reports", "*.failpoint.rpt"))
                    if u_hits or n_hits:
                        fm_u_path = u_hits[0] if u_hits else ""
                        fm_n_path = n_hits[0] if n_hits else ""
                        break

            # --- VSLP: try candidates in order, pick first existing ---
            # Path: {fm_base}/fm/{run_dir}/{step}/pgnet/reports/report_lp.rpt
            vslp_path = ""
            if fm_base:
                for be_dir in s.get("_fm_dirs", []):
                    cand = os.path.join(
                        fm_base, "fm", be_dir, step, "pgnet", "reports", "report_lp.rpt")
                    if cached_exists(cand):
                        vslp_path = cand
                        break
                if not vslp_path:
                    # Default to first dir variant — get_vslp_info will return N/A if missing
                    dirs = s.get("_fm_dirs", [])
                    if dirs:
                        vslp_path = os.path.join(
                            fm_base, "fm", dirs[0], step, "pgnet", "reports", "report_lp.rpt")

            s2["fm_u_path"]     = fm_u_path
            s2["fm_n_path"]     = fm_n_path
            s2["vslp_rpt_path"] = vslp_path
            s2["st_n"]          = get_fm_info(fm_n_path)
            s2["st_u"]          = get_fm_info(fm_u_path)
            s2["vslp_status"]   = get_vslp_info(vslp_path)
            s2["_lazy"]         = False
            enriched.append(s2)
        self.finished.emit(self.be_item, enriched)


# ===========================================================================
# QoR WORKER -- calls summary.py as subprocess, opens HTML output in Firefox
# summary.py call signature: python3 summary.py <dir1> <dir2> ...
# It auto-detects FE vs BE from directory names ("FE" or "BE" in path).
# ===========================================================================
# ===========================================================================
# MetricWorker -- extracts QoR metrics ON DEMAND for a single run/stage
# Called only when user right-clicks and selects "Show QoR Summary"
# Never runs during the main scan, so scan stays fast.
# ===========================================================================
class MetricWorker(QThread):
    finished = pyqtSignal(dict)  # emits metrics dict when done

    def __init__(self, run_path, b_name, run_type, source,
                 stage_name=None):
        super().__init__()
        self.run_path   = run_path
        self.b_name     = b_name
        self.run_type   = run_type
        self.source     = source
        self.stage_name = stage_name  # None for FE, stage name for BE

    def run(self):
        if not _METRICS_AVAILABLE:
            self.finished.emit({})
            return
        try:
            if self.run_type == "FE":
                m = extract_fe_metrics(
                    self.run_path,
                    source=self.source,
                    block=self.b_name)
            else:
                m = extract_pnr_stage_metrics(
                    self.run_path, self.stage_name,
                    source=self.source,
                    block=self.b_name)
            self.finished.emit(m)
        except Exception as e:
            self.finished.emit({"_error": str(e)})


class MetricBatchWorker(QThread):
    finished = pyqtSignal(list)  # list of task dicts with metrics

    def __init__(self, tasks):
        super().__init__()
        self.tasks = list(tasks or [])

    def run(self):
        out = []
        if not _METRICS_AVAILABLE:
            self.finished.emit(out)
            return
        for task in self.tasks:
            row = dict(task)
            try:
                if task.get("run_type") == "FE":
                    row["metrics"] = extract_fe_metrics(
                        task.get("path", ""),
                        source=task.get("source", "WS"),
                        block=task.get("block", ""))
                else:
                    row["metrics"] = extract_pnr_stage_metrics(
                        task.get("path", ""),
                        task.get("stage_name", ""),
                        source=task.get("source", "WS"),
                        block=task.get("block", ""))
            except Exception as e:
                row["metrics"] = {"_error": str(e)}
            out.append(row)
        self.finished.emit(out)


class QoRWorker(QThread):
    finished = pyqtSignal(str)  # emits path to HTML output file, or ""

    def __init__(self, script_path, run_dirs, python_bin="python3.6"):
        super().__init__()
        self.script_path = script_path
        self.run_dirs    = run_dirs
        self.python_bin  = python_bin

    def run(self):
        try:
            # summary.py outputs to qor_metrices/summary_<date>.html
            # Run from the script's directory so relative paths work
            script_dir = os.path.dirname(os.path.abspath(self.script_path))
            cmd = [self.python_bin, self.script_path] + self.run_dirs
            result = subprocess.run(
                cmd,
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300
            )
            output = result.stdout.decode("utf-8", errors="ignore")
            # summary.py prints: [Info]: Refer to output html file: <path>
            html_path = ""
            for line in output.splitlines():
                if "Refer to output html file" in line or ".html" in line:
                    parts = line.split(":")
                    for p in parts:
                        p = p.strip()
                        if p.endswith(".html"):
                            html_path = p
                            break
                        if ".html" in p:
                            idx = p.find(".html")
                            html_path = p[:idx + 5].strip()
                            break
                if html_path:
                    break
            # If not absolute, make it relative to script_dir
            if html_path and not os.path.isabs(html_path):
                html_path = os.path.join(script_dir, html_path)
            self.finished.emit(html_path if (html_path and os.path.exists(html_path)) else "")
        except Exception as e:
            self.finished.emit("")
