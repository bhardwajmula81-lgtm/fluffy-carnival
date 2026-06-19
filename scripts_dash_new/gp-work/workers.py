import os
import re
import glob
import fnmatch
import json
import gzip
import pwd
import subprocess
import concurrent.futures
import threading
import datetime
import getpass
import time

from PyQt5.QtCore import QThread, pyqtSignal

try:
    from debug_log import debug_log
except Exception:
    def debug_log(context, exc=None):
        pass

try:
    from metric_extract import extract_fe_metrics, extract_pnr_stage_metrics
    from metric_registry import apply_flat_metrics
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False
    def apply_flat_metrics(metrics, scope=None, tool=None):
        return metrics


_METRIC_CACHE_LOCK = threading.Lock()
_METRIC_CACHE_DATA = None
_METRIC_CACHE_DIRTY = False
_METRIC_CACHE_VERSION = 1

def _atomic_write_gzip_json(path, data, sort_keys=False):
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except Exception:
            pass
    tmp = path + ".tmp.{}.{}".format(os.getpid(), int(time.time() * 1000000))
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(data, f, sort_keys=sort_keys)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _metric_cache_file():
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "dashboard_notes")
    try:
        if not os.path.exists(base):
            os.makedirs(base)
    except Exception:
        pass
    return os.path.join(base, "metrics_cache.json.gz")

def _load_metric_cache_locked():
    global _METRIC_CACHE_DATA
    if _METRIC_CACHE_DATA is not None:
        return _METRIC_CACHE_DATA
    fp = _metric_cache_file()
    data = {"version": _METRIC_CACHE_VERSION, "entries": {}}
    if os.path.exists(fp):
        try:
            with gzip.open(fp, "rt", encoding="utf-8", errors="ignore") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict) and isinstance(loaded.get("entries"), dict):
                data = loaded
        except Exception:
            data = {"version": _METRIC_CACHE_VERSION, "entries": {}}
    _METRIC_CACHE_DATA = data
    return _METRIC_CACHE_DATA

def _save_metric_cache():
    global _METRIC_CACHE_DIRTY
    with _METRIC_CACHE_LOCK:
        if not _METRIC_CACHE_DIRTY:
            return
        data = _load_metric_cache_locked()
        payload = {
            "version": _METRIC_CACHE_VERSION,
            "saved_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "entries": data.get("entries", {}),
        }
        _METRIC_CACHE_DIRTY = False
    try:
        fp = _metric_cache_file()
        _atomic_write_gzip_json(fp, payload, sort_keys=True)
    except Exception as e:
        debug_log("workers: save metric cache failed", e)
        with _METRIC_CACHE_LOCK:
            _METRIC_CACHE_DIRTY = True

def _metric_cache_key(run_path, block, run_type, source, stage_name=None,
                      stage_path=None):
    return "|".join([
        str(run_type or ""),
        str(source or ""),
        str(block or ""),
        os.path.normpath(str(run_path or "")),
        str(stage_name or ""),
        os.path.normpath(str(stage_path or "")),
    ])

def _dir_signature(path, patterns=None):
    if not path or not os.path.isdir(path):
        return None
    patterns = list(patterns or ["*"])
    count = 0
    max_mtime = 0.0
    total_size = 0
    try:
        for name in os.listdir(path):
            matched = False
            for pat in patterns:
                if fnmatch.fnmatch(name, pat):
                    matched = True
                    break
            if not matched:
                continue
            fp = os.path.join(path, name)
            try:
                st = os.stat(fp)
            except Exception:
                continue
            count += 1
            total_size += int(getattr(st, "st_size", 0))
            mt = float(getattr(st, "st_mtime", 0.0))
            if mt > max_mtime:
                max_mtime = mt
    except Exception:
        debug_log("workers: du size failed for {}".format(path))
        return None
    return [os.path.normpath(path), count, int(max_mtime), total_size]

def _stage_report_dirs(run_path, stage_name, source, stage_path=None):
    dirs = []
    for d in (
            os.path.join(run_path or "", "reports", stage_name or ""),
            os.path.join(stage_path or "", "reports"),
            os.path.join(stage_path or "", "reports", stage_name or ""),
            os.path.join(run_path or "", stage_name or "", "reports"),
            os.path.join(run_path or "", stage_name or "", "reports", stage_name or ""),
            os.path.join(run_path or "", "reports"),
            stage_path or ""):
        if d and d not in dirs:
            dirs.append(d)
    return dirs

def _metric_signature(run_path, run_type, source, stage_name=None,
                      stage_path=None):
    patterns = [
        "*.rpt", "*.rpt.gz", "*.summary", "*.summary.gz",
        "*.qor_group_sum.rpt", "*.qor_sum.rpt", "*.qor.snap.rpt", "*.grc.rpt",
    ]
    sig = []
    if run_type == "FE":
        sig.append(_dir_signature(os.path.join(run_path or "", "reports"),
                                  patterns))
    else:
        for d in _stage_report_dirs(run_path, stage_name, source, stage_path):
            ds = _dir_signature(d, patterns)
            if ds:
                sig.append(ds)
    return [x for x in sig if x]

def _metric_cache_get(key, sig):
    with _METRIC_CACHE_LOCK:
        data = _load_metric_cache_locked()
        entry = data.get("entries", {}).get(key)
        if not isinstance(entry, dict):
            return None
        if entry.get("sig") != sig and sig:
            return None
        metrics = entry.get("metrics")
        return metrics if isinstance(metrics, dict) else None

def _metric_cache_put(key, sig, metrics):
    global _METRIC_CACHE_DIRTY
    if not isinstance(metrics, dict) or metrics.get("_error"):
        return
    stored = dict(metrics)
    stored["_cache"] = "stored"
    with _METRIC_CACHE_LOCK:
        data = _load_metric_cache_locked()
        entries = data.setdefault("entries", {})
        entries[key] = {
            "sig": sig,
            "metrics": stored,
            "saved_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if len(entries) > 5000:
            # Keep cache bounded. Insertion order is stable in Python 3.6.
            for old_key in list(entries.keys())[:len(entries) - 5000]:
                entries.pop(old_key, None)
        _METRIC_CACHE_DIRTY = True

def _extract_metrics_cached(run_path, block, run_type, source,
                            stage_name=None, stage_path=None, cancel_check=None):
    if cancel_check and cancel_check():
        return {"_cancelled": True}
    sig = _metric_signature(run_path, run_type, source, stage_name, stage_path)
    key = _metric_cache_key(run_path, block, run_type, source,
                            stage_name, stage_path)
    cached = _metric_cache_get(key, sig)
    if cached is not None:
        out = dict(cached)
        apply_flat_metrics(out)
        out["_cache"] = "hit"
        return out
    if run_type == "FE":
        metrics = extract_fe_metrics(run_path, source=source, block=block, cancel_check=cancel_check)
    else:
        metrics = extract_pnr_stage_metrics(
            run_path, stage_name, source=source, block=block,
            stage_path=stage_path, cancel_check=cancel_check)
    if isinstance(metrics, dict):
        if metrics.get("_cancelled"):
            return metrics
        apply_flat_metrics(metrics)
        metrics["_cache"] = "miss"
        _metric_cache_put(key, sig, metrics)
    return metrics

def get_metric_cache_payload():
    with _METRIC_CACHE_LOCK:
        data = _load_metric_cache_locked()
        return {
            "version": _METRIC_CACHE_VERSION,
            "entries": dict(data.get("entries", {})),
        }

def merge_metric_cache_payload(payload):
    global _METRIC_CACHE_DIRTY
    if not isinstance(payload, dict):
        return 0
    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        return 0
    with _METRIC_CACHE_LOCK:
        data = _load_metric_cache_locked()
        target = data.setdefault("entries", {})
        added = 0
        for key, val in entries.items():
            if isinstance(val, dict):
                target[str(key)] = val
                added += 1
        _METRIC_CACHE_DIRTY = True
    _save_metric_cache()
    return added

def metric_cache_status():
    with _METRIC_CACHE_LOCK:
        data = _load_metric_cache_locked()
        entries = data.get("entries", {})
        return {
            "file": _metric_cache_file(),
            "entries": len(entries) if isinstance(entries, dict) else 0,
            "saved_at": data.get("saved_at", "-"),
        }

def clear_metric_cache():
    global _METRIC_CACHE_DATA, _METRIC_CACHE_DIRTY
    with _METRIC_CACHE_LOCK:
        _METRIC_CACHE_DATA = {"version": _METRIC_CACHE_VERSION, "entries": {}}
        _METRIC_CACHE_DIRTY = False
    try:
        fp = _metric_cache_file()
        if os.path.exists(fp):
            os.remove(fp)
    except Exception:
        pass


def _format_size_bytes(total_size):
    if not total_size or total_size <= 0:
        return "N/A"
    for unit in ['K', 'M', 'G']:
        total_size /= 1024.0
        if total_size < 1024.0:
            return "{:.1f}{}".format(total_size, unit)
    return "{:.1f}T".format(total_size)


def _du_size(path, timeout_sec=60):
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

def _QOR_TIMEOUT_SEC():
    try:
        return max(1, int(_g("QOR_TIMEOUT_SEC", 600)))
    except Exception:
        return 600

def _path_list(raw):
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set, frozenset)):
        vals = raw
    else:
        vals = re.split(r'[,;\s]+', str(raw))
    out = []
    seen = set()
    for val in vals:
        p = str(val or "").strip()
        if not p:
            continue
        key = os.path.normpath(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out

def _BASE_WS_FE():   return _g("BASE_WS_FE_DIR")
def _BASE_WS_BE():   return _g("BASE_WS_BE_DIR")
def _BASE_OUTFEED(): return _g("BASE_OUTFEED_DIR")
def _BASE_WS_FE_LIST():   return _path_list(_BASE_WS_FE())
def _BASE_WS_BE_LIST():   return _path_list(_BASE_WS_BE())
def _BASE_OUTFEED_LIST(): return _path_list(_BASE_OUTFEED())
def _is_ws_fe_base(path):
    try:
        n = os.path.normpath(path)
        return n in set(os.path.normpath(p) for p in _BASE_WS_FE_LIST())
    except Exception:
        return path == _BASE_WS_FE()
def _BASE_IR():      return _g("BASE_IR_DIR", "")
def _PROJECT():      return _g("PROJECT_PREFIX", "S5K2P5SP")
def _PNR_TOOLS():    return _g("PNR_TOOL_NAMES", "fc innovus")
def _IGNORE_FE_RUN_PATTERNS():
    return _pattern_list(_g("IGNORE_FE_RUN_PATTERNS", ""))
def _IGNORE_BE_RUN_PATTERNS():
    return _pattern_list(_g("IGNORE_BE_RUN_PATTERNS", ""))
def _IGNORE_PNR_STAGE_PATTERNS():
    return _pattern_list(_g("IGNORE_PNR_STAGE_PATTERNS", "backup_*"))
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

def _pattern_list(raw):
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set, frozenset)):
        return [str(x).strip() for x in raw if str(x).strip()]
    parts = re.split(r'[,;\s]+', str(raw))
    return [p.strip() for p in parts if p.strip()]

def _ignored_by_pattern(name, patterns):
    base = os.path.basename(str(name or ""))
    clean = base
    for suffix in ("-FE", "-BE"):
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)]
            break
    for pat in patterns or []:
        try:
            if fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(clean, pat):
                return True
        except Exception:
            pass
    return False


# ---------------------------------------------------------------------------
# Path / file utilities (self-contained copies so workers.py has no deps)
# ---------------------------------------------------------------------------
_path_cache      = {}
_path_cache_lock = threading.Lock()
_owner_cache      = {}
_owner_cache_lock = threading.Lock()
_PATH_CACHE_MAX = 50000
_OWNER_CACHE_MAX = 10000

def _bounded_cache_put(cache, key, value, limit):
    cache[key] = value
    try:
        while len(cache) > limit:
            cache.pop(next(iter(cache)))
    except Exception:
        cache.clear()

def cached_exists(path):
    with _path_cache_lock:
        if path in _path_cache:
            return _path_cache[path]
    result = os.path.exists(path)
    with _path_cache_lock:
        _bounded_cache_put(_path_cache, path, result, _PATH_CACHE_MAX)
    return result

def clear_path_cache():
    with _path_cache_lock:
        _path_cache.clear()
    with _owner_cache_lock:
        _owner_cache.clear()

def prefetch_path_cache(paths):
    unique = [p for p in set(paths) if p]
    if not unique:
        return
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(30, len(unique))) as ex:
        results = list(ex.map(os.path.exists, unique))
    with _path_cache_lock:
        for p, r in zip(unique, results):
            _bounded_cache_put(_path_cache, p, r, _PATH_CACHE_MAX)

def get_owner(path):
    if not path or not cached_exists(path):
        return "Unknown"
    norm = os.path.normpath(path)
    with _owner_cache_lock:
        if norm in _owner_cache:
            return _owner_cache[norm]
    try:
        owner = pwd.getpwuid(os.stat(norm).st_uid).pw_name or "Unknown"
    except Exception:
        owner = "Unknown"
    with _owner_cache_lock:
        _bounded_cache_put(_owner_cache, norm, owner, _OWNER_CACHE_MAX)
    return owner

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
    evt = m.group(1)
    candidates = [os.path.join(base, block_name, evt)
                  for base in _BASE_OUTFEED_LIST()]
    for cand in candidates:
        if os.path.isdir(cand):
            return cand
    return candidates[0] if candidates else ""

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

def _parse_pnr_runtime_rpt_body(file_path):
    d = {"start": "-", "end": "-",
         "runtime": "-", "last_stage": "-"}
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
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
                    final_time_str = ("%02dh:%02dm:%02ds" %
                                      (d2 * 24 + h2, mn, sc))
        if first_ts:
            y, mo, dy, H, M = first_ts.groups()
            d["start"] = ("%s %02d, %s - %s:%s" %
                           (months[int(mo) - 1], int(dy), y, H, M))
        if last_ts:
            y, mo, dy, H, M = last_ts.groups()
            d["end"] = ("%s %02d, %s - %s:%s" %
                         (months[int(mo) - 1], int(dy), y, H, M))
        if final_time_str:
            d["runtime"] = final_time_str
    except Exception:
        pass
    return d


def parse_pnr_runtime_rpt(file_path):
    if not file_path or not cached_exists(file_path):
        return {"start": "-", "end": "-", "runtime": "-", "last_stage": "-"}
    return _parse_pnr_runtime_rpt_body(file_path)


def parse_pnr_runtime_rpt_uncached(file_path):
    """Branch Status uses fresh filesystem checks to avoid stale cache reads."""
    if not file_path or not os.path.exists(file_path):
        return {"start": "-", "end": "-", "runtime": "-", "last_stage": "-"}
    return _parse_pnr_runtime_rpt_body(file_path)

def _parse_stage_start_sort_value(info):
    txt = str((info or {}).get("start", "") or "")
    m = re.search(
        r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{1,2}):(\d{2})",
        txt)
    if not m:
        return None
    months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5,
              "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
              "Nov": 11, "Dec": 12}
    return (int(m.group(3)), months.get(m.group(1), 12),
            int(m.group(2)), int(m.group(4)), int(m.group(5)))

def _parse_fc_stage_marker(log_path, fallback_stage):
    marker = ""
    if not log_path or not os.path.exists(log_path):
        return marker
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = re.search(r"TimeStamp\s*:\s*(\S+)", line)
                if not m:
                    continue
                val = m.group(1).strip()
                if val and val not in ("TOTAL", "TOTAL_START"):
                    marker = val
    except Exception:
        marker = ""
    return marker

def _parse_innovus_stage_marker(log_path, fallback_stage):
    marker = ""
    if not log_path or not os.path.exists(log_path):
        return marker
    pat = re.compile(
        r"^\s*(?:@file\s+\d+\s*:\s*)?sec_StartTimer\s+([A-Za-z0-9_./-]+)\b")
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                m = pat.search(stripped)
                if m:
                    marker = m.group(1).strip()
    except Exception:
        marker = ""
    return marker


def _unique_existing_order(values):
    out = []
    seen = set()
    for val in values or []:
        if not val:
            continue
        key = os.path.normpath(val)
        if key in seen:
            continue
        seen.add(key)
        out.append(val)
    return out


def _stage_runtime_candidates(run_path, stage):
    stage = stage or {}
    step = stage.get("name", "")
    stage_path = stage.get("stage_path", "")
    vals = []
    vals.extend(stage.get("_rpt_cands", []) or [])
    if stage.get("rpt"):
        vals.append(stage.get("rpt"))
    if run_path and step:
        vals.append(os.path.join(run_path, "reports", step, "%s.runtime.rpt" % step))
        vals.append(os.path.join(run_path, "reports", "%s.runtime.rpt" % step))
        vals.append(os.path.join(run_path, step, "reports", step, "%s.runtime.rpt" % step))
        vals.append(os.path.join(run_path, step, "reports", "%s.runtime.rpt" % step))
        vals.append(os.path.join(run_path, step, "%s.runtime.rpt" % step))
    if stage_path and step:
        vals.append(os.path.join(stage_path, "reports", step, "%s.runtime.rpt" % step))
        vals.append(os.path.join(stage_path, "reports", "%s.runtime.rpt" % step))
        vals.append(os.path.join(stage_path, "%s.runtime.rpt" % step))
    return _unique_existing_order(vals)


def _stage_pass_candidates(run_path, stage):
    stage = stage or {}
    step = stage.get("name", "")
    vals = []
    if stage.get("pass_path"):
        vals.append(stage.get("pass_path"))
    if run_path and step:
        vals.append(os.path.join(run_path, "pass", "%s.pass" % step))
    return _unique_existing_order(vals)

def resolve_pnr_stage_status(stage, be_run):
    stage = dict(stage or {})
    be_run = be_run or {}
    source = be_run.get("source", "WS")
    step_name = stage.get("name", "")
    log_path = stage.get("log", "")
    pass_path = stage.get("pass_path", "")
    is_innovus = bool(stage.get("_is_innovus"))

    if source == "OUTFEED":
        return "COMPLETED", "COMPLETED", pass_path

    pass_candidates = list(stage.get("_pass_candidates", []) or [])
    if pass_path:
        pass_candidates.insert(0, pass_path)
    for cand in _unique_existing_order(pass_candidates):
        if os.path.exists(cand):
            return "COMPLETED", "COMPLETED", cand

    marker = (_parse_innovus_stage_marker(log_path, step_name)
              if is_innovus else _parse_fc_stage_marker(log_path, step_name))
    active_stage = marker or step_name
    if marker and log_path and os.path.exists(log_path):
        return "RUNNING", marker, pass_path
    return "NOT STARTED", active_stage, pass_path

def get_fm_info(report_path):
    if not report_path or not cached_exists(report_path):
        return "N/A"
    try:
        with open(report_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if re.search(r"No\s+failing\s+compare\s+points?", line, re.IGNORECASE):
                    return "PASS"
                m = re.search(r"(\d+)\s+Failing\s+compare\s+points?", line, re.IGNORECASE)
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
    # instead of one signal per item - prevents flooding the main-thread event queue.
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
                # Emit in chunks of 50 - about 10 signal deliveries vs 500
                if len(batch) >= 50:
                    self.sizes_batch_ready.emit(batch)
                    batch = []
        if batch and not self._is_cancelled:
            self.sizes_batch_ready.emit(batch)

    def get_size(self, path):
        fast = _du_size(path, timeout_sec=60)
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
                if self._is_cancelled or self.isInterruptionRequested():
                    return total
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass


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
        try:
            self.requestInterruption()
        except Exception:
            pass

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
# OwnerLookupWorker -- lightweight background Unix owner lookup
# ===========================================================================
class OwnerLookupWorker(QThread):
    batch_ready = pyqtSignal(list)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = list(tasks or [])
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self):
        if not self.tasks:
            return
        max_w = min(8, max(1, len(self.tasks)))
        batch = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {
                executor.submit(get_owner, t.get("path", "")): t
                for t in self.tasks
            }
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled:
                    break
                task = futures[future]
                try:
                    owner = future.result()
                except Exception:
                    owner = "Unknown"
                row = dict(task)
                row["owner"] = owner
                batch.append(row)
                if len(batch) >= 50:
                    self.batch_ready.emit(batch)
                    batch = []
        if batch and not self._is_cancelled:
            self.batch_ready.emit(batch)


# ===========================================================================
# SingleSizeWorker -- calculates folder size for one item on demand
# ===========================================================================
class SingleSizeWorker(QThread):
    result = pyqtSignal(str, str)

    def __init__(self, item_id, path):
        super().__init__()
        self.item_id = item_id
        self.path = path
        self._is_cancelled = False

    def run(self):
        if self._is_cancelled or not self.path or not os.path.exists(self.path):
            self.result.emit(self.item_id, "N/A")
            return
        fast = _du_size(self.path, timeout_sec=60)
        if fast is not None:
            if not self._is_cancelled:
                self.result.emit(self.item_id, fast)
            return
        total_size = 0
        try:
            for entry in os.scandir(self.path):
                if self._is_cancelled:
                    self.result.emit(self.item_id, "N/A")
                    return
                try:
                    if entry.is_file(follow_symlinks=False):
                        total_size += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        total_size += self._calc_dir(entry.path)
                except Exception:
                    continue
            self.result.emit(self.item_id, _format_size_bytes(total_size))
        except Exception:
            if not self._is_cancelled:
                self.result.emit(self.item_id, "N/A")

    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if self._is_cancelled or self.isInterruptionRequested():
                    return total
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass


# ===========================================================================
# DiskScannerWorker -- scans workspace/outfeed disk usage in background
# ===========================================================================
class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def __init__(self, run_targets=None, disk_cache=None, force=False):
        super().__init__()
        self._is_cancelled = False
        self.run_targets = list(run_targets or [])
        self.disk_cache = dict(disk_cache or {})
        self.force = bool(force)

    def cancel(self):
        self._is_cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def _get_batch_dir_info(self, paths):
        results = []
        if self._is_cancelled or self.isInterruptionRequested() or not paths:
            return results
        try:
            cmd    = ['du', '-sk'] + paths
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=60).decode('utf-8', errors='ignore')
            for line in output.strip().split('\n'):
                if self._is_cancelled or self.isInterruptionRequested():
                    return results
                if not line:
                    continue
                parts = line.split(None, 1)
                if len(parts) >= 2:
                    try:
                        sz_kb     = int(parts[0])
                        full_path = parts[1]
                        owner = get_owner(full_path)
                        results.append((owner, sz_kb, full_path, "updated"))
                    except Exception as e:
                        debug_log("DiskScannerWorker: failed to parse du line {}".format(line), e)
        except Exception as e:
            debug_log("DiskScannerWorker: batch du failed, using scandir fallback", e)
            for path in paths:
                if self._is_cancelled or self.isInterruptionRequested():
                    return results
                sz_kb = self._scandir_size_kb(path)
                if sz_kb is None:
                    continue
                results.append((get_owner(path), sz_kb, path, "fallback"))
        return results

    def _scandir_size_kb(self, path):
        if not path or not os.path.exists(path):
            return None
        total = self._scandir_size_bytes(path)
        if total is None:
            return None
        return int((total + 1023) / 1024)

    def _scandir_size_bytes(self, path):
        if self._is_cancelled or self.isInterruptionRequested():
            return None
        total = 0
        try:
            for entry in os.scandir(path):
                if self._is_cancelled or self.isInterruptionRequested():
                    return None
                try:
                    if entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        child = self._scandir_size_bytes(entry.path)
                        if child is not None:
                            total += child
                except Exception:
                    continue
        except Exception as e:
            debug_log("DiskScannerWorker: scandir size failed {}".format(path), e)
            return None
        return total

    def _category_for_run(self, run):
        src = run.get("source", "WS")
        rtype = run.get("run_type", "")
        if src == "OUTFEED":
            return "OUTFEED"
        if rtype == "BE":
            return "WS (BE)"
        return "WS (FE)"

    def _build_data_from_cache(self, cache):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        for _path, rec in (cache or {}).items():
            try:
                if not rec.get("exists", True):
                    continue
                gb_sz = float(rec.get("size_gb", 0.0) or 0.0)
                if gb_sz <= 0.01:
                    continue
                cat = rec.get("category") or "WS (FE)"
                owner = rec.get("owner") or "Unknown"
                full_path = rec.get("path") or _path
                if cat not in results:
                    results[cat] = {}
                if owner not in results[cat]:
                    results[cat][owner] = {"total": 0, "dirs": []}
                results[cat][owner]["total"] += gb_sz
                results[cat][owner]["dirs"].append((full_path, gb_sz))
            except Exception:
                continue
        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)
        return results

    def _run_incremental_cache_scan(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cache = {}
        current = {}
        for run in self.run_targets:
            if self._is_cancelled or self.isInterruptionRequested():
                return None
            path = os.path.normpath(str(run.get("path", "") or ""))
            if not path or path == "N/A":
                continue
            current[path] = run
        for path, rec in self.disk_cache.items():
            npath = os.path.normpath(str(path or ""))
            if npath in current:
                cache[npath] = dict(rec or {})
                cache[npath]["exists"] = True
        pending = []
        for path, run in current.items():
            rec = cache.get(path)
            stale_owner = rec and str(rec.get("owner", "")).strip() in ("", "Unknown")
            if self.force or not rec or stale_owner:
                pending.append(path)
            else:
                rec["path"] = path
                rec["source"] = run.get("source", "")
                rec["run_type"] = run.get("run_type", "")
                rec["block"] = run.get("block", "")
                rec["rtl"] = run.get("rtl", "")
                rec["run_name"] = run.get("r_name", "")
                rec["category"] = self._category_for_run(run)
                rec["size_status"] = rec.get("size_status") or "cached"
        for i in range(0, len(pending), 25):
            if self._is_cancelled or self.isInterruptionRequested():
                return None
            chunk = pending[i:i + 25]
            for owner, sz_kb, full_path, size_status in self._get_batch_dir_info(chunk):
                if self._is_cancelled or self.isInterruptionRequested():
                    return None
                path = os.path.normpath(full_path)
                run = current.get(path, {})
                run_owner = str(run.get("owner", "") or "").strip()
                calc_owner = str(owner or "").strip()
                final_owner = calc_owner if calc_owner and calc_owner != "Unknown" else (run_owner or "Unknown")
                cache[path] = {
                    "path": path,
                    "size_gb": sz_kb / float(1024 ** 2),
                    "owner": final_owner,
                    "source": run.get("source", ""),
                    "run_type": run.get("run_type", ""),
                    "block": run.get("block", ""),
                    "rtl": run.get("rtl", ""),
                    "run_name": run.get("r_name", ""),
                    "category": self._category_for_run(run),
                    "exists": True,
                    "size_status": size_status,
                    "updated_at": now,
                }
        data = self._build_data_from_cache(cache)
        data["__cache__"] = cache
        data["__pending_count__"] = len(pending)
        return data

    def run(self):
        if self.run_targets:
            data = self._run_incremental_cache_scan()
            if data is not None and not self._is_cancelled and not self.isInterruptionRequested():
                self.finished_scan.emit(data)
            return

        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        if self._is_cancelled or self.isInterruptionRequested():
            self.finished_scan.emit(results)
            return

        # OUTFEED: outfeed/{BLOCK}/EVT*/fc/* and innovus/*
        outfeed_targets = []
        for out_base in _BASE_OUTFEED_LIST():
            outfeed_targets.extend(glob.glob(os.path.join(out_base, "*", "EVT*", "fc", "*")))
            outfeed_targets.extend(glob.glob(os.path.join(out_base, "*", "EVT*", "innovus", "*")))
        if not outfeed_targets:
            for out_base in _BASE_OUTFEED_LIST():
                outfeed_targets.extend(glob.glob(os.path.join(out_base, "*")))

        targets_map = {
            "WS (FE)": [p for base in _BASE_WS_FE_LIST()
                        for p in glob.glob(os.path.join(base, "*"))],
            "WS (BE)": [p for base in _BASE_WS_BE_LIST()
                        for p in glob.glob(os.path.join(base, "*"))],
            "OUTFEED":  outfeed_targets,
        }

        tasks = []
        for cat, paths in targets_map.items():
            if self._is_cancelled or self.isInterruptionRequested():
                self.finished_scan.emit(results)
                return
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                chunk = valid_paths[i:i + 50]
                tasks.append((cat, chunk))

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                if self._is_cancelled or self.isInterruptionRequested():
                    break
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
                except Exception as e:
                    debug_log("DiskScannerWorker: batch directory size failed", e)

        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)

        if not self._is_cancelled and not self.isInterruptionRequested():
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

    def __init__(self):
        super().__init__()
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def _cancel_requested(self):
        return self._is_cancelled or self.isInterruptionRequested()

    # -----------------------------------------------------------------------
    # IR directory scanner -- called as a parallel future inside run()
    # -----------------------------------------------------------------------
    def scan_ir_dir(self):
        ir_data    = {}
        target_lef = f"{_PROJECT()}.lef.list"
        ir_dirs    = _BASE_IR().split()

        for ir_base in ir_dirs:
            if self._cancel_requested():
                return {}
            if not os.path.exists(ir_base):
                continue
            for root_dir, dirs, files in os.walk(ir_base):
                if self._cancel_requested():
                    return {}
                for f_name in files:
                    if self._cancel_requested():
                        return {}
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
        if self._cancel_requested():
            return tasks, releases_found
        ws_path         = os.path.join(ws_base, ws_name)
        if not os.path.isdir(ws_path):
            return tasks, releases_found

        current_rtl = "Unknown"
        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            if self._cancel_requested():
                return tasks, releases_found
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
            if self._cancel_requested():
                return tasks, releases_found
            ent_name = os.path.basename(ent_path)

            fc_path = os.path.join(ent_path, "fc")
            fc_entries = []
            try:
                for entry in os.scandir(fc_path):
                    if self._cancel_requested():
                        return tasks, releases_found
                    try:
                        if entry.name.startswith('.') or not entry.is_dir():
                            continue
                        fc_entries.append((entry.name, entry.path))
                    except Exception:
                        pass
            except Exception:
                fc_entries = []

            if _is_ws_fe_base(ws_base):
                for name, rd in fc_entries:
                    if self._cancel_requested():
                        return tasks, releases_found
                    if not name.endswith("-FE"):
                        continue
                    if _ignored_by_pattern(name, _IGNORE_FE_RUN_PATTERNS()):
                        continue
                    tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "FE", None))

            if "fc" in tools_to_scan:
                for name, rd in fc_entries:
                    if self._cancel_requested():
                        return tasks, releases_found
                    if not name.endswith("-BE"):
                        continue
                    if _ignored_by_pattern(name, _IGNORE_BE_RUN_PATTERNS()):
                        continue
                    tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

            if "innovus" in tools_to_scan:
                inv_path = os.path.join(ent_path, "innovus")
                try:
                    for entry in os.scandir(inv_path):
                        if self._cancel_requested():
                            return tasks, releases_found
                        try:
                            if entry.name.startswith('.') or not entry.is_dir():
                                continue
                            if _ignored_by_pattern(entry.name, _IGNORE_BE_RUN_PATTERNS()):
                                continue
                            tasks.append((ent_name, entry.path, ws_path, current_rtl, "WS", "BE", None))
                        except Exception:
                            pass
                except Exception:
                    pass

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
            ws_bases = []
            ws_seen = set()
            for _base in (_BASE_WS_FE_LIST() + _BASE_WS_BE_LIST()):
                _key = os.path.normpath(_base)
                if _key in ws_seen:
                    continue
                ws_seen.add(_key)
                ws_bases.append(_base)
            for ws_base in ws_bases:
                if self._cancel_requested():
                    self.finished.emit(ws_data, out_data, {}, scan_stats)
                    return
                if not ws_base:
                    continue
                if not os.path.exists(ws_base):
                    continue
                try:
                    ws_names = os.listdir(ws_base)
                except Exception as e:
                    debug_log("ScannerWorker: list workspace base failed {}".format(ws_base), e)
                    continue
                for ws_name in ws_names:
                    disc_futures.append(
                        disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan)
                    )

            for future in concurrent.futures.as_completed(disc_futures):
                if self._cancel_requested():
                    self.finished.emit(ws_data, out_data, {}, scan_stats)
                    return
                try:
                    new_tasks, new_releases = future.result()
                    tasks.extend(new_tasks)
                    for rtl, paths in new_releases.items():
                        for p in paths:
                            self._map_release(ws_data, rtl, p)
                except Exception as e:
                    debug_log("ScannerWorker: workspace discovery future failed", e)

        # --- Outfeed discovery ---
        self.status_update.emit("Discovering OUTFEED directories...")
        for outfeed_base in _BASE_OUTFEED_LIST():
            if self._cancel_requested():
                self.finished.emit(ws_data, out_data, {}, scan_stats)
                return
            if not outfeed_base or not os.path.exists(outfeed_base):
                continue
            try:
                outfeed_entries = list(os.scandir(outfeed_base))
            except Exception as e:
                debug_log("ScannerWorker: list OUTFEED root failed {}".format(outfeed_base), e)
                outfeed_entries = []
            for ent in outfeed_entries:
                if self._cancel_requested():
                    self.finished.emit(ws_data, out_data, {}, scan_stats)
                    return
                try:
                    if not ent.is_dir():
                        continue
                except Exception:
                    continue
                ent_name = ent.name
                ent_path = ent.path

                # Expected outfeed structure:
                # outfeed/{BLOCK}/{EVT_LABEL}/fc/{run}/{run}-FE
                # outfeed/{BLOCK}/{EVT_LABEL}/fc/{run}-BE
                # outfeed/{BLOCK}/{EVT_LABEL}/innovus/{run}[-BE]
                try:
                    evt_entries = [e for e in os.scandir(ent_path)
                                   if e.name.startswith("EVT") and e.is_dir()]
                except Exception as e:
                    debug_log("ScannerWorker: list OUTFEED EVT dirs failed {}".format(ent_path), e)
                    evt_entries = []

                for evt_entry in evt_entries:
                    if self._cancel_requested():
                        self.finished.emit(ws_data, out_data, {}, scan_stats)
                        return
                    evt_dir = evt_entry.path
                    phys_evt = evt_entry.name
                    blk_name = ent_name

                    fc_dir = os.path.join(evt_dir, "fc")
                    try:
                        fc_entries = [e for e in os.scandir(fc_dir) if e.is_dir()]
                    except Exception as e:
                        debug_log("ScannerWorker: list OUTFEED fc failed {}".format(fc_dir), e)
                        fc_entries = []
                    for fc_entry in fc_entries:
                        if self._cancel_requested():
                            self.finished.emit(ws_data, out_data, {}, scan_stats)
                            return
                        name = fc_entry.name
                        if name.endswith("-BE"):
                            if "fc" in tools_to_scan and not _ignored_by_pattern(name, _IGNORE_BE_RUN_PATTERNS()):
                                tasks.append((blk_name, fc_entry.path, fc_entry.path, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                            continue
                        try:
                            child_entries = [e for e in os.scandir(fc_entry.path) if e.is_dir()]
                        except Exception as e:
                            debug_log("ScannerWorker: list OUTFEED fc group failed {}".format(fc_entry.path), e)
                            child_entries = []
                        for child in child_entries:
                            if child.name.endswith("-FE"):
                                if not _ignored_by_pattern(child.name, _IGNORE_FE_RUN_PATTERNS()):
                                    tasks.append((blk_name, child.path, child.path, "UNKNOWN", "OUTFEED", "FE", phys_evt))

                    if "innovus" in tools_to_scan:
                        inv_dir = os.path.join(evt_dir, "innovus")
                        try:
                            inv_entries = [e for e in os.scandir(inv_dir) if e.is_dir()]
                        except Exception as e:
                            debug_log("ScannerWorker: list OUTFEED innovus failed {}".format(inv_dir), e)
                            inv_entries = []
                        for inv_entry in inv_entries:
                            if self._cancel_requested():
                                self.finished.emit(ws_data, out_data, {}, scan_stats)
                                return
                            if _ignored_by_pattern(inv_entry.name, _IGNORE_BE_RUN_PATTERNS()):
                                continue
                            tasks.append((blk_name, inv_entry.path, inv_entry.path, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        # --- Dedupe discovered tasks by normalized real path before processing ---
        seen_task_paths = set()
        deduped_tasks = []
        for task in tasks:
            if self._cancel_requested():
                self.finished.emit(ws_data, out_data, {}, scan_stats)
                return
            try:
                key = (task[4], task[5],
                       os.path.normcase(os.path.realpath(os.path.normpath(task[1]))))
            except Exception:
                key = (task[4], task[5], task[1])
            if key in seen_task_paths:
                continue
            seen_task_paths.add(key)
            deduped_tasks.append(task)
        tasks = deduped_tasks

        # --- Prefetch path cache ---
        paths_to_prefetch = []
        for t in tasks:
            if self._cancel_requested():
                self.finished.emit(ws_data, out_data, {}, scan_stats)
                return
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
                if self._cancel_requested():
                    self.finished.emit(ws_data, out_data, {}, scan_stats)
                    return
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
                except Exception as e:
                    debug_log("ScannerWorker: process run future failed", e)

                completed_tasks += 1
                # Throttle UI updates -- emit every 20 tasks to avoid flooding event loop
                if completed_tasks % 20 == 0 or completed_tasks == total_tasks:
                    self.progress_update.emit(completed_tasks, total_tasks)
                    self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")

            if ir_future:
                try:
                    ir_data = {} if self._cancel_requested() else ir_future.result()
                except Exception as e:
                    debug_log("ScannerWorker: IR scan future failed", e)
                    ir_data = {}
            else:
                ir_data = {}

        if self._cancel_requested():
            self.finished.emit(ws_data, out_data, {}, scan_stats)
            return
        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

    # -----------------------------------------------------------------------
    # Per-task run processor (called in thread pool)
    # -----------------------------------------------------------------------
    def _thread_process_run(self, task_tuple):
        if self._cancel_requested():
            return None
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
        if self._cancel_requested():
            return None
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

        if self._cancel_requested():
            return None
        evt_base     = get_dynamic_evt_path(rtl, b_name)
        if self._cancel_requested():
            return None
        owner        = get_owner(rd) if _SCAN_OWNER_ON_START() else "Unknown"

        fm_n     = os.path.join(evt_base, "fm",   clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm",   clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre",   "reports", "report_lp.rpt")
        if self._cancel_requested():
            return None
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

        if self._cancel_requested():
            return None
        stages = []
        if run_type == "BE":
            is_innovus_run = "/innovus/" in rd.replace("\\", "/")
            if source == "WS" and is_innovus_run:
                search_dir = os.path.join(rd, "reports")
            elif source == "WS":
                search_dir = os.path.join(rd, "outputs")
            else:
                search_dir = rd
            try:
                stage_entries = [e for e in os.scandir(search_dir) if e.is_dir()]
            except Exception:
                stage_entries = []
            for entry in stage_entries:
                if self._cancel_requested():
                    return None
                s_dir = entry.path
                step_name = os.path.basename(s_dir)
                if step_name in ["logs", "pass", "fail", "outputs"]:
                    continue
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]:
                    continue
                if _ignored_by_pattern(step_name, _IGNORE_PNR_STAGE_PATTERNS()):
                    continue

                is_fc = "/innovus/" not in rd.replace("\\", "/")
                # Build path candidates WITHOUT any os.path.exists() / glob during scan.
                # NFS stat calls here would block the scan worker for hundreds of ms per stage.
                # All path resolution is deferred to StageDetailWorker (background thread).
                if source == "WS":
                    stage_path = (os.path.join(rd, "outputs", step_name)
                                  if is_fc else os.path.join(rd, "reports", step_name))
                    if is_fc:
                        log = os.path.join(rd, "logs", f"{step_name}.log")
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
                    "source":        source,
                    "_origin_source": source,
                    "_origin_be_path": rd,
                    "_origin_stage_path": stage_path,
                    "rpt":           rpt_cands[0],   # primary (used as fallback)
                    "_rpt_cands":    rpt_cands,       # resolved lazily in StageDetailWorker
                    "log":           log,
                    "pass_path":     (os.path.join(rd, "pass", f"{step_name}.pass")
                                      if source == "WS" else ""),
                    "stage_status":  ("COMPLETED" if source == "OUTFEED" else "CHECKING"),
                    "active_stage":  step_name,
                    "_stage_index":  len(stages),
                    "_is_innovus":   bool(is_innovus_run),
                    # All deferred - filled by StageDetailWorker on expand
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
    finished = pyqtSignal(str, str, list)   # (be_path, run_name, enriched_stages)

    def __init__(self, be_run):
        super().__init__()
        self.be_run = dict(be_run or {})
        self.be_path = self.be_run.get("path", "")
        self.run_name = self.be_run.get("r_name", "")
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self):
        enriched = []
        for s in self.be_run.get("stages", []):
            if self._cancelled or self.isInterruptionRequested():
                self.finished.emit(self.be_path, self.run_name, [])
                return
            if not s.get("_lazy"):
                enriched.append(s)
                continue
            s2 = dict(s)

            rpt_file = s["rpt"]
            for cand in s.get("_rpt_cands", [rpt_file]):
                if self._cancelled or self.isInterruptionRequested():
                    self.finished.emit(self.be_path, self.run_name, [])
                    return
                if cached_exists(cand):
                    rpt_file = cand
                    break
            s2["info"] = parse_pnr_runtime_rpt_uncached(rpt_file)
            stage_status, active_stage, pass_path = resolve_pnr_stage_status(
                s2, self.be_run)
            s2["stage_status"] = stage_status
            s2["active_stage"] = active_stage
            s2["pass_path"] = pass_path

            fm_base = s.get("_fm_base", "")
            step = s.get("_fm_step", s["name"])
            fm_u_path = fm_n_path = ""
            if fm_base:
                for be_dir in s.get("_fm_dirs", []):
                    if self._cancelled or self.isInterruptionRequested():
                        self.finished.emit(self.be_path, self.run_name, [])
                        return
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

            vslp_path = ""
            if fm_base:
                for be_dir in s.get("_fm_dirs", []):
                    cand = os.path.join(
                        fm_base, "fm", be_dir, step, "pgnet", "reports", "report_lp.rpt")
                    if cached_exists(cand):
                        vslp_path = cand
                        break
                if not vslp_path:
                    dirs = s.get("_fm_dirs", [])
                    if dirs:
                        vslp_path = os.path.join(
                            fm_base, "fm", dirs[0], step, "pgnet", "reports", "report_lp.rpt")

            s2["fm_u_path"] = fm_u_path
            s2["fm_n_path"] = fm_n_path
            s2["vslp_rpt_path"] = vslp_path
            s2["st_n"] = get_fm_info(fm_n_path)
            s2["st_u"] = get_fm_info(fm_u_path)
            s2["vslp_status"] = get_vslp_info(vslp_path)
            s2["_lazy"] = False
            enriched.append(s2)
        enriched.sort(key=lambda st: (
            0 if _parse_stage_start_sort_value(st.get("info", {})) else 1,
            _parse_stage_start_sort_value(st.get("info", {})) or (9999, 12, 31, 23, 59),
            st.get("_stage_index", 9999),
            st.get("name", "")))
        self.finished.emit(self.be_path, self.run_name, enriched)


class BranchStatusWorker(QThread):
    finished = pyqtSignal(str, str, list)   # (be_path, run_name, status_stages)

    def __init__(self, be_run):
        super().__init__()
        self.be_run = dict(be_run or {})
        self.be_path = self.be_run.get("path", "")
        self.run_name = self.be_run.get("r_name", "")
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self):
        stages = []
        try:
            for s in self.be_run.get("stages", []):
                if self._cancelled or self.isInterruptionRequested():
                    self.finished.emit(self.be_path, self.run_name, [])
                    return
                s2 = dict(s)
                rpt_candidates = _stage_runtime_candidates(self.be_path, s2)
                s2["_runtime_candidates"] = rpt_candidates
                s2["_runtime_rpt_path"] = ""
                rpt_file = ""
                for cand in rpt_candidates:
                    if self._cancelled or self.isInterruptionRequested():
                        self.finished.emit(self.be_path, self.run_name, [])
                        return
                    if cand and os.path.exists(cand):
                        rpt_file = cand
                        s2["_runtime_rpt_path"] = cand
                        break
                if rpt_file:
                    s2["info"] = parse_pnr_runtime_rpt_uncached(rpt_file)
                else:
                    s2["info"] = s2.get("info", {"start": "-", "end": "-", "runtime": "-", "last_stage": "-"})
                pass_candidates = _stage_pass_candidates(self.be_path, s2)
                s2["_pass_candidates"] = pass_candidates
                if pass_candidates and not s2.get("pass_path"):
                    s2["pass_path"] = pass_candidates[0]
                stage_status, active_stage, pass_path = resolve_pnr_stage_status(
                    s2, self.be_run)
                s2["stage_status"] = stage_status
                s2["active_stage"] = active_stage
                s2["pass_path"] = pass_path
                s2["_branch_status_loaded"] = True
                stages.append(s2)
            stages.sort(key=lambda st: (
                0 if _parse_stage_start_sort_value(st.get("info", {})) else 1,
                _parse_stage_start_sort_value(st.get("info", {})) or (9999, 12, 31, 23, 59),
                st.get("_stage_index", 9999),
                st.get("name", "")))
        except Exception:
            # Emit whatever was resolved so far; the UI can keep existing rows
            # for unresolved stages instead of staying stuck at CHECKING forever.
            pass
        self.finished.emit(self.be_path, self.run_name, stages)


class StageIndexWorker(QThread):
    finished = pyqtSignal(dict)   # path -> enriched stage list
    progress = pyqtSignal(int, int)

    def __init__(self, runs):
        super().__init__()
        self.runs = list(runs or [])
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def _index_run(self, be_run):
        be_run = dict(be_run or {})
        be_path = be_run.get("path", "")
        stages = []
        for s in be_run.get("stages", []) or []:
            if self._cancelled or self.isInterruptionRequested():
                return []
            s2 = dict(s)
            rpt_candidates = _stage_runtime_candidates(be_path, s2)
            s2["_runtime_candidates"] = rpt_candidates
            s2["_runtime_rpt_path"] = ""
            info = s2.get("info", {}) or {}
            for cand in rpt_candidates:
                if self._cancelled or self.isInterruptionRequested():
                    return []
                if cand and os.path.exists(cand):
                    s2["_runtime_rpt_path"] = cand
                    info = parse_pnr_runtime_rpt_uncached(cand)
                    break
            if not info:
                info = {"start": "-", "end": "-", "runtime": "-", "last_stage": "-"}
            s2["info"] = info
            pass_candidates = _stage_pass_candidates(be_path, s2)
            s2["_pass_candidates"] = pass_candidates
            if pass_candidates and not s2.get("pass_path"):
                s2["pass_path"] = pass_candidates[0]
            status, active_stage, pass_path = resolve_pnr_stage_status(s2, be_run)
            s2["stage_status"] = status
            s2["active_stage"] = active_stage
            s2["pass_path"] = pass_path
            s2["_stage_index_loaded"] = True
            stages.append(s2)
        stages.sort(key=lambda st: (
            0 if _parse_stage_start_sort_value(st.get("info", {})) else 1,
            _parse_stage_start_sort_value(st.get("info", {})) or (9999, 12, 31, 23, 59),
            st.get("_stage_index", 9999),
            st.get("name", "")))
        for idx, st in enumerate(stages):
            st["_stage_order"] = idx
        return stages

    def run(self):
        out = {}
        total = len(self.runs)
        for idx, run in enumerate(self.runs):
            if self._cancelled or self.isInterruptionRequested():
                self.finished.emit(out)
                return
            try:
                if run.get("run_type") == "BE" and run.get("stages"):
                    path = os.path.normpath(run.get("path", "") or "")
                    if path:
                        out[path] = self._index_run(run)
            except Exception:
                pass
            if idx % 10 == 0 or idx + 1 == total:
                self.progress.emit(idx + 1, total)
        self.finished.emit(out)


class QuickStatusRefreshWorker(QThread):
    finished = pyqtSignal(list)
    progress = pyqtSignal(int, int)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = list(tasks or [])
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def _status_for_run(self, run_path, source):
        info = parse_runtime_rpt(os.path.join(run_path, "reports", "runtime.V2.rpt"))
        is_comp = True if source == "OUTFEED" else os.path.exists(
            os.path.join(run_path, "pass", "compile_opt.pass"))
        status = "COMPLETED" if is_comp else "RUNNING"
        if not is_comp:
            log_file = os.path.join(run_path, "logs", "compile_opt.log")
            if not os.path.exists(log_file):
                status = "NOT STARTED"
            else:
                try:
                    with open(log_file, "r", encoding="utf-8", errors="ignore") as lf:
                        for line in lf:
                            if "Stack trace for crashing thread" in line:
                                status = "FATAL ERROR"
                                break
                            if "Information: Process terminated by interrupt. (INT-4)" in line:
                                status = "INTERRUPTED"
                                break
                except Exception:
                    pass
        return is_comp, status, info

    def run(self):
        clear_path_cache()
        out = []
        total = len(self.tasks)
        for idx, task in enumerate(self.tasks, 1):
            if self._cancelled or self.isInterruptionRequested():
                break
            row = dict(task)
            try:
                is_comp, status, info = self._status_for_run(
                    task.get("path", ""), task.get("source", "WS"))
                row["is_comp"] = is_comp
                row["fe_status"] = status
                row["info"] = info
            except Exception as e:
                row["_error"] = str(e)
            out.append(row)
            if idx == total or idx % 10 == 0:
                self.progress.emit(idx, total)
        if self._cancelled or self.isInterruptionRequested():
            self.finished.emit([])
            return
        self.finished.emit(out)


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
                 stage_name=None, stage_path=None):
        super().__init__()
        self.run_path   = run_path
        self.b_name     = b_name
        self.run_type   = run_type
        self.source     = source
        self.stage_name = stage_name  # None for FE, stage name for BE
        self.stage_path = stage_path
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self):
        if not _METRICS_AVAILABLE:
            self.finished.emit({})
            return
        if self._cancelled or self.isInterruptionRequested():
            self.finished.emit({})
            return
        try:
            m = _extract_metrics_cached(
                self.run_path, self.b_name, self.run_type, self.source,
                self.stage_name, self.stage_path,
                cancel_check=lambda: self._cancelled or self.isInterruptionRequested())
            _save_metric_cache()
            if self._cancelled or self.isInterruptionRequested():
                self.finished.emit({"_cancelled": True})
                return
            self.finished.emit(m)
        except Exception as e:
            debug_log("MetricWorker: metric extraction failed", e)
            self.finished.emit({"_error": str(e)})


class MetricBatchWorker(QThread):
    finished = pyqtSignal(list)  # list of task dicts with metrics
    progress = pyqtSignal(int, int)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = list(tasks or [])
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass

    def run(self):
        out = []
        if not _METRICS_AVAILABLE:
            self.finished.emit(out)
            return
        for idx, task in enumerate(self.tasks, 1):
            if self._cancelled or self.isInterruptionRequested():
                break
            row = dict(task)
            try:
                if task.get("run_type") == "FE":
                    row["metrics"] = _extract_metrics_cached(
                        task.get("path", ""),
                        task.get("block", ""),
                        "FE",
                        source=task.get("source", "WS"),
                        stage_name=None,
                        stage_path=None,
                        cancel_check=lambda: self._cancelled or self.isInterruptionRequested())
                else:
                    row["metrics"] = _extract_metrics_cached(
                        task.get("path", ""),
                        task.get("block", ""),
                        "BE",
                        source=task.get("source", "WS"),
                        stage_name=task.get("stage_name", ""),
                        stage_path=task.get("stage_path", ""),
                        cancel_check=lambda: self._cancelled or self.isInterruptionRequested())
                    if (task.get("runtime")
                            and str(task.get("runtime")).strip() not in ("", "-", "N/A")
                            and row["metrics"].get("runtime", "-") in ("", "-", "N/A")):
                        row["metrics"]["runtime"] = task.get("runtime")
            except Exception as e:
                debug_log("MetricBatchWorker: metric extraction failed", e)
                row["metrics"] = {"_error": str(e)}
            out.append(row)
            if idx == len(self.tasks) or idx % 10 == 0:
                self.progress.emit(len(out), len(self.tasks))
        if self._cancelled or self.isInterruptionRequested():
            self.finished.emit([])
            return
        _save_metric_cache()
        self.finished.emit(out)


class QoRWorker(QThread):
    finished = pyqtSignal(str)  # emits path to HTML output file, or ""

    def __init__(self, script_path, run_dirs, python_bin="python3.6"):
        super().__init__()
        self.script_path = script_path
        self.run_dirs    = run_dirs
        self.python_bin  = python_bin
        self._cancelled  = False
        self._proc       = None

    def cancel(self):
        self._cancelled = True
        try:
            self.requestInterruption()
        except Exception:
            pass
        proc = getattr(self, "_proc", None)
        try:
            if proc and proc.poll() is None:
                proc.kill()
        except Exception:
            pass

    def run(self):
        try:
            # New summary.py writes qor_report.html in the launch PWD.
            script_dir = os.path.dirname(os.path.abspath(self.script_path))
            launch_cwd = os.getcwd()
            start_time = time.time()
            timeout_sec = _QOR_TIMEOUT_SEC()
            cmd = [self.python_bin, self.script_path] + self.run_dirs
            self._proc = subprocess.Popen(
                cmd,
                cwd=launch_cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            try:
                stdout, stderr = self._proc.communicate(timeout=timeout_sec)
            except subprocess.TimeoutExpired as e:
                debug_log("QoRWorker: timeout after {} seconds".format(timeout_sec), e)
                self.cancel()
                try:
                    stdout, stderr = self._proc.communicate(timeout=5)
                except Exception as e2:
                    debug_log("QoRWorker: collect after timeout failed", e2)
                    stdout, stderr = b"", b""
            if self._cancelled or self.isInterruptionRequested():
                self.finished.emit("")
                return
            output = stdout.decode("utf-8", errors="ignore")
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
                html_path = os.path.join(launch_cwd, html_path)
            try:
                if html_path and os.path.exists(html_path) and os.path.getmtime(html_path) < start_time:
                    html_path = ""
            except Exception as e:
                debug_log("QoRWorker: html mtime validation failed", e)
                html_path = ""
            if not html_path or not os.path.exists(html_path):
                candidates = [
                    os.path.join(launch_cwd, "qor_report.html"),
                    os.path.join(launch_cwd, "report_qor.html"),
                    os.path.join(script_dir, "qor_report.html"),
                    os.path.join(script_dir, "report_qor.html"),
                    os.path.join(script_dir, "qor_metrices", "qor_report.html"),
                ]
                try:
                    candidates.extend(glob.glob(os.path.join(
                        script_dir, "qor_metrices", "**", "qor_report.html"),
                        recursive=True))
                    candidates.extend(glob.glob(os.path.join(
                        script_dir, "qor_metrices", "**", "*.html"),
                        recursive=True))
                except Exception as e:
                    debug_log("QoRWorker: fallback html glob failed", e)
                hits = []
                for p in candidates:
                    try:
                        if p and os.path.exists(p) and os.path.getmtime(p) >= start_time:
                            hits.append(p)
                    except Exception as e:
                        debug_log("QoRWorker: fallback candidate check failed {}".format(p), e)
                if hits:
                    html_path = max(hits, key=os.path.getmtime)
            self.finished.emit(html_path if (html_path and os.path.exists(html_path)) else "")
        except Exception as e:
            debug_log("QoRWorker: run failed", e)
            self.finished.emit("")
