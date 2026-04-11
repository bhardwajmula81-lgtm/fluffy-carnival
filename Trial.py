import os
import glob
import re
import subprocess
import sys
import fnmatch
import pickle
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QComboBox, QLineEdit,
                             QTreeWidget, QTreeWidgetItem, QPushButton,
                             QMessageBox, QListWidget, QListWidgetItem,
                             QProgressBar, QMenu, QSplitter, QFontComboBox, QSpinBox,
                             QWidgetAction, QCheckBox, QDialog, QFormLayout,
                             QDialogButtonBox, QSizePolicy, QAbstractItemView)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QColor, QFont

# =====================================================================
# --- CONFIGURATION BLOCK (Project Dependent - Edit Here) ---
# =====================================================================
PROJECT_PREFIX    = "S5K2P5SP"
BASE_WS_FE_DIR    = "/user/s5k2p5sx.fe1/s5k2p5sp/WS"
BASE_WS_BE_DIR    = "/user/s5k2p5sp.be1/s5k2p5sp/WS"
BASE_OUTFEED_DIR  = "/user/s5k2p5sx.fe1/s5k2p5sp/outfeed"
SUMMARY_SCRIPT    = "/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py"
FIREFOX_PATH      = "/usr/bin/firefox"

# Parallel scan workers - local disk can handle 8-16 safely
MAX_SCAN_WORKERS  = 12

# Disk cache location and TTL (seconds). Set TTL=0 to disable cache.
CACHE_PATH        = os.path.expanduser("~/.pd_dashboard_cache.pkl")
CACHE_TTL_SECONDS = 300   # 5 minutes - rescan if data is older than this
# =====================================================================


# -----------------------------------------------------------------------
# LOGIC HELPERS
# -----------------------------------------------------------------------

def normalize_rtl(rtl_str):
    """Ensures RTL strings uniformly contain the project prefix."""
    if rtl_str and not rtl_str.startswith(PROJECT_PREFIX) and rtl_str.startswith("EVT"):
        return f"{PROJECT_PREFIX}_{rtl_str}"
    return rtl_str

def format_log_date(date_str):
    match = re.search(r'([A-Z][a-z]{2})\s+([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})', str(date_str))
    if match:
        day_of_week, mon, day, t, year = match.groups()
        return f"{day_of_week} {mon} {day}, {year} - {t}"
    return str(date_str).strip()

def get_dynamic_evt_path(rtl_tag, block_name):
    match = re.search(r'(EVT\d+_ML\d+_DEV\d+)', str(rtl_tag))
    if not match:
        return ""
    return os.path.join(BASE_OUTFEED_DIR, block_name, match.group(1))

def get_fm_info(report_path):
    if not report_path or not os.path.exists(report_path):
        return "N/A"
    try:
        with open(report_path, 'r') as f:
            content = f.read()
            if "No failing compare points" in content:
                return "PASS"
            m = re.search(r'(\d+)\s+Failing compare points', content)
            return f"{m.group(1)} FAILS" if m else "ERR"
    except:
        return "ERR"

def get_vslp_info(report_path):
    if not report_path or not os.path.exists(report_path):
        return "N/A"
    try:
        with open(report_path, 'r') as f:
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
    except:
        pass
    return "Not Found"

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path):
        return d
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if "TOTAL_START" in line and "Load :" in line:
                    d["start"] = format_log_date(line.split("Load :")[-1].strip())
                m = re.search(r'TimeStamp\s*:\s*(\S+)', line)
                if m and m.group(1) not in ["TOTAL", "TOTAL_START"]:
                    d["last_stage"] = m.group(1)
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(r'Total\s*:\s*(\d+)h:(\d+)m:(\d+)s', line)
                    if rt:
                        d["runtime"] = f"{int(rt.group(1)):02}h:{int(rt.group(2)):02}m:{int(rt.group(3)):02}s"
                    if "Load :" in line:
                        d["end"] = format_log_date(line.split("Load :")[-1].strip())
    except:
        pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path):
        return d
    try:
        first_ts, last_ts, total_cputime = None, None, None
        months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    ts_match  = re.search(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})', line)
                    cpu_match = re.search(r'(\d+)d:(\d+)h:(\d+)m:(\d+)s', line)
                    if ts_match and cpu_match:
                        if not first_ts:
                            first_ts = ts_match
                        last_ts = ts_match
                        days, hours, mins, secs = map(int, cpu_match.groups())
                        total_hours = days * 24 + hours
                        total_cputime = f"{total_hours:02}h:{mins:02}m:{secs:02}s"
                        if len(parts) > 1 and not parts[1].isdigit():
                            d["last_stage"] = parts[1]
        if first_ts:
            y, m, day, H, M = first_ts.groups()
            d["start"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if last_ts:
            y, m, day, H, M = last_ts.groups()
            d["end"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if total_cputime:
            d["runtime"] = total_cputime
    except:
        pass
    return d

def extract_rtl(run_dir):
    f = glob.glob(os.path.join(run_dir, "reports", "dump_variables.user_defined.*.rpt"))
    if not f:
        return "Unknown"
    try:
        with open(f[0], 'r') as file:
            for line in file:
                m = re.search(r'^\s*all\s*=\s*"(.*?)"', line)
                if m:
                    return normalize_rtl(m.group(1))
    except:
        pass
    return "Unknown"


# -----------------------------------------------------------------------
# DISK CACHE
# -----------------------------------------------------------------------

def _ws_cache_key():
    """A cheap fingerprint: mtime of the two WS root dirs and outfeed dir."""
    key_parts = []
    for d in [BASE_WS_FE_DIR, BASE_WS_BE_DIR, BASE_OUTFEED_DIR]:
        try:
            key_parts.append(str(os.path.getmtime(d)))
        except:
            key_parts.append("0")
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

def load_cache():
    """Return (ws_data, out_data) from disk cache if fresh, else (None, None)."""
    if CACHE_TTL_SECONDS <= 0 or not os.path.exists(CACHE_PATH):
        return None, None
    try:
        with open(CACHE_PATH, 'rb') as f:
            cached = pickle.load(f)
        age = time.time() - cached.get("ts", 0)
        if age > CACHE_TTL_SECONDS:
            return None, None
        if cached.get("key") != _ws_cache_key():
            return None, None
        return cached["ws"], cached["out"]
    except:
        return None, None

def save_cache(ws_data, out_data):
    try:
        with open(CACHE_PATH, 'wb') as f:
            pickle.dump({"ts": time.time(), "key": _ws_cache_key(),
                         "ws": ws_data, "out": out_data}, f)
    except:
        pass


# -----------------------------------------------------------------------
# PARALLEL SCANNER WORKER
# -----------------------------------------------------------------------

class ScannerWorker(QThread):
    finished  = pyqtSignal(dict, dict)
    progress  = pyqtSignal(str)          # status string for progress bar label

    # ---- shared helpers (called from worker threads) ------------------

    @staticmethod
    def _map_release(data_obj, rtl_str, path):
        # This is called under a lock in the threaded scan
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

    @staticmethod
    def _process_run(b_name, rd, parent_path, rtl, source, run_type):
        r_name      = os.path.basename(rd)
        clean_run   = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)
        evt_base    = get_dynamic_evt_path(rtl, b_name)

        fm_n     = os.path.join(evt_base, "fm", clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")

        info = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))

        stages = []
        if run_type == "BE":
            if source == "WS":
                for s_dir in glob.glob(os.path.join(rd, "outputs", "*")):
                    if os.path.isdir(s_dir):
                        step_name   = os.path.basename(s_dir)
                        rpt         = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt")
                        log         = os.path.join(rd, "logs", f"{step_name}.log")
                        stage_path  = os.path.join(rd, "outputs", step_name)
                        fm_u_glob   = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                        fm_n_glob   = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                        st_fm_u     = fm_u_glob[0] if fm_u_glob else ""
                        st_fm_n     = fm_n_glob[0] if fm_n_glob else ""
                        st_vslp     = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                        qor_path    = (rd if rd.endswith("/") else rd + "/")
                        stages.append({
                            "name": step_name, "rpt": rpt, "log": log,
                            "info": parse_pnr_runtime_rpt(rpt),
                            "st_n": get_fm_info(st_fm_n), "st_u": get_fm_info(st_fm_u),
                            "vslp_status": get_vslp_info(st_vslp),
                            "fm_u_path": st_fm_u, "fm_n_path": st_fm_n,
                            "vslp_rpt_path": st_vslp, "qor_path": qor_path,
                            "stage_path": stage_path
                        })
            elif source == "OUTFEED":
                for s_dir in glob.glob(os.path.join(rd, "*")):
                    if os.path.isdir(s_dir):
                        step_name = os.path.basename(s_dir)
                        if step_name in ["reports", "logs", "pass", "fail", "outputs"]:
                            continue
                        rpt        = os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                        log        = os.path.join(s_dir, "logs", f"{step_name}.log")
                        stage_path = os.path.join(rd, step_name)
                        fm_u_glob  = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                        fm_n_glob  = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                        st_fm_u    = fm_u_glob[0] if fm_u_glob else ""
                        st_fm_n    = fm_n_glob[0] if fm_n_glob else ""
                        st_vslp    = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                        qor_path   = os.path.join(s_dir, "reports", step_name)
                        if not qor_path.endswith("/"):
                            qor_path += "/"
                        stages.append({
                            "name": step_name, "rpt": rpt, "log": log,
                            "info": parse_pnr_runtime_rpt(rpt),
                            "st_n": get_fm_info(st_fm_n), "st_u": get_fm_info(st_fm_u),
                            "vslp_status": get_vslp_info(st_vslp),
                            "fm_u_path": st_fm_u, "fm_n_path": st_fm_n,
                            "vslp_rpt_path": st_vslp, "qor_path": qor_path,
                            "stage_path": stage_path
                        })

        return {
            "block": b_name, "path": rd, "parent": parent_path,
            "rtl": rtl, "r_name": r_name, "run_type": run_type,
            "stages": stages, "source": source,
            "is_comp": (True if source == "OUTFEED"
                        else os.path.exists(os.path.join(rd, "pass/compile_opt.pass"))),
            "st_n": get_fm_info(fm_n), "st_u": get_fm_info(fm_u),
            "vslp_status": get_vslp_info(vslp_rpt),
            "info": info, "fm_n_path": fm_n, "fm_u_path": fm_u,
            "vslp_rpt_path": vslp_rpt
        }

    # ---- per-workspace scanner (runs in thread pool) ------------------

    @staticmethod
    def _scan_one_ws(ws_path, ws_base):
        """Scan a single workspace directory. Returns list of run dicts + release map."""
        local_releases = {}
        local_runs     = []
        local_blocks   = set()
        current_rtl    = "Unknown"

        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            try:
                with open(sf, 'r') as f:
                    lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                    for l in set(lbls):
                        nrtl = normalize_rtl(l)
                        current_rtl = nrtl
                        if nrtl not in local_releases:
                            local_releases[nrtl] = []
                        if ws_path not in local_releases[nrtl]:
                            local_releases[nrtl].append(ws_path)
                        base = re.sub(r'_syn\d+$', '', nrtl)
                        if base != nrtl:
                            if base not in local_releases:
                                local_releases[base] = []
                            if ws_path not in local_releases[base]:
                                local_releases[base].append(ws_path)
            except:
                pass

        for ent_path in glob.glob(os.path.join(ws_path, "IMPLEMENTATION", "*", "SOC", "*")):
            ent_name = os.path.basename(ent_path)

            if ws_base == BASE_WS_FE_DIR:
                for rd in glob.glob(os.path.join(ent_path, "fc", "*-FE")):
                    local_blocks.add(ent_name)
                    local_runs.append(ScannerWorker._process_run(
                        ent_name, rd, ws_path, current_rtl, "WS", "FE"))

            # BE: scan in both FE and BE base dirs
            for pat in ["*-BE", "EVT*_ML*_DEV*_*_*-BE"]:
                for rd in glob.glob(os.path.join(ent_path, "fc", pat)):
                    be_rtl = extract_rtl(rd)
                    if be_rtl == "Unknown":
                        be_rtl = current_rtl
                    local_blocks.add(ent_name)
                    local_runs.append(ScannerWorker._process_run(
                        ent_name, rd, ws_path, be_rtl, "WS", "BE"))

        return local_releases, local_runs, local_blocks

    # ---- outfeed scanner (single-threaded, usually small) -------------

    @staticmethod
    def _scan_outfeed():
        local_releases = {}
        local_runs     = []
        local_blocks   = set()

        if not os.path.exists(BASE_OUTFEED_DIR):
            return local_releases, local_runs, local_blocks

        for ent_name in os.listdir(BASE_OUTFEED_DIR):
            ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
            if not os.path.isdir(ent_path):
                continue
            for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                phys_evt = os.path.basename(evt_dir)

                for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                    rtl = ScannerWorker._resolve_outfeed_rtl(rd, phys_evt)
                    if rtl not in local_releases:
                        local_releases[rtl] = []
                    if rd not in local_releases[rtl]:
                        local_releases[rtl].append(rd)
                    local_blocks.add(ent_name)
                    local_runs.append(ScannerWorker._process_run(
                        ent_name, rd, rd, rtl, "OUTFEED", "FE"))

                be_runs = (glob.glob(os.path.join(evt_dir, "fc", "*-BE")) +
                           glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")))
                for rd in be_runs:
                    rtl = ScannerWorker._resolve_outfeed_rtl(rd, phys_evt)
                    if rtl not in local_releases:
                        local_releases[rtl] = []
                    if rd not in local_releases[rtl]:
                        local_releases[rtl].append(rd)
                    local_blocks.add(ent_name)
                    local_runs.append(ScannerWorker._process_run(
                        ent_name, rd, rd, rtl, "OUTFEED", "BE"))

        return local_releases, local_runs, local_blocks

    @staticmethod
    def _resolve_outfeed_rtl(rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl):
            rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif rtl == "Unknown":
            rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    # ---- main run() ---------------------------------------------------

    def run(self):
        # Try cache first
        ws_data, out_data = load_cache()
        if ws_data is not None and out_data is not None:
            self.progress.emit("Loaded from cache")
            self.finished.emit(ws_data, out_data)
            return

        ws_data  = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data = {"releases": {}, "blocks": set(), "all_runs": []}

        # Collect all workspace paths from both FE and BE dirs
        ws_tasks = []
        for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
            if not os.path.exists(ws_base):
                continue
            for ws_name in os.listdir(ws_base):
                ws_path = os.path.join(ws_base, ws_name)
                if os.path.isdir(ws_path):
                    ws_tasks.append((ws_path, ws_base))

        total = len(ws_tasks)
        done  = 0

        # Parallel scan of workspaces
        with ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as pool:
            futures = {pool.submit(self._scan_one_ws, wp, wb): (wp, wb)
                       for wp, wb in ws_tasks}
            for fut in as_completed(futures):
                done += 1
                self.progress.emit(f"Scanning workspaces... ({done}/{total})")
                try:
                    rel, runs, blks = fut.result()
                except Exception:
                    continue
                # Merge results (GIL protects dict mutation here)
                for rtl, paths in rel.items():
                    if rtl not in ws_data["releases"]:
                        ws_data["releases"][rtl] = []
                    for p in paths:
                        if p not in ws_data["releases"][rtl]:
                            ws_data["releases"][rtl].append(p)
                ws_data["all_runs"].extend(runs)
                ws_data["blocks"].update(blks)

        # Outfeed scan (serial - usually small)
        self.progress.emit("Scanning outfeed...")
        rel, runs, blks = self._scan_outfeed()
        for rtl, paths in rel.items():
            if rtl not in out_data["releases"]:
                out_data["releases"][rtl] = []
            for p in paths:
                if p not in out_data["releases"][rtl]:
                    out_data["releases"][rtl].append(p)
        out_data["all_runs"].extend(runs)
        out_data["blocks"].update(blks)

        self.progress.emit("Saving cache...")
        save_cache(ws_data, out_data)

        self.finished.emit(ws_data, out_data)


# -----------------------------------------------------------------------
# FOLDER SIZE WORKER
# -----------------------------------------------------------------------

class SingleSizeWorker(QThread):
    result = pyqtSignal(QTreeWidgetItem, str)

    def __init__(self, item, path):
        super().__init__()
        self.item = item
        self.path = path

    def run(self):
        total = 0
        try:
            for dirpath, _, files in os.walk(self.path):
                for fn in files:
                    try:
                        total += os.path.getsize(os.path.join(dirpath, fn))
                    except:
                        pass
        except:
            pass
        if total >= 1 << 30:
            sz = f"{total / (1<<30):.2f} GB"
        elif total >= 1 << 20:
            sz = f"{total / (1<<20):.1f} MB"
        else:
            sz = f"{total / (1<<10):.0f} KB"
        self.result.emit(self.item, sz)


# -----------------------------------------------------------------------
# SETTINGS DIALOG
# -----------------------------------------------------------------------

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Settings")
        self.resize(360, 200)
        layout = QFormLayout(self)

        self.font_combo = QFontComboBox()
        self.font_combo.setCurrentFont(QApplication.font())
        layout.addRow("Font Family:", self.font_combo)

        self.size_spin = QSpinBox()
        self.size_spin.setRange(8, 24)
        sz = QApplication.font().pointSize()
        self.size_spin.setValue(sz if sz > 0 else 10)
        layout.addRow("Font Size:", self.size_spin)

        self.space_spin = QSpinBox()
        self.space_spin.setRange(0, 20)
        self.space_spin.setValue(parent.row_spacing if parent else 2)
        layout.addRow("Row Spacing (px):", self.space_spin)

        self.theme_cb = QCheckBox("Enable Dark Mode")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)

        self.cache_btn = QPushButton("Clear Disk Cache")
        self.cache_btn.clicked.connect(self._clear_cache)
        layout.addRow("Cache:", self.cache_btn)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def _clear_cache(self):
        try:
            if os.path.exists(CACHE_PATH):
                os.remove(CACHE_PATH)
            QMessageBox.information(self, "Cache Cleared",
                                    "Cache cleared. Next refresh will do a full scan.")
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))


# -----------------------------------------------------------------------
# MAIN DASHBOARD
# -----------------------------------------------------------------------

# Color palette used for status/source coloring
_C = {
    "green_d":  "#81c784", "green_l":  "#2e7d32",
    "blue_d":   "#4fc3f7", "blue_l":   "#0277bd",
    "red_d":    "#ef5350", "red_l":    "#d32f2f",
    "orange_d": "#ffb74d", "orange_l": "#f57c00",
    "purple_d": "#b39ddb", "purple_l": "#5e35b1",
    "dim_d":    "#aaaaaa", "dim_l":    "#616161",
}


class PDDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{PROJECT_PREFIX} - PD Runtime Dashboard")
        self.resize(1920, 960)

        self.ws_data      = {}
        self.out_data     = {}
        self.is_dark_mode = False
        self.row_spacing  = 2
        self.ignored_paths = set()
        self._block_items  = {}      # block_name -> QTreeWidgetItem (cache)
        self.size_workers  = []

        # Debounce timer for search + block filter
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.init_ui()
        self.start_fs_scan()

    # ------------------------------------------------------------------
    # UI SETUP
    # ------------------------------------------------------------------

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # ---- Top bar ----
        top = QHBoxLayout()

        top.addWidget(QLabel("Source:"))
        self.src_combo = QComboBox()
        self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.currentIndexChanged.connect(self.on_source_changed)
        top.addWidget(self.src_combo)

        top.addWidget(QLabel("RTL:"))
        self.rel_combo = QComboBox()
        self.rel_combo.setMinimumWidth(350)
        self.rel_combo.currentIndexChanged.connect(self.refresh_view)
        top.addWidget(self.rel_combo)

        top.addWidget(QLabel("Status:"))
        self.status_filter = QComboBox()
        self.status_filter.addItems(["All", "Running", "Completed"])
        self.status_filter.currentIndexChanged.connect(self.refresh_view)
        top.addWidget(self.status_filter)

        top.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItems(["FE + BE", "FE only", "BE only"])
        self.type_filter.currentIndexChanged.connect(self.refresh_view)
        top.addWidget(self.type_filter)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Filter runs (wildcards ok: *run1*) ...")
        self.search.textChanged.connect(lambda: self.search_timer.start(300))
        top.addWidget(self.search)

        self.exp_btn = QPushButton("Expand All")
        self.exp_btn.clicked.connect(lambda: self.tree.expandAll())
        top.addWidget(self.exp_btn)

        self.col_btn = QPushButton("Collapse All")
        self.col_btn.clicked.connect(lambda: self.tree.collapseAll())
        top.addWidget(self.col_btn)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.setToolTip("Force a full re-scan (ignores cache)")
        refresh_btn.clicked.connect(self.force_rescan)
        top.addWidget(refresh_btn)

        qor_btn = QPushButton("Compare QoR")
        qor_btn.clicked.connect(self.run_qor_comparison)
        top.addWidget(qor_btn)

        settings_btn = QPushButton("Settings")
        settings_btn.clicked.connect(self.open_settings)
        top.addWidget(settings_btn)

        layout.addLayout(top)

        # ---- Splitter: left=blocks, right=tree ----
        self.splitter = QSplitter(Qt.Horizontal)

        left_panel  = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(4, 4, 4, 4)
        left_layout.setSpacing(4)

        lbl = QLabel("<b>Blocks</b>")
        left_layout.addWidget(lbl)

        btn_row = QHBoxLayout()
        sel_all = QPushButton("All")
        sel_all.setMaximumWidth(50)
        sel_all.clicked.connect(lambda: self._set_all_blocks(Qt.Checked))
        sel_none = QPushButton("None")
        sel_none.setMaximumWidth(55)
        sel_none.clicked.connect(lambda: self._set_all_blocks(Qt.Unchecked))
        btn_row.addWidget(sel_all)
        btn_row.addWidget(sel_none)
        btn_row.addStretch()
        left_layout.addLayout(btn_row)

        self.blk_list = QListWidget()
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(80))
        left_layout.addWidget(self.blk_list)
        self.splitter.addWidget(left_panel)

        # ---- Tree ----
        self.tree = QTreeWidget()
        self.tree.setColumnCount(20)
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)       # big perf win for large trees
        self.tree.setAnimated(False)               # no expand animation lag
        self.tree.setExpandsOnDoubleClick(False)   # we handle double-click ourselves

        headers = [
            "Run Name (Select)", "RTL Release Version", "Source",
            "Status", "Stage",
            "FM - NONUPF", "FM - UPF", "VSLP Status",
            "Runtime", "Start", "End", "Size",
            "Path",          # col 12 (visible)
            "Log",           # col 13 (hidden)
            "FM_U_RPT",      # col 14 (hidden)
            "FM_N_RPT",      # col 15 (hidden)
            "VSLP_RPT",      # col 16 (hidden)
            "STA_RPT",       # col 17 (hidden)
            "IR_LOG",        # col 18 (hidden)
            "_RUN_PATH",     # col 19 (hidden, used internally)
        ]
        self.tree.setHeaderLabels(headers)

        self.tree.setColumnWidth(0, 380)
        self.tree.setColumnWidth(1, 280)
        self.tree.setColumnWidth(2, 80)
        self.tree.setColumnWidth(3, 100)
        self.tree.setColumnWidth(4, 120)
        self.tree.setColumnWidth(5, 160)
        self.tree.setColumnWidth(6, 160)
        self.tree.setColumnWidth(7, 200)
        self.tree.setColumnWidth(8, 100)
        self.tree.setColumnWidth(11, 80)
        self.tree.setColumnWidth(12, 300)

        for i in range(13, 20):
            self.tree.setColumnHidden(i, True)

        self.tree.setSortingEnabled(True)
        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)

        # Allow text selection (for copy)
        self.tree.setTextElideMode(Qt.ElideRight)
        self.tree.setSelectionMode(QAbstractItemView.ExtendedSelection)

        self.splitter.addWidget(self.tree)
        self.splitter.setSizes([200, 1700])
        layout.addWidget(self.splitter)

        # ---- Progress bar + status label ----
        status_row = QHBoxLayout()
        self.status_label = QLabel("Scanning...")
        self.status_label.setVisible(False)
        status_row.addWidget(self.status_label)
        self.prog = QProgressBar()
        self.prog.setVisible(False)
        self.prog.setMaximumHeight(14)
        status_row.addWidget(self.prog)
        layout.addLayout(status_row)

        self.apply_theme_and_spacing()

    # ------------------------------------------------------------------
    # SETTINGS / THEME
    # ------------------------------------------------------------------

    def open_settings(self):
        dlg = SettingsDialog(self)
        if dlg.exec_():
            font = dlg.font_combo.currentFont()
            font.setPointSize(dlg.size_spin.value())
            QApplication.setFont(font)
            self.is_dark_mode = dlg.theme_cb.isChecked()
            self.row_spacing  = dlg.space_spin.value()
            self.apply_theme_and_spacing()

    def apply_theme_and_spacing(self):
        pad = self.row_spacing
        if self.is_dark_mode:
            ss = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2b2b; color: #e0e0e0; }}
                QTreeWidget, QListWidget {{ background-color: #333333; color: #e0e0e0;
                    alternate-background-color: #3a3a3a; }}
                QHeaderView::section {{ background-color: #444444; color: white;
                    border: 1px solid #2b2b2b; padding: 4px; }}
                QLineEdit, QComboBox, QSpinBox {{ background-color: #444444; color: white;
                    border: 1px solid #555; padding: 2px; }}
                QPushButton {{ background-color: #555555; color: white;
                    border: 1px solid #333; padding: 4px; border-radius: 2px; }}
                QPushButton:hover {{ background-color: #666666; }}
                QSplitter::handle {{ background-color: #555555; }}
                QMenu {{ border: 1px solid gray; background-color: #333; color: white; }}
                QMenu::item:selected {{ background-color: #555; }}
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
            """
        else:
            ss = f"""
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
            """
        self.setStyleSheet(ss)
        self.refresh_view()

    # ------------------------------------------------------------------
    # SCAN CONTROL
    # ------------------------------------------------------------------

    def start_fs_scan(self):
        self.prog.setVisible(True)
        self.prog.setRange(0, 0)
        self.status_label.setText("Scanning workspaces...")
        self.status_label.setVisible(True)
        self.worker = ScannerWorker()
        self.worker.progress.connect(self._on_scan_progress)
        self.worker.finished.connect(self.on_scan_finished)
        self.worker.start()

    def force_rescan(self):
        """Delete cache and re-scan."""
        try:
            if os.path.exists(CACHE_PATH):
                os.remove(CACHE_PATH)
        except:
            pass
        self.start_fs_scan()

    def _on_scan_progress(self, msg):
        self.status_label.setText(msg)

    def on_scan_finished(self, ws, out):
        self.ws_data  = ws
        self.out_data = out
        self.prog.setVisible(False)
        self.status_label.setVisible(False)
        self.on_source_changed()

    # ------------------------------------------------------------------
    # FILTER POPULATION
    # ------------------------------------------------------------------

    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        releases, blocks = set(), set()

        if src_mode in ["WS", "ALL"] and self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            releases.update(self.out_data.get("releases", {}).keys())
            blocks.update(self.out_data.get("blocks", set()))

        self.rel_combo.blockSignals(True)
        self.rel_combo.clear()
        self.rel_combo.addItems(["[ SHOW ALL ]"] + sorted(releases))
        self.rel_combo.blockSignals(False)

        self.blk_list.blockSignals(True)
        self.blk_list.clear()
        for b in sorted(blocks):
            it = QListWidgetItem(b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(Qt.Checked)
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)

        self.refresh_view()

    def _set_all_blocks(self, state):
        self.blk_list.blockSignals(True)
        for i in range(self.blk_list.count()):
            self.blk_list.item(i).setCheckState(state)
        self.blk_list.blockSignals(False)
        self.search_timer.start(80)

    # ------------------------------------------------------------------
    # TREE BUILDING HELPERS
    # ------------------------------------------------------------------

    def _c(self, dark_key, light_key):
        return QColor(_C[dark_key] if self.is_dark_mode else _C[light_key])

    def _apply_fm_color(self, item, col, val):
        if "FAILS" in val:
            item.setForeground(col, self._c("red_d", "red_l"))
        elif "PASS" in val:
            item.setForeground(col, self._c("green_d", "green_l"))

    def _apply_vslp_color(self, item, col, val):
        if "Error" in val and "Error: 0" not in val:
            item.setForeground(col, self._c("red_d", "red_l"))
        elif "Error: 0" in val:
            item.setForeground(col, self._c("green_d", "green_l"))

    def _create_run_item(self, parent_item, run):
        child = QTreeWidgetItem()
        child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        child.setCheckState(0, Qt.Unchecked)

        child.setText(0, " " + run["r_name"])
        child.setText(1, run["rtl"])
        child.setText(2, run["source"])

        if run["run_type"] == "FE":
            status_str = "COMPLETED" if run["is_comp"] else "RUNNING"
            child.setText(3, status_str)
            child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(5, f"NONUPF - {run['st_n']}")
            child.setText(6, f"UPF - {run['st_u']}")
            child.setText(7, run["vslp_status"])
            child.setText(8, run["info"]["runtime"])
            child.setText(9, run["info"]["start"])
            child.setText(10, run["info"]["end"])
            child.setText(11, "")     # size (calculated on demand)
            child.setText(12, run["path"])
            child.setText(13, os.path.join(run["path"], "logs/compile_opt.log"))
            child.setText(14, run["fm_u_path"])
            child.setText(15, run["fm_n_path"])
            child.setText(16, run["vslp_rpt_path"])
            child.setText(19, run["path"])

            self._apply_fm_color(child, 5, run["st_n"])
            self._apply_fm_color(child, 6, run["st_u"])
            self._apply_vslp_color(child, 7, run["vslp_status"])
            child.setForeground(3, self._c(
                "green_d" if run["is_comp"] else "blue_d",
                "green_l" if run["is_comp"] else "blue_l"))
        else:
            # BE parent row: only RTL + source shown
            child.setText(12, run["path"])
            child.setText(19, run["path"])

        src_col = "purple_d" if run["source"] == "OUTFEED" else "orange_d"
        src_light = "purple_l" if run["source"] == "OUTFEED" else "orange_l"
        child.setForeground(2, self._c(src_col, src_light))

        parent_item.addChild(child)
        return child

    def _create_stage_item(self, parent_item, stage, be_run):
        st = QTreeWidgetItem()
        st.setFlags(st.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        st.setCheckState(0, Qt.Unchecked)
        st.setData(0, Qt.UserRole, "STAGE")
        st.setData(1, Qt.UserRole, stage["name"])
        st.setData(2, Qt.UserRole, stage["qor_path"])

        st.setText(0, "    > " + stage["name"])
        st.setText(2, be_run["source"])
        st.setText(4, stage["info"]["last_stage"])
        st.setText(5, f"NONUPF - {stage['st_n']}")
        st.setText(6, f"UPF - {stage['st_u']}")
        st.setText(7, stage["vslp_status"])
        st.setText(8, stage["info"]["runtime"])
        st.setText(9, stage["info"]["start"])
        st.setText(10, stage["info"]["end"])
        st.setText(12, stage.get("stage_path", be_run["path"]))
        st.setText(13, stage["log"])
        st.setText(14, stage["fm_u_path"])
        st.setText(15, stage["fm_n_path"])
        st.setText(16, stage["vslp_rpt_path"])
        st.setText(19, be_run["path"])

        st.setForeground(0, QColor(_C["dim_d"] if self.is_dark_mode else _C["dim_l"]))
        self._apply_fm_color(st, 5, stage["st_n"])
        self._apply_fm_color(st, 6, stage["st_u"])
        self._apply_vslp_color(st, 7, stage["vslp_status"])

        parent_item.addChild(st)
        return st

    def _get_or_create_block(self, name):
        if name in self._block_items:
            return self._block_items[name]
        p = QTreeWidgetItem(self.tree)
        p.setText(0, name)
        p.setExpanded(True)
        p.setBackground(0, QColor("#424242") if self.is_dark_mode else QColor("#e8eaf6"))
        f = p.font(0)
        f.setBold(True)
        p.setFont(0, f)
        self._block_items[name] = p
        return p

    # ------------------------------------------------------------------
    # REFRESH VIEW  (the hot path - keep lean)
    # ------------------------------------------------------------------

    def refresh_view(self):
        self.tree.setSortingEnabled(False)
        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        self._block_items.clear()

        src_mode   = self.src_combo.currentText()
        sel_rtl    = self.rel_combo.currentText()
        sf_text    = self.status_filter.currentText()   # All / Running / Completed
        tf_text    = self.type_filter.currentText()     # FE + BE / FE only / BE only

        raw_query = self.search.text().strip().lower()
        if not raw_query:
            search_pattern = "*"
        elif '*' not in raw_query:
            search_pattern = f"*{raw_query}*"
        else:
            search_pattern = raw_query

        checked_blks = set()
        for i in range(self.blk_list.count()):
            it = self.blk_list.item(i)
            if it.checkState() == Qt.Checked:
                checked_blks.add(it.text())

        # Build target_paths set for the selected RTL
        runs_pool   = []
        target_paths = set()

        if src_mode in ["WS", "ALL"] and self.ws_data:
            runs_pool.extend(self.ws_data.get("all_runs", []))
            if sel_rtl == "[ SHOW ALL ]":
                for paths in self.ws_data.get("releases", {}).values():
                    target_paths.update(paths)
            else:
                target_paths.update(self.ws_data.get("releases", {}).get(sel_rtl, []))

        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            runs_pool.extend(self.out_data.get("all_runs", []))
            if sel_rtl == "[ SHOW ALL ]":
                for paths in self.out_data.get("releases", {}).values():
                    target_paths.update(paths)
            else:
                target_paths.update(self.out_data.get("releases", {}).get(sel_rtl, []))

        # Separate FE / BE, filter ignored
        fe_runs = [r for r in runs_pool
                   if r["run_type"] == "FE" and r["path"] not in self.ignored_paths]
        be_runs = [r for r in runs_pool
                   if r["run_type"] == "BE" and r["path"] not in self.ignored_paths]

        # Index BE runs by (block, source, r_name) for O(1) lookup
        be_by_block = {}
        for be in be_runs:
            key = (be["block"], be["source"])
            be_by_block.setdefault(key, []).append(be)

        matched_be_ids = set()
        show_fe = tf_text in ("FE + BE", "FE only")
        show_be = tf_text in ("FE + BE", "BE only")

        for fe in fe_runs:
            if fe["parent"] not in target_paths:
                continue
            if fe["block"] not in checked_blks:
                continue

            # Status filter
            if sf_text == "Running"   and fe["is_comp"]:
                continue
            if sf_text == "Completed" and not fe["is_comp"]:
                continue

            # Search filter (run name OR block name)
            if not (fnmatch.fnmatch(fe["r_name"].lower(), search_pattern) or
                    fnmatch.fnmatch(fe["block"].lower(), search_pattern) or
                    fnmatch.fnmatch(fe["rtl"].lower(), search_pattern)):
                continue

            if not show_fe:
                # Still need to attach BE children if show_be
                pass
            else:
                blk_item = self._get_or_create_block(fe["block"])
                fe_item  = self._create_run_item(blk_item, fe)

            fe_base = fe["r_name"].replace("-FE", "")

            if show_be:
                for be in be_by_block.get((fe["block"], fe["source"]), []):
                    if (f"_{fe_base}_" in be["r_name"] or
                            be["r_name"].startswith(f"{fe_base}_")):
                        matched_be_ids.add(id(be))
                        if show_fe:
                            be_item = self._create_run_item(fe_item, be)
                            for stage in be["stages"]:
                                self._create_stage_item(be_item, stage, be)

        # Unmatched BE runs (no parent FE)
        if show_be:
            for be in be_runs:
                if id(be) in matched_be_ids:
                    continue
                if be["parent"] not in target_paths:
                    continue
                if be["block"] not in checked_blks:
                    continue
                if not (fnmatch.fnmatch(be["r_name"].lower(), search_pattern) or
                        fnmatch.fnmatch(be["block"].lower(), search_pattern)):
                    continue
                blk_item = self._get_or_create_block(be["block"])
                be_item  = self._create_run_item(blk_item, be)
                for stage in be["stages"]:
                    self._create_stage_item(be_item, stage, be)

        # Expand all block-level items and re-enable sorting/updates
        for i in range(self.tree.topLevelItemCount()):
            self.tree.topLevelItem(i).setExpanded(True)

        self.tree.setSortingEnabled(True)
        self.tree.setUpdatesEnabled(True)

    # ------------------------------------------------------------------
    # CONTEXT MENUS
    # ------------------------------------------------------------------

    def on_header_context_menu(self, pos):
        menu = QMenu(self)
        for i in range(1, 13):
            action = QWidgetAction(menu)
            cb = QCheckBox(self.tree.headerItem().text(i))
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.setStyleSheet("margin: 2px 8px; background: transparent; color: inherit;")
            cb.toggled.connect(lambda checked, col=i: self.tree.setColumnHidden(col, not checked))
            action.setDefaultWidget(cb)
            menu.addAction(action)
        menu.exec_(self.tree.header().mapToGlobal(pos))

    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item or not item.parent():
            return

        col        = self.tree.columnAt(pos.x())
        m          = QMenu()
        is_stage   = item.data(0, Qt.UserRole) == "STAGE"

        cell_text  = item.text(col).strip()
        copy_cell_act = None
        if cell_text:
            copy_cell_act = m.addAction(f"Copy Cell Text")
            m.addSeparator()

        run_path   = item.text(19)
        log_path   = item.text(13)
        fm_u_path  = item.text(14)
        fm_n_path  = item.text(15)
        vslp_path  = item.text(16)
        sta_path   = item.text(17)
        ir_path    = item.text(18)

        ignore_checked_act = m.addAction("Ignore All Checked Runs")
        m.addSeparator()

        ignore_act  = None
        restore_act = None
        if run_path and run_path != "N/A" and not is_stage:
            if run_path in self.ignored_paths:
                restore_act = m.addAction("Restore This Run (Un-ignore)")
            else:
                ignore_act = m.addAction("Ignore This Run")
            m.addSeparator()

        calc_size_act = None
        if run_path and os.path.exists(run_path):
            calc_size_act = m.addAction("Calculate Folder Size")
            m.addSeparator()

        def _file_act(path, label):
            if path and path != "N/A" and os.path.exists(path):
                return m.addAction(label)
            return None

        fm_n_act = _file_act(fm_n_path, "Open NONUPF Formality Report")
        fm_u_act = _file_act(fm_u_path, "Open UPF Formality Report")
        v_act    = _file_act(vslp_path, "Open VSLP Report")
        sta_act  = _file_act(sta_path,  "Open PT STA Summary")
        ir_act   = _file_act(ir_path,   "Open Static IR Log")
        log_act  = _file_act(log_path,  "Open Log File")

        m.addSeparator()
        copy_path_act = m.addAction("Copy Path")

        qor_act = None
        if is_stage:
            m.addSeparator()
            qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res:
            return

        if res == ignore_checked_act:
            def _ignore_checked(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    if (c.checkState(0) == Qt.Checked and
                            c.data(0, Qt.UserRole) != "STAGE"):
                        p = c.text(19)
                        if p and p != "N/A":
                            self.ignored_paths.add(p)
                    _ignore_checked(c)
            _ignore_checked(self.tree.invisibleRootItem())
            self.refresh_view()
        elif ignore_act  and res == ignore_act:
            self.ignored_paths.add(run_path); self.refresh_view()
        elif restore_act and res == restore_act:
            self.ignored_paths.discard(run_path); self.refresh_view()
        elif copy_cell_act and res == copy_cell_act:
            QApplication.clipboard().setText(cell_text)
        elif calc_size_act and res == calc_size_act:
            item.setText(11, "Calc...")
            w = SingleSizeWorker(item, run_path)
            w.result.connect(lambda it, sz: it.setText(11, sz))
            self.size_workers.append(w)
            w.finished.connect(lambda ww=w: self.size_workers.remove(ww)
                               if ww in self.size_workers else None)
            w.start()
        elif fm_n_act and res == fm_n_act:
            subprocess.Popen(['gvim', fm_n_path])
        elif fm_u_act and res == fm_u_act:
            subprocess.Popen(['gvim', fm_u_path])
        elif v_act    and res == v_act:
            subprocess.Popen(['gvim', vslp_path])
        elif sta_act  and res == sta_act:
            subprocess.Popen(['gvim', sta_path])
        elif ir_act   and res == ir_act:
            subprocess.Popen(['gvim', ir_path])
        elif log_act  and res == log_act:
            subprocess.Popen(['gvim', log_path])
        elif res == copy_path_act:
            QApplication.clipboard().setText(run_path)
        elif qor_act and res == qor_act:
            step_name = item.data(1, Qt.UserRole)
            qor_path  = item.data(2, Qt.UserRole)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
            if h:
                subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])

    def on_item_double_clicked(self, item, col):
        if item.parent():
            log = item.text(13)
            if log and os.path.exists(log):
                subprocess.Popen(['gvim', log])

    # ------------------------------------------------------------------
    # QoR COMPARISON
    # ------------------------------------------------------------------

    def run_qor_comparison(self):
        sel = []
        def _collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    qp = c.text(19)
                    if c.text(2) == "OUTFEED" and qp:
                        qp = os.path.dirname(qp)
                    if qp and not qp.endswith("/"):
                        qp += "/"
                    sel.append(qp)
                _collect(c)
        _collect(self.tree.invisibleRootItem())
        if len(sel) < 2:
            QMessageBox.information(self, "QoR Compare",
                                    "Please check at least 2 runs first.")
            return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
        if h:
            subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])


# -----------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = PDDashboard()
    w.show()
    sys.exit(app.exec_())
