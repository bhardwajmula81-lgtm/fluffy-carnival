import os
import glob
import re
import subprocess
import sys
import fnmatch
import concurrent.futures
import pwd

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QComboBox, QLineEdit, 
                             QTreeWidget, QTreeWidgetItem, QPushButton, 
                             QMessageBox, QListWidget, QListWidgetItem,
                             QProgressBar, QMenu, QSplitter, QFontComboBox, QSpinBox,
                             QWidgetAction, QCheckBox, QDialog, QFormLayout, QDialogButtonBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QColor, QFont, QClipboard

# =====================================================================
# --- CONFIGURATION BLOCK (Project Dependent) ---
# =====================================================================
PROJECT_PREFIX = "S5K2P5SP"

BASE_WS_FE_DIR = "/user/s5k2p5sx.fe1/s5k2p5sp/WS"
BASE_WS_BE_DIR = "/user/s5k2p5sp.be1/s5k2p5sp/WS"

BASE_OUTFEED_DIR = "/user/s5k2p5sx.fe1/s5k2p5sp/outfeed"

# IR Configuration
BASE_IR_DIR = "/user/s5k2p5sx.be1/LAYOUT/IR/"

PNR_TOOL_NAMES = "fc innovus"

SUMMARY_SCRIPT = "/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py"
FIREFOX_PATH = "/usr/bin/firefox"
# =====================================================================

# --- CUSTOM SORTING UI CLASS ---
class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""
        
        if col == 0:
            if t1 == "[ Ignored Runs ]": return False
            if t2 == "[ Ignored Runs ]": return True
            
            if t1 == "Other PNR runs": return False
            if t2 == "Other PNR runs": return True
            
            m_order = {"INITIAL RELEASE": 1, "PRE-SVP": 2, "SVP": 3, "FFN": 4}
            if t1 in m_order and t2 in m_order:
                if self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder:
                    return m_order[t1] < m_order[t2]
                else:
                    return m_order[t1] > m_order[t2]
                
        return t1 < t2

# --- LOGIC HELPERS ---

def get_owner(path):
    if not path or not os.path.exists(path): return "Unknown"
    try:
        return pwd.getpwuid(os.stat(path).st_uid).pw_name
    except Exception:
        return "Unknown"

def normalize_rtl(rtl_str):
    if rtl_str and rtl_str.startswith("EVT"):
        return f"{PROJECT_PREFIX}_{rtl_str}"
    return rtl_str

def get_milestone(rtl_str):
    if "_ML1_" in rtl_str: return "INITIAL RELEASE"
    if "_ML2_" in rtl_str: return "PRE-SVP"
    if "_ML3_" in rtl_str: return "SVP"
    if "_ML4_" in rtl_str: return "FFN"
    return None 

def format_log_date(date_str):
    match = re.search(r'([A-Z][a-z]{2})\s+([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})', str(date_str))
    if match:
        day_of_week, mon, day, time, year = match.groups()
        return f"{day_of_week} {mon} {day}, {year} - {time}"
    return str(date_str).strip()

def get_dynamic_evt_path(rtl_tag, block_name):
    match = re.search(r'(EVT\d+_ML\d+_DEV\d+)', str(rtl_tag))
    if not match: return ""
    folder_part = match.group(1) 
    return os.path.join(BASE_OUTFEED_DIR, block_name, folder_part)

def get_fm_info(report_path):
    if not report_path or not os.path.exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            for line in f:
                if "No failing compare points" in line: return "PASS"
                m = re.search(r'(\d+)\s+Failing compare points', line)
                if m: return f"{m.group(1)} FAILS"
    except: pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not os.path.exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            in_summary = False
            for line in f:
                if "Management Summary" in line:
                    in_summary = True
                    continue
                if in_summary and line.strip().startswith("Total"):
                    parts = line.strip().split()
                    if len(parts) >= 3: return f"Error: {parts[1]}, Warning: {parts[2]}"
                    break
    except: pass
    return "Not Found"

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path): return d
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
                    if rt: d["runtime"] = f"{int(rt.group(1)):02}h:{int(rt.group(2)):02}m:{int(rt.group(3)):02}s"
                    if "Load :" in line: d["end"] = format_log_date(line.split("Load :")[-1].strip())
    except Exception: pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path): return d
    try:
        first_ts, last_ts, final_time_str = None, None, None
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    ts_match = re.search(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})', line)
                    time_matches = re.findall(r'(\d+)d:(\d+)h:(\d+)m:(\d+)s', line)
                    
                    if ts_match and time_matches:
                        if not first_ts: first_ts = ts_match
                        last_ts = ts_match
                        
                        target_match = time_matches[1] if len(time_matches) > 1 else time_matches[0]
                        days, hours, mins, secs = map(int, target_match) 
                        total_hours = days * 24 + hours
                        final_time_str = f"{total_hours:02}h:{mins:02}m:{secs:02}s"
                        
                        if len(parts) > 1 and not parts[1].isdigit():
                            d["last_stage"] = parts[1]

        if first_ts:
            y, m, day, H, M = first_ts.groups()
            d["start"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if last_ts:
            y, m, day, H, M = last_ts.groups()
            d["end"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if final_time_str:
            d["runtime"] = final_time_str
    except Exception: pass
    return d

def extract_rtl(run_dir):
    f = glob.glob(os.path.join(run_dir, "reports", "dump_variables.user_defined.*.rpt"))
    if not f: return "Unknown"
    try:
        with open(f[0], 'r') as file:
            for line in file:
                m = re.search(r'^\s*all\s*=\s*"(.*?)"', line)
                if m: return normalize_rtl(m.group(1))
    except: pass
    return "Unknown"

# --- UI DIALOGS ---

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Settings")
        self.resize(350, 180)
        layout = QFormLayout(self)
        
        self.font_combo = QFontComboBox()
        self.font_combo.setCurrentFont(QApplication.font())
        layout.addRow("Font Family:", self.font_combo)
        
        self.size_spin = QSpinBox()
        self.size_spin.setRange(8, 24)
        current_size = QApplication.font().pointSize()
        self.size_spin.setValue(current_size if current_size > 0 else 10)
        layout.addRow("Font Size:", self.size_spin)
        
        self.space_spin = QSpinBox()
        self.space_spin.setRange(0, 20)
        self.space_spin.setValue(parent.row_spacing if parent else 2)
        layout.addRow("Row Spacing (Padding):", self.space_spin)
        
        self.theme_cb = QCheckBox("Enable Dark Mode")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)
        
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

# --- BACKGROUND WORKER THREADS ---

class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)
    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self._is_cancelled = False
    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(self.get_size, path): item_id for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled: break
                item_id = futures[future]
                try:
                    self.size_calculated.emit(item_id, future.result())
                except:
                    self.size_calculated.emit(item_id, "N/A")
    def get_size(self, path):
        if not path or not os.path.exists(path): return "N/A"
        try:
            return subprocess.check_output(['du', '-sh', path], stderr=subprocess.DEVNULL).decode('utf-8').split()[0]
        except: return "N/A"
    def cancel(self): self._is_cancelled = True

class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)
    def __init__(self, item, path):
        super().__init__()
        self.item = item; self.path = path; self._is_cancelled = False
    def run(self):
        if self._is_cancelled or not self.path or not os.path.exists(self.path):
            self.result.emit(self.item, "N/A"); return
        try:
            sz = subprocess.check_output(['du', '-sh', self.path], stderr=subprocess.DEVNULL).decode('utf-8').split()[0]
            if not self._is_cancelled: self.result.emit(self.item, sz)
        except:
            if not self._is_cancelled: self.result.emit(self.item, "N/A")
    def cancel(self): self._is_cancelled = True

class ScannerWorker(QThread):
    finished = pyqtSignal(dict, dict, dict)
    progress_update = pyqtSignal(int, int) 

    def scan_ir_dir(self):
        ir_data = {}
        if os.path.exists(BASE_IR_DIR):
            target_lef = f"{PROJECT_PREFIX}.lef.list"
            for root_dir, dirs, files in os.walk(BASE_IR_DIR):
                if "redhawk.log" in files:
                    log_path = os.path.join(root_dir, "redhawk.log")
                    run_be_name, step_name, inst_line, inst_value = None, None, "", ""
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    # Matches both outfeed (.../run-BE/stage/outputs/...) and WS (.../run-BE/outputs/stage/...)
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m: 
                                        run_be_name = m.group(1)
                                        step_name = m.group(2)
                                elif line.startswith("INST ") and "mV" in line:
                                    inst_line = line.strip()
                                    m2 = re.search(r'INST\s+(\S+mV)', inst_line)
                                    if m2: inst_value = m2.group(1)
                    except: pass
                    if run_be_name and step_name:
                        # Key uniquely by BE Run + PNR Stage
                        key = f"{run_be_name}/{step_name}"
                        ir_data[key] = {"log": log_path, "line": inst_line, "value": inst_value}
        return ir_data

    def run(self):
        ws_data = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data = {"releases": {}, "blocks": set(), "all_runs": []}
        tasks = []
        tools_to_scan = PNR_TOOL_NAMES.split()

        ws_bases = [BASE_WS_FE_DIR, BASE_WS_BE_DIR]
        for ws_base in ws_bases:
            if not os.path.exists(ws_base): continue
            for ws_name in os.listdir(ws_base):
                ws_path = os.path.join(ws_base, ws_name)
                if not os.path.isdir(ws_path): continue
                
                current_rtl = "Unknown"
                for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
                    try:
                        with open(sf, 'r') as f:
                            lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                            for l in set(lbls):
                                current_rtl = normalize_rtl(l)
                                self._map_release(ws_data, current_rtl, ws_path)
                    except: pass
                
                for ent_path in glob.glob(os.path.join(ws_path, "IMPLEMENTATION", "*", "SOC", "*")):
                    ent_name = os.path.basename(ent_path)
                    if ws_base == BASE_WS_FE_DIR:
                        for rd in glob.glob(os.path.join(ent_path, "fc", "*-FE")):
                            tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "FE", None))
                    if "fc" in tools_to_scan:
                        for pat in ["*-BE", "EVT*_ML*_DEV*_*_*-BE"]:
                            for rd in glob.glob(os.path.join(ent_path, "fc", pat)):
                                tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(ent_path, "innovus", "EVT*_ML*_DEV*_*")):
                            if os.path.isdir(rd):
                                tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

        if os.path.exists(BASE_OUTFEED_DIR):
            for ent_name in os.listdir(BASE_OUTFEED_DIR):
                ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
                if not os.path.isdir(ent_path): continue
                for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                    phys_evt = os.path.basename(evt_dir) 
                    for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                        tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                    if "fc" in tools_to_scan:
                        be_runs = glob.glob(os.path.join(evt_dir, "fc", "*-BE")) + glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE"))
                        for rd in be_runs:
                            tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                            if os.path.isdir(rd):
                                tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        total_tasks = len(tasks)
        completed_tasks = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            ir_future = executor.submit(self.scan_ir_dir)
            
            future_to_task = {executor.submit(self._thread_process_run, t): t for t in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        if result["source"] == "WS":
                            ws_data["blocks"].add(result["block"])
                            ws_data["all_runs"].append(result)
                            if result["run_type"] == "BE":
                                self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            self._map_release(out_data, result["rtl"], result["path"])
                except Exception: pass 
                completed_tasks += 1
                self.progress_update.emit(completed_tasks, total_tasks)
                
            ir_data = ir_future.result()

        self.finished.emit(ws_data, out_data, ir_data)

    def _thread_process_run(self, task_tuple):
        b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
        if source == "OUTFEED": rtl = self._resolve_outfeed_rtl(rd, phys_evt)
        else:
            if run_type == "BE":
                extracted = extract_rtl(rd)
                rtl = extracted if extracted != "Unknown" else base_rtl
            else: rtl = base_rtl
        return self._process_run(b_name, rd, parent_path, rtl, source, run_type)

    def _resolve_outfeed_rtl(self, rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl): rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif rtl == "Unknown": rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    def _process_run(self, b_name, rd, parent_path, rtl, source, run_type):
        r_name = os.path.basename(rd)
        clean_run = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name) 
        evt_base = get_dynamic_evt_path(rtl, b_name)
        owner = get_owner(rd)
        
        fm_n = os.path.join(evt_base, "fm", clean_run, "r2n", "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")
        info = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))
        
        stages = []
        if run_type == "BE":
            search_glob = os.path.join(rd, "outputs", "*") if source == "WS" else os.path.join(rd, "*")
            for s_dir in glob.glob(search_glob):
                if os.path.isdir(s_dir):
                    step_name = os.path.basename(s_dir)
                    if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]: continue
                    
                    rpt = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt") if source == "WS" else os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                    log = os.path.join(rd, "logs", f"{step_name}.log") if source == "WS" else os.path.join(s_dir, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, "outputs", step_name) if source == "WS" else os.path.join(rd, step_name)
                    
                    fm_u_glob = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                    fm_n_glob = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func", "reports", "*.failpoint.rpt"))
                    st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                    st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                    st_vslp_rpt = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                    
                    sta_rpt = os.path.join(evt_base, "pt", r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                    qor_path = rd if rd.endswith("/") else rd + "/"
                    
                    stages.append({
                        "name": step_name, "rpt": rpt, "log": log, "info": parse_pnr_runtime_rpt(rpt),
                        "st_n": get_fm_info(st_fm_n_path), "st_u": get_fm_info(st_fm_u_path),
                        "vslp_status": get_vslp_info(st_vslp_rpt),
                        "fm_u_path": st_fm_u_path, "fm_n_path": st_fm_n_path, "vslp_rpt_path": st_vslp_rpt,
                        "sta_rpt_path": sta_rpt, "qor_path": qor_path, "stage_path": stage_path
                    })

        return {
            "block": b_name, "path": rd, "parent": parent_path, "rtl": rtl, "r_name": r_name,
            "run_type": run_type, "stages": stages, "source": source, "owner": owner,
            "is_comp": True if source == "OUTFEED" else os.path.exists(os.path.join(rd, "pass/compile_opt.pass")),
            "st_n": get_fm_info(fm_n), "st_u": get_fm_info(fm_u), "vslp_status": get_vslp_info(vslp_rpt),
            "info": info, "fm_n_path": fm_n, "fm_u_path": fm_u, "vslp_rpt_path": vslp_rpt
        }

    def _map_release(self, data_obj, rtl_str, path):
        if rtl_str not in data_obj["releases"]: data_obj["releases"][rtl_str] = []
        if path not in data_obj["releases"][rtl_str]: data_obj["releases"][rtl_str].append(path)
        base = re.sub(r'_syn\d+$', '', rtl_str)
        if base != rtl_str:
            if base not in data_obj["releases"]: data_obj["releases"][base] = []
            if path not in data_obj["releases"][base]: data_obj["releases"][base].append(path)

# --- MAIN UI ---

class PDDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Unified PD Dashboard - Pro Multi-Threaded Edition")
        self.resize(1900, 950)
        
        self.ws_data = {}
        self.out_data = {}
        self.ir_data = {}
        self.is_dark_mode = False
        self.row_spacing = 2
        
        self.size_workers = []
        self.item_map = {} 
        self.ignored_paths = set() 
        
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)
        
        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)
        
        self.init_ui()
        self.start_fs_scan()

    def init_ui(self):
        central = QWidget(); self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        
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
        
        self.search = QLineEdit()
        self.search.setPlaceholderText("Global Search (Runs, Blocks, Status, Runtime, etc.)...")
        self.search.textChanged.connect(lambda: self.search_timer.start(300))
        top.addWidget(self.search)
        
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.start_fs_scan)
        top.addWidget(self.refresh_btn)
        
        top.addWidget(QLabel("Auto-Update:"))
        self.auto_combo = QComboBox()
        self.auto_combo.addItems(["Off", "1 Min", "5 Min", "10 Min"])
        self.auto_combo.currentIndexChanged.connect(self.on_auto_refresh_changed)
        top.addWidget(self.auto_combo)

        self.tools_btn = QPushButton("Tools")
        self.tools_menu = QMenu(self)
        
        fit_act = self.tools_menu.addAction("Fit Columns")
        fit_act.triggered.connect(self.fit_all_columns)
        
        exp_act = self.tools_menu.addAction("Expand All")
        exp_act.triggered.connect(lambda: self.tree.expandAll())
        
        col_act = self.tools_menu.addAction("Collapse All")
        col_act.triggered.connect(lambda: self.tree.collapseAll())
        
        self.tools_menu.addSeparator()
        
        calc_act = self.tools_menu.addAction("Calculate All Run Sizes")
        calc_act.triggered.connect(self.calculate_all_sizes)
        
        self.tools_btn.setMenu(self.tools_menu)
        top.addWidget(self.tools_btn)
        
        btn = QPushButton("Compare QoR")
        btn.clicked.connect(self.run_qor_comparison)
        top.addWidget(btn)
        
        settings_btn = QPushButton("Settings")
        settings_btn.clicked.connect(self.open_settings)
        top.addWidget(settings_btn)
        
        layout.addLayout(top)
        
        self.splitter = QSplitter(Qt.Horizontal)
        
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0,0,0,0)
        lbl = QLabel("<b>Blocks</b>")
        left_layout.addWidget(lbl)
        
        self.blk_list = QListWidget()
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(50)) 
        left_layout.addWidget(self.blk_list)
        
        self.splitter.addWidget(left_panel)
        
        self.tree = QTreeWidget()
        self.tree.setColumnCount(20) 
        self.tree.setAlternatingRowColors(True)
        
        headers = ["Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "Size",
                   "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Runtime", "Start", "End", 
                   "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG"]
        self.tree.setHeaderLabels(headers)
        
        for i in range(self.tree.columnCount()):
            self.tree.headerItem().setTextAlignment(i, Qt.AlignCenter)
        
        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)
        
        self.tree.setColumnWidth(0, 380); self.tree.setColumnWidth(1, 280); self.tree.setColumnWidth(2, 90)
        self.tree.setColumnWidth(5, 80) 
        self.tree.setColumnWidth(6, 160); self.tree.setColumnWidth(7, 160); self.tree.setColumnWidth(8, 200)
        self.tree.setColumnWidth(9, 100) 
        
        for i in range(13, 20): self.tree.setColumnHidden(i, True)
        
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        
        self.splitter.addWidget(self.tree)
        self.splitter.setSizes([200, 1700]) 
        
        layout.addWidget(self.splitter)
        
        self.prog = QProgressBar()
        self.prog.setVisible(False)
        self.prog.setFormat(" Scanning Network Files... %v / %m runs fetched ")
        layout.addWidget(self.prog)
        
        self.apply_theme_and_spacing()

    def on_auto_refresh_changed(self):
        val = self.auto_combo.currentText()
        if val == "Off":
            self.auto_refresh_timer.stop()
        elif val == "1 Min":
            self.auto_refresh_timer.start(60 * 1000)
        elif val == "5 Min":
            self.auto_refresh_timer.start(5 * 60 * 1000)
        elif val == "10 Min":
            self.auto_refresh_timer.start(10 * 60 * 1000)

    def fit_all_columns(self):
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i):
                self.tree.resizeColumnToContents(i)

    def calculate_all_sizes(self):
        size_tasks = []
        def gather_tasks(node):
            for i in range(node.childCount()):
                child = node.child(i)
                path = child.text(13) 
                if path and path != "N/A" and child.text(5) in ["-", "N/A", "Calc..."]:
                    item_id = str(id(child))
                    self.item_map[item_id] = child
                    size_tasks.append((item_id, path))
                    child.setText(5, "Calc...")
                gather_tasks(child)
        
        gather_tasks(self.tree.invisibleRootItem())
        
        if size_tasks:
            worker = BatchSizeWorker(size_tasks)
            worker.size_calculated.connect(self.update_item_size)
            self.size_workers.append(worker)
            worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
            worker.start()

    def update_item_size(self, item_id, size_str):
        item = self.item_map.get(item_id)
        if item:
            item.setText(5, size_str)
            old_tooltip = item.toolTip(0)
            if old_tooltip:
                new_tooltip = re.sub(r'Size: .*?\n', f'Size: {size_str}\n', old_tooltip)
                item.setToolTip(0, new_tooltip)

    def open_settings(self):
        dlg = SettingsDialog(self)
        if dlg.exec_():
            font = dlg.font_combo.currentFont()
            font.setPointSize(dlg.size_spin.value())
            QApplication.setFont(font)
            self.is_dark_mode = dlg.theme_cb.isChecked()
            self.row_spacing = dlg.space_spin.value()
            self.apply_theme_and_spacing()

    def apply_theme_and_spacing(self):
        pad = self.row_spacing
        if self.is_dark_mode:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2b2b; color: #e0e0e0; }}
                QTreeWidget, QListWidget {{ background-color: #333333; color: #e0e0e0; alternate-background-color: #3a3a3a; }}
                QHeaderView::section {{ background-color: #444444; color: white; border: 1px solid #2b2b2b; padding: 4px;}}
                QLineEdit, QComboBox, QSpinBox {{ background-color: #444444; color: white; border: 1px solid #555; padding: 2px;}}
                QPushButton {{ background-color: #555555; color: white; border: 1px solid #333; padding: 4px; border-radius: 2px;}}
                QPushButton:hover {{ background-color: #666666; }}
                QSplitter::handle {{ background-color: #555555; }}
                QMenu {{ border: 1px solid gray; background-color: #333; color: white; }}
                QMenu::item:selected {{ background-color: #555; }}
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
            """
        else:
            stylesheet = f"""
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
            """
        self.setStyleSheet(stylesheet)
        self.refresh_view()

    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning():
            return
            
        self.prog.setVisible(True); self.prog.setRange(0, 0)
        self.worker = ScannerWorker()
        self.worker.progress_update.connect(self.update_progress)
        self.worker.finished.connect(self.on_scan_finished)
        self.worker.start()
        
    def update_progress(self, current, total):
        self.prog.setRange(0, total)
        self.prog.setValue(current)

    def on_scan_finished(self, ws, out, ir):
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.prog.setVisible(False)
        self.on_source_changed()

    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        releases, blocks = set(), set()
        
        if src_mode in ["WS", "ALL"] and self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            releases.update(self.out_data.get("releases", {}).keys())
            blocks.update(self.out_data.get("blocks", set()))

        current_rtl = self.rel_combo.currentText()
        saved_states = {}
        for i in range(self.blk_list.count()):
            item = self.blk_list.item(i)
            saved_states[item.text()] = item.checkState()

        self.rel_combo.blockSignals(True); self.rel_combo.clear()
        
        valid_releases = [r for r in releases if "Unknown" not in r and get_milestone(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(list(valid_releases))
        self.rel_combo.addItems(new_releases)
        
        if current_rtl in new_releases: self.rel_combo.setCurrentText(current_rtl)
        else: self.rel_combo.setCurrentIndex(0)
            
        self.rel_combo.blockSignals(False)
        self.blk_list.blockSignals(True); self.blk_list.clear()
        
        for b in sorted(list(blocks)):
            it = QListWidgetItem(b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False); self.refresh_view()

    def _get_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.childCount()):
            if parent.child(i).text(0) == text:
                return parent.child(i)
        
        p = CustomTreeItem(parent)
        p.setText(0, text)
        p.setData(0, Qt.UserRole, node_type)
        p.setExpanded(True) 
        
        if node_type == "MILESTONE":
            p.setForeground(0, QColor("#1976D2") if not self.is_dark_mode else QColor("#64B5F6"))
            
        return p

    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        child.setCheckState(0, Qt.Unchecked)
        
        child.setText(0, run["r_name"])
        child.setText(1, run["rtl"]) 
        child.setText(2, run["source"]) 
        
        tooltip_text = (f"Owner: {run.get('owner', 'Unknown')}\n"
                        f"Size: Pending\n"
                        f"Runtime: {run['info']['runtime']}\n"
                        f"NONUPF: {run['st_n']}\n"
                        f"UPF: {run['st_u']}\n"
                        f"VSLP: {run['vslp_status']}\n"
                        f"Static IR: Check individual stage levels")
        child.setToolTip(0, tooltip_text)
        
        child.setExpanded(False)
        
        if run["source"] == "OUTFEED": child.setForeground(2, QColor("#b39ddb" if self.is_dark_mode else "#5e35b1")) 
        else: child.setForeground(2, QColor("#ffb74d" if self.is_dark_mode else "#f57c00")) 
        
        if run["run_type"] == "FE":
            child.setText(3, "COMPLETED" if run["is_comp"] else "RUNNING")
            child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(5, "-") 
            child.setText(6, f"NONUPF - {run['st_n']}") 
            child.setText(7, f"UPF - {run['st_u']}")
            child.setText(8, run["vslp_status"]) 
            child.setText(9, "-") 
            child.setText(10, run["info"]["runtime"])
            child.setText(11, run["info"]["start"])
            child.setText(12, run["info"]["end"])
            child.setText(13, run["path"])
            child.setText(14, os.path.join(run["path"], "logs/compile_opt.log"))
            child.setText(15, run["fm_u_path"])
            child.setText(16, run["fm_n_path"])
            child.setText(17, run["vslp_rpt_path"])
            child.setText(18, "") 
            child.setText(19, "") 
            
            if "FAILS" in run["st_n"]: child.setForeground(6, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "PASS" in run["st_n"]: child.setForeground(6, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))
            
            if "FAILS" in run["st_u"]: child.setForeground(7, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "PASS" in run["st_u"]: child.setForeground(7, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))
            
            if "Error" in run["vslp_status"] and "Error: 0" not in run["vslp_status"]: child.setForeground(8, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "Error: 0" in run["vslp_status"]: child.setForeground(8, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))
            
            child.setForeground(3, QColor("#81c784" if run["is_comp"] else "#4fc3f7") if self.is_dark_mode else QColor("#2e7d32" if run["is_comp"] else "#0277bd"))
        else:
            child.setText(5, "-") 
            child.setText(9, "-")
            child.setText(13, run["path"]) 
            child.setText(19, "") 
            
        for i in range(1, 20):
            child.setTextAlignment(i, Qt.AlignCenter)
        child.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
        
        return child

    def _add_stages(self, be_item, be_run):
        owner = be_run.get("owner", "Unknown")
        
        for stage in be_run["stages"]:
            st_item = CustomTreeItem(be_item)
            st_item.setFlags(st_item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            st_item.setCheckState(0, Qt.Unchecked)
            
            st_item.setData(0, Qt.UserRole, "STAGE")
            st_item.setData(1, Qt.UserRole, stage["name"])
            st_item.setData(2, Qt.UserRole, stage["qor_path"])
            
            ir_key = f"{be_run['r_name']}/{stage['name']}"
            ir_info = self.ir_data.get(ir_key, {"log": "", "line": "N/A", "value": "-"})
            
            tooltip_text = (f"Owner: {owner}\n"
                            f"Size: Pending\n"
                            f"Runtime: {stage['info']['runtime']}\n"
                            f"NONUPF: {stage['st_n']}\n"
                            f"UPF: {stage['st_u']}\n"
                            f"VSLP: {stage['vslp_status']}\n"
                            f"Static IR: {ir_info['line']}")
            st_item.setToolTip(0, tooltip_text)
            
            stage_status = "COMPLETED"
            if be_run["source"] == "WS" and not os.path.exists(stage["rpt"]):
                stage_status = "RUNNING"
                if os.path.exists(stage["log"]):
                    try:
                        with open(stage["log"], 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "START_CMD:" in line:
                                    stage_status = line.strip()
                    except: pass
            
            st_item.setText(0, stage["name"])
            st_item.setText(2, be_run["source"])
            st_item.setText(4, stage_status) 
            st_item.setText(5, "-") 
            st_item.setText(6, f"NONUPF - {stage['st_n']}") 
            st_item.setText(7, f"UPF - {stage['st_u']}")
            st_item.setText(8, stage["vslp_status"])
            st_item.setText(9, ir_info["value"])
            st_item.setText(10, stage["info"]["runtime"])
            st_item.setText(11, stage["info"]["start"])
            st_item.setText(12, stage["info"]["end"])
            st_item.setText(13, stage["stage_path"]) 
            st_item.setText(14, stage["log"])
            st_item.setText(15, stage["fm_u_path"])
            st_item.setText(16, stage["fm_n_path"])
            st_item.setText(17, stage["vslp_rpt_path"])
            st_item.setText(18, stage["sta_rpt_path"]) 
            st_item.setText(19, ir_info["log"])
            
            if "FAILS" in stage["st_n"]: st_item.setForeground(6, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "PASS" in stage["st_n"]: st_item.setForeground(6, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))
            if "FAILS" in stage["st_u"]: st_item.setForeground(7, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "PASS" in stage["st_u"]: st_item.setForeground(7, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))
            if "Error" in stage["vslp_status"] and "Error: 0" not in stage["vslp_status"]: st_item.setForeground(8, QColor("#ef5350" if self.is_dark_mode else "#d32f2f"))
            elif "Error: 0" in stage["vslp_status"]: st_item.setForeground(8, QColor("#66bb6a" if self.is_dark_mode else "#388e3c"))

            for i in range(1, 20):
                st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)

    def _get_item_path_id(self, item):
        parts = []
        while item is not None:
            parts.insert(0, item.text(0).strip())
            item = item.parent()
        return "|".join(parts)

    def refresh_view(self):
        for w in self.size_workers:
            if hasattr(w, 'cancel'):
                w.cancel()
        self.item_map.clear()
        
        expanded_states = {}
        def save_state(node):
            for i in range(node.childCount()):
                child = node.child(i)
                expanded_states[self._get_item_path_id(child)] = child.isExpanded()
                save_state(child)
                
        save_state(self.tree.invisibleRootItem())

        self.tree.setUpdatesEnabled(False)
        self.tree.clear()
        self.tree.setSortingEnabled(False)
        
        src_mode = self.src_combo.currentText()
        sel_rtl = self.rel_combo.currentText()
        
        raw_query = self.search.text().lower()
        if not raw_query: search_pattern = "*"
        elif '*' not in raw_query: search_pattern = f"*{raw_query}*"
        else: search_pattern = raw_query
            
        checked_blks = [self.blk_list.item(i).text() for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]
        
        runs_to_process = []
        if src_mode in ["WS", "ALL"] and self.ws_data:
            runs_to_process.extend(self.ws_data.get("all_runs", []))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            runs_to_process.extend(self.out_data.get("all_runs", []))

        ignored_runs_list = []
        normal_runs_list = []

        for run in runs_to_process:
            if run["path"] in self.ignored_paths:
                ignored_runs_list.append(run)
                continue
                
            if run["block"] not in checked_blks:
                continue
                
            base_rtl_filter = re.sub(r'_syn\d+$', '', run["rtl"])
            if get_milestone(base_rtl_filter) is None:
                continue
                
            if sel_rtl != "[ SHOW ALL ]":
                if not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")):
                    continue

            if search_pattern != "*":
                combined_text = f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} {run['st_n']} {run['st_u']} {run['vslp_status']} {run['info']['runtime']} {run['info']['start']} {run['info']['end']}".lower()
                matches = fnmatch.fnmatch(combined_text, search_pattern)
                
                if not matches and run["run_type"] == "BE":
                    for stage in run["stages"]:
                        st_comb = f"{stage['name']} {stage['st_n']} {stage['st_u']} {stage['vslp_status']} {stage['info']['runtime']}".lower()
                        if fnmatch.fnmatch(st_comb, search_pattern):
                            matches = True
                            break
                            
                if not matches:
                    continue

            normal_runs_list.append(run)

        fe_runs = [r for r in normal_runs_list if r["run_type"] == "FE"]
        be_runs = [r for r in normal_runs_list if r["run_type"] == "BE"]
        matched_be_ids = set()

        root = self.tree.invisibleRootItem()

        for fe_run in fe_runs:
            blk_name = fe_run["block"]
            run_rtl = fe_run["rtl"]
            base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
            has_syn = (run_rtl != base_rtl)

            block_node = self._get_node(root, blk_name, "BLOCK")

            if sel_rtl == "[ SHOW ALL ]":
                milestone = get_milestone(base_rtl)
                m_node = self._get_node(block_node, milestone, "MILESTONE")
                rtl_node = self._get_node(m_node, base_rtl, "RTL")
                parent_for_run = rtl_node
            elif sel_rtl == base_rtl and has_syn:
                syn_node = self._get_node(block_node, run_rtl, "RTL")
                parent_for_run = syn_node
            else:
                parent_for_run = block_node
            
            fe_item = self._create_run_item(parent_for_run, fe_run)
            fe_base = fe_run["r_name"].replace("-FE", "")
            
            for be_run in be_runs:
                if be_run["block"] == fe_run["block"] and (f"_{fe_base}_" in be_run["r_name"] or be_run["r_name"].startswith(f"{fe_base}_")):
                    be_item = self._create_run_item(fe_item, be_run)
                    self._add_stages(be_item, be_run)
                    matched_be_ids.add(id(be_run))

        for be_run in be_runs:
            if id(be_run) not in matched_be_ids:
                blk_name = be_run["block"]
                run_rtl = be_run["rtl"]
                base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
                has_syn = (run_rtl != base_rtl)

                block_node = self._get_node(root, blk_name, "BLOCK")

                if sel_rtl == "[ SHOW ALL ]":
                    milestone = get_milestone(base_rtl)
                    m_node = self._get_node(block_node, milestone, "MILESTONE")
                    rtl_node = self._get_node(m_node, base_rtl, "RTL")
                    other_pnr_node = self._get_node(rtl_node, "Other PNR runs", "OTHER")
                    parent_for_run = other_pnr_node
                elif sel_rtl == base_rtl and has_syn:
                    syn_node = self._get_node(block_node, run_rtl, "RTL")
                    other_pnr_node = self._get_node(syn_node, "Other PNR runs", "OTHER")
                    parent_for_run = other_pnr_node
                else:
                    other_pnr_node = self._get_node(block_node, "Other PNR runs", "OTHER")
                    parent_for_run = other_pnr_node
                
                be_item = self._create_run_item(parent_for_run, be_run)
                self._add_stages(be_item, be_run)

        if ignored_runs_list:
            ignored_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
            for run in ignored_runs_list:
                blk_name = run["block"]
                run_rtl = run["rtl"]
                base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
                has_syn = (run_rtl != base_rtl)

                block_node = self._get_node(ignored_root, blk_name, "BLOCK")

                if sel_rtl == "[ SHOW ALL ]":
                    milestone = get_milestone(base_rtl)
                    m_node = self._get_node(block_node, milestone, "MILESTONE")
                    rtl_node = self._get_node(m_node, base_rtl, "RTL")
                    parent_for_run = rtl_node
                elif sel_rtl == base_rtl and has_syn:
                    syn_node = self._get_node(block_node, run_rtl, "RTL")
                    parent_for_run = syn_node
                else:
                    parent_for_run = block_node
                
                item = self._create_run_item(parent_for_run, run)
                if run["run_type"] == "BE":
                    self._add_stages(item, run)

        self.tree.setSortingEnabled(True)

        def restore_state(node):
            for i in range(node.childCount()):
                child = node.child(i)
                path_key = self._get_item_path_id(child)
                node_type = child.data(0, Qt.UserRole)
                is_run = bool(child.text(13)) 
                
                if path_key in expanded_states:
                    child.setExpanded(expanded_states[path_key])
                else:
                    if child.parent() is None: 
                        child.setExpanded(True) 
                    elif node_type == "MILESTONE":
                        child.setExpanded(False) 
                    elif child.text(0).strip() == "Other PNR runs":
                        child.setExpanded(False) 
                    elif is_run:
                        child.setExpanded(False) 
                    else:
                        child.setExpanded(True) 
                        
                restore_state(child)

        restore_state(self.tree.invisibleRootItem())
        self.tree.setUpdatesEnabled(True)

    def on_header_context_menu(self, pos):
        menu = QMenu(self)
        menu.setStyleSheet("QMenu { border: 1px solid gray; }" if self.is_dark_mode else "")
        
        for i in range(1, 20): 
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
        if not item or not item.parent(): return
        
        col = self.tree.columnAt(pos.x())
        m = QMenu()
        
        cell_text = item.text(col).strip()
        copy_cell_act = None
        if cell_text:
            copy_cell_act = m.addAction(f"Copy Cell Text")
            m.addSeparator()
            
        run_path = item.text(13) 
        log_path = item.text(14)
        fm_u_path = item.text(15)
        fm_n_path = item.text(16)
        vslp_path = item.text(17)
        sta_path = item.text(18)
        ir_path = item.text(19)
        
        ignore_checked_act = m.addAction("Ignore All Checked Runs")
        m.addSeparator()

        ignore_act = None
        restore_act = None
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        
        if run_path and run_path != "N/A" and not is_stage:
            if run_path in self.ignored_paths:
                restore_act = m.addAction("Restore Current Run (Un-ignore)")
            else:
                ignore_act = m.addAction("Ignore Current Run")
            m.addSeparator()
        
        calc_size_act = None
        if run_path and run_path != "N/A" and os.path.exists(run_path):
            calc_size_act = m.addAction("Calculate Folder Size")
            m.addSeparator()

        fm_n_act = None
        if fm_n_path and fm_n_path != "N/A" and os.path.exists(fm_n_path):
            fm_n_act = m.addAction("Open NONUPF Formality Report")
            
        fm_u_act = None
        if fm_u_path and fm_u_path != "N/A" and os.path.exists(fm_u_path):
            fm_u_act = m.addAction("Open UPF Formality Report")
            
        v_act = None
        if vslp_path and vslp_path != "N/A" and os.path.exists(vslp_path):
            v_act = m.addAction("Open VSLP Report")
            
        sta_act = None
        if sta_path and sta_path != "N/A" and os.path.exists(sta_path):
            sta_act = m.addAction("Open PT STA Summary")
            
        ir_act = None
        if ir_path and ir_path != "N/A" and os.path.exists(ir_path):
            ir_act = m.addAction("Open Static IR Log")
            
        log_act = None
        if log_path and log_path != "N/A" and os.path.exists(log_path):
            log_act = m.addAction("Open Log File")
            
        m.addSeparator()
        c_act = m.addAction("Copy Path")
        
        qor_act = None
        if is_stage:
            m.addSeparator()
            qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        
        if res:
            if ignore_checked_act and res == ignore_checked_act:
                def ignore_checked(node):
                    for i in range(node.childCount()):
                        c = node.child(i)
                        if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                            p = c.text(13)
                            if p and p != "N/A": self.ignored_paths.add(p)
                        ignore_checked(c)
                ignore_checked(self.tree.invisibleRootItem())
                self.refresh_view()
            elif ignore_act and res == ignore_act:
                self.ignored_paths.add(run_path)
                self.refresh_view()
            elif restore_act and res == restore_act:
                self.ignored_paths.discard(run_path)
                self.refresh_view()
            elif copy_cell_act and res == copy_cell_act:
                QApplication.clipboard().setText(cell_text)
            elif calc_size_act and res == calc_size_act:
                item.setText(5, "Calc...")
                worker = SingleSizeWorker(item, run_path)
                worker.result.connect(lambda it, sz: (it.setText(5, sz), it.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {sz}\n', it.toolTip(0) if it.toolTip(0) else ""))))
                if not hasattr(self, 'size_workers'): self.size_workers = []
                self.size_workers.append(worker)
                worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
                worker.start()
            elif res == fm_n_act:
                subprocess.Popen(['gvim', fm_n_path])
            elif res == fm_u_act:
                subprocess.Popen(['gvim', fm_u_path])
            elif res == v_act:
                subprocess.Popen(['gvim', vslp_path])
            elif sta_act and res == sta_act:
                subprocess.Popen(['gvim', sta_path])
            elif ir_act and res == ir_act:
                subprocess.Popen(['gvim', ir_path])
            elif log_act and res == log_act:
                subprocess.Popen(['gvim', log_path])
            elif res == c_act:
                QApplication.clipboard().setText(run_path) 
            elif qor_act and res == qor_act:
                step_name = item.data(1, Qt.UserRole)
                qor_path = item.data(2, Qt.UserRole)
                subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
                h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
                if h: subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])

    def on_item_double_clicked(self, item, col):
        if item.parent():
            log = item.text(14)
            if log and os.path.exists(log): subprocess.Popen(['gvim', log])

    def run_qor_comparison(self):
        sel = []
        root = self.tree.invisibleRootItem()
        
        def get_checked_paths(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked:
                    if c.data(0, Qt.UserRole) != "STAGE":
                        qor_path = c.text(13) 
                        run_source = c.text(2)
                        
                        if run_source == "OUTFEED" and qor_path:
                            qor_path = os.path.dirname(qor_path)
                        
                        if qor_path and not qor_path.endswith("/"):
                            qor_path += "/"
                            
                        sel.append(qor_path)
                get_checked_paths(c) 
                
        get_checked_paths(root)
                    
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
        if h: subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = PDDashboard(); w.show()
    sys.exit(app.exec_())
