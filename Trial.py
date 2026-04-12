import os
import glob
import re
import subprocess
import sys
import fnmatch
import concurrent.futures
import pwd
import time
import datetime
import getpass

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QTreeWidget, QTreeWidgetItem,
    QPushButton, QMessageBox, QListWidget, QListWidgetItem,
    QProgressBar, QMenu, QSplitter, QFontComboBox, QSpinBox,
    QWidgetAction, QCheckBox, QDialog, QFormLayout, QDialogButtonBox,
    QStatusBar, QFrame, QShortcut, QAction, QToolButton, QStyle, QColorDialog,
    QTextEdit, QTableWidget, QTableWidgetItem, QHeaderView
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QDateTime, QRectF
from PyQt5.QtGui import QColor, QFont, QClipboard, QKeySequence, QPalette, QBrush, QPainter, QPen

# ===========================================================================
# --- CONFIGURATION BLOCK (Project Dependent) ---
# ===========================================================================
PROJECT_PREFIX = "S5K2P5SP"

BASE_WS_FE_DIR   = "/user/s5k2p5sx.fe1/s5k2p5sp/WS"
BASE_WS_BE_DIR   = "/user/s5k2p5sp.be1/s5k2p5sp/WS"
BASE_OUTFEED_DIR = "/user/s5k2p5sx.fe1/s5k2p5sp/outfeed"
BASE_IR_DIR      = "/user/s5k2p5sx.be1/LAYOUT/IR/"
PNR_TOOL_NAMES   = "fc innovus"
SUMMARY_SCRIPT   = "/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py"
FIREFOX_PATH     = "/usr/bin/firefox"
MAIL_UTIL        = "/user/vwpmailsystem/MAIL/send_mail_for_rhel7"
USER_INFO_UTIL   = "/usr/local/bin/user_info"
# ===========================================================================

# ---------------------------------------------------------------------------
# Performance Cache & Helpers
# ---------------------------------------------------------------------------
_path_cache = {}

def cached_exists(path):
    if path not in _path_cache: _path_cache[path] = os.path.exists(path)
    return _path_cache[path]

def clear_path_cache(): _path_cache.clear()

def relative_time(date_str):
    if not date_str or date_str == "N/A": return date_str
    try:
        m = re.search(r'(\w{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{2}):(\d{2})', str(date_str))
        if not m: return date_str
        month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        mon, day, year, hour, minute = m.groups()
        dt = datetime.datetime(int(year), month_map.get(mon, 1), int(day), int(hour), int(minute))
        delta = datetime.datetime.now() - dt
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0: return date_str
        if total_seconds < 3600: return f"{total_seconds // 60}m ago"
        if total_seconds < 86400: return f"{total_seconds // 3600}h {(total_seconds % 3600) // 60}m ago"
        days = total_seconds // 86400
        return f"{days}d ago"
    except: return date_str

def get_user_email(username):
    if not username or username == "Unknown": return ""
    try:
        res = subprocess.check_output([USER_INFO_UTIL, '-a', username], stderr=subprocess.DEVNULL).decode('utf-8')
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', res)
        if match: return match.group(0)
    except: pass
    return f"{username}@samsung.com" 

# ===========================================================================
# --- CUSTOM SORTING UI CLASS ---
# ===========================================================================
class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""
        if col == 0:
            if t1 == "[ Ignored Runs ]": return False
            if t2 == "[ Ignored Runs ]": return True
            m_order = {"INITIAL RELEASE": 1, "PRE-SVP": 2, "SVP": 3, "FFN": 4}
            if t1 in m_order and t2 in m_order:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return m_order[t1] < m_order[t2] if asc else m_order[t1] > m_order[t2]
        return t1 < t2

# ===========================================================================
# --- LOGIC HELPERS ---
# ===========================================================================
def get_owner(path):
    if not path or not cached_exists(path): return "Unknown"
    try: return pwd.getpwuid(os.stat(path).st_uid).pw_name
    except: return "Unknown"

def normalize_rtl(rtl_str):
    if rtl_str and rtl_str.startswith("EVT"): return f"{PROJECT_PREFIX}_{rtl_str}"
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
        day_of_week, mon, day, t, year = match.groups()
        return f"{day_of_week} {mon} {day}, {year} - {t}"
    return str(date_str).strip()

def get_dynamic_evt_path(rtl_tag, block_name):
    match = re.search(r'(EVT\d+_ML\d+_DEV\d+)', str(rtl_tag))
    if not match: return ""
    return os.path.join(BASE_OUTFEED_DIR, block_name, match.group(1))

def get_fm_info(report_path):
    if not report_path or not cached_exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            for line in f:
                if "No failing compare points" in line: return "PASS"
                m = re.search(r'(\d+)\s+Failing compare points', line)
                if m: return f"{m.group(1)} FAILS"
    except: pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not cached_exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            in_summary = False
            for line in f:
                if "Management Summary" in line: in_summary = True; continue
                if in_summary and line.strip().startswith("Total"):
                    parts = line.strip().split()
                    if len(parts) >= 3: return f"Error: {parts[1]}, Warning: {parts[2]}"
                    break
    except: pass
    return "Not Found"

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not cached_exists(file_path): return d
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if "TOTAL_START" in line and "Load :" in line:
                    d["start"] = format_log_date(line.split("Load :")[-1].strip())
                m = re.search(r'TimeStamp\s*:\s*(\S+)', line)
                if m and m.group(1) not in ["TOTAL", "TOTAL_START"]: d["last_stage"] = m.group(1)
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(r'Total\s*:\s*(\d+)h:(\d+)m:(\d+)s', line)
                    if rt: d["runtime"] = f"{int(rt.group(1)):02}h:{int(rt.group(2)):02}m:{int(rt.group(3)):02}s"
                    if "Load :" in line: d["end"] = format_log_date(line.split("Load :")[-1].strip())
    except: pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not cached_exists(file_path): return d
    try:
        first_ts, last_ts, final_time_str = None, None, None
        months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
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
                        if len(parts) > 1 and not parts[1].isdigit(): d["last_stage"] = parts[1]
        if first_ts:
            y, mo, day, H, M = first_ts.groups()
            d["start"] = f"{months[int(mo)-1]} {int(day):02d}, {y} - {H}:{M}"
        if last_ts:
            y, mo, day, H, M = last_ts.groups()
            d["end"] = f"{months[int(mo)-1]} {int(day):02d}, {y} - {H}:{M}"
        if final_time_str: d["runtime"] = final_time_str
    except: pass
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

# ===========================================================================
# --- UI DIALOGS ---
# ===========================================================================
class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Settings")
        self.resize(400, 350)
        layout = QFormLayout(self)
        layout.setSpacing(12)

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
        layout.addRow("Row Spacing (px):", self.space_spin)

        self.rel_time_cb = QCheckBox("Show relative timestamps (e.g. '2h ago')")
        self.rel_time_cb.setChecked(parent.show_relative_time if parent else False)
        layout.addRow("", self.rel_time_cb)
        
        self.theme_cb = QCheckBox("Enable Dark Mode (Default Theme)")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addRow(sep)
        
        self.use_custom_cb = QCheckBox("Enable Custom Colors")
        self.use_custom_cb.setChecked(parent.use_custom_colors if parent else False)
        layout.addRow("Theme:", self.use_custom_cb)

        self.bg_color = parent.custom_bg_color if parent else "#2b2d30"
        self.fg_color = parent.custom_fg_color if parent else "#dfe1e5"
        self.sel_color = parent.custom_sel_color if parent else "#2f65ca"

        self.bg_btn = QPushButton("Pick Background Color")
        self.bg_btn.clicked.connect(self.pick_bg)
        self.fg_btn = QPushButton("Pick Text Color")
        self.fg_btn.clicked.connect(self.pick_fg)
        self.sel_btn = QPushButton("Pick Selection Color")
        self.sel_btn.clicked.connect(self.pick_sel)

        layout.addRow("Background:", self.bg_btn)
        layout.addRow("Text:", self.fg_btn)
        layout.addRow("Highlight:", self.sel_btn)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addRow(self.buttons)

    def pick_bg(self):
        c = QColorDialog.getColor(QColor(self.bg_color), self)
        if c.isValid(): self.bg_color = c.name()
    def pick_fg(self):
        c = QColorDialog.getColor(QColor(self.fg_color), self)
        if c.isValid(): self.fg_color = c.name()
    def pick_sel(self):
        c = QColorDialog.getColor(QColor(self.sel_color), self)
        if c.isValid(): self.sel_color = c.name()

class MailDialog(QDialog):
    def __init__(self, default_subject, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Group Mail Configuration")
        self.resize(600, 250)
        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.subject_input = QLineEdit(default_subject)
        form.addRow("Subject:", self.subject_input)
        layout.addLayout(form)

        layout.addWidget(QLabel("Message Header (The selected runs will be appended automatically below this):"))
        self.body_input = QTextEdit()
        self.body_input.setPlainText("Hi,\n\nPlease remove these runs as they are consuming disk space and are no longer needed:")
        layout.addWidget(self.body_input)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Send Mail")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

# ===========================================================================
# --- PIE CHART & DISK USAGE UI ---
# ===========================================================================
class PieChartWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setMinimumSize(300, 300)
        self.data = {} 
        self.colors = [
            QColor("#ef5350"), QColor("#42a5f5"), QColor("#66bb6a"), QColor("#ffa726"),
            QColor("#ab47bc"), QColor("#26c6da"), QColor("#8d6e63"), QColor("#78909c"),
            QColor("#d4e157"), QColor("#ec407a")
        ]
        self.bg_col = "#ffffff"

    def set_data(self, data, is_dark):
        self.data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
        self.bg_col = "#2b2d30" if is_dark else "#ffffff"
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        rect = self.rect()
        margin = 15
        pie_rect = QRectF(rect.adjusted(margin, margin, -margin, -margin))
        
        total = sum(self.data.values())
        if total == 0:
            painter.setPen(QColor("#888888"))
            painter.drawText(rect, Qt.AlignCenter, "No Data Available")
            return

        start_angle = 0
        for i, (name, val) in enumerate(self.data.items()):
            span_angle = (val / total) * 360 * 16
            painter.setBrush(QBrush(self.colors[i % len(self.colors)]))
            painter.setPen(QPen(QColor(self.bg_col), 2)) 
            painter.drawPie(pie_rect, int(start_angle), int(span_angle))
            start_angle += span_angle

class DiskUsageDialog(QDialog):
    def __init__(self, disk_data, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Disk Space Usage (User-Wise)")
        self.resize(800, 500)
        self.disk_data = disk_data
        self.is_dark = is_dark
        
        layout = QVBoxLayout(self)
        
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("<b>Select Directory Scope:</b>"))
        self.combo = QComboBox()
        self.combo.addItems(["WS (FE)", "WS (BE)", "OUTFEED"])
        self.combo.currentIndexChanged.connect(self.update_view)
        top_row.addWidget(self.combo)
        top_row.addStretch()
        layout.addLayout(top_row)

        main_body = QHBoxLayout()
        self.pie = PieChartWidget()
        main_body.addWidget(self.pie, 1)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["", "User", "Size (GB)"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.verticalHeader().setVisible(False)
        main_body.addWidget(self.table, 1)

        layout.addLayout(main_body, 1)
        self.update_view()

    def update_view(self):
        cat = self.combo.currentText()
        data = self.disk_data.get(cat, {})
        self.pie.set_data(data, self.is_dark)
        
        self.table.setRowCount(0)
        sorted_data = sorted(data.items(), key=lambda item: item[1], reverse=True)
        for i, (user, sz) in enumerate(sorted_data):
            self.table.insertRow(i)
            
            color_item = QTableWidgetItem("")
            color_item.setBackground(QColor(self.pie.colors[i % len(self.pie.colors)]))
            
            it_u = QTableWidgetItem(user)
            it_u.setForeground(QColor(self.pie.colors[i % len(self.pie.colors)]))
            
            it_s = QTableWidgetItem(f"{sz:.2f} GB")
            
            self.table.setItem(i, 0, color_item)
            self.table.setItem(i, 1, it_u)
            self.table.setItem(i, 2, it_s)

class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def _get_dir_info(self, path):
        try:
            owner = pwd.getpwuid(os.stat(path).st_uid).pw_name
            sz = int(subprocess.check_output(['du', '-sk', path], stderr=subprocess.DEVNULL).decode().split()[0])
            return owner, sz
        except: return "Unknown", 0

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        
        targets_map = {
            "WS (FE)": glob.glob(os.path.join(BASE_WS_FE_DIR, "*")),
            "WS (BE)": glob.glob(os.path.join(BASE_WS_BE_DIR, "*")),
            "OUTFEED": glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "*")) 
        }
        
        tasks = []
        for cat, paths in targets_map.items():
            for p in paths:
                if os.path.isdir(p): tasks.append((cat, p))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_task = {executor.submit(self._get_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                cat = future_to_task[future]
                try:
                    owner, sz_kb = future.result()
                    if sz_kb > 0:
                        gb_sz = sz_kb / (1024**2) 
                        if gb_sz > 0.01: 
                            results[cat][owner] = results[cat].get(owner, 0) + gb_sz
                except: pass
                
        self.finished_scan.emit(results)

# ===========================================================================
# --- BACKGROUND WORKER THREADS (Sizes & Scan) ---
# ===========================================================================
class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)
    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self._is_cancelled = False
    def run(self):
        max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self.get_size, path): item_id for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled: break
                item_id = futures[future]
                try: self.size_calculated.emit(item_id, future.result())
                except: self.size_calculated.emit(item_id, "N/A")
    def get_size(self, path):
        if not path or not os.path.exists(path): return "N/A"
        try: return subprocess.check_output(['du', '-sh', path], stderr=subprocess.DEVNULL).decode('utf-8').split()[0]
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
    finished       = pyqtSignal(dict, dict, dict)
    progress_update = pyqtSignal(int, int)

    def scan_ir_dir(self):
        ir_data = {}
        if not os.path.exists(BASE_IR_DIR): return ir_data
        target_lef = f"{PROJECT_PREFIX}.lef.list"
        for root_dir, dirs, files in os.walk(BASE_IR_DIR):
            if "redhawk.log" not in files: continue
            log_path = os.path.join(root_dir, "redhawk.log")
            run_be_name = step_name = None
            inst_line = inst_value = ""
            try:
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        if line.startswith("Parsing ") and target_lef in line:
                            m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                            if m: run_be_name = m.group(1); step_name = m.group(2)
                        elif line.startswith("INST ") and "mV" in line:
                            inst_line = line.strip()
                            m2 = re.search(r'INST\s+(\S+mV)', inst_line)
                            if m2: inst_value = m2.group(1)
            except: pass
            if run_be_name and step_name:
                key = f"{run_be_name}/{step_name}"
                ir_data[key] = {"log": log_path, "line": inst_line, "value": inst_value}
        return ir_data

    def run(self):
        clear_path_cache()
        ws_data  = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data = {"releases": {}, "blocks": set(), "all_runs": []}
        tasks = []
        tools_to_scan = PNR_TOOL_NAMES.split()

        for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
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
                            if os.path.isdir(rd): tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

        if os.path.exists(BASE_OUTFEED_DIR):
            for ent_name in os.listdir(BASE_OUTFEED_DIR):
                ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
                if not os.path.isdir(ent_path): continue
                for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                    phys_evt = os.path.basename(evt_dir)
                    for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                        tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                    if "fc" in tools_to_scan:
                        be_runs = (glob.glob(os.path.join(evt_dir, "fc", "*-BE")) + glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")))
                        for rd in be_runs: tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                            if os.path.isdir(rd): tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        total_tasks = len(tasks)
        completed_tasks = 0
        max_w = min(40, (os.cpu_count() or 4) * 6)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            ir_future = executor.submit(self.scan_ir_dir)
            future_to_task = {executor.submit(self._thread_process_run, t): t for t in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        if result["source"] == "WS":
                            ws_data["blocks"].add(result["block"])
                            ws_data["all_runs"].append(result)
                            if result["run_type"] == "BE": self._map_release(ws_data, result["rtl"], result["parent"])
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
            rtl = extract_rtl(rd) if run_type == "BE" else base_rtl
            if rtl == "Unknown": rtl = base_rtl
        return self._process_run(b_name, rd, parent_path, rtl, source, run_type)

    def _resolve_outfeed_rtl(self, rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl): rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif rtl == "Unknown": rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    def _process_run(self, b_name, rd, parent_path, rtl, source, run_type):
        r_name      = os.path.basename(rd)
        clean_run   = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)
        evt_base    = get_dynamic_evt_path(rtl, b_name)
        owner       = get_owner(rd)

        fm_n     = os.path.join(evt_base, "fm", clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")
        info     = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))

        is_comp = True if source == "OUTFEED" else cached_exists(os.path.join(rd, "pass/compile_opt.pass"))
        fe_status = "RUNNING"
        
        if run_type == "FE":
            if is_comp: fe_status = "COMPLETED"
            else:
                log_file = os.path.join(rd, "logs/compile_opt.log")
                if not cached_exists(log_file): fe_status = "NOT STARTED"
                else:
                    fe_status = "RUNNING"
                    try:
                        if subprocess.run(["grep", "-m", "1", "Stack trace for crashing thread", log_file], stdout=subprocess.DEVNULL).returncode == 0: fe_status = "FATAL ERROR"
                        elif subprocess.run(["grep", "-m", "1", "Information: Process terminated by interrupt. (INT-4)", log_file], stdout=subprocess.DEVNULL).returncode == 0: fe_status = "INTERRUPTED"
                    except: pass

        stages = []
        if run_type == "BE":
            search_glob = os.path.join(rd, "outputs", "*") if source == "WS" else os.path.join(rd, "*")
            for s_dir in glob.glob(search_glob):
                if not os.path.isdir(s_dir): continue
                step_name = os.path.basename(s_dir)
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]: continue

                if source == "WS":
                    rpt, log = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt"), os.path.join(rd, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, "outputs", step_name)
                else:
                    rpt, log = os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt"), os.path.join(s_dir, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, step_name)

                fm_u_glob   = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                fm_n_glob   = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                st_vslp_rpt  = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                sta_rpt      = os.path.join(evt_base, "pt",   r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                qor_path     = rd if rd.endswith("/") else rd + "/"

                stages.append({
                    "name": step_name, "rpt": rpt, "log": log, "info": parse_pnr_runtime_rpt(rpt),
                    "st_n": get_fm_info(st_fm_n_path), "st_u": get_fm_info(st_fm_u_path), "vslp_status": get_vslp_info(st_vslp_rpt),
                    "fm_u_path": st_fm_u_path, "fm_n_path": st_fm_n_path, "vslp_rpt_path": st_vslp_rpt, "sta_rpt_path": sta_rpt,
                    "qor_path": qor_path, "stage_path": stage_path
                })

        return {
            "block": b_name, "path": rd, "parent": parent_path, "rtl": rtl, "r_name": r_name, "run_type": run_type,
            "stages": stages, "source": source, "owner": owner, "is_comp": is_comp, "fe_status": fe_status,
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


# ===========================================================================
# --- MAIN DASHBOARD WINDOW ---
# ===========================================================================
class PDDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Unified PD Dashboard  |  Pro Edition")
        self.resize(1920, 1000)

        # -- state
        self.ws_data  = {}
        self.out_data = {}
        self.ir_data  = {}
        
        self.is_dark_mode       = False
        self.use_custom_colors  = False
        self.custom_bg_color    = "#2b2d30"
        self.custom_fg_color    = "#dfe1e5"
        self.custom_sel_color   = "#2f65ca"
        
        self.row_spacing        = 2
        self.show_relative_time = False 
        self._columns_fitted_once = False
        self._last_scan_time    = None
        self.is_compact         = False

        self.size_workers  = []
        self.item_map      = {}
        self.ignored_paths = set()
        self._checked_paths = set()
        self.current_error_log_path = None

        # timers
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)

        self.init_ui()
        self._setup_shortcuts()
        self.start_fs_scan()

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(6, 6, 6, 4)
        root_layout.setSpacing(4)

        # ---- TOP TOOLBAR (Sleek Single Row) ----
        top_layout = QHBoxLayout()
        top_layout.setSpacing(6)

        # Source
        top_layout.addWidget(self._label("Source:"))
        self.src_combo = QComboBox()
        self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.setFixedWidth(100)
        self.src_combo.currentIndexChanged.connect(self.on_source_changed)
        top_layout.addWidget(self.src_combo)

        # RTL Release
        self._add_separator(top_layout)
        top_layout.addWidget(self._label("RTL Release:"))
        self.rel_combo = QComboBox()
        self.rel_combo.setMinimumWidth(220) 
        self.rel_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.rel_combo)

        # View
        self._add_separator(top_layout)
        top_layout.addWidget(self._label("View:"))
        self.view_combo = QComboBox()
        self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Running Only", "Failed Only", "Today's Runs"])
        self.view_combo.setFixedWidth(120)
        self.view_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.view_combo)

        # Search
        self._add_separator(top_layout)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search runs, blocks, status, runtime...   [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.textChanged.connect(lambda: self.search_timer.start(500))
        top_layout.addWidget(self.search)

        top_layout.addStretch()

        # Action Buttons
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setToolTip("Refresh [Ctrl+R]")
        self.refresh_btn.clicked.connect(self.start_fs_scan)
        top_layout.addWidget(self.refresh_btn)

        self.auto_combo = QComboBox()
        self.auto_combo.addItems(["Off", "1 Min", "5 Min", "10 Min"])
        self.auto_combo.setFixedWidth(75)
        self.auto_combo.setToolTip("Auto-refresh interval")
        self.auto_combo.currentIndexChanged.connect(self.on_auto_refresh_changed)
        top_layout.addWidget(self.auto_combo)

        self._add_separator(top_layout)

        self.tools_btn = QPushButton("Tools")
        self.tools_menu = QMenu(self)
        self.tools_menu.addAction("Fit Columns  [Ctrl+Shift+F]", self.fit_all_columns)
        self.tools_menu.addAction("Expand All   [Ctrl+E]",       lambda: self.tree.expandAll())
        self.tools_menu.addAction("Collapse All [Ctrl+W]",       lambda: self.tree.collapseAll())
        self.tools_menu.addSeparator()
        self.tools_menu.addAction("Calculate All Run Sizes",      self.calculate_all_sizes)
        self.tools_btn.setMenu(self.tools_menu)
        top_layout.addWidget(self.tools_btn)

        self.qor_btn = self._btn("Compare QoR", self.run_qor_comparison)
        top_layout.addWidget(self.qor_btn)
        
        self.mail_btn = self._btn("Send Mail", self.send_mail_action)
        top_layout.addWidget(self.mail_btn)
        
        self.disk_btn = self._btn("Disk Space", self.open_disk_usage)
        top_layout.addWidget(self.disk_btn)
        
        self.settings_btn = self._btn("Settings", self.open_settings)
        top_layout.addWidget(self.settings_btn)

        # Window Size Toggle Icon Button (placed gracefully at the end)
        self.size_toggle_btn = QToolButton()
        self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarNormalButton))
        self.size_toggle_btn.setToolTip("Toggle Compact Window Mode")
        self.size_toggle_btn.clicked.connect(self.toggle_window_size)
        self.size_toggle_btn.setFixedSize(28, 28)
        top_layout.addWidget(self.size_toggle_btn)

        root_layout.addLayout(top_layout)

        # ---- SCAN PROGRESS ----
        self.prog = QProgressBar()
        self.prog.setVisible(False)
        self.prog.setFixedHeight(6)
        self.prog.setTextVisible(False)
        self.prog.setStyleSheet("QProgressBar { border: none; border-radius: 3px; background: #ddd; }"
                                "QProgressBar::chunk { background: #1976D2; border-radius: 3px; }")
        root_layout.addWidget(self.prog)

        # ---- MAIN SPLITTER ----
        self.splitter = QSplitter(Qt.Horizontal)

        # Left: block filter panel
        left_panel = QWidget()
        left_panel.setMaximumWidth(320)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 4, 0)
        left_layout.setSpacing(6)

        blk_header = QHBoxLayout()
        blk_header.addWidget(self._label("<b>Blocks</b>"))
        blk_header.addStretch()
        
        all_btn  = QPushButton("All")
        none_btn = QPushButton("None")
        
        for btn in (all_btn, none_btn):
            btn.setCursor(Qt.PointingHandCursor)
            btn.setObjectName("linkBtn")
            
        all_btn.clicked.connect(lambda: self._set_all_blocks(True))
        none_btn.clicked.connect(lambda: self._set_all_blocks(False))
        
        blk_header.addWidget(all_btn)
        sep_label = self._label("|")
        sep_label.setStyleSheet("color: gray;")
        blk_header.addWidget(sep_label)
        blk_header.addWidget(none_btn)
        left_layout.addLayout(blk_header)

        self.blk_list = QListWidget()
        self.blk_list.setAlternatingRowColors(True)
        f = self.blk_list.font()
        f.setPointSize(f.pointSize() + 1)
        f.setBold(True)
        self.blk_list.setFont(f)
        
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(100))
        left_layout.addWidget(self.blk_list)
        
        self.fe_error_btn = QPushButton("")
        self.fe_error_btn.setCursor(Qt.PointingHandCursor)
        self.fe_error_btn.setObjectName("errorLinkBtn")
        self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log)
        left_layout.addWidget(self.fe_error_btn)

        self.splitter.addWidget(left_panel)

        # Right: tree
        self.tree = QTreeWidget()
        self.tree.setColumnCount(21) 
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)     
        self.tree.setAnimated(False)             
        self.tree.setExpandsOnDoubleClick(False) 
        self.tree.setSortingEnabled(True)
        self.tree.header().setSectionsMovable(True)
        self.tree.header().setStretchLastSection(True) 

        headers = [
            "Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "User", "Size",
            "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG"
        ]
        self.tree.setHeaderLabels(headers)
        for i in range(self.tree.columnCount()):
            self.tree.headerItem().setTextAlignment(i, Qt.AlignCenter)

        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)

        self.tree.setColumnWidth(0, 380); self.tree.setColumnWidth(1, 260)
        self.tree.setColumnWidth(2, 90);  self.tree.setColumnWidth(3, 110)
        self.tree.setColumnWidth(4, 130); self.tree.setColumnWidth(5, 100) 
        self.tree.setColumnWidth(6, 80);  self.tree.setColumnWidth(7, 160)
        self.tree.setColumnWidth(8, 160); self.tree.setColumnWidth(9, 200)
        self.tree.setColumnWidth(10, 100); self.tree.setColumnWidth(11, 110)
        self.tree.setColumnWidth(12, 120); self.tree.setColumnWidth(13, 120)

        self.tree.itemExpanded.connect(lambda: self.tree.resizeColumnToContents(0))
        self.tree.itemCollapsed.connect(lambda: self.tree.resizeColumnToContents(0))
        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)

        for i in [10, 14, 15, 16, 17, 18, 19, 20]: self.tree.setColumnHidden(i, True)

        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.tree.itemChanged.connect(self._on_item_check_changed)

        self.splitter.addWidget(self.tree)
        self.splitter.setSizes([260, 1660]) 
        root_layout.addWidget(self.splitter)

        # ---- STATUS BAR ----
        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(26)
        self.setStatusBar(self.status_bar)

        self.sb_total    = QLabel("Total: 0")
        self.sb_complete = QLabel("Completed: 0")
        self.sb_running  = QLabel("Running: 0")
        self.sb_selected = QLabel("Selected: 0")
        self.sb_scan_time = QLabel("")

        for lbl in [self.sb_total, self.sb_complete, self.sb_running, self.sb_selected, self.sb_scan_time]:
            lbl.setContentsMargins(8, 0, 8, 0)
            self.status_bar.addPermanentWidget(lbl)
            self.status_bar.addPermanentWidget(self._vsep())

        self.apply_theme_and_spacing()

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _label(self, text): return QLabel(text)
    
    def _btn(self, text, slot):
        b = QPushButton(text)
        b.clicked.connect(slot)
        return b

    def _add_separator(self, layout):
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

    def _vsep(self):
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFrameShadow(QFrame.Sunken)
        sep.setFixedHeight(16)
        return sep

    def _set_all_blocks(self, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        self.blk_list.blockSignals(True)
        for i in range(self.blk_list.count()): self.blk_list.item(i).setCheckState(state)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    def toggle_window_size(self):
        if not self.is_compact:
            self.showNormal() 
            self.resize(1280, 720)
            self.is_compact = True
            self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarMaxButton))
        else:
            self.showNormal()
            self.resize(1920, 1000)
            self.is_compact = False
            self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarNormalButton))

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"),       self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"),       self, lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"),       self, lambda: self.tree.expandAll())
        QShortcut(QKeySequence("Ctrl+W"),       self, lambda: self.tree.collapseAll())
        QShortcut(QKeySequence("Ctrl+Shift+F"), self, self.fit_all_columns)

    # ------------------------------------------------------------------
    # STATUS-BAR & ERROR UI UPDATE
    # ------------------------------------------------------------------
    def _update_status_bar(self, runs):
        total = completed = running = 0
        for r in runs:
            if r.get("run_type") != "FE": continue
            total += 1
            if r["is_comp"]: completed += 1
            elif r["fe_status"] == "RUNNING": running += 1

        self.sb_total.setText(f"  Total: {total}")
        self.sb_complete.setText(f"  Completed: {completed}")
        self.sb_running.setText(f"  Running: {running}")
        self.sb_selected.setText(f"  Selected: {len(self._checked_paths)}")
        if self._last_scan_time: self.sb_scan_time.setText(f"  Last scan: {self._last_scan_time}  ")

    def _on_item_check_changed(self, item, col=0):
        if col != 0: return
        path = item.text(14)
        if not path or path == "N/A": return
        hl_color = QColor(self.custom_sel_color if self.use_custom_colors else ("#404652" if self.is_dark_mode else "#e3f2fd"))
        
        if item.checkState(0) == Qt.Checked:
            self._checked_paths.add(path)
            for c in range(self.tree.columnCount()): item.setBackground(c, hl_color)
        else:
            self._checked_paths.discard(path)
            for c in range(self.tree.columnCount()): item.setBackground(c, QColor(0, 0, 0, 0))
        self.sb_selected.setText(f"  Selected: {len(self._checked_paths)}")

    def on_tree_selection_changed(self):
        sel = self.tree.selectedItems()
        self.fe_error_btn.setVisible(False)
        self.current_error_log_path = None
        
        if len(sel) == 1:
            item = sel[0]
            is_stage = item.data(0, Qt.UserRole) == "STAGE"
            if not is_stage:
                run_path = item.text(14)
                if run_path and run_path != "N/A":
                    err_file = os.path.join(run_path, "logs", "compile_opt.error.log")
                    if os.path.exists(err_file):
                        count = 0
                        try:
                            with open(err_file, 'r', encoding='utf-8', errors='ignore') as f:
                                for line in f:
                                    if line.strip(): count += 1
                        except: pass
                        self.current_error_log_path = err_file
                        
                        color = "#e57373" if self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888") else "#d32f2f"
                        if count == 0: color = "#81c784" if self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888") else "#388e3c"
                        
                        self.fe_error_btn.setStyleSheet(f"QPushButton#errorLinkBtn {{ border: none; background: transparent; color: {color}; font-weight: bold; text-align: left; padding: 6px 0px; }} QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                        self.fe_error_btn.setText(f"compile_opt errors: {count}")
                        self.fe_error_btn.setVisible(True)

    def open_error_log(self):
        if self.current_error_log_path and os.path.exists(self.current_error_log_path):
            subprocess.Popen(['gvim', self.current_error_log_path])

    # ------------------------------------------------------------------
    # SETTINGS / THEME
    # ------------------------------------------------------------------
    def open_settings(self):
        dlg = SettingsDialog(self)
        if dlg.exec_():
            font = dlg.font_combo.currentFont()
            font.setPointSize(dlg.size_spin.value())
            QApplication.setFont(font)
            
            self.is_dark_mode        = dlg.theme_cb.isChecked()
            self.use_custom_colors   = dlg.use_custom_cb.isChecked()
            self.custom_bg_color     = dlg.bg_color
            self.custom_fg_color     = dlg.fg_color
            self.custom_sel_color    = dlg.sel_color
            
            self.row_spacing         = dlg.space_spin.value()
            self.show_relative_time  = dlg.rel_time_cb.isChecked()
            self.apply_theme_and_spacing()
            self.on_tree_selection_changed() 

    def apply_theme_and_spacing(self):
        pad = self.row_spacing
        
        if self.use_custom_colors:
            bg, fg, sel = self.custom_bg_color, self.custom_fg_color, self.custom_sel_color
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: {bg}; color: {fg}; }}
                QHeaderView::section {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px; font-weight: bold; }}
                QTreeWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; border: 1px solid {fg}; }}
                QListWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; border: 1px solid {fg}; font-weight: bold; }}
                QLineEdit, QSpinBox, QComboBox, QTextEdit {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 4px; border-radius: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {bg}; color: {fg}; selection-background-color: {sel}; selection-color: #ffffff; }}
                QPushButton, QToolButton {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px 12px; border-radius: 4px; font-weight: bold; }}
                QPushButton:hover, QToolButton:hover {{ border-color: {sel}; }}
                QPushButton:pressed, QToolButton:pressed {{ background-color: {sel}; color: #ffffff; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: {sel}; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QSplitter::handle {{ background-color: {sel}; }}
                QMenu {{ border: 1px solid {fg}; background-color: {bg}; color: {fg}; }}
                QMenu::item:selected {{ background-color: {sel}; color: #ffffff; }}
                QStatusBar {{ background: {bg}; color: {fg}; border-top: 1px solid {fg}; }}
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: {sel}; color: #ffffff; }}
            """
        elif self.is_dark_mode:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2d30; color: #dfe1e5; }}
                QTreeWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #393b40; border: 1px solid #393b40; }}
                QListWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #393b40; border: 1px solid #393b40; font-weight: bold; }}
                QHeaderView::section {{ background-color: #2b2d30; color: #a9b7c6; border: 1px solid #1e1f22; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 4px; }}
                QComboBox {{ background-color: #2b2d30; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 4px; }}
                QComboBox QAbstractItemView {{ background-color: #2b2d30; color: #dfe1e5; selection-background-color: #2f65ca; selection-color: #ffffff; }}
                QPushButton, QToolButton {{ background-color: #393b40; color: #dfe1e5; border: 1px solid #43454a; padding: 5px 12px; border-radius: 4px; }}
                QPushButton:hover, QToolButton:hover {{ background-color: #43454a; }}
                QPushButton:pressed, QToolButton:pressed {{ background-color: #2f65ca; color: #ffffff; border-color: #2f65ca; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: #64b5f6; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QSplitter::handle {{ background-color: #393b40; }}
                QMenu {{ border: 1px solid #43454a; background-color: #2b2d30; color: #dfe1e5; }}
                QMenu::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QStatusBar {{ background: #2b2d30; color: #808080; border-top: 1px solid #393b40; }}
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QTableWidget {{ background-color: #1e1f22; alternate-background-color: #26282b; gridline-color: #393b40; }}
            """
        else:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #f5f7fa; color: #333333; }}
                QHeaderView::section {{ background-color: #e4e7eb; color: #4a5568; border: 1px solid #cbd5e0; padding: 5px; font-weight: bold; }}
                QTreeWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; border: 1px solid #cbd5e0; }}
                QListWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; border: 1px solid #cbd5e0; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 4px; border-radius: 4px; }}
                QComboBox {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 4px; border-radius: 4px; }}
                QComboBox QAbstractItemView {{ background-color: #ffffff; color: #333333; selection-background-color: #3182ce; selection-color: #ffffff; }}
                QPushButton, QToolButton {{ background-color: #ffffff; color: #4a5568; border: 1px solid #cbd5e0; padding: 5px 12px; border-radius: 4px; font-weight: bold; }}
                QPushButton:hover, QToolButton:hover {{ background-color: #edf2f7; border-color: #a0aec0; }}
                QPushButton:pressed, QToolButton:pressed {{ background-color: #e2e8f0; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: #3182ce; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QSplitter::handle {{ background-color: #cbd5e0; }}
                QMenu {{ border: 1px solid #cbd5e0; background-color: #ffffff; color: #333333; }}
                QMenu::item:selected {{ background-color: #3182ce; color: #ffffff; }}
                QStatusBar {{ background: #e4e7eb; color: #4a5568; border-top: 1px solid #cbd5e0; }}
                QTreeView::item {{ padding: {pad}px; }}
                QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #3182ce; color: #ffffff; }}
                QTableWidget {{ background-color: #ffffff; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; }}
            """
        self.setStyleSheet(stylesheet)
        self.refresh_view()

    # ------------------------------------------------------------------
    # SCAN CONTROL
    # ------------------------------------------------------------------
    def on_auto_refresh_changed(self):
        val = self.auto_combo.currentText()
        if   val == "Off":    self.auto_refresh_timer.stop()
        elif val == "1 Min":  self.auto_refresh_timer.start(60_000)
        elif val == "5 Min":  self.auto_refresh_timer.start(300_000)
        elif val == "10 Min": self.auto_refresh_timer.start(600_000)

    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning(): return
        self.prog.setVisible(True)
        self.prog.setRange(0, 0)
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Scanning...")
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
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.on_source_changed()
        if not self._columns_fitted_once:
            self._columns_fitted_once = True
            self.fit_all_columns()

    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        releases, blocks = set(), set()
        if src_mode in ["WS",  "ALL"] and self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            releases.update(self.out_data.get("releases", {}).keys())
            blocks.update(self.out_data.get("blocks", set()))

        current_rtl = self.rel_combo.currentText()
        saved_states = {self.blk_list.item(i).text(): self.blk_list.item(i).checkState() for i in range(self.blk_list.count())}

        self.rel_combo.blockSignals(True); self.rel_combo.clear()
        valid = [r for r in releases if "Unknown" not in r and get_milestone(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(valid)
        self.rel_combo.addItems(new_releases)
        self.rel_combo.setCurrentText(current_rtl if current_rtl in new_releases else "[ SHOW ALL ]")
        self.rel_combo.blockSignals(False)

        self.blk_list.blockSignals(True); self.blk_list.clear()
        for b in sorted(blocks):
            it = QListWidgetItem(b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    # ------------------------------------------------------------------
    # TREE HELPERS
    # ------------------------------------------------------------------
    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i): self.tree.resizeColumnToContents(i)
        self.tree.setUpdatesEnabled(True)

    def calculate_all_sizes(self):
        size_tasks = []
        def gather(node):
            for i in range(node.childCount()):
                child = node.child(i)
                path = child.text(14) 
                if path and path != "N/A" and child.text(6) in ["-", "N/A", "Calc..."]: 
                    item_id = str(id(child))
                    self.item_map[item_id] = child
                    size_tasks.append((item_id, path))
                    child.setText(6, "Calc...")
                gather(child)
        gather(self.tree.invisibleRootItem())
        if size_tasks:
            worker = BatchSizeWorker(size_tasks)
            worker.size_calculated.connect(self.update_item_size)
            self.size_workers.append(worker)
            worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
            worker.start()

    def update_item_size(self, item_id, size_str):
        item = self.item_map.get(item_id)
        if item:
            item.setText(6, size_str)
            old = item.toolTip(0)
            if old: item.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {size_str}\n', old))

    def _get_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.childCount()):
            if parent.child(i).text(0) == text: return parent.child(i)
        p = CustomTreeItem(parent)
        p.setText(0, text)
        p.setData(0, Qt.UserRole, node_type)
        p.setExpanded(True)
        if node_type == "MILESTONE":
            p.setForeground(0, QColor("#1e88e5" if not self.is_dark_mode else "#64b5f6"))
            f = p.font(0); f.setBold(True); p.setFont(0, f)
        elif node_type == "RTL":
            f = p.font(0); f.setItalic(True); p.setFont(0, f)
        return p

    def _get_item_path_id(self, item):
        parts = []
        while item is not None:
            parts.insert(0, item.text(0).strip())
            item = item.parent()
        return "|".join(parts)

    # ------------------------------------------------------------------
    # STATUS BADGE TEXT
    # ------------------------------------------------------------------
    def _apply_status_color(self, item, col, status):
        if status == "COMPLETED": item.setForeground(col, QColor("#1b5e20" if not self.is_dark_mode else "#81c784"))
        elif status == "RUNNING": item.setForeground(col, QColor("#0d47a1" if not self.is_dark_mode else "#64b5f6"))
        elif status == "NOT STARTED": item.setForeground(col, QColor("#757575" if not self.is_dark_mode else "#9e9e9e"))
        elif status == "INTERRUPTED": item.setForeground(col, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        elif status in ("FAILED", "FATAL ERROR", "ERROR"): item.setForeground(col, QColor("#b71c1c" if not self.is_dark_mode else "#e57373"))

    def _apply_fm_color(self, item, col, val):
        if "FAILS" in val: item.setForeground(col, QColor("#d32f2f" if not self.is_dark_mode else "#e57373"))
        elif "PASS" in val: item.setForeground(col, QColor("#388e3c" if not self.is_dark_mode else "#81c784"))

    def _apply_vslp_color(self, item, col, val):
        if "Error" in val and "Error: 0" not in val: item.setForeground(col, QColor("#d32f2f" if not self.is_dark_mode else "#e57373"))
        elif "Error: 0" in val: item.setForeground(col, QColor("#388e3c" if not self.is_dark_mode else "#81c784"))

    # ------------------------------------------------------------------
    # CREATE RUN ITEM
    # ------------------------------------------------------------------
    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        child.setCheckState(0, Qt.Unchecked)

        child.setText(0, run["r_name"])
        child.setText(1, run["rtl"])
        child.setText(2, run["source"])
        child.setText(5, run.get("owner", "Unknown")) 

        if run["path"] in self._checked_paths:
            child.setCheckState(0, Qt.Checked)
            hl_color = QColor(self.custom_sel_color if self.use_custom_colors else ("#404652" if self.is_dark_mode else "#e3f2fd"))
            for c in range(self.tree.columnCount()): child.setBackground(c, hl_color)

        is_ir_block = (run["block"].upper() == PROJECT_PREFIX.upper())
        ir_text = "Check individual stage levels" if is_ir_block else "N/A"
        tooltip_text = (f"Owner: {run.get('owner','Unknown')}\nSize: Pending\nRuntime: {run['info']['runtime']}\nNONUPF: {run['st_n']}\nUPF: {run['st_u']}\nVSLP: {run['vslp_status']}\n")
        if is_ir_block: tooltip_text += f"Static IR: {ir_text}"
        
        child.setToolTip(0, tooltip_text)
        child.setExpanded(False)

        if run["source"] == "OUTFEED": child.setForeground(2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))
        else: child.setForeground(2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        if run["run_type"] == "FE":
            status_str = run["fe_status"]
            child.setText(3, status_str)
            child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(6, "-") 
            child.setText(7, f"NONUPF - {run['st_n']}")
            child.setText(8, f"UPF - {run['st_u']}")
            child.setText(9, run["vslp_status"])
            child.setText(10, "-") 
            child.setText(11, run["info"]["runtime"])

            start_raw = run["info"]["start"]
            end_raw   = run["info"]["end"]
            if self.show_relative_time:
                child.setText(12, relative_time(start_raw))
                child.setText(13, relative_time(end_raw) if run["is_comp"] else "-")
            else:
                child.setText(12, start_raw)
                child.setText(13, end_raw)
            child.setToolTip(12, start_raw); child.setToolTip(13, end_raw)

            child.setText(14, run["path"])
            child.setText(15, os.path.join(run["path"], "logs/compile_opt.log"))
            child.setText(16, run["fm_u_path"])
            child.setText(17, run["fm_n_path"])
            child.setText(18, run["vslp_rpt_path"])
            child.setText(19, "")
            child.setText(20, "")

            self._apply_status_color(child, 3, status_str)
            self._apply_fm_color(child, 7, run["st_n"])
            self._apply_fm_color(child, 8, run["st_u"])
            self._apply_vslp_color(child, 9, run["vslp_status"])
        else:
            child.setText(6, "-"); child.setText(10, "-")
            child.setText(14, run["path"]); child.setText(20, "")

        for i in range(1, 21): child.setTextAlignment(i, Qt.AlignCenter)
        child.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
        return child

    # ------------------------------------------------------------------
    # ADD STAGES (BE)
    # ------------------------------------------------------------------
    def _add_stages(self, be_item, be_run):
        owner = be_run.get("owner", "Unknown")
        is_ir_block = (be_run["block"].upper() == PROJECT_PREFIX.upper())
        
        for stage in be_run["stages"]:
            st_item = CustomTreeItem(be_item)
            st_item.setFlags(st_item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            st_item.setCheckState(0, Qt.Unchecked)
            st_item.setData(0, Qt.UserRole, "STAGE")
            st_item.setData(1, Qt.UserRole, stage["name"])
            st_item.setData(2, Qt.UserRole, stage["qor_path"])

            ir_key  = f"{be_run['r_name']}/{stage['name']}"
            ir_info = self.ir_data.get(ir_key, {"log": "", "line": "N/A", "value": "-"})

            tooltip_text = (f"Owner: {owner}\nSize: Pending\nRuntime: {stage['info']['runtime']}\nNONUPF: {stage['st_n']}\nUPF: {stage['st_u']}\nVSLP: {stage['vslp_status']}\n")
            if is_ir_block: tooltip_text += f"Static IR: {ir_info['line']}"
            st_item.setToolTip(0, tooltip_text)

            stage_status = "COMPLETED"
            if be_run["source"] == "WS" and not cached_exists(stage["rpt"]):
                stage_status = "RUNNING"
                if cached_exists(stage["log"]):
                    try:
                        with open(stage["log"], 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "START_CMD:" in line: stage_status = line.strip(); break
                    except: pass

            st_item.setText(0, stage["name"])
            st_item.setText(2, be_run["source"])
            st_item.setText(4, stage_status)
            st_item.setText(5, owner) 
            st_item.setText(6, "-") 
            st_item.setText(7, f"NONUPF - {stage['st_n']}")
            st_item.setText(8, f"UPF - {stage['st_u']}")
            st_item.setText(9, stage["vslp_status"])
            st_item.setText(10, ir_info["value"] if is_ir_block else "-")
            st_item.setText(11, stage["info"]["runtime"])

            s_start = stage["info"]["start"]
            s_end   = stage["info"]["end"]
            if self.show_relative_time:
                st_item.setText(12, relative_time(s_start))
                st_item.setText(13, relative_time(s_end))
            else:
                st_item.setText(12, s_start)
                st_item.setText(13, s_end)
            st_item.setToolTip(12, s_start); st_item.setToolTip(13, s_end)

            st_item.setText(14, stage["stage_path"])
            st_item.setText(15, stage["log"])
            st_item.setText(16, stage["fm_u_path"])
            st_item.setText(17, stage["fm_n_path"])
            st_item.setText(18, stage["vslp_rpt_path"])
            st_item.setText(19, stage["sta_rpt_path"])
            st_item.setText(20, ir_info["log"])

            self._apply_fm_color(st_item, 7, stage["st_n"])
            self._apply_fm_color(st_item, 8, stage["st_u"])
            self._apply_vslp_color(st_item, 9, stage["vslp_status"])
            self._apply_status_color(st_item, 4, stage_status if stage_status in ("COMPLETED","RUNNING","FAILED") else "RUNNING")

            for i in range(1, 21): st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)

    # ------------------------------------------------------------------
    # BLOCK STATUS DOT (sidebar)
    # ------------------------------------------------------------------
    def _block_aggregate_status(self, block_name, runs):
        for r in runs:
            if r["block"] == block_name and r["run_type"] == "FE":
                if not r["is_comp"]: return "running"
        return "done"

    def _refresh_block_colors(self, runs):
        for i in range(self.blk_list.count()):
            it = self.blk_list.item(i)
            blk = it.text()
            status = self._block_aggregate_status(blk, runs)
            if status == "running": it.setForeground(QColor("#0277bd" if not self.is_dark_mode else "#64b5f6"))
            else: it.setForeground(QColor("#2e7d32" if not self.is_dark_mode else "#81c784"))

    # ------------------------------------------------------------------
    # VIEW PRESET FILTER
    # ------------------------------------------------------------------
    def _apply_view_preset(self, run):
        preset = self.view_combo.currentText()
        if preset == "All Runs":       return True
        if preset == "FE Only":        return run["run_type"] == "FE"
        if preset == "BE Only":        return run["run_type"] == "BE"
        if preset == "Running Only":   return run["run_type"] == "FE" and not run["is_comp"]
        if preset == "Failed Only":
            return ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","") or run.get("fe_status") == "FATAL ERROR")
        if preset == "Today's Runs":
            start = run["info"].get("start","")
            return relative_time(start).endswith("ago") and ("h ago" in relative_time(start) or "m ago" in relative_time(start))
        return True

    # ------------------------------------------------------------------
    # MAIN REFRESH
    # ------------------------------------------------------------------
    def refresh_view(self):
        for w in self.size_workers:
            if hasattr(w, 'cancel'): w.cancel()
        self.item_map.clear()

        expanded_states = {}
        def save_state(node):
            for i in range(node.childCount()):
                child = node.child(i)
                expanded_states[self._get_item_path_id(child)] = child.isExpanded()
                save_state(child)
        save_state(self.tree.invisibleRootItem())

        self.tree.setUpdatesEnabled(False)
        self.tree.setSortingEnabled(False)
        self.tree.clear()

        if self.rel_combo.currentText() == "[ SHOW ALL ]": self.tree.setColumnHidden(1, False) 
        else: self.tree.setColumnHidden(1, True) 

        src_mode = self.src_combo.currentText()
        sel_rtl  = self.rel_combo.currentText()

        raw_query = self.search.text().lower().strip()
        search_pattern = "*" if not raw_query else (f"*{raw_query}*" if '*' not in raw_query else raw_query)

        checked_blks = [self.blk_list.item(i).text() for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]

        runs_to_process = []
        if src_mode in ["WS",  "ALL"] and self.ws_data: runs_to_process.extend(self.ws_data.get("all_runs", []))
        if src_mode in ["OUTFEED","ALL"] and self.out_data: runs_to_process.extend(self.out_data.get("all_runs", []))

        ignored_runs_list, normal_runs_list = [], []

        for run in runs_to_process:
            if run["path"] in self.ignored_paths:
                ignored_runs_list.append(run); continue
            if run["block"] not in checked_blks: continue
            base_rtl_filter = re.sub(r'_syn\d+$', '', run["rtl"])
            if get_milestone(base_rtl_filter) is None: continue
            if sel_rtl != "[ SHOW ALL ]":
                if not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")): continue
            if not self._apply_view_preset(run): continue
            if search_pattern != "*":
                combined = (f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} {run['st_n']} {run['st_u']} {run['vslp_status']} {run['info']['runtime']} {run['info']['start']} {run['info']['end']}").lower()
                matches = fnmatch.fnmatch(combined, search_pattern)
                if not matches and run["run_type"] == "BE":
                    for stage in run["stages"]:
                        sc = f"{stage['name']} {stage['st_n']} {stage['st_u']} {stage['vslp_status']} {stage['info']['runtime']}".lower()
                        if fnmatch.fnmatch(sc, search_pattern): matches = True; break
                if not matches: continue
            normal_runs_list.append(run)

        fe_runs = [r for r in normal_runs_list if r["run_type"] == "FE"]
        be_runs = [r for r in normal_runs_list if r["run_type"] == "BE"]
        
        root = self.tree.invisibleRootItem()

        # Place FE runs
        for fe_run in fe_runs:
            blk_name = fe_run["block"]
            run_rtl  = fe_run["rtl"]
            base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
            has_syn  = (run_rtl != base_rtl)
            block_node = self._get_node(root, blk_name, "BLOCK")

            if sel_rtl == "[ SHOW ALL ]":
                milestone  = get_milestone(base_rtl)
                m_node     = self._get_node(block_node, milestone, "MILESTONE")
                rtl_node   = self._get_node(m_node, base_rtl,  "RTL")
                parent_for_run = rtl_node
            elif sel_rtl == base_rtl and has_syn: parent_for_run = self._get_node(block_node, run_rtl, "RTL")
            else: parent_for_run = block_node

            self._create_run_item(parent_for_run, fe_run)

        # Place BE runs (Hierarchical matching to FE without "Other PNR runs")
        for be_run in be_runs:
            blk_name = be_run["block"]
            run_rtl  = be_run["rtl"]
            base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
            has_syn  = (run_rtl != base_rtl)
            block_node = self._get_node(root, blk_name, "BLOCK")

            if sel_rtl == "[ SHOW ALL ]":
                milestone  = get_milestone(base_rtl)
                m_node     = self._get_node(block_node, milestone, "MILESTONE")
                parent_for_run   = self._get_node(m_node, base_rtl,  "RTL")
            elif sel_rtl == base_rtl and has_syn: parent_for_run = self._get_node(block_node, run_rtl, "RTL")
            else: parent_for_run = block_node

            # Nest inside matching FE run
            fe_parent = None
            for i in range(parent_for_run.childCount()):
                c = parent_for_run.child(i)
                fe_base = c.text(0).replace("-FE", "")
                if c.data(0, Qt.UserRole) != "STAGE" and (f"_{fe_base}_" in be_run["r_name"] or be_run["r_name"].startswith(f"{fe_base}_")):
                    fe_parent = c
                    break

            actual_parent = fe_parent if fe_parent else parent_for_run
            be_item = self._create_run_item(actual_parent, be_run)
            self._add_stages(be_item, be_run)

        if ignored_runs_list:
            ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
            for run in ignored_runs_list:
                blk_name = run["block"]
                run_rtl  = run["rtl"]
                base_rtl = re.sub(r'_syn\d+$', '', run_rtl)
                has_syn  = (run_rtl != base_rtl)
                block_node = self._get_node(ign_root, blk_name, "BLOCK")
                if sel_rtl == "[ SHOW ALL ]":
                    milestone  = get_milestone(base_rtl)
                    m_node     = self._get_node(block_node, milestone, "MILESTONE")
                    parent_for_run = self._get_node(m_node, base_rtl, "RTL")
                elif sel_rtl == base_rtl and has_syn: parent_for_run = self._get_node(block_node, run_rtl, "RTL")
                else: parent_for_run = block_node
                
                item = self._create_run_item(parent_for_run, run)
                if run["run_type"] == "BE": self._add_stages(item, run)

        self.tree.setSortingEnabled(True)

        def restore_state(node):
            for i in range(node.childCount()):
                child     = node.child(i)
                path_key  = self._get_item_path_id(child)
                node_type = child.data(0, Qt.UserRole)
                is_run    = bool(child.text(14)) 
                if path_key in expanded_states: child.setExpanded(expanded_states[path_key])
                else:
                    if child.parent() is None: child.setExpanded(True)
                    elif node_type == "MILESTONE": child.setExpanded(False)
                    elif sel_rtl == "[ SHOW ALL ]" and node_type == "RTL": child.setExpanded(False) 
                    elif is_run: child.setExpanded(False)
                    else: child.setExpanded(True)
                restore_state(child)
        restore_state(root)

        self.tree.resizeColumnToContents(0)
        self.tree.setUpdatesEnabled(True)

        all_runs = list(runs_to_process)
        self._refresh_block_colors(all_runs)
        self._update_status_bar(normal_runs_list)
        
        self.on_tree_selection_changed()

    # ------------------------------------------------------------------
    # CONTEXT MENUS
    # ------------------------------------------------------------------
    def on_header_context_menu(self, pos):
        menu = QMenu(self)
        for i in range(1, 21):
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
            copy_cell_act = m.addAction("Copy Cell Text")
            m.addSeparator()

        run_path, log_path = item.text(14), item.text(15)
        fm_u_path, fm_n_path = item.text(16), item.text(17)
        vslp_path, sta_path, ir_path = item.text(18), item.text(19), item.text(20)
        is_stage = item.data(0, Qt.UserRole) == "STAGE"

        ignore_checked_act = m.addAction("Ignore All Checked Runs")
        m.addSeparator()
        ignore_act = restore_act = None
        if run_path and run_path != "N/A" and not is_stage:
            if run_path in self.ignored_paths: restore_act = m.addAction("Restore Current Run (Un-ignore)")
            else: ignore_act = m.addAction("Ignore Current Run")
            m.addSeparator()

        calc_size_act = None
        if run_path and run_path != "N/A" and cached_exists(run_path):
            calc_size_act = m.addAction("Calculate Folder Size")
            m.addSeparator()

        fm_n_act = m.addAction("Open NONUPF Formality Report") if fm_n_path and fm_n_path != "N/A" and cached_exists(fm_n_path) else None
        fm_u_act = m.addAction("Open UPF Formality Report")   if fm_u_path and fm_u_path != "N/A" and cached_exists(fm_u_path) else None
        v_act    = m.addAction("Open VSLP Report")            if vslp_path and vslp_path != "N/A" and cached_exists(vslp_path) else None
        sta_act  = m.addAction("Open PT STA Summary")         if sta_path  and sta_path  != "N/A" and cached_exists(sta_path)  else None
        ir_act   = m.addAction("Open Static IR Log")          if ir_path   and ir_path   != "N/A" and cached_exists(ir_path)   else None
        log_act  = m.addAction("Open Log File")               if log_path  and log_path  != "N/A" and cached_exists(log_path)  else None
        m.addSeparator()
        c_act = m.addAction("Copy Path")
        qor_act = None
        if is_stage:
            m.addSeparator()
            qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res: return

        if res == ignore_checked_act:
            def ig(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                        p = c.text(14)
                        if p and p != "N/A": self.ignored_paths.add(p)
                    ig(c)
            ig(self.tree.invisibleRootItem()); self.refresh_view()
        elif res == ignore_act: self.ignored_paths.add(run_path); self.refresh_view()
        elif res == restore_act: self.ignored_paths.discard(run_path); self.refresh_view()
        elif copy_cell_act and res == copy_cell_act: QApplication.clipboard().setText(cell_text)
        elif calc_size_act and res == calc_size_act:
            item.setText(6, "Calc...") 
            worker = SingleSizeWorker(item, run_path)
            worker.result.connect(lambda it, sz: (it.setText(6, sz), it.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {sz}\n', it.toolTip(0) if it.toolTip(0) else ""))))
            if not hasattr(self, 'size_workers'): self.size_workers = []
            self.size_workers.append(worker)
            worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
            worker.start()
        elif fm_n_act and res == fm_n_act: subprocess.Popen(['gvim', fm_n_path])
        elif fm_u_act and res == fm_u_act: subprocess.Popen(['gvim', fm_u_path])
        elif v_act    and res == v_act:    subprocess.Popen(['gvim', vslp_path])
        elif sta_act  and res == sta_act:  subprocess.Popen(['gvim', sta_path])
        elif ir_act   and res == ir_act:   subprocess.Popen(['gvim', ir_path])
        elif log_act  and res == log_act:  subprocess.Popen(['gvim', log_path])
        elif res == c_act: QApplication.clipboard().setText(run_path)
        elif qor_act and res == qor_act:
            step_name = item.data(1, Qt.UserRole)
            qor_path  = item.data(2, Qt.UserRole)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
            if h: subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])

    def on_item_double_clicked(self, item, col):
        if item.parent():
            log = item.text(15)
            if log and cached_exists(log): subprocess.Popen(['gvim', log])

    # ------------------------------------------------------------------
    # MAIL FEATURE
    # ------------------------------------------------------------------
    def send_mail_action(self):
        if not self._checked_paths:
            QMessageBox.warning(self, "No Runs Selected", "Please select at least one run using the checkboxes before sending mail.")
            return

        user_runs = {} 
        is_fe_selected = False
        
        def find_owners(node):
            nonlocal is_fe_selected
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    path = c.text(14)
                    owner = c.text(5)
                    run_name = c.text(0)
                    
                    if "FE" in run_name: 
                        is_fe_selected = True

                    if path and path != "N/A" and owner and owner != "Unknown":
                        if owner not in user_runs: user_runs[owner] = []
                        user_runs[owner].append(path)
                find_owners(c)
                
        find_owners(self.tree.invisibleRootItem())

        if not user_runs:
            QMessageBox.warning(self, "Unknown Users", "Could not determine the owners for the selected runs.")
            return

        default_subject = "Please remove your old PI runs" if is_fe_selected else "Please remove your old PD runs"

        dlg = MailDialog(default_subject, self)
        if dlg.exec_():
            subject = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()

            current_user = getpass.getuser()
            sender_email = get_user_email(current_user)
            if not sender_email: sender_email = f"{current_user}@samsung.com"

            success_count = 0
            for owner, paths in user_runs.items():
                owner_email = get_user_email(owner)
                if not owner_email: continue
                
                final_body = body_template + "\n\n" + "\n".join(paths)
                
                cmd = [
                    MAIL_UTIL,
                    "-to", owner_email,
                    "-sd", sender_email,
                    "-s", subject,
                    "-c", final_body,
                    "-fm", "text" 
                ]
                try:
                    subprocess.Popen(cmd)
                    success_count += 1
                except Exception as e:
                    print(f"Failed to send mail to {owner_email}: {e}")

            QMessageBox.information(self, "Mail Operation Complete", f"Successfully triggered {success_count} emails to individual owners.")

    # ------------------------------------------------------------------
    # DISK USAGE FEATURE
    # ------------------------------------------------------------------
    def open_disk_usage(self):
        if hasattr(self, 'disk_worker') and self.disk_worker.isRunning():
            return
            
        self.disk_btn.setEnabled(False)
        self.disk_btn.setText("Scanning...")
        
        self.disk_worker = DiskScannerWorker()
        self.disk_worker.finished_scan.connect(self._on_disk_scan_finished)
        self.disk_worker.start()

    def _on_disk_scan_finished(self, results):
        self.disk_btn.setEnabled(True)
        self.disk_btn.setText("Disk Space")
        
        dlg = DiskUsageDialog(results, self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"), self)
        dlg.exec_()

    # ------------------------------------------------------------------
    # QoR COMPARISON
    # ------------------------------------------------------------------
    def run_qor_comparison(self):
        sel  = []
        root = self.tree.invisibleRootItem()
        def get_checked(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    qp  = c.text(14)
                    src = c.text(2)
                    if src == "OUTFEED" and qp: qp = os.path.dirname(qp)
                    if qp and not qp.endswith("/"): qp += "/"
                    sel.append(qp)
                get_checked(c)
        get_checked(root)
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
        if h: subprocess.Popen([FIREFOX_PATH, sorted(h, key=os.path.getmtime)[-1]])

# ===========================================================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")          
    w = PDDashboard()
    w.show()
    sys.exit(app.exec_())
