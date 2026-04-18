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
import shutil
import math
import threading
import configparser
import json

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QTreeWidget, QTreeWidgetItem,
    QPushButton, QMessageBox, QListWidget, QListWidgetItem,
    QProgressBar, QMenu, QSplitter, QFontComboBox, QSpinBox,
    QWidgetAction, QCheckBox, QDialog, QFormLayout, QDialogButtonBox,
    QStatusBar, QFrame, QShortcut, QAction, QToolButton, QStyle, QColorDialog,
    QTextEdit, QTableWidget, QTableWidgetItem, QHeaderView, QProgressDialog,
    QFileDialog, QCompleter, QGroupBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QDateTime, QRectF, QStringListModel
from PyQt5.QtGui import QColor, QFont, QClipboard, QKeySequence, QPalette, QBrush, QPainter, QPen

# ===========================================================================
# --- CONFIGURATION INITIALIZATION ---
# ===========================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "project_config.ini")
MAIL_USERS_FILE = os.path.join(SCRIPT_DIR, "mail_users.ini")
NOTES_DIR = os.path.join(SCRIPT_DIR, "dashboard_notes")

if not os.path.exists(NOTES_DIR):
    try: os.makedirs(NOTES_DIR)
    except: pass

config = configparser.ConfigParser()
DEFAULT_CONFIG = {
    'PROJECT': {
        'PROJECT_PREFIX': 'S5K2P5SP',
        'BASE_WS_FE_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/WS',
        'BASE_WS_BE_DIR': '/user/s5k2p5sp.be1/s5k2p5sp/WS',
        'BASE_OUTFEED_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/outfeed',
        'BASE_IR_DIR': '/user/s5k2p5sx.be1/LAYOUT/IR/ /user/s5k2p5sx.be1/LAYOUT/IR2/'
    },
    'TOOLS': {
        'PNR_TOOL_NAMES': 'fc innovus',
        'SUMMARY_SCRIPT': '/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py',
        'FIREFOX_PATH': '/usr/bin/firefox',
        'MAIL_UTIL': '/user/vwpmailsystem/MAIL/send_mail_for_rhel7',
        'USER_INFO_UTIL': '/usr/local/bin/user_info'
    }
}

if not os.path.exists(CONFIG_FILE):
    config.read_dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_FILE, 'w') as f:
            config.write(f)
    except: pass
else:
    config.read(CONFIG_FILE)

# Map Global Variables
PROJECT_PREFIX   = config.get('PROJECT', 'PROJECT_PREFIX', fallback='S5K2P5SP')
BASE_WS_FE_DIR   = config.get('PROJECT', 'BASE_WS_FE_DIR', fallback='')
BASE_WS_BE_DIR   = config.get('PROJECT', 'BASE_WS_BE_DIR', fallback='')
BASE_OUTFEED_DIR = config.get('PROJECT', 'BASE_OUTFEED_DIR', fallback='')
BASE_IR_DIR      = config.get('PROJECT', 'BASE_IR_DIR', fallback='')

PNR_TOOL_NAMES   = config.get('TOOLS', 'PNR_TOOL_NAMES', fallback='fc innovus')
SUMMARY_SCRIPT   = config.get('TOOLS', 'SUMMARY_SCRIPT', fallback='')
FIREFOX_PATH     = config.get('TOOLS', 'FIREFOX_PATH', fallback='/usr/bin/firefox')
MAIL_UTIL        = config.get('TOOLS', 'MAIL_UTIL', fallback='')
USER_INFO_UTIL   = config.get('TOOLS', 'USER_INFO_UTIL', fallback='')

# Load Mail Config
mail_config = configparser.ConfigParser()
DEFAULT_MAIL_CONFIG = {
    'PERMANENT_MEMBERS': {
        'always_to': '',
        'always_cc': 'mohit.bhar'
    },
    'KNOWN_USERS': {
        'users': ''
    }
}
if not os.path.exists(MAIL_USERS_FILE):
    mail_config.read_dict(DEFAULT_MAIL_CONFIG)
    try:
        with open(MAIL_USERS_FILE, 'w') as f:
            mail_config.write(f)
    except: pass
else:
    mail_config.read(MAIL_USERS_FILE)

# ===========================================================================
# --- THREAD-SAFE CACHE ---
# ===========================================================================
_path_cache = {}
_path_cache_lock = threading.Lock()

def cached_exists(path):
    with _path_cache_lock:
        if path in _path_cache: return _path_cache[path]
    result = os.path.exists(path)
    with _path_cache_lock: _path_cache[path] = result
    return result

def clear_path_cache():
    with _path_cache_lock: _path_cache.clear()

def prefetch_path_cache(paths):
    unique_paths = [p for p in set(paths) if p]
    if not unique_paths: return
    max_w = min(30, len(unique_paths))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
        results = list(ex.map(os.path.exists, unique_paths))
    with _path_cache_lock:
        for path, exists in zip(unique_paths, results):
            _path_cache[path] = exists

# ===========================================================================
# --- HELPERS ---
# ===========================================================================
def convert_kst_to_ist_str(time_str):
    if not time_str or time_str == "N/A": return time_str
    formats = ["%a %b %d, %Y - %H:%M:%S", "%b %d, %Y - %H:%M", "%b %d, %Y - %H:%M:%S"]
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(time_str, fmt)
            dt_ist = dt - datetime.timedelta(hours=3, minutes=30)
            return dt_ist.strftime(fmt)
        except ValueError: continue
    return time_str

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
        return f"{total_seconds // 86400}d ago"
    except: return date_str

def get_user_email(username):
    username = username.strip()
    if not username or username == "Unknown": return ""
    if "@" in username: return username
    try:
        res = subprocess.check_output([USER_INFO_UTIL, '-a', username], stderr=subprocess.DEVNULL).decode('utf-8')
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', res)
        if match: return match.group(0)
    except: pass
    return f"{username}@samsung.com"

def get_partition_space(path_str):
    try:
        total, used, free = shutil.disk_usage(path_str)
        t_gb = total / (1024**3); u_gb = used / (1024**3); f_gb = free / (1024**3)
        perc = (used / total) * 100 if total > 0 else 0
        return f"Total: {t_gb:.1f} GB | Used: {u_gb:.1f} GB ({perc:.1f}%) | Free: {f_gb:.1f} GB"
    except: return "Partition Space Information Unavailable"

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
    if match: return f"{match.group(1)} {match.group(2)} {match.group(3)}, {match.group(5)} - {match.group(4)}"
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
                        final_time_str = f"{days*24+hours:02}h:{mins:02}m:{secs:02}s"
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

def find_latest_qor_report():
    h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
    if h: return sorted(h, key=os.path.getmtime)[-1]
    return None

def save_mail_users_config(new_users):
    try:
        mail_config.read(MAIL_USERS_FILE)
        existing_str = mail_config.get('KNOWN_USERS', 'users', fallback='')
        existing = set([u.strip() for u in existing_str.split(',') if u.strip()])
        existing.update(new_users)
        if not mail_config.has_section('KNOWN_USERS'): mail_config.add_section('KNOWN_USERS')
        mail_config.set('KNOWN_USERS', 'users', ', '.join(sorted(existing)))
        with open(MAIL_USERS_FILE, 'w') as f:
            mail_config.write(f)
    except: pass

def get_all_known_mail_users():
    try:
        mail_config.read(MAIL_USERS_FILE)
        existing_str = mail_config.get('KNOWN_USERS', 'users', fallback='')
        return sorted(list(set([u.strip() for u in existing_str.split(',') if u.strip()])))
    except: return []

def load_all_notes():
    global_notes = {}
    if not os.path.exists(NOTES_DIR): return global_notes
    for file in os.listdir(NOTES_DIR):
        if file.endswith(".json"):
            try:
                with open(os.path.join(NOTES_DIR, file), 'r') as f:
                    data = json.load(f)
                    for key, val in data.items():
                        if key not in global_notes: global_notes[key] = []
                        global_notes[key].append(val)
            except: pass
    return global_notes

def save_user_note(identifier, note_text):
    current_user = getpass.getuser()
    user_file = os.path.join(NOTES_DIR, f"notes_{current_user}.json")
    user_data = {}
    if os.path.exists(user_file):
        try:
            with open(user_file, 'r') as f:
                user_data = json.load(f)
        except: pass
    
    if note_text.strip():
        user_data[identifier] = f"[{current_user}] {note_text.strip()}"
    else:
        if identifier in user_data: del user_data[identifier]
        
    try:
        with open(user_file, 'w') as f:
            json.dump(user_data, f, indent=4)
    except Exception as e:
        print(f"Failed to save note: {e}")

# ===========================================================================
# --- CUSTOM SORTING UI CLASS ---
# ===========================================================================
class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""

        if col in [3, 7, 8, 9]:
            def score(val):
                v_up = val.upper()
                if "PASS" in v_up or "ERROR: 0" in v_up or "COMPLETED" in v_up: return 4
                if "RUNNING" in v_up: return 3
                if "FAILS" in v_up or "ERROR:" in v_up or "FATAL" in v_up: return 2
                if "INTERRUPTED" in v_up or "NOT STARTED" in v_up: return 1
                return 0
            s1, s2 = score(t1), score(t2)
            if s1 != s2:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return s1 < s2 if asc else s1 > s2

        if col == 0:
            if t1 == "[ Ignored Runs ]": return False
            if t2 == "[ Ignored Runs ]": return True
            m_order = {"INITIAL RELEASE": 1, "PRE-SVP": 2, "SVP": 3, "FFN": 4}
            if t1 in m_order and t2 in m_order:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return m_order[t1] < m_order[t2] if asc else m_order[t1] > m_order[t2]
        return t1 < t2

# ===========================================================================
# --- UI DIALOGS ---
# ===========================================================================
class MultiCompleterLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.completer = QCompleter()
        self.completer.setWidget(self)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.activated.connect(self.insertCompletion)
        self.words = []

    def setModel(self, string_list):
        self.words = string_list
        model = QStringListModel(self.words, self.completer)
        self.completer.setModel(model)

    def insertCompletion(self, completion):
        text = self.text()
        parts = text.split(',')
        if len(parts) > 1: text = ','.join(parts[:-1]) + ', ' + completion + ', '
        else: text = completion + ', '
        self.setText(text)

    def keyPressEvent(self, e):
        if self.completer.popup().isVisible():
            if e.key() in (Qt.Key_Enter, Qt.Key_Return):
                e.ignore()
                return
        super().keyPressEvent(e)
        cr = self.cursorRect()
        cr.setWidth(self.completer.popup().sizeHintForColumn(0) + self.completer.popup().verticalScrollBar().sizeHint().width())
        
        text = self.text()
        current_word = text.split(',')[-1].strip()
        
        if current_word:
            self.completer.setCompletionPrefix(current_word)
            if self.completer.completionCount() > 0:
                self.completer.complete(cr)
            else:
                self.completer.popup().hide()
        else:
            self.completer.popup().hide()


class AdvancedMailDialog(QDialog):
    def __init__(self, default_subject, default_body, all_users, prefill_to="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send EMail Message")
        self.resize(700, 550)
        self.attachments = []
        
        layout = QVBoxLayout(self)
        
        form_layout = QFormLayout()
        self.to_input = MultiCompleterLineEdit()
        self.to_input.setModel(all_users)
        
        self.cc_input = MultiCompleterLineEdit()
        self.cc_input.setModel(all_users)
        
        try:
            always_to = mail_config.get('PERMANENT_MEMBERS', 'always_to', fallback='').strip()
            always_cc = mail_config.get('PERMANENT_MEMBERS', 'always_cc', fallback='').strip()
            
            final_to = always_to
            if prefill_to:
                final_to = always_to + (", " if always_to else "") + prefill_to
                
            if final_to: self.to_input.setText(final_to + (', ' if not final_to.endswith(',') else ' '))
            if always_cc: self.cc_input.setText(always_cc + (', ' if not always_cc.endswith(',') else ' '))
        except: pass

        self.subject_input = QLineEdit(default_subject)
        
        form_layout.addRow("<b>To:</b>", self.to_input)
        form_layout.addRow("<b>CC:</b>", self.cc_input)
        form_layout.addRow("<b>Subject:</b>", self.subject_input)
        layout.addLayout(form_layout)

        # Attachments Section
        attach_layout = QHBoxLayout()
        attach_layout.addWidget(QLabel("<b>Attachments:</b>"))
        self.attach_lbl = QLabel("None")
        self.attach_lbl.setStyleSheet("color: #1976D2;")
        attach_layout.addWidget(self.attach_lbl)
        attach_layout.addStretch()
        
        btn_qor = QPushButton("Attach Latest QoR Report")
        btn_qor.clicked.connect(self.attach_qor)
        btn_browse = QPushButton("Browse Files...")
        btn_browse.clicked.connect(self.browse_files)
        attach_layout.addWidget(btn_qor)
        attach_layout.addWidget(btn_browse)
        layout.addLayout(attach_layout)

        self.body_input = QTextEdit()
        self.body_input.setPlainText(default_body)
        layout.addWidget(self.body_input)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Send Mail")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def update_attach_lbl(self):
        if not self.attachments: self.attach_lbl.setText("None")
        else: self.attach_lbl.setText(f"{len(self.attachments)} file(s) attached: " + ", ".join([os.path.basename(p) for p in self.attachments]))

    def attach_qor(self):
        report = find_latest_qor_report()
        if report:
            if report not in self.attachments:
                self.attachments.append(report)
                self.update_attach_lbl()
        else:
            QMessageBox.warning(self, "Not Found", "No QoR summary.html found in the current environment.")

    def browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Attachments", "", "All Files (*)")
        if files:
            for f in files:
                if f not in self.attachments: self.attachments.append(f)
            self.update_attach_lbl()

class ScanSummaryDialog(QDialog):
    def __init__(self, stats, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scan Complete")
        self.resize(500, 450)
        layout = QVBoxLayout(self)

        header = QLabel("<b>Scan Summary</b>")
        font = header.font()
        font.setPointSize(14)
        header.setFont(font)
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

        grid = QFormLayout()
        grid.addRow("<b>Total Workspace Runs Found:</b>", QLabel(str(stats['ws'])))
        grid.addRow("<b>Total Outfeed Runs Found:</b>", QLabel(str(stats['outfeed'])))
        grid.addRow("", QLabel(""))
        grid.addRow("<b>Total FC Runs:</b>", QLabel(str(stats['fc'])))
        grid.addRow("<b>Total Innovus Runs:</b>", QLabel(str(stats['innovus'])))
        layout.addLayout(grid)

        layout.addWidget(QLabel("<b>Runs per Block Breakdown:</b>"))
        table = QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["Block Name", "Number of Runs"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.setRowCount(len(stats['blocks']))
        
        row = 0
        for blk, count in sorted(stats['blocks'].items(), key=lambda x: x[1], reverse=True):
            table.setItem(row, 0, QTableWidgetItem(blk))
            count_item = QTableWidgetItem(str(count))
            count_item.setTextAlignment(Qt.AlignCenter)
            table.setItem(row, 1, count_item)
            row += 1
            
        layout.addWidget(table)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok)
        btn_box.accepted.connect(self.accept)
        layout.addWidget(btn_box)


class EditNoteDialog(QDialog):
    def __init__(self, current_text, identifier_name, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Alias / Personal Note")
        self.resize(400, 250)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<b>Edit Note for:</b><br>{identifier_name}"))
        
        self.text_edit = QTextEdit()
        # Clean current user tag if editing own note
        clean_text = current_text
        user_tag = f"[{getpass.getuser()}]"
        if user_tag in clean_text:
            clean_text = clean_text.split(user_tag)[-1].strip()
        self.text_edit.setPlainText(clean_text)
        layout.addWidget(self.text_edit)
        
        layout.addWidget(QLabel("<i>Notes are visible to all dashboard users.</i>"))
        
        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def get_text(self):
        return self.text_edit.toPlainText().strip()

class FilterDialog(QDialog):
    def __init__(self, col_name, unique_values, active_values, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Filter Column: {col_name}")
        self.resize(320, 420)
        layout = QVBoxLayout(self)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search items...")
        self.search.textChanged.connect(self.filter_list)
        layout.addWidget(self.search)

        self.list_widget = QListWidget()
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()
        sel_all = QPushButton("Select All")
        desel_all = QPushButton("Clear All")
        sel_all.clicked.connect(lambda: self.set_all(True))
        desel_all.clicked.connect(lambda: self.set_all(False))
        btn_layout.addWidget(sel_all)
        btn_layout.addWidget(desel_all)
        layout.addLayout(btn_layout)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        for val in sorted(unique_values):
            item = QListWidgetItem(val)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if val in active_values else Qt.Unchecked)
            self.list_widget.addItem(item)

    def set_all(self, state):
        s = Qt.Checked if state else Qt.Unchecked
        for i in range(self.list_widget.count()):
            if not self.list_widget.item(i).isHidden():
                self.list_widget.item(i).setCheckState(s)

    def filter_list(self, text):
        text = text.lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setHidden(text not in item.text().lower())

    def get_selected(self):
        return [self.list_widget.item(i).text() for i in range(self.list_widget.count())
                if self.list_widget.item(i).checkState() == Qt.Checked]

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Settings")
        self.resize(400, 420)
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

        self.rel_time_cb = QCheckBox("Show relative timestamps")
        self.rel_time_cb.setChecked(parent.show_relative_time if parent else False)
        layout.addRow("", self.rel_time_cb)

        self.ist_cb = QCheckBox("Convert Timestamps to IST (from KST)")
        self.ist_cb.setChecked(parent.convert_to_ist if parent else False)
        layout.addRow("", self.ist_cb)
        
        self.hide_blocks_cb = QCheckBox("Hide Block grouping in Tree")
        self.hide_blocks_cb.setChecked(parent.hide_block_nodes if parent else False)
        layout.addRow("", self.hide_blocks_cb)

        self.theme_cb = QCheckBox("Enable Dark Mode")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addRow(sep)

        self.use_custom_cb = QCheckBox("Enable Custom Colors")
        self.use_custom_cb.setChecked(parent.use_custom_colors if parent else False)
        layout.addRow("Theme:", self.use_custom_cb)

        self.bg_color  = parent.custom_bg_color  if parent else "#2b2d30"
        self.fg_color  = parent.custom_fg_color  if parent else "#dfe1e5"
        self.sel_color = parent.custom_sel_color if parent else "#2f65ca"

        self.bg_btn  = QPushButton("Pick Background Color");  self.bg_btn.clicked.connect(self.pick_bg)
        self.fg_btn  = QPushButton("Pick Text Color");        self.fg_btn.clicked.connect(self.pick_fg)
        self.sel_btn = QPushButton("Pick Selection Color");   self.sel_btn.clicked.connect(self.pick_sel)

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


class PieChartWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setMinimumSize(450, 450)
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
        margin = 30
        min_dim = min(rect.width(), rect.height()) - 2 * margin
        if min_dim <= 0: return
        center = rect.center()
        pie_rect = QRectF(center.x() - min_dim/2, center.y() - min_dim/2, min_dim, min_dim)
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
            if (val / total) > 0.03:
                mid_angle_deg = (start_angle + span_angle / 2) / 16.0
                mid_angle_rad = math.radians(mid_angle_deg)
                text_x = center.x() + (min_dim / 2 * 0.65) * math.cos(mid_angle_rad)
                text_y = center.y() - (min_dim / 2 * 0.65) * math.sin(mid_angle_rad)
                perc = (val / total) * 100
                text = f"{name}\n{perc:.1f}%"
                font = painter.font(); font.setBold(True); font.setPointSize(9)
                painter.setFont(font)
                fm = painter.fontMetrics()
                lines = text.split('\n'); th = fm.height()
                y_offset = text_y - (th * len(lines)) / 2
                for line in lines:
                    tw = fm.horizontalAdvance(line)
                    painter.setPen(QPen(QColor(0, 0, 0, 180)))
                    painter.drawText(int(text_x - tw/2 + 1), int(y_offset + th + 1), line)
                    painter.setPen(QPen(Qt.white))
                    painter.drawText(int(text_x - tw/2), int(y_offset + th), line)
                    y_offset += th
            start_angle += span_angle

class DiskUsageDialog(QDialog):
    def __init__(self, disk_data, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Disk Space Usage (Advanced Drill-Down)")
        self.resize(1200, 700)
        self.disk_data = disk_data
        self.is_dark = is_dark
        self.parent_window = parent
        layout = QVBoxLayout(self)
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("<b>Select Directory Scope:</b>"))
        self.combo = QComboBox()
        self.combo.addItems(["WS (FE)", "WS (BE)", "OUTFEED"])
        self.combo.currentIndexChanged.connect(self.update_view)
        top_row.addWidget(self.combo)
        top_row.addSpacing(30)
        self.partition_lbl = QLabel("")
        self.partition_lbl.setStyleSheet("color: #d32f2f; font-weight: bold;" if not is_dark else "color: #e57373; font-weight: bold;")
        top_row.addWidget(self.partition_lbl)
        top_row.addStretch()
        layout.addLayout(top_row)
        main_body = QHBoxLayout()
        self.pie = PieChartWidget()
        main_body.addWidget(self.pie, 1)
        self.tree = QTreeWidget()
        self.tree.setColumnCount(2)
        self.tree.setHeaderLabels(["User / Directory Path", "Size (GB)"])
        self.tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.tree.setAlternatingRowColors(True)
        self.tree.setSelectionMode(QTreeWidget.ExtendedSelection)
        self.tree.itemChanged.connect(self.handle_item_changed)
        main_body.addWidget(self.tree, 2)
        layout.addLayout(main_body, 1)
        
        bottom_row = QHBoxLayout()
        self.recalc_btn = QPushButton("Recalculate Disk Usage")
        self.recalc_btn.setMinimumHeight(35)
        self.recalc_btn.clicked.connect(self.trigger_recalc)
        bottom_row.addWidget(self.recalc_btn)
        
        bottom_row.addStretch()
        
        self.mail_btn = QPushButton("Send Cleanup Mail to Selected")
        self.mail_btn.setMinimumHeight(35)
        self.mail_btn.clicked.connect(self.send_disk_mail)
        btn_color = "#2f65ca" if self.is_dark else "#3182ce"
        self.mail_btn.setStyleSheet(f"QPushButton {{ background-color: {btn_color}; color: white; font-weight: bold; padding: 5px 15px; border-radius: 4px; }}")
        bottom_row.addWidget(self.mail_btn)
        layout.addLayout(bottom_row)
        self.update_view()

    def update_view(self):
        self.tree.blockSignals(True)
        self.tree.clear()
        cat = self.combo.currentText()
        data = self.disk_data.get(cat, {})
        if cat == "WS (FE)": p_path = BASE_WS_FE_DIR
        elif cat == "WS (BE)": p_path = BASE_WS_BE_DIR
        else: p_path = BASE_OUTFEED_DIR
        self.partition_lbl.setText(get_partition_space(p_path))
        pie_data = {user: info["total"] for user, info in data.items()}
        self.pie.set_data(pie_data, self.is_dark)
        sorted_data = sorted(data.items(), key=lambda item: item[1]["total"], reverse=True)
        for i, (user, info) in enumerate(sorted_data):
            user_item = QTreeWidgetItem(self.tree)
            user_item.setFlags(user_item.flags() | Qt.ItemIsUserCheckable)
            user_item.setCheckState(0, Qt.Unchecked)
            user_item.setText(0, user)
            user_item.setText(1, f"{info['total']:.2f} GB")
            color = QColor(self.pie.colors[i % len(self.pie.colors)])
            user_item.setForeground(0, color)
            font = user_item.font(0); font.setBold(True)
            user_item.setFont(0, font); user_item.setFont(1, font)
            for dir_path, dir_sz in info["dirs"]:
                dir_item = QTreeWidgetItem(user_item)
                dir_item.setFlags(dir_item.flags() | Qt.ItemIsUserCheckable)
                dir_item.setCheckState(0, Qt.Unchecked)
                dir_item.setText(0, os.path.basename(dir_path))
                dir_item.setToolTip(0, dir_path)
                dir_item.setText(1, f"{dir_sz:.2f} GB")
                dir_item.setData(0, Qt.UserRole, user)
                dir_item.setData(0, Qt.UserRole + 1, dir_path)
        self.tree.blockSignals(False)

    def trigger_recalc(self):
        if self.parent_window:
            self.recalc_btn.setText("Calculating...")
            self.recalc_btn.setEnabled(False)
            self.parent_window.start_bg_disk_scan(force=True)

    def handle_item_changed(self, item, column):
        if column != 0: return
        self.tree.blockSignals(True)
        state = item.checkState(0)
        for i in range(item.childCount()): item.child(i).setCheckState(0, state)
        self.tree.blockSignals(False)

    def send_disk_mail(self):
        user_runs = {}
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            user_item = root.child(i)
            for j in range(user_item.childCount()):
                dir_item = user_item.child(j)
                if dir_item.checkState(0) == Qt.Checked:
                    owner = dir_item.data(0, Qt.UserRole)
                    path  = dir_item.data(0, Qt.UserRole + 1)
                    if owner not in user_runs: user_runs[owner] = []
                    user_runs[owner].append(path)
        if not user_runs:
            QMessageBox.warning(self, "No Directories Selected", "Please check at least one directory to request cleanup.")
            return
            
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Action Required: Please clean up heavy disk usage runs",
                                 "Hi,\n\nPlease remove these runs as they are consuming disk space and are no longer needed:\n\n",
                                 all_known, "", self)
        
        if dlg.exec_():
            subject = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            
            success_count = 0
            for owner, paths in user_runs.items():
                owner_email = get_user_email(owner)
                if not owner_email: continue
                
                final_body = body_template + "\n" + "\n".join(paths)
                
                all_recipients = set(base_to + base_cc + [owner_email])
                recipients_str = ",".join(all_recipients)
                
                cmd = [MAIL_UTIL, "-to", recipients_str, "-sd", sender_email, "-s", subject, "-c", final_body, "-fm", "text"]
                for att in dlg.attachments:
                    cmd.extend(["-a", att])
                    
                try:
                    subprocess.Popen(cmd); success_count += 1
                except Exception as e:
                    print(f"Failed to send mail: {e}")
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} cleanup emails.")

# ===========================================================================
# --- BACKGROUND WORKER THREADS ---
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

class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def _get_batch_dir_info(self, paths):
        results = []
        if not paths: return results
        try:
            cmd = ['du', '-sk'] + paths
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode('utf-8')
            for line in output.strip().split('\n'):
                if not line: continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    sz_kb = int(parts[0])
                    full_path = parts[1]
                    try:
                        owner = pwd.getpwuid(os.stat(full_path).st_uid).pw_name
                    except:
                        owner = "Unknown"
                    results.append((owner, sz_kb, full_path))
        except: pass
        return results

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "fc", "*"))
        outfeed_targets.extend(glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "innovus", "*")))
        if not outfeed_targets:
            outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*"))

        targets_map = {
            "WS (FE)":  glob.glob(os.path.join(BASE_WS_FE_DIR, "*")),
            "WS (BE)":  glob.glob(os.path.join(BASE_WS_BE_DIR, "*")),
            "OUTFEED":  outfeed_targets
        }

        tasks = []
        for cat, paths in targets_map.items():
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                chunk = valid_paths[i:i+50]
                tasks.append((cat, chunk))

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                cat = future_to_cat[future]
                try:
                    batch_results = future.result()
                    for owner, sz_kb, full_path in batch_results:
                        if sz_kb > 0:
                            gb_sz = sz_kb / (1024**2)
                            if gb_sz > 0.01:
                                if owner not in results[cat]: results[cat][owner] = {"total": 0, "dirs": []}
                                results[cat][owner]["total"] += gb_sz
                                results[cat][owner]["dirs"].append((full_path, gb_sz))
                except: pass

        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)

        self.finished_scan.emit(results)

# ===========================================================================
# --- MAIN SCANNER WORKER ---
# ===========================================================================
class ScannerWorker(QThread):
    finished        = pyqtSignal(dict, dict, dict, dict)
    progress_update = pyqtSignal(int, int)
    status_update   = pyqtSignal(str)

    def scan_ir_dir(self):
        ir_data = {}
        target_lef = f"{PROJECT_PREFIX}.lef.list"
        ir_dirs = BASE_IR_DIR.split()

        for ir_base in ir_dirs:
            if not os.path.exists(ir_base): continue
            for root_dir, dirs, files in os.walk(ir_base):
                for f_name in files:
                    if not f_name.startswith("redhawk.log"): continue
                    log_path = os.path.join(root_dir, f_name)
                    
                    run_be_name = step_name = None
                    static_val = dynamic_val = "-"
                    in_static = in_dynamic = False
                    
                    static_lines = []
                    dynamic_lines = []
                    
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m: run_be_name = m.group(1); step_name = m.group(2)

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
                                    "static": "-", "dynamic": "-", "log": log_path,
                                    "static_table": "", "dynamic_table": ""
                                }
                            if static_val != "-": ir_data[key]["static"] = static_val
                            if dynamic_val != "-": ir_data[key]["dynamic"] = dynamic_val
                            if static_lines: ir_data[key]["static_table"] = "\n".join(static_lines)
                            if dynamic_lines: ir_data[key]["dynamic_table"] = "\n".join(dynamic_lines)
                    except: pass
        return ir_data

    def _scan_single_workspace(self, ws_base, ws_name, tools_to_scan):
        tasks = []
        releases_found = {}
        ws_path = os.path.join(ws_base, ws_name)
        if not os.path.isdir(ws_path): return tasks, releases_found

        current_rtl = "Unknown"
        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            try:
                with open(sf, 'r') as f:
                    lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                    for l in set(lbls):
                        current_rtl = normalize_rtl(l)
                        if current_rtl not in releases_found: releases_found[current_rtl] = []
                        releases_found[current_rtl].append(ws_path)
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

        return tasks, releases_found

    def run(self):
        clear_path_cache()
        self.status_update.emit("Discovering Workspaces...")
        
        ws_data  = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data = {"releases": {}, "blocks": set(), "all_runs": []}
        scan_stats = {'ws': 0, 'outfeed': 0, 'blocks': {}, 'fc': 0, 'innovus': 0}
        
        tasks = []
        tools_to_scan = PNR_TOOL_NAMES.split()

        disc_futures = []
        disc_max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=disc_max_w) as disc_ex:
            for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
                if not os.path.exists(ws_base): continue
                try: ws_names = os.listdir(ws_base)
                except: continue
                for ws_name in ws_names:
                    disc_futures.append(
                        (disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan))
                    )

            for future in concurrent.futures.as_completed(disc_futures):
                try:
                    new_tasks, new_releases = future.result()
                    tasks.extend(new_tasks)
                    for rtl, paths in new_releases.items():
                        for p in paths: self._map_release(ws_data, rtl, p)
                except: pass

        self.status_update.emit("Discovering OUTFEED directories...")
        if os.path.exists(BASE_OUTFEED_DIR):
            for ent_name in os.listdir(BASE_OUTFEED_DIR):
                ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
                if not os.path.isdir(ent_path): continue
                for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                    phys_evt = os.path.basename(evt_dir)
                    for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                        tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                    if "fc" in tools_to_scan:
                        be_runs = (glob.glob(os.path.join(evt_dir, "fc", "*-BE")) +
                                   glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")))
                        for rd in be_runs:
                            tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                            if os.path.isdir(rd):
                                tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        paths_to_prefetch = []
        for t in tasks:
            rd = t[1]
            paths_to_prefetch.append(os.path.join(rd, "pass/compile_opt.pass"))
            paths_to_prefetch.append(os.path.join(rd, "logs/compile_opt.log"))
            paths_to_prefetch.append(os.path.join(rd, "reports/runtime.V2.rpt"))
            
        self.status_update.emit("Prefetching file metadata...")
        prefetch_path_cache(paths_to_prefetch)

        total_tasks = len(tasks)
        completed_tasks = 0
        max_w = min(40, (os.cpu_count() or 4) * 6)

        self.status_update.emit("Processing run data and parsing reports...")
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
                            scan_stats['ws'] += 1
                            if result["run_type"] == "BE":
                                self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            scan_stats['outfeed'] += 1
                            self._map_release(out_data, result["rtl"], result["path"])
                            
                        # Stat gathering
                        blk = result["block"]
                        if blk not in scan_stats['blocks']: scan_stats['blocks'][blk] = 0
                        scan_stats['blocks'][blk] += 1
                        
                        if "/fc/" in result["path"]: scan_stats['fc'] += 1
                        elif "/innovus/" in result["path"]: scan_stats['innovus'] += 1
                            
                except Exception: pass
                completed_tasks += 1
                self.progress_update.emit(completed_tasks, total_tasks)
                if completed_tasks % 20 == 0:
                    self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")

            ir_data = ir_future.result()

        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

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
        r_name       = os.path.basename(rd)
        clean_run    = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)
        evt_base     = get_dynamic_evt_path(rtl, b_name)
        owner        = get_owner(rd)

        fm_n     = os.path.join(evt_base, "fm", clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")
        info     = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))

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
                    except: pass

        stages = []
        if run_type == "BE":
            search_glob = os.path.join(rd, "outputs", "*") if source == "WS" else os.path.join(rd, "*")
            for s_dir in glob.glob(search_glob):
                if not os.path.isdir(s_dir): continue
                step_name = os.path.basename(s_dir)
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]: continue

                if source == "WS":
                    rpt = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt")
                    log = os.path.join(rd, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, "outputs", step_name)
                else:
                    rpt = os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                    log = os.path.join(s_dir, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, step_name)

                fm_u_glob    = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                fm_n_glob    = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                st_vslp_rpt  = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                sta_rpt      = os.path.join(evt_base, "pt",   r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                qor_path     = rd if rd.endswith("/") else rd + "/"

                stages.append({
                    "name": step_name, "rpt": rpt, "log": log,
                    "info": parse_pnr_runtime_rpt(rpt),
                    "st_n": get_fm_info(st_fm_n_path), "st_u": get_fm_info(st_fm_u_path),
                    "vslp_status": get_vslp_info(st_vslp_rpt),
                    "fm_u_path": st_fm_u_path, "fm_n_path": st_fm_n_path,
                    "vslp_rpt_path": st_vslp_rpt, "sta_rpt_path": sta_rpt,
                    "qor_path": qor_path, "stage_path": stage_path
                })

        return {
            "block": b_name, "path": rd, "parent": parent_path, "rtl": rtl,
            "r_name": r_name, "run_type": run_type, "stages": stages,
            "source": source, "owner": owner, "is_comp": is_comp, "fe_status": fe_status,
            "st_n": get_fm_info(fm_n), "st_u": get_fm_info(fm_u),
            "vslp_status": get_vslp_info(vslp_rpt),
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
        self.setWindowTitle("Singularity PD | Pro Edition")
        self.resize(1920, 1000)

        self.ws_data  = {}
        self.out_data = {}
        self.ir_data  = {}
        self.global_notes = {}

        self.is_dark_mode       = False
        self.use_custom_colors  = False
        self.custom_bg_color    = "#2b2d30"
        self.custom_fg_color    = "#dfe1e5"
        self.custom_sel_color   = "#2f65ca"

        self.row_spacing          = 2
        self.show_relative_time   = False
        self.convert_to_ist       = False
        self.hide_block_nodes     = False
        self._columns_fitted_once = False
        self._initial_size_calc_done = False
        self._last_scan_time      = None
        self.is_compact           = False

        self.size_workers        = []
        self.item_map            = {}
        self.ignored_paths       = set()
        self._checked_paths      = set()
        self.current_error_log_path = None
        
        self.run_filter_config   = None
        self.current_config_path = None
        self.active_col_filters  = {}
        
        self._cached_disk_data = None
        self.disk_worker = None

        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)

        self.init_ui()
        self._setup_shortcuts()
        
        self.start_fs_scan()
        self.start_bg_disk_scan()

    def closeEvent(self, event):
        os._exit(0)

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(6, 6, 6, 4)
        root_layout.setSpacing(4)

        top_layout = QHBoxLayout()
        top_layout.setSpacing(6)

        top_layout.addWidget(self._label("Source:"))
        self.src_combo = QComboBox()
        self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.setFixedWidth(100)
        self.src_combo.currentIndexChanged.connect(self.on_source_changed)
        top_layout.addWidget(self.src_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("RTL Release:"))
        self.rel_combo = QComboBox()
        self.rel_combo.setMinimumWidth(220)
        self.rel_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.rel_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("View:"))
        self.view_combo = QComboBox()
        self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Running Only", "Failed Only", "Today's Runs"])
        self.view_combo.setFixedWidth(120)
        self.view_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.view_combo)

        self._add_separator(top_layout)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search runs, blocks, status, runtime...   [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.textChanged.connect(lambda: self.search_timer.start(500))
        top_layout.addWidget(self.search)

        top_layout.addStretch()

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
        self.tools_menu.addAction("Load Run Filter Config...",   self.load_filter_config)
        self.tools_menu.addAction("Clear Run Filter Config",     self.clear_filter_config)
        self.tools_menu.addAction("Generate Sample Config",      self.generate_sample_config)
        self.tools_menu.addSeparator()
        self.tools_menu.addAction("Calculate All Run Sizes",     self.calculate_all_sizes)
        self.tools_btn.setMenu(self.tools_menu)
        top_layout.addWidget(self.tools_btn)

        self.qor_btn  = self._btn("Compare QoR", self.run_qor_comparison)
        top_layout.addWidget(self.qor_btn)
        
        # New Mail Dropdown
        self.mail_btn = QPushButton("Send Mail")
        self.mail_menu = QMenu(self)
        self.mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        self.mail_menu.addAction("Send Compare QoR Mail", self.send_qor_mail_action)
        self.mail_menu.addAction("Send Custom Mail", self.send_custom_mail_action)
        self.mail_btn.setMenu(self.mail_menu)
        top_layout.addWidget(self.mail_btn)
        
        self.disk_btn = self._btn("Disk Space",   self.open_disk_usage)
        self.settings_btn = self._btn("Settings", self.open_settings)
        top_layout.addWidget(self.disk_btn)
        top_layout.addWidget(self.settings_btn)

        self.size_toggle_btn = QToolButton()
        self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarNormalButton))
        self.size_toggle_btn.setToolTip("Toggle Compact Window Mode")
        self.size_toggle_btn.clicked.connect(self.toggle_window_size)
        self.size_toggle_btn.setFixedSize(28, 28)
        top_layout.addWidget(self.size_toggle_btn)

        root_layout.addLayout(top_layout)

        # Enhanced Progress UI
        self.prog_container = QWidget()
        self.prog_container.setVisible(False)
        self.prog_layout = QHBoxLayout(self.prog_container)
        self.prog_layout.setContentsMargins(4, 0, 4, 0)
        self.prog_lbl = QLabel("Initializing Scanner...")
        self.prog_lbl.setStyleSheet("color: #1976D2; font-weight: bold;")
        self.prog = QProgressBar()
        self.prog.setFixedHeight(6)
        self.prog.setTextVisible(False)
        self.prog.setStyleSheet(
            "QProgressBar { border: none; border-radius: 3px; background: #ddd; }"
            "QProgressBar::chunk { background: #1976D2; border-radius: 3px; }")
        self.prog_layout.addWidget(self.prog_lbl)
        self.prog_layout.addWidget(self.prog, 1)
        root_layout.addWidget(self.prog_container)

        self.splitter = QSplitter(Qt.Horizontal)

        left_panel = QWidget()
        left_panel.setMaximumWidth(320)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 4, 0)
        left_layout.setSpacing(6)

        blk_header = QHBoxLayout()
        blk_header.addWidget(self._label("<b>Blocks</b>"))
        blk_header.addStretch()
        all_btn  = QPushButton("All");  all_btn.setCursor(Qt.PointingHandCursor);  all_btn.setObjectName("linkBtn")
        none_btn = QPushButton("None"); none_btn.setCursor(Qt.PointingHandCursor); none_btn.setObjectName("linkBtn")
        all_btn.clicked.connect(lambda: self._set_all_blocks(True))
        none_btn.clicked.connect(lambda: self._set_all_blocks(False))
        blk_header.addWidget(all_btn)
        sep_lbl = self._label("|"); sep_lbl.setStyleSheet("color: gray;")
        blk_header.addWidget(sep_lbl)
        blk_header.addWidget(none_btn)
        left_layout.addLayout(blk_header)

        self.blk_list = QListWidget()
        self.blk_list.setAlternatingRowColors(True)
        f = self.blk_list.font(); f.setPointSize(f.pointSize() + 1); f.setBold(True)
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

        self.tree = QTreeWidget()
        self.tree.setColumnCount(23)
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)
        self.tree.setAnimated(False)
        self.tree.setExpandsOnDoubleClick(False)
        self.tree.setSortingEnabled(True)
        self.tree.header().setSectionsMovable(True)
        self.tree.header().setStretchLastSection(True)

        headers = [
            "Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "User", "Size",
            "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes"
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
        self.tree.setColumnWidth(10, 100); self.tree.setColumnWidth(11, 100)
        self.tree.setColumnWidth(12, 110); self.tree.setColumnWidth(13, 120)
        self.tree.setColumnWidth(14, 120); self.tree.setColumnWidth(22, 300)

        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        
        # Auto-fit Column 0 on Expand/Collapse
        self.tree.itemExpanded.connect(lambda: self.tree.resizeColumnToContents(0))
        self.tree.itemCollapsed.connect(lambda: self.tree.resizeColumnToContents(0))

        for i in [15, 16, 17, 18, 19, 20, 21]: self.tree.setColumnHidden(i, True)

        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.tree.itemChanged.connect(self._on_item_check_changed)

        self.splitter.addWidget(self.tree)
        self.splitter.setSizes([260, 1660])
        root_layout.addWidget(self.splitter)

        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(26)
        self.setStatusBar(self.status_bar)

        self.sb_total     = QLabel("Total: 0")
        self.sb_complete  = QLabel("Completed: 0")
        self.sb_running   = QLabel("Running: 0")
        self.sb_selected  = QLabel("Selected: 0")
        self.sb_scan_time = QLabel("")
        self.sb_config    = QLabel("Config: None")

        for lbl in [self.sb_total, self.sb_complete, self.sb_running, self.sb_selected, self.sb_scan_time, self.sb_config]:
            lbl.setContentsMargins(8, 0, 8, 0)
            self.status_bar.addPermanentWidget(lbl)
            self.status_bar.addPermanentWidget(self._vsep())

        self.apply_theme_and_spacing()

    # ------------------------------------------------------------------
    # CONFIGURATION FILTERING
    # ------------------------------------------------------------------
    def generate_sample_config(self):
        sample_text = """# PD Dashboard Run Filter Configuration
# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...
# 
# Rules:
# - SOURCE can be WS or OUTFEED
# - If a Source, RTL, and Block are defined here, ONLY the runs listed will be shown.
# - If a Source, RTL, or Block is NOT defined here, ALL runs for it will be shown normally.
# - Use spaces to separate multiple run names.

OUTFEED : EVT0_ML4_DEV00_syn2 : BLK_CPU : my_test_run fast_route_run
WS : EVT0_ML4_DEV00 : BLK_GPU : golden_run
"""
        path, _ = QFileDialog.getSaveFileName(self, "Save Sample Config", "dashboard_filter.cfg", "Config Files (*.cfg *.txt)")
        if path:
            with open(path, 'w') as f:
                f.write(sample_text)
            QMessageBox.information(self, "Success", f"Sample config saved to:\n{path}")

    def load_filter_config(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Run Filter Config", "", "Config Files (*.cfg *.txt);;All Files (*)")
        if not path: return

        parsed_config = {}
        try:
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    parts = line.split(":", 3)
                    if len(parts) == 4:
                        src = parts[0].strip()
                        rtl = parts[1].strip()
                        blk = parts[2].strip()
                        runs = set(parts[3].strip().split())
                        
                        if src not in parsed_config: parsed_config[src] = {}
                        if rtl not in parsed_config[src]: parsed_config[src][rtl] = {}
                        parsed_config[src][rtl][blk] = runs
            
            self.run_filter_config = parsed_config
            self.current_config_path = path
            self.sb_config.setText(f"Config: Active ({os.path.basename(path)})")
            self.sb_config.setStyleSheet("color: #d32f2f; font-weight: bold;" if not self.is_dark_mode else "color: #ffb74d; font-weight: bold;")
            self.refresh_view()
            QMessageBox.information(self, "Config Loaded", "Filter configuration applied successfully. Runs not in the config are now hidden.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to parse config file:\n{e}")

    def clear_filter_config(self):
        if self.run_filter_config is not None:
            self.run_filter_config = None
            self.current_config_path = None
            self.sb_config.setText("Config: None")
            self.sb_config.setStyleSheet("")
            self.refresh_view()

    def _save_current_config(self):
        if not self.current_config_path or self.run_filter_config is None: return
        try:
            with open(self.current_config_path, 'w') as f:
                f.write("# PD Dashboard Run Filter Configuration\n")
                f.write("# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...\n\n")
                for src, rtls in self.run_filter_config.items():
                    for rtl, blocks in rtls.items():
                        for blk, runs in blocks.items():
                            if runs:
                                f.write(f"{src} : {rtl} : {blk} : {' '.join(sorted(list(runs)))}\n")
        except Exception as e:
            print(f"Failed to save config: {e}")

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _label(self, text): return QLabel(text)

    def _btn(self, text, slot):
        b = QPushButton(text); b.clicked.connect(slot); return b

    def _add_separator(self, layout):
        sep = QFrame(); sep.setFrameShape(QFrame.VLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

    def _vsep(self):
        sep = QFrame(); sep.setFrameShape(QFrame.VLine); sep.setFrameShadow(QFrame.Sunken)
        sep.setFixedHeight(16); return sep

    def _set_all_blocks(self, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        self.blk_list.blockSignals(True)
        for i in range(self.blk_list.count()): self.blk_list.item(i).setCheckState(state)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    def toggle_window_size(self):
        if not self.is_compact:
            self.showNormal(); self.resize(1280, 720); self.is_compact = True
            self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarMaxButton))
        else:
            self.showNormal(); self.resize(1920, 1000); self.is_compact = False
            self.size_toggle_btn.setIcon(self.style().standardIcon(QStyle.SP_TitleBarNormalButton))

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"),       self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"),       self, lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"),       self, lambda: self.tree.expandAll())
        QShortcut(QKeySequence("Ctrl+W"),       self, lambda: self.tree.collapseAll())
        QShortcut(QKeySequence("Ctrl+Shift+F"), self, self.fit_all_columns)

    # ------------------------------------------------------------------
    # STATUS-BAR & ERROR UI
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
        path = item.text(15)
        if not path or path == "N/A": return
        hl_color = QColor(self.custom_sel_color if self.use_custom_colors
                          else ("#404652" if self.is_dark_mode else "#e3f2fd"))
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
            if item.data(0, Qt.UserRole) != "STAGE":
                run_path = item.text(15)
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
                        color = ("#e57373" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"))
                                 else "#d32f2f")
                        if count == 0: color = ("#81c784" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"))
                                                else "#388e3c")
                        self.fe_error_btn.setStyleSheet(
                            f"QPushButton#errorLinkBtn {{ border: none; background: transparent; color: {color}; "
                            f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                            f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
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
            self.is_dark_mode       = dlg.theme_cb.isChecked()
            self.use_custom_colors  = dlg.use_custom_cb.isChecked()
            self.custom_bg_color    = dlg.bg_color
            self.custom_fg_color    = dlg.fg_color
            self.custom_sel_color   = dlg.sel_color
            self.row_spacing        = dlg.space_spin.value()
            self.show_relative_time = dlg.rel_time_cb.isChecked()
            self.convert_to_ist     = dlg.ist_cb.isChecked()
            self.hide_block_nodes   = dlg.hide_blocks_cb.isChecked()
            self.apply_theme_and_spacing()
            self.refresh_view()

    def apply_theme_and_spacing(self):
        pad = self.row_spacing
        cb_style = """
            QTreeView::indicator:checked   { background-color: #4CAF50; border: 1px solid #388E3C; image: none; }
            QTreeView::indicator:unchecked { background-color: white;   border: 1px solid gray; }
        """
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
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: {sel}; color: #ffffff; }}
                {cb_style}"""
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
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QTableWidget {{ background-color: #1e1f22; alternate-background-color: #26282b; gridline-color: #393b40; }}
                {cb_style}"""
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
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #3182ce; color: #ffffff; }}
                QTableWidget {{ background-color: #ffffff; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; }}
                {cb_style}"""

        self.setStyleSheet(stylesheet)
        self._recolor_existing_items()

    def _recolor_existing_items(self):
        def recolor(node):
            for i in range(node.childCount()):
                child = node.child(i)
                node_type = child.data(0, Qt.UserRole)
                if node_type == "MILESTONE":
                    child.setForeground(0, QColor("#1e88e5" if not self.is_dark_mode else "#64b5f6"))
                if node_type not in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"):
                    self._apply_status_color(child, 3, child.text(3))
                    self._apply_fm_color(child, 7, child.text(7))
                    self._apply_fm_color(child, 8, child.text(8))
                    self._apply_vslp_color(child, 9, child.text(9))
                    src = child.text(2)
                    if src == "OUTFEED":
                        child.setForeground(2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))
                    elif src == "WS":
                        child.setForeground(2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
                recolor(child)
        recolor(self.tree.invisibleRootItem())

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
        self.prog_container.setVisible(True)
        self.prog.setRange(0, 0)
        self.prog_lbl.setText("Initializing Scanner...")
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Scanning...")
        self.worker = ScannerWorker()
        self.worker.progress_update.connect(self.update_progress)
        self.worker.status_update.connect(self.update_status_lbl)
        self.worker.finished.connect(self.on_scan_finished)
        self.worker.start()

    def update_progress(self, current, total):
        self.prog.setRange(0, total)
        self.prog.setValue(current)
        
    def update_status_lbl(self, message):
        self.prog_lbl.setText(message)

    def on_scan_finished(self, ws, out, ir, stats):
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.prog_container.setVisible(False)
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.global_notes = load_all_notes()
        
        self.on_source_changed()
        if not self._columns_fitted_once:
            self._columns_fitted_once = True
            self.fit_all_columns()
            
        if not self._initial_size_calc_done:
            self._initial_size_calc_done = True
            self.calculate_all_sizes()

        all_owners = set()
        for r in self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", []):
            if r.get("owner") and r["owner"] != "Unknown": all_owners.add(r["owner"])
        if all_owners: save_mail_users_config(all_owners)
        
        # Display Scan Summary Pop-up
        summary_dlg = ScanSummaryDialog(stats, self)
        summary_dlg.exec_()

    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, True);  self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False); self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)

        releases, blocks = set(), set()
        if src_mode in ["WS",  "ALL"] and self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            releases.update(self.out_data.get("releases", {}).keys())
            blocks.update(self.out_data.get("blocks", set()))

        current_rtl  = self.rel_combo.currentText()
        saved_states = {self.blk_list.item(i).data(Qt.UserRole): self.blk_list.item(i).checkState()
                        for i in range(self.blk_list.count())}

        self.rel_combo.blockSignals(True); self.rel_combo.clear()
        valid = [r for r in releases if "Unknown" not in r and get_milestone(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(valid)
        self.rel_combo.addItems(new_releases)
        self.rel_combo.setCurrentText(current_rtl if current_rtl in new_releases else "[ SHOW ALL ]")
        self.rel_combo.blockSignals(False)

        self.blk_list.blockSignals(True); self.blk_list.clear()
        for b in sorted(blocks):
            it = QListWidgetItem(b)
            it.setData(Qt.UserRole, b) # Store RAW block name
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
                path = child.text(15)
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
            if text in self.global_notes:
                notes = " | ".join(self.global_notes[text])
                p.setText(22, notes)
                p.setToolTip(22, notes)
                p.setForeground(22, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        return p

    def _get_item_path_id(self, item):
        parts = []
        while item is not None:
            parts.insert(0, item.text(0).strip()); item = item.parent()
        return "|".join(parts)

    # ------------------------------------------------------------------
    # COLOR HELPERS
    # ------------------------------------------------------------------
    def _apply_status_color(self, item, col, status):
        if status == "COMPLETED":   item.setForeground(col, QColor("#1b5e20" if not self.is_dark_mode else "#81c784"))
        elif status == "RUNNING":   item.setForeground(col, QColor("#0d47a1" if not self.is_dark_mode else "#64b5f6"))
        elif status == "NOT STARTED": item.setForeground(col, QColor("#757575" if not self.is_dark_mode else "#9e9e9e"))
        elif status == "INTERRUPTED": item.setForeground(col, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        elif status in ("FAILED", "FATAL ERROR", "ERROR"):
            item.setForeground(col, QColor("#b71c1c" if not self.is_dark_mode else "#e57373"))

    def _apply_fm_color(self, item, col, val):
        if "FAILS" in val: item.setForeground(col, QColor("#d32f2f" if not self.is_dark_mode else "#e57373"))
        elif "PASS" in val: item.setForeground(col, QColor("#388e3c" if not self.is_dark_mode else "#81c784"))

    def _apply_vslp_color(self, item, col, val):
        if "Error" in val and "Error: 0" not in val:
            item.setForeground(col, QColor("#d32f2f" if not self.is_dark_mode else "#e57373"))
        elif "Error: 0" in val:
            item.setForeground(col, QColor("#388e3c" if not self.is_dark_mode else "#81c784"))

    # ------------------------------------------------------------------
    # TREE FILTERING (COLUMN VALUES)
    # ------------------------------------------------------------------
    def show_column_filter_dialog(self, col):
        unique_values = set()
        def gather(node):
            if node.data(0, Qt.UserRole) not in ["BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"]:
                unique_values.add(node.text(col).strip())
            for i in range(node.childCount()):
                gather(node.child(i))
        gather(self.tree.invisibleRootItem())

        if not unique_values:
            QMessageBox.information(self, "Filter", "No data available in this column to filter.")
            return

        active = self.active_col_filters.get(col, unique_values)
        col_name = self.tree.headerItem().text(col).replace(" [*]", "")

        dlg = FilterDialog(col_name, unique_values, active, self)
        if dlg.exec_():
            selected = dlg.get_selected()
            if len(selected) == len(unique_values):
                if col in self.active_col_filters:
                    del self.active_col_filters[col]
            else:
                self.active_col_filters[col] = selected
            self.apply_tree_filters()

    def apply_tree_filters(self):
        for col in range(self.tree.columnCount()):
            orig_text = self.tree.headerItem().text(col).replace(" [*]", "")
            if col in self.active_col_filters:
                self.tree.headerItem().setText(col, orig_text + " [*]")
            else:
                self.tree.headerItem().setText(col, orig_text)

        def update_visibility(item):
            item_matches = True
            is_group_node = item.data(0, Qt.UserRole) in ["BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"]

            if not is_group_node:
                for col, allowed in self.active_col_filters.items():
                    val = item.text(col).strip()
                    if val not in allowed:
                        item_matches = False
                        break

            any_child_visible = False
            for i in range(item.childCount()):
                if update_visibility(item.child(i)):
                    any_child_visible = True

            if is_group_node:
                is_visible = any_child_visible
            else:
                is_visible = item_matches or any_child_visible

            item.setHidden(not is_visible)
            return is_visible

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            update_visibility(root.child(i))


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
        
        child.setData(0, Qt.UserRole + 2, run["block"])
        child.setData(0, Qt.UserRole + 4, run["r_name"].replace("-FE", "").replace("-BE", ""))

        if run["path"] in self._checked_paths:
            child.setCheckState(0, Qt.Checked)
            hl_color = QColor(self.custom_sel_color if self.use_custom_colors
                              else ("#404652" if self.is_dark_mode else "#e3f2fd"))
            for c in range(self.tree.columnCount()): child.setBackground(c, hl_color)

        is_ir_block = (run["block"].upper() == PROJECT_PREFIX.upper())
        tooltip_text = (f"Owner: {run.get('owner','Unknown')}\nSize: Pending\n"
                        f"Runtime: {run['info']['runtime']}\nNONUPF: {run['st_n']}\n"
                        f"UPF: {run['st_u']}\nVSLP: {run['vslp_status']}\n")
        if is_ir_block: tooltip_text += "\nStatic/Dynamic IR: Check individual stage levels for full tables."
        child.setToolTip(0, tooltip_text)
        child.setExpanded(False)

        if run["source"] == "OUTFEED": child.setForeground(2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))
        else: child.setForeground(2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        if run["run_type"] == "FE":
            status_str = run["fe_status"]
            child.setText(3, status_str)
            child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-")
            child.setText(7, f"NONUPF - {run['st_n']}")
            child.setText(8, f"UPF - {run['st_u']}")
            child.setText(9, run["vslp_status"])
            child.setText(12, run["info"]["runtime"])
            
            start_raw = run["info"]["start"]; end_raw = run["info"]["end"]
            if self.convert_to_ist:
                start_raw = convert_kst_to_ist_str(start_raw)
                end_raw = convert_kst_to_ist_str(end_raw)
                
            if self.show_relative_time:
                child.setText(13, relative_time(start_raw))
                child.setText(14, relative_time(end_raw) if run["is_comp"] else "-")
            else:
                child.setText(13, start_raw); child.setText(14, end_raw)
            child.setToolTip(13, start_raw); child.setToolTip(14, end_raw)
            
            child.setText(15, run["path"])
            child.setText(16, os.path.join(run["path"], "logs/compile_opt.log"))
            child.setText(17, run["fm_u_path"]); child.setText(18, run["fm_n_path"])
            child.setText(19, run["vslp_rpt_path"]); child.setText(20, ""); child.setText(21, "")
            self._apply_status_color(child, 3, status_str)
            self._apply_fm_color(child, 7, run["st_n"])
            self._apply_fm_color(child, 8, run["st_u"])
            self._apply_vslp_color(child, 9, run["vslp_status"])
        else:
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-")
            child.setText(15, run["path"]); child.setText(21, "")

        for i in range(1, 23): child.setTextAlignment(i, Qt.AlignCenter)
        child.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
        child.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)
        
        # Load and set notes
        run_identifier = f"{run['rtl']} : {run['r_name']}"
        if run_identifier in self.global_notes:
            notes = " | ".join(self.global_notes[run_identifier])
            child.setText(22, notes)
            child.setToolTip(22, notes)
            child.setForeground(22, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        return child

    # ------------------------------------------------------------------
    # ADD STAGES (BE)
    # ------------------------------------------------------------------
    def _add_stages(self, be_item, be_run, ign_root):
        owner = be_run.get("owner", "Unknown")
        is_ir_block = (be_run["block"].upper() == PROJECT_PREFIX.upper())

        for stage in be_run["stages"]:
            parent_node = ign_root if stage["stage_path"] in self.ignored_paths else be_item
            st_item = CustomTreeItem(parent_node)
            st_item.setFlags(st_item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            st_item.setCheckState(0, Qt.Unchecked)
            st_item.setData(0, Qt.UserRole, "STAGE")
            st_item.setData(1, Qt.UserRole, stage["name"])
            st_item.setData(2, Qt.UserRole, stage["qor_path"])

            ir_key  = f"{be_run['r_name']}/{stage['name']}"
            ir_info = self.ir_data.get(ir_key, {"static": "-", "dynamic": "-", "log": "", "static_table": "", "dynamic_table": ""})

            tooltip_text = (f"Owner: {owner}\nSize: Pending\nRuntime: {stage['info']['runtime']}\n"
                            f"NONUPF: {stage['st_n']}\nUPF: {stage['st_u']}\nVSLP: {stage['vslp_status']}\n")
            
            if is_ir_block:
                tooltip_text += f"\nStatic IR Value: {ir_info['static']}"
                if ir_info.get('static_table'):
                    tooltip_text += f"\n{ir_info['static_table']}\n"
                    
                tooltip_text += f"\nDynamic IR Value: {ir_info['dynamic']}"
                if ir_info.get('dynamic_table'):
                    tooltip_text += f"\n{ir_info['dynamic_table']}\n"
                    
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

            st_item.setText(0, stage["name"]); st_item.setText(2, be_run["source"])
            st_item.setText(4, stage_status);  st_item.setText(5, owner)
            st_item.setText(6, "-")
            st_item.setText(7, f"NONUPF - {stage['st_n']}")
            st_item.setText(8, f"UPF - {stage['st_u']}")
            st_item.setText(9, stage["vslp_status"])
            st_item.setText(10, ir_info["static"]  if is_ir_block else "-")
            st_item.setText(11, ir_info["dynamic"] if is_ir_block else "-")
            st_item.setText(12, stage["info"]["runtime"])
            
            s_start = stage["info"]["start"]; s_end = stage["info"]["end"]
            if self.convert_to_ist:
                s_start = convert_kst_to_ist_str(s_start)
                s_end = convert_kst_to_ist_str(s_end)

            if self.show_relative_time:
                st_item.setText(13, relative_time(s_start)); st_item.setText(14, relative_time(s_end))
            else:
                st_item.setText(13, s_start); st_item.setText(14, s_end)
            st_item.setToolTip(13, s_start); st_item.setToolTip(14, s_end)
            st_item.setText(15, stage["stage_path"]); st_item.setText(16, stage["log"])
            st_item.setText(17, stage["fm_u_path"]);  st_item.setText(18, stage["fm_n_path"])
            st_item.setText(19, stage["vslp_rpt_path"]); st_item.setText(20, stage["sta_rpt_path"])
            st_item.setText(21, ir_info["log"])

            self._apply_fm_color(st_item, 7, stage["st_n"])
            self._apply_fm_color(st_item, 8, stage["st_u"])
            self._apply_vslp_color(st_item, 9, stage["vslp_status"])
            self._apply_status_color(st_item, 4,
                stage_status if stage_status in ("COMPLETED","RUNNING","FAILED") else "RUNNING")

            for i in range(1, 23): st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
            st_item.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

    # ------------------------------------------------------------------
    # BLOCK SIDEBAR COLORS
    # ------------------------------------------------------------------
    def _block_aggregate_status(self, block_name, runs):
        for r in runs:
            if r["block"] == block_name and r["run_type"] == "FE" and not r["is_comp"]: return "running"
        return "done"

    def _refresh_block_colors(self, runs):
        for i in range(self.blk_list.count()):
            it = self.blk_list.item(i)
            raw_name = it.data(Qt.UserRole)
            status = self._block_aggregate_status(raw_name, runs)
            # User request: Green if running, Blue if not
            it.setForeground(QColor("#2e7d32" if not self.is_dark_mode else "#81c784")
                             if status == "running"
                             else QColor("#0277bd" if not self.is_dark_mode else "#64b5f6"))

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

        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        self.tree.setSortingEnabled(False)
        self.tree.clear()

        if self.rel_combo.currentText() == "[ SHOW ALL ]": self.tree.setColumnHidden(1, False)
        else: self.tree.setColumnHidden(1, True)

        src_mode   = self.src_combo.currentText()
        sel_rtl    = self.rel_combo.currentText()
        is_filtered = (self.view_combo.currentText() != "All Runs") or (self.search.text().strip() != "")
        is_running_view = (self.view_combo.currentText() == "Running Only")

        raw_query      = self.search.text().lower().strip()
        search_pattern = "*" if not raw_query else (f"*{raw_query}*" if '*' not in raw_query else raw_query)

        checked_blks = [self.blk_list.item(i).data(Qt.UserRole) for i in range(self.blk_list.count())
                        if self.blk_list.item(i).checkState() == Qt.Checked]

        runs_to_process = []
        if src_mode in ["WS",  "ALL"] and self.ws_data:  runs_to_process.extend(self.ws_data.get("all_runs", []))
        if src_mode in ["OUTFEED","ALL"] and self.out_data: runs_to_process.extend(self.out_data.get("all_runs", []))

        # Dynamically fetch running count per block to update sidebar
        running_counts = {b: 0 for b in [self.blk_list.item(x).data(Qt.UserRole) for x in range(self.blk_list.count())]}
        
        # Synchronize BE runs RTL with their FE parent
        fe_info = {}
        for run in runs_to_process:
            if run["run_type"] == "FE":
                fe_base = run["r_name"].replace("-FE", "")
                fe_info[(run["block"], fe_base)] = run["rtl"]
                if not run["is_comp"]:
                    if run["block"] in running_counts: running_counts[run["block"]] += 1

        for run in runs_to_process:
            if run["run_type"] == "BE":
                clean_be = run["r_name"].replace("-BE", "")
                for (blk, fe_base), fe_rtl in fe_info.items():
                    if run["block"] == blk and (clean_be == fe_base or f"_{fe_base}_" in clean_be or clean_be.startswith(f"{fe_base}_") or clean_be.endswith(f"_{fe_base}")):
                        run["rtl"] = fe_rtl
                        break

        # Sidebar text update
        for i in range(self.blk_list.count()):
            it = self.blk_list.item(i)
            raw_name = it.data(Qt.UserRole)
            if is_running_view:
                c = running_counts.get(raw_name, 0)
                it.setText(f"{raw_name} - {c} runs" if c > 0 else f"{raw_name} - NO runs")
            else:
                it.setText(raw_name)


        ignored_runs_list, normal_runs_list = [], []

        for run in runs_to_process:
            if run["path"] in self.ignored_paths: ignored_runs_list.append(run); continue
            if run["block"] not in checked_blks: continue

            if self.run_filter_config is not None:
                r_src = run["source"]
                r_rtl = run["rtl"]
                r_blk = run["block"]
                if r_src in self.run_filter_config and r_rtl in self.run_filter_config[r_src] and r_blk in self.run_filter_config[r_src][r_rtl]:
                    allowed_runs = self.run_filter_config[r_src][r_rtl][r_blk]
                    base_run_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                    if base_run_name not in allowed_runs and run["r_name"] not in allowed_runs:
                        continue 

            base_rtl_filter = re.sub(r'_syn\d+$', '', run["rtl"])
            if get_milestone(base_rtl_filter) is None: continue
            if sel_rtl != "[ SHOW ALL ]":
                if not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")): continue

            preset = self.view_combo.currentText()
            if preset == "FE Only"      and run["run_type"] != "FE": continue
            if preset == "BE Only"      and run["run_type"] != "BE": continue
            if preset == "Running Only" and not (run["run_type"] == "FE" and not run["is_comp"]): continue
            if preset == "Failed Only"  and not ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","") or run.get("fe_status") == "FATAL ERROR"): continue
            if preset == "Today's Runs":
                start = run["info"].get("start","")
                rt = relative_time(start)
                if not (rt.endswith("ago") and ("h ago" in rt or "m ago" in rt)): continue

            if search_pattern != "*":
                # Include notes in search
                note_id = f"{run['rtl']} : {run['r_name']}"
                notes = " | ".join(self.global_notes.get(note_id, []))
                
                combined = (f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} "
                            f"{run['st_n']} {run['st_u']} {run['vslp_status']} "
                            f"{run['info']['runtime']} {run['info']['start']} {run['info']['end']} "
                            f"{notes}").lower()
                matches = fnmatch.fnmatch(combined, search_pattern)
                if not matches and run["run_type"] == "BE":
                    for stage in run["stages"]:
                        sc = f"{stage['name']} {stage['st_n']} {stage['st_u']} {stage['vslp_status']} {stage['info']['runtime']}".lower()
                        if fnmatch.fnmatch(sc, search_pattern): matches = True; break
                if not matches: continue

            normal_runs_list.append(run)

        fe_runs = [r for r in normal_runs_list if r["run_type"] == "FE"]
        be_runs = [r for r in normal_runs_list if r["run_type"] == "BE"]

        root     = self.tree.invisibleRootItem()
        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

        for fe_run in fe_runs:
            blk_name = fe_run["block"]; run_rtl = fe_run["rtl"]
            base_rtl = re.sub(r'_syn\d+$', '', run_rtl); has_syn = (run_rtl != base_rtl)
            
            base_attach_node = root if self.hide_block_nodes else self._get_node(root, blk_name, "BLOCK")

            if sel_rtl == "[ SHOW ALL ]":
                m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
                parent_for_run = self._get_node(m_node, base_rtl, "RTL")
            elif sel_rtl == base_rtl and has_syn:
                parent_for_run = self._get_node(base_attach_node, run_rtl, "RTL")
            else:
                parent_for_run = base_attach_node
                
            self._create_run_item(parent_for_run, fe_run)

        for be_run in be_runs:
            blk_name = be_run["block"]; run_rtl = be_run["rtl"]
            base_rtl = re.sub(r'_syn\d+$', '', run_rtl); has_syn = (run_rtl != base_rtl)
            
            base_attach_node = root if self.hide_block_nodes else self._get_node(root, blk_name, "BLOCK")

            if sel_rtl == "[ SHOW ALL ]":
                m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
                parent_for_run = self._get_node(m_node, base_rtl, "RTL")
            elif sel_rtl == base_rtl and has_syn:
                parent_for_run = self._get_node(base_attach_node, run_rtl, "RTL")
            else:
                parent_for_run = base_attach_node

            fe_parent = None
            for i in range(parent_for_run.childCount()):
                c = parent_for_run.child(i)
                if c.data(0, Qt.UserRole) != "STAGE":
                    fe_base = c.text(0).replace("-FE", "")
                    clean_be = be_run["r_name"].replace("-BE", "")
                    if c.text(2) == be_run["source"] and c.data(0, Qt.UserRole + 2) == be_run["block"] and (clean_be == fe_base or f"_{fe_base}_" in clean_be or clean_be.startswith(f"{fe_base}_") or clean_be.endswith(f"_{fe_base}")):
                        fe_parent = c; break

            actual_parent = fe_parent if fe_parent else parent_for_run
            be_item = self._create_run_item(actual_parent, be_run)
            self._add_stages(be_item, be_run, ign_root)

        if ignored_runs_list:
            for run in ignored_runs_list:
                blk_name = run["block"]; run_rtl = run["rtl"]
                base_rtl = re.sub(r'_syn\d+$', '', run_rtl); has_syn = (run_rtl != base_rtl)
                
                base_attach_node = ign_root if self.hide_block_nodes else self._get_node(ign_root, blk_name, "BLOCK")
                
                if sel_rtl == "[ SHOW ALL ]":
                    m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
                    parent_for_run = self._get_node(m_node, base_rtl, "RTL")
                elif sel_rtl == base_rtl and has_syn:
                    parent_for_run = self._get_node(base_attach_node, run_rtl, "RTL")
                else:
                    parent_for_run = base_attach_node
                    
                item = self._create_run_item(parent_for_run, run)
                if run["run_type"] == "BE": self._add_stages(item, run, ign_root)

        if ign_root.childCount() == 0: root.removeChild(ign_root)

        self.apply_tree_filters()
        self.tree.setSortingEnabled(True)

        def restore_state(node):
            for i in range(node.childCount()):
                child     = node.child(i)
                path_key  = self._get_item_path_id(child)
                node_type = child.data(0, Qt.UserRole)
                is_run    = bool(child.text(15))
                if is_filtered and node_type in ["BLOCK", "MILESTONE", "RTL"]:
                    child.setExpanded(True)
                elif path_key in expanded_states:
                    child.setExpanded(expanded_states[path_key])
                else:
                    if child.parent() is None:   child.setExpanded(True)
                    elif node_type == "MILESTONE": child.setExpanded(False)
                    elif sel_rtl == "[ SHOW ALL ]" and node_type == "RTL": child.setExpanded(False)
                    elif is_run:                   child.setExpanded(False)
                    else:                          child.setExpanded(True)
                restore_state(child)
        restore_state(root)

        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)

        self._refresh_block_colors(list(runs_to_process))
        self._update_status_bar(normal_runs_list)
        self.on_tree_selection_changed()
        
        QTimer.singleShot(50, lambda: self.tree.resizeColumnToContents(0))

    # ------------------------------------------------------------------
    # CONTEXT MENUS
    # ------------------------------------------------------------------
    def on_header_context_menu(self, pos):
        col = self.tree.header().logicalIndexAt(pos)
        menu = QMenu(self)
        col_name = self.tree.headerItem().text(col).replace(" [*]", "")

        sort_asc_act = menu.addAction(f"Sort A to Z")
        sort_desc_act = menu.addAction(f"Sort Z to A")
        menu.addSeparator()

        filter_act = menu.addAction(f"Filter Column '{col_name}'...")
        clear_act = menu.addAction(f"Clear Filter")
        clear_act.setEnabled(col in self.active_col_filters)
        
        clear_all_act = menu.addAction("Clear All Filters")
        clear_all_act.setEnabled(len(self.active_col_filters) > 0)
        menu.addSeparator()

        vis_menu = menu.addMenu("Show / Hide Columns")
        for i in range(1, 23):
            action = QWidgetAction(vis_menu)
            cb = QCheckBox(self.tree.headerItem().text(i).replace(" [*]", ""))
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.setStyleSheet("margin: 2px 8px; background: transparent; color: inherit;")
            cb.toggled.connect(lambda checked, c=i: self.tree.setColumnHidden(c, not checked))
            action.setDefaultWidget(cb)
            vis_menu.addAction(action)

        action = menu.exec_(self.tree.header().mapToGlobal(pos))
        if action == sort_asc_act:
            self.tree.sortByColumn(col, Qt.AscendingOrder)
        elif action == sort_desc_act:
            self.tree.sortByColumn(col, Qt.DescendingOrder)
        elif action == filter_act:
            self.show_column_filter_dialog(col)
        elif action == clear_act:
            if col in self.active_col_filters:
                del self.active_col_filters[col]
                self.apply_tree_filters()
        elif action == clear_all_act:
            self.active_col_filters.clear()
            self.apply_tree_filters()

    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item or not item.parent(): return
        col = self.tree.columnAt(pos.x())
        m   = QMenu()

        cell_text    = item.text(col).strip()
        copy_cell_act = m.addAction("Copy Cell Text") if cell_text else None
        if copy_cell_act: m.addSeparator()

        run_path  = item.text(15); log_path  = item.text(16)
        fm_u_path = item.text(17); fm_n_path = item.text(18)
        vslp_path = item.text(19); sta_path  = item.text(20); ir_path = item.text(21)
        is_stage  = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl    = item.data(0, Qt.UserRole) == "RTL"
        
        target_item = item if not is_stage else item.parent()
        b_name = target_item.data(0, Qt.UserRole + 2)
        r_rtl = target_item.text(1)
        base_run = target_item.data(0, Qt.UserRole + 4)
        run_source = target_item.text(2)
        
        # Add / Edit Note Logic
        edit_note_act = None
        note_identifier = ""
        if run_path and run_path != "N/A" and not is_stage:
            note_identifier = f"{r_rtl} : {item.text(0)}"
            edit_note_act = m.addAction("Add / Edit Personal Note")
            m.addSeparator()
        elif is_rtl:
            note_identifier = item.text(0)
            edit_note_act = m.addAction("Add / Edit Alias Note for RTL")
            m.addSeparator()
        
        add_config_act = None
        if b_name and r_rtl and base_run and run_source:
            if self.current_config_path:
                add_config_act = m.addAction("Add Run to Active Filter Config")
            else:
                add_config_act = m.addAction("Create New Filter Config & Add Run")
            m.addSeparator()

        ignore_checked_act = m.addAction("Hide All Checked Runs/Stages")
        m.addSeparator()

        ignore_act = restore_act = None
        target_path = item.text(15)
        if target_path and target_path != "N/A":
            if target_path in self.ignored_paths: restore_act = m.addAction("Restore (Unhide)")
            else: ignore_act = m.addAction("Hide/Ignore")
            m.addSeparator()

        calc_size_act = m.addAction("Calculate Folder Size") if run_path and run_path != "N/A" and cached_exists(run_path) else None
        if calc_size_act: m.addSeparator()

        fm_n_act    = m.addAction("Open NONUPF Formality Report") if fm_n_path and fm_n_path != "N/A" and cached_exists(fm_n_path) else None
        fm_u_act    = m.addAction("Open UPF Formality Report")    if fm_u_path and fm_u_path != "N/A" and cached_exists(fm_u_path) else None
        v_act       = m.addAction("Open VSLP Report")             if vslp_path and vslp_path != "N/A" and cached_exists(vslp_path) else None
        sta_act     = m.addAction("Open PT STA Summary")          if sta_path  and sta_path  != "N/A" and cached_exists(sta_path)  else None
        ir_stat_act = m.addAction("Open Static IR Log")           if ir_path   and ir_path   != "N/A" and cached_exists(ir_path)   else None
        ir_dyn_act  = m.addAction("Open Dynamic IR Log")          if is_stage and ir_path and ir_path != "N/A" and cached_exists(ir_path) else None
        log_act     = m.addAction("Open Log File")                if log_path  and log_path  != "N/A" and cached_exists(log_path)  else None

        m.addSeparator()
        c_act   = m.addAction("Copy Path")
        qor_act = None
        if is_stage: m.addSeparator(); qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res: return
        
        if edit_note_act and res == edit_note_act:
            current_note = item.text(22)
            dlg = EditNoteDialog(current_note, note_identifier, self)
            if dlg.exec_():
                save_user_note(note_identifier, dlg.get_text())
                self.global_notes = load_all_notes()
                self.refresh_view()

        elif add_config_act and res == add_config_act:
            if not self.current_config_path:
                path, _ = QFileDialog.getSaveFileName(self, "Create New Config", "dashboard_filter.cfg", "Config Files (*.cfg *.txt)")
                if not path: return
                self.current_config_path = path
                self.run_filter_config = {}
                
            if run_source not in self.run_filter_config: self.run_filter_config[run_source] = {}
            if r_rtl not in self.run_filter_config[run_source]: self.run_filter_config[run_source][r_rtl] = {}
            if b_name not in self.run_filter_config[run_source][r_rtl]: self.run_filter_config[run_source][r_rtl][b_name] = set()
            self.run_filter_config[run_source][r_rtl][b_name].add(base_run)
            
            self._save_current_config()
            self.sb_config.setText(f"Config: Active ({os.path.basename(self.current_config_path)})")
            color = "#d32f2f" if not self.is_dark_mode else "#ffb74d"
            self.sb_config.setStyleSheet(f"color: {color}; font-weight: bold;")
            self.refresh_view()
            
        elif res == ignore_checked_act:
            def ig(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    if c.checkState(0) == Qt.Checked:
                        p = c.text(15)
                        if p and p != "N/A": self.ignored_paths.add(p)
                    ig(c)
            ig(self.tree.invisibleRootItem()); self.refresh_view()
        elif res == ignore_act:  self.ignored_paths.add(target_path);    self.refresh_view()
        elif res == restore_act: self.ignored_paths.discard(target_path); self.refresh_view()
        elif copy_cell_act and res == copy_cell_act: QApplication.clipboard().setText(cell_text)
        elif calc_size_act and res == calc_size_act:
            item.setText(6, "Calc...")
            worker = SingleSizeWorker(item, run_path)
            worker.result.connect(lambda it, sz: (
                it.setText(6, sz),
                it.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {sz}\n', it.toolTip(0) if it.toolTip(0) else ""))
            ))
            if not hasattr(self, 'size_workers'): self.size_workers = []
            self.size_workers.append(worker)
            worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
            worker.start()
        elif fm_n_act    and res == fm_n_act:    subprocess.Popen(['gvim', fm_n_path])
        elif fm_u_act    and res == fm_u_act:    subprocess.Popen(['gvim', fm_u_path])
        elif v_act       and res == v_act:       subprocess.Popen(['gvim', vslp_path])
        elif sta_act     and res == sta_act:     subprocess.Popen(['gvim', sta_path])
        elif ir_stat_act and res == ir_stat_act: subprocess.Popen(['gvim', ir_path])
        elif ir_dyn_act  and res == ir_dyn_act:  subprocess.Popen(['gvim', ir_path])
        elif log_act     and res == log_act:     subprocess.Popen(['gvim', log_path])
        elif res == c_act: QApplication.clipboard().setText(run_path)
        elif qor_act and res == qor_act:
            step_name = item.data(1, Qt.UserRole)
            qor_path  = item.data(2, Qt.UserRole)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = find_latest_qor_report()
            if h: subprocess.Popen([FIREFOX_PATH, h])

    def on_item_double_clicked(self, item, col):
        if item.parent():
            log = item.text(16)
            if log and cached_exists(log): 
                subprocess.Popen(['gvim', log])

    # ------------------------------------------------------------------
    # MAIL FEATURE
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        user_runs = {}
        is_fe_selected = False

        def find_owners(node):
            nonlocal is_fe_selected
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    path  = c.text(15)
                    owner = c.text(5)
                    if "FE" in c.text(0): is_fe_selected = True
                    if path and path != "N/A" and owner and owner != "Unknown":
                        if owner not in user_runs: user_runs[owner] = []
                        user_runs[owner].append(path)
                find_owners(c)
        find_owners(self.tree.invisibleRootItem())

        if not user_runs:
            QMessageBox.warning(self, "No Runs Selected", "Please select at least one run to send a cleanup mail.")
            return

        default_subject = "Action Required: Please clean up heavy disk usage runs"
        if user_runs:
            default_subject = "Please remove your old PI runs" if is_fe_selected else "Please remove your old PD runs"
            
        all_known = get_all_known_mail_users()
        
        # Prefill 'To' based on selected owners
        unique_emails = []
        for owner in user_runs.keys():
            e = get_user_email(owner)
            if e: unique_emails.append(e)
            
        dlg = AdvancedMailDialog(default_subject,
                                 "Hi,\n\nPlease remove these runs as they are consuming disk space and are no longer needed:\n\n",
                                 all_known, ", ".join(unique_emails), self)
                                 
        if dlg.exec_():
            subject = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            
            success_count = 0
            for owner, paths in user_runs.items():
                owner_email = get_user_email(owner)
                if not owner_email: continue
                
                final_body = body_template + "\n" + "\n".join(paths)
                all_recipients = set(base_to + base_cc + [owner_email])
                recipients_str = ",".join(all_recipients)
                
                cmd = [MAIL_UTIL, "-to", recipients_str, "-sd", sender_email, "-s", subject, "-c", final_body, "-fm", "text"]
                for att in dlg.attachments: cmd.extend(["-a", att])
                    
                try:
                    subprocess.Popen(cmd); success_count += 1
                except Exception as e:
                    print(f"Failed to send mail: {e}")
                    
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} emails.")
            
    def send_qor_mail_action(self):
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Latest Compare QoR Report",
                                 "Hi Team,\n\nPlease find the attached latest QoR Report for your reference.\n\nRegards",
                                 all_known, "", self)
        dlg.attach_qor()
        
        if dlg.exec_():
            subject = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            
            all_recipients = set(base_to + base_cc)
            if not all_recipients: return
            recipients_str = ",".join(all_recipients)
            
            cmd = [MAIL_UTIL, "-to", recipients_str, "-sd", sender_email, "-s", subject, "-c", body_template, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
                
            try:
                subprocess.Popen(cmd)
                QMessageBox.information(self, "Mail Sent", "Successfully sent the Compare QoR mail.")
            except Exception as e:
                print(f"Failed to send mail: {e}")

    def send_custom_mail_action(self):
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("", "", all_known, "", self)
        if dlg.exec_():
            subject = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            
            all_recipients = set(base_to + base_cc)
            if not all_recipients: return
            recipients_str = ",".join(all_recipients)
            
            cmd = [MAIL_UTIL, "-to", recipients_str, "-sd", sender_email, "-s", subject, "-c", body_template, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
                
            try:
                subprocess.Popen(cmd)
                QMessageBox.information(self, "Mail Sent", "Successfully sent the custom mail.")
            except Exception as e:
                print(f"Failed to send mail: {e}")


    # ------------------------------------------------------------------
    # DISK USAGE FEATURE
    # ------------------------------------------------------------------
    def start_bg_disk_scan(self, force=False):
        if self.disk_worker and self.disk_worker.isRunning(): return
        if not force and self._cached_disk_data is not None: return
        
        self.disk_btn.setEnabled(False)
        self.disk_btn.setText("Scanning Disk...")
        self.disk_worker = DiskScannerWorker()
        self.disk_worker.finished_scan.connect(self._on_bg_disk_scan_finished)
        self.disk_worker.start()

    def _on_bg_disk_scan_finished(self, results):
        self._cached_disk_data = results
        self.disk_btn.setEnabled(True)
        self.disk_btn.setText("Disk Space")
        
        if hasattr(self, 'disk_dialog') and self.disk_dialog is not None and self.disk_dialog.isVisible():
            self.disk_dialog.disk_data = results
            self.disk_dialog.update_view()
            self.disk_dialog.recalc_btn.setText("Recalculate Disk Usage")
            self.disk_dialog.recalc_btn.setEnabled(True)

    def open_disk_usage(self):
        if self._cached_disk_data is None:
            QMessageBox.information(self, "Scanning", "Disk usage is still calculating in the background.\nPlease wait a moment and try again.")
            return
            
        self.disk_dialog = DiskUsageDialog(
            self._cached_disk_data,
            self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"),
            self)
        self.disk_dialog.exec_()

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
                    qp  = c.text(15)
                    src = c.text(2)
                    if src == "OUTFEED" and qp: qp = os.path.dirname(qp)
                    if qp and not qp.endswith("/"): qp += "/"
                    sel.append(qp)
                get_checked(c)
        get_checked(root)
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = find_latest_qor_report()
        if h: subprocess.Popen([FIREFOX_PATH, h])

# ===========================================================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.show()
    sys.exit(app.exec_())
