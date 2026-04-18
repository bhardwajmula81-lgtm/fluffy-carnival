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
import csv

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QTreeWidget, QTreeWidgetItem,
    QPushButton, QMessageBox, QListWidget, QListWidgetItem,
    QProgressBar, QMenu, QSplitter, QFontComboBox, QSpinBox,
    QWidgetAction, QCheckBox, QDialog, QFormLayout, QDialogButtonBox,
    QStatusBar, QFrame, QShortcut, QAction, QToolButton, QStyle, QColorDialog,
    QTextEdit, QTableWidget, QTableWidgetItem, QHeaderView, QProgressDialog,
    QFileDialog, QCompleter, QGroupBox, QPlainTextEdit, QScrollArea
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QDateTime, QRectF, QStringListModel, QSettings
from PyQt5.QtGui import QColor, QFont, QClipboard, QKeySequence, QPalette, QBrush, QPainter, QPen, QPixmap, QIcon

# ===========================================================================
# --- CONFIGURATION INITIALIZATION ---
# ===========================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "project_config.ini")
MAIL_USERS_FILE = os.path.join(SCRIPT_DIR, "mail_users.ini")
USER_PREFS_FILE = os.path.join(SCRIPT_DIR, "user_prefs.ini")
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
        with open(CONFIG_FILE, 'w') as f: config.write(f)
    except: pass
else: config.read(CONFIG_FILE)

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

prefs = configparser.ConfigParser()
if os.path.exists(USER_PREFS_FILE): prefs.read(USER_PREFS_FILE)

mail_config = configparser.ConfigParser()
if os.path.exists(MAIL_USERS_FILE): mail_config.read(MAIL_USERS_FILE)

# ===========================================================================
# --- THREAD-SAFE CACHE & HELPERS ---
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

def time_to_seconds(time_str):
    if not time_str or time_str == "N/A" or "h" not in time_str: return 0
    try:
        m = re.search(r'(\d+)h:(\d+)m:(\d+)s', time_str)
        if m: return int(m.group(1))*3600 + int(m.group(2))*60 + int(m.group(3))
    except: pass
    return 0

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
        ts = int(delta.total_seconds())
        if ts < 0: return date_str
        if ts < 3600: return f"{ts // 60}m ago"
        if ts < 86400: return f"{ts // 3600}h {(ts % 3600) // 60}m ago"
        return f"{ts // 86400}d ago"
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
        existing_str = mail_config.get('KNOWN_USERS', 'users', fallback='')
        existing = set([u.strip() for u in existing_str.split(',') if u.strip()])
        existing.update(new_users)
        if not mail_config.has_section('KNOWN_USERS'): mail_config.add_section('KNOWN_USERS')
        mail_config.set('KNOWN_USERS', 'users', ', '.join(sorted(existing)))
        with open(MAIL_USERS_FILE, 'w') as f: mail_config.write(f)
    except: pass

def get_all_known_mail_users():
    try:
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
            with open(user_file, 'r') as f: user_data = json.load(f)
        except: pass
    if note_text.strip(): user_data[identifier] = f"[{current_user}] {note_text.strip()}"
    else:
        if identifier in user_data: del user_data[identifier]
    try:
        with open(user_file, 'w') as f: json.dump(user_data, f, indent=4)
    except: pass


# ===========================================================================
# --- CUSTOM UI WIDGETS (GANTT, PIE, LOG TAILER, TREES) ---
# ===========================================================================
class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""

        # Pin/Star logic (Float starred runs to the top based on hidden data flag)
        if col == 0:
            s1 = bool(self.data(0, Qt.UserRole + 5))
            s2 = bool(other.data(0, Qt.UserRole + 5))
            if s1 != s2:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return s1 if asc else not s1

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
            if "[ Ignored" in t1: return False
            if "[ Ignored" in t2: return True
            m_order = {"INITIAL RELEASE": 1, "PRE-SVP": 2, "SVP": 3, "FFN": 4}
            if t1 in m_order and t2 in m_order:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return m_order[t1] < m_order[t2] if asc else m_order[t1] > m_order[t2]
        return t1 < t2

class GanttChartDialog(QDialog):
    def __init__(self, run_name, stages_data, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Timeline: {run_name}")
        self.resize(800, 400)
        layout = QVBoxLayout(self)
        self.scene = QWidget()
        self.scene.setMinimumHeight(max(200, len(stages_data) * 40 + 50))
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.scene)
        layout.addWidget(scroll)
        self.stages_data = stages_data
        self.is_dark = parent.is_dark_mode if parent else False

    def paintEvent(self, event):
        super().paintEvent(event)
        if not self.stages_data: return
        painter = QPainter(self.scene)
        painter.setRenderHint(QPainter.Antialiasing)
        w = self.scene.width() - 40
        x_start = 120
        usable_w = w - x_start
        max_sec = max([d['sec'] for d in self.stages_data if d['sec'] > 0] + [1])
        scale = usable_w / max_sec
        y = 30
        for data in self.stages_data:
            painter.setPen(QPen(Qt.white if self.is_dark else Qt.black))
            painter.drawText(10, y + 15, data['name'])
            bar_w = data['sec'] * scale
            color = QColor("#4CAF50") if data['sec'] > 0 else QColor("#9E9E9E")
            painter.setBrush(QBrush(color))
            painter.setPen(Qt.NoPen)
            painter.drawRect(x_start, y, int(bar_w), 20)
            painter.setPen(QPen(Qt.white if self.is_dark else Qt.black))
            painter.drawText(x_start + int(bar_w) + 10, y + 15, data['time_str'])
            y += 40

class MiniPieChart(QWidget):
    def __init__(self):
        super().__init__()
        self.setMinimumSize(200, 150)
        self.data = {'Passed': 0, 'Failed': 0, 'Running': 0}
        self.colors = {'Passed': QColor("#4CAF50"), 'Failed': QColor("#F44336"), 'Running': QColor("#2196F3")}
        self.bg_col = "#ffffff"

    def update_data(self, p, f, r, is_dark):
        self.data = {'Passed': p, 'Failed': f, 'Running': r}
        self.bg_col = "#2b2d30" if is_dark else "#f5f7fa"
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        rect = self.rect()
        margin = 10
        min_dim = min(rect.width(), rect.height()) - 2 * margin
        if min_dim <= 0: return
        center_x = margin + min_dim/2
        center_y = rect.center().y()
        pie_rect = QRectF(center_x - min_dim/2, center_y - min_dim/2, min_dim, min_dim)
        total = sum(self.data.values())
        if total == 0:
            painter.setPen(QColor("#888888"))
            painter.drawText(rect, Qt.AlignCenter, "No Data")
            return
        start_angle = 0
        for name, val in self.data.items():
            if val == 0: continue
            span_angle = (val / total) * 360 * 16
            painter.setBrush(QBrush(self.colors[name]))
            painter.setPen(QPen(QColor(self.bg_col), 1))
            painter.drawPie(pie_rect, int(start_angle), int(span_angle))
            start_angle += span_angle
        leg_x = center_x + min_dim/2 + 15
        leg_y = center_y - 20
        font = painter.font(); font.setPointSize(8); painter.setFont(font)
        for name in ['Passed', 'Running', 'Failed']:
            painter.setBrush(QBrush(self.colors[name]))
            painter.drawRect(int(leg_x), int(leg_y), 10, 10)
            painter.setPen(QPen(Qt.white if self.bg_col == "#2b2d30" else Qt.black))
            painter.drawText(int(leg_x) + 15, int(leg_y) + 10, f"{name}: {self.data[name]}")
            leg_y += 18

class LogTailer(QWidget):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0,0,0,0)
        header = QHBoxLayout()
        self.lbl = QLabel("<b>Live Log Viewer</b>")
        self.close_btn = QPushButton("Close"); self.close_btn.clicked.connect(self.hide)
        header.addWidget(self.lbl); header.addStretch(); header.addWidget(self.close_btn)
        self.layout.addLayout(header)
        self.text = QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFont("Courier", 9))
        self.layout.addWidget(self.text)
        self.timer = QTimer()
        self.timer.timeout.connect(self.read_log)
        self.current_file = None
        self.last_pos = 0

    def tail_file(self, path):
        self.current_file = path
        self.lbl.setText(f"<b>Tailing:</b> {path}")
        self.text.clear()
        self.last_pos = 0
        self.show()
        self.timer.start(1000)
        self.read_log()

    def read_log(self):
        if not self.current_file or not os.path.exists(self.current_file): return
        try:
            with open(self.current_file, 'r') as f:
                f.seek(self.last_pos)
                new_data = f.read()
                if new_data:
                    self.text.appendPlainText(new_data)
                    self.last_pos = f.tell()
                    bar = self.text.verticalScrollBar()
                    bar.setValue(bar.maximum())
        except: pass

    def hideEvent(self, event):
        self.timer.stop(); super().hideEvent(event)


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
        self.words = string_list; model = QStringListModel(self.words, self.completer)
        self.completer.setModel(model)
    def insertCompletion(self, completion):
        text = self.text(); parts = text.split(',')
        if len(parts) > 1: text = ','.join(parts[:-1]) + ', ' + completion + ', '
        else: text = completion + ', '
        self.setText(text)
    def keyPressEvent(self, e):
        if self.completer.popup().isVisible() and e.key() in (Qt.Key_Enter, Qt.Key_Return):
            e.ignore(); return
        super().keyPressEvent(e)
        cr = self.cursorRect(); cr.setWidth(self.completer.popup().sizeHintForColumn(0) + self.completer.popup().verticalScrollBar().sizeHint().width())
        current_word = self.text().split(',')[-1].strip()
        if current_word:
            self.completer.setCompletionPrefix(current_word)
            if self.completer.completionCount() > 0: self.completer.complete(cr)
            else: self.completer.popup().hide()
        else: self.completer.popup().hide()

class AdvancedMailDialog(QDialog):
    def __init__(self, default_subject, default_body, all_users, prefill_to="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send EMail Message")
        self.resize(700, 550)
        self.attachments = []
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()
        self.to_input = MultiCompleterLineEdit(); self.to_input.setModel(all_users)
        self.cc_input = MultiCompleterLineEdit(); self.cc_input.setModel(all_users)
        try:
            always_to = mail_config.get('PERMANENT_MEMBERS', 'always_to', fallback='').strip()
            always_cc = mail_config.get('PERMANENT_MEMBERS', 'always_cc', fallback='').strip()
            final_to = always_to
            if prefill_to: final_to = always_to + (", " if always_to else "") + prefill_to
            if final_to: self.to_input.setText(final_to + (', ' if not final_to.endswith(',') else ' '))
            if always_cc: self.cc_input.setText(always_cc + (', ' if not always_cc.endswith(',') else ' '))
        except: pass
        self.subject_input = QLineEdit(default_subject)
        form_layout.addRow("<b>To:</b>", self.to_input)
        form_layout.addRow("<b>CC:</b>", self.cc_input)
        form_layout.addRow("<b>Subject:</b>", self.subject_input)
        layout.addLayout(form_layout)
        attach_layout = QHBoxLayout()
        attach_layout.addWidget(QLabel("<b>Attachments:</b>"))
        self.attach_lbl = QLabel("None"); self.attach_lbl.setStyleSheet("color: #1976D2;")
        attach_layout.addWidget(self.attach_lbl); attach_layout.addStretch()
        btn_qor = QPushButton("Attach Latest QoR Report"); btn_qor.clicked.connect(self.attach_qor)
        btn_browse = QPushButton("Browse Files..."); btn_browse.clicked.connect(self.browse_files)
        attach_layout.addWidget(btn_qor); attach_layout.addWidget(btn_browse)
        layout.addLayout(attach_layout)
        self.body_input = QTextEdit(); self.body_input.setPlainText(default_body)
        layout.addWidget(self.body_input)
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Send Mail")
        self.buttons.accepted.connect(self.accept); self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def update_attach_lbl(self):
        if not self.attachments: self.attach_lbl.setText("None")
        else: self.attach_lbl.setText(f"{len(self.attachments)} file(s) attached.")
    def attach_qor(self):
        report = find_latest_qor_report()
        if report and report not in self.attachments:
            self.attachments.append(report); self.update_attach_lbl()
    def browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Attachments", "", "All Files (*)")
        if files:
            for f in files:
                if f not in self.attachments: self.attachments.append(f)
            self.update_attach_lbl()

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

        self.ist_cb = QCheckBox("Convert Timestamps to IST")
        self.ist_cb.setChecked(parent.convert_to_ist if parent else False)
        layout.addRow("", self.ist_cb)
        
        self.hide_blocks_cb = QCheckBox("Hide Block grouping in Tree")
        self.hide_blocks_cb.setChecked(parent.hide_block_nodes if parent else False)
        layout.addRow("", self.hide_blocks_cb)

        self.theme_cb = QCheckBox("Enable Dark Mode")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept); self.buttons.rejected.connect(self.reject)
        layout.addRow(self.buttons)

class ScanSummaryDialog(QDialog):
    def __init__(self, stats, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scan Complete")
        self.resize(500, 450)
        layout = QVBoxLayout(self)

        header = QLabel("<b>Scan Summary</b>")
        font = header.font(); font.setPointSize(14); header.setFont(font); header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

        grid = QFormLayout()
        grid.addRow("<b>Total Workspace Runs:</b>", QLabel(str(stats['ws'])))
        grid.addRow("<b>Total Outfeed Runs:</b>", QLabel(str(stats['outfeed'])))
        grid.addRow("", QLabel(""))
        grid.addRow("<b>Total FC Runs:</b>", QLabel(str(stats['fc'])))
        grid.addRow("<b>Total Innovus Runs:</b>", QLabel(str(stats['innovus'])))
        layout.addLayout(grid)

        layout.addWidget(QLabel("<b>Runs per Block Breakdown:</b>"))
        table = QTableWidget(); table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["Block Name", "Number of Runs"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.setRowCount(len(stats['blocks']))
        
        row = 0
        for blk, count in sorted(stats['blocks'].items(), key=lambda x: x[1], reverse=True):
            table.setItem(row, 0, QTableWidgetItem(blk))
            count_item = QTableWidgetItem(str(count)); count_item.setTextAlignment(Qt.AlignCenter)
            table.setItem(row, 1, count_item)
            row += 1
        layout.addWidget(table)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok); btn_box.accepted.connect(self.accept)
        layout.addWidget(btn_box)

class DiskUsageDialog(QDialog):
    def __init__(self, disk_data, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Disk Space Usage")
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
        self.pie = MiniPieChart()
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
        self.recalc_btn.clicked.connect(self.trigger_recalc)
        bottom_row.addWidget(self.recalc_btn)
        bottom_row.addStretch()
        
        self.mail_btn = QPushButton("Send Cleanup Mail to Selected")
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
        # Re-use pie chart for disk users
        self.pie.data = dict(sorted(pie_data.items(), key=lambda item: item[1], reverse=True)[:5]) # Top 5
        self.pie.bg_col = "#2b2d30" if self.is_dark else "#f5f7fa"
        self.pie.update()

        sorted_data = sorted(data.items(), key=lambda item: item[1]["total"], reverse=True)
        for i, (user, info) in enumerate(sorted_data):
            user_item = QTreeWidgetItem(self.tree)
            user_item.setFlags(user_item.flags() | Qt.ItemIsUserCheckable)
            user_item.setCheckState(0, Qt.Unchecked)
            user_item.setText(0, user); user_item.setText(1, f"{info['total']:.2f} GB")
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
                for att in dlg.attachments: cmd.extend(["-a", att])
                    
                try:
                    subprocess.Popen(cmd); success_count += 1
                except: pass
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} cleanup emails.")


# ===========================================================================
# --- WORKER THREADS ---
# ===========================================================================
class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)
    def __init__(self, tasks):
        super().__init__(); self.tasks = tasks; self._is_cancelled = False
    def run(self):
        max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self.get_size, path): item_id for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled: break
                try: self.size_calculated.emit(futures[future], future.result())
                except: self.size_calculated.emit(futures[future], "N/A")
    def get_size(self, path):
        if not path or not os.path.exists(path): return "N/A"
        try: return subprocess.check_output(['du', '-sh', path], stderr=subprocess.DEVNULL).decode('utf-8').split()[0]
        except: return "N/A"
    def cancel(self): self._is_cancelled = True

class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)
    def __init__(self, item, path):
        super().__init__(); self.item = item; self.path = path; self._is_cancelled = False
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
                    sz_kb = int(parts[0]); full_path = parts[1]
                    try: owner = pwd.getpwuid(os.stat(full_path).st_uid).pw_name
                    except: owner = "Unknown"
                    results.append((owner, sz_kb, full_path))
        except: pass
        return results

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "fc", "*"))
        outfeed_targets.extend(glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "innovus", "*")))
        if not outfeed_targets: outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*"))

        targets_map = {
            "WS (FE)":  glob.glob(os.path.join(BASE_WS_FE_DIR, "*")),
            "WS (BE)":  glob.glob(os.path.join(BASE_WS_BE_DIR, "*")),
            "OUTFEED":  outfeed_targets
        }

        tasks = []
        for cat, paths in targets_map.items():
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                tasks.append((cat, valid_paths[i:i+50]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                cat = future_to_cat[future]
                try:
                    for owner, sz_kb, full_path in future.result():
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


class ScannerWorker(QThread):
    finished        = pyqtSignal(dict, dict, dict, dict)
    progress_update = pyqtSignal(int, int)
    status_update   = pyqtSignal(str)

    def scan_ir_dir(self):
        ir_data = {}; target_lef = f"{PROJECT_PREFIX}.lef.list"; ir_dirs = BASE_IR_DIR.split()
        for ir_base in ir_dirs:
            if not os.path.exists(ir_base): continue
            for root_dir, dirs, files in os.walk(ir_base):
                for f_name in files:
                    if not f_name.startswith("redhawk.log"): continue
                    log_path = os.path.join(root_dir, f_name)
                    run_be_name = step_name = None
                    static_val = dynamic_val = "-"
                    in_static = in_dynamic = False
                    static_lines = []; dynamic_lines = []
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m: run_be_name = m.group(1); step_name = m.group(2)
                                if "Worst Static IR Drop:" in line:
                                    in_static = True; in_dynamic = False
                                    static_lines.append(line.rstrip()); continue
                                if "Worst Dynamic Voltage Drop:" in line:
                                    in_dynamic = True; in_static = False
                                    dynamic_lines.append(line.rstrip()); continue
                                if in_static:
                                    if line.startswith("****") or line.startswith("Finish"): in_static = False
                                    elif line.strip():
                                        static_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and static_val == "-": static_val = parts[1]
                                if in_dynamic:
                                    if line.startswith("****") or line.startswith("Finish"): in_dynamic = False
                                    elif line.strip():
                                        dynamic_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and dynamic_val == "-": dynamic_val = parts[1]
                        if run_be_name and step_name:
                            key = f"{run_be_name}/{step_name}"
                            if key not in ir_data:
                                ir_data[key] = {"static": "-", "dynamic": "-", "log": log_path, "static_table": "", "dynamic_table": ""}
                            if static_val != "-": ir_data[key]["static"] = static_val
                            if dynamic_val != "-": ir_data[key]["dynamic"] = dynamic_val
                            if static_lines: ir_data[key]["static_table"] = "\n".join(static_lines)
                            if dynamic_lines: ir_data[key]["dynamic_table"] = "\n".join(dynamic_lines)
                    except: pass
        return ir_data

    def _scan_single_workspace(self, ws_base, ws_name, tools_to_scan):
        tasks = []; releases_found = {}
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
                    disc_futures.append((disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan)))
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
                        be_runs = (glob.glob(os.path.join(evt_dir, "fc", "*-BE")) + glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")))
                        for rd in be_runs: tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                            if os.path.isdir(rd): tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

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
                            if result["run_type"] == "BE": self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            scan_stats['outfeed'] += 1
                            self._map_release(out_data, result["rtl"], result["path"])
                            
                        blk = result["block"]
                        if blk not in scan_stats['blocks']: scan_stats['blocks'][blk] = 0
                        scan_stats['blocks'][blk] += 1
                        if "/fc/" in result["path"]: scan_stats['fc'] += 1
                        elif "/innovus/" in result["path"]: scan_stats['innovus'] += 1
                except: pass
                completed_tasks += 1
                self.progress_update.emit(completed_tasks, total_tasks)
                if completed_tasks % 20 == 0: self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")
            ir_data = ir_future.result()

        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

    def _thread_process_run(self, task_tuple):
        b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
        if source == "OUTFEED":
            rtl = extract_rtl(rd)
            if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl): rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
            elif rtl == "Unknown": rtl = normalize_rtl(phys_evt)
            rtl = normalize_rtl(rtl)
        else:
            rtl = extract_rtl(rd) if run_type == "BE" else base_rtl
            if rtl == "Unknown": rtl = base_rtl
            
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
            if is_comp: fe_status = "COMPLETED"
            else:
                log_file = os.path.join(rd, "logs/compile_opt.log")
                if not cached_exists(log_file): fe_status = "NOT STARTED"
                else:
                    fe_status = "RUNNING"
                    try:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as lf:
                            for line in lf:
                                if "Stack trace for crashing thread" in line: fe_status = "FATAL ERROR"; break
                                if "Information: Process terminated by interrupt" in line: fe_status = "INTERRUPTED"; break
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
        self.setWindowTitle("Singularity PD | Enterprise Edition")
        self.resize(1920, 1000)

        # State Variables
        self.ws_data  = {}; self.out_data = {}; self.ir_data  = {}
        self.global_notes = load_all_notes()
        
        # Load Preferences
        self.is_dark_mode = prefs.getboolean('UI', 'dark_mode', fallback=False)
        self.row_spacing  = prefs.getint('UI', 'row_spacing', fallback=2)
        self.show_relative_time = prefs.getboolean('UI', 'rel_time', fallback=False)
        self.convert_to_ist = prefs.getboolean('UI', 'ist_time', fallback=False)
        self.hide_block_nodes = prefs.getboolean('UI', 'hide_blocks', fallback=False)
        
        starred_str = prefs.get('USER', 'starred_runs', fallback='')
        self.starred_runs = set([x for x in starred_str.split('|') if x])
        
        self.active_col_filters  = {}
        self.ignored_paths       = set()
        self._checked_paths      = set()
        self.size_workers        = []
        self._cached_disk_data   = None
        self.disk_worker         = None

        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        # Icon Generator
        self.star_icon = self._create_golden_dot()

        self.init_ui()
        self.apply_theme()
        
        # Start Initial Scans
        self.start_fs_scan()
        self.start_bg_disk_scan()

    def _create_golden_dot(self):
        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QBrush(QColor("#FFC107"))) # Golden amber
        painter.setPen(QPen(QColor("#FF9800"), 1))  # Slightly darker border
        painter.drawEllipse(4, 4, 8, 8)
        painter.end()
        return QIcon(pixmap)

    def closeEvent(self, event):
        if not prefs.has_section('UI'): prefs.add_section('UI')
        if not prefs.has_section('USER'): prefs.add_section('USER')
        
        prefs.set('UI', 'dark_mode', str(self.is_dark_mode))
        prefs.set('UI', 'row_spacing', str(self.row_spacing))
        prefs.set('UI', 'rel_time', str(self.show_relative_time))
        prefs.set('UI', 'ist_time', str(self.convert_to_ist))
        prefs.set('UI', 'hide_blocks', str(self.hide_block_nodes))
        prefs.set('USER', 'starred_runs', '|'.join(self.starred_runs))
        prefs.set('UI', 'main_splitter', ','.join(map(str, self.main_splitter.sizes())))
        prefs.set('UI', 'v_splitter', ','.join(map(str, self.v_splitter.sizes())))
        
        with open(USER_PREFS_FILE, 'w') as f: prefs.write(f)
        os._exit(0)

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(6, 6, 6, 4)

        # Top Toolbar
        top_layout = QHBoxLayout()
        self.src_combo = QComboBox(); self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.currentIndexChanged.connect(self.on_source_changed)
        
        self.rel_combo = QComboBox(); self.rel_combo.setMinimumWidth(180)
        self.rel_combo.currentIndexChanged.connect(self.refresh_view)
        
        self.view_combo = QComboBox(); self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Running Only", "Failed Only"])
        self.view_combo.currentIndexChanged.connect(self.refresh_view)
        
        self.search = QLineEdit(); self.search.setPlaceholderText("Search runs, notes... [Ctrl+F]")
        self.search.textChanged.connect(lambda: self.search_timer.start(500))
        
        self.refresh_btn = QPushButton("Refresh"); self.refresh_btn.clicked.connect(self.start_fs_scan)
        
        # Tools Menu
        self.tools_btn = QPushButton("Tools")
        self.tools_menu = QMenu(self)
        self.tools_menu.addAction("Fit Columns  [Ctrl+Shift+F]", self.fit_all_columns)
        self.tools_menu.addAction("Expand All   [Ctrl+E]",       lambda: self.tree.expandAll())
        self.tools_menu.addAction("Collapse All [Ctrl+W]",       lambda: self.tree.collapseAll())
        self.tools_menu.addSeparator()
        self.tools_menu.addAction("Calculate All Run Sizes",     self.calculate_all_sizes)
        self.tools_btn.setMenu(self.tools_menu)

        self.export_btn = QPushButton("Export CSV"); self.export_btn.clicked.connect(self.export_csv)
        self.qor_btn = QPushButton("Compare QoR"); self.qor_btn.clicked.connect(self.run_qor_comparison)
        
        # Mail Menu
        self.mail_btn = QPushButton("Send Mail")
        self.mail_menu = QMenu(self)
        self.mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        self.mail_menu.addAction("Send Compare QoR Mail", self.send_qor_mail_action)
        self.mail_menu.addAction("Send Custom Mail", self.send_custom_mail_action)
        self.mail_btn.setMenu(self.mail_menu)
        
        self.disk_btn = QPushButton("Disk Space"); self.disk_btn.clicked.connect(self.open_disk_usage)
        self.settings_btn = QPushButton("Settings"); self.settings_btn.clicked.connect(self.open_settings)
        
        top_layout.addWidget(QLabel("Source:")); top_layout.addWidget(self.src_combo)
        top_layout.addWidget(QLabel("RTL:")); top_layout.addWidget(self.rel_combo)
        top_layout.addWidget(QLabel("View:")); top_layout.addWidget(self.view_combo)
        top_layout.addWidget(self.search)
        top_layout.addStretch()
        top_layout.addWidget(self.refresh_btn)
        top_layout.addWidget(self.tools_btn)
        top_layout.addWidget(self.export_btn)
        top_layout.addWidget(self.qor_btn)
        top_layout.addWidget(self.mail_btn)
        top_layout.addWidget(self.disk_btn)
        top_layout.addWidget(self.settings_btn)
        root_layout.addLayout(top_layout)

        # Skeleton Loading Marquee
        self.loading_overlay = QProgressBar()
        self.loading_overlay.setFixedHeight(4)
        self.loading_overlay.setTextVisible(False)
        self.loading_overlay.setRange(0, 0)
        self.loading_overlay.setVisible(False)
        root_layout.addWidget(self.loading_overlay)

        # Splitters
        self.v_splitter = QSplitter(Qt.Vertical)
        self.main_splitter = QSplitter(Qt.Horizontal)

        # --- Left Panel (Blocks + Mini Stats) ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        self.blk_list = QListWidget()
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(100))
        
        left_layout.addWidget(QLabel("<b>Blocks</b>"))
        left_layout.addWidget(self.blk_list)
        
        self.mini_pie = MiniPieChart()
        left_layout.addWidget(QLabel("<b>Current View Health</b>"))
        left_layout.addWidget(self.mini_pie)
        self.main_splitter.addWidget(left_panel)

        # --- Center Panel (Tree) ---
        self.tree = QTreeWidget()
        self.tree.setColumnCount(24)
        headers = ["Run Name", "RTL Release Version", "Source", "Status", "Stage", "User", "Size", "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Dynamic IR", "Runtime", "Start", "End", "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes", "Starred_Hidden"]
        self.tree.setHeaderLabels(headers)
        
        # Hide unneeded path columns but show actual data columns
        for i in range(15, 24): self.tree.setColumnHidden(i, True)
        
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        self.tree.itemExpanded.connect(lambda: self.tree.resizeColumnToContents(0))
        self.tree.itemCollapsed.connect(lambda: self.tree.resizeColumnToContents(0))
        self.tree.itemChanged.connect(self._on_item_check_changed)
        self.main_splitter.addWidget(self.tree)

        # --- Right Panel (Inspector) ---
        self.inspector = QGroupBox("Run Details & Notes")
        ins_layout = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view details.")
        self.ins_lbl.setWordWrap(True)
        self.ins_note = QTextEdit()
        self.ins_note.setPlaceholderText("Enter aliases or personal notes here...\n\nVisible to all dashboard users.")
        self.ins_save_btn = QPushButton("Save Note")
        self.ins_save_btn.clicked.connect(self.save_inspector_note)
        
        ins_layout.addWidget(self.ins_lbl)
        ins_layout.addWidget(QLabel("<b>Shared Notes:</b>"))
        ins_layout.addWidget(self.ins_note)
        ins_layout.addWidget(self.ins_save_btn)
        self.main_splitter.addWidget(self.inspector)

        self.v_splitter.addWidget(self.main_splitter)
        
        # Log Viewer Dock
        self.log_viewer = LogTailer()
        self.log_viewer.hide()
        self.v_splitter.addWidget(self.log_viewer)

        root_layout.addWidget(self.v_splitter)

        try:
            m_sizes = [int(x) for x in prefs.get('UI', 'main_splitter', fallback='250,1200,300').split(',')]
            self.main_splitter.setSizes(m_sizes)
            v_sizes = [int(x) for x in prefs.get('UI', 'v_splitter', fallback='800,200').split(',')]
            self.v_splitter.setSizes(v_sizes)
        except: pass

        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.sb_stats = QLabel(""); self.status_bar.addWidget(self.sb_stats)

    # ------------------------------------------------------------------
    # ACTIONS & FEATURES
    # ------------------------------------------------------------------
    def apply_theme(self):
        pad = self.row_spacing
        if self.is_dark_mode:
            self.setStyleSheet(f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2d30; color: #dfe1e5; }}
                QTreeWidget, QListWidget {{ background-color: #1e1f22; alternate-background-color: #26282b; border: 1px solid #393b40; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QHeaderView::section {{ background-color: #2b2d30; padding: 4px; border: 1px solid #1e1f22; }}
                QLineEdit, QTextEdit, QPlainTextEdit, QComboBox {{ background-color: #1e1f22; border: 1px solid #43454a; padding: 2px;}}
                QPushButton {{ background-color: #393b40; border: 1px solid #43454a; padding: 4px; }}
                QPushButton:hover {{ background-color: #43454a; }}
                QGroupBox {{ border: 1px solid #43454a; margin-top: 10px; }}
                QGroupBox::title {{ subcontrol-origin: margin; subcontrol-position: top left; padding: 0 3px; }}
                QTreeView::indicator:checked {{ background-color: #4CAF50; border: 1px solid #388E3C; image: none; }}
                QTreeView::indicator:unchecked {{ background-color: white; border: 1px solid gray; }}
            """)
        else:
            self.setStyleSheet(f"""
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::indicator:checked {{ background-color: #4CAF50; border: 1px solid #388E3C; image: none; }}
                QTreeView::indicator:unchecked {{ background-color: white; border: 1px solid gray; }}
            """)

    def open_settings(self):
        dlg = SettingsDialog(self)
        if dlg.exec_():
            font = dlg.font_combo.currentFont()
            font.setPointSize(dlg.size_spin.value())
            QApplication.setFont(font)
            self.is_dark_mode = dlg.theme_cb.isChecked()
            self.row_spacing = dlg.space_spin.value()
            self.show_relative_time = dlg.rel_time_cb.isChecked()
            self.convert_to_ist = dlg.ist_cb.isChecked()
            self.hide_block_nodes = dlg.hide_blocks_cb.isChecked()
            self.apply_theme()
            self.refresh_view()

    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i): self.tree.resizeColumnToContents(i)
        self.tree.setUpdatesEnabled(True)

    def calculate_all_sizes(self):
        size_tasks = []
        self.item_map = {}
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
        item = getattr(self, 'item_map', {}).get(item_id)
        if item: item.setText(6, size_str)

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "dashboard_export.csv", "CSV Files (*.csv)")
        if not path: return
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                headers = [self.tree.headerItem().text(i) for i in range(15)] + ["Alias / Notes"]
                writer.writerow(headers)
                def write_node(node):
                    if not node.isHidden() and node.text(0) and "[ Ignored" not in node.text(0):
                        row = [node.text(i) for i in range(15)] + [node.text(22)]
                        writer.writerow(row)
                    for i in range(node.childCount()): write_node(node.child(i))
                write_node(self.tree.invisibleRootItem())
            QMessageBox.information(self, "Export Successful", f"Data exported to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to export: {e}")

    # ------------------------------------------------------------------
    # DISK SCANNING
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
        self.disk_dialog = DiskUsageDialog(self._cached_disk_data, self.is_dark_mode, self)
        self.disk_dialog.exec_()

    # ------------------------------------------------------------------
    # MAIL ACTIONS
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        user_runs = {}
        def find_owners(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    path  = c.text(15); owner = c.text(5)
                    if path and path != "N/A" and owner and owner != "Unknown":
                        if owner not in user_runs: user_runs[owner] = []
                        user_runs[owner].append(path)
                find_owners(c)
        find_owners(self.tree.invisibleRootItem())

        if not user_runs:
            QMessageBox.warning(self, "No Runs Selected", "Please select at least one run to send a cleanup mail.")
            return

        all_known = get_all_known_mail_users()
        unique_emails = []
        for owner in user_runs.keys():
            e = get_user_email(owner)
            if e: unique_emails.append(e)
            
        dlg = AdvancedMailDialog("Action Required: Please clean up heavy disk usage runs",
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
                try: subprocess.Popen(cmd); success_count += 1
                except: pass
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} emails.")
            
    def send_qor_mail_action(self):
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Latest Compare QoR Report", "Hi Team,\n\nPlease find the attached latest QoR Report for your reference.\n\nRegards", all_known, "", self)
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
            except: pass

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
            try: subprocess.Popen(cmd); QMessageBox.information(self, "Mail Sent", "Successfully sent the custom mail.")
            except: pass

    def run_qor_comparison(self):
        sel = []
        def get_checked(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    qp  = c.text(15); src = c.text(2)
                    if src == "OUTFEED" and qp: qp = os.path.dirname(qp)
                    if qp and not qp.endswith("/"): qp += "/"
                    sel.append(qp)
                get_checked(c)
        get_checked(self.tree.invisibleRootItem())
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = find_latest_qor_report()
        if h: subprocess.Popen([FIREFOX_PATH, h])

    # ------------------------------------------------------------------
    # SKELETON LOADING & SCANNING
    # ------------------------------------------------------------------
    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning(): return
        self.loading_overlay.setVisible(True)
        self.tree.setEnabled(False)
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Scanning...")
        
        self.worker = ScannerWorker()
        self.worker.status_update.connect(lambda s: self.status_bar.showMessage(s))
        self.worker.finished.connect(self.on_scan_finished)
        self.worker.start()

    def on_scan_finished(self, ws, out, ir, stats):
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.global_notes = load_all_notes()
        
        self.loading_overlay.setVisible(False)
        self.tree.setEnabled(True)
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self.status_bar.clearMessage()
        
        # Populate mail users list
        all_owners = set()
        for r in self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", []):
            if r.get("owner") and r["owner"] != "Unknown": all_owners.add(r["owner"])
        if all_owners: save_mail_users_config(all_owners)
        
        self.on_source_changed() # Trigger full rebuild

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
        saved_states = {self.blk_list.item(i).data(Qt.UserRole): self.blk_list.item(i).checkState() for i in range(self.blk_list.count())}

        self.rel_combo.blockSignals(True); self.rel_combo.clear()
        valid = [r for r in releases if "Unknown" not in r and get_milestone(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(valid)
        self.rel_combo.addItems(new_releases)
        self.rel_combo.setCurrentText(current_rtl if current_rtl in new_releases else "[ SHOW ALL ]")
        self.rel_combo.blockSignals(False)

        self.blk_list.blockSignals(True); self.blk_list.clear()
        for b in sorted(blocks):
            it = QListWidgetItem(b); it.setData(Qt.UserRole, b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    # ------------------------------------------------------------------
    # INSPECTOR (RIGHT PANEL) LOGIC
    # ------------------------------------------------------------------
    def on_tree_selection_changed(self):
        sel = self.tree.selectedItems()
        if not sel:
            self.ins_lbl.setText("Select a run to view details.")
            self.ins_note.clear()
            self.ins_note.setEnabled(False)
            self.ins_save_btn.setEnabled(False)
            return

        item = sel[0]
        run_name = item.text(0)
        rtl = item.text(1)
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl = item.data(0, Qt.UserRole) == "RTL"
        path = item.text(15)

        self.ins_note.setEnabled(True)
        self.ins_save_btn.setEnabled(True)

        if is_stage:
            p_name = item.parent().text(0)
            self.ins_lbl.setText(f"<b>Stage:</b> {run_name}<br><b>Parent:</b> {p_name}<br><b>Path:</b> {path}")
            self._current_note_id = f"{item.parent().text(1)} : {p_name}"
        elif is_rtl:
            self.ins_lbl.setText(f"<b>RTL Release:</b> {run_name}")
            self._current_note_id = run_name
        else:
            self.ins_lbl.setText(f"<b>Run:</b> {run_name}<br><b>RTL:</b> {rtl}<br><b>Path:</b> {path}")
            self._current_note_id = f"{rtl} : {run_name}"

        notes = self.global_notes.get(self._current_note_id, [])
        clean_text = "\n".join(notes)
        tag = f"[{getpass.getuser()}]"
        for line in notes:
            if line.startswith(tag):
                clean_text = line.replace(tag, "").strip(); break
                
        self.ins_note.setPlainText(clean_text)

    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'): return
        txt = self.ins_note.toPlainText()
        save_user_note(self._current_note_id, txt)
        self.global_notes = load_all_notes()
        self.refresh_view()
        self.on_tree_selection_changed()

    # ------------------------------------------------------------------
    # CONTEXT MENUS (LOGS, STARS, GANTT)
    # ------------------------------------------------------------------
    def _on_item_check_changed(self, item, col=0):
        if col != 0: return
        path = item.text(15)
        if not path or path == "N/A": return
        hl_color = QColor("#2f65ca" if self.is_dark_mode else "#e3f2fd")
        if item.checkState(0) == Qt.Checked:
            self._checked_paths.add(path)
            for c in range(self.tree.columnCount()): item.setBackground(c, hl_color)
        else:
            self._checked_paths.discard(path)
            for c in range(self.tree.columnCount()): item.setBackground(c, QColor(0, 0, 0, 0))

    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item: return
        m = QMenu()
        
        run_path = item.text(15)
        log_path = item.text(16)
        is_run = bool(run_path and run_path != "N/A")
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        
        if is_run or is_stage:
            star_act = m.addAction("Unpin Run" if run_path in self.starred_runs else "Pin Run (Golden Dot)")
            m.addSeparator()
            tail_act = m.addAction("Tail Log in Dashboard") if log_path else None
            m.addSeparator()
            gantt_act = None
            if item.childCount() > 0 and item.child(0).data(0, Qt.UserRole) == "STAGE":
                gantt_act = m.addAction("Show Timeline (Gantt Chart)")
                
            res = m.exec_(self.tree.viewport().mapToGlobal(pos))
            if not res: return
            
            if res == star_act:
                if run_path in self.starred_runs: self.starred_runs.remove(run_path)
                else: self.starred_runs.add(run_path)
                self.refresh_view()
                
            elif tail_act and res == tail_act:
                self.log_viewer.tail_file(log_path)
                
            elif gantt_act and res == gantt_act:
                stages = []
                for i in range(item.childCount()):
                    c = item.child(i)
                    if c.data(0, Qt.UserRole) == "STAGE":
                        rt = c.text(12)
                        stages.append({'name': c.text(0), 'time_str': rt, 'sec': time_to_seconds(rt)})
                dlg = GanttChartDialog(item.text(0), stages, self)
                dlg.exec_()

    # ------------------------------------------------------------------
    # COLORING HELPERS
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
        if "Error" in val and "Error: 0" not in val: item.setForeground(col, QColor("#d32f2f" if not self.is_dark_mode else "#e57373"))
        elif "Error: 0" in val: item.setForeground(col, QColor("#388e3c" if not self.is_dark_mode else "#81c784"))


    # ------------------------------------------------------------------
    # TREE POPULATION (FULL DATA)
    # ------------------------------------------------------------------
    def _get_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.childCount()):
            if parent.child(i).text(0) == text: return parent.child(i)
        p = CustomTreeItem(parent)
        p.setText(0, text); p.setData(0, Qt.UserRole, node_type); p.setExpanded(True)
        if node_type == "MILESTONE":
            p.setForeground(0, QColor("#1e88e5" if not self.is_dark_mode else "#64b5f6"))
            f = p.font(0); f.setBold(True); p.setFont(0, f)
        elif node_type == "RTL":
            f = p.font(0); f.setItalic(True); p.setFont(0, f)
            if text in self.global_notes:
                notes = " | ".join(self.global_notes[text])
                p.setText(22, notes); p.setForeground(22, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        return p

    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        child.setCheckState(0, Qt.Checked if run["path"] in self._checked_paths else Qt.Unchecked)

        child.setText(0, run["r_name"]); child.setText(1, run["rtl"]); child.setText(2, run["source"])
        child.setText(5, run.get("owner", "Unknown"))
        child.setData(0, Qt.UserRole + 2, run["block"]); child.setData(0, Qt.UserRole + 4, run["r_name"].replace("-FE", "").replace("-BE", ""))

        if run["source"] == "OUTFEED": child.setForeground(2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))
        else: child.setForeground(2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        if run["run_type"] == "FE":
            status_str = run["fe_status"]
            child.setText(3, status_str); child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-")
            child.setText(7, f"NONUPF - {run['st_n']}"); child.setText(8, f"UPF - {run['st_u']}"); child.setText(9, run["vslp_status"])
            child.setText(12, run["info"]["runtime"])
            
            s_raw = convert_kst_to_ist_str(run["info"]["start"]) if self.convert_to_ist else run["info"]["start"]
            e_raw = convert_kst_to_ist_str(run["info"]["end"]) if self.convert_to_ist else run["info"]["end"]
            child.setText(13, relative_time(s_raw) if self.show_relative_time else s_raw)
            child.setText(14, relative_time(e_raw) if self.show_relative_time and run["is_comp"] else ("-" if not run["is_comp"] else e_raw))

            child.setText(15, run["path"]); child.setText(16, os.path.join(run["path"], "logs/compile_opt.log"))
            child.setText(17, run["fm_u_path"]); child.setText(18, run["fm_n_path"]); child.setText(19, run["vslp_rpt_path"])
            
            self._apply_status_color(child, 3, status_str)
            self._apply_fm_color(child, 7, run["st_n"]); self._apply_fm_color(child, 8, run["st_u"])
            self._apply_vslp_color(child, 9, run["vslp_status"])
        else:
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-"); child.setText(15, run["path"])

        for i in range(1, 23): child.setTextAlignment(i, Qt.AlignCenter)
        child.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter); child.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

        run_id = f"{run['rtl']} : {run['r_name']}"
        if run_id in self.global_notes:
            notes = " | ".join(self.global_notes[run_id])
            child.setText(22, notes); child.setForeground(22, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        return child

    def _add_stages(self, be_item, be_run, ign_root):
        is_ir_block = (be_run["block"].upper() == PROJECT_PREFIX.upper())
        owner = be_run.get("owner", "Unknown")

        for stage in be_run["stages"]:
            st_item = CustomTreeItem(ign_root if stage["stage_path"] in self.ignored_paths else be_item)
            st_item.setFlags(st_item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            st_item.setCheckState(0, Qt.Unchecked)
            st_item.setData(0, Qt.UserRole, "STAGE"); st_item.setData(1, Qt.UserRole, stage["name"]); st_item.setData(2, Qt.UserRole, stage["qor_path"])

            ir_key  = f"{be_run['r_name']}/{stage['name']}"
            ir_info = self.ir_data.get(ir_key, {"static": "-", "dynamic": "-", "log": "", "static_table": "", "dynamic_table": ""})

            stage_status = "COMPLETED"
            if be_run["source"] == "WS" and not cached_exists(stage["rpt"]):
                stage_status = "RUNNING"
                if cached_exists(stage["log"]):
                    try:
                        with open(stage["log"], 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "START_CMD:" in line: stage_status = line.strip(); break
                    except: pass

            st_item.setText(0, stage["name"]); st_item.setText(2, be_run["source"]); st_item.setText(4, stage_status); st_item.setText(5, owner)
            st_item.setText(6, "-")
            st_item.setText(7, f"NONUPF - {stage['st_n']}"); st_item.setText(8, f"UPF - {stage['st_u']}"); st_item.setText(9, stage["vslp_status"])
            st_item.setText(10, ir_info["static"] if is_ir_block else "-"); st_item.setText(11, ir_info["dynamic"] if is_ir_block else "-")
            st_item.setText(12, stage["info"]["runtime"])

            s_raw = convert_kst_to_ist_str(stage["info"]["start"]) if self.convert_to_ist else stage["info"]["start"]
            e_raw = convert_kst_to_ist_str(stage["info"]["end"]) if self.convert_to_ist else stage["info"]["end"]
            st_item.setText(13, relative_time(s_raw) if self.show_relative_time else s_raw)
            st_item.setText(14, relative_time(e_raw) if self.show_relative_time else e_raw)

            st_item.setText(15, stage["stage_path"]); st_item.setText(16, stage["log"])
            st_item.setText(17, stage["fm_u_path"]);  st_item.setText(18, stage["fm_n_path"])
            st_item.setText(19, stage["vslp_rpt_path"]); st_item.setText(20, stage["sta_rpt_path"]); st_item.setText(21, ir_info["log"])

            self._apply_fm_color(st_item, 7, stage["st_n"]); self._apply_fm_color(st_item, 8, stage["st_u"])
            self._apply_vslp_color(st_item, 9, stage["vslp_status"])
            self._apply_status_color(st_item, 4, stage_status if stage_status in ("COMPLETED","RUNNING","FAILED") else "RUNNING")

            for i in range(1, 23): st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter); st_item.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

    def refresh_view(self):
        for w in self.size_workers:
            if hasattr(w, 'cancel'): w.cancel()
            
        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        self.tree.setSortingEnabled(False)
        self.tree.clear()

        src_mode   = self.src_combo.currentText()
        sel_rtl    = self.rel_combo.currentText()
        search_pattern = f"*{self.search.text().lower().strip()}*" if self.search.text() else "*"

        checked_blks = [self.blk_list.item(i).data(Qt.UserRole) for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]

        runs_to_process = []
        if src_mode in ["WS", "ALL"]: runs_to_process.extend(self.ws_data.get("all_runs", []))
        if src_mode in ["OUTFEED","ALL"]: runs_to_process.extend(self.out_data.get("all_runs", []))

        # Synchronize BE runs RTL with their FE parent
        fe_info = {}
        for run in runs_to_process:
            if run["run_type"] == "FE":
                fe_base = run["r_name"].replace("-FE", "")
                fe_info[(run["block"], fe_base)] = run["rtl"]

        for run in runs_to_process:
            if run["run_type"] == "BE":
                clean_be = run["r_name"].replace("-BE", "")
                for (blk, fe_base), fe_rtl in fe_info.items():
                    if run["block"] == blk and (clean_be == fe_base or f"_{fe_base}_" in clean_be or clean_be.startswith(f"{fe_base}_") or clean_be.endswith(f"_{fe_base}")):
                        run["rtl"] = fe_rtl; break

        normal_runs_list, ignored_runs_list = [], []
        for run in runs_to_process:
            if run["path"] in self.ignored_paths: ignored_runs_list.append(run); continue
            if run["block"] not in checked_blks: continue
            
            base_rtl_filter = re.sub(r'_syn\d+$', '', run["rtl"])
            if get_milestone(base_rtl_filter) is None: continue
            if sel_rtl != "[ SHOW ALL ]" and not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")): continue

            preset = self.view_combo.currentText()
            if preset == "FE Only"      and run["run_type"] != "FE": continue
            if preset == "BE Only"      and run["run_type"] != "BE": continue
            if preset == "Running Only" and not (run["run_type"] == "FE" and not run["is_comp"]): continue
            if preset == "Failed Only"  and not ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","") or run.get("fe_status") == "FATAL ERROR"): continue

            if search_pattern != "**":
                note_id = f"{run['rtl']} : {run['r_name']}"
                notes = " ".join(self.global_notes.get(note_id, []))
                combined = f"{run['r_name']} {run['rtl']} {notes} {run['st_n']} {run['st_u']} {run['vslp_status']} {run['info']['runtime']}".lower()
                if not fnmatch.fnmatch(combined, search_pattern): continue

            normal_runs_list.append(run)

        # Build Tree
        root = self.tree.invisibleRootItem()
        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
        
        fe_runs = [r for r in normal_runs_list if r["run_type"] == "FE"]
        be_runs = [r for r in normal_runs_list if r["run_type"] == "BE"]

        for run in fe_runs:
            base_rtl = re.sub(r'_syn\d+$', '', run["rtl"])
            base_attach_node = root if self.hide_block_nodes else self._get_node(root, run["block"], "BLOCK")
            if sel_rtl == "[ SHOW ALL ]":
                m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
                parent_for_run = self._get_node(m_node, base_rtl, "RTL")
            elif sel_rtl == base_rtl and (run["rtl"] != base_rtl):
                parent_for_run = self._get_node(base_attach_node, run["rtl"], "RTL")
            else: parent_for_run = base_attach_node
            self._create_run_item(parent_for_run, run)

        for be_run in be_runs:
            base_rtl = re.sub(r'_syn\d+$', '', be_run["rtl"])
            base_attach_node = root if self.hide_block_nodes else self._get_node(root, be_run["block"], "BLOCK")
            if sel_rtl == "[ SHOW ALL ]":
                m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
                parent_for_run = self._get_node(m_node, base_rtl, "RTL")
            elif sel_rtl == base_rtl and (be_run["rtl"] != base_rtl):
                parent_for_run = self._get_node(base_attach_node, be_run["rtl"], "RTL")
            else: parent_for_run = base_attach_node

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
                base_attach_node = ign_root if self.hide_block_nodes else self._get_node(ign_root, run["block"], "BLOCK")
                item = self._create_run_item(base_attach_node, run)
                if run["run_type"] == "BE": self._add_stages(item, run, ign_root)

        if ign_root.childCount() == 0: root.removeChild(ign_root)

        # Update Pie Chart & Golden Dot Stars
        p = f = r = 0
        def update_nodes(node):
            nonlocal p, f, r
            if node.data(0, Qt.UserRole) not in ["BLOCK", "RTL", "MILESTONE"]:
                st = node.text(3).upper()
                if "COMPLETED" in st: p += 1
                elif "FAILED" in st or "FATAL" in st: f += 1
                elif "RUNNING" in st: r += 1
                
                if node.text(15) in self.starred_runs:
                    node.setIcon(0, self.star_icon)
                    node.setData(0, Qt.UserRole + 5, True) 
                else:
                    node.setIcon(0, QIcon())
                    node.setData(0, Qt.UserRole + 5, False)
                    
            for i in range(node.childCount()): update_nodes(node.child(i))
            
        update_nodes(root)
        
        self.mini_pie.update_data(p, f, r, self.is_dark_mode)
        self.sb_stats.setText(f"  Total: {p+f+r} | Completed: {p} | Running: {r} | Failed: {f}")

        self.tree.setSortingEnabled(True)
        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)
        self.on_tree_selection_changed()

# ===========================================================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.show()
    sys.exit(app.exec_())
