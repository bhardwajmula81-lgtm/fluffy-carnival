# -*- coding: utf-8 -*-
# Singularity PD | Pro Edition -- main.py
# Pure ASCII comments only (Python 3.6 compatible on Linux)

import os
import re
import sys
import fnmatch
import subprocess
import csv
import json
import pwd
import math
import time
import shutil
import threading
import datetime
import getpass
import configparser
import concurrent.futures
import tempfile

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QTreeWidget, QTreeWidgetItem,
    QPushButton, QMessageBox, QListWidget, QListWidgetItem,
    QProgressBar, QMenu, QSplitter, QWidgetAction, QCheckBox,
    QStatusBar, QFrame, QShortcut, QToolButton, QStyle,
    QHeaderView, QFileDialog, QGroupBox, QTextEdit, QDockWidget,
    QFormLayout, QDialog, QDialogButtonBox, QFontComboBox,
    QSpinBox, QColorDialog, QTabWidget, QTableWidget,
    QTableWidgetItem, QScrollArea, QAbstractItemView
)
from PyQt5.QtCore import Qt, QTimer, QDateTime, pyqtSignal, QThread, QDate, QPoint
from PyQt5.QtWidgets import QDateEdit as _QDateEditImport
from PyQt5.QtGui import (QColor, QFont, QKeySequence, QBrush,
                         QPainter, QPen, QPixmap, QIcon, QPolygon)

# ===========================================================================
# CONFIG + MAIL HELPERS (module-level, loaded once at startup)
# ===========================================================================

def _load_project_config():
    """Load project_config.ini if present, else use hardcoded defaults."""
    import configparser as _cp
    cfg = _cp.ConfigParser()
    cfg_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "project_config.ini")
    defaults = {
        'PROJECT': {
            'PROJECT_PREFIX':   'S5K2P5SP',
            'BASE_WS_FE_DIR':   '/user/s5k2p5sx.fe1/s5k2p5sp/WS',
            'BASE_WS_BE_DIR':   '/user/s5k2p5sx.be1/s5k2p5sp/WS',
            'BASE_OUTFEED_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/outfeed',
            'BASE_IR_DIR':      '/user/s5k2p5sx.be1/LAYOUT/IR',
            'BLOCKS':           '',
        },
        'PERFORMANCE': {
            'SCAN_IR_ON_START':      'false',
            'SCAN_OWNER_ON_START':   'false',
            'SCAN_SIGNOFF_ON_START': 'false',
            'AUTO_SIZE_ON_START':    'false',
            'BACKGROUND_SIGNOFF_AFTER_SCAN': 'true',
            'SIGNOFF_BG_WORKERS': '6',
        },
        'TOOLS': {
            'PNR_TOOL_NAMES':  'fc innovus',
            'SUMMARY_SCRIPT':  '',
            'FIREFOX_PATH':    '/usr/bin/firefox',
            'MAIL_UTIL':       '/user/vwpmailsystem/MAIL/send_mail_for_rhel7',
            'USER_INFO_UTIL':  '/usr/local/bin/user_info',
            'PYTHON_BIN':      'python3.6',
        }
    }
    if os.path.exists(cfg_file):
        cfg.read(cfg_file)
    else:
        cfg.read_dict(defaults)
        try:
            with open(cfg_file, 'w') as f:
                cfg.write(f)
        except Exception:
            pass
    return cfg

def _load_mail_config():
    import configparser as _cp
    mc = _cp.ConfigParser()
    mc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "mail_users.ini")
    if not os.path.exists(mc_file):
        mc.read_dict({'PERMANENT_MEMBERS': {'always_to': '',
                                             'always_cc': ''},
                       'KNOWN_USERS':       {'users': ''}})
        try:
            with open(mc_file, 'w') as f:
                mc.write(f)
        except Exception:
            pass
    else:
        mc.read(mc_file)
    return mc, mc_file

_proj_cfg   = _load_project_config()
mail_config, _MAIL_USERS_FILE = _load_mail_config()

MAIL_UTIL      = _proj_cfg.get('TOOLS', 'MAIL_UTIL',      fallback='')
FIREFOX_PATH   = _proj_cfg.get('TOOLS', 'FIREFOX_PATH',   fallback='/usr/bin/firefox')
USER_INFO_UTIL = _proj_cfg.get('TOOLS', 'USER_INFO_UTIL', fallback='/usr/local/bin/user_info')
_PYTHON_BIN    = _proj_cfg.get('TOOLS', 'PYTHON_BIN',     fallback='python3.6')
_SUMMARY_SCRIPT = _proj_cfg.get('TOOLS', 'SUMMARY_SCRIPT', fallback='')

# Project paths -- defined here so they are available even without config.py
SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
USER_PREFS_FILE  = os.path.join(SCRIPT_DIR, 'user_prefs.ini')
NOTES_DIR        = os.path.join(SCRIPT_DIR, 'dashboard_notes')
PROJECT_PREFIX   = _proj_cfg.get('PROJECT', 'PROJECT_PREFIX',   fallback='S5K2P5SP')
BASE_WS_FE_DIR   = _proj_cfg.get('PROJECT', 'BASE_WS_FE_DIR',   fallback='')
BASE_WS_BE_DIR   = _proj_cfg.get('PROJECT', 'BASE_WS_BE_DIR',   fallback='')
BASE_OUTFEED_DIR = _proj_cfg.get('PROJECT', 'BASE_OUTFEED_DIR', fallback='')
BASE_IR_DIR      = _proj_cfg.get('PROJECT', 'BASE_IR_DIR',      fallback='')
PNR_TOOL_NAMES   = _proj_cfg.get('TOOLS',   'PNR_TOOL_NAMES',   fallback='fc innovus')
SCAN_IR_ON_START      = _proj_cfg.getboolean('PERFORMANCE', 'SCAN_IR_ON_START',      fallback=False)
SCAN_OWNER_ON_START   = _proj_cfg.getboolean('PERFORMANCE', 'SCAN_OWNER_ON_START',   fallback=False)
SCAN_SIGNOFF_ON_START = _proj_cfg.getboolean('PERFORMANCE', 'SCAN_SIGNOFF_ON_START', fallback=False)
AUTO_SIZE_ON_START    = _proj_cfg.getboolean('PERFORMANCE', 'AUTO_SIZE_ON_START',    fallback=False)
BACKGROUND_SIGNOFF_AFTER_SCAN = _proj_cfg.getboolean(
    'PERFORMANCE', 'BACKGROUND_SIGNOFF_AFTER_SCAN', fallback=True)
SIGNOFF_BG_WORKERS = _proj_cfg.getint('PERFORMANCE', 'SIGNOFF_BG_WORKERS', fallback=6)
_blocks_raw      = _proj_cfg.get('PROJECT', 'BLOCKS',           fallback='')
BLOCKS           = set(b.strip() for b in _blocks_raw.split(',') if b.strip())

# Load user preferences
prefs = configparser.ConfigParser()
if os.path.exists(USER_PREFS_FILE):
    prefs.read(USER_PREFS_FILE)

# Ensure notes dir exists
if not os.path.exists(NOTES_DIR):
    try:
        os.makedirs(NOTES_DIR)
    except Exception:
        pass

# Expose constants to builtins so workers.py lazy-resolution works
import builtins as _bt
_bt.BASE_WS_FE_DIR   = BASE_WS_FE_DIR
_bt.BASE_WS_BE_DIR   = BASE_WS_BE_DIR
_bt.BASE_OUTFEED_DIR = BASE_OUTFEED_DIR
_bt.BASE_IR_DIR      = BASE_IR_DIR
_bt.PROJECT_PREFIX   = PROJECT_PREFIX
_bt.PNR_TOOL_NAMES   = PNR_TOOL_NAMES
_bt.BLOCKS           = BLOCKS
_bt.SCAN_IR_ON_START      = SCAN_IR_ON_START
_bt.SCAN_OWNER_ON_START   = SCAN_OWNER_ON_START
_bt.SCAN_SIGNOFF_ON_START = SCAN_SIGNOFF_ON_START
_bt.AUTO_SIZE_ON_START    = AUTO_SIZE_ON_START
_bt.BACKGROUND_SIGNOFF_AFTER_SCAN = BACKGROUND_SIGNOFF_AFTER_SCAN
_bt.SIGNOFF_BG_WORKERS = SIGNOFF_BG_WORKERS


def _get_user_email(username):
    username = username.strip()
    if not username or username == "Unknown": return ""
    if "@" in username: return username
    try:
        res = subprocess.check_output(
            [USER_INFO_UTIL, '-a', username],
            stderr=subprocess.DEVNULL).decode('utf-8', errors='ignore')
        # user_info output is comma-separated; email is field 9 (index 8)
        fields = res.split(',')
        if len(fields) >= 9:
            email = fields[8].strip()
            if '@' in email:
                return email
        # fallback: regex scan
        m = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', res)
        if m: return m.group(0)
    except Exception:
        pass
    return ""


def _split_mail_tokens(text):
    return [x.strip() for x in re.split(r'[,;\s]+', text or '') if x.strip()]


def _resolve_mail_recipients(tokens):
    resolved = []
    unresolved = []
    for token in tokens:
        if "@" in token:
            resolved.append(token)
        else:
            email = _get_user_email(token)
            if email:
                resolved.append(email)
            else:
                unresolved.append(token)
    seen = set()
    uniq = []
    for email in resolved:
        key = email.lower()
        if key not in seen:
            seen.add(key)
            uniq.append(email)
    return uniq, unresolved


def _write_mail_body_file(body, fmt):
    ext = ".html" if fmt == "html" else ".txt"
    path = os.path.join(
        tempfile.gettempdir(),
        "singularity_pd_mail_{}_{}{}".format(
            os.getpid(), int(time.time() * 1000), ext))
    with open(path, "w", encoding="utf-8") as f:
        f.write(body or "")
    return path


def _get_all_known_mail_users():
    try:
        mail_config.read(_MAIL_USERS_FILE)
        s = mail_config.get('KNOWN_USERS', 'users', fallback='')
        return sorted(set(u.strip() for u in s.split(',') if u.strip()))
    except Exception:
        return []


def _save_mail_users(new_users):
    try:
        existing = set(_get_all_known_mail_users())
        existing.update(new_users)
        if not mail_config.has_section('KNOWN_USERS'):
            mail_config.add_section('KNOWN_USERS')
        mail_config.set('KNOWN_USERS', 'users',
                         ', '.join(sorted(existing)))
        with open(_MAIL_USERS_FILE, 'w') as f:
            mail_config.write(f)
    except Exception:
        pass


# ===========================================================================
# PIN PERSISTENCE -- inline so no dependency on utils.py version
# ===========================================================================
import getpass as _getpass

# NOTES_DIR may come from utils.py; define fallback here
try:
    _ = NOTES_DIR
except NameError:
    NOTES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "dashboard_notes")
    if not os.path.exists(NOTES_DIR):
        try: os.makedirs(NOTES_DIR)
        except Exception: pass

def _get_pins_file():
    import os as _os
    return _os.path.join(NOTES_DIR, f"pins_{_getpass.getuser()}.json")

def load_user_pins():
    fp = _get_pins_file()
    if os.path.exists(fp):
        try:
            with open(fp, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_user_pins(pins_dict):
    try:
        with open(_get_pins_file(), 'w') as f:
            json.dump(pins_dict, f, indent=4)
    except Exception:
        pass


def _send_mail_via_util(dlg):
    """Fire MAIL_UTIL subprocess from an AdvancedMailDialog."""
    if not MAIL_UTIL:
        QMessageBox.warning(
            None, "Mail Not Configured",
            "MAIL_UTIL is not set.\n"
            "Add it to project_config.ini:\n\n"
            "MAIL_UTIL = /user/vwpmailsystem/MAIL/send_mail_for_rhel7")
        return
    subject  = dlg.subject_input.text().strip()
    # Prefer raw HTML body if dialog stored one (e.g. from BlockSummaryDialog._send_mail)
    if hasattr(dlg, '_html_body') and dlg._html_body:
        body = dlg._html_body
        fmt  = "html"
    else:
        body = dlg.body_input.toPlainText()
        fmt  = "text"
    to_raw   = _split_mail_tokens(dlg.to_input.text())
    cc_raw   = _split_mail_tokens(dlg.cc_input.text())
    to_list, bad_to = _resolve_mail_recipients(to_raw)
    cc_list, bad_cc = _resolve_mail_recipients(cc_raw)
    unresolved = bad_to + bad_cc
    if unresolved:
        QMessageBox.warning(
            None, "Mail User Lookup Failed",
            "Could not resolve these user IDs to email addresses:\n\n" +
            ", ".join(unresolved) +
            "\n\nUse full email addresses or check USER_INFO_UTIL.")
        return
    sender   = _get_user_email(getpass.getuser()) or f"{getpass.getuser()}@samsung.com"
    if not to_list and not cc_list:
        QMessageBox.warning(None, "No Recipients",
                             "Please add at least one email address in To or CC.")
        return
    _save_mail_users(to_raw + cc_raw + to_list + cc_list)
    cmd = [MAIL_UTIL, "-sd", sender, "-s", subject, "-fm", fmt]
    if fmt == "html":
        try:
            cmd.extend(["-f", _write_mail_body_file(body, fmt)])
        except Exception as e:
            QMessageBox.warning(None, "Mail Body Error", str(e))
            return
    else:
        cmd.extend(["-c", body])
    if to_list:
        cmd.extend(["-to", ",".join(to_list)])
    if cc_list:
        cmd.extend(["-cc", ",".join(cc_list)])
    for att in dlg.attachments:
        cmd.extend(["-a", att])
    try:
        subprocess.Popen(cmd)
        QMessageBox.information(None, "Mail Sent", "Email triggered successfully.")
    except Exception as e:
        QMessageBox.warning(None, "Mail Error", str(e))

# config.py and utils.py: if you have your own versions, they will be imported.
# All constants and utilities are also defined self-contained in this file.
try:
    from config import *
except ImportError:
    pass
try:
    from utils import *
except ImportError:
    pass
try:
    from workers import *
except Exception as _e:
    print(f"[ERROR] Failed to import workers.py: {_e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

try:
    from widgets import *
except Exception as _e:
    print(f"[ERROR] Failed to import widgets.py: {_e}")
    import traceback; traceback.print_exc()
    sys.exit(1)
# dialogs inlined directly in main.py



# ===========================================================================
# TIMESTAMP HELPERS
# ===========================================================================

def relative_time(time_str):
    """Convert a timestamp string to relative time like '2h ago', '3d ago'.
    Handles formats: 'Jan 01, 2026 - 14:30:00' and 'Apr 16, 2026 - 16:33'"""
    if not time_str or time_str in ("-", "N/A", "Unknown"):
        return time_str or "-"
    try:
        # Try parsing common formats from parse_runtime_rpt
        import datetime as _dt
        ts = None
        for fmt in [
            "%a %b %d, %Y - %H:%M:%S",
            "%b %d, %Y - %H:%M:%S",
            "%b %d, %Y - %H:%M",
            "%b %d, %Y",
        ]:
            try:
                ts = _dt.datetime.strptime(time_str.strip(), fmt)
                break
            except ValueError:
                pass
        if ts is None:
            return time_str
        now   = _dt.datetime.now()
        delta = now - ts
        secs  = int(delta.total_seconds())
        if secs < 0:
            return time_str
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            return f"{secs // 3600}h ago"
        if secs < 86400 * 7:
            return f"{secs // 86400}d ago"
        if secs < 86400 * 30:
            return f"{secs // (86400*7)}w ago"
        return f"{secs // (86400*30)}mo ago"
    except Exception:
        return time_str


def convert_kst_to_ist_str(time_str):
    """Convert KST timestamp string to IST (KST - 3h 30min).
    KST = UTC+9, IST = UTC+5:30, difference = 3h 30min."""
    if not time_str or time_str in ("-", "N/A", "Unknown"):
        return time_str or "-"
    try:
        import datetime as _dt
        ts = None
        fmt_used = None
        for fmt in [
            "%a %b %d, %Y - %H:%M:%S",
            "%b %d, %Y - %H:%M:%S",
            "%b %d, %Y - %H:%M",
            "%b %d, %Y",
        ]:
            try:
                ts = _dt.datetime.strptime(time_str.strip(), fmt)
                fmt_used = fmt
                break
            except ValueError:
                pass
        if ts is None:
            return time_str
        ts_ist = ts - _dt.timedelta(hours=3, minutes=30)
        return ts_ist.strftime(fmt_used)
    except Exception:
        return time_str


# ===========================================================================
# MAIL HELPERS
# ===========================================================================

class MultiCompleterLineEdit(QLineEdit):
    """Comma-separated username input with auto-complete."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._completer = QCompleter()
        self._completer.setWidget(self)
        self._completer.setCompletionMode(QCompleter.PopupCompletion)
        self._completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._completer.activated.connect(self._insert_completion)
        self.words = []

    def set_words(self, words):
        self.words = words
        from PyQt5.QtCore import QStringListModel
        self._completer.setModel(QStringListModel(words, self._completer))

    def _insert_completion(self, completion):
        text = self.text()
        parts = text.split(',')
        base = ','.join(parts[:-1])
        self.setText((base + ', ' if base else '') + completion + ', ')

    def keyPressEvent(self, e):
        if self._completer.popup().isVisible():
            if e.key() in (Qt.Key_Enter, Qt.Key_Return):
                e.ignore(); return
        super().keyPressEvent(e)
        current_word = self.text().split(',')[-1].strip()
        if current_word:
            self._completer.setCompletionPrefix(current_word)
            if self._completer.completionCount() > 0:
                cr = self.cursorRect()
                cr.setWidth(
                    self._completer.popup().sizeHintForColumn(0)
                    + self._completer.popup().verticalScrollBar().sizeHint().width())
                self._completer.complete(cr)
            else:
                self._completer.popup().hide()
        else:
            self._completer.popup().hide()


class AdvancedMailDialog(QDialog):
    """Full mail compose dialog with To/CC/Subject/Body/Attachments."""
    def __init__(self, default_subject, default_body,
                 all_users, prefill_to="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send Email")
        self.resize(720, 560)
        self.attachments = []
        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.to_input = MultiCompleterLineEdit()
        self.to_input.set_words(all_users)
        self.cc_input = MultiCompleterLineEdit()
        self.cc_input.set_words(all_users)

        # Pre-fill from mail_config permanent members
        try:
            always_to = mail_config.get(
                'PERMANENT_MEMBERS', 'always_to', fallback='').strip()
            always_cc = mail_config.get(
                'PERMANENT_MEMBERS', 'always_cc', fallback='').strip()
            final_to = always_to
            if prefill_to:
                final_to = (always_to + ', ' if always_to else '') + prefill_to
            if final_to:
                self.to_input.setText(
                    final_to + (', ' if not final_to.endswith(',') else ' '))
            if always_cc:
                self.cc_input.setText(
                    always_cc + (', ' if not always_cc.endswith(',') else ' '))
        except Exception:
            pass

        self.subject_input = QLineEdit(default_subject)
        form.addRow("<b>To:</b>", self.to_input)
        form.addRow("<b>CC:</b>", self.cc_input)
        form.addRow("<b>Subject:</b>", self.subject_input)
        layout.addLayout(form)

        # Attachments row
        att_row = QHBoxLayout()
        att_row.addWidget(QLabel("<b>Attachments:</b>"))
        self.attach_lbl = QLabel("None")
        self.attach_lbl.setStyleSheet("color: #1976d2;")
        att_row.addWidget(self.attach_lbl)
        att_row.addStretch()
        qor_btn = QPushButton("Attach Latest QoR Report")
        qor_btn.clicked.connect(self._attach_qor)
        browse_btn = QPushButton("Browse Files...")
        browse_btn.clicked.connect(self._browse_files)
        att_row.addWidget(qor_btn)
        att_row.addWidget(browse_btn)
        layout.addLayout(att_row)

        self.body_input = QTextEdit()
        self.body_input.setPlainText(default_body)
        layout.addWidget(self.body_input, 1)

        btn_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.button(QDialogButtonBox.Ok).setText("Send Mail")
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def _update_attach_lbl(self):
        if not self.attachments:
            self.attach_lbl.setText("None")
        else:
            names = ", ".join(os.path.basename(p) for p in self.attachments)
            self.attach_lbl.setText(
                f"{len(self.attachments)} file(s): {names}")

    def _attach_qor(self):
        # Find latest QoR HTML in qor_metrices/
        import glob as _glob
        hits = _glob.glob(
            os.path.join(os.getcwd(), "qor_metrices", "**", "*.html"),
            recursive=True)
        if hits:
            latest = sorted(hits, key=os.path.getmtime)[-1]
            if latest not in self.attachments:
                self.attachments.append(latest)
                self._update_attach_lbl()
        else:
            QMessageBox.warning(
                self, "Not Found",
                "No QoR HTML found in qor_metrices/.")

    def _browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Attachments", "", "All Files (*)")
        for f in files:
            if f not in self.attachments:
                self.attachments.append(f)
        if files:
            self._update_attach_lbl()


# ===========================================================================
# INLINE DIALOG CLASSES (replaces dialogs.py dependency)
# ===========================================================================

class EditNoteDialog(QDialog):
    def __init__(self, current_note, note_id, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit Note: {note_id[:50]}")
        self.resize(500, 250)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<b>Note for:</b> {note_id}"))
        self._edit = QTextEdit()
        self._edit.setPlainText(current_note or "")
        layout.addWidget(self._edit)
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def get_text(self):
        return self._edit.toPlainText().strip()


class FilterDialog(QDialog):
    def __init__(self, col_name, all_values, active_values, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Filter: {col_name}")
        self.resize(300, 400)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<b>Select values to show:</b>"))
        btn_row = QHBoxLayout()
        all_btn  = QPushButton("All")
        none_btn = QPushButton("None")
        btn_row.addWidget(all_btn); btn_row.addWidget(none_btn)
        layout.addLayout(btn_row)
        self._checks = {}
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        inner = QWidget(); inner_layout = QVBoxLayout(inner)
        for val in sorted(all_values):
            cb = QCheckBox(str(val))
            cb.setChecked(val in active_values)
            inner_layout.addWidget(cb)
            self._checks[val] = cb
        inner_layout.addStretch()
        scroll.setWidget(inner)
        layout.addWidget(scroll, 1)
        all_btn.clicked.connect(lambda: [c.setChecked(True)
                                         for c in self._checks.values()])
        none_btn.clicked.connect(lambda: [c.setChecked(False)
                                          for c in self._checks.values()])
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def get_selected(self):
        return {v for v, cb in self._checks.items() if cb.isChecked()}


class PieChartWidget(QWidget):
    """Simple pie chart for disk usage by user."""
    def __init__(self):
        super().__init__()
        self.setMinimumSize(220, 220)
        self.data   = {}
        self.colors = [
            QColor("#ef5350"), QColor("#42a5f5"), QColor("#66bb6a"),
            QColor("#ffa726"), QColor("#ab47bc"), QColor("#26c6da"),
            QColor("#8d6e63"), QColor("#78909c"), QColor("#d4e157"),
            QColor("#ec407a")]
        self.bg_col = "#ffffff"

    def set_data(self, data, is_dark):
        self.data   = dict(sorted(data.items(),
                                   key=lambda x: x[1], reverse=True))
        self.bg_col = "#2b2d30" if is_dark else "#ffffff"
        self.update()

    def paintEvent(self, event):
        import math
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        rect    = self.rect()
        margin  = 20
        dim     = min(rect.width(), rect.height()) - 2 * margin
        if dim <= 0: return
        cx = rect.center().x(); cy = rect.center().y()
        from PyQt5.QtCore import QRectF
        pie_rect = QRectF(cx - dim/2, cy - dim/2, dim, dim)
        total    = sum(self.data.values())
        if total == 0:
            painter.setPen(QColor("#888"))
            painter.drawText(rect, Qt.AlignCenter, "No Data")
            return
        start = 0
        for i, (name, val) in enumerate(self.data.items()):
            span = int((val / total) * 360 * 16)
            painter.setBrush(QBrush(self.colors[i % len(self.colors)]))
            painter.setPen(QPen(QColor(self.bg_col), 1))
            painter.drawPie(pie_rect, start, span)
            start += span


class DiskUsageDialog(QDialog):
    """Full disk usage dialog -- exact logic from original script."""
    def __init__(self, disk_data, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Disk Space Usage")
        self.resize(1100, 650)
        self.disk_data    = disk_data or {}
        self.is_dark      = is_dark
        self.parent_win   = parent
        self._building    = False   # guard for itemChanged signal

        layout = QVBoxLayout(self)

        # Top row -- scope selector + partition info
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("<b>Scope:</b>"))
        self.combo = QComboBox()
        self.combo.addItems(["WS (FE)", "WS (BE)", "OUTFEED"])
        self.combo.currentIndexChanged.connect(self.update_view)
        top_row.addWidget(self.combo)
        top_row.addSpacing(20)
        self.part_lbl = QLabel("")
        self.part_lbl.setStyleSheet("font-weight: bold; color: #d32f2f;")
        top_row.addWidget(self.part_lbl)
        top_row.addStretch()
        layout.addLayout(top_row)

        # Main body -- pie + tree
        body = QHBoxLayout()
        self.pie = PieChartWidget()
        self.pie.setFixedSize(220, 220)
        body.addWidget(self.pie, 0)

        self.tree = QTreeWidget()
        self.tree.setColumnCount(2)
        self.tree.setHeaderLabels(["User / Path", "Size (GB)"])
        self.tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.tree.setAlternatingRowColors(True)
        self.tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tree.itemChanged.connect(self._on_item_changed)
        body.addWidget(self.tree, 1)
        layout.addLayout(body, 1)

        # Bottom row
        bot_row = QHBoxLayout()
        self.recalc_btn = QPushButton("Recalculate Disk Usage")
        self.recalc_btn.clicked.connect(self._recalc)
        bot_row.addWidget(self.recalc_btn)
        bot_row.addStretch()
        self.mail_btn = QPushButton("Send Cleanup Mail to Selected")
        self.mail_btn.clicked.connect(self._send_mail)
        bot_row.addWidget(self.mail_btn)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        bot_row.addWidget(close_btn)
        layout.addLayout(bot_row)

        self.update_view()

    def _partition_info(self, path):
        import shutil
        try:
            total, used, free = shutil.disk_usage(path)
            t = total/(1024**3); u = used/(1024**3); f = free/(1024**3)
            pct = (used/total)*100 if total > 0 else 0
            return (f"Total: {t:.1f} GB  Used: {u:.1f} GB ({pct:.0f}%)"
                    f"  Free: {f:.1f} GB")
        except Exception:
            return ""

    def update_view(self):
        self._building = True
        self.tree.clear()
        cat  = self.combo.currentText()
        data = self.disk_data.get(cat, {})

        # Partition space label
        path_map = {"WS (FE)": BASE_WS_FE_DIR,
                     "WS (BE)": BASE_WS_BE_DIR,
                     "OUTFEED": BASE_OUTFEED_DIR}
        self.part_lbl.setText(
            self._partition_info(path_map.get(cat, "/")))

        # Pie chart
        pie_data = {u: v["total"] for u, v in data.items()}
        self.pie.set_data(pie_data, self.is_dark)

        # Tree -- one top-level item per user, children = dirs
        for i, (user, info) in enumerate(
                sorted(data.items(),
                       key=lambda x: x[1]["total"], reverse=True)):
            u_item = QTreeWidgetItem(self.tree)
            u_item.setFlags(
                u_item.flags() | Qt.ItemIsUserCheckable)
            u_item.setCheckState(0, Qt.Unchecked)
            u_item.setText(0, user)
            u_item.setText(1, f"{info['total']:.2f} GB")
            from PyQt5.QtGui import QBrush
            color = QColor(
                ["#ef5350","#42a5f5","#66bb6a","#ffa726",
                 "#ab47bc","#26c6da","#8d6e63","#78909c",
                 "#d4e157","#ec407a"][i % 10])
            u_item.setForeground(0, color)
            f = u_item.font(0); f.setBold(True)
            u_item.setFont(0, f); u_item.setFont(1, f)

            for dir_path, dir_sz in info["dirs"]:
                d_item = QTreeWidgetItem(u_item)
                d_item.setFlags(
                    d_item.flags() | Qt.ItemIsUserCheckable)
                d_item.setCheckState(0, Qt.Unchecked)
                d_item.setText(0, os.path.basename(dir_path))
                d_item.setToolTip(0, dir_path)
                d_item.setText(1, f"{dir_sz:.2f} GB")
                d_item.setData(0, Qt.UserRole,     user)
                d_item.setData(0, Qt.UserRole + 1, dir_path)
                d_item.setData(0, Qt.UserRole + 2, dir_sz)

        self._building = False

    def _on_item_changed(self, item, col):
        if self._building or col != 0: return
        self.tree.blockSignals(True)
        state = item.checkState(0)
        for i in range(item.childCount()):
            item.child(i).setCheckState(0, state)
        self.tree.blockSignals(False)

    def _recalc(self):
        if self.parent_win:
            self.recalc_btn.setEnabled(False)
            self.recalc_btn.setText("Calculating...")
            self.parent_win.start_bg_disk_scan(force=True)

    def _send_mail(self):
        """Collect checked dirs, compose cleanup mail with sizes."""
        user_runs = {}
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            u_item = root.child(i)
            for j in range(u_item.childCount()):
                d_item = u_item.child(j)
                if d_item.checkState(0) == Qt.Checked:
                    owner    = d_item.data(0, Qt.UserRole)
                    path     = d_item.data(0, Qt.UserRole + 1)
                    sz       = d_item.data(0, Qt.UserRole + 2)
                    if owner not in user_runs:
                        user_runs[owner] = []
                    user_runs[owner].append((path, sz))

        if not user_runs:
            QMessageBox.warning(
                self, "Nothing Selected",
                "Please check at least one directory.")
            return

        # Build mail body with size info
        lines = ["Hi,", "",
                  "Please clean up the following directories:", ""]
        for owner, items in sorted(user_runs.items()):
            lines.append(f"Owner: {owner}")
            for path, sz in items:
                lines.append(f"  {path}  [{sz:.2f} GB]")
            lines.append("")
        lines.append("Thank you.")

        owner_emails = []
        for owner in user_runs:
            e = _get_user_email(owner)
            if e: owner_emails.append(e)

        all_known = _get_all_known_mail_users()
        dlg = AdvancedMailDialog(
            "Action Required: Please clean up disk space",
            "\n".join(lines),
            all_known,
            ", ".join(owner_emails),
            self)
        if dlg.exec_():
            _send_mail_via_util(dlg)


class QoRSummaryDialog(QDialog):
    """QoR summary matching Image 1 layout: rows like R2R Setup, Total Area etc."""

    def __init__(self, run_name, metrics, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("QoR Summary: " + str(run_name))
        self.resize(600, 750)
        layout = QVBoxLayout(self)

        # Header
        hdr = QLabel("<b>" + str(run_name) + "</b>")
        hf  = hdr.font(); hf.setPointSize(11); hdr.setFont(hf)
        hdr.setAlignment(Qt.AlignCenter)
        hdr_bg = "#1565c0" if is_dark else "#2196f3"
        hdr.setStyleSheet(
            "background:" + hdr_bg + "; color:white; padding:8px; border-radius:4px;")
        layout.addWidget(hdr)

        # Build rows matching Image 1
        area  = metrics.get("area",       {})
        cong  = metrics.get("congestion", {})
        power = metrics.get("power",      {})

        def _v(d, *keys):
            for k in keys:
                v = d.get(k)
                if v and v != "-":
                    return str(v)
            return "-"

        # LVT/RVT/HVT combined inst/area strings from parse_cell_usage
        vth              = metrics.get("vth", {})
        lvt_rvt_hvt_inst = vth.get("lvt_rvt_hvt_inst", vth.get("lvt_rvt_inst", "-/-"))
        lvt_rvt_hvt_area = vth.get("lvt_rvt_hvt_area", vth.get("lvt_rvt_area", "-/-"))

        # Congestion: cong_both already formatted as "Both%/V%/H%"
        cong_str = _v(cong, "cong_both")

        # Power: key is "leakage", value includes unit e.g. "87.468 uW"
        pwr_str = _v(power, "leakage")

        # Util string: from utilization report
        util_str = _v(metrics.get("util", {}), "std_util_str", "std_util")

        # (label, value, is_section, path_key_or_None)
        _paths = metrics.get("_paths", {})
        rows = [
            ("Timing",                         None,                       True,  None),
            ("R2R (Setup)  WNS/TNS/FEPs",     metrics.get("r2r_setup","-"), False, "r2r_setup"),
            ("R2R (Hold)   WNS/TNS/FEPs",     metrics.get("r2r_hold",  "-"), False, "r2r_hold"),
            ("Area",                            None,                       True,  None),
            ("Total Area",                     _v(area,"total_area"),       False, "total_area"),
            ("Std Cell Area",                  _v(area,"std_cell_area_total",
                                                      "std_cell_area"),     False, "std_cell_area"),
            ("Memory Area",                    _v(area,"memory_area"),      False, "memory_area"),
            ("Macro Area (Inc. Mem)",          _v(area,"macro_area"),       False, "macro_area"),
            ("Instance Count",                 _v(area,"instance_count",
                                                   "total_count"),          False, "instance_count"),
            ("Physical",                       None,                        True,  None),
            ("LVT*/RVT*/HVT* Inst",            lvt_rvt_hvt_inst,            False, "vth"),
            ("LVT*/RVT*/HVT* Area",            lvt_rvt_hvt_area,            False, "vth"),
            ("Congestion (Both/V/H Dir)",      cong_str,                    False, "congestion"),
            ("StdCell/StdCell Only Util",      util_str,                    False, "std_cell_area"),
            ("Quality",                        None,                        True,  None),
            ("MBIT Ratio",                     _v(metrics, "mbit"),         False, "mbit"),
            ("CGC Ratio",                      metrics.get("cgc", "-"),     False, "cgc"),
            ("Power",                          None,                        True,  None),
            ("Cell Leakage Power",             pwr_str,                     False, "leakage"),
            ("Runtime",                        metrics.get("runtime","-"),  False, "runtime"),
            ("Logic Depth",                    metrics.get("logic_depth","-"), False, "logic_depth"),
        ]

        # Table
        tbl = QTableWidget(0, 2)
        tbl.setHorizontalHeaderLabels(["Metric", "Value"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        tbl.verticalHeader().setVisible(False)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.setSelectionBehavior(QTableWidget.SelectRows)
        tbl.setToolTip("Double-click a value to open its report in gvim")

        sec_bg  = "#1565c0" if is_dark else "#bbdefb"
        sec_fg  = "white"   if is_dark else "#0d47a1"
        neg_fg  = "#ef5350" if is_dark else "#c62828"
        zero_fg = "#66bb6a" if is_dark else "#2e7d32"

        for label, val, is_sec, path_key in rows:
            r = tbl.rowCount()
            tbl.insertRow(r)

            if is_sec:
                sec_item = QTableWidgetItem(label)
                sec_item.setBackground(QColor(sec_bg))
                sec_item.setForeground(QColor(sec_fg))
                f2 = sec_item.font(); f2.setBold(True); sec_item.setFont(f2)
                tbl.setItem(r, 0, sec_item)
                tbl.setItem(r, 1, QTableWidgetItem(""))
                tbl.item(r, 1).setBackground(QColor(sec_bg))
                tbl.setRowHeight(r, 24)
            else:
                m_item = QTableWidgetItem("  " + str(label))
                v_item = QTableWidgetItem(str(val) if val else "-")
                v_item.setTextAlignment(Qt.AlignCenter)
                # Store report path so double-click can open it
                rpt_path = _paths.get(path_key) if path_key else None
                v_item.setData(Qt.UserRole, rpt_path)
                if rpt_path:
                    v_item.setToolTip("Double-click to open in gvim:\n" + str(rpt_path))
                # Color negative timing values
                try:
                    fv = float(str(val).split("/")[0])
                    if fv < 0:
                        v_item.setForeground(QColor(neg_fg))
                    elif fv == 0.0:
                        v_item.setForeground(QColor(zero_fg))
                except Exception:
                    pass
                tbl.setItem(r, 0, m_item)
                tbl.setItem(r, 1, v_item)

        # Double-click value → open report in gvim
        def _open_qor_rpt(clicked_item):
            path = clicked_item.data(Qt.UserRole)
            if path and os.path.exists(path):
                subprocess.Popen(['gvim', path])
            elif path:
                QMessageBox.information(
                    self, "Not Found",
                    "Report file not found:\n" + str(path))
        tbl.itemDoubleClicked.connect(_open_qor_rpt)

        layout.addWidget(tbl, 1)

        # Show timing scenarios if available
        timing_raw = metrics.get("timing_raw", {})
        scenarios  = timing_raw.get("timing", {}) if timing_raw else {}
        if scenarios:
            sc_lbl = QLabel("<b>Timing Detail (per scenario)</b>")
            sc_lbl.setStyleSheet("padding-top:6px;")
            layout.addWidget(sc_lbl)
            sc_tbl = QTableWidget(0, 4)
            sc_tbl.setHorizontalHeaderLabels(
                ["Scenario", "Path Group/Type", "WNS", "TNS"])
            sc_tbl.horizontalHeader().setSectionResizeMode(
                0, QHeaderView.Stretch)
            sc_tbl.horizontalHeader().setSectionResizeMode(
                1, QHeaderView.ResizeToContents)
            sc_tbl.setEditTriggers(QTableWidget.NoEditTriggers)
            sc_tbl.setAlternatingRowColors(True)
            sc_tbl.verticalHeader().setVisible(False)
            sc_tbl.setMaximumHeight(180)
            for sce, grps in sorted(scenarios.items()):
                for grp_key, vals in sorted(grps.items()):
                    r2 = sc_tbl.rowCount(); sc_tbl.insertRow(r2)
                    sc_tbl.setItem(r2, 0, QTableWidgetItem(sce))
                    sc_tbl.setItem(r2, 1, QTableWidgetItem(grp_key))
                    wv = QTableWidgetItem(vals.get("wns", "-"))
                    wv.setTextAlignment(Qt.AlignCenter)
                    try:
                        if float(vals.get("wns","0")) < 0:
                            wv.setForeground(QColor(neg_fg))
                    except Exception:
                        pass
                    sc_tbl.setItem(r2, 2, wv)
                    tv2 = QTableWidgetItem(vals.get("tns", "-"))
                    tv2.setTextAlignment(Qt.AlignCenter)
                    sc_tbl.setItem(r2, 3, tv2)
            layout.addWidget(sc_tbl)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)


class QoRWorker(QThread):
    finished = pyqtSignal(str)

    def __init__(self, script_path, run_dirs, python_bin="python3.6"):
        super().__init__()
        self.script_path = script_path
        self.run_dirs    = run_dirs
        self.python_bin  = python_bin

    def run(self):
        try:
            script_dir = os.path.dirname(os.path.abspath(self.script_path))
            cmd = [self.python_bin, self.script_path] + self.run_dirs
            result = subprocess.run(
                cmd, cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600)
            output = result.stdout.decode("utf-8", errors="ignore")
            html_path = ""
            for line in output.splitlines():
                if ".html" in line:
                    for part in line.split():
                        if part.endswith(".html"):
                            html_path = part
                            break
                    if html_path:
                        break
            if html_path and not os.path.isabs(html_path):
                html_path = os.path.join(script_dir, html_path)
            # Also search qor_metrices/ in script dir
            if not html_path or not os.path.exists(html_path):
                import glob as _g
                hits = _g.glob(os.path.join(
                    script_dir, "qor_metrices", "**", "*.html"),
                    recursive=True)
                if hits:
                    html_path = sorted(hits, key=os.path.getmtime)[-1]
            self.finished.emit(
                html_path if (html_path and os.path.exists(html_path))
                else "")
        except Exception:
            self.finished.emit("")

# ---------------------------------------------------------------------------
# Lightweight PyQt5-native chart widgets (no matplotlib dependency)
# ---------------------------------------------------------------------------
class _PieChartWidget(QWidget):
    """Pie chart using QPainter. data = {label: float}."""
    _COLORS = [
        QColor("#42a5f5"), QColor("#66bb6a"), QColor("#ffa726"),
        QColor("#ef5350"), QColor("#ab47bc"), QColor("#26c6da"),
        QColor("#8d6e63"), QColor("#78909c")]

    def __init__(self, title=""):
        super().__init__()
        self.title   = title
        self.data    = {}
        self.is_dark = False
        self.setMinimumSize(260, 220)

    def set_data(self, data, is_dark=False):
        clean = []
        for k, v in data.items():
            try:
                fv = float(v)
            except Exception:
                fv = 0.0
            if fv > 0:
                clean.append((str(k), fv))
        clean.sort(key=lambda x: x[1], reverse=True)
        if len(clean) > 6:
            other = sum(v for _, v in clean[5:])
            clean = clean[:5] + [("Other", other)]
        self.data    = dict(clean)
        self.is_dark = is_dark
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        fg  = QColor("#dfe1e5" if self.is_dark else "#333333")
        bg  = QColor("#2b2d30" if self.is_dark else "#ffffff")
        p.fillRect(self.rect(), bg)
        r   = self.rect()
        p.setPen(fg)
        p.drawText(r.adjusted(0, 6, 0, 0),
                   Qt.AlignHCenter | Qt.AlignTop, self.title)
        total = sum(self.data.values())
        if not total:
            p.drawText(r, Qt.AlignCenter, "No Data"); return
        top_margin = 28
        legend_h = 20 * len(self.data) + 8
        chart_h = max(80, r.height() - top_margin - legend_h)
        dim = min(r.width() - 24, chart_h) - 6
        if dim <= 0: return
        from PyQt5.QtCore import QRectF
        cx  = r.center().x()
        cy  = top_margin + dim / 2
        pie = QRectF(cx - dim/2, cy - dim/2, dim, dim)
        start = 0
        items = list(self.data.items())
        for i, (label, val) in enumerate(items):
            span  = int(val / total * 360 * 16)
            color = self._COLORS[i % len(self._COLORS)]
            p.setBrush(QBrush(color))
            p.setPen(QPen(bg, 1))
            p.drawPie(pie, start, span)
            start += span
        hole_dim = dim * 0.54
        hole = QRectF(cx - hole_dim/2, cy - hole_dim/2, hole_dim, hole_dim)
        p.setBrush(QBrush(bg))
        p.setPen(QPen(bg, 1))
        p.drawEllipse(hole)
        p.setPen(fg)
        p.drawText(hole, Qt.AlignCenter, "{:.1f}%".format(total))
        legend_y = int(top_margin + dim + 8)
        col_w = max(110, r.width() // 2)
        for i, (label, val) in enumerate(items):
            color = self._COLORS[i % len(self._COLORS)]
            col = i % 2
            row = i // 2
            lx = 14 + col * col_w
            ly = legend_y + row * 20
            p.setBrush(color); p.setPen(Qt.NoPen)
            p.drawEllipse(lx, ly + 4, 10, 10)
            p.setPen(fg)
            text = "{}  {:.1f}%".format(label, val / total * 100)
            p.drawText(lx + 14, ly, col_w - 22, 18,
                       Qt.AlignVCenter | Qt.AlignLeft, text)


class _BarChartWidget(QWidget):
    """Bar chart (vertical or horizontal) using QPainter."""
    def __init__(self, title="", horizontal=False):
        super().__init__()
        self.title      = title
        self.horizontal = horizontal
        self.labels     = []
        self.values     = []
        self.bar_colors = []
        self.is_dark    = False
        self.y_label    = ""
        self.setMinimumSize(180, 160)

    def set_data(self, labels, values, colors=None, is_dark=False, y_label=""):
        self.labels     = labels
        self.values     = values
        self.bar_colors = colors or [QColor("#42a5f5")] * len(values)
        self.is_dark    = is_dark
        self.y_label    = y_label
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        fg  = QColor("#dfe1e5" if self.is_dark else "#333333")
        bg  = QColor("#2b2d30" if self.is_dark else "#ffffff")
        ax  = QColor("#888888")
        p.fillRect(self.rect(), bg)
        r   = self.rect()
        if not self.values:
            p.setPen(fg); p.drawText(r, Qt.AlignCenter, "No Data"); return
        # Title
        p.setPen(fg)
        p.drawText(r.adjusted(0, 4, 0, 0), Qt.AlignHCenter | Qt.AlignTop, self.title)
        margin_t = 22; margin_b = 36; margin_l = 12; margin_r = 8
        area_w = r.width()  - margin_l - margin_r
        area_h = r.height() - margin_t - margin_b
        if area_w <= 0 or area_h <= 0: return
        n    = len(self.values)
        vmax = max(abs(v) for v in self.values) or 1
        vmin = min(self.values)
        zero_y = margin_t + area_h if vmin >= 0 else (
            margin_t + int(area_h * max(self.values) / (max(self.values) - vmin)))
        # Axis line
        p.setPen(QPen(ax, 1))
        p.drawLine(margin_l, margin_t, margin_l, margin_t + area_h)
        p.drawLine(margin_l, zero_y,   margin_l + area_w, zero_y)
        # Bars
        bar_w = max(2, area_w // n - 2)
        for i, (val, col) in enumerate(zip(self.values, self.bar_colors)):
            x = margin_l + i * (area_w // n) + (area_w // n - bar_w) // 2
            if val >= 0:
                h   = int(area_h * val / (vmax if vmax != 0 else 1))
                top = zero_y - h
            else:
                h   = int(area_h * abs(val) / (vmax if vmax != 0 else 1))
                top = zero_y
            p.setBrush(QBrush(col)); p.setPen(Qt.NoPen)
            p.drawRect(x, top, bar_w, max(1, h))
            # X label
            lbl = self.labels[i] if i < len(self.labels) else ""
            p.setPen(fg)
            fm  = p.fontMetrics()
            lbl_short = lbl[:8] + ".." if fm.width(lbl) > bar_w + 12 else lbl
            p.drawText(x - 4, zero_y + 2, bar_w + 8, 32,
                       Qt.AlignHCenter | Qt.AlignTop, lbl_short)


class _StackedVtChartWidget(QWidget):
    """Per-run stacked VT distribution. Clearer than averaging into a pie."""
    def __init__(self, title="VT Area Distribution per Run"):
        super().__init__()
        self.title = title
        self.labels = []
        self.rows = []
        self.is_dark = False
        self.setMinimumSize(320, 220)

    def set_data(self, labels, rows, is_dark=False):
        self.labels = labels or []
        self.rows = rows or []
        self.is_dark = is_dark
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        colors = [QColor("#43a047"), QColor("#1e88e5"), QColor("#fb8c00")]
        names = ["LVT", "RVT", "HVT"]
        p.fillRect(self.rect(), bg)
        r = self.rect()
        p.setPen(fg)
        p.drawText(8, 6, r.width() - 16, 20,
                   Qt.AlignHCenter | Qt.AlignVCenter, self.title)
        valid = []
        for label, row in zip(self.labels, self.rows):
            vals = [max(0.0, float(v or 0.0)) for v in row]
            if sum(vals) > 0:
                valid.append((label, vals))
        if not valid:
            p.drawText(r, Qt.AlignCenter, "No VT data")
            return
        left = 96
        right = 18
        top = 38
        row_h = 22
        gap = 10
        max_rows = max(1, min(len(valid), int((r.height() - top - 34) / (row_h + gap))))
        bar_w = max(80, r.width() - left - right)
        for i, (label, vals) in enumerate(valid[:max_rows]):
            y = top + i * (row_h + gap)
            lbl = label
            if len(lbl) > 14:
                lbl = lbl[:11] + "..."
            p.setPen(fg)
            p.drawText(6, y, left - 12, row_h,
                       Qt.AlignRight | Qt.AlignVCenter, lbl)
            total = sum(vals) or 1.0
            x = left
            for idx, val in enumerate(vals):
                w = int(bar_w * val / total)
                if idx == len(vals) - 1:
                    w = left + bar_w - x
                if w <= 0:
                    continue
                p.setBrush(QBrush(colors[idx]))
                p.setPen(Qt.NoPen)
                p.drawRect(x, y + 3, w, row_h - 6)
                pct = val / total * 100.0
                if w > 44:
                    p.setPen(QColor("#ffffff"))
                    p.drawText(x, y, w, row_h, Qt.AlignCenter,
                               "{:.0f}%".format(pct))
                x += w
        ly = r.height() - 24
        lx = left
        for i, name in enumerate(names):
            p.setBrush(QBrush(colors[i]))
            p.setPen(Qt.NoPen)
            p.drawRect(lx, ly + 5, 10, 10)
            p.setPen(muted)
            p.drawText(lx + 14, ly, 60, 20,
                       Qt.AlignLeft | Qt.AlignVCenter, name)
            lx += 64


class _TimelineChartWidget(QWidget):
    """Readable sequential pipeline timeline for FE and PNR stage events."""
    def __init__(self, events=None, parser=None, is_dark=False):
        super().__init__()
        self.events = events or []
        self.parser = parser
        self.is_dark = is_dark
        self.setMinimumHeight(240)

    def set_data(self, events, parser, is_dark=False):
        self.events = events or []
        self.parser = parser
        self.is_dark = is_dark
        self.update()

    def preferred_height(self, width):
        card_w = 156
        card_h = 62
        gap_x = 40
        gap_y = 28
        left = 16
        top = 36
        usable_w = max(card_w, int(width) - left * 2)
        per_row = max(1, int((usable_w + gap_x) / (card_w + gap_x)))
        rows = max(1, (len(self.events or []) + per_row - 1) // per_row)
        return top + rows * (card_h + gap_y) + 20

    def _dt(self, val):
        if self.parser:
            try:
                return self.parser(val)
            except Exception:
                return None
        return None

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        line = QColor("#6b7280" if self.is_dark else "#cfd8dc")
        fe_color = QColor("#42a5f5")
        stage_color = QColor("#66bb6a")
        warn_color = QColor("#ffa726")
        p.fillRect(self.rect(), bg)
        r = self.rect()
        events = list(self.events or [])
        if not events:
            p.setPen(fg)
            p.drawText(r, Qt.AlignCenter, "No timestamp data available")
            return
        p.setPen(fg)
        p.drawText(8, 8, r.width() - 16, 18,
                   Qt.AlignLeft | Qt.AlignVCenter,
                   "Timeline flow")
        card_w = 156
        card_h = 62
        gap_x = 40
        gap_y = 28
        left = 16
        top = 36
        usable_w = max(card_w, r.width() - left * 2)
        per_row = max(1, int((usable_w + gap_x) / (card_w + gap_x)))
        shown = events
        prev_end = None
        for idx, ev in enumerate(shown):
            row = idx // per_row
            col = idx % per_row
            x = left + col * (card_w + gap_x)
            y = top + row * (card_h + gap_y)
            color = fe_color if ev.get("kind") == "FE" else stage_color
            if ev.get("runtime", "-") in ("", "-", "N/A"):
                color = warn_color
            p.setBrush(QBrush(color))
            p.setPen(QPen(color.darker(120), 1))
            p.drawRoundedRect(x, y, card_w, card_h, 6, 6)
            p.setPen(QColor("#ffffff"))
            name = ev.get("name", "-")
            if " / " in name:
                name = name.split(" / ")[-1]
            if len(name) > 20:
                name = name[:17] + "..."
            p.drawText(x + 8, y + 6, card_w - 16, 18,
                       Qt.AlignLeft | Qt.AlignVCenter, name)
            txt = ev.get("runtime", "-")
            p.drawText(x + 8, y + 26, card_w - 16, 16,
                       Qt.AlignLeft | Qt.AlignVCenter,
                       "Runtime: " + txt)
            st = self._dt(ev.get("start"))
            en = self._dt(ev.get("end"))
            when = ev.get("start", "-")
            if st:
                when = st.strftime("%m/%d %H:%M")
            p.drawText(x + 8, y + 43, card_w - 16, 16,
                       Qt.AlignLeft | Qt.AlignVCenter, when)
            if idx > 0:
                if col == 0:
                    x1 = left + (per_row - 1) * (card_w + gap_x) + card_w
                    y1 = y - gap_y + card_h // 2
                    x2 = x
                    y2 = y + card_h // 2
                    p.setPen(QPen(line, 1))
                    p.drawLine(x1, y1, x1 + 14, y1)
                    p.drawLine(x1 + 14, y1, x1 + 14, y2)
                    p.drawLine(x1 + 14, y2, x2 - 8, y2)
                else:
                    x1 = x - gap_x
                    y1 = y + card_h // 2
                    x2 = x
                    y2 = y1
                    p.setPen(QPen(line, 1))
                    p.drawLine(x1, y1, x2 - 8, y2)
                p.setBrush(QBrush(line))
                p.setPen(Qt.NoPen)
                p.drawPolygon(QPolygon([
                    QPoint(x2 - 8, y2 - 4),
                    QPoint(x2 - 8, y2 + 4),
                    QPoint(x2 - 1, y2)]))
                gap_txt = self._gap_text(prev_end, st)
                if gap_txt != "-":
                    p.setPen(muted)
                    if col == 0:
                        p.drawText(x + 4, y - 24, card_w - 8, 18,
                                   Qt.AlignCenter, gap_txt)
                    else:
                        p.drawText(x - gap_x + 4, y + 2, gap_x - 8, 18,
                                   Qt.AlignCenter, gap_txt)
            prev_end = en or prev_end
    def _gap_text(self, prev_end, start):
        if not prev_end or not start:
            return "-"
        secs = int((start - prev_end).total_seconds())
        if secs < 0:
            return "overlap"
        h = secs // 3600
        m = (secs % 3600) // 60
        if h >= 24:
            return "{}d {}h".format(h // 24, h % 24)
        return "{}h {}m".format(h, m)


class BlockSummaryDialog(QDialog):
    """Block synthesis summary table.
    One row per selected run. User clicks Generate to start loading.
    Tabs: Table | Charts (requires matplotlib)."""

    HEADERS = [
        "BLK Name", "Run Name", "MBIT%", "CG%",
        "Instance Count", "Std Area (um2)", "Gate Count",
        "VT L/R/H Area%",
        "R2R Setup (W/T/F)",
        "R2R Hold (W/T/F)",
        "Logic Depth",
        "Runtime"
    ]
    # Column indices for coloring / chart reads
    _COL_R2R_SETUP = 8
    _COL_R2R_HOLD  = 9

    def __init__(self, rtl_label, run_list, is_dark, parent=None):
        """run_list: list of (blk, run_path, run_name, runtime, source)"""
        super().__init__(parent)
        self.setWindowTitle("Block Summary: " + str(rtl_label))
        self.setWindowFlags(
            self.windowFlags()
            | Qt.WindowMaximizeButtonHint
            | Qt.WindowMinimizeButtonHint)
        self.setSizeGripEnabled(True)
        self.resize(1400, 600)
        self.is_dark   = is_dark
        self._run_list = run_list
        self._pending  = []
        self._active_worker = None

        layout = QVBoxLayout(self)

        # Header
        hdr = QLabel("<b>" + str(rtl_label) + " -- Synthesis Summary</b>")
        hf  = hdr.font(); hf.setPointSize(11); hdr.setFont(hf)
        hdr.setAlignment(Qt.AlignCenter)
        layout.addWidget(hdr)

        # Info + status
        info = QLabel(str(len(run_list)) +
                      " run(s) selected. Click Generate to extract metrics.")
        info.setStyleSheet("color: #1976d2;")
        layout.addWidget(info)

        self.status_lbl = QLabel("")
        self.status_lbl.setStyleSheet("color: #e65100; font-style: italic;")
        layout.addWidget(self.status_lbl)

        # Tab widget
        self._tabs = QTabWidget()
        layout.addWidget(self._tabs, 1)

        # ── Tab 1: Table ────────────────────────────────────────────────
        tab_tbl = QWidget()
        tab_tbl_layout = QVBoxLayout(tab_tbl)
        tab_tbl_layout.setContentsMargins(0, 0, 0, 0)

        self.tbl = QTableWidget(0, len(self.HEADERS))
        self.tbl.setHorizontalHeaderLabels(self.HEADERS)
        hh = self.tbl.horizontalHeader()
        hh.setSectionsMovable(True)
        hh.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.Stretch)
        for c in range(2, len(self.HEADERS)):
            if c != 1:
                hh.setSectionResizeMode(c, QHeaderView.ResizeToContents)
        self.tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.verticalHeader().setVisible(False)
        self.tbl.setSortingEnabled(True)
        self.tbl.setToolTip("Double-click a metric cell to open its report in gvim")
        self.tbl.itemDoubleClicked.connect(self._open_cell_report)
        tab_tbl_layout.addWidget(self.tbl)
        self._tabs.addTab(tab_tbl, "Table")

        # ── Tab 2: Charts (PyQt5 native, no matplotlib) ──────────────────
        tab_charts = QWidget()
        tab_charts_layout = QVBoxLayout(tab_charts)
        tab_charts_layout.setContentsMargins(4, 4, 4, 4)

        refresh_charts_btn = QPushButton("Refresh Charts")
        refresh_charts_btn.clicked.connect(self._draw_charts)
        tab_charts_layout.addWidget(refresh_charts_btn, 0)

        self._chart_vt   = _StackedVtChartWidget("VT Area Distribution per Run")
        self._chart_area = _BarChartWidget("Std Cell Area per Run")
        self._chart_wns  = _BarChartWidget("R2R Setup WNS per Run")
        self._chart_cgc  = _BarChartWidget("Clock Gating % per Run")
        from PyQt5.QtWidgets import QGridLayout as _QGL
        charts_grid = _QGL()
        charts_grid.addWidget(self._chart_vt,   0, 0)
        charts_grid.addWidget(self._chart_area, 0, 1)
        charts_grid.addWidget(self._chart_wns,  1, 0)
        charts_grid.addWidget(self._chart_cgc,  1, 1)
        tab_charts_layout.addLayout(charts_grid, 1)

        self._tabs.addTab(tab_charts, "Charts")

        # ── Buttons ──────────────────────────────────────────────────────
        btn_row = QHBoxLayout()
        self.gen_btn = QPushButton("Generate Table")
        self.gen_btn.setStyleSheet(
            "QPushButton { background:#1976d2; color:white; "
            "font-weight:bold; padding:6px 18px; border-radius:4px; }"
            "QPushButton:disabled { background:#888; }")
        self.gen_btn.clicked.connect(self._start_loading)
        btn_row.addWidget(self.gen_btn)

        self.prog = QProgressBar()
        self.prog.setRange(0, len(run_list))
        self.prog.setValue(0)
        self.prog.setVisible(False)
        btn_row.addWidget(self.prog, 1)

        export_btn = QPushButton("Export CSV")
        export_btn.clicked.connect(self._export_csv)
        btn_row.addWidget(export_btn)

        mail_btn = QPushButton("Send as Mail")
        mail_btn.clicked.connect(self._send_mail)
        btn_row.addWidget(mail_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        self.neg_fg  = QColor("#ef5350" if is_dark else "#c62828")
        self.zero_fg = QColor("#66bb6a" if is_dark else "#2e7d32")
        self.pos_fg  = QColor("#66bb6a" if is_dark else "#2e7d32")
        self._done_count = 0

    # ── Loading ──────────────────────────────────────────────────────────

    def _start_loading(self):
        if not self._run_list:
            return
        self.gen_btn.setEnabled(False)
        self.tbl.setRowCount(0)
        self._done_count = 0
        self._pending = list(self._run_list)
        self.prog.setValue(0)
        self.prog.setVisible(True)
        self.status_lbl.setText(
            "Loading 1/" + str(len(self._run_list)) + "...")
        self._load_next()

    def _load_next(self):
        if not self._pending:
            self.status_lbl.setText(
                "Done. " + str(self.tbl.rowCount()) + " rows loaded.")
            self.prog.setVisible(False)
            self.gen_btn.setEnabled(True)
            self._draw_charts()
            return
        blk, run_path, run_name, runtime, source = self._pending.pop(0)
        self.status_lbl.setText(
            "Loading " + blk + " (" + run_name + ")... " +
            str(self._done_count + 1) + "/" + str(len(self._run_list)))
        try:
            from workers import MetricWorker
            w = MetricWorker(run_path, blk, "FE", source)
            w.finished.connect(
                lambda m, b=blk, rn=run_name, rt=runtime:
                self._on_row_done(b, rn, rt, m))
            w.start()
            self._active_worker = w
        except Exception as e:
            self._add_row(blk, run_name, runtime, {})
            self._done_count += 1
            self.prog.setValue(self._done_count)
            QTimer.singleShot(10, self._load_next)

    def _on_row_done(self, blk, run_name, runtime, metrics):
        self._add_row(blk, run_name, runtime, metrics)
        self._done_count += 1
        self.prog.setValue(self._done_count)
        QTimer.singleShot(10, self._load_next)

    # ── Row builder ──────────────────────────────────────────────────────

    def _add_row(self, blk, run_name, runtime, metrics):
        area = metrics.get("area", {})

        def _v(*keys):
            for src in [area, metrics]:
                for k in keys:
                    v = src.get(k)
                    if v and str(v).strip() not in ("-", ""):
                        return str(v)
            return "-"

        # MBIT
        mbit = metrics.get("mbit", area.get("mbit", "-"))
        if mbit != "-":
            try:
                mbit = "{:.2f}%".format(float(str(mbit).rstrip('%')))
            except Exception:
                pass

        # CGC
        cgc = metrics.get("cgc", "-")
        if cgc != "-":
            pct = re.search(r"(\d+\.?\d*)%", str(cgc))
            cgc = pct.group(1) + "%" if pct else cgc

        # Instance count
        inst = _v("instance_count", "total_count")

        # Std Cell Area
        std_area = _v("std_cell_area", "combinational_area")

        # Gate Count
        gc = _v("gate_count")
        if gc == "-":
            try:
                gc = str(int(float(std_area) / 0.2419))
            except Exception:
                gc = "-"

        # VTH — use new flat structure from parse_cell_usage
        vth_data = metrics.get("vth", {})
        vth_str  = vth_data.get("lvt_rvt_hvt_area",
                    vth_data.get("lvt_rvt_area", "-/-"))

        # R2R timing
        r2r_setup   = metrics.get("r2r_setup",    "-")
        r2r_hold    = metrics.get("r2r_hold",     "-")
        logic_depth = metrics.get("logic_depth",  "-")

        # Runtime: prefer fresh value from metrics over passed-in runtime arg
        rt = metrics.get("runtime", runtime) or runtime or "-"

        vals = [blk, run_name, mbit, cgc, inst, std_area,
                gc, vth_str, r2r_setup, r2r_hold, logic_depth, rt]

        # run_path stored in metrics — need it for double-click open
        _run_path = metrics.get("run_dir", "")

        self.tbl.setSortingEnabled(False)
        r = self.tbl.rowCount()
        self.tbl.insertRow(r)

        for c, val in enumerate(vals):
            item = QTableWidgetItem(str(val) if val else "-")
            item.setTextAlignment(Qt.AlignCenter)
            if c in (0, 1):
                item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                if c == 0:
                    f2 = item.font(); f2.setBold(True); item.setFont(f2)
                    # Store run_path on col-0 for double-click gvim open
                    item.setData(Qt.UserRole, _run_path)
            # Color R2R Setup WNS (col 8)
            if c == self._COL_R2R_SETUP:
                try:
                    wv = float(str(val).split("/")[0])
                    if wv < 0:
                        item.setForeground(self.neg_fg)
                    elif wv == 0.0:
                        item.setForeground(self.zero_fg)
                    else:
                        item.setForeground(self.pos_fg)
                except Exception:
                    pass
            self.tbl.setItem(r, c, item)
        self.tbl.setSortingEnabled(True)

    # ── Charts (PyQt5 native) ─────────────────────────────────────────────

    def _draw_charts(self):
        if self.tbl.rowCount() == 0:
            return
        n = self.tbl.rowCount()

        def _cell(r, c):
            it = self.tbl.item(r, c)
            return it.text() if it else "-"

        blks      = [_cell(r, 0) for r in range(n)]
        run_names = [_cell(r, 1) for r in range(n)]
        labels    = [rn[:14] + ".." if len(rn) > 16 else rn
                     for rn in run_names]
        std_areas, r2r_wns, cgc_vals = [], [], []
        vth_lvt, vth_rvt, vth_hvt   = [], [], []

        for r in range(n):
            try:   std_areas.append(float(_cell(r, 5)))
            except: std_areas.append(0.0)
            try:   r2r_wns.append(float(_cell(r, self._COL_R2R_SETUP).split("/")[0]))
            except: r2r_wns.append(0.0)
            try:   cgc_vals.append(float(_cell(r, 3).rstrip('%')))
            except: cgc_vals.append(0.0)
            parts = _cell(r, 7).replace('%', '').split('/')
            try:
                vth_lvt.append(float(parts[0]) if len(parts) > 0 else 0.0)
                vth_rvt.append(float(parts[1]) if len(parts) > 1 else 0.0)
                vth_hvt.append(float(parts[2]) if len(parts) > 2 else 0.0)
            except:
                vth_lvt.append(0.0); vth_rvt.append(0.0); vth_hvt.append(0.0)

        wns_colors = [QColor("#ef5350") if v < 0 else QColor("#66bb6a") for v in r2r_wns]

        vt_rows = list(zip(vth_lvt, vth_rvt, vth_hvt))
        self._chart_vt.set_data(labels, vt_rows, self.is_dark)
        self._chart_area.set_data(labels, std_areas, is_dark=self.is_dark)
        self._chart_wns.set_data(labels, r2r_wns, colors=wns_colors, is_dark=self.is_dark)
        self._chart_cgc.set_data(labels, cgc_vals,
                                  colors=[QColor("#ffa726")] * n, is_dark=self.is_dark)

    # ── Open cell report in gvim ──────────────────────────────────────────

    _COL_REPORT = {
        2:  ["multibit_banking_ratio.*.rpt"],
        3:  ["clock_gating_info.mission.rpt", "clock_gating_info.*.rpt"],
        7:  ["cell_usage.summary.*.rpt"],
        8:  ["qor.*.rpt"],
        9:  ["qor.*.rpt"],
        10: ["report_logic_depth.summary.*.rpt"],
        11: ["runtime.V2.rpt"],
    }

    def _open_cell_report(self, item):
        c = item.column()
        col0 = self.tbl.item(item.row(), 0)
        run_path = col0.data(Qt.UserRole) if col0 else None
        pats = self._COL_REPORT.get(c)
        if not pats or not run_path:
            return
        try:
            from metric_extract import _find_rpt
            rpt = _find_rpt(os.path.join(run_path, "reports"), pats)
            if rpt and os.path.exists(rpt):
                subprocess.Popen(['gvim', rpt])
            else:
                QMessageBox.information(
                    self, "Not Found",
                    "No report found for this column.\nRun path: " + str(run_path))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    # ── Mail ─────────────────────────────────────────────────────────────

    def _send_mail(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(self, "Mail", "Generate table first.")
            return
        nc = self.tbl.columnCount()
        headers = [self.tbl.horizontalHeaderItem(c).text() for c in range(nc)]
        # Build HTML table for proper alignment in mail
        html = ["<table border='1' cellpadding='4' cellspacing='0' "
                "style='border-collapse:collapse;font-family:monospace;font-size:12px;'>"]
        html.append("<tr>" + "".join(
            "<th style='background:#1976d2;color:white;'>{}</th>".format(h)
            for h in headers) + "</tr>")
        for r in range(self.tbl.rowCount()):
            html.append("<tr>" + "".join(
                "<td>{}</td>".format(
                    self.tbl.item(r, c).text() if self.tbl.item(r, c) else "")
                for c in range(nc)) + "</tr>")
        html.append("</table>")
        html_body = "\n".join(html)
        parent = self.parent()
        if parent and hasattr(parent, '_open_mail_compose_dialog'):
            parent._open_mail_compose_dialog(
                subject="Block Summary: " + self.windowTitle(),
                body=html_body,
                html_body=html_body)
        else:
            QMessageBox.information(self, "Mail Body", html_body[:3000])

    # ── Export CSV ───────────────────────────────────────────────────────

    def _export_csv(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(
                self, "Export", "No data yet. Click Generate first.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export", "block_summary.csv", "CSV Files (*.csv)")
        if not path:
            return
        try:
            import csv as _csv
            with open(path, "w", newline="") as f:
                w = _csv.writer(f)
                hdrs = [
                    self.tbl.horizontalHeaderItem(c).text()
                    for c in range(self.tbl.columnCount())]
                w.writerow(hdrs)
                for r in range(self.tbl.rowCount()):
                    row = [
                        self.tbl.item(r, c).text()
                        if self.tbl.item(r, c) else ""
                        for c in range(self.tbl.columnCount())]
                    w.writerow(row)
            QMessageBox.information(self, "Export", "Saved: " + path)
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))


class PDDashboard(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Singularity PD | Pro Edition")
        self.resize(1280, 720)
        self.setMinimumSize(800, 600)

        # -- data ---------------------------------------------------------
        self.ws_data      = {}
        self.out_data     = {}
        self.ir_data      = {}
        self.global_notes = {}
        self.user_pins    = load_user_pins()

        # -- theme/display ------------------------------------------------
        self.is_dark_mode          = False
        self.use_custom_colors     = False
        self.custom_bg_color       = "#2b2d30"
        self.custom_fg_color       = "#dfe1e5"
        self.custom_sel_color      = "#2f65ca"
        self.row_spacing           = 2
        self.show_relative_time    = False
        self.convert_to_ist        = False
        self.hide_block_nodes      = False

        # -- worker/state -------------------------------------------------
        self.size_workers           = []
        self._stage_workers         = []
        self.item_map               = {}
        self._signoff_items_by_path  = {}
        self._signoff_worker         = None
        self._signoff_bg_done        = False
        self._last_view_preset       = "All Runs"
        self.ignored_paths          = set()
        self._checked_paths         = set()
        self.current_error_log_path = None
        self._building_tree         = False
        self._last_stylesheet       = ""
        self._closure_enabled       = prefs.get(
            'UI', 'closure_enabled', fallback='true').lower() != 'false' 
        self._columns_fitted_once   = False
        self._initial_size_calc_done= False
        self._last_scan_time        = ""
        self.run_filter_config      = None
        self.current_config_path    = None
        self.active_col_filters     = {}
        self._tree_builder          = None

        # -- milestone map (user-configurable in Settings > Milestones) --
        self._milestone_map = self._load_milestone_map()

        # -- load QoR script path from prefs if not in config.py --
        try:
            _ = QOR_SUMMARY_SCRIPT  # already defined in config.py
        except NameError:
            import builtins
            saved_qor = prefs.get('QOR', 'script_path', fallback='')
            if saved_qor:
                builtins.QOR_SUMMARY_SCRIPT = saved_qor

        # -- tapeout countdown --
        self._tapeout_date = None
        try:
            td = prefs.get('UI', 'tapeout_date', fallback='')
            if td:
                import datetime
                self._tapeout_date = datetime.datetime.strptime(td, '%Y-%m-%d')
        except Exception:
            pass
        # Timer to update title bar countdown every hour
        self._tapeout_timer = QTimer(self)
        self._tapeout_timer.setInterval(3600000)  # 1 hour
        self._tapeout_timer.timeout.connect(self._update_title)
        self._tapeout_timer.start()

        # -- run history for regression detection --
        self._run_history = self._load_run_history()

        # -- color palette (rebuilt on theme change) ----------------------
        self._colors = {
            "completed":   QColor("#1b5e20"), "running":     QColor("#0d47a1"),
            "not_started": QColor("#757575"), "interrupted": QColor("#e65100"),
            "failed":      QColor("#b71c1c"), "pass":        QColor("#388e3c"),
            "fail":        QColor("#d32f2f"), "outfeed":     QColor("#8e24aa"),
            "ws":          QColor("#e65100"), "milestone":   QColor("#1e88e5"),
            "note":        QColor("#e65100"),
        }

        # -- icons --------------------------------------------------------
        self.icons = {
            "golden":    self._create_dot_icon("#ffd700", "#b8860b"),
            "good":      self._create_dot_icon("#4caf50", "#388e3c"),
            "redundant": self._create_dot_icon("#f44336", "#c62828"),
            "later":     self._create_dot_icon("#9c27b0", "#6a1b9a"),
        }

        # -- search history -----------------------------------------------
        self._search_history = []
        try:
            h = prefs.get('UI', 'search_history', fallback='')
            if h:
                self._search_history = [x for x in h.split('|||') if x.strip()][:15]
        except Exception:
            pass

        # -- timers -------------------------------------------------------
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)

        self._smart_poll_timer = QTimer(self)
        self._smart_poll_timer.setSingleShot(False)
        self._smart_poll_timer.timeout.connect(self._smart_poll_running)

        self._live_timer = QTimer(self)
        self._live_timer.setInterval(60000)
        self._live_timer.timeout.connect(self._update_live_runtimes)
        self._live_timer.start()

        # -- preset sets (loaded from prefs or defaults) ------------------
        self._load_preset_sets()

        # -- col0 resize timer (throttled on expand/collapse) -------------
        self._col0_resize_timer = QTimer(self)
        self._col0_resize_timer.setSingleShot(True)
        self._col0_resize_timer.setInterval(150)

        self.init_ui()
        self._setup_shortcuts()
        self.apply_theme_and_spacing()
        QTimer.singleShot(250, self.start_fs_scan)
        # DiskScannerWorker runs `du -sk` on NFS — extremely I/O heavy.
        # Removed auto-start: it now only runs when user clicks "Disk Space".
        # This eliminates NFS contention that made all post-scan clicks sluggish.

    # ------------------------------------------------------------------
    # CLOSE
    # ------------------------------------------------------------------
    def closeEvent(self, event):
        if not prefs.has_section('UI'):
            prefs.add_section('UI')
        prefs.set('UI', 'main_splitter', ','.join(
            map(str, self.main_splitter.sizes())))
        prefs.set('UI', 'last_source',  self.src_combo.currentText())
        prefs.set('UI', 'last_rtl',     self.rel_combo.currentText())
        prefs.set('UI', 'last_view',    self.view_combo.currentText())
        prefs.set('UI', 'last_search',  self.search.text())
        prefs.set('UI', 'last_auto',    self.auto_combo.currentText())
        prefs.set('UI', 'search_history', '|||'.join(self._search_history[:15]))
        col_widths = ','.join(
            str(self.tree.columnWidth(i)) if not self.tree.isColumnHidden(i) else '0'
            for i in range(self.tree.columnCount()))
        col_hidden = ','.join(
            '1' if self.tree.isColumnHidden(i) else '0'
            for i in range(self.tree.columnCount()))
        prefs.set('UI', 'col_widths', col_widths)
        prefs.set('UI', 'col_hidden', col_hidden)
        with open(USER_PREFS_FILE, 'w') as f:
            prefs.write(f)
        os._exit(0)

    # ------------------------------------------------------------------
    # MILESTONE MAP (user-configurable)
    # ------------------------------------------------------------------
    def _load_milestone_map(self):
        """Load milestone pattern->label map from prefs.
        Default: _ML1_->INITIAL RELEASE, _ML2_->PRE-SVP, etc.
        User can add custom patterns like _ML0_->TAPE-IN."""
        import json
        default = {
            "_ML1_": "INITIAL RELEASE",
            "_ML2_": "PRE-SVP",
            "_ML3_": "SVP",
            "_ML4_": "FFN",
        }
        try:
            saved = prefs.get('MILESTONES', 'map', fallback='')
            if saved:
                loaded = json.loads(saved)
                if isinstance(loaded, dict) and loaded:
                    return loaded
        except Exception:
            pass
        return default

    def _save_milestone_map(self, m):
        import json
        if not prefs.has_section('MILESTONES'):
            prefs.add_section('MILESTONES')
        prefs.set('MILESTONES', 'map', json.dumps(m))
        with open(USER_PREFS_FILE, 'w') as f:
            prefs.write(f)

    def get_milestone_label(self, rtl_str):
        """Apply user-defined milestone map to an RTL string."""
        for pattern, label in self._milestone_map.items():
            if pattern in rtl_str:
                return label
        return None

    # ------------------------------------------------------------------
    # RUN HISTORY + REGRESSION DETECTION  (FEAT 3 + 5)
    # ------------------------------------------------------------------
    def _history_file(self):
        """JSON file storing per-run completion history."""
        try:
            base = os.path.dirname(USER_PREFS_FILE)
        except Exception:
            base = os.path.expanduser("~")
        return os.path.join(base, "run_history.json")

    def _load_run_history(self):
        """Load run history dict: {run_key: [{status, runtime, fm, vslp, ts}]}"""
        import json
        try:
            with open(self._history_file(), 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_run_history(self):
        """Save run history in a daemon thread — never block the main thread on NFS write."""
        import json, threading
        data = dict(self._run_history)   # shallow snapshot is safe (values are lists)
        fp   = self._history_file()
        def _write():
            try:
                with open(fp, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass
        threading.Thread(target=_write, daemon=True).start()

    def _record_run_history(self, run):
        """Record completed run metrics for regression detection."""
        import datetime
        if not run.get("is_comp"):
            return
        key = f"{run['block']}|{run['r_name']}"
        entry = {
            "ts":      datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "status":  run.get("fe_status", ""),
            "runtime": run.get("info", {}).get("runtime", ""),
            "fm_n":    run.get("st_n", ""),
            "fm_u":    run.get("st_u", ""),
            "vslp":    run.get("vslp_status", ""),
            "rtl":     run.get("rtl", ""),
        }
        if key not in self._run_history:
            self._run_history[key] = []
        # Keep last 20 entries per run
        self._run_history[key].append(entry)
        self._run_history[key] = self._run_history[key][-20:]

    def _check_regression(self, run):
        """Compare run to previous entry. Return (has_regression, message).
        All helpers are module-level -- no imports or closures per call."""
        key = f"{run['block']}|{run['r_name']}"
        history = self._run_history.get(key, [])
        if len(history) < 2:
            return False, ""
        prev = history[-2]
        curr = history[-1]
        issues = []
        # Runtime regression: >15% increase
        def _mins(rt):
            m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
            return int(m.group(1))*60+int(m.group(2)) if m else None
        pm, cm = _mins(prev.get("runtime","")), _mins(curr.get("runtime",""))
        if pm and cm and pm > 0 and cm > pm * 1.15:
            issues.append(
                f"Runtime +{int((cm-pm)/pm*100)}%"
                f" ({prev['runtime']} -> {curr['runtime']})")
        # FM regression
        if ("PASS" in prev.get("fm_n","").upper()
                and "FAIL" in curr.get("fm_n","").upper()):
            issues.append("FM-NONUPF PASS->FAILS")
        if ("PASS" in prev.get("fm_u","").upper()
                and "FAIL" in curr.get("fm_u","").upper()):
            issues.append("FM-UPF PASS->FAILS")
        # VSLP regression
        def _verr(v):
            m = re.search(r'Error:\s*(\d+)', v or "")
            return int(m.group(1)) if m else 0
        pe, ce = _verr(prev.get("vslp","")), _verr(curr.get("vslp",""))
        if ce > pe and pe == 0:
            issues.append(f"VSLP errors 0->{ce}")
        elif ce > pe*1.5 and pe > 0:
            issues.append(f"VSLP errors {pe}->{ce}")
        return (True, " | ".join(issues)) if issues else (False, "")

    def _get_run_history_text(self, run):
        """Return formatted history string for inspector panel."""
        key = f"{run['block']}|{run['r_name']}"
        history = self._run_history.get(key, [])
        if not history:
            return "No history yet."
        lines = []
        for h in reversed(history[-5:]):
            lines.append(f"{h['ts']}  {h['status']}  {h['runtime']}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # TAPEOUT COUNTDOWN
    # ------------------------------------------------------------------
    def _update_title(self):
        import datetime
        base = "Singularity PD | Pro Edition"
        if self._tapeout_date:
            delta = self._tapeout_date - datetime.datetime.now()
            days  = delta.days
            if days > 0:
                self.setWindowTitle(f"{base}  [T-{days} days]")
            elif days == 0:
                self.setWindowTitle(f"{base}  [TAPEOUT TODAY]")
            else:
                self.setWindowTitle(f"{base}  [T+{abs(days)} days post-tapeout]")
        else:
            self.setWindowTitle(base)

    # ------------------------------------------------------------------
    # PIN ICONS -- apply/refresh without full rebuild
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # ON-DEMAND METRIC EXTRACTION (MetricWorker)
    # ------------------------------------------------------------------
    def _launch_metric_worker(self, item):
        """Start MetricWorker for the selected run or stage item.
        Shows a progress dialog while running, then opens QoRSummaryDialog."""
        run_path  = item.text(15)
        run_name  = item.text(0)
        is_stage  = item.data(0, Qt.UserRole) == "STAGE"
        source    = item.text(2)
        dark      = (self.is_dark_mode
                     or (self.use_custom_colors
                         and self.custom_bg_color < "#888888"))

        if not run_path or run_path == "N/A":
            QMessageBox.information(
                self, "QoR Summary", "No path available for this item.")
            return

        if is_stage:
            stage_name = item.text(0)
            # run_path for stage is the stage dir -- we need BE run dir
            parent = item.parent()
            be_run_path = parent.text(15) if parent else run_path
            run_type = "BE"
            actual_path = be_run_path
        else:
            stage_name  = None
            run_type    = "FE"
            actual_path = run_path

        # Show progress indicator in status bar
        self.status_bar.showMessage(
            f"Extracting QoR metrics for {run_name}...")
        self.setEnabled(False)

        self._metric_worker = MetricWorker(
            actual_path, item.data(0, Qt.UserRole + 2) or "",
            run_type, source, stage_name)
        self._metric_item_name = run_name
        self._metric_dark      = dark
        self._metric_worker.finished.connect(self._on_metric_done)
        self._metric_worker.start()

    def _on_metric_done(self, metrics):
        """Called when MetricWorker finishes -- show the summary dialog."""
        self.setEnabled(True)
        self.status_bar.clearMessage()

        if "_error" in metrics:
            err_msg = str(metrics.get("_error", "Unknown"))
            QMessageBox.warning(
                self, "QoR Summary",
                "Error extracting metrics:\n" + err_msg)

        dlg = QoRSummaryDialog(
            self._metric_item_name, metrics,
            self._metric_dark, self)
        dlg.exec_()

    def _fmt_ts(self, raw):
        """Apply IST conversion and/or relative formatting to a raw timestamp."""
        val = raw or ""
        if not val or val in ("-", "N/A", "Unknown"):
            return val
        if self.convert_to_ist:
            val = convert_kst_to_ist_str(val)
        if self.show_relative_time:
            val = relative_time(val)
        return val

    def _refresh_timestamps(self):
        """Re-apply IST/relative format to all timestamp columns using stored
        raw values (UserRole+40/41). No tree rebuild needed."""
        GROUP = frozenset(("BLOCK", "MILESTONE", "RTL",
                           "IGNORED_ROOT", "STANDALONE_ROOT", "__PLACEHOLDER__"))
        def _walk(node):
            for i in range(node.childCount()):
                item = node.child(i)
                if item.data(0, Qt.UserRole) not in GROUP:
                    s_raw = item.data(0, Qt.UserRole + 40) or ""
                    e_raw = item.data(0, Qt.UserRole + 41) or ""
                    item.setText(13, self._fmt_ts(s_raw))
                    item.setText(14, self._fmt_ts(e_raw))
                _walk(item)
        _walk(self.tree.invisibleRootItem())

    def _ensure_standalone_root(self, root):
        """Get or create the Standalone PNR Runs top-level node."""
        for i in range(root.childCount()):
            if root.child(i).data(0, Qt.UserRole) == "STANDALONE_ROOT":
                return root.child(i)
        node = QTreeWidgetItem(root)
        node.setText(0, "[ Standalone PNR Runs ]")
        node.setData(0, Qt.UserRole, "STANDALONE_ROOT")
        node.setToolTip(0, "BE/Innovus runs with no matching FE parent run")
        node.setFlags(Qt.ItemIsEnabled)
        f = node.font(0)
        f.setBold(True)
        node.setFont(0, f)
        return node

    def _apply_pin_icons(self):
        """Walk all tree items and set/clear pin icons from self.user_pins.
        Called after any pin change so icons appear immediately."""
        GROUP = frozenset(("BLOCK","RTL","MILESTONE",
                           "IGNORED_ROOT","STANDALONE_ROOT","__PLACEHOLDER__"))
        _UR = Qt.UserRole
        def _walk(node):
            for i in range(node.childCount()):
                child = node.child(i)
                nt    = child.data(0, _UR)
                if nt not in GROUP:
                    path     = child.text(15)
                    pin_type = self.user_pins.get(path)
                    if pin_type and pin_type in self.icons:
                        child.setIcon(0, self.icons[pin_type])
                        child.setData(0, Qt.UserRole + 5, pin_type)
                    else:
                        child.setIcon(0, QIcon())
                        child.setData(0, Qt.UserRole + 5, None)
                _walk(child)
        _walk(self.tree.invisibleRootItem())

    # ------------------------------------------------------------------
    # BACKGROUND LOG-PATH CACHE WARM-UP
    # ------------------------------------------------------------------
    def _prefetch_log_paths(self):
        """Collect all log + error-log paths from tree items and prefetch
        them in background threads so the first click on any item is
        instant (no blocking NFS stat on the main thread)."""
        paths = []
        _GROUP = frozenset(("BLOCK","MILESTONE","RTL",
                            "IGNORED_ROOT","STANDALONE_ROOT","__PLACEHOLDER__"))
        _UR   = Qt.UserRole
        _UR10 = Qt.UserRole + 10
        def _collect(node):
            for i in range(node.childCount()):
                child = node.child(i)
                nt = child.data(0, _UR)
                if nt not in _GROUP:
                    lv = child.text(16)
                    if lv and lv not in ("N/A", ""):
                        paths.append(lv)
                    # Also prefetch error log path for FE runs
                    run = child.data(0, _UR10)
                    if run and run.get("run_type") == "FE":
                        rp = run.get("path", "")
                        if rp and rp != "N/A":
                            paths.append(os.path.join(
                                rp, "logs", "compile_opt.error.log"))
                _collect(child)
        _collect(self.tree.invisibleRootItem())
        if paths:
            prefetch_path_cache(paths)

    # ------------------------------------------------------------------
    # CLOSURE PASS (deferred -- runs after tree is fully painted)
    # ------------------------------------------------------------------
    def _run_closure_pass(self):
        """Apply closure scorecard + regression detection to all FE items.
        Deferred via QTimer so it doesn't block the initial tree paint.
        Only walks run items (skips group nodes) for maximum speed."""
        GROUP = frozenset(("BLOCK","MILESTONE","RTL",
                           "IGNORED_ROOT","STANDALONE_ROOT","STAGE","__PLACEHOLDER__"))
        _UR   = Qt.UserRole
        _UR10 = Qt.UserRole + 10
        count = [0]

        def _walk(node):
            for i in range(node.childCount()):
                child = node.child(i)
                nt = child.data(0, _UR)
                if nt in GROUP:
                    _walk(child)
                    continue
                if nt is not None:
                    continue
                # FE run item only
                run = child.data(0, _UR10)
                if not run or run.get("run_type") != "FE":
                    _walk(child)
                    continue
                # Closure scorecard
                self._update_closure_on_item(child)
                # Regression check (only for completed runs with history)
                if run.get("is_comp"):
                    has_reg, msg = self._check_regression(run)
                    if has_reg:
                        child.setToolTip(
                            0, child.toolTip(0) + f"\n[REGRESSION] {msg}")
                        child.setForeground(0, QColor("#f57c00"))
                        child.setData(0, Qt.UserRole + 30, msg)
                # Yield to Qt every 50 items to keep UI responsive
                count[0] += 1
                if count[0] % 50 == 0:
                    QApplication.processEvents()
                _walk(child)

        _walk(self.tree.invisibleRootItem())

    # ------------------------------------------------------------------
    # SIGN-OFF CLOSURE SCORECARD
    # ------------------------------------------------------------------
    def _closure_score(self, run_item):
        """Return (score 0-6, label string) for the 6 sign-off items.
        G=green/pass  R=red/fail  .=grey/not run"""
        scores = []
        labels = []
        checks = [
            ("FM-N",  run_item.text(7)),   # col 7 FM NONUPF
            ("FM-U",  run_item.text(8)),   # col 8 FM UPF
            ("VSLP",  run_item.text(9)),   # col 9 VSLP
            ("STA",   run_item.text(20)),  # col 20 STA rpt path
            ("IR-S",  run_item.text(10)),  # col 10 Static IR
            ("IR-D",  run_item.text(11)),  # col 11 Dynamic IR
        ]
        for name, val in checks:
            v = val.strip().upper()
            if not v or v in ("-", "N/A", ""):
                scores.append(0)
                labels.append(f"{name}:?")
            elif ("PASS" in v or "ERROR: 0" in v or "PASS" in v):
                scores.append(2)
                labels.append(f"{name}:OK")
            elif ("FAIL" in v or "ERROR:" in v):
                scores.append(1)
                labels.append(f"{name}:FAIL")
            else:
                scores.append(0)
                labels.append(f"{name}:?")
        total_pass = sum(1 for s in scores if s == 2)
        return total_pass, scores, labels

    def _update_closure_on_item(self, item):
        """Add closure summary to run item tooltip."""
        if item.data(0, Qt.UserRole) in (
                "BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                "STAGE","__PLACEHOLDER__"):
            return
        run = item.data(0, Qt.UserRole + 10)
        if not run or run.get("run_type") != "FE":
            return
        total_pass, scores, labels = self._closure_score(item)
        dot_chars = []
        for s in scores:
            if s == 2:   dot_chars.append("(OK)")
            elif s == 1: dot_chars.append("(FAIL)")
            else:        dot_chars.append("(?)")
        summary = f"Closure: {total_pass}/6  " + "  ".join(labels)
        old_tip = item.toolTip(0)
        # Replace or append closure line (re already imported at module level)
        if "Closure:" in old_tip:
            new_tip = re.sub(r"Closure:.*", summary, old_tip)
        else:
            new_tip = old_tip + "\n" + summary
        item.setToolTip(0, new_tip)
        # Color the run name based on closure completeness
        if total_pass == 6:
            item.setForeground(0, QColor("#388e3c"))  # all green
        elif total_pass == 0 and any(s==1 for s in scores):
            item.setForeground(0, QColor("#d32f2f"))  # all failing

    # ------------------------------------------------------------------
    # ICONS
    # ------------------------------------------------------------------
    def _create_dot_icon(self, fill, border, size=12):
        # Cache: only 6 distinct status colors, reuse QIcon objects
        key = (fill, border, size)
        if not hasattr(self, '_dot_icon_cache'):
            self._dot_icon_cache = {}
        cached = self._dot_icon_cache.get(key)
        if cached:
            return cached
        px = QPixmap(size, size)
        px.fill(Qt.transparent)
        p = QPainter(px)
        p.setRenderHint(QPainter.Antialiasing)
        p.setBrush(QBrush(QColor(fill)))
        p.setPen(QPen(QColor(border), 1.2))
        p.drawEllipse(1, 1, size - 2, size - 2)
        p.end()
        icon = QIcon(px)
        self._dot_icon_cache[key] = icon
        return icon

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(6, 6, 6, 4)
        root_layout.setSpacing(4)

        # ---- TOOLBAR ----
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
        self.view_combo.addItems([
            "All Runs", "FE Only", "BE Only",
            "Running Only", "Failed Only", "Today's Runs",
            "Pinned Only", "Selected Only"])
        self.view_combo.setFixedWidth(120)
        self._last_view_preset = self.view_combo.currentText()
        self.view_combo.currentIndexChanged.connect(self._on_view_changed)
        top_layout.addWidget(self.view_combo)

        self._add_separator(top_layout)

        self.search = QLineEdit()
        self.search.setPlaceholderText(
            "Search runs, blocks, status, runtime...  [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.textChanged.connect(lambda: self.search_timer.start(250))
        self.search.setContextMenuPolicy(Qt.CustomContextMenu)
        self.search.customContextMenuRequested.connect(
            self._show_search_history)
        top_layout.addWidget(self.search)

        # Search result count label
        self.search_count_lbl = QLabel("")
        self.search_count_lbl.setFixedWidth(70)
        self.search_count_lbl.setStyleSheet(
            "font-size: 11px; color: #1976d2; font-weight: bold;")
        self.search_count_lbl.setVisible(False)
        top_layout.addWidget(self.search_count_lbl)

        top_layout.addStretch()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.start_fs_scan)
        top_layout.addWidget(self.refresh_btn)

        self.auto_combo = QComboBox()
        self.auto_combo.addItems(["Off", "1 Min", "5 Min", "10 Min"])
        self.auto_combo.setFixedWidth(75)
        self.auto_combo.currentIndexChanged.connect(self.on_auto_refresh_changed)
        top_layout.addWidget(self.auto_combo)

        self._add_separator(top_layout)

        # Utilities menu (renamed from Actions)
        self.actions_btn  = QPushButton("Utilities  v")
        self.actions_menu = QMenu(self)

        self.actions_menu.addAction("Fit Columns",             self.fit_all_columns)
        self.actions_menu.addAction("Expand All",              self.safe_expand_all)
        self.actions_menu.addAction("Collapse All",            self.safe_collapse_all)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Calculate All Run Sizes", self.calculate_all_sizes)
        self.actions_menu.addAction("Export to CSV",           self.export_csv)
        self.actions_menu.addAction("Compare QoR",             self.run_qor_comparison)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Block Summary Table",     self.open_block_summary)
        self.actions_menu.addSeparator()

        mail_menu = self.actions_menu.addMenu("Send Mail...")
        mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        mail_menu.addAction("Send Compare QoR Mail",        self.send_qor_mail_action)
        mail_menu.addAction("Send Custom Mail",             self.send_custom_mail_action)
        self.actions_menu.addSeparator()

        filt_menu = self.actions_menu.addMenu("Filter Configs...")
        filt_menu.addAction("Load Run Filter Config...", self.load_filter_config)
        filt_menu.addAction("Clear Run Filter Config",   self.clear_filter_config)
        filt_menu.addAction("Generate Sample Config",    self.generate_sample_config)
        self.actions_menu.addSeparator()

        self.actions_menu.addAction("Disk Space",              self.open_disk_usage)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Failed Runs Digest",      self.show_failed_digest)
        self.actions_menu.addAction("Timeline Overview",       self.show_selected_timeline_overview)
        self.actions_menu.addAction("Compare Selected Runs",   self.show_run_diff)
        self.actions_menu.addAction("RoR Metric Diff",         self.show_ror_metric_diff)
        self.actions_menu.addAction("Golden Benchmark",        self.show_golden_benchmark)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Analytics / Charts",      self.show_analytics)
        self.actions_menu.addAction("Team Workload View",       self.show_team_workload)

        self.actions_btn.setMenu(self.actions_menu)
        top_layout.addWidget(self.actions_btn)

        # Settings button -- always visible in toolbar
        self.settings_btn = QPushButton("Settings")
        self.settings_btn.clicked.connect(self.open_settings)
        top_layout.addWidget(self.settings_btn)

        self._add_separator(top_layout)

        # Mode dropdown
        top_layout.addWidget(self._label("Mode:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Standard", "Compact", "Full"])
        self.mode_combo.setFixedWidth(82)
        self.mode_combo.setToolTip(
            "Column view preset  (keys: 1=Compact  2=Standard  3=Full)")
        self.mode_combo.currentIndexChanged.connect(
            lambda i: self._set_col_preset(
                {"Standard": 2, "Compact": 1, "Full": 3}.get(
                    self.mode_combo.currentText(), 2)))
        top_layout.addWidget(self.mode_combo)

        self._add_separator(top_layout)

        # Notes toggle button
        self.notes_toggle_btn = QPushButton("[ Notes >> ]")
        self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock)
        top_layout.addWidget(self.notes_toggle_btn)

        root_layout.addLayout(top_layout)

        # ---- PROGRESS BAR ----
        self.prog_container = QWidget()
        self.prog_container.setFixedHeight(30)
        self.prog_container.setVisible(False)
        prog_layout = QHBoxLayout(self.prog_container)
        prog_layout.setContentsMargins(4, 0, 4, 0)
        self.prog_lbl = QLabel("Initializing Scanner...")
        self.prog_lbl.setStyleSheet("color: #1976D2; font-weight: bold;")
        self.prog = QProgressBar()
        self.prog.setFixedHeight(6)
        self.prog.setTextVisible(False)
        self.prog.setStyleSheet(
            "QProgressBar { border: none; border-radius: 3px; background: #ddd; }"
            "QProgressBar::chunk { background: #1976D2; border-radius: 3px; }")
        prog_layout.addWidget(self.prog_lbl)
        prog_layout.addWidget(self.prog, 1)
        root_layout.addWidget(self.prog_container)

        # ---- HEALTH STRIP ----
        self.health_strip = QWidget()
        self.health_strip.setFixedHeight(28)
        hs_layout = QHBoxLayout(self.health_strip)
        hs_layout.setContentsMargins(4, 2, 4, 2)
        hs_layout.setSpacing(6)

        def _badge(label, color, view_filter):
            btn = QPushButton(label)
            btn.setObjectName("healthBadge")
            btn.setFixedHeight(22)
            btn.setCursor(Qt.PointingHandCursor)
            btn.setStyleSheet(
                "QPushButton#healthBadge { background: " + color + "18; color: " + color + "; "
                "border: 1px solid " + color + "55; border-radius: 10px; "
                "padding: 0 10px; font-size: 11px; font-weight: bold; }"
                "QPushButton#healthBadge:hover { background: " + color + "33; }")
            btn.clicked.connect(
                lambda _, vf=view_filter: self.view_combo.setCurrentText(vf))
            return btn

        self.badge_completed = _badge("Completed: 0", "#388e3c", "All Runs")
        self.badge_running   = _badge("Running: 0",   "#1976d2", "Running Only")
        self.badge_failed    = _badge("Failed: 0",    "#d32f2f", "Failed Only")
        # Keep only 3 badges -- Completed / Running / Failed
        for b in [self.badge_completed, self.badge_running, self.badge_failed]:
            hs_layout.addWidget(b)
        hs_layout.addStretch()
        self.lbl_scan_stats = QLabel("")
        self.lbl_scan_stats.setStyleSheet("font-size: 11px; color: gray;")
        hs_layout.addWidget(self.lbl_scan_stats)
        root_layout.addWidget(self.health_strip)

        # ---- SPLITTER ----
        self.main_splitter = QSplitter(Qt.Horizontal)

        # LEFT PANEL
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
        for b in [all_btn, none_btn]:
            b.setCursor(Qt.PointingHandCursor)
            b.setObjectName("linkBtn")
        all_btn.clicked.connect(lambda: self._set_all_blocks(True))
        none_btn.clicked.connect(lambda: self._set_all_blocks(False))
        blk_header.addWidget(all_btn)
        sep_lbl = self._label("|")
        sep_lbl.setStyleSheet("color: gray;")
        blk_header.addWidget(sep_lbl)
        blk_header.addWidget(none_btn)
        left_layout.addLayout(blk_header)

        self.blk_list = QListWidget()
        self.blk_list.setAlternatingRowColors(True)
        f = self.blk_list.font()
        f.setPointSize(f.pointSize() + 1)
        f.setBold(True)
        self.blk_list.setFont(f)
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(100))
        left_layout.addWidget(self.blk_list, 1)

        self.fe_error_btn = QPushButton("")
        self.fe_error_btn.setCursor(Qt.PointingHandCursor)
        self.fe_error_btn.setObjectName("errorLinkBtn")
        self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log)
        left_layout.addWidget(self.fe_error_btn)

        # META PANEL -- Path and Log only (Status removed -- visible in tree)
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel)
        meta_layout.setContentsMargins(0, 6, 0, 0)
        meta_layout.setSpacing(4)
        meta_layout.addWidget(QLabel("<b>Quick Info:</b>"))
        self.meta_run_name = QLabel("")
        self.meta_run_name.setWordWrap(True)
        self.meta_run_name.setStyleSheet(
            "font-weight: bold; font-size: 11px; color: #1976d2;")
        meta_layout.addWidget(self.meta_run_name)

        def _field_row(label_txt):
            grp = QWidget()
            gl = QVBoxLayout(grp)
            gl.setContentsMargins(0, 0, 0, 0)
            gl.setSpacing(1)
            hdr = QHBoxLayout()
            hdr.setContentsMargins(0, 0, 0, 0)
            hdr.setSpacing(4)
            lbl = QLabel(label_txt)
            lbl.setStyleSheet(
                "font-size: 11px; font-weight: bold; color: gray;")
            copy_btn = QPushButton("Copy")
            copy_btn.setFixedHeight(18)
            copy_btn.setFixedWidth(40)
            copy_btn.setObjectName("linkBtn")
            copy_btn.setStyleSheet(
                "QPushButton#linkBtn { font-size: 10px; padding: 0 2px; }")
            copy_btn.setCursor(Qt.PointingHandCursor)
            hdr.addWidget(lbl)
            hdr.addStretch()
            hdr.addWidget(copy_btn)
            gl.addLayout(hdr)
            field = QLineEdit()
            field.setReadOnly(True)
            field.setStyleSheet("font-size: 11px;")
            field.setAlignment(Qt.AlignLeft)
            copy_btn.clicked.connect(
                lambda _, f=field: QApplication.clipboard().setText(f.text())
                if f.text() else None)
            gl.addWidget(field)
            meta_layout.addWidget(grp)
            return field

        self.meta_status = QLineEdit()
        self.meta_status.setVisible(False)
        self.meta_path = _field_row("Run Path:")
        self.meta_log  = _field_row("Log File:")
        left_layout.addWidget(self.meta_panel, 0)

        self.main_splitter.addWidget(left_panel)

        # ---- TREE ----
        self.tree = QTreeWidget()
        self.tree.setColumnCount(24)
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)
        self.tree.setAnimated(False)
        self.tree.setExpandsOnDoubleClick(False)
        self.tree.setSortingEnabled(True)
        self.tree.header().setSectionsMovable(True)
        self.tree.header().setStretchLastSection(True)

        headers = [
            "Run Name (Select)", "RTL Release Version", "Source", "Status",
            "Stage", "User", "Size", "FM - NONUPF", "FM - UPF", "VSLP Status",
            "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT",
            "STA_RPT", "IR_LOG", "Alias / Notes", "Starred"
        ]
        self.tree.setHeaderLabels(headers)
        for i in range(self.tree.columnCount()):
            self.tree.headerItem().setTextAlignment(i, Qt.AlignCenter)

        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(
            self.on_header_context_menu)

        self.tree.setColumnWidth(0, 380);  self.tree.setColumnWidth(1, 260)
        self.tree.setColumnWidth(2, 90);   self.tree.setColumnWidth(3, 110)
        self.tree.setColumnWidth(4, 130);  self.tree.setColumnWidth(5, 100)
        self.tree.setColumnWidth(6, 80);   self.tree.setColumnWidth(7, 160)
        self.tree.setColumnWidth(8, 160);  self.tree.setColumnWidth(9, 200)
        self.tree.setColumnWidth(10, 100); self.tree.setColumnWidth(11, 100)
        self.tree.setColumnWidth(12, 110); self.tree.setColumnWidth(13, 120)
        self.tree.setColumnWidth(14, 120); self.tree.setColumnWidth(22, 300)

        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        self.tree.itemExpanded.connect(self.on_item_expanded)

        # Auto-fit Run Name column on expand/collapse (throttled 150ms)
        self._col0_resize_timer.timeout.connect(
            self._fit_run_name_column)
        self.tree.itemExpanded.connect(
            lambda _: self._col0_resize_timer.start())
        self.tree.itemCollapsed.connect(
            lambda _: self._col0_resize_timer.start())

        for i in [15, 16, 17, 18, 19, 20, 21, 23]:
            self.tree.setColumnHidden(i, True)

        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.tree.itemChanged.connect(self._on_item_check_changed)

        self.main_splitter.addWidget(self.tree)
        root_layout.addWidget(self.main_splitter)

        # ---- INSPECTOR DOCK ----
        self.inspector = QWidget()
        ins_layout = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view details.")
        self.ins_lbl.setWordWrap(True)
        self.ins_note = QTextEdit()
        self.ins_note.setPlaceholderText(
            "Enter aliases or personal notes here...\n\nVisible to all dashboard users.")
        self.ins_save_btn = QPushButton("Save Note")
        self.ins_save_btn.clicked.connect(self.save_inspector_note)
        ins_layout.addWidget(self.ins_lbl)
        ins_layout.addWidget(QLabel("<b>Shared Notes:</b>"))
        ins_layout.addWidget(self.ins_note)
        ins_layout.addWidget(self.ins_save_btn)

        self.inspector_dock = QDockWidget(self)
        self.inspector_dock.setAllowedAreas(
            Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.inspector_dock.setTitleBarWidget(QWidget())
        self.inspector_dock.setWidget(self.inspector)
        self.addDockWidget(Qt.RightDockWidgetArea, self.inspector_dock)
        self.inspector_dock.hide()

        # Restore splitter sizes and column state
        try:
            m_sizes = [int(x) for x in prefs.get(
                'UI', 'main_splitter', fallback='250,1200').split(',')]
            self.main_splitter.setSizes(m_sizes)
        except Exception:
            pass

        try:
            col_hidden_str = prefs.get('UI', 'col_hidden', fallback='')
            col_widths_str = prefs.get('UI', 'col_widths', fallback='')
            if col_hidden_str:
                hv = [x.strip() for x in col_hidden_str.split(',')]
                for i, h in enumerate(hv):
                    if i < self.tree.columnCount():
                        self.tree.setColumnHidden(i, h == '1')
            if col_widths_str:
                wv = [x.strip() for x in col_widths_str.split(',')]
                for i, w in enumerate(wv):
                    if i < self.tree.columnCount() and int(w) > 0:
                        self.tree.setColumnWidth(i, int(w))
        except Exception:
            pass

        # ---- STATUS BAR ----
        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(26)
        self.setStatusBar(self.status_bar)

        self.sb_total    = QLabel("Total: 0")
        self.sb_complete = QLabel("Completed: 0")
        self.sb_running  = QLabel("Running: 0")
        self.sb_selected = QLabel("Selected: 0")
        self.sb_selected.setCursor(Qt.PointingHandCursor)
        self.sb_selected.setToolTip("Click to show only selected (checked) runs")
        self.sb_selected.mousePressEvent = lambda e: self._toggle_selected_only()
        self.sb_scan_time = QLabel("")
        self.sb_config   = QLabel("Config: None")

        for lbl in [self.sb_total, self.sb_complete, self.sb_running,
                    self.sb_selected, self.sb_scan_time, self.sb_config]:
            lbl.setContentsMargins(8, 0, 8, 0)
            self.status_bar.addPermanentWidget(lbl)
            self.status_bar.addPermanentWidget(self._vsep())

        self.apply_theme_and_spacing()

    # ------------------------------------------------------------------
    # HELPER WIDGETS
    # ------------------------------------------------------------------
    def _label(self, text):
        l = QLabel(text)
        return l

    def _add_separator(self, layout):
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

    def _vsep(self):
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFrameShadow(QFrame.Sunken)
        return sep

    # ------------------------------------------------------------------
    # DOCK / EXPAND / COLLAPSE
    # ------------------------------------------------------------------
    def toggle_notes_dock(self):
        if self.inspector_dock.isVisible():
            self.inspector_dock.hide()
            self.notes_toggle_btn.setText("[ Notes >> ]")
        else:
            self.inspector_dock.show()
            self.notes_toggle_btn.setText("[ << Notes ]")

    def safe_expand_all(self):
        # Populate all lazy BE placeholders first, then expand
        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        root = self.tree.invisibleRootItem()
        ign_root = self._ensure_ign_root(root)

        _lazy_count = [0]
        def _load_all_lazy(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if child.childCount() == 1:
                    ph = child.child(0)
                    if ph.data(0, Qt.UserRole) == "__PLACEHOLDER__":
                        be_run = child.data(0, Qt.UserRole + 11)
                        if be_run:
                            child.removeChild(ph)
                            self._add_stages(child, be_run, ign_root)
                            _lazy_count[0] += 1
                            if _lazy_count[0] % 20 == 0:
                                QApplication.processEvents()
                _load_all_lazy(child)
        _load_all_lazy(root)
        self.tree.setUpdatesEnabled(True)
        self.tree.expandAll()
        self.tree.blockSignals(False)
        self.tree.resizeColumnToContents(0)

    def safe_collapse_all(self):
        self.tree.collapseAll()

    def _ensure_ign_root(self, root):
        for i in range(root.childCount()):
            if root.child(i).data(0, Qt.UserRole) == "IGNORED_ROOT":
                return root.child(i)
        return self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

    def _expand_to_rtl_level(self):
        """Expand to RTL/EVT level only -- BLOCK, MILESTONE, RTL open."""
        RTL_TYPES = frozenset(("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"))
        self.tree.setUpdatesEnabled(False)
        def _expand(node):
            for i in range(node.childCount()):
                child = node.child(i)
                nt = child.data(0, Qt.UserRole)
                if nt in RTL_TYPES:
                    child.setExpanded(True)
                    _expand(child)
                else:
                    child.setExpanded(False)
        _expand(self.tree.invisibleRootItem())
        self.tree.setUpdatesEnabled(True)
        self.tree.resizeColumnToContents(0)

    # ------------------------------------------------------------------
    # SHORTCUTS
    # ------------------------------------------------------------------
    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self,      self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self,      lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"), self,      self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"), self,      self.safe_collapse_all)
        QShortcut(QKeySequence("Ctrl+C"), self.tree, self._copy_tree_cell)
        QShortcut(QKeySequence("Ctrl+?"), self,      self.open_settings)
        QShortcut(QKeySequence("L"),      self,      self._shortcut_open_log)
        QShortcut(QKeySequence("D"),      self,      self._toggle_dark_mode)
        QShortcut(QKeySequence("1"),      self,      lambda: self._set_col_preset(1))
        QShortcut(QKeySequence("2"),      self,      lambda: self._set_col_preset(2))
        QShortcut(QKeySequence("3"),      self,      lambda: self._set_col_preset(3))
        # FEAT 7: Keyboard navigation between visible run items
        QShortcut(QKeySequence("N"),      self,      self._nav_next_run)
        QShortcut(QKeySequence("P"),      self,      self._nav_prev_run)
        QShortcut(QKeySequence("F"),      self,      self._nav_next_failed)

    def _get_visible_run_items(self):
        """Collect all visible FE run items in tree order."""
        items = []
        GROUP = frozenset(("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STANDALONE_ROOT",
                           "STAGE","__PLACEHOLDER__"))
        def collect(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if child.isHidden():
                    continue
                nt = child.data(0, Qt.UserRole)
                if nt in GROUP:
                    collect(child)
                elif nt is None:
                    run = child.data(0, Qt.UserRole + 10)
                    if run and run.get("run_type") == "FE":
                        items.append(child)
                    collect(child)
        collect(self.tree.invisibleRootItem())
        return items

    def _nav_to_item(self, item):
        """Select item, scroll to it, update inspector."""
        self.tree.setCurrentItem(item)
        self.tree.scrollToItem(item, QAbstractItemView.PositionAtCenter)

    def _nav_next_run(self):
        """N key: navigate to next visible FE run."""
        items = self._get_visible_run_items()
        if not items:
            return
        curr = self.tree.currentItem()
        if curr in items:
            idx = items.index(curr)
            self._nav_to_item(items[(idx + 1) % len(items)])
        else:
            self._nav_to_item(items[0])

    def _nav_prev_run(self):
        """P key: navigate to previous visible FE run."""
        items = self._get_visible_run_items()
        if not items:
            return
        curr = self.tree.currentItem()
        if curr in items:
            idx = items.index(curr)
            self._nav_to_item(items[(idx - 1) % len(items)])
        else:
            self._nav_to_item(items[-1])

    def _nav_next_failed(self):
        """F key: jump to next FAILED/FATAL ERROR run."""
        items = self._get_visible_run_items()
        failed = [it for it in items
                  if it.text(3) in ("FAILED","FATAL ERROR","INTERRUPTED")]
        if not failed:
            return
        curr = self.tree.currentItem()
        if curr in failed:
            idx = failed.index(curr)
            self._nav_to_item(failed[(idx + 1) % len(failed)])
        else:
            self._nav_to_item(failed[0])

    def _shortcut_open_log(self):
        item = self.tree.currentItem()
        if item:
            log = item.text(16)
            if log and log not in ("N/A", ""):
                self._open_file_or_warn(log, "Log File")

    def _toggle_dark_mode(self):
        self.is_dark_mode = not self.is_dark_mode
        self.apply_theme_and_spacing()

    def _copy_tree_cell(self):
        item = self.tree.currentItem()
        if item:
            col = self.tree.currentColumn()
            if col >= 0:
                text = item.text(col).strip()
                if text:
                    QApplication.clipboard().setText(text)

    # ------------------------------------------------------------------
    # SEARCH HISTORY
    # ------------------------------------------------------------------
    def _show_search_history(self, pos):
        q = self.search.text().strip()
        if q and q not in self._search_history:
            self._search_history.insert(0, q)
            self._search_history = self._search_history[:15]
        m = QMenu(self)
        if self._search_history:
            m.addAction("--- Recent Searches ---").setEnabled(False)
            for h in self._search_history:
                act = m.addAction(h)
                act.triggered.connect(lambda _, v=h: self.search.setText(v))
            m.addSeparator()
        m.addAction("Clear History").triggered.connect(
            lambda: self._search_history.clear())
        m.exec_(self.search.mapToGlobal(pos))

    # ------------------------------------------------------------------
    # COLUMN PRESETS
    # ------------------------------------------------------------------
    def _load_preset_sets(self):
        def _get(key, default):
            try:
                saved = prefs.get('PRESETS', key, fallback='')
                if saved:
                    return set(int(x) for x in saved.split(',')
                               if x.strip().isdigit())
            except Exception:
                pass
            return set(default)
        self._preset_compact  = _get('compact',  {0, 3, 4, 5, 12, 13})
        self._preset_standard = _get('standard',
                                     {0, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14})
        self._preset_full     = _get('full',     set(range(15)) | {22})

    def _set_col_preset(self, preset):
        if not hasattr(self, '_preset_compact'):
            self._load_preset_sets()
        always_hidden = {15, 16, 17, 18, 19, 20, 21, 23}
        if preset == 1:   visible = self._preset_compact
        elif preset == 2: visible = self._preset_standard
        else:             visible = self._preset_full
        for i in range(self.tree.columnCount()):
            self.tree.setColumnHidden(i, i not in visible or i in always_hidden)
        name_map = {1: "Compact", 2: "Standard", 3: "Full"}
        if hasattr(self, 'mode_combo'):
            self.mode_combo.blockSignals(True)
            idx = self.mode_combo.findText(name_map.get(preset, "Standard"))
            if idx >= 0:
                self.mode_combo.setCurrentIndex(idx)
            self.mode_combo.blockSignals(False)

    # ------------------------------------------------------------------
    # STATUS BAR
    # ------------------------------------------------------------------
    def _update_status_bar(self, runs):
        total = completed = running = not_started = failed = 0
        for r in runs:
            if r.get("run_type") != "FE":
                continue
            total += 1
            st = r.get("fe_status", "")
            if r["is_comp"]:
                completed += 1
            elif st == "RUNNING":
                running += 1
            elif st == "NOT STARTED":
                not_started += 1
            elif st in ("FAILED", "FATAL ERROR", "INTERRUPTED"):
                failed += 1
        self.sb_total.setText(f"     Total: {total}")
        self.sb_complete.setText(f"     Completed: {completed}")
        self.sb_running.setText(f"    Running: {running}")
        self.sb_selected.setText(f"     Selected: {len(self._checked_paths)}")
        if self._last_scan_time:
            self.sb_scan_time.setText(
                f"     Last scan: {self._last_scan_time}   ")

        # Health strip badges
        def _restyle(btn, label, color):
            btn.setText(label)
            btn.setStyleSheet(
                "QPushButton#healthBadge { background: " + color + "18; color: " + color + "; "
                "border: 1px solid " + color + "55; border-radius: 10px; "
                "padding: 0 10px; font-size: 11px; font-weight: bold; }"
                "QPushButton#healthBadge:hover { background: " + color + "33; }")

        _restyle(self.badge_completed, f"Completed: {completed}", "#388e3c")
        _restyle(self.badge_running,   f"Running: {running}",
                 "#1976d2" if running == 0 else "#f57c00")
        _restyle(self.badge_failed,    f"Failed: {failed}",
                 "#757575" if failed == 0 else "#d32f2f")

    # ------------------------------------------------------------------
    # ITEM CHECK
    # ------------------------------------------------------------------
    def _on_item_check_changed(self, item, col=0):
        if self._building_tree:
            return
        if col != 0:
            return
        state = item.checkState(0)
        self.tree.blockSignals(True)
        # Cascade to already-loaded STAGE children
        for i in range(item.childCount()):
            ch = item.child(i)
            ch_type = ch.data(0, Qt.UserRole)
            if ch_type == "STAGE":
                ch.setCheckState(0, state)
            elif ch_type == "__PLACEHOLDER__":
                # Stages not yet expanded -- force-load them now so
                # cascade works even before user expands the BE run.
                be_run = item.data(0, Qt.UserRole + 11)
                if be_run:
                    ign_root = self._ensure_ign_root(
                        self.tree.invisibleRootItem())
                    item.removeChild(ch)
                    self._add_stages(item, be_run, ign_root)
                    # Now cascade to freshly created stage children
                    for j in range(item.childCount()):
                        s = item.child(j)
                        if s.data(0, Qt.UserRole) == "STAGE":
                            s.setCheckState(0, state)
                break
        self.tree.blockSignals(False)
        path = item.text(15)
        if not path or path == "N/A":
            return
        if state == Qt.Checked:
            self._checked_paths.add(path)
        else:
            self._checked_paths.discard(path)
        self._update_status_bar([])

    # ------------------------------------------------------------------
    # INSPECTOR / SELECTION
    # ------------------------------------------------------------------
    def on_tree_selection_changed(self):
        sel = self.tree.selectedItems()
        self.fe_error_btn.setVisible(False)
        self.current_error_log_path = None

        if not sel:
            self.ins_lbl.setText("Select a run to view details.")
            self.meta_run_name.setText("")
            self.meta_path.clear()
            self.meta_log.clear()
            self.ins_note.clear()
            self.ins_note.setEnabled(False)
            self.ins_save_btn.setEnabled(False)
            return

        item     = sel[0]
        run_name = item.text(0)
        rtl      = item.text(1)
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl   = item.data(0, Qt.UserRole) == "RTL"
        path     = item.text(15)

        self.meta_path.setText(path)
        log_val = item.text(16)
        if log_val and log_val not in ("N/A", ""):
            # Use cached_exists to avoid blocking NFS stat on every click
            if cached_exists(log_val):
                self.meta_log.setText(log_val)
                self.meta_log.setStyleSheet("")
                self.meta_log.setToolTip(log_val)
            else:
                self.meta_log.setText("Log not found: " + log_val)
                self.meta_log.setStyleSheet("color: #c0392b; font-style: italic;")
                self.meta_log.setToolTip("File does not exist:\n" + log_val)
        else:
            self.meta_log.setText(log_val or "")
            self.meta_log.setStyleSheet("")
            self.meta_log.setToolTip("")
        self.meta_path.home(False)
        self.meta_log.home(False)
        # Show run name in Quick Info
        if is_stage:
            self.meta_run_name.setText(
                f"{item.parent().text(0)} / {run_name}")
        elif is_rtl:
            self.meta_run_name.setText(run_name)
        else:
            self.meta_run_name.setText(run_name)

        self.ins_note.setEnabled(True)
        self.ins_save_btn.setEnabled(True)

        if is_stage:
            p_name = item.parent().text(0)
            self.ins_lbl.setText(
                f"<b>Stage:</b> {run_name}<br><b>Parent:</b> {p_name}")
            self._current_note_id = f"{item.parent().text(1)} : {p_name}"
        elif is_rtl:
            self.ins_lbl.setText(f"<b>RTL Release:</b> {run_name}")
            self._current_note_id = run_name
        else:
            # FEAT 3+5: Show regression warning and history in inspector
            run_data = item.data(0, Qt.UserRole + 10)
            reg_msg  = item.data(0, Qt.UserRole + 30)
            reg_part = (
                f"<br><span style='color:#f57c00'>"
                f"[!] Regression: {reg_msg}</span>"
                if reg_msg else "")
            self.ins_lbl.setText(
                f"<b>Run:</b> {run_name}<br><b>RTL:</b> {rtl}"
                f"{reg_part}")
            self._current_note_id = f"{rtl} : {run_name}"

        notes      = self.global_notes.get(self._current_note_id, [])
        clean_text = "\n".join(notes)
        tag        = f"[{getpass.getuser()}]"
        for line in notes:
            if line.startswith(tag):
                clean_text = line.replace(tag, "").strip()
                break
        self.ins_note.setPlainText(clean_text)

        # Lazy error count: use cached_exists to avoid blocking NFS on first click
        if len(sel) == 1 and not is_stage and path and path != "N/A":
            err_count = item.data(0, Qt.UserRole + 12)
            err_path  = os.path.join(path, "logs", "compile_opt.error.log")
            if err_count is None:
                # Only read if cache already has the answer (no cold NFS stat)
                if cached_exists(err_path):
                    try:
                        with open(err_path, 'r',
                                  encoding='utf-8', errors='ignore') as _ef:
                            err_count = sum(1 for ln in _ef if ln.strip())
                    except Exception:
                        err_count = 0
                    item.setData(0, Qt.UserRole + 12, err_count)
                # else: leave as None — button stays hidden this click; shown next
            if err_count is not None:
                self.current_error_log_path = err_path
                dark = (self.is_dark_mode or
                        (self.use_custom_colors and
                         self.custom_bg_color < "#888888"))
                color = (("#81c784" if dark else "#388e3c")
                         if err_count == 0
                         else ("#e57373" if dark else "#d32f2f"))
                self.fe_error_btn.setStyleSheet(
                    f"QPushButton#errorLinkBtn {{ border: none; "
                    f"background: transparent; color: {color}; "
                    f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                    f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                self.fe_error_btn.setText(f"compile_opt errors: {err_count}")
                self.fe_error_btn.setVisible(True)

    def _open_file_or_warn(self, path, label="File"):
        """Open path in gvim, or show a non-blocking warning if it doesn't exist."""
        if path and os.path.exists(path):
            subprocess.Popen(['gvim', path])
        else:
            QMessageBox.warning(
                self, "{} Not Found".format(label),
                "{} does not exist:\n{}".format(label, path or "(no path)"))

    def on_item_double_clicked(self, item, col):
        log = item.text(16)
        if log and log != "N/A":
            self._open_file_or_warn(log, "Log File")
        elif (item.data(0, Qt.UserRole) == "STAGE"
              and item.text(15) and item.text(15) != "N/A"):
            stage_dir = item.text(15)
            if os.path.isdir(stage_dir):
                subprocess.Popen(['gvim', stage_dir])

    def open_error_log(self):
        if self.current_error_log_path and os.path.exists(
                self.current_error_log_path):
            subprocess.Popen(['gvim', self.current_error_log_path])

    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'):
            return
        txt = self.ins_note.toPlainText()
        save_user_note(self._current_note_id, txt)
        self.global_notes = load_all_notes()
        sel = self.tree.selectedItems()
        if sel:
            item      = sel[0]
            notes     = self.global_notes.get(self._current_note_id, [])
            note_text = " | ".join(notes)
            item.setText(22, note_text)
            item.setToolTip(22, note_text)
            if note_text:
                item.setForeground(22, self._colors["note"])
                # Note indicator: italic run name
                f = item.font(0); f.setItalic(True); item.setFont(0, f)
            else:
                # No notes -- remove italic
                f = item.font(0); f.setItalic(False); item.setFont(0, f)
        self._update_status_bar([])

    # ------------------------------------------------------------------
    # THEME
    # ------------------------------------------------------------------
    def apply_theme_and_spacing(self):
        pad      = self.row_spacing
        cb_style = ""
        dark = (self.is_dark_mode or
                (self.use_custom_colors and self.custom_bg_color < "#888888"))
        self._colors = {
            "completed":   QColor("#81c784" if dark else "#1b5e20"),
            "running":     QColor("#64b5f6" if dark else "#0d47a1"),
            "not_started": QColor("#9e9e9e" if dark else "#757575"),
            "interrupted": QColor("#ffb74d" if dark else "#e65100"),
            "failed":      QColor("#e57373" if dark else "#b71c1c"),
            "pass":        QColor("#81c784" if dark else "#388e3c"),
            "fail":        QColor("#e57373" if dark else "#d32f2f"),
            "outfeed":     QColor("#ce93d8" if dark else "#8e24aa"),
            "ws":          QColor("#ffb74d" if dark else "#e65100"),
            "milestone":   QColor("#64b5f6" if dark else "#1e88e5"),
            "note":        QColor("#ffb74d" if dark else "#e65100"),
        }

        if self.use_custom_colors:
            bg  = self.custom_bg_color
            fg  = self.custom_fg_color
            sel = self.custom_sel_color
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: {bg}; color: {fg}; }}
                QHeaderView::section {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px; font-weight: bold; }}
                QTreeWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; }}
                QListWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; }}
                QLineEdit, QSpinBox, QComboBox, QTextEdit {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {bg}; color: {fg}; selection-background-color: {sel}; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px 12px; border-radius: 4px; }}
                QPushButton:hover, QToolButton:hover {{ border-color: {sel}; }}
                QPushButton:pressed {{ background-color: {sel}; color: #ffffff; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: {sel}; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QMenu {{ border: 1px solid {fg}; background-color: {bg}; color: {fg}; }}
                QMenu::item:selected {{ background-color: {sel}; color: #ffffff; }}
                QStatusBar {{ background: {bg}; color: {fg}; border-top: 1px solid {fg}; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: {sel}; color: #ffffff; }}
                {cb_style}"""
        elif self.is_dark_mode:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2d30; color: #dfe1e5; }}
                QTreeWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; }}
                QListWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; }}
                QHeaderView::section {{ background-color: #2b2d30; color: #a9b7c6; border: 1px solid #1e1f22; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 3px; }}
                QComboBox {{ background-color: #2b2d30; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 3px; }}
                QComboBox QAbstractItemView {{ background-color: #2b2d30; color: #dfe1e5; selection-background-color: #2f65ca; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: #3c3f41; color: #dfe1e5; border: 1px solid #555759; padding: 5px 12px; border-radius: 4px; }}
                QPushButton:hover {{ border-color: #2f65ca; }}
                QPushButton:pressed {{ background-color: #2f65ca; color: #ffffff; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: #64b5f6; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QMenu {{ border: 1px solid #43454a; background-color: #2b2d30; color: #dfe1e5; }}
                QMenu::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QStatusBar {{ background: #2b2d30; color: #aaaaaa; border-top: 1px solid #43454a; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QSplitter::handle {{ background-color: #43454a; }}
                {cb_style}"""
        else:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #f5f5f5; color: #212121; }}
                QTreeWidget {{ background-color: #ffffff; color: #212121; alternate-background-color: #f9f9f9; }}
                QListWidget {{ background-color: #ffffff; color: #212121; alternate-background-color: #f9f9f9; }}
                QHeaderView::section {{ background-color: #e0e0e0; color: #212121; border: 1px solid #bdbdbd; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #ffffff; color: #212121; border: 1px solid #bdbdbd; padding: 4px; border-radius: 3px; }}
                QComboBox {{ background-color: #ffffff; color: #212121; border: 1px solid #bdbdbd; padding: 4px; border-radius: 3px; }}
                QComboBox QAbstractItemView {{ background-color: #ffffff; color: #212121; selection-background-color: #1976D2; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: #e0e0e0; color: #212121; border: 1px solid #bdbdbd; padding: 5px 12px; border-radius: 4px; }}
                QPushButton:hover {{ border-color: #1976D2; }}
                QPushButton:pressed {{ background-color: #1976D2; color: #ffffff; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: #1976D2; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QMenu {{ border: 1px solid #bdbdbd; background-color: #ffffff; color: #212121; }}
                QMenu::item:selected {{ background-color: #1976D2; color: #ffffff; }}
                QStatusBar {{ background: #eeeeee; color: #616161; border-top: 1px solid #bdbdbd; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #1976D2; color: #ffffff; }}
                QSplitter::handle {{ background-color: #bdbdbd; }}
                {cb_style}"""

        if stylesheet != self._last_stylesheet:
            self._last_stylesheet = stylesheet
            self.setStyleSheet(stylesheet)
            self._recolor_existing_items()

    def _recolor_existing_items(self):
        if self.tree.invisibleRootItem().childCount() == 0:
            return
        self.tree.setUpdatesEnabled(False)
        c = self._colors

        def recolor(node):
            for i in range(node.childCount()):
                child     = node.child(i)
                node_type = child.data(0, Qt.UserRole)
                if node_type == "MILESTONE":
                    child.setForeground(0, c["milestone"])
                if node_type not in (
                        "BLOCK", "MILESTONE", "RTL",
                        "IGNORED_ROOT", "__PLACEHOLDER__"):
                    self._apply_status_color(child, 3, child.text(3))
                    self._apply_fm_color(child, 7, child.text(7))
                    self._apply_fm_color(child, 8, child.text(8))
                    self._apply_vslp_color(child, 9, child.text(9))
                    src = child.text(2)
                    if src == "OUTFEED":
                        child.setForeground(2, c["outfeed"])
                    elif src == "WS":
                        child.setForeground(2, c["ws"])
                recolor(child)
        recolor(self.tree.invisibleRootItem())
        self.tree.setUpdatesEnabled(True)

    def _apply_status_color(self, item, col, status):
        c = self._colors
        if   status == "COMPLETED":   item.setForeground(col, c["completed"])
        elif status == "RUNNING":     item.setForeground(col, c["running"])
        elif status == "NOT STARTED": item.setForeground(col, c["not_started"])
        elif status == "INTERRUPTED": item.setForeground(col, c["interrupted"])
        elif status in ("FAILED", "FATAL ERROR", "ERROR"):
            item.setForeground(col, c["failed"])

    def _apply_fm_color(self, item, col, val):
        c = self._colors
        if   "FAILS" in val: item.setForeground(col, c["fail"])
        elif "PASS"  in val: item.setForeground(col, c["pass"])

    def _apply_vslp_color(self, item, col, val):
        c = self._colors
        if "Error" in val and "Error: 0" not in val:
            item.setForeground(col, c["fail"])
        elif "Error: 0" in val:
            item.setForeground(col, c["pass"])

    # ------------------------------------------------------------------
    # SCAN
    # ------------------------------------------------------------------
    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning():
            return
        clear_path_cache()
        for w in list(self.size_workers):
            if hasattr(w, 'cancel'):
                w.cancel()
        self.size_workers.clear()
        self.item_map.clear()
        self._signoff_bg_done = False
        if self._signoff_worker and self._signoff_worker.isRunning():
            if hasattr(self._signoff_worker, 'cancel'):
                self._signoff_worker.cancel()

        self.prog_container.setVisible(True)
        self.prog.setRange(0, 0)
        self.prog_lbl.setText("Scanning Workspaces...")
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Scanning...")

        self.tree.blockSignals(True)
        self.tree.clear()
        skel_color = QColor("#555555" if self.is_dark_mode else "#aaaaaa")
        for _ in range(8):
            skel = QTreeWidgetItem(self.tree)
            skel.setText(0, "Discovering runs...")
            skel.setText(1, "...")
            skel.setText(3, "SCANNING")
            skel.setText(5, "...")
            skel.setFlags(Qt.NoItemFlags)
            for col in range(24):
                skel.setForeground(col, skel_color)
        self.tree.blockSignals(False)
        self.tree.setEnabled(False)

        QApplication.processEvents()

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
        self.ws_data  = ws
        self.out_data = out
        self.ir_data  = ir
        self.prog_container.setVisible(False)
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self.tree.setEnabled(True)
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.global_notes    = load_all_notes()

        # FEAT 3+5: Record history for all completed runs
        all_runs_for_history = (self.ws_data.get("all_runs", []) +
                                self.out_data.get("all_runs", []))
        for r in all_runs_for_history:
            if r.get("run_type") == "FE" and r.get("is_comp"):
                self._record_run_history(r)
        self._save_run_history()

        self._rebuild_filter_dropdowns()
        self._restore_filter_state()

        # Update scan stats in health strip
        ws_c  = stats.get("ws", 0)
        out_c = stats.get("outfeed", 0)
        fc_c  = stats.get("fc", 0)
        inv_c = stats.get("innovus", 0)
        self.lbl_scan_stats.setText(
            f"WS: {ws_c}  OUTFEED: {out_c}  FC: {fc_c}  Innovus: {inv_c}")
        total_r = ws_c + out_c
        self.sb_scan_time.setText(
            f"     Last scan: {self._last_scan_time} "
            f"({total_r} runs)   ")

        # Defer tree build so Qt can repaint the UI first
        QTimer.singleShot(0, self._build_tree)

    # ------------------------------------------------------------------
    # FILTER DROPDOWN RESTORE
    # ------------------------------------------------------------------
    def _rebuild_filter_dropdowns(self):
        src_mode = self.src_combo.currentText()
        releases, blocks = set(), set()
        if src_mode in ["WS", "ALL"] and self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if src_mode in ["OUTFEED", "ALL"] and self.out_data:
            releases.update(self.out_data.get("releases", {}).keys())
            blocks.update(self.out_data.get("blocks", set()))

        current_rtl = self.rel_combo.currentText()
        self.rel_combo.blockSignals(True)
        self.rel_combo.clear()
        valid = [r for r in releases
                 if "Unknown" not in r and self.get_milestone_label(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(valid)
        self.rel_combo.addItems(new_releases)
        self.rel_combo.setCurrentText(
            current_rtl if current_rtl in new_releases else "[ SHOW ALL ]")
        self.rel_combo.blockSignals(False)

        saved_states = {
            self.blk_list.item(i).data(Qt.UserRole):
            self.blk_list.item(i).checkState()
            for i in range(self.blk_list.count())
        }
        self.blk_list.blockSignals(True)
        self.blk_list.clear()
        for b in sorted(blocks):
            it = QListWidgetItem(b)
            it.setData(Qt.UserRole, b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)

    def _restore_filter_state(self):
        try:
            src  = prefs.get('UI', 'last_source', fallback='ALL')
            rtl  = prefs.get('UI', 'last_rtl',    fallback='[ SHOW ALL ]')
            view = prefs.get('UI', 'last_view',   fallback='All Runs')
            srch = prefs.get('UI', 'last_search', fallback='')
            auto = prefs.get('UI', 'last_auto',   fallback='Off')
            idx = self.src_combo.findText(src)
            if idx >= 0:
                self.src_combo.blockSignals(True)
                self.src_combo.setCurrentIndex(idx)
                self.src_combo.blockSignals(False)
            if self.rel_combo.findText(rtl) >= 0:
                self.rel_combo.blockSignals(True)
                self.rel_combo.setCurrentText(rtl)
                self.rel_combo.blockSignals(False)
            idx = self.view_combo.findText(view)
            if idx >= 0:
                self.view_combo.blockSignals(True)
                self.view_combo.setCurrentIndex(idx)
                self.view_combo.blockSignals(False)
            if srch:
                self.search.blockSignals(True)
                self.search.setText(srch)
                self.search.blockSignals(False)
            idx = self.auto_combo.findText(auto)
            if idx >= 0:
                self.auto_combo.blockSignals(True)
                self.auto_combo.setCurrentIndex(idx)
                self.auto_combo.blockSignals(False)
                self.on_auto_refresh_changed()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # SOURCE CHANGE
    # ------------------------------------------------------------------
    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True)
            self.tree.setColumnHidden(3, False)
            self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True)
            self.tree.setColumnHidden(3, True)
            self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False)
            self.tree.setColumnHidden(3, False)
            self.tree.setColumnHidden(4, False)
        self._rebuild_filter_dropdowns()
        self.refresh_view()

    # ------------------------------------------------------------------
    # AUTO REFRESH
    # ------------------------------------------------------------------
    def on_auto_refresh_changed(self):
        val = self.auto_combo.currentText()
        if val == "Off":
            self.auto_refresh_timer.stop()
            self._smart_poll_timer.stop()
        elif val == "1 Min":
            self.auto_refresh_timer.start(60000)
            self._smart_poll_timer.start(60000)
        elif val == "5 Min":
            self.auto_refresh_timer.start(300000)
            self._smart_poll_timer.start(60000)
        elif val == "10 Min":
            self.auto_refresh_timer.start(600000)
            self._smart_poll_timer.start(60000)

    def _smart_poll_running(self):
        """Re-check only RUNNING FE runs -- no full NFS scan."""
        running_items = []
        def find_running(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if (child.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                         "STAGE","__PLACEHOLDER__")
                        and child.text(3) == "RUNNING"):
                    running_items.append(child)
                find_running(child)
        find_running(self.tree.invisibleRootItem())
        if not running_items:
            return
        changed = False
        for item in running_items:
            run_path = item.text(15)
            if not run_path or run_path == "N/A":
                continue
            pass_file = os.path.join(run_path, "pass", "compile_opt.pass")
            if os.path.exists(pass_file):
                item.setIcon(3, self._create_dot_icon(
                    "#388e3c", "#388e3c"))
                item.setText(3, "COMPLETED")
                item.setForeground(3, self._colors["completed"])
                item.setText(4, "COMPLETED")
                try:
                    from utils import parse_runtime_rpt
                    info = parse_runtime_rpt(
                        os.path.join(run_path, "reports", "runtime.V2.rpt"))
                    item.setText(12, info.get("runtime", item.text(12)))
                    item.setText(14, info.get("end", item.text(14)))
                except Exception:
                    pass
                changed = True
        if changed:
            visible = []
            def collect(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    run = c.data(0, Qt.UserRole + 10)
                    if run and not c.isHidden():
                        visible.append(run)
                    collect(c)
            collect(self.tree.invisibleRootItem())
            self._update_status_bar(visible)

    def _update_live_runtimes(self):
        """Update elapsed time display for RUNNING FE runs every 60s."""
        import datetime
        month_map = {
            "Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
            "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        now = datetime.datetime.now()
        def update_node(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if (child.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                         "STAGE","__PLACEHOLDER__")
                        and child.text(3) == "RUNNING"):
                    start_str = child.toolTip(13)
                    try:
                        m = re.search(
                            r'(\w{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{2}):(\d{2})',
                            start_str or "")
                        if m:
                            mon, day, yr, hr, mn = m.groups()
                            dt = datetime.datetime(
                                int(yr), month_map.get(mon, 1),
                                int(day), int(hr), int(mn))
                            delta = now - dt
                            h  = int(delta.total_seconds() // 3600)
                            mi = int((delta.total_seconds() % 3600) // 60)
                            child.setText(12, f"Running: {h:02d}h:{mi:02d}m")
                    except Exception:
                        pass
                update_node(child)
        update_node(self.tree.invisibleRootItem())

    # ------------------------------------------------------------------
    # BUILD TREE
    # ------------------------------------------------------------------
    def _build_tree(self):
        """Build the full tree once. Filtering done by setHidden() only."""
        for w in list(self.size_workers):
            if hasattr(w, 'cancel'):
                w.cancel()
        self.size_workers.clear()
        self.item_map.clear()
        self._signoff_items_by_path.clear()
        if self._signoff_worker and self._signoff_worker.isRunning():
            if hasattr(self._signoff_worker, 'cancel'):
                self._signoff_worker.cancel()
            self._signoff_bg_done = False

        self._building_tree = True

        # Save expand state before clear so filter/ignore actions don't collapse tree
        def _collect_expanded(node):
            out = set()
            nt = node.data(0, Qt.UserRole)
            if nt in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT", "STANDALONE_ROOT"):
                if node.isExpanded():
                    out.add((nt, node.text(0)))
            for i in range(node.childCount()):
                out |= _collect_expanded(node.child(i))
            return out
        _saved_expanded = _collect_expanded(self.tree.invisibleRootItem())

        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        self.tree.setSortingEnabled(False)
        self.tree.clear()

        runs_to_process = []
        runs_to_process.extend(self.ws_data.get("all_runs", []))
        runs_to_process.extend(self.out_data.get("all_runs", []))

        # Resolve BE RTL from matching FE run
        fe_info = {}
        for run in runs_to_process:
            if run["run_type"] == "FE":
                fe_base = run["r_name"]
                if fe_base.endswith("-FE"):
                    fe_base = fe_base[:-3]
                fe_info[(run["block"], fe_base)] = run["rtl"]

        for run in runs_to_process:
            if run["run_type"] == "BE":
                # FE names have NO underscores (only hyphens).
                # Pattern: EVT*_ML*_DEV**_<FE_NAME>_<PNR_SUFFIX>
                # After stripping EVT prefix, split at FIRST underscore
                # to get FE_NAME exactly.
                r = re.sub(r'^EVT\d+_ML\d+_DEV\d+(?:_syn\d+)?_', '', run["r_name"])
                idx = r.find('_')
                if idx == -1:
                    # No underscore -- could be a direct fc BE run like run1-BE
                    fe_name_from_be = r[:-3] if r.endswith('-BE') else r
                else:
                    fe_name_from_be = r[:idx]   # everything before first _
                # O(1) dict lookup instead of O(n) iteration
                fe_rtl = fe_info.get((run["block"], fe_name_from_be))
                if fe_rtl:
                    run["rtl"] = fe_rtl

        root            = self.tree.invisibleRootItem()
        ign_root        = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
        standalone_root = self._ensure_standalone_root(root)

        # Pre-compute base_rtl and milestone per unique RTL string
        _rtl_cache = {}
        for run in runs_to_process:
            rtl = run["rtl"]
            if rtl not in _rtl_cache:
                base = re.sub(r'_syn\d+$', '', rtl)
                ms   = self.get_milestone_label(base)
                _rtl_cache[rtl] = (base, base != rtl, ms)

        _ignored  = self.ignored_paths
        _hide_blk = self.hide_block_nodes
        _build_be_only = (self.view_combo.currentText() == "BE Only")

        # CRITICAL: process FE runs first so FE tree items exist
        # before any BE/innovus run tries to find its FE parent.
        # Without this, BE runs processed before their FE run silently
        # attach to the RTL node instead of the FE item.
        runs_fe = [r for r in runs_to_process if r["run_type"] == "FE"]
        runs_be = [r for r in runs_to_process if r["run_type"] != "FE"]
        ordered_runs = runs_fe + runs_be

        # O(1) FE parent lookup dict: (block, fe_base_name, source) -> QTreeWidgetItem
        # Built while processing FE runs; used instantly by BE runs.
        _fe_lookup = {}

        _item_count = 0
        for run in ordered_runs:
            run_rtl = run["rtl"]
            base_rtl, has_syn, milestone = _rtl_cache.get(
                run_rtl, (run_rtl, False, None))
            if milestone is None:
                continue

            _item_count += 1
            # processEvents removed: setUpdatesEnabled(False) is active so no
            # visual benefit, and it lets premature size signals through mid-build.

            is_ignored  = run["path"] in _ignored
            attach_root = ign_root if is_ignored else root
            blk_name    = run["block"]

            base_attach = (attach_root if _hide_blk
                           else self._get_node(attach_root, blk_name, "BLOCK"))

            m_node = self._get_node(base_attach, milestone, "MILESTONE")
            parent_for_run = self._get_node(m_node, base_rtl, "RTL")

            if run["run_type"] == "FE":
                if _build_be_only:
                    continue
                run_item = self._create_run_item(parent_for_run, run)
                run_item.setData(0, Qt.UserRole + 10, run)
                if run.get("path"):
                    self._signoff_items_by_path[run["path"]] = run_item
                # Register in O(1) lookup so BE runs can find this instantly
                fe_text = run["r_name"]
                fe_base = fe_text[:-3] if fe_text.endswith("-FE") else fe_text
                src     = run["source"]
                _fe_lookup[(run["block"], fe_base, src)]  = run_item
                _fe_lookup[(run["block"], fe_base, "")]   = run_item  # source-agnostic fallback

            elif run["run_type"] == "BE":
                be_block  = run["block"]
                be_source = run["source"]

                # Derive FE base name from BE run name
                _r = re.sub(r'^EVT\d+_ML\d+_DEV\d+(?:_syn\d+)?_', '', run["r_name"])
                _idx = _r.find('_')
                if _idx == -1:
                    fe_name_from_be = _r[:-3] if _r.endswith('-BE') else _r
                else:
                    fe_name_from_be = _r[:_idx]

                # O(1) lookup: exact source first, then source-agnostic fallback
                fe_parent = (_fe_lookup.get((be_block, fe_name_from_be, be_source))
                             or _fe_lookup.get((be_block, fe_name_from_be, "")))

                if _build_be_only:
                    actual_parent = parent_for_run
                elif fe_parent is None and not is_ignored:
                    st_base = (standalone_root if _hide_blk
                               else self._get_node(standalone_root, blk_name, "BLOCK"))
                    st_m   = self._get_node(st_base, milestone, "MILESTONE")
                    st_rtl = self._get_node(st_m, base_rtl, "RTL")
                    actual_parent = st_rtl
                else:
                    actual_parent = fe_parent if fe_parent else parent_for_run

                be_item = self._create_run_item(actual_parent, run)
                be_item.setData(0, Qt.UserRole + 10, run)
                if run.get("path"):
                    self._signoff_items_by_path[run["path"]] = be_item
                if run.get("stages"):
                    ph = QTreeWidgetItem(be_item)
                    ph.setText(0, "Loading stages...")
                    ph.setData(0, Qt.UserRole, "__PLACEHOLDER__")
                    ph.setFlags(Qt.NoItemFlags)
                    be_item.setData(0, Qt.UserRole + 11, run)

        if ign_root.childCount() == 0:
            root.removeChild(ign_root)
        if standalone_root.childCount() == 0:
            root.removeChild(standalone_root)

        self.tree.setSortingEnabled(True)
        # Default sort: Run Name column A-Z ascending
        self.tree.sortByColumn(0, Qt.AscendingOrder)
        self.tree.header().setSortIndicator(0, Qt.AscendingOrder)
        self.tree.setUpdatesEnabled(True)
        self.tree.blockSignals(False)
        self._building_tree = False

        # Restore expand state (ignore action / rescan keeps tree looking the same)
        if _saved_expanded:
            def _apply_expanded(node):
                nt = node.data(0, Qt.UserRole)
                if nt in ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STANDALONE_ROOT"):
                    if (nt, node.text(0)) in _saved_expanded:
                        node.setExpanded(True)
                for i in range(node.childCount()):
                    _apply_expanded(node.child(i))
            _apply_expanded(self.tree.invisibleRootItem())

        all_owners = set()
        for r in (self.ws_data.get("all_runs", []) +
                  self.out_data.get("all_runs", [])):
            if r.get("owner") and r["owner"] != "Unknown":
                all_owners.add(r["owner"])
        if all_owners:
            _save_mail_users(all_owners)

        self.refresh_view()

        # --- Deferred post-build work so UI is interactive immediately ---
        # fit_all_columns: 23-column resize is expensive on main thread;
        # defer 100ms so tree paints first and user can interact.
        if not self._columns_fitted_once:
            self._columns_fitted_once = True
            QTimer.singleShot(100, self.fit_all_columns)

        # Folder-size calculation is expensive on NFS. Run it on startup only
        # when explicitly enabled in project_config.ini.
        if AUTO_SIZE_ON_START and not self._initial_size_calc_done:
            self._initial_size_calc_done = True
            QTimer.singleShot(2000, self.calculate_all_sizes)

        if (BACKGROUND_SIGNOFF_AFTER_SCAN and not SCAN_SIGNOFF_ON_START
                and not self._signoff_bg_done):
            QTimer.singleShot(1200, self.start_bg_signoff_scan)

        # Closure+regression scan deferred 300ms (after fit_all_columns)
        if self._closure_enabled:
            QTimer.singleShot(300, self._run_closure_pass)
        # Auto-expand to RTL level on first load only
        if not hasattr(self, '_auto_expanded_once'):
            self._auto_expanded_once = True
            QTimer.singleShot(50, self._expand_to_rtl_level)
        # Pre-warm log paths later so it does not compete with the FM/VSLP
        # background scan immediately after tree build.
        QTimer.singleShot(5000, self._prefetch_log_paths)

    # ------------------------------------------------------------------
    # CREATE RUN ITEM
    # ------------------------------------------------------------------
    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(
            Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
        child.setCheckState(0, Qt.Unchecked)

        r_name = run["r_name"]
        child.setText(0, r_name)
        child.setText(1, run["rtl"])
        child.setText(2, run["source"])
        child.setText(5, run.get("owner", ""))
        child.setText(15, run["path"])
        child.setText(22, "")
        child.setData(0, Qt.UserRole + 2, run["block"])
        child.setData(0, Qt.UserRole + 4,
                      r_name.replace("-FE","").replace("-BE",""))

        note_id = f"{run['rtl']} : {r_name}"
        notes   = self.global_notes.get(note_id, [])
        if notes:
            note_text = " | ".join(notes)
            child.setText(22, note_text)
            child.setToolTip(22, note_text)
            child.setForeground(22, self._colors["note"])
            # FEAT 5: Visual note indicator -- italic run name
            f = child.font(0)
            f.setItalic(True)
            child.setFont(0, f)
            child.setToolTip(0, (child.toolTip(0) or "") +
                             "\n[Has shared notes]")

        tooltip_text = (
            f"Run: {r_name}\n"
            f"Block: {run['block']}\n"
            f"RTL: {run['rtl']}\n"
            f"Source: {run['source']}\n"
            f"Path: {run['path']}")

        if run["run_type"] == "FE":
            status_str = run["fe_status"]
            _dot_map = {
                "COMPLETED":   "#388e3c", "RUNNING":    "#1976d2",
                "NOT STARTED": "#9e9e9e", "INTERRUPTED":"#e65100",
                "FAILED":      "#d32f2f", "FATAL ERROR":"#b71c1c",
            }
            dc = _dot_map.get(status_str, "#9e9e9e")
            child.setIcon(3, self._create_dot_icon(dc, dc))
            child.setText(3, status_str)
            child.setText(4, ("COMPLETED" if run["is_comp"]
                              else run["info"]["last_stage"]))
            child.setText(6, "-")
            child.setText(10, "-")
            child.setText(11, "-")
            child.setText(7, f"NONUPF - {run['st_n']}")
            child.setText(8, f"UPF - {run['st_u']}")
            child.setText(9, run["vslp_status"])
            child.setText(12, run["info"]["runtime"])

            start_raw = run["info"]["start"]
            end_raw   = run["info"]["end"]
            child.setData(0, Qt.UserRole + 40, start_raw)
            child.setData(0, Qt.UserRole + 41, end_raw)
            child.setText(13, self._fmt_ts(start_raw))
            child.setText(14, self._fmt_ts(end_raw))
            child.setToolTip(13, start_raw)
            child.setToolTip(14, end_raw)

            child.setText(16, run.get("log_path", "") or
                          os.path.join(run["path"],
                                       "logs", "compile_opt.log")
                          if run["path"] != "N/A" else "N/A")
            child.setText(17, run.get("fm_u_path",   "N/A"))
            child.setText(18, run.get("fm_n_path",   "N/A"))
            child.setText(19, run.get("vslp_rpt_path","N/A"))

            self._apply_status_color(child, 3, status_str)
            self._apply_fm_color(child, 7, child.text(7))
            self._apply_fm_color(child, 8, child.text(8))
            self._apply_vslp_color(child, 9, child.text(9))

            ir_info = self.ir_data.get(run["block"], {})
            static_val  = ir_info.get("static", "N/A")
            dynamic_val = ir_info.get("dynamic", "N/A")
            child.setText(10, static_val)
            child.setText(11, dynamic_val)

        elif run["run_type"] == "BE":
            child.setText(3, "COMPLETED" if run.get("is_comp") else "-")
            child.setText(4, "-")
            for col in [6, 7, 8, 9, 10, 11]:
                child.setText(col, "-")
            # BE run folders are containers only. Runtime/start/end belongs to
            # the PNR stage rows under this item.
            child.setText(12, "-")
            be_start_raw = "-"
            be_end_raw   = "-"
            child.setData(0, Qt.UserRole + 40, be_start_raw)
            child.setData(0, Qt.UserRole + 41, be_end_raw)
            child.setText(13, self._fmt_ts(be_start_raw))
            child.setText(14, self._fmt_ts(be_end_raw))

        child.setData(0, Qt.UserRole, "STAGE"
                      if run["run_type"] == "STAGE" else None)

        tooltip_text += f"\nSize: -\n"
        child.setToolTip(0, tooltip_text)
        child.setExpanded(False)

        # Error log count deferred: checked lazily on first click via
        # cached_exists (no blocking NFS stat during tree build).
        # _prefetch_log_paths() warms the cache 500ms after build.
        child.setData(0, Qt.UserRole + 12, None)  # sentinel: not yet checked

        # Pre-compute path existence flags for context menu (no NFS on right-click)
        child.setData(0, Qt.UserRole + 20, {
            'run_path': bool(run.get("path") and run["path"] != "N/A"),
            'log':      bool(run.get("path") and run["path"] != "N/A"),
            'fm_n':     bool(run.get("fm_n_path")),
            'fm_u':     bool(run.get("fm_u_path")),
            'vslp':     bool(run.get("vslp_rpt_path")),
        })

        if run["source"] == "OUTFEED":
            child.setForeground(
                2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        else:
            child.setForeground(
                2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))

        # Apply pin icon at creation time — O(1), replaces post-build tree walk
        pin_type = self.user_pins.get(run["path"])
        if pin_type and pin_type in self.icons:
            child.setIcon(0, self.icons[pin_type])
            child.setData(0, Qt.UserRole + 5, pin_type)

        return child

    # ------------------------------------------------------------------
    # GET / ADD TREE NODES
    # ------------------------------------------------------------------
    def _get_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.childCount()):
            if parent.child(i).text(0) == text:
                return parent.child(i)
        p = CustomTreeItem(parent)
        p.setText(0, text)
        p.setData(0, Qt.UserRole, node_type)
        p.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        if node_type == "MILESTONE":
            p.setForeground(0, self._colors["milestone"])
            f = p.font(0); f.setBold(True); p.setFont(0, f)
        elif node_type == "RTL":
            f = p.font(0); f.setItalic(True); p.setFont(0, f)
            if text in self.global_notes:
                notes = " | ".join(self.global_notes[text])
                p.setText(22, notes); p.setToolTip(22, notes)
                p.setForeground(22, self._colors["note"])
        return p

    def _add_stages(self, be_item, be_run, ign_root):
        for stage in be_run.get("stages", []):
            s_item = CustomTreeItem(be_item)
            s_item.setData(0, Qt.UserRole, "STAGE")
            s_item.setFlags(
                Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
            s_item.setCheckState(0, Qt.Unchecked)
            s_item.setText(0,  stage.get("name", ""))
            s_item.setText(7,  f"NONUPF - {stage.get('st_n', '')}")
            s_item.setText(8,  f"UPF - {stage.get('st_u', '')}")
            s_item.setText(9,  stage.get("vslp_status", ""))
            s_item.setText(12, stage.get("info", {}).get("runtime", ""))
            s_start_raw = stage.get("info", {}).get("start", "")
            s_end_raw   = stage.get("info", {}).get("end", "")
            s_item.setData(0, Qt.UserRole + 40, s_start_raw)
            s_item.setData(0, Qt.UserRole + 41, s_end_raw)
            s_item.setText(13, self._fmt_ts(s_start_raw))
            s_item.setText(14, self._fmt_ts(s_end_raw))
            # Col 15 = stage directory path, Col 16 = stage log file
            s_item.setText(15, stage.get("stage_path", "N/A"))
            s_item.setText(16, stage.get("log",        "N/A"))
            s_item.setText(20, stage.get("sta_rpt_path",  "N/A"))
            s_item.setText(21, stage.get("qor_path",      "N/A"))



            self._apply_fm_color(s_item, 7, s_item.text(7))
            self._apply_fm_color(s_item, 8, s_item.text(8))
            self._apply_vslp_color(s_item, 9, s_item.text(9))

    def on_item_expanded(self, item):
        if item.childCount() == 1:
            ph = item.child(0)
            if ph.data(0, Qt.UserRole) == "__PLACEHOLDER__":
                be_run = item.data(0, Qt.UserRole + 11)
                if be_run:
                    ign_root = self._ensure_ign_root(
                        self.tree.invisibleRootItem())
                    parent_checked = item.checkState(0) == Qt.Checked
                    item.removeChild(ph)
                    self._add_stages(item, be_run, ign_root)
                    # Propagate parent check state to newly created stages
                    if parent_checked:
                        self.tree.blockSignals(True)
                        for i in range(item.childCount()):
                            ch = item.child(i)
                            if ch.data(0, Qt.UserRole) == "STAGE":
                                ch.setCheckState(0, Qt.Checked)
                        self.tree.blockSignals(False)
                    # Load stage timing/FM/VSLP in background if deferred
                    if any(s.get("_lazy") for s in be_run.get("stages", [])):
                        from workers import StageDetailWorker
                        w = StageDetailWorker(be_run, item)
                        w.finished.connect(self._on_stage_details_loaded)
                        w.start()
                        self._stage_workers.append(w)

    def _on_stage_details_loaded(self, be_item, enriched_stages):
        """Called by StageDetailWorker when stage timing/FM/VSLP is ready."""
        be_run = be_item.data(0, Qt.UserRole + 11)
        if be_run:
            be_run["stages"] = enriched_stages
        for i in range(be_item.childCount()):
            ch = be_item.child(i)
            if ch.data(0, Qt.UserRole) != "STAGE":
                continue
            sname = ch.text(0)
            for s in enriched_stages:
                if s["name"] == sname:
                    s_start = s.get("info", {}).get("start", "")
                    s_end   = s.get("info", {}).get("end", "")
                    ch.setData(0, Qt.UserRole + 40, s_start)
                    ch.setData(0, Qt.UserRole + 41, s_end)
                    ch.setText(12, s.get("info", {}).get("runtime", "-"))
                    ch.setText(13, self._fmt_ts(s_start))
                    ch.setText(14, self._fmt_ts(s_end))
                    ch.setText(7,  "NONUPF - " + s["st_n"])
                    ch.setText(8,  "UPF - "    + s["st_u"])
                    ch.setText(9,  s["vslp_status"])
                    self._apply_fm_color(ch, 7, ch.text(7))
                    self._apply_fm_color(ch, 8, ch.text(8))
                    self._apply_vslp_color(ch, 9, ch.text(9))
                    break
        # Clean up finished workers
        self._stage_workers = [w for w in self._stage_workers
                               if w.isRunning()]

    # ------------------------------------------------------------------
    # REFRESH VIEW (pure hide/show -- zero item creation)
    # ------------------------------------------------------------------
    def _on_view_changed(self):
        new_view = self.view_combo.currentText()
        old_view = getattr(self, "_last_view_preset", "")
        self._last_view_preset = new_view
        if "BE Only" in (old_view, new_view) and self.ws_data:
            QTimer.singleShot(0, self._build_tree)
        else:
            self.refresh_view()

    def refresh_view(self):
        src_mode = self.src_combo.currentText()
        sel_rtl  = self.rel_combo.currentText()
        preset   = self.view_combo.currentText()

        raw_query      = self.search.text().lower().strip()
        search_pattern = ("*" if not raw_query
                          else (f"*{raw_query}*"
                                if '*' not in raw_query else raw_query))

        checked_blks = set(
            self.blk_list.item(i).data(Qt.UserRole)
            for i in range(self.blk_list.count())
            if self.blk_list.item(i).checkState() == Qt.Checked)

        self.tree.setColumnHidden(1, sel_rtl != "[ SHOW ALL ]")
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True)
            self.tree.setColumnHidden(3, False)
            self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True)
            self.tree.setColumnHidden(3, True)
            self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False)
            self.tree.setColumnHidden(3, False)
            self.tree.setColumnHidden(4, False)

        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)

        visible_runs = []

        # Pre-compute filter constants outside the loop
        _src_ws      = (src_mode == "WS")
        _src_out     = (src_mode == "OUTFEED")
        _sel_rtl_all = (sel_rtl == "[ SHOW ALL ]")
        _sel_rtl_sfx = sel_rtl + "_"
        _do_search   = (search_pattern != "*")
        _fe_only       = (preset == "FE Only")
        _be_only       = (preset == "BE Only")
        _run_only      = (preset == "Running Only")
        _fail_only     = (preset == "Failed Only")
        _today_only    = (preset == "Today's Runs")
        _pinned_only   = (preset == "Pinned Only")
        _selected_only = (preset == "Selected Only")
        _checked_set   = self._checked_paths
        _pins          = self.user_pins
        _rfc           = self.run_filter_config
        _notes         = self.global_notes

        def _passes(run):
            if run is None:
                return False
            src = run["source"]
            if _src_ws  and src != "WS":      return False
            if _src_out and src != "OUTFEED": return False
            path = run["path"]
            is_golden = (_pins.get(path) == "golden")
            if _pinned_only and path not in _pins:    return False
            if _selected_only and path not in _checked_set: return False
            if not is_golden:
                if run["block"] not in checked_blks:
                    return False
                if _rfc is not None:
                    rr, rb = run["rtl"], run["block"]
                    if (src in _rfc and rr in _rfc[src]
                            and rb in _rfc[src][rr]):
                        allowed   = _rfc[src][rr][rb]
                        base_name = run["r_name"].replace(
                            "-FE","").replace("-BE","")
                        if (base_name not in allowed
                                and run["r_name"] not in allowed):
                            return False
            rtl = run["rtl"]
            if not _sel_rtl_all:
                if rtl != sel_rtl and not rtl.startswith(_sel_rtl_sfx):
                    return False
            rt_type = run["run_type"]
            if _fe_only and rt_type != "FE": return False
            if _be_only and rt_type != "BE": return False
            if _run_only and not (rt_type == "FE" and not run["is_comp"]):
                return False
            if _fail_only:
                if not ("FAILS" in run.get("st_n","")
                        or "FAILS" in run.get("st_u","")
                        or run.get("fe_status","")
                        in ("FAILED","FATAL ERROR","ERROR")):
                    return False
            if _today_only:
                rt = relative_time(run["info"].get("start",""))
                if not (rt.endswith("ago")
                        and ("h ago" in rt or "m ago" in rt)):
                    return False
            if _do_search:
                note_id  = f"{rtl} : {run['r_name']}"
                notes    = " | ".join(_notes.get(note_id, []))
                combined = (
                    f"{run['r_name']} {rtl} {src} {rt_type} "
                    f"{run.get('owner','')} "
                    f"{run.get('st_n','')} {run.get('st_u','')} "
                    f"{run.get('vslp_status','')} "
                    f"{run['info']['runtime']} {run['info']['start']} "
                    f"{run['info']['end']} {notes}").lower()
                # Fast path: plain substring check when no wildcards in query
                _raw_lc = raw_query
                if '*' not in _raw_lc:
                    _hit = _raw_lc in combined
                else:
                    _hit = fnmatch.fnmatch(combined, search_pattern)
                if not _hit:
                    if rt_type == "BE":
                        def _stage_hit(s):
                            sc = (f"{s['name']} {s['st_n']} {s['st_u']} "
                                  f"{s['vslp_status']} "
                                  f"{s['info']['runtime']}").lower()
                            return (_raw_lc in sc if '*' not in _raw_lc
                                    else fnmatch.fnmatch(sc, search_pattern))
                        if not any(_stage_hit(s) for s in run.get("stages",[])):
                            return False
                    else:
                        return False
            return True

        _GROUP_TYPES = frozenset(
            ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STANDALONE_ROOT"))
        _UR   = Qt.UserRole
        _UR10 = Qt.UserRole + 10

        def _update_visibility(item):
            node_type = item.data(0, _UR)
            if node_type == "__PLACEHOLDER__":
                item.setHidden(True)
                return False
            # Standalone PNR Runs: hide in FE Only / BE Only views
            if node_type == "STANDALONE_ROOT":
                hide_it = (_fe_only or _be_only)
                item.setHidden(hide_it)
                if not hide_it:
                    any_visible = False
                    for i in range(item.childCount()):
                        if _update_visibility(item.child(i)):
                            any_visible = True
                    item.setHidden(not any_visible)
                    return not item.isHidden()
                return False
            # Group nodes (BLOCK, MILESTONE, RTL, IGNORED_ROOT) recurse
            # into children. Never auto-expand — preserve user's expand state.
            if node_type in _GROUP_TYPES or node_type == "MILESTONE":
                # Short-circuit: if this is a BLOCK node whose block is
                # entirely excluded by the block-list filter, hide it and
                # skip recursing all its children — big win when many blocks
                # are unchecked (skips 70-80% of tree walk).
                if node_type == "BLOCK" and item.text(0) not in checked_blks:
                    item.setHidden(True)
                    return False
                any_visible = False
                for i in range(item.childCount()):
                    if _update_visibility(item.child(i)):
                        any_visible = True
                item.setHidden(not any_visible)
                # No setExpanded() — user expand state is preserved
                return any_visible
            else:
                run         = item.data(0, _UR10)
                passes      = _passes(run)
                rt_type_run = run.get("run_type") if run else None
                item.setHidden(not passes)
                if passes and run:
                    visible_runs.append(run)
                for i in range(item.childCount()):
                    ch = item.child(i)
                    if ch.data(0, _UR) == "__PLACEHOLDER__":
                        ch.setHidden(True)
                    elif ch.data(0, _UR) == "STAGE":
                        # When BE-only: hide synthesis stages of FE parent
                        hide_stage = not passes or (
                            _be_only and rt_type_run == "FE")
                        ch.setHidden(hide_stage)
                    else:
                        # BE child run under FE item: hide when FE-only
                        child_run = ch.data(0, _UR10)
                        child_rt  = child_run.get("run_type") if child_run else None
                        hide_child = not passes or (_fe_only and child_rt == "BE")
                        ch.setHidden(hide_child)
                return passes

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            _update_visibility(root.child(i))

        if self.active_col_filters:
            self.apply_tree_filters()

        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)
        # FEAT 6: Show search result count when search is active
        if raw_query:
            fe_visible = sum(1 for r in visible_runs
                             if r.get("run_type") == "FE")
            self.search_count_lbl.setText(f"{fe_visible} found")
            self.search_count_lbl.setVisible(True)
        else:
            self.search_count_lbl.setVisible(False)

        self._update_status_bar(visible_runs)
        QTimer.singleShot(80, self._fit_run_name_column)

    # ------------------------------------------------------------------
    # COLUMN FILTER
    # ------------------------------------------------------------------
    def show_column_filter_dialog(self, col):
        unique_values = set()
        def gather(node):
            if node.data(0, Qt.UserRole) not in (
                    "BLOCK","MILESTONE","RTL","IGNORED_ROOT"):
                unique_values.add(node.text(col).strip())
            for i in range(node.childCount()):
                gather(node.child(i))
        gather(self.tree.invisibleRootItem())
        if not unique_values:
            QMessageBox.information(
                self, "Filter",
                "No data available in this column to filter.")
            return
        active   = self.active_col_filters.get(col, unique_values)
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
            orig = self.tree.headerItem().text(col).replace(" [*]", "")
            self.tree.headerItem().setText(
                col, orig + " [*]" if col in self.active_col_filters else orig)
        if not self.active_col_filters:
            return
        def _filter(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if child.isHidden():
                    _filter(child)
                    continue
                nt = child.data(0, Qt.UserRole)
                if nt in ("BLOCK","MILESTONE","RTL","IGNORED_ROOT"):
                    _filter(child)
                else:
                    hidden = any(
                        col in self.active_col_filters
                        and child.text(col).strip()
                        not in self.active_col_filters[col]
                        for col in self.active_col_filters)
                    child.setHidden(hidden)
                    _filter(child)
        _filter(self.tree.invisibleRootItem())

    # ------------------------------------------------------------------
    # CONTEXT MENU
    # ------------------------------------------------------------------
    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item or not item.parent():
            return
        m = QMenu()

        run_path  = item.text(15)
        fm_u_path = item.text(17); fm_n_path = item.text(18)
        vslp_path = item.text(19); sta_path  = item.text(20)
        ir_path   = item.text(21)
        log_path  = item.text(16)
        is_stage  = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl    = item.data(0, Qt.UserRole) == "RTL"

        target_item = item if not is_stage else item.parent()
        b_name      = target_item.data(0, Qt.UserRole + 2)
        r_rtl       = target_item.text(1)
        base_run    = target_item.data(0, Qt.UserRole + 4)
        run_source  = target_item.text(2)

        act_gold = act_good = act_red = act_later = act_clear = None
        gantt_act = None
        timeline_act = None

        if (run_path and run_path != "N/A") or is_stage:
            pin_menu  = m.addMenu("Pin as...")
            act_gold  = pin_menu.addAction(self.icons['golden'],    "Golden Run")
            act_good  = pin_menu.addAction(self.icons['good'],      "Good Run")
            act_red   = pin_menu.addAction(self.icons['redundant'], "Redundant Run")
            act_later = pin_menu.addAction(self.icons['later'],     "Mark for Later")
            pin_menu.addSeparator()
            act_clear = pin_menu.addAction("Clear Pin")
            m.addSeparator()
            if (item.childCount() > 0
                    and item.child(0).data(0, Qt.UserRole) == "STAGE"):
                gantt_act = m.addAction("Show Timeline (Gantt Chart)")
                m.addSeparator()
            timeline_act = m.addAction("Run Timeline Overview")
            m.addSeparator()

        edit_note_act = None; note_identifier = ""
        if run_path and run_path != "N/A" and not is_stage:
            note_identifier = f"{r_rtl} : {item.text(0)}"
            edit_note_act   = m.addAction("Add / Edit Personal Note")
            m.addSeparator()
        elif is_rtl:
            note_identifier = item.text(0)
            edit_note_act   = m.addAction("Add / Edit Alias Note for RTL")
            m.addSeparator()

        add_config_act = None
        if b_name and r_rtl and base_run and run_source:
            if self.current_config_path:
                add_config_act = m.addAction("Add Run to Active Filter Config")
            else:
                add_config_act = m.addAction(
                    "Create New Filter Config & Add Run")
            m.addSeparator()

        restore_all_act = None
        if self.ignored_paths:
            restore_all_act = m.addAction("Restore All Ignored Runs")
        ignore_checked_act = m.addAction("Ignore All Checked Runs")
        m.addSeparator()

        ignore_act = restore_act = None
        target_path = item.text(15)
        if target_path and target_path != "N/A":
            if target_path in self.ignored_paths:
                restore_act = m.addAction("Restore (Unhide)")
            else:
                ignore_act = m.addAction("Ignore Run")
            m.addSeparator()

        # Do NOT call cached_exists() here -- it blocks on NFS and makes right-click laggy.
        # Always show relevant actions; existence is checked only when user clicks.
        def _has(path):
            return bool(path and path not in ("N/A", ""))

        calc_size_act = (m.addAction("Calculate Folder Size")
                         if _has(run_path) else None)
        if calc_size_act: m.addSeparator()

        fm_n_act    = m.addAction("Open NONUPF Formality Report") if _has(fm_n_path) else None
        fm_u_act    = m.addAction("Open UPF Formality Report")    if _has(fm_u_path) else None
        v_act       = m.addAction("Open VSLP Report")             if _has(vslp_path) else None
        sta_act     = m.addAction("Open PT STA Summary")          if _has(sta_path)  else None
        ir_stat_act = m.addAction("Open Static IR Log")           if _has(ir_path)   else None
        ir_dyn_act  = m.addAction("Open Dynamic IR Log")          if (is_stage and _has(ir_path)) else None
        log_act     = m.addAction("Open Log File")                if _has(log_path)  else None

        m.addSeparator()
        qor_act = None
        if is_stage:
            m.addSeparator()
            qor_act = m.addAction("Run Single Stage QoR")

        # QoR Summary action
        qor_sum_act = None
        if (run_path and run_path != "N/A") or is_stage:
            qor_sum_act = m.addAction("Show QoR Summary")
            m.addSeparator()

        # Copy cell submenu -- copy any visible column value
        m.addSeparator()
        copy_menu = m.addMenu("Copy Cell Value...")
        _col_names = [
            "Run Name", "RTL Release", "Source", "Status", "Stage",
            "User", "Size", "FM-NONUPF", "FM-UPF", "VSLP",
            "Static IR", "Dynamic IR", "Runtime", "Start", "End"]
        _copy_acts = {}
        for _ci, _cn in enumerate(_col_names):
            _val = item.text(_ci)
            if _val and _val not in ("-", "N/A", ""):
                _act = copy_menu.addAction(f"{_cn}: {_val[:40]}")
                _copy_acts[_act] = _val
        if item.text(22):
            _act = copy_menu.addAction(f"Notes: {item.text(22)[:40]}")
            _copy_acts[_act] = item.text(22)

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res:
            return

        # Show QoR Summary -- launch MetricWorker on demand
        if qor_sum_act and res == qor_sum_act:
            self._launch_metric_worker(item)
            return

        # Handle copy actions
        if res in _copy_acts:
            QApplication.clipboard().setText(_copy_acts[res])
            return

        if res in [act_gold, act_good, act_red, act_later, act_clear]:
            p_target = (run_path if (run_path and run_path != "N/A")
                        else (item.parent().text(15) if is_stage else None))
            if p_target:
                if   res == act_gold:  self.user_pins[p_target] = 'golden'
                elif res == act_good:  self.user_pins[p_target] = 'good'
                elif res == act_red:   self.user_pins[p_target] = 'redundant'
                elif res == act_later: self.user_pins[p_target] = 'later'
                elif res == act_clear: self.user_pins.pop(p_target, None)
                save_user_pins(self.user_pins)
                # Apply icon immediately on the pinned item
                pin_type = self.user_pins.get(p_target)
                if pin_type and pin_type in self.icons:
                    item.setIcon(0, self.icons[pin_type])
                    item.setData(0, Qt.UserRole + 5, pin_type)
                else:
                    item.setIcon(0, QIcon())
                    item.setData(0, Qt.UserRole + 5, None)
                # Also walk all items in case same path appears multiple times
                self._apply_pin_icons()

        elif gantt_act and res == gantt_act:
            stages = []
            for i in range(item.childCount()):
                c = item.child(i)
                if c.data(0, Qt.UserRole) == "STAGE":
                    rt = c.text(12)
                    stages.append({
                        'name': c.text(0),
                        'time_str': rt,
                        'sec': self._time_to_seconds(rt)})
            dlg = GanttChartDialog(item.text(0), stages, self)
            dlg.exec_()

        elif timeline_act and res == timeline_act:
            self.show_timeline_overview(item)

        elif edit_note_act and res == edit_note_act:
            dlg = EditNoteDialog(item.text(22), note_identifier, self)
            if dlg.exec_():
                save_user_note(note_identifier, dlg.get_text())
                self.global_notes = load_all_notes()
                self.refresh_view()

        elif add_config_act and res == add_config_act:
            if not self.current_config_path:
                path, _ = QFileDialog.getSaveFileName(
                    self, "Create New Config", "dashboard_filter.cfg",
                    "Config Files (*.cfg *.txt)")
                if not path:
                    return
                self.current_config_path = path
            self._save_current_config()
            self.sb_config.setText(
                f"Config: {os.path.basename(self.current_config_path)}")

        elif res == ignore_checked_act:
            paths_to_ignore = [p for p in self._checked_paths
                               if p and p not in ("N/A", "")]
            if paths_to_ignore:
                for p in paths_to_ignore:
                    self.ignored_paths.add(p)
                QTimer.singleShot(50, self._build_tree)

        elif res == ignore_act:
            checked_paths = list(self._checked_paths) if hasattr(self, '_checked_paths') else []
            if checked_paths:
                for p in checked_paths:
                    self.ignored_paths.add(p)
            else:
                self.ignored_paths.add(target_path)
            QTimer.singleShot(50, self._build_tree)

        elif res == restore_act:
            self.ignored_paths.discard(target_path)
            QTimer.singleShot(50, self._build_tree)

        elif restore_all_act and res == restore_all_act:
            self.ignored_paths.clear()
            QTimer.singleShot(50, self._build_tree)

        elif calc_size_act and res == calc_size_act:
            item.setText(6, "Calc...")
            item_id = f"{item.text(0)}|{item.text(1)}|{item.text(15)}"
            self.item_map[item_id] = item
            worker = SingleSizeWorker(run_path)
            def _safe_set_size(it, sz):
                try:
                    it.setText(6, sz)
                    old_tip = it.toolTip(0)
                    if old_tip:
                        it.setToolTip(0, re.sub(
                            r'Size: .*?\n', f'Size: {sz}\n', old_tip))
                except RuntimeError:
                    pass
            worker.result.connect(lambda sz: _safe_set_size(item, sz))
            self.size_workers.append(worker)
            worker.finished.connect(
                lambda w=worker: self.size_workers.remove(w)
                if w in self.size_workers else None)
            worker.start()

        elif fm_n_act     and res == fm_n_act:     self._open_file_or_warn(fm_n_path, "NONUPF Formality Report")
        elif fm_u_act     and res == fm_u_act:     self._open_file_or_warn(fm_u_path, "UPF Formality Report")
        elif v_act        and res == v_act:        self._open_file_or_warn(vslp_path, "VSLP Report")
        elif sta_act      and res == sta_act:      self._open_file_or_warn(sta_path,  "PT STA Summary")
        elif ir_stat_act  and res == ir_stat_act:  self._open_file_or_warn(ir_path,   "Static IR Log")
        elif ir_dyn_act   and res == ir_dyn_act:   self._open_file_or_warn(ir_path,   "Dynamic IR Log")
        elif log_act      and res == log_act:      self._open_file_or_warn(log_path,  "Log File")
        elif qor_act      and res == qor_act:
            self._run_single_stage_qor(item, b_name, r_rtl, base_run)

    def on_header_context_menu(self, pos):
        col = self.tree.header().logicalIndexAt(pos)
        if col < 0:
            return
        m = QMenu(self)
        m.addAction("Filter this column...",
                    lambda: self.show_column_filter_dialog(col))
        m.addAction("Clear column filter", lambda: (
            self.active_col_filters.pop(col, None),
            self.apply_tree_filters()))
        m.addSeparator()
        m.addAction("Fit all columns", self.fit_all_columns)
        act = m.addAction(
            "Hide this column",
            lambda: self.tree.setColumnHidden(col, True))
        m.exec_(self.tree.header().mapToGlobal(pos))

    # ------------------------------------------------------------------
    # SIZE CALCULATION
    # ------------------------------------------------------------------
    def calculate_all_sizes(self):
        size_tasks = []
        def gather(node):
            for i in range(node.childCount()):
                child = node.child(i)
                path  = child.text(15)
                if (path and path != "N/A"
                        and child.text(6) in ["-", "N/A", "Calc..."]):
                    item_id = (f"{child.text(0)}|"
                               f"{child.text(1)}|{child.text(15)}")
                    self.item_map[item_id] = child
                    size_tasks.append((item_id, path))
                    child.setText(6, "Calc...")
                gather(child)
        gather(self.tree.invisibleRootItem())
        if size_tasks:
            worker = BatchSizeWorker(size_tasks)
            # Use batch signal: ~10 deliveries instead of 500 individual signals
            worker.sizes_batch_ready.connect(self._on_batch_sizes)
            self.size_workers.append(worker)
            worker.finished.connect(
                lambda w=worker: self.size_workers.remove(w)
                if w in self.size_workers else None)
            worker.start()

    def _on_batch_sizes(self, batch):
        """Handle a batch of (item_id, size_str) tuples from BatchSizeWorker.
        One call per 50 results instead of one call per result — keeps UI fluid."""
        for item_id, size_str in batch:
            self.update_item_size(item_id, size_str)

    # ------------------------------------------------------------------
    # BACKGROUND FE SIGNOFF SCAN
    # ------------------------------------------------------------------
    def start_bg_signoff_scan(self):
        if self._building_tree:
            return
        if self._signoff_worker and self._signoff_worker.isRunning():
            return
        runs = []
        for r in (self.ws_data.get("all_runs", []) +
                  self.out_data.get("all_runs", [])):
            if r.get("path"):
                runs.append(r)
        if not runs:
            return
        self._signoff_bg_done = True
        self.status_bar.showMessage(
            "Owner/FM/VSLP background scan started for {} runs".format(len(runs)),
            5000)
        self._signoff_worker = SignoffStatusWorker(runs)
        self._signoff_worker.batch_ready.connect(self._on_signoff_batch)
        self._signoff_worker.finished.connect(self._on_signoff_finished)
        self._signoff_worker.start()

    def _on_signoff_batch(self, batch):
        for row in batch:
            path = row.get("path")
            item = self._signoff_items_by_path.get(path)
            if not item:
                continue
            run = item.data(0, Qt.UserRole + 10)
            if run:
                if row.get("owner") and row.get("owner") != "Unknown":
                    run["owner"] = row["owner"]
                    item.setText(5, row["owner"])
                if run.get("run_type") == "FE":
                    run["st_n"] = row.get("st_n", "N/A")
                    run["st_u"] = row.get("st_u", "N/A")
                    run["vslp_status"] = row.get("vslp_status", "N/A")
                    item.setText(7, "NONUPF - " + row.get("st_n", "N/A"))
                    item.setText(8, "UPF - " + row.get("st_u", "N/A"))
                    item.setText(9, row.get("vslp_status", "N/A"))
                    self._apply_fm_color(item, 7, item.text(7))
                    self._apply_fm_color(item, 8, item.text(8))
                    self._apply_vslp_color(item, 9, item.text(9))

    def _on_signoff_finished(self):
        self.status_bar.showMessage("Owner/FM/VSLP background scan finished", 5000)

    def update_item_size(self, item_id, size_str):
        item = self.item_map.get(item_id)
        if item is None:
            return
        try:
            item.setText(6, size_str)
            old = item.toolTip(0)
            if old:
                item.setToolTip(0, re.sub(
                    r'Size: .*?\n', f'Size: {size_str}\n', old))
        except RuntimeError:
            self.item_map.pop(item_id, None)

    def fit_all_columns(self):
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i):
                self.tree.resizeColumnToContents(i)
        self._fit_run_name_column()

    def _fit_run_name_column(self):
        try:
            self.tree.resizeColumnToContents(0)
            w = self.tree.columnWidth(0)
            self.tree.setColumnWidth(0, max(380, min(w + 24, 760)))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # CSV EXPORT
    # ------------------------------------------------------------------
    def open_block_summary(self):
        """Open BlockSummaryDialog using CHECKED runs from the tree.
        User must check runs first using the checkboxes in col 0."""
        run_list = []
        root = self.tree.invisibleRootItem()

        def _collect(node):
            nt = node.data(0, Qt.UserRole)
            # Only FE run items that are checked
            if (nt not in ("BLOCK", "MILESTONE", "RTL",
                           "IGNORED_ROOT", "STAGE", "__PLACEHOLDER__")
                    and node.checkState(0) == Qt.Checked
                    and node.text(2) in ("WS", "OUTFEED", "")
                    and "FE" in node.text(0)):
                blk  = node.data(0, Qt.UserRole + 2) or "UNKNOWN"
                path = node.text(15)
                name = node.text(0)
                rt   = node.text(12)
                src  = node.text(2)
                if path and path != "N/A":
                    run_list.append((blk, path, name, rt, src))
            for i in range(node.childCount()):
                _collect(node.child(i))
        _collect(root)

        if not run_list:
            QMessageBox.information(
                self, "Block Summary",
                "Please check (tick) the FE runs you want to include\n"
                "in the summary table, then click Block Summary Table.")
            return

        rtl_label = self.rel_combo.currentText()
        dark = (self.is_dark_mode
                or (self.use_custom_colors
                    and self.custom_bg_color < "#888888"))
        dlg = BlockSummaryDialog(rtl_label, run_list, dark, self)
        dlg.exec_()

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export to CSV", "dashboard_export.csv",
            "CSV Files (*.csv)")
        if not path:
            return
        headers = ([self.tree.headerItem().text(i) for i in range(15)]
                   + ["Alias / Notes"])
        rows = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (not c.isHidden()
                        and c.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL",
                         "IGNORED_ROOT","__PLACEHOLDER__")):
                    rows.append([c.text(j) for j in range(15)]
                                + [c.text(22)])
                collect(c)
        collect(self.tree.invisibleRootItem())
        try:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(headers)
                w.writerows(rows)
            QMessageBox.information(
                self, "Export", f"Exported {len(rows)} rows to:\n{path}")
        except Exception as e:
            QMessageBox.warning(self, "Export Error", str(e))

    # ------------------------------------------------------------------
    # BLOCK LIST
    # ------------------------------------------------------------------
    def _set_all_blocks(self, checked):
        self.blk_list.blockSignals(True)
        for i in range(self.blk_list.count()):
            self.blk_list.item(i).setCheckState(
                Qt.Checked if checked else Qt.Unchecked)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    # ------------------------------------------------------------------
    # FILTER CONFIGS
    # ------------------------------------------------------------------
    def load_filter_config(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Run Filter Config", "",
            "Config Files (*.cfg *.txt)")
        if not path:
            return
        try:
            cfg = {}
            with open(path, 'r', encoding='utf-8',
                      errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split(':')
                    if len(parts) == 4:
                        source, rtl, block, runs_str = parts
                        run_list = [r.strip() for r in runs_str.split(',')]
                        cfg.setdefault(source.strip(), {}).setdefault(
                            rtl.strip(), {})[block.strip()] = run_list
            self.run_filter_config  = cfg
            self.current_config_path = path
            self.sb_config.setText(
                f"Config: {os.path.basename(path)}")
            self.refresh_view()
        except Exception as e:
            QMessageBox.warning(self, "Load Config Error", str(e))

    def clear_filter_config(self):
        self.run_filter_config  = None
        self.current_config_path = None
        self.sb_config.setText("Config: None")
        self.refresh_view()

    def generate_sample_config(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Sample Config", "sample_filter.cfg",
            "Config Files (*.cfg *.txt)")
        if not path:
            return
        sample = (
            "# Format: source:rtl_release:block:run1,run2,...\n"
            "# Example:\n"
            "WS:S5K2P5SP_EVT0_ML4_DEV00_syn1:BLK_CMU:run1,run2\n"
            "OUTFEED:S5K2P5SP_EVT0_ML4_DEV00:BLK_CPU:run1\n")
        with open(path, 'w') as f:
            f.write(sample)
        QMessageBox.information(self, "Sample Config", f"Saved to:\n{path}")

    def _save_current_config(self):
        if not self.current_config_path or not self.run_filter_config:
            return
        with open(self.current_config_path, 'w',
                  encoding='utf-8') as f:
            f.write("# dashboard_filter.cfg\n")
            for src, rtl_dict in self.run_filter_config.items():
                for rtl, blk_dict in rtl_dict.items():
                    for blk, runs in blk_dict.items():
                        f.write(f"{src}:{rtl}:{blk}:{','.join(runs)}\n")

    # ------------------------------------------------------------------
    # SETTINGS DIALOG
    # ------------------------------------------------------------------
    def open_settings(self):
        col_names   = [
            "Run Name", "RTL Release", "Source", "Status", "Stage", "User",
            "Size", "FM-NONUPF", "FM-UPF", "VSLP", "Static IR", "Dynamic IR",
            "Runtime", "Start", "End", "Notes"]
        col_indices = list(range(15)) + [22]

        def _load_preset(key, default_set):
            try:
                saved = prefs.get('PRESETS', key, fallback='')
                if saved:
                    return set(int(x) for x in saved.split(',')
                               if x.strip().isdigit())
            except Exception:
                pass
            return set(default_set)

        cur_compact  = _load_preset('compact',  {0, 3, 4, 5, 12, 13})
        cur_standard = _load_preset('standard',
                                    {0, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14})
        cur_full     = _load_preset('full',     set(range(15)) | {22})

        dlg = QDialog(self)
        dlg.setWindowTitle("Settings")
        dlg.resize(560, 600)
        outer = QVBoxLayout(dlg)
        tabs  = QTabWidget()

        # -- General tab --
        gen_w = QWidget()
        gen_l = QFormLayout(gen_w)
        gen_l.setSpacing(10)

        font_combo = QFontComboBox()
        font_combo.setCurrentFont(QApplication.font())
        gen_l.addRow("Font Family:", font_combo)

        size_spin = QSpinBox()
        size_spin.setRange(8, 24)
        size_spin.setValue(QApplication.font().pointSize() or 10)
        gen_l.addRow("Font Size:", size_spin)

        space_spin = QSpinBox()
        space_spin.setRange(0, 20)
        space_spin.setValue(self.row_spacing)
        gen_l.addRow("Row Spacing (px):", space_spin)

        rel_time_cb = QCheckBox("Show relative timestamps")
        rel_time_cb.setChecked(self.show_relative_time)
        gen_l.addRow("", rel_time_cb)

        ist_cb = QCheckBox("Convert timestamps to IST (from KST)")
        ist_cb.setChecked(self.convert_to_ist)
        gen_l.addRow("", ist_cb)

        # Tapeout date
        # QDateEdit imported at module level as _QDateEditImport
        gen_l.addRow(QLabel("--- Tapeout ---"))
        tapeout_edit = _QDateEditImport()
        tapeout_edit.setDisplayFormat("yyyy-MM-dd")
        tapeout_edit.setCalendarPopup(True)
        if self._tapeout_date:
            td = self._tapeout_date
            tapeout_edit.setDate(QDate(td.year, td.month, td.day))
        else:
            tapeout_edit.setDate(QDate.currentDate().addDays(30))
        tapeout_clear = QCheckBox("Set tapeout date (shows T-N countdown in title)")
        tapeout_clear.setChecked(self._tapeout_date is not None)
        gen_l.addRow("Tapeout Date:", tapeout_edit)
        gen_l.addRow("", tapeout_clear)

        hide_blk_cb = QCheckBox("Hide Block grouping level in tree")
        hide_blk_cb.setChecked(self.hide_block_nodes)
        gen_l.addRow("", hide_blk_cb)

        closure_cb = QCheckBox("Enable Closure Scorecard (colors run names by sign-off status)")
        closure_cb.setChecked(getattr(self, '_closure_enabled', True))
        gen_l.addRow("", closure_cb)

        theme_cb = QCheckBox("Enable Dark Mode")
        theme_cb.setChecked(self.is_dark_mode)
        gen_l.addRow("", theme_cb)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        gen_l.addRow(sep)

        use_custom_cb = QCheckBox("Enable Custom Colors")
        use_custom_cb.setChecked(self.use_custom_colors)
        gen_l.addRow("Custom Theme:", use_custom_cb)

        _colors = [self.custom_bg_color,
                   self.custom_fg_color,
                   self.custom_sel_color]

        def _pick(idx, swatch):
            c = QColorDialog.getColor(QColor(_colors[idx]), dlg)
            if c.isValid():
                _colors[idx] = c.name()
                swatch.setStyleSheet(
                    f"background:{c.name()};border:1px solid #888;")

        for idx, label in enumerate(
                ["Background Color", "Text Color", "Highlight Color"]):
            swatch = QLabel("  ")
            swatch.setFixedSize(60, 20)
            swatch.setStyleSheet(
                f"background:{_colors[idx]};border:1px solid #888;")
            btn = QPushButton(label)
            btn.clicked.connect(lambda _=None, i=idx, s=swatch: _pick(i, s))
            row = QHBoxLayout()
            row.addWidget(btn)
            row.addWidget(swatch)
            gen_l.addRow("", row)

        tabs.addTab(gen_w, "General")

        # -- Column Presets tab --
        preset_w = QWidget()
        preset_outer = QVBoxLayout(preset_w)
        preset_outer.addWidget(QLabel(
            "<b>Choose which columns appear in each view preset.</b><br>"
            "<small>Run Name is always visible. Path/Log columns always hidden.</small>"))

        ptbl = QTableWidget(len(col_names), 3)
        ptbl.setHorizontalHeaderLabels(["Compact", "Standard", "Full"])
        ptbl.setVerticalHeaderLabels(col_names)
        ptbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        ptbl.verticalHeader().setSectionResizeMode(
            QHeaderView.ResizeToContents)
        ptbl.setEditTriggers(QTableWidget.NoEditTriggers)

        preset_checks = {}
        preset_sets   = [cur_compact, cur_standard, cur_full]

        for r, (name, idx) in enumerate(zip(col_names, col_indices)):
            for c, pset in enumerate(preset_sets):
                cw = QWidget()
                cl = QHBoxLayout(cw)
                cl.setContentsMargins(0, 0, 0, 0)
                cl.setAlignment(Qt.AlignCenter)
                cb = QCheckBox()
                cb.setChecked(idx in pset)
                if idx == 0:
                    cb.setChecked(True)
                    cb.setEnabled(False)
                cl.addWidget(cb)
                ptbl.setCellWidget(r, c, cw)
                preset_checks[(r, c)] = cb

        preset_outer.addWidget(ptbl)
        tabs.addTab(preset_w, "Column Presets")

        # -- Shortcuts tab --
        sc_w = QWidget()
        sc_l = QVBoxLayout(sc_w)
        sc_l.addWidget(QLabel("<b>Keyboard Shortcuts</b>"))
        sc_tbl = QTableWidget(0, 2)
        sc_tbl.setHorizontalHeaderLabels(["Shortcut", "Action"])
        sc_tbl.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        sc_tbl.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.Stretch)
        sc_tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        sc_tbl.setAlternatingRowColors(True)
        sc_tbl.verticalHeader().setVisible(False)
        shortcuts_list = [
            ("Ctrl+R",       "Refresh / rescan all workspaces"),
            ("Ctrl+F",       "Focus the search bar"),
            ("Ctrl+E",       "Expand all tree nodes"),
            ("Ctrl+W",       "Collapse all tree nodes"),
            ("Ctrl+C",       "Copy selected cell to clipboard"),
            ("Ctrl+?",       "Open Settings (this dialog)"),
            ("L",            "Open log file for selected run (gvim)"),
            ("D",            "Toggle dark / light mode"),
            ("1",            "Switch to Compact column view"),
            ("2",            "Switch to Standard column view"),
            ("3",            "Switch to Full column view"),
            ("Double-click", "Open log file in gvim"),
            ("Right-click",  "Context menu: pin, diff, note, Gantt..."),
        ]
        for key, action in shortcuts_list:
            r = sc_tbl.rowCount()
            sc_tbl.insertRow(r)
            sc_tbl.setItem(r, 0, QTableWidgetItem(key))
            sc_tbl.setItem(r, 1, QTableWidgetItem(action))
        sc_l.addWidget(sc_tbl)
        tabs.addTab(sc_w, "Shortcuts")

        # -- Milestones tab --
        ms_w = QWidget()
        ms_l = QVBoxLayout(ms_w)
        ms_l.addWidget(QLabel(
            "<b>Milestone Pattern Mapping</b><br>"
            "<small>Pattern is matched as substring of RTL release name.<br>"
            "e.g. pattern <b>_ML2_</b> matches S5K2P5SP_EVT0_ML2_DEV00.<br>"
            "Add custom patterns like _ML0_ -> TAPE-IN for new releases.</small>"))

        ms_tbl = QTableWidget(0, 2)
        ms_tbl.setHorizontalHeaderLabels(["Pattern (e.g. _ML2_)", "Label (e.g. PRE-SVP)"])
        ms_tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        ms_tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        ms_tbl.setAlternatingRowColors(True)
        ms_tbl.verticalHeader().setVisible(False)
        ms_tbl.setSortingEnabled(False)

        # Populate with current map
        current_ms_map = dict(self._milestone_map)
        for pattern, label in current_ms_map.items():
            r = ms_tbl.rowCount(); ms_tbl.insertRow(r)
            ms_tbl.setItem(r, 0, QTableWidgetItem(pattern))
            ms_tbl.setItem(r, 1, QTableWidgetItem(label))

        ms_l.addWidget(ms_tbl)

        ms_btn_row = QHBoxLayout()
        add_ms_btn = QPushButton("Add Row")
        del_ms_btn = QPushButton("Delete Selected Row")
        reset_ms_btn = QPushButton("Reset to Defaults")
        add_ms_btn.clicked.connect(lambda: (
            ms_tbl.insertRow(ms_tbl.rowCount()),
            ms_tbl.setItem(ms_tbl.rowCount()-1, 0, QTableWidgetItem("")),
            ms_tbl.setItem(ms_tbl.rowCount()-1, 1, QTableWidgetItem(""))))
        del_ms_btn.clicked.connect(lambda: (
            ms_tbl.removeRow(ms_tbl.currentRow())
            if ms_tbl.currentRow() >= 0 else None))
        reset_ms_btn.clicked.connect(lambda: (
            ms_tbl.setRowCount(0),
            [ms_tbl.insertRow(r) or
             ms_tbl.setItem(r, 0, QTableWidgetItem(p)) or
             ms_tbl.setItem(r, 1, QTableWidgetItem(l))
             for r, (p, l) in enumerate({
                 "_ML1_":"INITIAL RELEASE","_ML2_":"PRE-SVP",
                 "_ML3_":"SVP","_ML4_":"FFN"}.items())]))
        ms_btn_row.addWidget(add_ms_btn)
        ms_btn_row.addWidget(del_ms_btn)
        ms_btn_row.addWidget(reset_ms_btn)
        ms_l.addLayout(ms_btn_row)
        ms_l.addWidget(QLabel(
            "<small><i>Changes take effect after next Refresh.</i></small>"))
        tabs.addTab(ms_w, "Milestones")

        # -- QoR Script tab --
        qor_w = QWidget()
        qor_l = QFormLayout(qor_w)
        qor_l.setSpacing(10)
        qor_l.addRow(QLabel(
            "<b>QoR Summary Script Path</b><br>"
            "<small>Path to summary.py used for QoR comparison.<br>"
            "Set this to use the Compare QoR feature.</small>"))
        qor_script_edit = QLineEdit()
        try:
            qor_script_edit.setText(QOR_SUMMARY_SCRIPT)
        except NameError:
            saved_qor = prefs.get('QOR', 'script_path', fallback='')
            qor_script_edit.setText(saved_qor)
        qor_script_edit.setPlaceholderText(
            "/user/scripts/summary/summary.py")
        browse_btn = QPushButton("Browse...")
        def _browse_qor():
            p, _ = QFileDialog.getOpenFileName(
                dlg, "Select summary.py", "",
                "Python Files (*.py)")
            if p:
                qor_script_edit.setText(p)
        browse_btn.clicked.connect(_browse_qor)
        row_qor = QHBoxLayout()
        row_qor.addWidget(qor_script_edit, 1)
        row_qor.addWidget(browse_btn)
        qor_l.addRow("summary.py path:", row_qor)
        qor_l.addWidget(QLabel(
            "<small><i>Saved to user_prefs.ini. "
            "Also add QOR_SUMMARY_SCRIPT = '...' to config.py "
            "to make it permanent.</i></small>"))
        tabs.addTab(qor_w, "QoR Script")

        outer.addWidget(tabs)
        btn_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        outer.addWidget(btn_box)

        if not dlg.exec_():
            return

        # Apply general settings
        _need_rebuild = False  # set True below if tree structure changes
        font = font_combo.currentFont()
        font.setPointSize(size_spin.value())
        QApplication.setFont(font)
        self.is_dark_mode       = theme_cb.isChecked()
        self.use_custom_colors  = use_custom_cb.isChecked()
        self.custom_bg_color    = _colors[0]
        self.custom_fg_color    = _colors[1]
        self.custom_sel_color   = _colors[2]
        self.row_spacing        = space_spin.value()
        old_rel_time = self.show_relative_time
        old_ist      = self.convert_to_ist
        self.show_relative_time = rel_time_cb.isChecked()
        self.convert_to_ist     = ist_cb.isChecked()
        if old_rel_time != self.show_relative_time or old_ist != self.convert_to_ist:
            QTimer.singleShot(50, self._refresh_timestamps)
        old_hide_blk = self.hide_block_nodes
        self.hide_block_nodes   = hide_blk_cb.isChecked()
        _need_rebuild = _need_rebuild or (old_hide_blk != self.hide_block_nodes)
        self._closure_enabled   = closure_cb.isChecked()
        prefs.set('UI', 'closure_enabled',
                  'true' if self._closure_enabled else 'false')

        # Save tapeout date
        import datetime
        if tapeout_clear.isChecked():
            qd = tapeout_edit.date()
            self._tapeout_date = datetime.datetime(qd.year(), qd.month(), qd.day())
            prefs.set('UI', 'tapeout_date',
                      self._tapeout_date.strftime('%Y-%m-%d'))
        else:
            self._tapeout_date = None
            prefs.set('UI', 'tapeout_date', '')
        self._update_title()

        # Apply column presets
        new_presets = [{}, {}, {}]
        for r, (name, idx) in enumerate(zip(col_names, col_indices)):
            for c in range(3):
                cb = preset_checks.get((r, c))
                if cb and cb.isChecked():
                    new_presets[c][idx] = True

        compact_set  = set(new_presets[0].keys()) | {0}
        standard_set = set(new_presets[1].keys()) | {0}
        full_set     = set(new_presets[2].keys()) | {0}

        if not prefs.has_section('PRESETS'):
            prefs.add_section('PRESETS')
        prefs.set('PRESETS', 'compact',
                  ','.join(str(i) for i in sorted(compact_set)))
        prefs.set('PRESETS', 'standard',
                  ','.join(str(i) for i in sorted(standard_set)))
        prefs.set('PRESETS', 'full',
                  ','.join(str(i) for i in sorted(full_set)))
        with open(USER_PREFS_FILE, 'w') as f:
            prefs.write(f)

        self._preset_compact  = compact_set
        self._preset_standard = standard_set
        self._preset_full     = full_set

        # Save QoR script path
        qor_path_val = qor_script_edit.text().strip()
        if not prefs.has_section('QOR'):
            prefs.add_section('QOR')
        prefs.set('QOR', 'script_path', qor_path_val)
        if qor_path_val:
            # Inject into module globals so try/except in run_qor finds it
            import builtins
            builtins.QOR_SUMMARY_SCRIPT = qor_path_val

        # Save milestone map
        new_ms_map = {}
        for r in range(ms_tbl.rowCount()):
            p_item = ms_tbl.item(r, 0)
            l_item = ms_tbl.item(r, 1)
            if p_item and l_item:
                p = p_item.text().strip()
                l = l_item.text().strip()
                if p and l:
                    new_ms_map[p] = l
        if new_ms_map:
            self._milestone_map = new_ms_map
            self._save_milestone_map(new_ms_map)

        self.apply_theme_and_spacing()
        if _need_rebuild:
            # hide_block_nodes changed -- must rebuild tree structure
            QTimer.singleShot(50, self._build_tree)
        else:
            self.refresh_view()
        current_mode = self.mode_combo.currentText()
        self._set_col_preset(
            {"Standard": 2, "Compact": 1, "Full": 3}.get(current_mode, 2))

    # ------------------------------------------------------------------
    # DISK USAGE
    # ------------------------------------------------------------------
    def open_disk_usage(self):
        data = getattr(self, "_disk_data", None)
        if not data:
            QMessageBox.information(
                self, "Disk Space",
                "Disk scan not yet complete. Please wait a moment and try again.")
            return
        dlg = DiskUsageDialog(data, self.is_dark_mode, self)
        dlg.exec_()

    def start_bg_disk_scan(self, force=False):
        if (not force and hasattr(self, '_disk_scan_worker')
                and self._disk_scan_worker.isRunning()):
            return
        # Disable disk button while scanning
        if hasattr(self, 'disk_btn'):
            self.disk_btn.setEnabled(False)
            self.disk_btn.setText("Scanning Disk...")
        self._disk_scan_worker = DiskScannerWorker()
        # DiskScannerWorker uses finished_scan signal
        sig = getattr(self._disk_scan_worker, "finished_scan", None)
        if sig is None:
            sig = self._disk_scan_worker.finished
        sig.connect(self._on_bg_disk_scan_finished)
        self._disk_scan_worker.start()

    def _on_bg_disk_scan_finished(self, data):
        self._disk_data = data
        # Re-enable disk button
        if hasattr(self, 'disk_btn'):
            self.disk_btn.setEnabled(True)
            self.disk_btn.setText("Disk Space")

    # ------------------------------------------------------------------
    # QoR
    # ------------------------------------------------------------------
    def run_qor_comparison(self):
        """Run summary.py on checked runs then open HTML in Firefox."""
        # Collect checked run paths -- normalize trailing slash
        sel = []
        for item in self._iter_checked_items():
            path = item.text(15)
            if not path or path == "N/A":
                continue
            if item.text(2) == "OUTFEED":
                path = os.path.dirname(path)
            if not path.endswith("/"):
                path += "/"
            sel.append(path)

        if len(sel) < 2:
            QMessageBox.information(
                self, "QoR Compare",
                "Please check at least 2 runs first.\n"
                "(Check boxes in the Run Name column)")
            return

        script = self._resolve_qor_script()
        if not script: return

        worker = QoRWorker(script, sel, _PYTHON_BIN)
        worker.finished.connect(self._on_qor_done)
        worker.start()
        self._qor_worker = worker

    def _on_qor_done(self, html_path):
        if html_path and os.path.exists(html_path):
            subprocess.Popen([FIREFOX_PATH, html_path])
        else:
            # Also try finding latest in qor_metrices/
            import glob as _glob
            hits = _glob.glob(
                os.path.join(os.getcwd(), "qor_metrices", "**", "*.html"),
                recursive=True)
            if hits:
                latest = sorted(hits, key=os.path.getmtime)[-1]
                subprocess.Popen([FIREFOX_PATH, latest])
            else:
                QMessageBox.warning(
                    self, "QoR Compare",
                    "QoR script ran but no HTML output found.\n"
                    "Check terminal output for errors.")

    def _run_single_stage_qor(self, item, b_name, r_rtl, base_run):
        """Run QoR for a single PNR stage.
        Call: python3.6 summary.py /path/to/run-BE/ -stage {stage_name}
        The stage name is the step name e.g. place_opt, route_opt."""
        stage_name  = item.text(0)
        parent_item = item.parent()
        be_run_path = parent_item.text(15) if parent_item else item.text(15)

        script = self._resolve_qor_script()
        if not script: return

        # Ensure trailing slash as summary.py expects
        if be_run_path and not be_run_path.endswith("/"):
            be_run_path += "/"

        worker = QoRWorker(script, [be_run_path, "-stage", stage_name],
                            _PYTHON_BIN)
        worker.finished.connect(self._on_qor_done)
        worker.start()
        self._qor_worker = worker

    def _resolve_qor_script(self):
        """Find summary.py from QOR_SUMMARY_SCRIPT / prefs / project_config.ini."""
        script = ""
        try:
            script = QOR_SUMMARY_SCRIPT
        except NameError:
            pass
        if not script:
            script = prefs.get("QOR", "script_path", fallback="") or _SUMMARY_SCRIPT
        if not script or not os.path.exists(script):
            QMessageBox.warning(
                self, "QoR Script Not Found",
                "summary.py path not configured.\n"
                "Go to Settings > QoR Script and browse to summary.py.\n\n"
                "Or add to project_config.ini:\n"
                "SUMMARY_SCRIPT = /path/to/summary.py")
            return ""
        return script

    def _iter_checked_items(self):
        items = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (c.checkState(0) == Qt.Checked
                        and c.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL",
                         "IGNORED_ROOT","STAGE","__PLACEHOLDER__")):
                    items.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())
        return items

    # ------------------------------------------------------------------
    # MAIL
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        """Collect checked runs, group by owner, compose cleanup mail."""
        checked = self._iter_checked_items()
        if not checked:
            QMessageBox.information(self, "Cleanup Mail",
                                    "Please check some runs first.")
            return

        # Build owner -> [(path, size)] mapping, skip golden pins
        user_runs = {}
        for c in checked:
            path  = c.text(15)
            owner = c.text(5)
            size  = c.text(6) if c.text(6) not in ("-","N/A","Calc...","") else "?"
            if not path or path == "N/A":
                continue
            if self.user_pins.get(path) == "golden":
                continue
            if not owner or owner == "Unknown":
                owner = "Unknown"
            if owner not in user_runs:
                user_runs[owner] = []
            user_runs[owner].append((path, size))

        if not user_runs:
            QMessageBox.information(self, "Cleanup Mail",
                                    "No non-golden runs selected.")
            return

        # Build body with path + size
        body_lines = [
            "Hi,",
            "",
            "Please remove these runs as they are consuming disk space:",
            ""]
        for owner, items in sorted(user_runs.items()):
            body_lines.append(f"Owner: {owner}")
            for path, sz in items:
                body_lines.append(f"  {path}  [{sz}]")
            body_lines.append("")
        body_lines.append("Thank you.")

        # Pre-fill To with owner emails
        owner_emails = []
        for owner in user_runs:
            if owner != "Unknown":
                e = _get_user_email(owner)
                if e:
                    owner_emails.append(e)

        all_known = _get_all_known_mail_users()
        dlg = AdvancedMailDialog(
            "Action Required: Please clean up disk space runs",
            "\n".join(body_lines),
            all_known,
            ", ".join(owner_emails),
            self)

        if dlg.exec_():
            _send_mail_via_util(dlg)

    def send_qor_mail_action(self):
        all_known = _get_all_known_mail_users()
        dlg = AdvancedMailDialog(
            "Latest Compare QoR Report",
            "Hi Team,\n\nPlease find the attached latest QoR Report.\n\nRegards",
            all_known, "", self)
        dlg._attach_qor()  # auto-attach latest report
        if dlg.exec_():
            _send_mail_via_util(dlg)

    def _toggle_selected_only(self):
        """Click on Selected count label → toggle Selected Only view."""
        if self.view_combo.currentText() == "Selected Only":
            self.view_combo.setCurrentText("All Runs")
        else:
            if not self._checked_paths:
                return  # nothing checked, ignore
            self.view_combo.setCurrentText("Selected Only")

    def send_custom_mail_action(self):
        all_known = _get_all_known_mail_users()
        dlg = AdvancedMailDialog("", "", all_known, "", self)
        if dlg.exec_():
            _send_mail_via_util(dlg)

    def _open_mail_compose_dialog(self, subject="", body="", prefill_to="",
                                   html_body=""):
        """Open AdvancedMailDialog pre-filled with subject/body.
        Called by BlockSummaryDialog 'Send as Mail' button.
        Pass html_body to send as HTML email (rendered table etc.)."""
        all_known = _get_all_known_mail_users()
        # Show rendered HTML preview in body widget; store raw HTML for sending
        display_body = html_body if html_body else body
        dlg = AdvancedMailDialog(subject, display_body, all_known, prefill_to, self)
        if html_body:
            dlg._html_body = html_body  # picked up by _send_mail_via_util
        if dlg.exec_():
            _send_mail_via_util(dlg)

    # ------------------------------------------------------------------
    # ANALYTICS
    # ------------------------------------------------------------------
    def show_analytics(self):
        """Analytics Dashboard -- reads raw scan data directly.
        Uses ws_data + out_data (complete scan results, all blocks/sources).
        No tree dependency. Deduplicates by run path."""
        from PyQt5.QtWidgets import (QTableWidget, QTableWidgetItem,
                                     QHeaderView, QTabWidget)

        def _hrs(rt):
            try:
                m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
                if m:
                    h = (int(m.group(1))
                         + int(m.group(2))/60
                         + int(m.group(3))/3600)
                    return h if h > 0.001 else None
            except Exception:
                pass
            return None

        def _stage_group(name):
            parts = name.split("_")
            return "_".join(parts[:2]) if len(parts) >= 3 else name

        def _status(r):
            if r.get("is_comp"):
                return "COMPLETED"
            st = r.get("fe_status", "").strip()
            return st if st else "NOT STARTED"

        seen_paths = set()
        fe_runs    = []
        be_runs    = []
        for r in (self.ws_data.get("all_runs", []) +
                  self.out_data.get("all_runs", [])):
            p = r.get("path", "")
            if p in seen_paths:
                continue
            seen_paths.add(p)
            # Ensure block field is populated -- fall back to path extraction
            if not r.get("block"):
                # Try to extract block from path:
                # .../IMPLEMENTATION/S5K2P5SP/SOC/BLK_CMU/fc/run-FE
                m = re.search(r'/SOC/([^/]+)/', p)
                if not m:
                    # Try without SOC level:
                    # .../IMPLEMENTATION/PROJ/BLK_CMU/fc/run-FE
                    m = re.search(r'/IMPLEMENTATION/[^/]+/([^/]+)/', p)
                if m:
                    r = dict(r)  # copy so we don't mutate original
                    r["block"] = m.group(1)
            if r.get("run_type") == "FE":
                fe_runs.append(r)
            else:
                be_runs.append(r)

        if not fe_runs and not be_runs:
            QMessageBox.information(
                self, "Analytics",
                "No run data yet. Please wait for a scan to complete.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(
            f"Analytics Dashboard  "
            f"({len(fe_runs)} FE runs, {len(be_runs)} BE runs)")
        dlg.resize(1020, 700)
        layout = QVBoxLayout(dlg)

        # Summary bar
        filter_bar = QHBoxLayout()
        filter_bar.addStretch()
        filter_bar.addWidget(QLabel(
            f"<small>Total: {len(fe_runs)} FE runs, {len(be_runs)} BE runs</small>"))
        layout.addLayout(filter_bar)

        tabs   = QTabWidget()

        # TAB 1: FE Block Summary
        blk_stats = {}
        for r in fe_runs:
            blk = r.get("block", "Unknown") or "Unknown"
            if blk not in blk_stats:
                blk_stats[blk] = dict(
                    total=0, comp=0, running=0,
                    failed=0, ns=0, rts=[], sources=set())
            s  = blk_stats[blk]
            st = _status(r)
            s["total"]   += 1
            s["sources"].add(r.get("source", ""))
            if   st == "COMPLETED":                             s["comp"]    += 1
            elif st == "RUNNING":                               s["running"] += 1
            elif st in ("FAILED","FATAL ERROR","INTERRUPTED"):  s["failed"]  += 1
            else:                                               s["ns"]      += 1
            if r.get("is_comp"):
                h = _hrs(r.get("info", {}).get("runtime", ""))
                if h: s["rts"].append(h)

        t1 = QTableWidget(0, 8)
        t1.setHorizontalHeaderLabels([
            "Block","Source","Total","Completed",
            "Running","Failed","Not Started","Avg Runtime (hrs)"])
        t1.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        t1.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents)
        t1.horizontalHeader().setSectionResizeMode(7, QHeaderView.Stretch)
        for i in range(2, 7):
            t1.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.ResizeToContents)
        t1.setEditTriggers(QTableWidget.NoEditTriggers)
        t1.setAlternatingRowColors(True)
        t1.verticalHeader().setVisible(False)
        t1.setSortingEnabled(False)  # enable AFTER insert to avoid row misalignment

        for blk, s in sorted(blk_stats.items()):
            row = t1.rowCount(); t1.insertRow(row)
            rts = s["rts"]
            avg = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            src = "+".join(sorted(x for x in s["sources"] if x))
            vals = [blk, src, s["total"], s["comp"],
                    s["running"], s["failed"], s["ns"], avg]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(
                    Qt.AlignLeft | Qt.AlignVCenter if c <= 1
                    else Qt.AlignCenter)
                if c == 5 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#d32f2f"))
                if c == 3 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#388e3c"))
                if c == 4 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#1976d2"))
                t1.setItem(row, c, it)
        t1.setSortingEnabled(True)
        tabs.addTab(t1, f"FE Block Summary ({len(fe_runs)} runs)")

        # TAB 2: BE Stage Summary
        stage_stats = {}
        for r in be_runs:
            blk = r.get("block", "Unknown") or "Unknown"
            for stage in r.get("stages", []):
                name = stage.get("name", "")
                if not name:
                    continue
                grp = _stage_group(name)
                key = (blk, grp)
                if key not in stage_stats:
                    stage_stats[key] = dict(
                        count=0, with_rt=0, rts=[], examples=set())
                s = stage_stats[key]
                s["count"]   += 1
                s["examples"].add(name)
                h = _hrs(stage.get("info", {}).get("runtime", ""))
                if h:
                    s["with_rt"] += 1
                    s["rts"].append(h)

        t2 = QTableWidget(0, 6)
        t2.setHorizontalHeaderLabels([
            "Block","Stage Group","Example Names",
            "Total","With Runtime","Avg Runtime (hrs)"])
        t2.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        t2.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents)
        t2.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        for i in [3, 4, 5]:
            t2.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.ResizeToContents)
        t2.setEditTriggers(QTableWidget.NoEditTriggers)
        t2.setAlternatingRowColors(True)
        t2.verticalHeader().setVisible(False)
        t2.setSortingEnabled(False)

        for (blk, grp), s in sorted(stage_stats.items()):
            row = t2.rowCount(); t2.insertRow(row)
            rts = s["rts"]
            avg = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            exs = ", ".join(sorted(s["examples"])[:3])
            if len(s["examples"]) > 3:
                exs += "..."
            vals = [blk, grp, exs, s["count"], s["with_rt"], avg]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(
                    Qt.AlignLeft | Qt.AlignVCenter if c <= 2
                    else Qt.AlignCenter)
                t2.setItem(row, c, it)

        note = QLabel(
            "<small><i>"
            "Stages grouped by first 2 underscore-separated parts. "
            "eco01_abcd + eco01_xyz both appear under eco01. "
            "Avg runtime uses only stages where runtime data is available."
            "</i></small>")
        t2.setSortingEnabled(True)
        w2 = QWidget(); l2 = QVBoxLayout(w2)
        l2.setContentsMargins(0, 0, 0, 0)
        l2.addWidget(t2); l2.addWidget(note)
        tabs.addTab(w2, f"BE Stage Summary ({len(be_runs)} runs)")

        # TAB 3: RTL Release Summary
        rtl_fe = {}; rtl_be = {}
        for r in fe_runs:
            rtl = r.get("rtl", "Unknown") or "Unknown"
            if rtl not in rtl_fe:
                rtl_fe[rtl] = dict(total=0, comp=0, fail=0)
            rtl_fe[rtl]["total"] += 1
            if r.get("is_comp"):
                rtl_fe[rtl]["comp"] += 1
            elif r.get("fe_status","") in (
                    "FAILED","FATAL ERROR","INTERRUPTED"):
                rtl_fe[rtl]["fail"] += 1
        for r in be_runs:
            rtl = r.get("rtl", "Unknown") or "Unknown"
            if rtl not in rtl_be:
                rtl_be[rtl] = dict(total=0, comp=0)
            rtl_be[rtl]["total"] += 1
            if r.get("is_comp"):
                rtl_be[rtl]["comp"] += 1

        t3 = QTableWidget(0, 6)
        t3.setHorizontalHeaderLabels([
            "RTL Release","FE Total","FE Completed",
            "FE Failed","BE Total","BE Completed"])
        t3.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        for i in range(1, 6):
            t3.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.ResizeToContents)
        t3.setEditTriggers(QTableWidget.NoEditTriggers)
        t3.setAlternatingRowColors(True)
        t3.verticalHeader().setVisible(False)
        t3.setSortingEnabled(False)

        for rtl in sorted(set(rtl_fe) | set(rtl_be)):
            fe = rtl_fe.get(rtl, dict(total=0, comp=0, fail=0))
            be = rtl_be.get(rtl, dict(total=0, comp=0))
            row = t3.rowCount(); t3.insertRow(row)
            for c, v in enumerate([rtl, fe["total"], fe["comp"],
                                    fe["fail"], be["total"], be["comp"]]):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(
                    Qt.AlignLeft | Qt.AlignVCenter if c == 0
                    else Qt.AlignCenter)
                if c == 3 and str(v) != "0":
                    it.setForeground(QColor("#d32f2f"))
                if c in (2, 5) and str(v) != "0":
                    it.setForeground(QColor("#388e3c"))
                t3.setItem(row, c, it)
        t3.setSortingEnabled(True)
        tabs.addTab(t3, "RTL Release Summary")

        # TAB 4: WS vs OUTFEED
        src_stats = {}
        for r in fe_runs:
            k  = r.get("source", "Unknown") or "Unknown"
            if k not in src_stats:
                src_stats[k] = dict(total=0, comp=0, running=0, fail=0)
            s  = src_stats[k]
            st = _status(r)
            s["total"] += 1
            if   st == "COMPLETED":                            s["comp"]    += 1
            elif st == "RUNNING":                              s["running"] += 1
            elif st in ("FAILED","FATAL ERROR","INTERRUPTED"): s["fail"]    += 1

        t4 = QTableWidget(0, 5)
        t4.setHorizontalHeaderLabels(
            ["Source","FE Total","Completed","Running","Failed"])
        t4.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        for i in range(1, 5):
            t4.horizontalHeader().setSectionResizeMode(i, QHeaderView.Stretch)
        t4.setEditTriggers(QTableWidget.NoEditTriggers)
        t4.setAlternatingRowColors(True)
        t4.verticalHeader().setVisible(False)

        for src, s in sorted(src_stats.items()):
            row = t4.rowCount(); t4.insertRow(row)
            for c, v in enumerate([src, s["total"], s["comp"],
                                    s["running"], s["fail"]]):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(Qt.AlignCenter)
                if c == 4 and str(v) != "0":
                    it.setForeground(QColor("#d32f2f"))
                if c == 2 and str(v) != "0":
                    it.setForeground(QColor("#388e3c"))
                t4.setItem(row, c, it)
        tabs.addTab(t4, "WS vs OUTFEED")

        layout.addWidget(tabs)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.accept)
        layout.addWidget(close_btn)
        dlg.exec_()

    # ------------------------------------------------------------------
    # TEAM WORKLOAD
    # ------------------------------------------------------------------
    def show_team_workload(self):
        """Team Workload -- reads raw scan data. Deduplicates by run path."""
        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView

        def _hrs(rt):
            try:
                m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
                if m:
                    h = (int(m.group(1))
                         + int(m.group(2))/60
                         + int(m.group(3))/3600)
                    return h if h > 0.001 else None
            except Exception:
                pass
            return None

        def _owner(r):
            o = r.get("owner", "")
            if o and o != "Unknown":
                return o
            path = r.get("path", "") or r.get("parent", "")
            m = re.search(r'/WS/([^/_]+(?:\.[^/_]+)*)_', path)
            return m.group(1) if m else "Unknown"

        def _status(r):
            if r.get("is_comp"):
                return "COMPLETED"
            return (r.get("fe_status", "NOT STARTED").strip()
                    or "NOT STARTED")

        seen     = set()
        all_runs = []
        for r in (self.ws_data.get("all_runs", []) +
                  self.out_data.get("all_runs", [])):
            p = r.get("path", "")
            if p not in seen:
                seen.add(p)
                # Ensure block is populated
                if not r.get("block"):
                    m = re.search(r'/SOC/([^/]+)/', p)
                    if not m:
                        m = re.search(r'/IMPLEMENTATION/[^/]+/([^/]+)/', p)
                    if m:
                        r = dict(r)
                        r["block"] = m.group(1)
                all_runs.append(r)

        if not all_runs:
            QMessageBox.information(
                self, "Team Workload",
                "No run data yet. Please wait for a scan to complete.")
            return

        stats = {}

        def _ensure(owner):
            if owner not in stats:
                stats[owner] = dict(
                    fe_total=0, fe_comp=0, fe_run=0,
                    fe_fail=0, fe_ns=0,
                    be_total=0, be_comp=0,
                    blocks=set(), sources=set(), rts=[])
            return stats[owner]

        for r in all_runs:
            owner  = _owner(r)
            blk    = r.get("block",  "") or ""
            source = r.get("source", "") or ""
            s = _ensure(owner)
            if blk:    s["blocks"].add(blk)
            if source: s["sources"].add(source)

            if r.get("run_type") == "FE":
                s["fe_total"] += 1
                st = _status(r)
                if st == "COMPLETED":
                    s["fe_comp"] += 1
                    h = _hrs(r.get("info", {}).get("runtime", ""))
                    if h: s["rts"].append(h)
                elif st == "RUNNING":
                    s["fe_run"] += 1
                elif st in ("FAILED","FATAL ERROR","INTERRUPTED"):
                    s["fe_fail"] += 1
                else:
                    s["fe_ns"] += 1
            else:
                s["be_total"] += 1
                if r.get("is_comp"):
                    s["be_comp"] += 1

        dlg = QDialog(self)
        dlg.setWindowTitle("Team Workload View")
        dlg.resize(980, 500)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(
            f"<b>Team Workload</b> -- {len(stats)} engineers, "
            f"{len(all_runs)} unique runs  (FE + BE, WS + OUTFEED)"))

        tbl = QTableWidget(0, 9)
        tbl.setHorizontalHeaderLabels([
            "Engineer","Source","Blocks",
            "FE Total","FE Done","FE Running","FE Failed",
            "BE Total","FE Avg Runtime"])
        tbl.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        for i in range(3, 9):
            tbl.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.ResizeToContents)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)
        tbl.setSortingEnabled(False)  # enable after insert

        for owner, s in sorted(
                stats.items(),
                key=lambda x: -(x[1]["fe_total"] + x[1]["be_total"])):
            row = tbl.rowCount(); tbl.insertRow(row)
            rts     = s["rts"]
            avg_h   = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            blk_str = ", ".join(sorted(s["blocks"]))
            src_str = "+".join(sorted(x for x in s["sources"] if x))
            vals = [owner, src_str, blk_str,
                    s["fe_total"], s["fe_comp"], s["fe_run"],
                    s["fe_fail"], s["be_total"], avg_h]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(
                    Qt.AlignLeft | Qt.AlignVCenter if c == 2
                    else Qt.AlignCenter)
                if c == 6 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#d32f2f"))
                if c == 4 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#388e3c"))
                if c == 5 and str(v) not in ("0","N/A"):
                    it.setForeground(QColor("#1976d2"))
                tbl.setItem(row, c, it)

        tbl.setSortingEnabled(True)
        layout.addWidget(tbl)
        hint = QLabel(
            "Double-click any row to filter tree to that engineer's runs.")
        hint.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(hint)
        tbl.cellDoubleClicked.connect(
            lambda r, c, t=tbl: (
                self.search.setText(
                    t.item(r, 0).text() if t.item(r, 0) else ""),
                dlg.accept()))
        btn = QPushButton("Close")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    # ------------------------------------------------------------------
    # FAILED DIGEST
    # ------------------------------------------------------------------
    def show_failed_digest(self):
        groups = {"FATAL ERROR": [], "INTERRUPTED": [], "FM FAILS": [],
                  "VSLP ERRORS": [], "NOT STARTED": []}
        def collect(node):
            for i in range(node.childCount()):
                c  = node.child(i)
                nt = c.data(0, Qt.UserRole)
                if nt not in ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                              "STAGE","__PLACEHOLDER__"):
                    st   = c.text(3); fm = c.text(7); vslp = c.text(9)
                    blk  = c.data(0, Qt.UserRole + 2) or ""
                    user = c.text(5); run = c.text(0); log = c.text(16)
                    entry = (blk, run, user, log)
                    if st == "FATAL ERROR":   groups["FATAL ERROR"].append(entry)
                    elif st == "INTERRUPTED": groups["INTERRUPTED"].append(entry)
                    elif st == "NOT STARTED": groups["NOT STARTED"].append(entry)
                    if "FAILS" in fm or "FAILS" in c.text(8):
                        groups["FM FAILS"].append(entry)
                    if "Error" in vslp and "Error: 0" not in vslp:
                        groups["VSLP ERRORS"].append(entry)
                collect(c)
        collect(self.tree.invisibleRootItem())

        total = sum(len(v) for v in groups.values())
        dlg   = QDialog(self)
        dlg.setWindowTitle(f"Failed Runs Digest  ({total} issues)")
        dlg.resize(700, 480)
        layout = QVBoxLayout(dlg)

        tabs = QTabWidget()
        for grp_name, items in groups.items():
            if not items:
                continue
            tbl = QTableWidget(0, 4)
            tbl.setHorizontalHeaderLabels(["Block","Run","User","Log"])
            tbl.horizontalHeader().setSectionResizeMode(
                3, QHeaderView.Stretch)
            tbl.setEditTriggers(QTableWidget.NoEditTriggers)
            tbl.setAlternatingRowColors(True)
            tbl.verticalHeader().setVisible(False)
            for blk, run, user, log in items:
                r = tbl.rowCount(); tbl.insertRow(r)
                tbl.setItem(r, 0, QTableWidgetItem(blk))
                tbl.setItem(r, 1, QTableWidgetItem(run))
                tbl.setItem(r, 2, QTableWidgetItem(user))
                tbl.setItem(r, 3, QTableWidgetItem(log))
            tbl.cellDoubleClicked.connect(
                lambda row, col, t=tbl: subprocess.Popen(
                    ['gvim', t.item(row, 3).text()])
                if (col == 3 and t.item(row, 3)
                    and os.path.exists(t.item(row, 3).text()))
                else None)
            tabs.addTab(tbl, f"{grp_name} ({len(items)})")

        layout.addWidget(tabs)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    # ------------------------------------------------------------------
    # RUN DIFF (N runs)
    # ------------------------------------------------------------------
    def _parse_dashboard_time(self, s):
        if not s or s in ("-", "N/A", "Unknown"):
            return None
        for fmt in ("%a %b %d, %Y - %H:%M:%S",
                    "%b %d, %Y - %H:%M:%S",
                    "%b %d, %Y - %H:%M",
                    "%b %d, %Y"):
            try:
                return datetime.datetime.strptime(str(s).strip(), fmt)
            except Exception:
                pass
        return None

    def _fmt_gap(self, a, b):
        if not a or not b:
            return "-"
        secs = int((b - a).total_seconds())
        sign = "-" if secs < 0 else ""
        secs = abs(secs)
        h = secs // 3600
        m = (secs % 3600) // 60
        return "{}{:02d}h:{:02d}m".format(sign, h, m)

    def _valid_timeline_event(self, ev):
        if not ev:
            return False
        runtime = str(ev.get("runtime", "") or "").strip()
        if runtime in ("", "-", "N/A", "Unknown"):
            return False
        start_dt = self._parse_dashboard_time(ev.get("start"))
        end_dt = self._parse_dashboard_time(ev.get("end"))
        return bool(start_dt and end_dt and end_dt >= start_dt)

    def _stage_info_for_timeline(self, be_run, stage):
        info = dict(stage.get("info", {}) or {})
        if info.get("start") not in ("", "-", "N/A", None):
            return info
        for cand in stage.get("_rpt_cands", [stage.get("rpt", "")]):
            try:
                if cand and cached_exists(cand):
                    return parse_pnr_runtime_rpt(cand)
            except Exception:
                pass
        return info

    def _timeline_events_for_item(self, item):
        node_type = item.data(0, Qt.UserRole)
        base_item = item.parent() if node_type == "STAGE" else item
        run = base_item.data(0, Qt.UserRole + 10)
        if not run:
            return []

        events = []
        fe_item = base_item.parent() if run.get("run_type") == "BE" else base_item
        fe_run = fe_item.data(0, Qt.UserRole + 10) if fe_item else None

        if fe_run and fe_run.get("run_type") == "FE":
            ev = {
                "name": fe_run.get("r_name", fe_item.text(0)),
                "kind": "FE",
                "start": fe_item.data(0, Qt.UserRole + 40) or fe_run.get("info", {}).get("start", "-"),
                "end": fe_item.data(0, Qt.UserRole + 41) or fe_run.get("info", {}).get("end", "-"),
                "runtime": fe_run.get("info", {}).get("runtime", "-"),
            }
            if self._valid_timeline_event(ev):
                events.append(ev)

        be_items = []
        if run.get("run_type") == "BE":
            be_items = [base_item]
        elif run.get("run_type") == "FE":
            for i in range(base_item.childCount()):
                ch = base_item.child(i)
                ch_run = ch.data(0, Qt.UserRole + 10)
                if ch_run and ch_run.get("run_type") == "BE":
                    be_items.append(ch)

        for be_item in be_items:
            be_run = be_item.data(0, Qt.UserRole + 10) or {}
            be_name = be_run.get("r_name", be_item.text(0))
            if be_run.get("stages"):
                for st in be_run.get("stages", []):
                    info = self._stage_info_for_timeline(be_run, st)
                    ev = {
                        "name": be_name + " / " + st.get("name", "-"),
                        "kind": "STAGE",
                        "start": info.get("start", "-"),
                        "end": info.get("end", "-"),
                        "runtime": info.get("runtime", "-"),
                    }
                    if self._valid_timeline_event(ev):
                        events.append(ev)

        def _key(ev):
            return self._parse_dashboard_time(ev.get("start")) or datetime.datetime.max
        return sorted(events, key=_key)

    def show_timeline_overview(self, item):
        events = self._timeline_events_for_item(item)
        if not events:
            QMessageBox.information(
                self, "Timeline",
                "No timed FE/stage rows found for this run. "
                "Rows without runtime/start/end are hidden from the timeline.")
            return
        dlg = QDialog(self)
        dlg.setWindowTitle("Timeline Overview: " + item.text(0))
        try:
            avail = QApplication.desktop().availableGeometry(self)
            dlg.resize(min(1100, int(avail.width() * 0.90)),
                       min(720, int(avail.height() * 0.88)))
        except Exception:
            dlg.resize(1000, 620)
        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(10, 10, 10, 10)
        first_dt = min(self._parse_dashboard_time(ev.get("start"))
                       for ev in events)
        last_dt = max(self._parse_dashboard_time(ev.get("end"))
                      for ev in events)
        span_txt = self._fmt_gap(first_dt, last_dt)
        summary = QLabel(
            "<b>Timeline:</b> {} timed step(s), total span {}".format(
                len(events), span_txt))
        summary.setStyleSheet("color: #1976d2;")
        layout.addWidget(summary)

        chart = _TimelineChartWidget(events, self._parse_dashboard_time,
                                     self.is_dark_mode)
        chart.setMinimumHeight(chart.preferred_height(max(720, dlg.width() - 40)))
        chart_scroll = QScrollArea()
        chart_scroll.setWidgetResizable(True)
        chart_scroll.setWidget(chart)
        chart_scroll.setMinimumHeight(230)
        chart_scroll.setMaximumHeight(360)
        layout.addWidget(chart_scroll)

        tbl = QTableWidget(0, 6)
        tbl.setHorizontalHeaderLabels(["Step", "Type", "Start", "End", "Runtime", "Gap From Previous"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        for c in range(1, 6):
            tbl.horizontalHeader().setSectionResizeMode(c, QHeaderView.Interactive)
        tbl.setColumnWidth(1, 70)
        tbl.setColumnWidth(2, 150)
        tbl.setColumnWidth(3, 150)
        tbl.setColumnWidth(4, 110)
        tbl.setColumnWidth(5, 120)
        tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        prev_end = None
        for ev in events:
            r = tbl.rowCount(); tbl.insertRow(r)
            start_dt = self._parse_dashboard_time(ev.get("start"))
            gap = self._fmt_gap(prev_end, start_dt)
            vals = [ev.get("name", "-"), ev.get("kind", "-"),
                    ev.get("start", "-"), ev.get("end", "-"),
                    ev.get("runtime", "-"), gap]
            for c, val in enumerate(vals):
                it = QTableWidgetItem(str(val))
                if ev.get("kind") == "FE":
                    it.setBackground(QColor("#e3f2fd"))
                elif ev.get("kind") == "BE":
                    it.setBackground(QColor("#fff3e0"))
                tbl.setItem(r, c, it)
            prev_end = self._parse_dashboard_time(ev.get("end")) or prev_end
        layout.addWidget(tbl)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    def show_selected_timeline_overview(self):
        items = self.tree.selectedItems()
        if not items:
            QMessageBox.information(
                self, "Timeline Overview",
                "Select a FE run, BE run, or stage row first.")
            return
        self.show_timeline_overview(items[0])

    def _checked_run_items(self):
        items = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                nt = c.data(0, Qt.UserRole)
                if (c.checkState(0) == Qt.Checked
                        and nt not in ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                                       "__PLACEHOLDER__")):
                    items.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())
        return items

    def _metric_task_from_item(self, item):
        nt = item.data(0, Qt.UserRole)
        if nt == "STAGE":
            parent = item.parent()
            if not parent:
                return None
            run = parent.data(0, Qt.UserRole + 10) or {}
            return {"name": parent.text(0) + " / " + item.text(0),
                    "path": parent.text(15), "run_type": "BE",
                    "stage_name": item.text(0),
                    "source": parent.text(2), "block": parent.data(0, Qt.UserRole + 2) or ""}
        run = item.data(0, Qt.UserRole + 10) or {}
        if run.get("run_type") != "FE":
            return None
        return {"name": item.text(0), "path": item.text(15), "run_type": "FE",
                "stage_name": None, "source": item.text(2),
                "block": item.data(0, Qt.UserRole + 2) or ""}

    def _metric_value(self, metrics, key):
        area = metrics.get("area", {}) or {}
        cong = metrics.get("congestion", {}) or {}
        power = metrics.get("power", {}) or {}
        util = metrics.get("util", {}) or {}
        flat = {
            "r2r_setup": metrics.get("r2r_setup", "-"),
            "r2r_hold": metrics.get("r2r_hold", "-"),
            "total_area": area.get("total_area", "-"),
            "instance_count": area.get("instance_count", "-"),
            "std_cell_area": area.get("std_cell_area", "-"),
            "memory_area": area.get("memory_area", "-"),
            "macro_area": area.get("macro_area", "-"),
            "std_util": util.get("std_util_str", metrics.get("std_util_str", "-")),
            "mbit": metrics.get("mbit", "-"),
            "cgc": metrics.get("cgc", "-"),
            "congestion": cong.get("cong_both", "-"),
            "leakage": power.get("leakage", "-"),
            "runtime": metrics.get("runtime", "-"),
            "logic_depth": metrics.get("logic_depth", "-"),
        }
        return flat.get(key, "-")

    def _num(self, val):
        s = str(val or "")
        # Runtime strings: 00d:05h:12m:39s, 05h:12m:39s, etc.
        if re.search(r'[dhms:]', s):
            rt = re.search(
                r'(?:(\d+)\s*d[: ]*)?(?:(\d+)\s*h[: ]*)?(?:(\d+)\s*m[: ]*)?(?:(\d+)\s*s)?',
                s)
            if rt and any(rt.groups()):
                d = float(rt.group(1) or 0)
                h = float(rt.group(2) or 0)
                m = float(rt.group(3) or 0)
                sec = float(rt.group(4) or 0)
                return d * 24.0 + h + (m / 60.0) + (sec / 3600.0)
        m = re.search(r'-?\d+(?:\.\d+)?', str(val or ""))
        return float(m.group(0)) if m else None

    def _show_metric_diff_dialog(self, title, rows, baseline_name=None):
        fields = [
            ("R2R Setup WNS/TNS/FEPs", "r2r_setup"),
            ("R2R Hold WNS/TNS/FEPs", "r2r_hold"),
            ("Total Area", "total_area"),
            ("Instance Count", "instance_count"),
            ("Std Cell Area", "std_cell_area"),
            ("Memory Area", "memory_area"),
            ("Macro Area", "macro_area"),
            ("Std Util", "std_util"),
            ("MBIT Ratio", "mbit"),
            ("CGC Ratio", "cgc"),
            ("Congestion", "congestion"),
            ("Leakage", "leakage"),
            ("Runtime", "runtime"),
            ("Logic Depth", "logic_depth"),
        ]
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        dlg.resize(1100, 650)
        layout = QVBoxLayout(dlg)
        names = [r.get("name", "-") for r in rows]
        if baseline_name:
            layout.addWidget(QLabel("<b>Baseline:</b> " + baseline_name))
        layout.addWidget(QLabel("<b>Runs:</b> " + "  |  ".join(names)))
        tbl = QTableWidget(0, 5 if len(rows) == 2 else len(rows) + 2)
        if len(rows) == 2:
            tbl.setHorizontalHeaderLabels(["Metric", names[0], names[1], "Delta", "Delta %"])
        else:
            tbl.setHorizontalHeaderLabels(["Metric"] + names + ["Worst Delta %"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        for c in range(1, tbl.columnCount()):
            tbl.horizontalHeader().setSectionResizeMode(c, QHeaderView.Stretch)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        chart = _BarChartWidget(
            "Metric Delta % (comparison vs baseline)")
        chart.setMinimumHeight(210)
        chart_labels = []
        chart_values = []
        chart_colors = []
        for label, key in fields:
            r = tbl.rowCount(); tbl.insertRow(r)
            tbl.setItem(r, 0, QTableWidgetItem(label))
            vals = [self._metric_value(row.get("metrics", {}), key) for row in rows]
            nums = [self._num(v) for v in vals]
            for c, val in enumerate(vals):
                tbl.setItem(r, c + 1, QTableWidgetItem(str(val)))
            if len(rows) == 2:
                delta_txt = pct_txt = "-"
                if nums[0] is not None and nums[1] is not None:
                    d = nums[1] - nums[0]
                    delta_txt = "{:+.4g}".format(d)
                    if nums[0] != 0:
                        pct_val = (d / abs(nums[0])) * 100.0
                        pct_txt = "{:+.2f}%".format(pct_val)
                        chart_labels.append(label.split()[0])
                        chart_values.append(pct_val)
                        chart_colors.append(
                            QColor("#ef5350") if pct_val < 0 else QColor("#66bb6a"))
                tbl.setItem(r, 3, QTableWidgetItem(delta_txt))
                tbl.setItem(r, 4, QTableWidgetItem(pct_txt))
            else:
                base = nums[0]
                worst = "-"
                if base not in (None, 0):
                    pcts = [((n - base) / abs(base)) * 100.0
                            for n in nums[1:] if n is not None]
                    if pcts:
                        pct_val = max(pcts, key=lambda x: abs(x))
                        worst = "{:+.2f}%".format(pct_val)
                        chart_labels.append(label.split()[0])
                        chart_values.append(pct_val)
                        chart_colors.append(
                            QColor("#ef5350") if pct_val < 0 else QColor("#66bb6a"))
                tbl.setItem(r, tbl.columnCount() - 1, QTableWidgetItem(worst))
        chart.set_data(chart_labels, chart_values,
                       colors=chart_colors, is_dark=self.is_dark_mode)
        layout.addWidget(chart)
        layout.addWidget(tbl)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    def show_ror_metric_diff(self):
        tasks = []
        for item in self._checked_run_items():
            task = self._metric_task_from_item(item)
            if task:
                tasks.append(task)
        if len(tasks) != 2:
            QMessageBox.information(
                self, "RoR Metric Diff",
                "Check exactly two FE runs or two stage rows for metric diff.")
            return
        self.status_bar.showMessage("Extracting metrics for RoR diff...")
        self._metric_batch_worker = MetricBatchWorker(tasks)
        self._metric_batch_worker.finished.connect(self._on_ror_metric_done)
        self._metric_batch_worker.start()

    def _on_ror_metric_done(self, rows):
        self.status_bar.showMessage("RoR metric diff ready", 3000)
        self._show_metric_diff_dialog("RoR Metric Diff", rows)

    def _find_golden_item_for(self, target_item):
        target_run = target_item.data(0, Qt.UserRole + 10) or {}
        target_block = target_run.get("block") or target_item.data(0, Qt.UserRole + 2)
        golden_path = None
        for path, pin in self.user_pins.items():
            if pin == "golden":
                golden_path = path
                break
        found = [None]
        def walk(node):
            for i in range(node.childCount()):
                c = node.child(i)
                run = c.data(0, Qt.UserRole + 10)
                if run and self.user_pins.get(c.text(15)) == "golden":
                    if not found[0] or run.get("block") == target_block:
                        found[0] = c
                walk(c)
        walk(self.tree.invisibleRootItem())
        return found[0]

    def show_golden_benchmark(self):
        checked = [i for i in self._checked_run_items()
                   if self._metric_task_from_item(i)]
        if not checked:
            QMessageBox.information(
                self, "Golden Benchmark",
                "Check one or more FE runs or stage rows to compare against the Golden Run.")
            return
        golden = self._find_golden_item_for(checked[0])
        if not golden:
            QMessageBox.information(
                self, "Golden Benchmark",
                "No Golden Run is pinned in the current tree. Right-click a baseline run and choose Pin as... > Golden Run.")
            return
        tasks = []
        gtask = self._metric_task_from_item(golden)
        if not gtask:
            QMessageBox.information(
                self, "Golden Benchmark",
                "The pinned Golden Run is not a supported metric target. Use a FE run or stage row.")
            return
        gtask["name"] = "GOLDEN: " + gtask["name"]
        tasks.append(gtask)
        for item in checked:
            if item.text(15) == golden.text(15):
                continue
            task = self._metric_task_from_item(item)
            if task:
                tasks.append(task)
        if len(tasks) < 2:
            QMessageBox.information(
                self, "Golden Benchmark",
                "Select at least one non-golden run to compare.")
            return
        self.status_bar.showMessage("Extracting metrics for golden benchmark...")
        self._metric_batch_worker = MetricBatchWorker(tasks)
        self._metric_batch_worker.finished.connect(self._on_golden_metric_done)
        self._metric_batch_worker.start()

    def _on_golden_metric_done(self, rows):
        self.status_bar.showMessage("Golden benchmark ready", 3000)
        self._show_metric_diff_dialog(
            "Golden Benchmark", rows, baseline_name=rows[0].get("name", "Golden"))

    def show_run_diff(self):
        """Compare N checked runs side-by-side."""
        checked = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (c.checkState(0) == Qt.Checked
                        and c.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                         "STAGE","__PLACEHOLDER__")):
                    checked.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())

        if len(checked) < 2:
            QMessageBox.information(
                self, "Compare Runs",
                "Please check 2 or more runs using the checkboxes,\n"
                "then click Compare Runs.")
            return

        fields = [
            ("Run Name",    0), ("RTL Release", 1), ("Source",  2),
            ("Status",      3), ("Stage",        4), ("User",    5),
            ("Size",        6), ("FM NONUPF",    7), ("FM UPF",  8),
            ("VSLP",        9), ("Static IR",   10), ("Dynamic IR", 11),
            ("Runtime",    12), ("Start",       13), ("End",    14),
        ]
        n = len(checked)

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Run Comparison  ({n} runs selected)")
        dlg.resize(min(300 + n * 200, 1400), 520)
        layout = QVBoxLayout(dlg)
        run_names = [item.text(0) for item in checked]
        layout.addWidget(QLabel(
            "<b>Comparing:</b>  " + "   |   ".join(run_names)))

        tbl = QTableWidget(len(fields), n + 1)
        headers = ["Field"] + run_names
        tbl.setHorizontalHeaderLabels(headers)
        tbl.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        for i in range(1, n + 1):
            tbl.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.Stretch)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)

        amber    = QColor("#fff3e0")
        red_bg   = QColor("#ffebee")
        green_bg = QColor("#e8f5e9")

        for row, (label, col) in enumerate(fields):
            lbl_item = QTableWidgetItem(label)
            lbl_item.setFont(QFont("", -1, QFont.Bold))
            tbl.setItem(row, 0, lbl_item)
            vals     = [item.text(col) for item in checked]
            all_same = len(set(vals)) == 1
            for c_idx, (item, val) in enumerate(zip(checked, vals)):
                cell = QTableWidgetItem(val)
                cell.setTextAlignment(Qt.AlignCenter)
                if not all_same:
                    if col in (3, 7, 8, 9):
                        v_up = val.upper()
                        if "FAIL" in v_up or "ERROR" in v_up:
                            cell.setBackground(red_bg)
                        elif "PASS" in v_up or "COMPLETED" in v_up:
                            cell.setBackground(green_bg)
                        else:
                            cell.setBackground(amber)
                    else:
                        cell.setBackground(amber)
                tbl.setItem(row, c_idx + 1, cell)

        layout.addWidget(tbl)
        n_diff = sum(
            1 for _, col in fields
            if len(set(item.text(col) for item in checked)) > 1)
        summary = QLabel(
            f"<small>{n_diff} of {len(fields)} fields differ across "
            f"{n} selected runs.  "
            "Amber = any difference.  "
            "Red = fail/error.  Green = pass/completed.</small>")
        summary.setStyleSheet("color: gray;")
        layout.addWidget(summary)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.accept)
        layout.addWidget(close_btn)
        dlg.exec_()

    # ------------------------------------------------------------------
    # UTILITIES
    # ------------------------------------------------------------------
    def _time_to_seconds(self, time_str):
        try:
            m = re.match(r'(\d+)h:(\d+)m:(\d+)s', time_str or "")
            if m:
                return (int(m.group(1)) * 3600
                        + int(m.group(2)) * 60
                        + int(m.group(3)))
        except Exception:
            pass
        return 0

    def fit_all_columns(self):
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i):
                self.tree.resizeColumnToContents(i)
        self._fit_run_name_column()


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Singularity PD")
    window = PDDashboard()
    window.showMaximized()
    sys.exit(app.exec_())
