# -*- coding: utf-8 -*-
# Flow Pulse | Beta 1 -- main.py
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
import gzip
import html
import io

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QTreeWidget, QTreeWidgetItem,
    QPushButton, QMessageBox, QListWidget, QListWidgetItem,
    QProgressBar, QMenu, QSplitter, QWidgetAction, QCheckBox,
    QStatusBar, QFrame, QShortcut, QToolButton, QStyle,
    QHeaderView, QFileDialog, QGroupBox, QTextEdit, QDockWidget,
    QFormLayout, QDialog, QDialogButtonBox, QFontComboBox,
    QSpinBox, QDoubleSpinBox, QAbstractSpinBox, QColorDialog, QTabWidget, QTableWidget,
    QTableWidgetItem, QScrollArea, QAbstractItemView, QSizePolicy,
    QToolTip
)
from PyQt5.QtCore import Qt, QTimer, QDateTime, pyqtSignal, QThread, QDate, QPoint, QRect
from PyQt5.QtWidgets import QDateEdit as _QDateEditImport
from PyQt5.QtGui import (QColor, QFont, QKeySequence, QBrush,
                         QPainter, QPen, QPixmap, QIcon, QPolygon, QImage)

# ===========================================================================
# CONFIG + MAIL HELPERS (module-level, loaded once at startup)
# ===========================================================================


_ATOMIC_WRITE_LOCKS = {}
_ATOMIC_WRITE_LOCKS_GUARD = threading.Lock()


def _atomic_lock_for(path):
    key = os.path.abspath(path)
    with _ATOMIC_WRITE_LOCKS_GUARD:
        lock = _ATOMIC_WRITE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _ATOMIC_WRITE_LOCKS[key] = lock
        return lock


def _atomic_replace_path(path, writer_func):
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except Exception:
            pass
    tmp = path + ".tmp.{}.{}".format(os.getpid(), int(time.time() * 1000000))
    lock = _atomic_lock_for(path)
    with lock:
        try:
            writer_func(tmp)
            os.replace(tmp, path)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass


def _atomic_write_text(path, text, encoding="utf-8"):
    def _write(tmp):
        with open(tmp, "w", encoding=encoding) as f:
            f.write(text)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
    _atomic_replace_path(path, _write)


def _atomic_write_json(path, data, indent=None, sort_keys=False):
    def _write(tmp):
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, sort_keys=sort_keys)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
    _atomic_replace_path(path, _write)


def _atomic_write_gzip_json(path, data, indent=None, sort_keys=False):
    def _write(tmp):
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, sort_keys=sort_keys)
    _atomic_replace_path(path, _write)


def _write_config_atomic(config, path):
    buf = io.StringIO()
    config.write(buf)
    _atomic_write_text(path, buf.getvalue())

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
        'SCAN_IGNORE': {
            'FE_RUN_PATTERNS': '',
            'BE_RUN_PATTERNS': '',
            'PNR_STAGE_PATTERNS': 'backup_*',
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
        changed = False
        for sec, vals in defaults.items():
            if not cfg.has_section(sec):
                cfg.add_section(sec)
                changed = True
            for key, val in vals.items():
                if not cfg.has_option(sec, key):
                    cfg.set(sec, key, val)
                    changed = True
        if changed:
            try:
                _write_config_atomic(cfg, cfg_file)
            except Exception:
                pass
    else:
        cfg.read_dict(defaults)
        try:
            _write_config_atomic(cfg, cfg_file)
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
            _write_config_atomic(mc, mc_file)
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
NOTES_DIR        = os.path.join(SCRIPT_DIR, 'dashboard_notes')
SNAPSHOT_DIR     = os.path.join(NOTES_DIR, 'snapshots')
def _safe_user_name():
    try:
        name = getpass.getuser()
    except Exception:
        name = "user"
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', name or "user")

OLD_USER_PREFS_FILE = os.path.join(SCRIPT_DIR, 'user_prefs.ini')
USER_PREFS_FILE  = os.path.join(
    NOTES_DIR, 'user_prefs_{}.ini'.format(_safe_user_name()))
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
IGNORE_FE_RUN_PATTERNS = _proj_cfg.get(
    'SCAN_IGNORE', 'FE_RUN_PATTERNS', fallback='')
IGNORE_BE_RUN_PATTERNS = _proj_cfg.get(
    'SCAN_IGNORE', 'BE_RUN_PATTERNS', fallback='')
IGNORE_PNR_STAGE_PATTERNS = _proj_cfg.get(
    'SCAN_IGNORE', 'PNR_STAGE_PATTERNS', fallback='backup_*')
_blocks_raw      = _proj_cfg.get('PROJECT', 'BLOCKS',           fallback='')
BLOCKS           = set(b.strip() for b in _blocks_raw.split(',') if b.strip())

# Ensure notes dir exists
if not os.path.exists(NOTES_DIR):
    try:
        os.makedirs(NOTES_DIR)
    except Exception:
        pass
if not os.path.exists(SNAPSHOT_DIR):
    try:
        os.makedirs(SNAPSHOT_DIR)
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
_bt.IGNORE_FE_RUN_PATTERNS = IGNORE_FE_RUN_PATTERNS
_bt.IGNORE_BE_RUN_PATTERNS = IGNORE_BE_RUN_PATTERNS
_bt.IGNORE_PNR_STAGE_PATTERNS = IGNORE_PNR_STAGE_PATTERNS


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
        _write_config_atomic(mail_config, _MAIL_USERS_FILE)
    except Exception:
        pass


def _save_mail_users_async(new_users):
    try:
        users = list(new_users or [])
        t = threading.Thread(target=_save_mail_users, args=(users,))
        t.daemon = False
        t.start()
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
            with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_user_pins(pins_dict):
    try:
        _atomic_write_json(_get_pins_file(), pins_dict, indent=4)
    except Exception:
        pass

# Load per-user preferences. Migrate the old shared prefs once per user.
if (not os.path.exists(USER_PREFS_FILE)) and os.path.exists(OLD_USER_PREFS_FILE):
    try:
        shutil.copy2(OLD_USER_PREFS_FILE, USER_PREFS_FILE)
    except Exception:
        pass
prefs = configparser.ConfigParser()
if os.path.exists(USER_PREFS_FILE):
    prefs.read(USER_PREFS_FILE)


def _ensure_notes_dir():
    if not os.path.exists(NOTES_DIR):
        try:
            os.makedirs(NOTES_DIR)
        except Exception:
            pass

def _get_personal_notes_file():
    _ensure_notes_dir()
    return os.path.join(NOTES_DIR, "personal_notes_{}.json".format(_getpass.getuser()))

def _get_shared_notes_file():
    _ensure_notes_dir()
    return os.path.join(NOTES_DIR, "shared_notes.json")

def _get_notes_file():
    # Backward-compatible alias. Personal notes are per user.
    return _get_personal_notes_file()

def _read_json_dict(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def load_personal_notes():
    data = _read_json_dict(_get_personal_notes_file())
    out = {}
    for key, val in data.items():
        if isinstance(val, dict):
            txt = val.get("text", "")
        elif isinstance(val, list):
            txt = "\n".join(str(x) for x in val)
        else:
            txt = str(val)
        if txt.strip():
            out[key] = txt.strip()
    return out

def save_personal_note(identifier, note_text):
    if not identifier:
        return False
    notes = load_personal_notes()
    text = (note_text or '').strip()
    if text:
        notes[identifier] = text
    else:
        notes.pop(identifier, None)
    try:
        _atomic_write_json(_get_personal_notes_file(), notes, indent=4, sort_keys=True)
        return True
    except Exception:
        return False

def _format_shared_entry(entry):
    if not isinstance(entry, dict):
        return str(entry)
    ts = entry.get("updated_at", "")
    user = entry.get("user", "unknown")
    text = entry.get("text", "")
    if isinstance(text, (list, tuple)):
        text = "; ".join(_note_lines(text))
    return "{}  {}: {}".format(ts, user, text).strip()

def _note_lines(value):
    lines = []
    def _walk(v):
        if isinstance(v, dict):
            s = _format_shared_entry(v).strip()
            if s:
                lines.append(s)
        elif isinstance(v, (list, tuple)):
            for x in v:
                _walk(x)
        elif v is not None:
            s = str(v).strip()
            if s:
                lines.append(s)
    _walk(value)
    return lines

def load_shared_note_entries():
    data = _read_json_dict(_get_shared_notes_file())
    out = {}
    for key, val in data.items():
        entries = []
        if isinstance(val, list):
            for entry in val:
                if isinstance(entry, dict):
                    if str(entry.get("text", "")).strip():
                        entries.append(entry)
                elif str(entry).strip():
                    entries.append({
                        "user": "unknown",
                        "text": str(entry).strip(),
                        "updated_at": "",
                    })
        elif isinstance(val, dict):
            txt = str(val.get("text", "")).strip()
            if txt:
                entries.append({
                    "user": val.get("updated_by", val.get("user", "unknown")),
                    "text": txt,
                    "updated_at": val.get("updated_at", ""),
                })
        elif str(val).strip():
            entries.append({"user": "unknown", "text": str(val).strip(), "updated_at": ""})
        entries.sort(key=lambda e: e.get("updated_at", ""))
        if entries:
            out[key] = entries
    return out

def load_all_notes():
    # Shared notes, formatted for display/search compatibility.
    entries = load_shared_note_entries()
    return dict((key, _note_lines([_format_shared_entry(e) for e in vals]))
                for key, vals in entries.items())

def save_shared_note(identifier, note_text):
    if not identifier:
        return False
    text = (note_text or '').strip()
    if not text:
        return False
    data = load_shared_note_entries()
    data.setdefault(identifier, []).append({
        "user": _getpass.getuser(),
        "text": text,
        "updated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })
    try:
        _atomic_write_json(_get_shared_notes_file(), data, indent=4, sort_keys=True)
        return True
    except Exception:
        return False

def save_user_note(identifier, note_text):
    # Backward-compatible name used by older code paths: personal only.
    return save_personal_note(identifier, note_text)
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
    _save_mail_users_async(to_raw + cc_raw + to_list + cc_list)
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
        vt_label = vth.get("stage_vt_label", "LVT*/RVT*/HVT*")
        lvt_rvt_hvt_inst = vth.get("stage_vt_inst",
                                   vth.get("lvt_rvt_hvt_inst", vth.get("lvt_rvt_inst", "-/-")))
        lvt_rvt_hvt_area = vth.get("stage_vt_area",
                                   vth.get("lvt_rvt_hvt_area", vth.get("lvt_rvt_area", "-/-")))

        # Congestion: cong_both already formatted as "Both%/V%/H%"
        cong_str = _v(cong, "cong_both")

        # Power: key is "leakage", value includes unit e.g. "87.468 uW"
        pwr_str = _v(power, "leakage")

        # Util string: from utilization report
        util_str = _v(metrics.get("util", {}), "std_util_str", "std_util")
        gate_count = metrics.get("gate_count", "-")
        if gate_count == "-":
            try:
                parent_obj = self.parent()
                factor = getattr(parent_obj, "gate_count_unit_area", 0.2419) or 0.2419
                gate_count = str(int(float(area.get("std_cell_area", "-")) / factor))
            except Exception:
                gate_count = "-"

        # (label, value, is_section, path_key_or_None)
        _paths = metrics.get("_paths", {})
        if metrics.get("stage"):
            rows = [
                ("Timing",                         None,                         True,  None),
                ("R2R Setup WNS/TNS/NVE",          metrics.get("setup_r2r", metrics.get("r2r_setup", "-")), False, "r2r_setup"),
                ("Total Setup WNS/TNS/NVE",        metrics.get("setup_total", "-"), False, "r2r_setup"),
                ("R2R Hold WNS/TNS/NVE",           metrics.get("hold_r2r", metrics.get("r2r_hold", "-")), False, "r2r_hold"),
                ("Total Hold WNS/TNS/NVE",         metrics.get("hold_total", metrics.get("hold_all", "-")), False, "r2r_hold"),
                ("Physical",                       None,                         True,  None),
                ("Congestion",                     cong_str,                     False, "congestion"),
                ("Std Cell Count/Area",            metrics.get("std_cell_count_area", "-"), False, "std_cell_area"),
                ("Gate Count",                     gate_count, False, "std_cell_area"),
                ("StdCell/StdCell Only Util",      util_str,                     False, "std_cell_area"),
                ("Total Util",                     metrics.get("total_util", "-"), False, "std_cell_area"),
                (vt_label + " Inst",               lvt_rvt_hvt_inst,             False, "vth"),
                (vt_label + " Area",               lvt_rvt_hvt_area,             False, "vth"),
                ("Clock",                          None,                         True,  None),
                ("Skew/Latency",                   metrics.get("skew_latency", "-"), False, "skew_latency"),
                ("Clock Repeater Count/Area",      metrics.get("clock_repeater_count_area", "-"), False, "skew_latency"),
                ("Runtime",                        metrics.get("runtime","-"),  False, "runtime"),
            ]
        else:
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

        # Double-click value -> open report in gvim
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


class FeCongestionLookupWorker(QThread):
    finished = pyqtSignal(int, str, str, str, str, object)

    def __init__(self, token, run_path, block):
        super().__init__()
        self.token = token
        self.run_path = run_path or ""
        self.block = block or ""

    def run(self):
        fp_ver = "-"
        img_path = ""
        img = QImage()
        try:
            log_path = os.path.join(self.run_path, "logs", "compile_opt.log")
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        m = re.search(r'^\s*INFO\s*:\s*FP_VER\s*[-:]\s*(\S.*)$', line)
                        if not m:
                            continue
                        cand = m.group(1).strip().strip('"')
                        if '$' in cand or cand.upper() in ('FP_VER', '$FP_VER'):
                            continue
                        fp_ver = cand
                        break
        except Exception:
            fp_ver = "-"

        try:
            rpt_dir = os.path.join(self.run_path, "reports")
            if os.path.isdir(rpt_dir):
                pats = []
                if self.block:
                    pats.append("congestion.window.{}.*.jpg".format(self.block))
                    pats.append("congestion.window.{}.*.jpeg".format(self.block))
                pats.extend(["congestion.window.*.jpg", "congestion.window.*.jpeg"])
                matches = []
                for name in os.listdir(rpt_dir):
                    for pat in pats:
                        if fnmatch.fnmatch(name, pat):
                            matches.append(os.path.join(rpt_dir, name))
                            break
                if matches:
                    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                    img_path = matches[0]
                    try:
                        with open(img_path, 'rb') as f:
                            img.loadFromData(f.read())
                    except Exception:
                        img = QImage()
        except Exception:
            img_path = ""
            img = QImage()

        self.finished.emit(self.token, self.run_path, self.block, fp_ver, img_path, img)


class StageScreenshotLookupWorker(QThread):
    finished = pyqtSignal(int, str, str, str, object, object)

    def __init__(self, token, be_path, stage_path, stage_name, block):
        super().__init__()
        self.token = token
        self.be_path = be_path or ""
        self.stage_path = stage_path or ""
        self.stage_name = stage_name or ""
        self.block = block or ""

    def _candidate_dirs(self):
        out = []
        for d in (
                os.path.join(self.stage_path, "screenshot"),
                os.path.join(self.be_path, self.stage_name, "screenshot"),
                os.path.join(self.be_path, "screenshot"),
                os.path.join(self.be_path, "outputs", self.stage_name, "screenshot"),
                os.path.join(self.be_path, "reports", self.stage_name, "screenshot")):
            if d and d not in out:
                out.append(d)
        return out

    def _match(self, names, patterns):
        for pat in patterns:
            for name in names:
                if fnmatch.fnmatch(name.lower(), pat.lower()):
                    return name
        return ""

    def run(self):
        labels = [
            ("Congestion Map", [
                "{}.{}.jpg".format(self.stage_name, self.block),
                "{}.{}.jpeg".format(self.stage_name, self.block)]),
            ("Pin Map", [
                "{}.{}_pin_density.*".format(self.stage_name, self.block),
                "{}.*_pin_density.*".format(self.stage_name)]),
            ("Cell Density Map", [
                "{}.{}_cell_density.*".format(self.stage_name, self.block),
                "{}.*_cell_density.*".format(self.stage_name)]),
            ("Shorts Map", [
                "{}.{}_Short*.jpg".format(self.stage_name, self.block),
                "{}.*_Short*.jpg".format(self.stage_name),
                "{}.{}_Short*.jpeg".format(self.stage_name, self.block),
                "{}.*_Short*.jpeg".format(self.stage_name)]),
        ]
        found = {}
        img = QImage()
        try:
            for d in self._candidate_dirs():
                if not os.path.isdir(d):
                    continue
                names = [n for n in os.listdir(d)
                         if n.lower().endswith((".jpg", ".jpeg"))]
                if not names:
                    continue
                for label, pats in labels:
                    if label in found:
                        continue
                    hit = self._match(names, pats)
                    if hit:
                        found[label] = os.path.join(d, hit)
                if "Congestion Map" in found and img.isNull():
                    try:
                        with open(found["Congestion Map"], 'rb') as f:
                            img.loadFromData(f.read())
                    except Exception:
                        img = QImage()
                if len(found) == len(labels):
                    break
        except Exception:
            found = {}
            img = QImage()
        self.finished.emit(
            self.token, self.be_path, self.stage_name, self.block, found, img)


class StageMetricLookupWorker(QThread):
    finished = pyqtSignal(int, str, str, object)

    def __init__(self, token, be_path, stage_path, stage_name, block, runtime, gate_factor):
        super().__init__()
        self.token = token
        self.be_path = be_path or ""
        self.stage_path = stage_path or ""
        self.stage_name = stage_name or ""
        self.block = block or ""
        self.runtime = runtime or "-"
        self.gate_factor = gate_factor or 0.2419

    def _candidate_report_dirs(self):
        dirs = [
            os.path.join(self.be_path, "reports", self.stage_name),
            os.path.join(self.stage_path, "reports"),
            os.path.join(self.stage_path, "reports", self.stage_name),
            os.path.join(self.be_path, self.stage_name, "reports"),
            os.path.join(self.be_path, self.stage_name, "reports", self.stage_name),
            os.path.join(self.be_path, "reports"),
            self.stage_path,
        ]
        out = []
        for d in dirs:
            if d and d not in out:
                out.append(d)
        return out

    def _find_file(self, patterns):
        hits = []
        for d in self._candidate_report_dirs():
            try:
                if not os.path.isdir(d):
                    continue
                names = os.listdir(d)
            except Exception:
                continue
            for pat in patterns:
                for name in names:
                    if fnmatch.fnmatch(name, pat):
                        hits.append(os.path.join(d, name))
                if hits:
                    break
            if hits:
                break
        if not hits:
            return ""
        try:
            return sorted(hits, key=os.path.getmtime)[-1]
        except Exception:
            return sorted(hits)[-1]

    def _read_text(self, path):
        if not path or not os.path.exists(path):
            return ""
        try:
            if path.endswith(".gz"):
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                    return f.read()
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            return ""

    def _trip(self, a, b, c):
        return "{}/{}/{}".format(a, b, c)

    def _fc_timing_section(self, text, label):
        m = re.search(label + r".*?(?=\n\s*(?:Setup violations|Hold violations|END_CMD|Report :|$))",
                      text, re.S | re.I)
        return m.group(0) if m else ""

    def _parse_fc_timing(self, text, label):
        low = text.lower()
        if label.lower().startswith("setup") and "no setup violations found" in low:
            return ("0/0/0", "0/0/0")
        if label.lower().startswith("hold") and "no hold violations found" in low:
            return ("0/0/0", "0/0/0")
        sec = self._fc_timing_section(text, label)
        if not sec:
            return ("-", "-")
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
        total = self._trip(wv[0], tv[0], nv[0])
        r2r = self._trip(wv[1], tv[1], nv[1])
        return (total, r2r)

    def _pipe_cells(self, line):
        return [c.strip() for c in line.strip().strip("|").split("|")]

    def _parse_innovus_setup(self, text):
        lines = text.splitlines()
        header = None
        rows = {}
        for line in lines:
            if "|" not in line:
                continue
            cells = self._pipe_cells(line)
            if not cells:
                continue
            if cells[0].lower().startswith("setup mode"):
                header = [c.lower() for c in cells]
                continue
            if header and cells[0].lower().startswith("wns"):
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
            total = self._trip(rows["wns"][all_i], rows["tns"][all_i], rows["num"][all_i])
            r2r = self._trip(rows["wns"][r2r_i], rows["tns"][r2r_i], rows["num"][r2r_i])
            return (total, r2r)
        except Exception:
            return ("-", "-")

    def _parse_innovus_hold(self, text):
        m = re.search(r"#\s*HOLD.*?View\s*:\s*ALL\s+([-\d.]+)\s+([-\d.]+)\s+(\d+)",
                      text, re.S | re.I)
        if m:
            return self._trip(m.group(1), m.group(2), m.group(3))
        return "-"

    def _parse_congestion(self, text):
        m = re.search(
            r"Overflow:\s*\S+\s*=\s*\S+\s*\(([^)]*H)\)\s*\+\s*\S+\s*\(([^)]*V)\)",
            text, re.I)
        if m:
            return "{} + {}".format(m.group(1).strip(), m.group(2).strip())
        h = re.search(r"H\s+routing.*?\(\s*([\d.]+%)\s*\)", text, re.I)
        v = re.search(r"V\s+routing.*?\(\s*([\d.]+%)\s*\)", text, re.I)
        if h and v:
            return "{} H + {} V".format(h.group(1), v.group(1))
        both = re.search(r"Both\s+Dirs.*?\(\s*([\d.]+%)\s*\)", text, re.I)
        if both:
            return both.group(1)
        return "-"

    def _parse_area(self, text):
        out = {}
        m = re.search(r"^\s*std_cell\(\+headbuf\+epbuf\)\s+(\d+)\s+([\d.]+)",
                      text, re.M)
        if m:
            out["std_cell_count"] = m.group(1)
            out["std_cell_area"] = m.group(2)
            out["std_cell_count_area"] = "{}/{}".format(m.group(1), m.group(2))
            try:
                out["gate_count"] = str(int(float(m.group(2)) / self.gate_factor))
            except Exception:
                out["gate_count"] = "-"
        m = re.search(r"Standard\s+cell\s+only\s+utilization\s*:\s*([\d.]+)%", text, re.I)
        if m:
            out["std_cell_only_util"] = m.group(1) + "%"
        m = re.search(r"^\s*Total\s+utilization\s*:\s*([\d.]+)%", text, re.I | re.M)
        if m:
            out["total_util"] = m.group(1) + "%"
        return out

    def _parse_vth(self, text):
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
            group = r.group(1).split("_", 1)[0].upper()
            vals = groups.setdefault(group, [0.0, 0.0])
            vals[0] += float(r.group(2))
            vals[1] += float(r.group(3))
        if not groups:
            return {}
        preferred = ["UHVT", "HVT", "RVT", "LVT", "SLVT"]
        order = [g for g in preferred if g in groups]
        order.extend(sorted(g for g in groups if g not in order))
        labels = "/".join(g + "*" for g in order)
        inst = "/".join("{:.2f}%".format(groups[g][0]) for g in order)
        area = "/".join("{:.2f}%".format(groups[g][1]) for g in order)
        return {"vt_label": labels, "vt_inst": inst, "vt_area": area}

    def _parse_cts(self, text):
        for line in text.splitlines():
            if not re.match(r"^\s*All\s+Clocks\b", line):
                continue
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?", line)
            if len(nums) >= 7:
                return {
                    "skew_latency": "{}/{}".format(nums[6], nums[5]),
                    "clock_repeater_count_area": "{}/{}".format(nums[2], nums[3]),
                }
        return {}

    def run(self):
        result = {
            "stage": self.stage_name,
            "runtime": self.runtime or "-",
            "report_dir": "-",
        }
        try:
            qor_sum = self._find_file(["{}.qor_sum.rpt".format(self.stage_name), "*.qor_sum.rpt"])
            if qor_sum:
                text = self._read_text(qor_sum)
                setup_total, setup_r2r = self._parse_fc_timing(text, "Setup violations")
                hold_total, hold_r2r = self._parse_fc_timing(text, "Hold violations")
                result["setup_total"] = setup_total
                result["setup_r2r"] = setup_r2r
                result["hold_total"] = hold_total
                result["hold_r2r"] = hold_r2r
                result["timing_report"] = qor_sum
                result["report_dir"] = os.path.dirname(qor_sum)
            else:
                setup_path = self._find_file([
                    "{}_p*.summary.gz".format(self.stage_name),
                    "{}_p*.summary".format(self.stage_name),
                    "*_p*.summary.gz"])
                if setup_path:
                    setup_total, setup_r2r = self._parse_innovus_setup(self._read_text(setup_path))
                    result["setup_total"] = setup_total
                    result["setup_r2r"] = setup_r2r
                    result["timing_report"] = setup_path
                    result["report_dir"] = os.path.dirname(setup_path)
                hold_path = self._find_file(["{}.qor.snap.rpt".format(self.stage_name), "*.qor.snap.rpt"])
                if hold_path:
                    result["hold_all"] = self._parse_innovus_hold(self._read_text(hold_path))
                    result["hold_report"] = hold_path
                    result["report_dir"] = os.path.dirname(hold_path)

            grc = self._find_file(["{}.grc.rpt".format(self.stage_name), "*.grc.rpt"])
            if grc:
                result["congestion"] = self._parse_congestion(self._read_text(grc))
                result["congestion_report"] = grc
                result["report_dir"] = os.path.dirname(grc)

            area = self._find_file(["{}.sec_get_area.rpt".format(self.stage_name), "*.sec_get_area.rpt"])
            if area:
                result.update(self._parse_area(self._read_text(area)))
                result["area_report"] = area
                result["report_dir"] = os.path.dirname(area)

            vth = self._find_file(["{}.sec_vth_use.rpt".format(self.stage_name), "*.sec_vth_use.rpt"])
            if vth:
                result.update(self._parse_vth(self._read_text(vth)))
                result["vth_report"] = vth
                result["report_dir"] = os.path.dirname(vth)

            cts = self._find_file(["{}.cts.qor.final.rpt".format(self.stage_name), "*.cts.qor.final.rpt"])
            if cts:
                result.update(self._parse_cts(self._read_text(cts)))
                result["cts_report"] = cts
                result["report_dir"] = os.path.dirname(cts)
        except Exception as e:
            result["error"] = str(e)
        self.finished.emit(self.token, self.be_path, self.stage_name, result)

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
    """Interactive signed bar chart using QPainter."""
    bar_clicked = pyqtSignal(int)

    def __init__(self, title="", horizontal=False):
        super().__init__()
        self.title      = title
        self.horizontal = horizontal
        self.labels     = []
        self.values     = []
        self.bar_colors = []
        self.is_dark    = False
        self.y_label    = ""
        self.row_indices = []
        self.tooltips = []
        self.value_format = "{:.3g}"
        self._bar_rects = []
        self.setMouseTracking(True)
        self.setMinimumSize(260, 190)

    def set_data(self, labels, values, colors=None, is_dark=False, y_label="",
                 row_indices=None, tooltips=None, value_format="{:.3g}"):
        self.labels = list(labels or [])
        clean_vals = []
        for v in values or []:
            try:
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    fv = 0.0
            except Exception:
                fv = 0.0
            clean_vals.append(fv)
        self.values = clean_vals
        self.bar_colors = list(colors or [QColor("#42a5f5")] * len(clean_vals))
        self.is_dark = is_dark
        self.y_label = y_label or ""
        self.row_indices = list(row_indices or range(len(clean_vals)))
        self.tooltips = list(tooltips or [])
        self.value_format = value_format or "{:.3g}"
        self._bar_rects = []
        self.update()

    def _fmt_value(self, val):
        try:
            return self.value_format.format(float(val))
        except Exception:
            return str(val)

    def _hit_index(self, pos):
        for rect, row_idx, tip in self._bar_rects:
            if rect.contains(pos):
                return row_idx, tip
        return None, ""

    def mouseMoveEvent(self, event):
        row_idx, tip = self._hit_index(event.pos())
        if tip:
            QToolTip.showText(event.globalPos(), tip, self)
        else:
            QToolTip.hideText()
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event):
        row_idx, tip = self._hit_index(event.pos())
        if row_idx is not None:
            self.bar_clicked.emit(int(row_idx))
            return
        super().mousePressEvent(event)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        grid = QColor("#55585c" if self.is_dark else "#d7dbe0")
        p.fillRect(self.rect(), bg)
        r = self.rect()
        self._bar_rects = []
        if not self.values:
            p.setPen(fg)
            p.drawText(r, Qt.AlignCenter, "No chart data")
            return

        p.setPen(fg)
        p.drawText(r.adjusted(0, 5, 0, 0), Qt.AlignHCenter | Qt.AlignTop, self.title)
        margin_t = 28
        margin_b = 44
        margin_l = 42
        margin_r = 16
        area_w = r.width() - margin_l - margin_r
        area_h = r.height() - margin_t - margin_b
        if area_w <= 4 or area_h <= 4:
            return

        vals = list(self.values)
        vmax = max(vals)
        vmin = min(vals)
        if vmax == vmin:
            if vmax == 0:
                axis_min, axis_max = -1.0, 1.0
            elif vmax > 0:
                axis_min, axis_max = 0.0, vmax * 1.15
            else:
                axis_min, axis_max = vmin * 1.15, 0.0
        else:
            axis_min = min(0.0, vmin)
            axis_max = max(0.0, vmax)
        span = axis_max - axis_min
        if abs(span) < 1e-12:
            span = 1.0

        def _y(value):
            return margin_t + int((axis_max - value) / span * area_h)

        zero_y = _y(0.0)
        p.setPen(QPen(grid, 1))
        p.drawLine(margin_l, margin_t, margin_l, margin_t + area_h)
        p.drawLine(margin_l, zero_y, margin_l + area_w, zero_y)
        # Light reference lines.
        for frac in (0.25, 0.5, 0.75):
            yy = margin_t + int(area_h * frac)
            p.setPen(QPen(grid, 1, Qt.DotLine))
            p.drawLine(margin_l, yy, margin_l + area_w, yy)

        n = max(1, len(vals))
        slot = max(1, area_w // n)
        bar_w = max(4, min(34, slot - 4))
        for i, val in enumerate(vals):
            col = self.bar_colors[i] if i < len(self.bar_colors) else QColor("#42a5f5")
            x = margin_l + i * slot + max(1, (slot - bar_w) // 2)
            y_val = _y(val)
            top = min(y_val, zero_y)
            h = abs(y_val - zero_y)
            if h < 1:
                h = 1
                top = zero_y - 1 if val >= 0 else zero_y
            rect = QRect(x, top, bar_w, h)
            p.setBrush(QBrush(col))
            p.setPen(Qt.NoPen)
            p.drawRect(rect)
            row_idx = self.row_indices[i] if i < len(self.row_indices) else i
            label = self.labels[i] if i < len(self.labels) else ""
            tip = self.tooltips[i] if i < len(self.tooltips) else (
                "{}\n{}: {}".format(label, self.title, self._fmt_value(val)))
            self._bar_rects.append((rect, row_idx, tip))

            p.setPen(fg)
            val_txt = self._fmt_value(val)
            if val >= 0:
                vy = max(margin_t, top - 17)
            else:
                vy = min(margin_t + area_h + 2, top + h + 2)
            p.drawText(x - 18, vy, bar_w + 36, 16, Qt.AlignCenter, val_txt)

            if slot > 28:
                p.setPen(muted)
                short = label
                if len(short) > 9:
                    short = short[:7] + ".."
                p.drawText(x - 18, margin_t + area_h + 4, bar_w + 36, 34,
                           Qt.AlignHCenter | Qt.AlignTop, short)

        p.setPen(muted)
        p.drawText(4, margin_t - 2, margin_l - 8, 18,
                   Qt.AlignRight | Qt.AlignVCenter, self._fmt_value(axis_max))
        p.drawText(4, margin_t + area_h - 16, margin_l - 8, 18,
                   Qt.AlignRight | Qt.AlignVCenter, self._fmt_value(axis_min))


class _TimingOverviewWidget(QWidget):
    """Combined WNS/TNS/NVE chart with one row per metric."""
    bar_clicked = pyqtSignal(int)

    def __init__(self, title="Timing Overview"):
        super().__init__()
        self.title = title
        self.labels = []
        self.series = []
        self.row_indices = []
        self.tooltips = {}
        self.is_dark = False
        self._bar_rects = []
        self.setMouseTracking(True)
        self.setMinimumSize(620, 260)

    def set_data(self, labels, series, row_indices=None, tooltips=None, is_dark=False):
        self.labels = list(labels or [])
        cleaned = []
        for name, vals, colors in list(series or []):
            out_vals = []
            for v in vals or []:
                try:
                    fv = float(v)
                    if math.isnan(fv) or math.isinf(fv):
                        fv = 0.0
                except Exception:
                    fv = 0.0
                out_vals.append(fv)
            cleaned.append((name, out_vals, list(colors or [])))
        self.series = cleaned
        self.row_indices = list(row_indices or range(len(self.labels)))
        self.tooltips = dict(tooltips or {})
        self.is_dark = is_dark
        self._bar_rects = []
        self.setMinimumWidth(max(620, 110 + max(1, len(self.labels)) * 58))
        self.setMinimumHeight(88 + max(1, len(self.series)) * 86)
        self.update()

    def _fmt(self, val):
        try:
            return "{:.4g}".format(float(val))
        except Exception:
            return str(val)

    def _hit_index(self, pos):
        for rect, row_idx, tip in self._bar_rects:
            if rect.contains(pos):
                return row_idx, tip
        return None, ""

    def mouseMoveEvent(self, event):
        row_idx, tip = self._hit_index(event.pos())
        if tip:
            QToolTip.showText(event.globalPos(), tip, self)
        else:
            QToolTip.hideText()
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event):
        row_idx, tip = self._hit_index(event.pos())
        if row_idx is not None:
            self.bar_clicked.emit(int(row_idx))
            return
        super().mousePressEvent(event)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        grid = QColor("#55585c" if self.is_dark else "#d7dbe0")
        p.fillRect(self.rect(), bg)
        r = self.rect()
        self._bar_rects = []
        if not self.series or not self.labels:
            p.setPen(fg)
            p.drawText(r, Qt.AlignCenter, "No timing data")
            return

        p.setPen(fg)
        p.drawText(r.adjusted(0, 6, 0, 0),
                   Qt.AlignHCenter | Qt.AlignTop, self.title)
        margin_l = 72
        margin_r = 18
        margin_t = 38
        lane_gap = 20
        lane_h = max(58, int((r.height() - margin_t - 22 -
                              lane_gap * (len(self.series) - 1)) /
                             max(1, len(self.series))))
        area_w = r.width() - margin_l - margin_r
        n = max(1, len(self.labels))
        slot = max(1, area_w // n)
        bar_w = max(4, min(30, slot - 8))

        for lane_idx, (metric, vals, colors) in enumerate(self.series):
            top0 = margin_t + lane_idx * (lane_h + lane_gap)
            bottom = top0 + lane_h
            local = vals or [0.0]
            vmax = max(local)
            vmin = min(local)
            if vmax == vmin:
                if vmax == 0:
                    axis_min, axis_max = -1.0, 1.0
                elif vmax > 0:
                    axis_min, axis_max = 0.0, vmax * 1.15
                else:
                    axis_min, axis_max = vmin * 1.15, 0.0
            else:
                axis_min = min(0.0, vmin)
                axis_max = max(0.0, vmax)
            span = axis_max - axis_min
            if abs(span) < 1e-12:
                span = 1.0

            def _y(value):
                return top0 + int((axis_max - value) / span * lane_h)

            zero_y = _y(0.0)
            p.setPen(fg)
            p.drawText(4, top0, margin_l - 10, 18,
                       Qt.AlignRight | Qt.AlignVCenter, metric)
            p.setPen(QPen(grid, 1))
            p.drawLine(margin_l, zero_y, margin_l + area_w, zero_y)
            p.drawLine(margin_l, top0, margin_l, bottom)
            p.setPen(QPen(grid, 1, Qt.DotLine))
            p.drawLine(margin_l, top0 + lane_h // 2,
                       margin_l + area_w, top0 + lane_h // 2)

            for i, val in enumerate(vals):
                color = colors[i] if i < len(colors) else QColor("#42a5f5")
                x = margin_l + i * slot + max(1, (slot - bar_w) // 2)
                y_val = _y(val)
                bar_top = min(y_val, zero_y)
                h = abs(y_val - zero_y)
                if h < 1:
                    h = 1
                    bar_top = zero_y - 1 if val >= 0 else zero_y
                rect = QRect(x, bar_top, bar_w, h)
                p.setBrush(QBrush(color))
                p.setPen(Qt.NoPen)
                p.drawRect(rect)
                row_idx = self.row_indices[i] if i < len(self.row_indices) else i
                tip = self.tooltips.get((metric, i), "")
                self._bar_rects.append((rect, row_idx, tip))
                if slot >= 54:
                    p.setPen(fg)
                    vy = max(top0, bar_top - 15) if val >= 0 else min(bottom - 14, bar_top + h + 1)
                    p.drawText(x - 18, vy, bar_w + 36, 14,
                               Qt.AlignCenter, self._fmt(val))
            p.setPen(muted)
            p.drawText(4, top0 + 18, margin_l - 10, 16,
                       Qt.AlignRight | Qt.AlignVCenter, self._fmt(axis_max))
            p.drawText(4, bottom - 16, margin_l - 10, 16,
                       Qt.AlignRight | Qt.AlignVCenter, self._fmt(axis_min))


class _StackedVtChartWidget(QWidget):
    """Interactive per-run stacked VT percentage chart."""
    row_clicked = pyqtSignal(int)
    _PALETTE = [
        QColor("#3949ab"), QColor("#00897b"), QColor("#7cb342"),
        QColor("#f9a825"), QColor("#fb8c00"), QColor("#e53935"),
        QColor("#8e24aa"), QColor("#00acc1"), QColor("#6d4c41")
    ]

    def __init__(self, title="VT Area Distribution per Run"):
        super().__init__()
        self.title = title
        self.labels = []
        self.rows = []
        self.vt_names = []
        self.row_indices = []
        self.is_dark = False
        self._segments = []
        self.setMouseTracking(True)
        self.setMinimumSize(420, 250)

    def set_data(self, labels, rows, is_dark=False, vt_names=None, row_indices=None):
        self.labels = list(labels or [])
        self.rows = [list(r or []) for r in (rows or [])]
        self.vt_names = list(vt_names or [])
        if not self.vt_names:
            max_len = max([len(r) for r in self.rows] or [0])
            self.vt_names = ["VT{}".format(i + 1) for i in range(max_len)]
        self.row_indices = list(row_indices or range(len(self.rows)))
        self.is_dark = is_dark
        self._segments = []
        self.setMinimumWidth(760)
        self.setMinimumHeight(84 + max(1, len(self.rows)) * 35 + 42)
        self.update()

    def _hit_segment(self, pos):
        for rect, row_idx, tip in self._segments:
            if rect.contains(pos):
                return row_idx, tip
        return None, ""

    def mouseMoveEvent(self, event):
        row_idx, tip = self._hit_segment(event.pos())
        if tip:
            QToolTip.showText(event.globalPos(), tip, self)
        else:
            QToolTip.hideText()
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event):
        row_idx, tip = self._hit_segment(event.pos())
        if row_idx is not None:
            self.row_clicked.emit(int(row_idx))
            return
        super().mousePressEvent(event)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        border = QColor("#59616a" if self.is_dark else "#cfd8dc")
        p.fillRect(self.rect(), bg)
        r = self.rect()
        self._segments = []
        p.setPen(fg)
        p.drawText(8, 6, r.width() - 16, 22,
                   Qt.AlignHCenter | Qt.AlignVCenter, self.title)

        valid = []
        for idx, row in enumerate(self.rows):
            vals = []
            for v in row:
                try:
                    vals.append(max(0.0, float(v)))
                except Exception:
                    vals.append(0.0)
            if sum(vals) > 0:
                label = self.labels[idx] if idx < len(self.labels) else ""
                row_idx = self.row_indices[idx] if idx < len(self.row_indices) else idx
                valid.append((label, vals, row_idx))
        if not valid:
            p.drawText(r, Qt.AlignCenter, "No VT data")
            return

        left = 130
        right = 22
        top = 42
        row_h = 26
        gap = 9
        legend_h = 34
        shown = valid
        bar_w = max(90, r.width() - left - right)

        for i, (label, vals, row_idx) in enumerate(shown):
            y = top + i * (row_h + gap)
            short = label if len(label) <= 18 else label[:15] + "..."
            p.setPen(fg)
            p.drawText(6, y, left - 14, row_h,
                       Qt.AlignRight | Qt.AlignVCenter, short)
            total = sum(vals) or 1.0
            x = left
            for j, val in enumerate(vals):
                w = int(bar_w * val / total)
                if j == len(vals) - 1:
                    w = left + bar_w - x
                if w <= 0:
                    continue
                color = self._PALETTE[j % len(self._PALETTE)]
                rect = QRect(x, y + 3, w, row_h - 6)
                p.setBrush(QBrush(color))
                p.setPen(QPen(border, 1))
                p.drawRect(rect)
                name = self.vt_names[j] if j < len(self.vt_names) else "VT{}".format(j + 1)
                pct = val / total * 100.0
                tip = "{}\n{}: {:.2f}%".format(label, name, pct)
                self._segments.append((rect, row_idx, tip))
                if w > 46:
                    p.setPen(QColor("#ffffff"))
                    p.drawText(rect, Qt.AlignCenter, "{:.1f}%".format(pct))
                x += w
        lx = left
        ly = r.height() - 28
        for i, name in enumerate(self.vt_names):
            color = self._PALETTE[i % len(self._PALETTE)]
            p.setBrush(QBrush(color))
            p.setPen(Qt.NoPen)
            p.drawRect(lx, ly + 6, 10, 10)
            p.setPen(muted)
            txt = str(name)
            width = max(46, min(90, p.fontMetrics().width(txt) + 18))
            p.drawText(lx + 14, ly, width, 22, Qt.AlignLeft | Qt.AlignVCenter, txt)
            lx += width + 18
            if lx > r.width() - 90:
                break

class _TimelineChartWidget(QWidget):
    """Timeline chart with one FE trunk and one row per child BE/Innovus run."""
    event_clicked = pyqtSignal(object)

    def __init__(self, events=None, parser=None, is_dark=False):
        super().__init__()
        self.events = events or []
        self.parser = parser
        self.is_dark = is_dark
        self._event_rects = []
        self.setCursor(Qt.PointingHandCursor)
        self.setMinimumHeight(220)

    def set_data(self, events, parser, is_dark=False):
        self.events = events or []
        self.parser = parser
        self.is_dark = is_dark
        self.update()

    def _branches(self):
        out = []
        seen = set()
        for ev in self.events or []:
            if ev.get("kind") == "FE":
                continue
            b = ev.get("branch") or "PNR"
            if b not in seen:
                seen.add(b)
                out.append(b)
        return out

    def preferred_height(self, width):
        return 70 + max(1, len(self._branches())) * 126

    def preferred_width(self):
        card_w = 220
        gap_x = 96
        max_seq = 0
        for ev in self.events or []:
            if ev.get("kind") != "FE":
                max_seq = max(max_seq, int(ev.get("seq", 0) or 0) + 1)
        return 380 + max(1, max_seq) * (card_w + gap_x) + card_w + 220

    def _dt(self, val):
        if self.parser:
            try:
                return self.parser(val)
            except Exception:
                return None
        return None

    def _short(self, text, limit):
        text = str(text or "-")
        return text if len(text) <= limit else text[:limit - 3] + "..."

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

    def _draw_card(self, p, rect, ev, color, fg, muted, card_bg, card_border):
        p.setBrush(QBrush(card_bg))
        p.setPen(QPen(card_border, 1))
        p.drawRoundedRect(rect, 7, 7)
        p.setBrush(QBrush(color))
        p.setPen(Qt.NoPen)
        p.drawRoundedRect(QRect(rect.x(), rect.y(), 7, rect.height()), 4, 4)
        p.setPen(fg)
        name = ev.get("name", "-")
        if " / " in name:
            name = name.split(" / ")[-1]
        p.drawText(rect.x() + 16, rect.y() + 8, rect.width() - 26, 20,
                   Qt.AlignLeft | Qt.AlignVCenter, self._short(name, 26))
        p.setPen(color)
        p.drawText(rect.x() + 16, rect.y() + 32, rect.width() - 26, 18,
                   Qt.AlignLeft | Qt.AlignVCenter,
                   "Runtime  " + str(ev.get("runtime", "-")))
        st = self._dt(ev.get("start"))
        en = self._dt(ev.get("end"))
        p.setPen(muted)
        p.drawText(rect.x() + 16, rect.y() + 55, rect.width() - 26, 16,
                   Qt.AlignLeft | Qt.AlignVCenter,
                   "Start    " + (st.strftime("%m/%d %H:%M") if st else "-"))
        p.drawText(rect.x() + 16, rect.y() + 73, rect.width() - 26, 16,
                   Qt.AlignLeft | Qt.AlignVCenter,
                   "End      " + (en.strftime("%m/%d %H:%M") if en else "-"))
        self._event_rects.append((rect, ev))

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        bg = QColor("#2b2d30" if self.is_dark else "#ffffff")
        fg = QColor("#dfe1e5" if self.is_dark else "#263238")
        muted = QColor("#9aa0a6" if self.is_dark else "#6b7280")
        line = QColor("#7b8794" if self.is_dark else "#90a4ae")
        fe_color = QColor("#42a5f5")
        stage_color = QColor("#66bb6a")
        card_bg = QColor("#30343a" if self.is_dark else "#f8fafc")
        card_border = QColor("#555b64" if self.is_dark else "#cfd8dc")
        p.fillRect(self.rect(), bg)
        self._event_rects = []
        events = list(self.events or [])
        if not events:
            p.setPen(fg)
            p.drawText(self.rect(), Qt.AlignCenter, "No timestamp data available")
            return
        p.setPen(fg)
        p.drawText(8, 8, self.width() - 16, 18,
                   Qt.AlignLeft | Qt.AlignVCenter,
                   "FE to child PNR branch timeline")

        fe_events = [ev for ev in events if ev.get("kind") == "FE"]
        fe_ev = fe_events[0] if fe_events else None
        branches = self._branches()
        branch_map = {}
        for b in branches:
            evs = [ev for ev in events
                   if ev.get("kind") != "FE" and ev.get("branch") == b]
            evs.sort(key=lambda ev: int(ev.get("seq", 0) or 0))
            branch_map[b] = evs

        card_w = 220
        card_h = 94
        row_h = 126
        fe_x = 18
        stage_x0 = 380
        top0 = 46
        gap_x = 96
        fe_y = top0 + (max(1, len(branches)) - 1) * row_h // 2
        trunk_x = fe_x + card_w + 28
        if fe_ev:
            self._draw_card(p, QRect(fe_x, fe_y, card_w, card_h),
                            fe_ev, fe_color, fg, muted, card_bg, card_border)
            if branches:
                first_y = top0 + card_h // 2
                last_y = top0 + (len(branches) - 1) * row_h + card_h // 2
                fe_mid_y = fe_y + card_h // 2
                p.setPen(QPen(line, 1))
                p.drawLine(fe_x + card_w, fe_mid_y, trunk_x, fe_mid_y)
                p.drawLine(trunk_x, first_y, trunk_x, last_y)
                p.setBrush(QBrush(line))
                p.setPen(Qt.NoPen)
                p.drawPolygon(QPolygon([
                    QPoint(trunk_x - 1, fe_mid_y),
                    QPoint(trunk_x - 8, fe_mid_y - 4),
                    QPoint(trunk_x - 8, fe_mid_y + 4)]))

        for row, b in enumerate(branches):
            y = top0 + row * row_h
            p.setPen(muted)
            p.drawText(stage_x0, y - 20, 620, 18,
                       Qt.AlignLeft | Qt.AlignVCenter, self._short(b, 90))
            prev_end = self._dt(fe_ev.get("end")) if fe_ev else None
            prev_right = trunk_x if fe_ev else fe_x + card_w
            for idx, ev in enumerate(branch_map.get(b, [])):
                x = stage_x0 + idx * (card_w + gap_x)
                rect = QRect(x, y, card_w, card_h)
                line_y = y + card_h // 2
                p.setPen(QPen(line, 1))
                p.drawLine(prev_right, line_y, x - 8, line_y)
                p.setBrush(QBrush(line))
                p.setPen(Qt.NoPen)
                p.drawPolygon(QPolygon([
                    QPoint(x - 8, line_y - 4),
                    QPoint(x - 8, line_y + 4),
                    QPoint(x - 1, line_y)]))
                gap_txt = self._gap_text(prev_end, self._dt(ev.get("start")))
                if gap_txt != "-":
                    fm = p.fontMetrics()
                    badge_w = max(54, fm.width(gap_txt) + 16)
                    badge_x = int((prev_right + x) / 2 - badge_w / 2)
                    badge = QRect(badge_x, line_y - 29, badge_w, 18)
                    p.setBrush(QBrush(bg))
                    p.setPen(QPen(line, 1))
                    p.drawRoundedRect(badge, 5, 5)
                    p.setPen(muted)
                    p.drawText(badge, Qt.AlignCenter, gap_txt)
                self._draw_card(p, rect, ev, stage_color, fg, muted,
                                card_bg, card_border)
                prev_end = self._dt(ev.get("end")) or prev_end
                prev_right = rect.x() + rect.width()

    def mousePressEvent(self, event):
        for rect, ev in self._event_rects:
            if rect.contains(event.pos()):
                self.event_clicked.emit(ev)
                return
        super().mousePressEvent(event)

class BlockSummaryDialog(QDialog):
    """Block synthesis summary table.
    One row per selected run. User clicks Generate to start loading.
    Tabs: Table | Charts (requires matplotlib)."""

    HEADERS = [
        "BLK Name", "Run Name", "MBIT%", "CG%",
        "Instance Count", "Std Area (um2)", "Gate Count",
        "VT Area%",
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
        self.setWindowTitle("FE Block Summary: " + str(rtl_label))
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
        self._cancelled = False

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

        # -- Tab 1: Table ------------------------------------------------
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

        # -- Tab 2: Charts (PyQt5 native, no matplotlib) ------------------
        tab_charts = QWidget()
        tab_charts_layout = QVBoxLayout(tab_charts)
        tab_charts_layout.setContentsMargins(6, 6, 6, 6)

        chart_top = QHBoxLayout()
        chart_hint = QLabel(
            "Timing histograms are clickable. Hover bars for exact run details.")
        chart_hint.setStyleSheet("color: #607d8b;")
        refresh_charts_btn = QPushButton("Refresh Charts")
        refresh_charts_btn.clicked.connect(self._draw_charts)
        chart_top.addWidget(chart_hint, 1)
        chart_top.addWidget(refresh_charts_btn, 0)
        tab_charts_layout.addLayout(chart_top)

        self._chart_timing = _TimingOverviewWidget("R2R Setup Timing")
        self._chart_timing.bar_clicked.connect(self._select_chart_row)
        self._timing_scroll = QScrollArea()
        self._timing_scroll.setWidgetResizable(False)
        self._timing_scroll.setMinimumHeight(285)
        self._timing_scroll.setWidget(self._chart_timing)
        tab_charts_layout.addWidget(self._timing_scroll, 1)

        self._chart_vt = _StackedVtChartWidget("VT Area % per Run")
        self._chart_vt.row_clicked.connect(self._select_chart_row)
        self._vt_scroll = QScrollArea()
        self._vt_scroll.setWidgetResizable(False)
        self._vt_scroll.setMinimumHeight(260)
        self._vt_scroll.setWidget(self._chart_vt)
        tab_charts_layout.addWidget(self._vt_scroll, 1)
        self._chart_run_map = QTableWidget(0, 2)
        self._chart_run_map.setHorizontalHeaderLabels(["Alias", "Run Name"])
        self._chart_run_map.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._chart_run_map.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._chart_run_map.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._chart_run_map.setMaximumHeight(130)
        tab_charts_layout.addWidget(self._chart_run_map)

        self._tabs.addTab(tab_charts, "Charts")

        # -- Buttons ------------------------------------------------------
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

        self.max_btn = QPushButton("Maximize")
        self.max_btn.clicked.connect(self._toggle_maximize)
        btn_row.addWidget(self.max_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        self.neg_fg  = QColor("#ef5350" if is_dark else "#c62828")
        self.zero_fg = QColor("#66bb6a" if is_dark else "#2e7d32")
        self.pos_fg  = QColor("#66bb6a" if is_dark else "#2e7d32")
        self._done_count = 0

    # -- Loading ----------------------------------------------------------

    def _start_loading(self):
        if not self._run_list:
            return
        self._cancelled = False
        self.gen_btn.setEnabled(False)
        self.tbl.setSortingEnabled(False)
        self.tbl.setRowCount(0)
        self.tbl.setSortingEnabled(True)
        self._done_count = 0
        self.prog.setValue(0)
        self.prog.setRange(0, len(self._run_list))
        self.prog.setVisible(True)
        self.status_lbl.setText(
            "Extracting FE metrics for " + str(len(self._run_list)) +
            " run(s)...")
        try:
            from workers import MetricBatchWorker
            tasks = []
            for blk, run_path, run_name, runtime, source in self._run_list:
                tasks.append({
                    "block": blk,
                    "path": run_path,
                    "run_name": run_name,
                    "runtime": runtime,
                    "source": source,
                    "run_type": "FE",
                })
            w = MetricBatchWorker(tasks)
            if hasattr(w, "progress"):
                w.progress.connect(self._on_batch_progress)
            w.finished.connect(self._on_rows_done)
            w.finished.connect(
                lambda *_args, ww=w:
                setattr(self, "_active_worker", None)
                if getattr(self, "_active_worker", None) is ww else None)
            w.start()
            self._active_worker = w
        except Exception as e:
            self.status_lbl.setText("Metric extraction failed: " + str(e))
            self.prog.setVisible(False)
            self.gen_btn.setEnabled(True)

    def _on_batch_progress(self, done, total):
        if self._cancelled:
            return
        self.prog.setRange(0, total)
        self.prog.setValue(done)
        self.status_lbl.setText(
            "Extracting FE metrics... " + str(done) + "/" + str(total))

    def _on_rows_done(self, rows):
        if self._cancelled:
            return
        self.tbl.setSortingEnabled(False)
        self.tbl.setRowCount(0)
        for row in rows:
            self._add_row(
                row.get("block", "-"),
                row.get("run_name", row.get("name", "-")),
                row.get("runtime", "-"),
                row.get("metrics", {}))
        self.tbl.setSortingEnabled(True)
        self._done_count = len(rows)
        self.prog.setValue(self._done_count)
        self.status_lbl.setText(
            "Done. " + str(self.tbl.rowCount()) + " rows loaded.")
        self.prog.setVisible(False)
        self.gen_btn.setEnabled(True)
        self._draw_charts()

    def _on_row_done(self, blk, run_name, runtime, metrics):
        if self._cancelled:
            return
        self._add_row(blk, run_name, runtime, metrics)
        self._done_count += 1
        self.prog.setValue(self._done_count)
        QTimer.singleShot(10, self._load_next)

    def closeEvent(self, event):
        if self._stop_active_worker():
            event.accept()
        else:
            event.ignore()

    def accept(self):
        self._stop_active_worker()
        super().accept()

    def _toggle_maximize(self):
        if self.isMaximized():
            self.showNormal()
            self.max_btn.setText("Maximize")
        else:
            self.showMaximized()
            self.max_btn.setText("Restore")

    def reject(self):
        if self._stop_active_worker():
            super().reject()

    def _stop_active_worker(self):
        self._cancelled = True
        w = getattr(self, "_active_worker", None)
        try:
            if w and w.isRunning():
                if hasattr(w, "cancel"):
                    w.cancel()
                w.wait(1000)
                if w.isRunning():
                    self.status_lbl.setText(
                        "Stopping metric extraction... please close again in a moment.")
                    return False
        except Exception:
            return True
        return True

    # -- Row builder ------------------------------------------------------

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
                factor = getattr(self.parent(), "gate_count_unit_area", 0.2419)
                gc = str(int(float(std_area) / factor))
            except Exception:
                gc = "-"

        # VTH - dynamic if metric_extract provides labels, fallback compatible.
        vth_data = metrics.get("vth", {})
        vt_label_raw = vth_data.get("vt_labels", vth_data.get("stage_vt_label", "LVT*/RVT*/HVT*"))
        vt_labels = [x.replace("*", "").strip() for x in str(vt_label_raw).split("/") if x.strip()]
        vth_str = vth_data.get("vt_area",
                  vth_data.get("stage_vt_area",
                  vth_data.get("lvt_rvt_hvt_area",
                  vth_data.get("lvt_rvt_area", "-/-"))))

        # R2R timing
        r2r_setup   = metrics.get("r2r_setup",    "-")
        r2r_hold    = metrics.get("r2r_hold",     "-")
        logic_depth = metrics.get("logic_depth",  "-")

        # Runtime: prefer fresh value from metrics over passed-in runtime arg
        rt = metrics.get("runtime", runtime) or runtime or "-"

        vals = [blk, run_name, mbit, cgc, inst, std_area,
                gc, vth_str, r2r_setup, r2r_hold, logic_depth, rt]

        # run_path stored in metrics - need it for double-click open
        _run_path = metrics.get("run_dir", "")

        self.tbl.setSortingEnabled(False)
        r = self.tbl.rowCount()
        self.tbl.insertRow(r)

        for c, val in enumerate(vals):
            item = QTableWidgetItem(str(val) if val else "-")
            item.setTextAlignment(Qt.AlignCenter)
            if c in (0, 1):
                item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                item.setForeground(QColor("#64b5f6" if self.is_dark else "#1565c0"))
                item.setToolTip("Double-click to open run folder:\n" + str(_run_path))
                item.setData(Qt.UserRole, _run_path)
                if c == 0:
                    f2 = item.font(); f2.setBold(True); item.setFont(f2)
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
            if c == 7:
                item.setData(Qt.UserRole + 1, vt_labels)
                if vt_labels:
                    item.setToolTip("VT order: " + "/".join(vt_labels))
            self.tbl.setItem(r, c, item)
        self.tbl.setSortingEnabled(True)

    # -- Charts (PyQt5 native) ---------------------------------------------

    def _select_chart_row(self, row):
        try:
            row = int(row)
            if row < 0 or row >= self.tbl.rowCount():
                return
            self.tbl.selectRow(row)
            self.tbl.scrollToItem(
                self.tbl.item(row, 1), QAbstractItemView.PositionAtCenter)
            self._tabs.setCurrentIndex(0)
        except Exception:
            pass

    def _parse_metric_triplet(self, text):
        vals = []
        for part in str(text or "").replace("%", "").split("/"):
            try:
                vals.append(float(part.strip()))
            except Exception:
                vals.append(0.0)
        while len(vals) < 3:
            vals.append(0.0)
        return vals[:3]

    def _parse_vt_values(self, text):
        vals = []
        for part in str(text or "").replace("%", "").split("/"):
            try:
                vals.append(float(part.strip()))
            except Exception:
                vals.append(0.0)
        return vals

    def _draw_charts(self):
        n = self.tbl.rowCount()
        if n == 0:
            empty = []
            if hasattr(self, "_chart_run_map"):
                self._chart_run_map.setRowCount(0)
            self._chart_timing.set_data(empty, [], is_dark=self.is_dark)
            self._chart_vt.set_data(empty, empty, is_dark=self.is_dark)
            return

        def _cell(row, col):
            it = self.tbl.item(row, col)
            return it.text() if it else "-"

        labels = []
        full_names = []
        row_ids = []
        wns_vals = []
        tns_vals = []
        nve_vals = []
        vt_rows = []
        vt_names = []

        for row in range(n):
            name = _cell(row, 1)
            full_names.append(name)
            alias = "run{}".format(row + 1)
            labels.append(alias)
            row_ids.append(row)
            wns, tns, nve = self._parse_metric_triplet(_cell(row, self._COL_R2R_SETUP))
            wns_vals.append(wns)
            tns_vals.append(tns)
            nve_vals.append(nve)
            vt_item = self.tbl.item(row, 7)
            row_vt_names = []
            if vt_item:
                stored = vt_item.data(Qt.UserRole + 1)
                if stored:
                    row_vt_names = list(stored)
            vals = self._parse_vt_values(_cell(row, 7))
            if row_vt_names and len(row_vt_names) > len(vt_names):
                vt_names = row_vt_names
            vt_rows.append(vals)

        if hasattr(self, "_chart_run_map"):
            self._chart_run_map.setRowCount(0)
            for row, name in enumerate(full_names):
                alias = "run{}".format(row + 1)
                self._chart_run_map.insertRow(row)
                a_item = QTableWidgetItem(alias)
                n_item = QTableWidgetItem(name)
                a_item.setToolTip(name)
                n_item.setToolTip(name)
                self._chart_run_map.setItem(row, 0, a_item)
                self._chart_run_map.setItem(row, 1, n_item)

        if not vt_names:
            max_vt = max([len(x) for x in vt_rows] or [0])
            default_names = ["LVT", "RVT", "HVT"]
            vt_names = default_names[:max_vt]
            while len(vt_names) < max_vt:
                vt_names.append("VT{}".format(len(vt_names) + 1))
        for vals in vt_rows:
            while len(vals) < len(vt_names):
                vals.append(0.0)

        timing_tips = {}
        def _add_timing_tips(metric_name, vals):
            for i, val in enumerate(vals):
                timing_tips[(metric_name, i)] = "{}: {}\n{}: {}\nR2R Setup: {}".format(
                    labels[i], full_names[i], metric_name, val,
                    _cell(i, self._COL_R2R_SETUP))
        _add_timing_tips("WNS", wns_vals)
        _add_timing_tips("TNS", tns_vals)
        _add_timing_tips("NVE", nve_vals)

        self._chart_timing.set_data(
            labels,
            [
                ("WNS", wns_vals,
                 [QColor("#ef5350") if v < 0 else QColor("#66bb6a") for v in wns_vals]),
                ("TNS", tns_vals,
                 [QColor("#ef5350") if v < 0 else QColor("#66bb6a") for v in tns_vals]),
                ("NVE", nve_vals,
                 [QColor("#ef5350") if v > 0 else QColor("#66bb6a") for v in nve_vals]),
            ],
            row_indices=row_ids,
            tooltips=timing_tips,
            is_dark=self.is_dark)
        self._chart_vt.set_data(
            labels, vt_rows, self.is_dark, vt_names=vt_names,
            row_indices=row_ids)

    # -- Open cell report in gvim ------------------------------------------

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
        if c in (0, 1) and run_path:
            try:
                subprocess.Popen(['xdg-open', run_path])
            except Exception:
                try:
                    subprocess.Popen(['gvim', run_path])
                except Exception as e:
                    QMessageBox.warning(self, "Open Path", str(e))
            return
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

    def _html_table(self):
        nc = self.tbl.columnCount()
        headers = [
            self.tbl.horizontalHeaderItem(c).text()
            if self.tbl.horizontalHeaderItem(c) else ""
            for c in range(nc)]
        html_lines = [
            "<table border='1' cellpadding='4' cellspacing='0' "
            "style='border-collapse:collapse;font-family:Arial,monospace;font-size:12px;'>",
            "<tr>" + "".join(
                "<th style='background:#1976d2;color:white;'>{}</th>".format(
                    html.escape(h)) for h in headers) + "</tr>"
        ]
        for r in range(self.tbl.rowCount()):
            html_lines.append("<tr>" + "".join(
                "<td>{}</td>".format(html.escape(
                    self.tbl.item(r, c).text() if self.tbl.item(r, c) else ""))
                for c in range(nc)) + "</tr>")
        html_lines.append("</table>")
        return "\n".join(html_lines)

    def _send_mail(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(self, "Mail", "Generate table first.")
            return
        html_body = self._html_table()
        parent = self.parent()
        if parent and hasattr(parent, '_open_mail_compose_dialog'):
            parent._open_mail_compose_dialog(
                subject="FE Block Summary: " + self.windowTitle(),
                body=html_body,
                html_body=html_body)
        else:
            QMessageBox.information(self, "Mail Body", html_body[:3000])

    def _export_csv(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(
                self, "Export", "No data yet. Click Generate first.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export", "fe_block_summary.csv", "CSV Files (*.csv)")
        if not path:
            return
        try:
            import csv as _csv
            with open(path, "w", newline="") as f:
                w = _csv.writer(f)
                hdrs = [
                    self.tbl.horizontalHeaderItem(c).text()
                    if self.tbl.horizontalHeaderItem(c) else ""
                    for c in range(self.tbl.columnCount())]
                w.writerow(hdrs)
                for r in range(self.tbl.rowCount()):
                    row = [
                        self.tbl.item(r, c).text()
                        if self.tbl.item(r, c) else ""
                        for c in range(self.tbl.columnCount())]
                    w.writerow(row)
            QMessageBox.information(self, "Export", "Saved:\n" + path)
        except Exception as e:
            QMessageBox.warning(self, "Export Error", str(e))


class BEStageSummaryDialog(QDialog):
    HEADERS = [
        "Block", "BE Run", "Stage",
        "R2R Setup W/T/N", "Total Setup W/T/N",
        "Hold W/T/N",
        "Cong/Shorts", "Std Cell Count/Area", "GC",
        "Std Cell/Std Only Util", "Total Util",
        "VT Inst%", "VT Area%", "Skew/Latency",
        "Clock Repeater Count/Area", "Runtime",
    ]

    def __init__(self, title, tasks, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowFlags(
            self.windowFlags()
            | Qt.WindowMaximizeButtonHint
            | Qt.WindowMinimizeButtonHint)
        self.setSizeGripEnabled(True)
        self.resize(1500, 650)
        self._tasks = list(tasks or [])
        self._worker = None
        self._cancelled = False
        self.is_dark = is_dark
        layout = QVBoxLayout(self)

        hdr = QLabel("<b>{}</b>".format(html.escape(str(title))))
        hdr.setAlignment(Qt.AlignCenter)
        layout.addWidget(hdr)

        self.status_lbl = QLabel("{} stage(s) selected. Click Generate to extract metrics.".format(len(self._tasks)))
        self.status_lbl.setStyleSheet("color: #1976d2;")
        layout.addWidget(self.status_lbl)

        self.tbl = QTableWidget(0, len(self.HEADERS))
        self.tbl.setHorizontalHeaderLabels(self.HEADERS)
        hh = self.tbl.horizontalHeader()
        hh.setSectionsMovable(True)
        for c in range(len(self.HEADERS)):
            hh.setSectionResizeMode(c, QHeaderView.Interactive)
            self.tbl.setColumnWidth(c, 115)
        self.tbl.setColumnWidth(1, 260)
        self.tbl.setColumnWidth(2, 120)
        self.tbl.setColumnWidth(7, 160)
        self.tbl.setColumnWidth(14, 170)
        self.tbl.setColumnWidth(15, 130)
        self.tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.verticalHeader().setVisible(False)
        self.tbl.setSortingEnabled(True)
        self.tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        parent_obj = self.parent()
        if parent_obj and hasattr(parent_obj, "_make_table_user_adjustable"):
            parent_obj._make_table_user_adjustable(self.tbl)
        layout.addWidget(self.tbl, 1)

        btn_row = QHBoxLayout()
        self.gen_btn = QPushButton("Generate Table")
        self.gen_btn.clicked.connect(self._start_loading)
        btn_row.addWidget(self.gen_btn)
        self.prog = QProgressBar()
        self.prog.setRange(0, len(self._tasks))
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

    def _metric_value(self, metrics, key):
        metrics = metrics or {}
        area = metrics.get("area", {}) if isinstance(metrics.get("area", {}), dict) else {}
        vth = metrics.get("vth", {}) if isinstance(metrics.get("vth", {}), dict) else {}
        cong = metrics.get("congestion", {}) if isinstance(metrics.get("congestion", {}), dict) else {}
        if key == "std_count_area":
            return metrics.get("std_cell_count_area", "-")
        if key == "gc":
            val = metrics.get("gate_count", "-")
            if val != "-":
                return val
            parent_obj = self.parent()
            try:
                factor = getattr(parent_obj, "gate_count_unit_area", 0.2419) or 0.2419
                return str(int(float(area.get("std_cell_area", "-")) / factor))
            except Exception:
                return "-"
        if key == "std_util":
            util = metrics.get("util", {}) if isinstance(metrics.get("util", {}), dict) else {}
            return util.get("std_util_str", metrics.get("std_util_str", "-"))
        if key == "vt_inst":
            if vth.get("stage_vt_inst"):
                return vth.get("stage_vt_inst")
            return vth.get("vt_inst", vth.get("lvt_rvt_hvt_inst", vth.get("lvt_rvt_inst", "-")))
        if key == "vt_area":
            if vth.get("stage_vt_area"):
                return vth.get("stage_vt_area")
            return vth.get("vt_area", vth.get("lvt_rvt_hvt_area", vth.get("lvt_rvt_area", "-")))
        if key == "cong":
            return cong.get("cong_both", metrics.get("congestion", "-"))
        if key == "runtime":
            return metrics.get("runtime", "-")
        return metrics.get(key, "-")

    def _vt_header_label(self, rows, suffix):
        for row in rows or []:
            metrics = row.get("metrics", {}) if isinstance(row, dict) else {}
            vth = metrics.get("vth", {}) if isinstance(metrics.get("vth", {}), dict) else {}
            label = vth.get("stage_vt_label")
            if label:
                return label + " " + suffix
        return "VT " + suffix

    def _start_loading(self):
        if not self._tasks:
            return
        self._cancelled = False
        self.tbl.setSortingEnabled(False)
        self.tbl.setRowCount(0)
        self.tbl.setSortingEnabled(True)
        self.gen_btn.setEnabled(False)
        self.prog.setVisible(True)
        self.prog.setRange(0, len(self._tasks))
        self.status_lbl.setText("Extracting BE stage metrics...")
        try:
            self._worker = MetricBatchWorker(self._tasks)
            if hasattr(self._worker, "progress"):
                self._worker.progress.connect(self._on_batch_progress)
            self._worker.finished.connect(self._on_metrics_done)
            self._worker.finished.connect(
                lambda *_: setattr(self, "_worker", None))
            self._worker.start()
        except Exception as e:
            self.gen_btn.setEnabled(True)
            QMessageBox.warning(self, "BE Stage Summary", str(e))

    def _on_batch_progress(self, done, total):
        if self._cancelled:
            return
        self.prog.setRange(0, total)
        self.prog.setValue(done)
        self.status_lbl.setText(
            "Extracting BE stage metrics... {} / {}".format(done, total))

    def _on_metrics_done(self, rows):
        if self._cancelled:
            return
        self.tbl.setSortingEnabled(False)
        self.tbl.setRowCount(0)
        headers = list(self.HEADERS)
        headers[11] = self._vt_header_label(rows, "Inst%")
        headers[12] = self._vt_header_label(rows, "Area%")
        self.tbl.setHorizontalHeaderLabels(headers)
        for row in rows:
            self._add_row(row, row.get("metrics", {}))
        self.tbl.setSortingEnabled(True)
        self.prog.setValue(len(rows))
        self.prog.setVisible(False)
        self.gen_btn.setEnabled(True)
        self.status_lbl.setText("Done. {} row(s) loaded.".format(self.tbl.rowCount()))

    def closeEvent(self, event):
        if self._stop_active_worker():
            event.accept()
        else:
            event.ignore()

    def accept(self):
        self._stop_active_worker()
        super().accept()

    def reject(self):
        if self._stop_active_worker():
            super().reject()

    def _stop_active_worker(self):
        self._cancelled = True
        w = getattr(self, "_worker", None)
        try:
            if w and w.isRunning():
                if hasattr(w, "cancel"):
                    w.cancel()
                w.wait(1000)
                if w.isRunning():
                    self.status_lbl.setText(
                        "Stopping metric extraction... please close again in a moment.")
                    return False
        except Exception:
            return True
        return True

    def _add_row(self, task, metrics):
        values = [
            task.get("block", "-"),
            task.get("be_name", task.get("name", "-")),
            task.get("stage_name", "-"),
            self._metric_value(metrics, "setup_r2r"),
            self._metric_value(metrics, "setup_total"),
            self._metric_value(metrics, "hold_total") if self._metric_value(metrics, "hold_total") != "-" else self._metric_value(metrics, "hold_all"),
            self._metric_value(metrics, "cong"),
            self._metric_value(metrics, "std_count_area"),
            self._metric_value(metrics, "gc"),
            self._metric_value(metrics, "std_util"),
            self._metric_value(metrics, "total_util"),
            self._metric_value(metrics, "vt_inst"),
            self._metric_value(metrics, "vt_area"),
            self._metric_value(metrics, "skew_latency"),
            self._metric_value(metrics, "clock_repeater_count_area"),
            (self._metric_value(metrics, "runtime")
             if self._metric_value(metrics, "runtime") not in ("", "-", "N/A")
             else task.get("runtime", "-")),
        ]
        r = self.tbl.rowCount()
        self.tbl.insertRow(r)
        for c, val in enumerate(values):
            item = QTableWidgetItem(str(val) if val else "-")
            item.setTextAlignment(Qt.AlignCenter)
            if c in (0, 1, 2):
                item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            if c in (3, 4, 5):
                try:
                    first = float(str(val).split("/")[0])
                    if first < 0:
                        item.setForeground(QColor("#ef5350" if self.is_dark else "#c62828"))
                    elif first == 0:
                        item.setForeground(QColor("#66bb6a" if self.is_dark else "#2e7d32"))
                except Exception:
                    pass
            self.tbl.setItem(r, c, item)

    def _html_table(self):
        nc = self.tbl.columnCount()
        headers = [self.tbl.horizontalHeaderItem(c).text() for c in range(nc)]
        html_lines = [
            "<table border='1' cellpadding='4' cellspacing='0' "
            "style='border-collapse:collapse;font-family:Arial,monospace;font-size:12px;'>",
            "<tr>" + "".join(
                "<th style='background:#1976d2;color:white;'>{}</th>".format(
                    html.escape(h)) for h in headers) + "</tr>"
        ]
        for r in range(self.tbl.rowCount()):
            html_lines.append("<tr>" + "".join(
                "<td>{}</td>".format(html.escape(
                    self.tbl.item(r, c).text() if self.tbl.item(r, c) else ""))
                for c in range(nc)) + "</tr>")
        html_lines.append("</table>")
        return "\n".join(html_lines)

    # -- Mail -------------------------------------------------------------

    def _send_mail(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(self, "Mail", "Generate table first.")
            return
        html_body = self._html_table()
        parent = self.parent()
        if parent and hasattr(parent, '_open_mail_compose_dialog'):
            parent._open_mail_compose_dialog(
                subject="BE Stage Summary: " + self.windowTitle(),
                body=html_body,
                html_body=html_body)
        else:
            QMessageBox.information(self, "Mail Body", html_body[:3000])

    # -- Export CSV -------------------------------------------------------

    def _export_csv(self):
        if self.tbl.rowCount() == 0:
            QMessageBox.information(
                self, "Export", "No data yet. Click Generate first.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export", "be_stage_summary.csv", "CSV Files (*.csv)")
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
        self.setWindowTitle("Flow Pulse | Beta 1")
        self.resize(1280, 720)
        self.setMinimumSize(800, 600)

        # -- data ---------------------------------------------------------
        self.ws_data      = {}
        self.out_data     = {}
        self.ir_data      = {}
        self.global_notes = load_all_notes()
        self.personal_notes = load_personal_notes()
        self.user_pins    = load_user_pins()
        self._fp_ver_cache = {}
        self._cong_img_cache = {}
        self._cong_image_cache = {}
        self._fe_cong_workers = []
        self._fe_cong_request_token = 0
        self._stage_screenshot_cache = {}
        self._stage_screenshot_workers = []
        self._stage_screenshot_request_token = 0
        self._stage_metric_cache = {}
        self._stage_metric_workers = []
        self._stage_metric_request_token = 0
        self._running_items = []
        self._visible_run_item_cache = None
        self._quick_refresh_worker = None
        self._quick_refresh_items = {}
        self._owner_lookup_worker = None
        self._owner_items_by_path = {}
        self._stage_metric_last_key = None

        # -- theme/display ------------------------------------------------
        self.is_dark_mode          = False
        self.use_custom_colors     = False
        self.custom_bg_color       = "#2b2d30"
        self.custom_fg_color       = "#dfe1e5"
        self.custom_sel_color      = "#2f65ca"
        self.row_spacing           = 2
        self.show_relative_time    = prefs.get(
            'UI', 'show_relative_time', fallback='false').lower() == 'true'
        self.convert_to_ist        = prefs.get(
            'UI', 'convert_to_ist', fallback='false').lower() == 'true'
        self.hide_block_nodes      = prefs.get(
            'UI', 'hide_block_nodes', fallback='false').lower() == 'true'
        self.gate_count_unit_area  = prefs.getfloat('UI', 'gate_count_unit_area', fallback=0.2419)

        # -- worker/state -------------------------------------------------
        self.size_workers           = []
        self._stage_workers         = []
        self._io_threads            = []
        self._closure_pass_token    = 0
        self.worker                 = None
        self._metric_worker          = None
        self._qor_worker             = None
        self._disk_scan_worker       = None
        self._metric_batch_worker    = None
        self.item_map               = {}
        self._signoff_items_by_path  = {}
        self._signoff_worker         = None
        self._signoff_bg_done        = False
        self._last_view_preset       = "All Runs"
        self._tree_sort_mode         = prefs.get(
            'UI', 'last_sort', fallback='Start Date Old->New')
        self.ignored_paths          = set()
        self._checked_paths         = set()
        self.current_error_log_path = None
        self._building_tree         = False
        self._last_stylesheet       = ""
        self._closure_enabled       = prefs.get(
            'UI', 'closure_enabled', fallback='false').lower() == 'true'
        self._status_regression_enabled = prefs.get(
            'UI', 'status_regression_enabled', fallback='false').lower() == 'true'
        self._qor_regression_enabled = prefs.get(
            'UI', 'qor_regression_enabled', fallback='false').lower() == 'true'
        self.enable_fe_hover_metrics = prefs.get(
            'UI', 'enable_fe_hover_metrics', fallback='false').lower() == 'true'
        self._hover_metric_cache     = {}
        self._hover_metric_worker    = None
        self._hover_metric_path      = ""
        self._columns_fitted_once   = False
        self._initial_size_calc_done= False
        self._last_scan_time        = ""
        self.run_filter_config      = None
        self.current_config_path    = None
        self.ignore_run_filter      = False
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
        self.auto_refresh_timer.timeout.connect(self.start_quick_refresh)

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
        # DiskScannerWorker runs `du -sk` on NFS - extremely I/O heavy.
        # Removed auto-start: it now only runs when user clicks "Disk Space".
        # This eliminates NFS contention that made all post-scan clicks sluggish.

    # ------------------------------------------------------------------
    # CLOSE
    # ------------------------------------------------------------------
    def _start_io_thread(self, target):
        """Start a tracked non-daemon writer thread for critical JSON data."""
        try:
            self._io_threads = [t for t in getattr(self, "_io_threads", []) if t.is_alive()]
            t = threading.Thread(target=target)
            t.daemon = False
            t.start()
            self._io_threads.append(t)
            return t
        except Exception:
            try:
                target()
            except Exception:
                pass
            return None

    def _wait_for_io_threads(self, timeout_ms=1500):
        deadline = time.time() + (float(timeout_ms) / 1000.0)
        alive = []
        for t in list(getattr(self, "_io_threads", [])):
            try:
                remaining = deadline - time.time()
                if remaining <= 0:
                    alive.append(t)
                    continue
                t.join(remaining)
                if t.is_alive():
                    alive.append(t)
            except Exception:
                pass
        self._io_threads = alive

    def _cancel_worker_if_possible(self, worker):
        if not worker:
            return
        try:
            if hasattr(worker, "cancel"):
                worker.cancel()
        except Exception:
            pass
        try:
            if hasattr(worker, "requestInterruption"):
                worker.requestInterruption()
        except Exception:
            pass

    def _stop_worker_if_running(self, worker, timeout_ms=1200):
        if not worker:
            return True
        try:
            if not worker.isRunning():
                return True
        except RuntimeError:
            return True
        except Exception:
            return True
        self._cancel_worker_if_possible(worker)
        try:
            worker.wait(timeout_ms)
        except RuntimeError:
            return True
        except Exception:
            pass
        try:
            if worker.isRunning():
                return False
        except RuntimeError:
            return True
        except Exception:
            return False
        return not self._worker_is_running(worker)

    def _stop_worker_attr(self, name, timeout_ms=1200):
        worker = getattr(self, name, None)
        stopped = self._stop_worker_if_running(worker, timeout_ms)
        if stopped:
            try:
                setattr(self, name, None)
            except Exception:
                pass
        return stopped

    def _clear_worker_attr_if_current(self, name, worker):
        if getattr(self, name, None) is worker:
            try:
                setattr(self, name, None)
            except Exception:
                pass

    def _cancel_worker_list_keep_running(self, workers):
        kept = []
        for worker in list(workers or []):
            self._cancel_worker_if_possible(worker)
            if self._worker_is_running(worker):
                kept.append(worker)
        return kept

    def _stop_worker_list_now(self, workers, timeout_ms=500):
        kept = []
        for worker in list(workers or []):
            if not self._stop_worker_if_running(worker, timeout_ms):
                kept.append(worker)
        return kept

    def _has_running_workers(self):
        for seq_name in (
                "size_workers", "_stage_workers", "_fe_cong_workers",
                "_stage_screenshot_workers", "_stage_metric_workers"):
            for worker in list(getattr(self, seq_name, []) or []):
                if self._worker_is_running(worker):
                    return True
        for name in (
                "worker", "_signoff_worker", "_metric_worker",
                "_qor_worker", "_disk_scan_worker", "_metric_batch_worker",
                "_hover_metric_worker", "_quick_refresh_worker",
                "_owner_lookup_worker"):
            if self._worker_is_running(getattr(self, name, None)):
                return True
        return False

    def _shutdown_all_workers(self):
        for seq_name in (
                "size_workers", "_stage_workers", "_fe_cong_workers",
                "_stage_screenshot_workers", "_stage_metric_workers"):
            seq = getattr(self, seq_name, [])
            try:
                setattr(self, seq_name, self._stop_worker_list_now(seq, 1200))
            except Exception:
                pass
        for name in (
                "worker", "_signoff_worker", "_metric_worker",
                "_qor_worker", "_disk_scan_worker", "_metric_batch_worker",
                "_hover_metric_worker", "_quick_refresh_worker",
                "_owner_lookup_worker"):
            self._stop_worker_attr(name)

    def closeEvent(self, event):
        if not prefs.has_section('UI'):
            prefs.add_section('UI')
        prefs.set('UI', 'main_splitter', ','.join(
            map(str, self.main_splitter.sizes())))
        prefs.set('UI', 'last_source',  self.src_combo.currentText())
        prefs.set('UI', 'last_rtl',     self.rel_combo.currentText())
        prefs.set('UI', 'last_view',    self.view_combo.currentText())
        prefs.set('UI', 'last_sort', getattr(
            self, "_tree_sort_mode", "Start Date Old->New"))
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
        _write_config_atomic(prefs, USER_PREFS_FILE)
        self._shutdown_all_workers()
        self._wait_for_io_threads(1500)
        if self._has_running_workers():
            try:
                self.status_bar.showMessage(
                    "Waiting for background workers to stop before closing...",
                    3000)
                QTimer.singleShot(1000, self.close)
            except Exception:
                pass
            event.ignore()
            return
        event.accept()

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
        _write_config_atomic(prefs, USER_PREFS_FILE)

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
            base = NOTES_DIR
        except Exception:
            base = os.path.dirname(USER_PREFS_FILE)
        return os.path.join(
            base, "run_history_{}.json".format(_safe_user_name()))

    def _load_run_history(self):
        """Load run history dict: {run_key: [{status, runtime, fm, vslp, ts}]}"""
        import json
        fp = self._history_file()
        old_fp = os.path.join(SCRIPT_DIR, "run_history.json")
        if (not os.path.exists(fp)) and os.path.exists(old_fp):
            try:
                _ensure_notes_dir()
                shutil.copy2(old_fp, fp)
            except Exception:
                pass
        try:
            with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_run_history(self):
        """Save run history in a tracked writer thread with atomic replace."""
        data = dict(self._run_history)
        fp = self._history_file()
        def _write():
            try:
                _atomic_write_json(fp, data, indent=2)
            except Exception:
                pass
        self._start_io_thread(_write)

    # ------------------------------------------------------------------
    # LIGHTWEIGHT SNAPSHOTS
    # ------------------------------------------------------------------
    def _snapshot_dir(self):
        try:
            if not os.path.exists(SNAPSHOT_DIR):
                os.makedirs(SNAPSHOT_DIR)
            return SNAPSHOT_DIR
        except Exception:
            return os.path.join(os.path.dirname(USER_PREFS_FILE), "snapshots")

    def _snapshot_latest_file(self):
        return os.path.join(self._snapshot_dir(), "latest_snapshot.json.gz")

    def _json_safe(self, obj):
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, (list, tuple)):
            return [self._json_safe(x) for x in obj]
        if isinstance(obj, set):
            return sorted(self._json_safe(x) for x in obj)
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                try:
                    key = str(k)
                except Exception:
                    key = "<key>"
                out[key] = self._json_safe(v)
            return out
        try:
            return str(obj)
        except Exception:
            return ""

    def _scan_data_for_snapshot(self):
        return {
            "ws_data": self._json_safe(self.ws_data),
            "out_data": self._json_safe(self.out_data),
            "ir_data": self._json_safe(self.ir_data),
        }

    def _snapshot_payload(self, stats=None):
        all_runs = ((self.ws_data or {}).get("all_runs", []) +
                    (self.out_data or {}).get("all_runs", []))
        completed = 0
        running = 0
        failed = 0
        stages = 0
        for run in all_runs:
            st = str(run.get("fe_status", "")).upper()
            if run.get("is_comp") or st == "COMPLETED":
                completed += 1
            elif "RUN" in st:
                running += 1
            elif "FAIL" in st or "ERROR" in st or "FATAL" in st:
                failed += 1
            try:
                stages += len(run.get("stages", []) or [])
            except Exception:
                pass
        return {
            "schema": "flow_pulse_light_snapshot_v1",
            "project": PROJECT_PREFIX,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_by": getpass.getuser(),
            "stats": self._json_safe(stats or {}),
            "summary": {
                "runs": len(all_runs),
                "completed": completed,
                "running": running,
                "failed": failed,
                "stages": stages,
                "ws_runs": len((self.ws_data or {}).get("all_runs", []) or []),
                "outfeed_runs": len((self.out_data or {}).get("all_runs", []) or []),
            },
            "scan_data": self._scan_data_for_snapshot(),
            "user_state": {
                "pins": self._json_safe(getattr(self, "user_pins", {})),
                "shared_notes": self._json_safe(getattr(self, "global_notes", {})),
                "personal_notes": self._json_safe(getattr(self, "personal_notes", {})),
                "run_filter_config": self._json_safe(getattr(self, "run_filter_config", {})),
                "ignored_paths": self._json_safe(getattr(self, "ignored_paths", set())),
            },
            "lazy_cache": {
                "stage_metrics": self._json_safe(getattr(self, "_stage_metric_cache", {})),
                "fp_version": self._json_safe(getattr(self, "_fp_ver_cache", {})),
                "congestion_images": self._json_safe(getattr(self, "_cong_img_cache", {})),
            },
            "metrics_cache": self._json_safe(self._current_metrics_cache_payload()),
        }

    def _current_metrics_cache_payload(self):
        try:
            import workers
            if hasattr(workers, "get_metric_cache_payload"):
                return workers.get_metric_cache_payload()
        except Exception:
            pass
        fp = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "dashboard_notes", "metrics_cache.json.gz")
        if os.path.exists(fp):
            try:
                with gzip.open(fp, "rt", encoding="utf-8", errors="ignore") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"version": 1, "entries": {}}

    def _merge_metrics_cache_from_snapshot(self, payload):
        cache = payload.get("metrics_cache", {}) if isinstance(payload, dict) else {}
        if not isinstance(cache, dict):
            return
        try:
            import workers
            if hasattr(workers, "merge_metric_cache_payload"):
                workers.merge_metric_cache_payload(cache)
                return
        except Exception:
            pass
        try:
            fp = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "dashboard_notes", "metrics_cache.json.gz")
            _atomic_write_gzip_json(fp, cache, sort_keys=True)
        except Exception:
            pass

    def show_metric_cache_status(self):
        try:
            import workers
            status = workers.metric_cache_status()
        except Exception as e:
            QMessageBox.warning(
                self, "Metric Cache",
                "Could not read metric cache status:\n" + str(e))
            return
        lines = [
            "Metric cache:",
            str(status.get("file", "-")),
            "",
            "Entries: " + str(status.get("entries", 0)),
            "Saved at: " + str(status.get("saved_at", "-")),
            "",
            "Used by FE Block Summary, BE Stage Summary, QoR Summary, RoR, and Golden Benchmark.",
        ]
        QMessageBox.information(self, "Metric Cache", "\n".join(lines))

    def clear_metric_cache(self):
        res = QMessageBox.question(
            self, "Clear Metric Cache",
            "Clear saved FE/BE metric extraction cache?\n\n"
            "Next table generation will re-parse reports.",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if res != QMessageBox.Yes:
            return
        try:
            import workers
            workers.clear_metric_cache()
            QMessageBox.information(
                self, "Metric Cache",
                "Metric cache cleared.")
        except Exception as e:
            QMessageBox.warning(
                self, "Metric Cache",
                "Could not clear metric cache:\n" + str(e))

    def _write_snapshot_payload(self, payload):
        snap_dir = self._snapshot_dir()
        latest = self._snapshot_latest_file()
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dated = os.path.join(snap_dir, "snapshot_{}.json.gz".format(stamp))
        for fp in (latest, dated):
            try:
                _atomic_write_gzip_json(fp, payload, indent=2, sort_keys=True)
            except Exception:
                pass
        self._prune_old_snapshots(snap_dir, keep=25)
        return latest

    def _prune_old_snapshots(self, snap_dir, keep=25):
        try:
            names = [os.path.join(snap_dir, n) for n in os.listdir(snap_dir)
                     if n.startswith("snapshot_") and n.endswith(".json.gz")]
            names.sort(key=os.path.getmtime, reverse=True)
            for fp in names[keep:]:
                try:
                    os.remove(fp)
                except Exception:
                    pass
        except Exception:
            pass

    def _save_lightweight_snapshot_async(self, stats=None):
        payload = self._snapshot_payload(stats)
        def _write():
            try:
                self._write_snapshot_payload(payload)
            except Exception:
                pass
        self._start_io_thread(_write)

    def save_snapshot_now(self):
        if not (self.ws_data or self.out_data):
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "No scan data is loaded yet. Run a full scan first.")
            return
        try:
            fp = self._write_snapshot_payload(self._snapshot_payload({}))
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "Snapshot saved:\n" + fp)
        except Exception as e:
            QMessageBox.warning(
                self, "Lightweight Snapshot",
                "Could not save snapshot:\n" + str(e))

    def _read_snapshot(self, fp=None):
        fp = fp or self._snapshot_latest_file()
        if not os.path.exists(fp):
            return None
        try:
            with gzip.open(fp, "rt", encoding="utf-8", errors="ignore") as f:
                return json.load(f)
        except Exception:
            try:
                with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                    return json.load(f)
            except Exception:
                return None

    def _restore_scan_data_shape(self, data):
        data = data if isinstance(data, dict) else {}
        for key in ("ws_data", "out_data"):
            section = data.get(key) or {}
            if isinstance(section.get("blocks"), list):
                section["blocks"] = set(section.get("blocks") or [])
            if not isinstance(section.get("releases"), dict):
                section["releases"] = {}
            if not isinstance(section.get("all_runs"), list):
                section["all_runs"] = []
            data[key] = section
        if not isinstance(data.get("ir_data"), dict):
            data["ir_data"] = {}
        return data

    def show_snapshot_status(self):
        fp = self._snapshot_latest_file()
        payload = self._read_snapshot(fp)
        if not payload:
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "No snapshot found yet.\n\nExpected:\n" + fp)
            return
        summary = payload.get("summary", {})
        lines = [
            "Latest snapshot:",
            fp,
            "",
            "Created: " + str(payload.get("created_at", "-")),
            "User: " + str(payload.get("created_by", "-")),
            "Project: " + str(payload.get("project", "-")),
            "",
            "Runs: " + str(summary.get("runs", 0)),
            "WS runs: " + str(summary.get("ws_runs", 0)),
            "OUTFEED runs: " + str(summary.get("outfeed_runs", 0)),
            "Stages: " + str(summary.get("stages", 0)),
            "Completed: " + str(summary.get("completed", 0)),
            "Running: " + str(summary.get("running", 0)),
            "Failed: " + str(summary.get("failed", 0)),
        ]
        QMessageBox.information(
            self, "Lightweight Snapshot", "\n".join(lines))

    def load_latest_snapshot_view(self):
        self._load_snapshot_view_from_path(self._snapshot_latest_file())

    def load_snapshot_file_view(self):
        fp, _ = QFileDialog.getOpenFileName(
            self, "Load Lightweight Snapshot", self._snapshot_dir(),
            "Snapshot Files (*.json.gz *.json);;All Files (*)")
        if not fp:
            return
        self._load_snapshot_view_from_path(fp)

    def _load_snapshot_view_from_path(self, fp):
        payload = self._read_snapshot(fp)
        if not payload:
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "Snapshot could not be loaded:\n" + str(fp))
            return
        scan_data = self._restore_scan_data_shape(
            payload.get("scan_data", {}))
        self.ws_data = scan_data.get("ws_data", {})
        self.out_data = scan_data.get("out_data", {})
        self.ir_data = scan_data.get("ir_data", {})
        state = payload.get("user_state", {})
        if isinstance(state.get("pins"), dict):
            self.user_pins = state.get("pins", {})
        if isinstance(state.get("shared_notes"), dict):
            self.global_notes = state.get("shared_notes", {})
        if isinstance(state.get("personal_notes"), dict):
            self.personal_notes = state.get("personal_notes", {})
        if isinstance(state.get("ignored_paths"), list):
            self.ignored_paths = set(state.get("ignored_paths") or [])
        cache = payload.get("lazy_cache", {})
        if isinstance(cache.get("fp_version"), dict):
            self._fp_ver_cache = cache.get("fp_version", {})
        if isinstance(cache.get("congestion_images"), dict):
            self._cong_img_cache = cache.get("congestion_images", {})
        self._merge_metrics_cache_from_snapshot(payload)
        self._last_scan_time = "snapshot " + str(payload.get("created_at", "-"))
        self.sb_scan_time.setText("     Loaded snapshot: " + str(payload.get("created_at", "-")) + "   ")
        self._rebuild_filter_dropdowns()
        self._restore_filter_state()
        self._force_default_expand = True
        QTimer.singleShot(0, self._build_tree)

    def export_latest_snapshot(self):
        src = self._snapshot_latest_file()
        if not os.path.exists(src):
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "No latest snapshot found.")
            return
        default = os.path.join(
            os.path.expanduser("~"),
            "flow_pulse_snapshot_{}.json.gz".format(
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")))
        dst, _ = QFileDialog.getSaveFileName(
            self, "Export Lightweight Snapshot", default,
            "GZip JSON (*.json.gz);;JSON (*.json);;All Files (*)")
        if not dst:
            return
        try:
            if dst.lower().endswith(".json"):
                payload = self._read_snapshot(src)
                with open(dst, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, sort_keys=True)
            else:
                shutil.copy2(src, dst)
            QMessageBox.information(
                self, "Lightweight Snapshot",
                "Snapshot exported:\n" + dst)
        except Exception as e:
            QMessageBox.warning(
                self, "Lightweight Snapshot",
                "Could not export snapshot:\n" + str(e))

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
        if self._run_history[key]:
            last = dict(self._run_history[key][-1])
            last.pop("ts", None)
            cmp_entry = dict(entry)
            cmp_entry.pop("ts", None)
            if last == cmp_entry:
                return
        # Keep last 20 entries per run
        self._run_history[key].append(entry)
        self._run_history[key] = self._run_history[key][-20:]

    def _run_regression_entry(self, run):
        return {
            "name": run.get("r_name", ""),
            "runtime": run.get("info", {}).get("runtime", ""),
            "fm_n": run.get("st_n", ""),
            "fm_u": run.get("st_u", ""),
            "vslp": run.get("vslp_status", ""),
        }

    def _compare_regression_entries(self, prev, curr):
        issues = []
        def _mins(rt):
            m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
            return int(m.group(1))*60+int(m.group(2)) if m else None
        pm, cm = _mins(prev.get("runtime","")), _mins(curr.get("runtime",""))
        if pm and cm and pm > 0 and cm > pm * 1.15:
            issues.append(
                "Runtime +{}% ({} -> {})".format(
                    int((cm-pm)/pm*100), prev.get("runtime","-"),
                    curr.get("runtime","-")))
        if ("PASS" in prev.get("fm_n","").upper()
                and "FAIL" in curr.get("fm_n","").upper()):
            issues.append("FM-NONUPF PASS->FAILS")
        if ("PASS" in prev.get("fm_u","").upper()
                and "FAIL" in curr.get("fm_u","").upper()):
            issues.append("FM-UPF PASS->FAILS")
        def _verr(v):
            m = re.search(r'Error:\s*(\d+)', v or "")
            return int(m.group(1)) if m else 0
        pe, ce = _verr(prev.get("vslp","")), _verr(curr.get("vslp",""))
        if ce > pe and pe == 0:
            issues.append("VSLP errors 0->{}".format(ce))
        elif ce > pe*1.5 and pe > 0:
            issues.append("VSLP errors {}->{}".format(pe, ce))
        return " | ".join(issues)

    def _check_regression(self, run):
        """Compare run to previous entry. Return (has_regression, message).
        All helpers are module-level -- no imports or closures per call."""
        key = f"{run['block']}|{run['r_name']}"
        history = self._run_history.get(key, [])
        if len(history) < 2:
            return False, ""
        prev = history[-2]
        curr = history[-1]
        msg = self._compare_regression_entries(prev, curr)
        return (True, msg) if msg else (False, "")


    def _cached_metrics_for_task(self, task):
        if not task:
            return None
        try:
            import workers
            key = workers._metric_cache_key(
                task.get("path", ""),
                task.get("block", ""),
                task.get("run_type", ""),
                task.get("source", "WS"),
                task.get("stage_name", None),
                task.get("stage_path", None))
            payload = workers.get_metric_cache_payload()
            entry = (payload.get("entries", {}) or {}).get(key)
            if isinstance(entry, dict) and isinstance(entry.get("metrics"), dict):
                return dict(entry.get("metrics"))
        except Exception:
            pass
        return None

    def _triplet_number(self, value, index):
        parts = str(value or "").split("/")
        if index >= len(parts):
            return None
        try:
            return float(parts[index].replace("%", "").strip())
        except Exception:
            return None

    def _congestion_number(self, value):
        nums = []
        for m in re.finditer(r'([-+]?\d+(?:\.\d+)?)\s*%', str(value or "")):
            try:
                nums.append(float(m.group(1)))
            except Exception:
                pass
        if nums:
            return sum(nums)
        return self._num(value)

    def _qor_regression_message(self, prev_metrics, curr_metrics):
        prev_metrics = prev_metrics or {}
        curr_metrics = curr_metrics or {}
        issues = []

        p_setup = prev_metrics.get("setup_r2r", prev_metrics.get("r2r_setup", "-"))
        c_setup = curr_metrics.get("setup_r2r", curr_metrics.get("r2r_setup", "-"))
        p_wns = self._triplet_number(p_setup, 0)
        c_wns = self._triplet_number(c_setup, 0)
        if p_wns is not None and c_wns is not None and c_wns < p_wns - 0.010:
            issues.append("WNS {:.4g}->{:.4g}".format(p_wns, c_wns))
        p_tns = self._triplet_number(p_setup, 1)
        c_tns = self._triplet_number(c_setup, 1)
        if p_tns is not None and c_tns is not None:
            tol = max(abs(p_tns) * 0.05, 0.001)
            if c_tns < p_tns - tol:
                issues.append("TNS {:.4g}->{:.4g}".format(p_tns, c_tns))
        p_nve = self._triplet_number(p_setup, 2)
        c_nve = self._triplet_number(c_setup, 2)
        if p_nve is not None and c_nve is not None:
            if c_nve > p_nve + max(abs(p_nve) * 0.05, 1.0):
                issues.append("NVE {:.4g}->{:.4g}".format(p_nve, c_nve))

        for key, label, pct in (
                ("std_cell_area", "Std Area", 0.02),
                ("gate_count", "Gate Count", 0.02)):
            pv = self._num(self._metric_value(prev_metrics, key))
            cv = self._num(self._metric_value(curr_metrics, key))
            if pv is not None and cv is not None and pv > 0 and cv > pv * (1.0 + pct):
                issues.append("{} +{:.1f}%".format(label, (cv - pv) / pv * 100.0))

        pc = self._congestion_number(self._metric_value(prev_metrics, "congestion"))
        cc = self._congestion_number(self._metric_value(curr_metrics, "congestion"))
        if pc is not None and cc is not None and cc > pc + 0.05:
            issues.append("Congestion {:.3g}%->{:.3g}%".format(pc, cc))
        return " | ".join(issues)

    def _collect_qor_regressions_from_cache(self):
        out = {}
        if not getattr(self, "_qor_regression_enabled", False):
            return out
        groups = {}
        try:
            for it in self._iter_tree_items():
                role = it.data(0, Qt.UserRole)
                run = it.data(0, Qt.UserRole + 10) or {}
                if role == "STAGE":
                    task = self._metric_task_from_item(it)
                    key = ("BE", task.get("block", ""), task.get("stage_name", "")) if task else None
                elif run and run.get("run_type") == "FE":
                    task = self._metric_task_from_item(it)
                    key = ("FE", run.get("block", ""), run.get("rtl", ""))
                else:
                    continue
                metrics = self._cached_metrics_for_task(task)
                if not task or not metrics:
                    continue
                sort_key = it.data(0, Qt.UserRole + 42)
                groups.setdefault(key, []).append((sort_key, it, metrics))
            for key, rows in groups.items():
                rows.sort(key=lambda x: x[0] if x[0] is not None else (9999, 99, 99, 99, 99))
                prev_metrics = None
                for sort_key, item, metrics in rows:
                    if prev_metrics is not None:
                        msg = self._qor_regression_message(prev_metrics, metrics)
                        if msg:
                            out[id(item)] = msg
                    prev_metrics = metrics
        except Exception:
            return out
        return out

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
        base = "Flow Pulse | Beta 1"
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
            stage_path = run_path
        else:
            stage_name  = None
            run_type    = "FE"
            actual_path = run_path
            stage_path = None

        if self._worker_is_running(getattr(self, "_metric_worker", None)):
            self.status_bar.showMessage("Stopping previous QoR extraction...", 3000)
            if not self._stop_worker_attr("_metric_worker"):
                QMessageBox.information(
                    self, "QoR Summary",
                    "Previous QoR extraction is still running. Please try again in a moment.")
                return

        # Show progress indicator in status bar
        self.status_bar.showMessage(
            f"Extracting QoR metrics for {run_name}...")
        self.setEnabled(False)

        worker = MetricWorker(
            actual_path, item.data(0, Qt.UserRole + 2) or "",
            run_type, source, stage_name, stage_path)
        self._metric_worker = worker
        self._metric_item_name = run_name
        self._metric_dark      = dark
        worker.finished.connect(self._on_metric_done)
        worker.finished.connect(
            lambda *_args, ww=worker:
            self._clear_worker_attr_if_current("_metric_worker", ww))
        worker.start()

    def _on_metric_done(self, metrics):
        """Called when MetricWorker finishes -- show the summary dialog."""
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_metric_worker", None):
            return
        self.setEnabled(True)
        self.status_bar.clearMessage()

        if metrics.get("_cancelled"):
            return

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

    def _set_item_time_data(self, item, start_raw, end_raw):
        item.setData(0, Qt.UserRole + 40, start_raw)
        item.setData(0, Qt.UserRole + 41, end_raw)
        try:
            item.setData(0, Qt.UserRole + 42,
                         CustomTreeItem._date_sort_key(start_raw))
            item.setData(0, Qt.UserRole + 43,
                         CustomTreeItem._date_sort_key(end_raw))
        except Exception:
            item.setData(0, Qt.UserRole + 42, None)
            item.setData(0, Qt.UserRole + 43, None)

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
    def _find_tree_item_by_path(self, path, node_type=None, name=None):
        """Resolve a current tree item by stable path/name; never keep old item refs."""
        if not path:
            return None
        for item in self._iter_tree_items():
            try:
                if node_type and item.data(0, Qt.UserRole) != node_type:
                    continue
                if name and item.text(0) != name:
                    continue
                run = item.data(0, Qt.UserRole + 10) or item.data(0, Qt.UserRole + 11)
                if run and run.get("path") == path:
                    return item
                if item.text(15) == path:
                    return item
            except RuntimeError:
                continue
            except Exception:
                continue
        return None

    def _qor_regression_key_for_item(self, item):
        try:
            run = item.data(0, Qt.UserRole + 10)
            if run:
                return (run.get("path", ""), item.data(0, Qt.UserRole), item.text(0))
            return (item.text(15), item.data(0, Qt.UserRole), item.text(0))
        except Exception:
            return ("", "", "")

    def _run_closure_pass(self):
        """Apply optional scorecard/regression annotations in small GUI-safe chunks."""
        closure_on = bool(getattr(self, "_closure_enabled", False))
        status_reg_on = bool(getattr(self, "_status_regression_enabled", False))
        qor_reg_on = bool(getattr(self, "_qor_regression_enabled", False))
        if not (closure_on or status_reg_on or qor_reg_on):
            return

        self._closure_pass_token = getattr(self, "_closure_pass_token", 0) + 1
        token = self._closure_pass_token
        _UR = Qt.UserRole
        _UR10 = Qt.UserRole + 10
        entries = []
        sibling_regressions = {}

        try:
            for it in self._iter_tree_items():
                try:
                    nt = it.data(0, _UR)
                    run = it.data(0, _UR10)
                    path = run.get("path", "") if run else ""
                    if not path:
                        path = it.text(15)
                    if path and nt not in ("BLOCK", "MILESTONE", "RTL",
                                           "IGNORED_ROOT", "STANDALONE_ROOT",
                                           "__PLACEHOLDER__"):
                        entries.append((path, nt, it.text(0)))
                except RuntimeError:
                    continue
                except Exception:
                    continue
        except Exception:
            entries = []

        if status_reg_on:
            try:
                groups = {}
                for path, nt, name in entries:
                    item = self._find_tree_item_by_path(path, nt, name)
                    if item is None:
                        continue
                    run = item.data(0, _UR10)
                    if not run or run.get("run_type") != "FE" or not run.get("is_comp"):
                        continue
                    key = (run.get("block", ""), run.get("rtl", ""))
                    groups.setdefault(key, []).append((path, run))
                for key, vals in groups.items():
                    vals.sort(key=lambda pair:
                              self._parse_dashboard_time(pair[1].get("info", {}).get("start", ""))
                              or datetime.datetime.max)
                    prev_run = None
                    for path, run in vals:
                        if prev_run:
                            msg = self._compare_regression_entries(
                                self._run_regression_entry(prev_run),
                                self._run_regression_entry(run))
                            if msg:
                                sibling_regressions[path] = msg
                        prev_run = run
            except Exception:
                sibling_regressions = {}

        qor_regressions = {}
        if qor_reg_on:
            try:
                raw = self._collect_qor_regressions_from_cache()
                for item_id, msg in raw.items():
                    for it in self._iter_tree_items():
                        if id(it) == item_id:
                            qor_regressions[self._qor_regression_key_for_item(it)] = msg
                            break
            except Exception:
                qor_regressions = {}

        def _annotate_regression(item, msg, label):
            if not msg:
                return
            old = item.toolTip(0) or ""
            marker = "[{}]".format(label)
            if marker not in old:
                item.setToolTip(0, old + "\n{} {}".format(marker, msg))
            item.setForeground(0, QColor("#f57c00"))
            item.setData(0, Qt.UserRole + 30, msg)

        index = [0]
        batch_size = 80

        def _process_batch():
            if token != getattr(self, "_closure_pass_token", 0):
                return
            if getattr(self, "_building_tree", False):
                return
            end_i = min(len(entries), index[0] + batch_size)
            while index[0] < end_i:
                path, nt, name = entries[index[0]]
                index[0] += 1
                item = self._find_tree_item_by_path(path, nt, name)
                if item is None:
                    continue
                try:
                    run = item.data(0, _UR10)
                    if closure_on and run and run.get("run_type") == "FE":
                        self._update_closure_on_item(item)
                    if status_reg_on and run and run.get("run_type") == "FE" and run.get("is_comp"):
                        has_reg, msg = self._check_regression(run)
                        if not has_reg:
                            msg = sibling_regressions.get(run.get("path", ""), "")
                            has_reg = bool(msg)
                        if has_reg:
                            _annotate_regression(item, msg, "REGRESSION")
                    if qor_reg_on:
                        msg = qor_regressions.get((path, nt, name), "")
                        if msg:
                            _annotate_regression(item, msg, "QOR REGRESSION")
                except RuntimeError:
                    continue
                except Exception:
                    continue
            if index[0] < len(entries):
                QTimer.singleShot(0, _process_batch)

        QTimer.singleShot(0, _process_batch)

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
        toolbar_layout = QHBoxLayout()
        toolbar_layout.setContentsMargins(0, 0, 0, 0)
        toolbar_layout.setSpacing(6)
        top_layout = toolbar_layout
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
            "Completed Only", "Running Only", "Failed Only", "Today's Runs",
            "Pinned Only", "Selected Only"])
        self.view_combo.setFixedWidth(120)
        self._last_view_preset = self.view_combo.currentText()
        self.view_combo.currentIndexChanged.connect(self._on_view_changed)
        top_layout.addWidget(self.view_combo)

        self.search = QLineEdit()
        self.search.setPlaceholderText(
            "Search runs, blocks, status, runtime...  [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
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

        top_layout.addStretch(1)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setToolTip(
            "Quick refresh checks only in-progress FE runs. Use Utilities > Tree View > Full Rescan to rediscover all runs.")
        self.refresh_btn.clicked.connect(self.start_quick_refresh)
        top_layout.addWidget(self.refresh_btn)

        self.auto_combo = QComboBox()
        self.auto_combo.addItems(["Off", "1 Min", "5 Min", "10 Min"])
        self.auto_combo.setFixedWidth(75)
        self.auto_combo.currentIndexChanged.connect(self.on_auto_refresh_changed)
        top_layout.addWidget(self.auto_combo)

        self._add_separator(top_layout)

        # Utilities menu (renamed from Actions)
        self.actions_btn  = QPushButton("Utilities")
        self.actions_menu = QMenu(self)

        view_menu = self.actions_menu.addMenu("Tree View")
        view_menu.addAction("Fit Columns", self.fit_all_columns)
        view_menu.addAction("Full Rescan", self.start_fs_scan)
        view_menu.addAction("Reset View to Default", self.reset_view_defaults)
        view_menu.addAction("Expand All", self.safe_expand_all)
        view_menu.addAction("Collapse All", self.safe_collapse_all)
        view_menu.addAction("Deselect All Checked Runs",
                            self.deselect_all_checked_runs)

        export_menu = self.actions_menu.addMenu("Export / Mail")
        export_menu.addAction("Export to CSV", self.export_csv)
        mail_menu = export_menu.addMenu("Send Mail...")
        mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        mail_menu.addAction("Send Compare QoR Mail",        self.send_qor_mail_action)
        mail_menu.addAction("Send Custom Mail",             self.send_custom_mail_action)
        export_menu.addAction("Failed Runs Digest", self.show_failed_digest)

        analysis_menu = self.actions_menu.addMenu("Run Analysis")
        analysis_menu.addAction("Compare QoR", self.run_qor_comparison)
        analysis_menu.addAction("Compare Selected Runs", self.show_run_diff)
        analysis_menu.addAction("RoR Metric Diff", self.show_ror_metric_diff)
        analysis_menu.addAction("Golden Benchmark", self.show_golden_benchmark)
        analysis_menu.addAction("App Options Diff", self.show_app_options_diff)
        self.fe_hover_metrics_act = analysis_menu.addAction("Enable FE Hover Metrics")
        self.fe_hover_metrics_act.setCheckable(True)
        self.fe_hover_metrics_act.setChecked(self.enable_fe_hover_metrics)
        self.fe_hover_metrics_act.triggered.connect(self.toggle_fe_hover_metrics)

        summary_menu = self.actions_menu.addMenu("Summaries / Timeline")
        summary_menu.addAction("FE Block Summary Table", self.open_block_summary)
        summary_menu.addAction("BE Stage Summary Table",
                               self.show_be_stage_summary_table)
        summary_menu.addAction("Timeline Overview",
                               self.show_selected_timeline_overview)
        summary_menu.addAction("Analytics / Charts", self.show_analytics)

        filt_menu = self.actions_menu.addMenu("Config / Filters")
        filt_menu.addAction("Load Run Filter Config...", self.load_filter_config)
        self.ignore_run_filter_act = filt_menu.addAction("Ignore Run Filter")
        self.ignore_run_filter_act.setCheckable(True)
        self.ignore_run_filter_act.triggered.connect(self.toggle_ignore_run_filter)
        filt_menu.addAction("Clear Run Filter Config",   self.clear_filter_config)
        filt_menu.addAction("Generate Sample Config",    self.generate_sample_config)
        filt_menu.addAction("Add Checked Runs to Active Filter Config",
                            self.add_checked_runs_to_filter_config)

        resource_menu = self.actions_menu.addMenu("Storage / Team")
        resource_menu.addAction("Calculate All Run Sizes",
                                self.calculate_all_sizes)
        resource_menu.addAction("Disk Space", self.open_disk_usage)
        resource_menu.addAction("Team Workload View", self.show_team_workload)
        resource_menu.addAction("Metric Cache Status", self.show_metric_cache_status)
        resource_menu.addAction("Clear Metric Cache", self.clear_metric_cache)
        snapshot_menu = resource_menu.addMenu("Lightweight Snapshots")
        snapshot_menu.addAction("Save Snapshot Now", self.save_snapshot_now)
        snapshot_menu.addAction("Snapshot Status", self.show_snapshot_status)
        snapshot_menu.addAction("Load Last Snapshot View", self.load_latest_snapshot_view)
        snapshot_menu.addAction("Load Snapshot File...", self.load_snapshot_file_view)
        snapshot_menu.addAction("Export Last Snapshot...", self.export_latest_snapshot)

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
        self.mode_combo.setMinimumWidth(112)
        self.mode_combo.setToolTip(
            "Column view preset  (keys: 1=Compact  2=Standard  3=Full)")
        self.mode_combo.currentIndexChanged.connect(
            lambda i: self._set_col_preset(
                {"Standard": 2, "Compact": 1, "Full": 3}.get(
                    self.mode_combo.currentText(), 2)))
        top_layout.addWidget(self.mode_combo)

        self._add_separator(top_layout)

        # Notes toggle button
        self.notes_toggle_btn = QPushButton("Notes  >")
        self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock)
        top_layout.addWidget(self.notes_toggle_btn)

        root_layout.addLayout(toolbar_layout)

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
        self.fe_cong_panel = QGroupBox("Run Images")
        fe_cong_layout = QVBoxLayout(self.fe_cong_panel)
        fe_cong_layout.setContentsMargins(6, 6, 6, 6)
        fe_cong_layout.setSpacing(4)
        self.fe_fp_ver_lbl = QLabel("FP_VER: -")
        self.fe_fp_ver_lbl.setWordWrap(True)
        self.fe_fp_ver_lbl.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.fe_fp_ver_lbl.setStyleSheet("font-size: 10px; font-weight: bold; color: #1565c0;")
        self.fe_cong_img_lbl = QLabel("No FE congestion image")
        self.fe_cong_img_lbl.setAlignment(Qt.AlignCenter)
        self.fe_cong_img_lbl.setFixedSize(296, 180)
        self.fe_cong_img_lbl.setCursor(Qt.PointingHandCursor)
        self.fe_cong_img_lbl.setStyleSheet("border: 1px solid #9e9e9e; background: #f5f5f5; color: #757575; font-size: 10px;")
        self.fe_cong_img_lbl.mousePressEvent = lambda e: self._open_fe_congestion_fullscreen()
        self._current_cong_img_path = None
        self.stage_map_links = {}
        fe_cong_layout.addWidget(self.fe_fp_ver_lbl)
        fe_cong_layout.addWidget(self.fe_cong_img_lbl)
        for _label in ("Congestion Map", "Pin Map", "Cell Density Map", "Shorts Map"):
            _btn = QPushButton(_label)
            _btn.setObjectName("linkBtn")
            _btn.setCursor(Qt.PointingHandCursor)
            _btn.setVisible(False)
            _btn.clicked.connect(lambda _, name=_label: self._open_stage_map_link(name))
            self.stage_map_links[_label] = _btn
            fe_cong_layout.addWidget(_btn)
        self.fe_cong_panel.setVisible(False)
        left_layout.addWidget(self.fe_cong_panel, 0)
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
        self.tree.setMouseTracking(True)
        self.tree.viewport().setMouseTracking(True)
        self.tree.itemEntered.connect(self._on_tree_item_hovered)

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
        self.stage_metric_box = QGroupBox("BE Stage Metrics")
        stage_metric_layout = QVBoxLayout(self.stage_metric_box)
        stage_metric_layout.setContentsMargins(6, 6, 6, 6)
        self.stage_metric_text = QTextEdit()
        self.stage_metric_text.setReadOnly(True)
        self.stage_metric_text.setMaximumHeight(230)
        self.stage_metric_text.setPlaceholderText("Select a BE stage to load QoR metrics.")
        stage_metric_layout.addWidget(self.stage_metric_text)
        self.stage_metric_box.setVisible(False)
        self.personal_note_box = QGroupBox("Personal Note")
        self.personal_note_box.setCheckable(True)
        self.personal_note_box.setChecked(False)
        personal_note_layout = QVBoxLayout(self.personal_note_box)
        personal_note_layout.setContentsMargins(6, 6, 6, 6)
        self.ins_note = QTextEdit()
        self.ins_note.setPlaceholderText(
            "Personal note visible only to your user account.")
        self.ins_note.setMaximumHeight(64)
        self.ins_save_btn = QPushButton("Save Personal Note")
        self.ins_save_btn.clicked.connect(self.save_inspector_note)
        personal_note_layout.addWidget(self.ins_note)
        personal_note_layout.addWidget(self.ins_save_btn)
        self.personal_note_box.toggled.connect(self._toggle_personal_note_box)
        self.shared_note_history = QTextEdit()
        self.shared_note_history.setReadOnly(True)
        self.shared_note_history.setPlaceholderText("No shared notes for this item.")
        self.shared_note_history.setMinimumHeight(240)
        self.shared_note_history.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.shared_note_input = QTextEdit()
        self.shared_note_input.setPlaceholderText(
            "Add shared note visible to all dashboard users.")
        self.shared_note_input.setMaximumHeight(70)
        self.shared_save_btn = QPushButton("Add Shared Note")
        self.shared_save_btn.clicked.connect(self.save_shared_inspector_note)
        ins_layout.addWidget(self.ins_lbl)
        ins_layout.addWidget(self.stage_metric_box)
        ins_layout.addWidget(self.personal_note_box)
        ins_layout.addWidget(QLabel("<b>Shared Notes:</b>"))
        ins_layout.addWidget(self.shared_note_history, 1)
        ins_layout.addWidget(self.shared_note_input)
        ins_layout.addWidget(self.shared_save_btn)

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
        self.sb_scan_time = QLabel("")
        self.sb_config   = QLabel("Config: None")

        self._make_status_label_clickable(
            self.sb_total, "Click to show all runs",
            lambda: self.view_combo.setCurrentText("All Runs"))
        self._make_status_label_clickable(
            self.sb_complete, "Click to show completed FE runs",
            lambda: self.view_combo.setCurrentText("Completed Only"))
        self._make_status_label_clickable(
            self.sb_running, "Click to show running FE runs",
            lambda: self.view_combo.setCurrentText("Running Only"))
        self._make_status_label_clickable(
            self.sb_selected, "Click to show only selected (checked) runs",
            self._toggle_selected_only)
        self._make_status_label_clickable(
            self.sb_scan_time, "Click to quick refresh in-progress runs",
            self.start_quick_refresh)
        self._make_status_label_clickable(
            self.sb_config, "Click to load or open active filter config",
            self._on_status_config_clicked)

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

    def _make_status_label_clickable(self, label, tooltip, callback):
        label.setObjectName("statusLink")
        label.setCursor(Qt.PointingHandCursor)
        label.setToolTip(tooltip)
        label.mousePressEvent = lambda e, cb=callback: cb()

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
            self.notes_toggle_btn.setText("Notes  >")
        else:
            self.inspector_dock.show()
            self.notes_toggle_btn.setText("<  Notes")

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
                _load_all_lazy(child)
        _load_all_lazy(root)
        self.tree.setUpdatesEnabled(True)
        self.tree.expandAll()
        self.tree.blockSignals(False)
        self.tree.resizeColumnToContents(0)

    def safe_collapse_all(self):
        self.tree.collapseAll()

    def reset_view_defaults(self):
        if QMessageBox.question(
                self, "Reset View",
                "Reset view/filter settings to the first-open dashboard layout?\n\n"
                "Notes, pins, snapshots and metric cache will not be deleted.",
                QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        self.search.clear()
        self.active_col_filters.clear()
        self.ignored_paths.clear()
        self._checked_paths.clear()
        self.run_filter_config = None
        self.current_config_path = None
        self.ignore_run_filter = False
        self.hide_block_nodes = False
        self.show_relative_time = False
        self.convert_to_ist = False
        self._tree_sort_mode = "Start Date Old->New"
        self._last_view_preset = "All Runs"
        self.is_dark_mode = False
        self.use_custom_colors = False
        self.custom_bg_color = "#2b2d30"
        self.custom_fg_color = "#dfe1e5"
        self.custom_sel_color = "#2f65ca"
        self.row_spacing = 2
        self.gate_count_unit_area = 0.2419
        self._closure_enabled = False
        self._status_regression_enabled = False
        self._qor_regression_enabled = False
        self.enable_fe_hover_metrics = False
        self._clear_fe_hover_metric_tooltips()
        try:
            self.auto_refresh_timer.stop()
        except Exception:
            pass
        if hasattr(self, "ignore_run_filter_act"):
            self.ignore_run_filter_act.setChecked(False)
        if hasattr(self, "fe_hover_metrics_act"):
            self.fe_hover_metrics_act.setChecked(False)
        for combo, text in ((getattr(self, "src_combo", None), "ALL"),
                            (getattr(self, "view_combo", None), "All Runs"),
                            (getattr(self, "auto_combo", None), "Off")):
            if combo:
                combo.blockSignals(True)
                idx = combo.findText(text)
                if idx >= 0:
                    combo.setCurrentIndex(idx)
                combo.blockSignals(False)
        if hasattr(self, "rel_combo"):
            self.rel_combo.blockSignals(True)
            idx = self.rel_combo.findText("[ SHOW ALL ]")
            if idx >= 0:
                self.rel_combo.setCurrentIndex(idx)
            self.rel_combo.blockSignals(False)
        if not prefs.has_section('UI'):
            prefs.add_section('UI')
        for key, val in (
                ('last_source', 'ALL'),
                ('last_rtl', '[ SHOW ALL ]'),
                ('last_view', 'All Runs'),
                ('last_sort', 'Start Date Old->New'),
                ('last_search', ''),
                ('last_auto', 'Off'),
                ('hide_block_nodes', 'false'),
                ('show_relative_time', 'false'),
                ('convert_to_ist', 'false'),
                ('closure_enabled', 'false'),
                ('status_regression_enabled', 'false'),
                ('qor_regression_enabled', 'false'),
                ('enable_fe_hover_metrics', 'false'),
                ('gate_count_unit_area', '0.241900')):
            prefs.set('UI', key, val)
        try:
            _write_config_atomic(prefs, USER_PREFS_FILE)
        except Exception:
            pass
        self.apply_theme_and_spacing()
        self._set_col_preset(2)
        QTimer.singleShot(0, self._build_tree)

    def _ensure_ign_root(self, root):
        for i in range(root.childCount()):
            if root.child(i).data(0, Qt.UserRole) == "IGNORED_ROOT":
                return root.child(i)
        return self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

    def _expand_to_rtl_level(self):
        """First-open layout: show blocks and milestone labels only."""
        self.tree.setUpdatesEnabled(False)
        def _expand(node):
            for i in range(node.childCount()):
                child = node.child(i)
                nt = child.data(0, Qt.UserRole)
                if nt == "STANDALONE_ROOT":
                    child.setExpanded(False)
                    continue
                if nt in ("BLOCK", "IGNORED_ROOT"):
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
        QShortcut(QKeySequence("Ctrl+R"), self,      self.start_quick_refresh)
        QShortcut(QKeySequence("Ctrl+Shift+R"), self, self.start_fs_scan)
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
        cached = getattr(self, "_visible_run_item_cache", None)
        if cached is not None:
            return [it for it in cached if it is not None and not it.isHidden()]
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
        self._visible_run_item_cache = list(items)
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
    def _visible_runs_for_status(self):
        out = []
        try:
            for item in self._iter_tree_items():
                run = item.data(0, Qt.UserRole + 10)
                if run and not item.isHidden():
                    out.append(run)
        except Exception:
            pass
        return out

    def _update_status_bar(self, runs=None):
        if runs is None:
            runs = self._visible_runs_for_status()
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
        self._update_status_bar()

    def _on_tree_item_hovered(self, item, column):
        """Optional FE hover hook. Keep it lightweight; no report parsing on hover."""
        if not getattr(self, 'enable_fe_hover_metrics', False):
            return
        if not item:
            return
        try:
            role = item.data(0, Qt.UserRole)
            if role in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT", "STAGE", "__PLACEHOLDER__"):
                return
            run = item.data(0, Qt.UserRole + 10) or {}
            if run.get("run_type") != "FE":
                return
            path = item.text(15)
            cached = self._hover_metric_cache.get(path, {}) if path else {}
            if cached:
                self._set_fe_hover_tooltip(item, self._format_fe_hover_metrics(cached))
                return
            self._set_fe_hover_tooltip(item, ["Loading FE metrics..."])
            if not path or path == "N/A":
                return
            if self._worker_is_running(getattr(self, "_hover_metric_worker", None)):
                return
            worker = MetricWorker(
                path, run.get("block", "") or item.data(0, Qt.UserRole + 2) or "",
                "FE", run.get("source", item.text(2) or "WS"),
                None, None)
            self._hover_metric_worker = worker
            self._hover_metric_path = path
            worker.finished.connect(
                lambda metrics, p=path, it=item:
                self._on_fe_hover_metric_done(p, it, metrics))
            worker.finished.connect(worker.deleteLater)
            worker.start()
        except Exception:
            return

    def _show_utilities_menu(self):
        try:
            pos = self.actions_btn.mapToGlobal(
                QPoint(0, self.actions_btn.height()))
            self.actions_menu.exec_(pos)
        except Exception:
            pass

    def toggle_fe_hover_metrics(self, checked):
        self.enable_fe_hover_metrics = bool(checked)
        if hasattr(self, "fe_hover_metrics_act"):
            self.fe_hover_metrics_act.setChecked(self.enable_fe_hover_metrics)
        if not prefs.has_section('UI'):
            prefs.add_section('UI')
        prefs.set('UI', 'enable_fe_hover_metrics',
                  'true' if self.enable_fe_hover_metrics else 'false')
        try:
            _write_config_atomic(prefs, USER_PREFS_FILE)
        except Exception:
            pass
        if not self.enable_fe_hover_metrics:
            self._clear_fe_hover_metric_tooltips()

    def _format_fe_hover_metrics(self, metrics):
        if not isinstance(metrics, dict) or not metrics:
            return ["FE metrics unavailable."]
        area = metrics.get("area", {})
        if not isinstance(area, dict):
            area = {}
        vth = metrics.get("vth", {})
        if not isinstance(vth, dict):
            vth = {}
        gc = self._metric_value(metrics, "gate_count")
        if gc in ("", "-", "N/A"):
            gc = self._metric_value(metrics, "gc")
        inst = area.get("instance_count", "-")
        if inst in ("", "-", "N/A"):
            inst = self._metric_value(metrics, "instance_count")
        vt_area = vth.get("lvt_rvt_hvt_area", "-")
        if vt_area in ("", "-", "N/A"):
            vt_area = self._metric_value(metrics, "vt_area")
        lines = [
            "WNS: " + str(self._metric_value(metrics, "wns")),
            "Gate Count: " + str(gc),
            "Instance Count: " + str(inst),
            "VT Area %: " + str(vt_area),
            "Logic Depth: " + str(metrics.get("logic_depth", "-")),
        ]
        std_area = area.get("std_cell_area", "-")
        if std_area not in ("", "-", "N/A"):
            lines.append("Std Cell Area: " + str(std_area))
        return lines

    def _set_fe_hover_tooltip(self, item, lines):
        marker = "\n[FE Hover Metrics]"
        try:
            base = self._strip_tooltip_block(item.toolTip(0) or item.text(0), marker)
            item.setToolTip(0, base + marker + "\n" + "\n".join(lines))
        except Exception:
            pass

    def _on_fe_hover_metric_done(self, path, item, metrics):
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_hover_metric_worker", None):
            return
        self._hover_metric_worker = None
        self._hover_metric_path = ""
        if isinstance(metrics, dict) and not metrics.get("_error"):
            self._hover_metric_cache[path] = metrics
        lines = self._format_fe_hover_metrics(metrics)
        try:
            if item and item.text(15) == path:
                self._set_fe_hover_tooltip(item, lines)
        except RuntimeError:
            pass
        except Exception:
            pass

    def _clear_fe_hover_metric_tooltips(self):
        marker = "\n[FE Hover Metrics]"
        try:
            def walk(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    tip = c.toolTip(0) or ""
                    if marker in tip:
                        c.setToolTip(0, tip.split(marker, 1)[0])
                    walk(c)
            walk(self.tree.invisibleRootItem())
        except Exception:
            pass

    def _hide_stage_map_links(self):
        for btn in getattr(self, "stage_map_links", {}).values():
            btn.setVisible(False)
            btn.setEnabled(False)
            btn.setProperty("_path", "")

    def _hide_run_image_panel(self):
        self._fe_cong_request_token += 1
        self._stage_screenshot_request_token += 1
        self._current_cong_img_path = None
        self._hide_stage_map_links()
        self.fe_cong_img_lbl.setPixmap(QPixmap())
        self.fe_cong_panel.setVisible(False)

    def _keep_running_workers(self, workers):
        alive = []
        for worker in list(workers or []):
            try:
                if worker and worker.isRunning():
                    alive.append(worker)
            except RuntimeError:
                # Qt may already have deleted the wrapped C++ QThread object.
                pass
            except Exception:
                pass
        return alive

    def _worker_is_running(self, worker):
        try:
            return bool(worker and worker.isRunning())
        except RuntimeError:
            return False
        except Exception:
            return False

    def _hide_stage_metric_panel(self):
        self._stage_metric_request_token += 1
        if hasattr(self, "stage_metric_box"):
            self.stage_metric_text.clear()
            self.stage_metric_box.setVisible(False)

    def _format_stage_metrics(self, metrics):
        metrics = metrics if isinstance(metrics, dict) else {}
        rows = [
            ("Runtime", metrics.get("runtime")),
            ("Setup R2R-WNS/TNS/NVE", metrics.get("setup_r2r")),
            ("Setup Total-WNS/TNS/NVE", metrics.get("setup_total")),
            ("Hold R2R-WNS/TNS/NVE", metrics.get("hold_r2r")),
            ("Hold Total-WNS/TNS/NVE", metrics.get("hold_total")),
            ("Hold-WNS/TNS/NVE", metrics.get("hold_all")),
            ("Congestion", metrics.get("congestion")),
            ("Std Cell Count/Area", metrics.get("std_cell_count_area")),
            ("Gate Count", metrics.get("gate_count")),
            ("Std Cell Only Util%", metrics.get("std_cell_only_util")),
            ("Total Util%", metrics.get("total_util")),
            ("Skew/Latency", metrics.get("skew_latency")),
            ("Clock Repeater Count/Area", metrics.get("clock_repeater_count_area")),
        ]
        vt_label = metrics.get("vt_label")
        if vt_label:
            rows.append((vt_label + " Inst%", metrics.get("vt_inst")))
            rows.append((vt_label + " Area%", metrics.get("vt_area")))
        lines = []
        for label, value in rows:
            if value and value != "-":
                lines.append("{}: {}".format(label, value))
        rpt_dir = metrics.get("report_dir")
        if rpt_dir and rpt_dir != "-":
            lines.append("")
            lines.append("Report dir: {}".format(rpt_dir))
        if metrics.get("error"):
            lines.append("")
            lines.append("Parser warning: {}".format(metrics.get("error")))
        return "\n".join(lines) if lines else "No BE stage metric reports found."

    def _strip_tooltip_block(self, tip, marker):
        tip = tip or ""
        if marker in tip:
            return tip.split(marker, 1)[0].rstrip()
        loose = "[BE Stage Metrics]"
        if loose in tip:
            return tip.split(loose, 1)[0].rstrip()
        return tip

    def _update_stage_metric_panel(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE" or not item.parent():
            self._hide_stage_metric_panel()
            return
        parent = item.parent()
        be_path = parent.text(15)
        stage_path = item.text(15)
        stage_name = item.text(0)
        block = item.data(0, Qt.UserRole + 2) or parent.data(0, Qt.UserRole + 2) or ""
        runtime = item.text(12) or "-"
        key = (be_path or "", stage_path or "", stage_name or "", block or "", runtime or "")
        self.stage_metric_box.setVisible(True)
        if self._stage_metric_last_key != key:
            self.stage_metric_text.setPlainText("Loading BE stage metrics...")
        self._stage_metric_last_key = key
        self._stage_metric_request_token += 1
        token = self._stage_metric_request_token
        if key in self._stage_metric_cache:
            self._apply_stage_metric_lookup(token, item, self._stage_metric_cache.get(key, {}))
            return
        QTimer.singleShot(
            120,
            lambda t=token, bp=be_path, sp=stage_path, sn=stage_name, b=block, rt=runtime, k=key:
                self._start_stage_metric_lookup(t, bp, sp, sn, b, rt, k))

    def _start_stage_metric_lookup(self, token, be_path, stage_path, stage_name, block, runtime, key):
        if token != self._stage_metric_request_token:
            return
        worker = StageMetricLookupWorker(
            token, be_path, stage_path, stage_name, block, runtime,
            getattr(self, "gate_count_unit_area", 0.2419))
        worker._cache_key = key
        self._stage_metric_workers.append(worker)
        worker.finished.connect(self._on_stage_metric_lookup_done)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _on_stage_metric_lookup_done(self, token, be_path, stage_name, metrics):
        cache_key = None
        sender = self.sender()
        try:
            cache_key = getattr(sender, "_cache_key", None)
        except Exception:
            cache_key = None
        if cache_key:
            self._stage_metric_cache[cache_key] = metrics if isinstance(metrics, dict) else {}
        self._stage_metric_workers = self._keep_running_workers(
            self._stage_metric_workers)
        if token != self._stage_metric_request_token:
            return
        sel = self.tree.selectedItems()
        item = sel[0] if sel else None
        self._apply_stage_metric_lookup(token, item, metrics)

    def _apply_stage_metric_lookup(self, token, item, metrics):
        if token != self._stage_metric_request_token:
            return
        if not item or item.data(0, Qt.UserRole) != "STAGE":
            self._hide_stage_metric_panel()
            return
        text = self._format_stage_metrics(metrics)
        self.stage_metric_text.setPlainText(text)
        try:
            marker = "\n\n[BE Stage Metrics]\n"
            base_tip = self._strip_tooltip_block(item.toolTip(0) or item.text(0), marker)
            item.setToolTip(0, base_tip + "\n\n[BE Stage Metrics]\n" + text)
        except Exception:
            pass

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
            self._hide_run_image_panel()
            self._hide_stage_metric_panel()
            self.ins_note.clear()
            self.ins_note.setEnabled(False)
            self.ins_save_btn.setEnabled(False)
            self.shared_note_history.clear()
            self.shared_note_input.clear()
            return

        item     = sel[0]
        run_name = item.text(0)
        rtl      = item.text(1)
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl   = item.data(0, Qt.UserRole) == "RTL"
        path     = item.text(15)
        run_data = None

        self.meta_path.setText(path)
        log_val = item.text(16)
        if log_val and log_val not in ("N/A", ""):
            # Do not stat NFS on selection; open action validates the path.
            self.meta_log.setText(log_val)
            self.meta_log.setStyleSheet("")
            self.meta_log.setToolTip(log_val)
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

        self.ins_note.setPlainText(self.personal_notes.get(self._current_note_id, ""))
        self.shared_note_history.setPlainText(self._shared_notes_text(self._current_note_id))
        self.shared_note_input.clear()
        if is_stage:
            self._update_stage_screenshot_panel(item)
            self._update_stage_metric_panel(item)
        else:
            self._hide_stage_metric_panel()
            self._update_fe_congestion_panel(item, run_data if not is_rtl else None)

        # FE error count is loaded lazily on first selection and then cached
        # on the item, so the error button is visible without scan-time cost.
        if (len(sel) == 1 and not is_stage and path and path != "N/A"
                and run_data and run_data.get("run_type") == "FE"):
            err_count = item.data(0, Qt.UserRole + 12)
            err_path  = os.path.join(path, "logs", "compile_opt.error.log")
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
            else:
                err_count = self._count_compile_error_lines(err_path)
                item.setData(0, Qt.UserRole + 12, err_count)
                self.current_error_log_path = err_path
                color = "#388e3c" if err_count == 0 else "#d32f2f"
                if self.is_dark_mode:
                    color = "#81c784" if err_count == 0 else "#e57373"
                self.fe_error_btn.setStyleSheet(
                    f"QPushButton#errorLinkBtn {{ border: none; "
                    f"background: transparent; color: {color}; "
                    f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                    f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                self.fe_error_btn.setText(f"compile_opt errors: {err_count}")
                self.fe_error_btn.setVisible(True)

    def _find_fe_congestion_image(self, run_path, block):
        key = (run_path or "", block or "")
        if key in self._cong_img_cache:
            return self._cong_img_cache[key]
        hit = None
        try:
            rpt_dir = os.path.join(run_path, "reports")
            if os.path.isdir(rpt_dir):
                pats = []
                if block:
                    pats.append("congestion.window.{}.*.jpg".format(block))
                    pats.append("congestion.window.{}.*.jpeg".format(block))
                pats.extend(["congestion.window.*.jpg", "congestion.window.*.jpeg"])
                matches = []
                for name in os.listdir(rpt_dir):
                    for pat in pats:
                        if fnmatch.fnmatch(name, pat):
                            matches.append(os.path.join(rpt_dir, name))
                            break
                if matches:
                    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                    hit = matches[0]
        except Exception:
            hit = None
        self._cong_img_cache[key] = hit
        return hit

    def _extract_fe_fp_ver(self, run_path):
        if run_path in self._fp_ver_cache:
            return self._fp_ver_cache[run_path]
        val = "-"
        log_path = os.path.join(run_path or "", "logs", "compile_opt.log")
        try:
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        m = re.search(r'^\s*INFO\s*:\s*FP_VER\s*[-:]\s*(\S.*)$', line)
                        if m:
                            cand = m.group(1).strip().strip('"')
                            if '$' in cand or cand.upper() in ('FP_VER', '$FP_VER'):
                                continue
                            val = cand
                            break
        except Exception:
            val = "-"
        self._fp_ver_cache[run_path] = val
        return val

    def _update_fe_congestion_panel(self, item, run_data):
        is_fe = bool(run_data and run_data.get("run_type") == "FE")
        run_path = item.text(15) if item else ""
        if not is_fe or not run_path or run_path == "N/A":
            self._hide_run_image_panel()
            return
        self._stage_screenshot_request_token += 1
        self._hide_stage_map_links()
        self.fe_cong_panel.setTitle("FE Congestion Window")
        block = (run_data.get("block") or item.data(0, Qt.UserRole + 2) or "")
        dark = (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"))
        self.fe_fp_ver_lbl.setStyleSheet("font-size: 10px; font-weight: bold; color: {};".format("#90caf9" if dark else "#1565c0"))
        self.fe_cong_img_lbl.setStyleSheet("border: 1px solid {}; background: {}; color: {}; font-size: 10px;".format("#555b64" if dark else "#9e9e9e", "#30343a" if dark else "#f5f5f5", "#dfe1e5" if dark else "#757575"))
        self.fe_cong_panel.setVisible(True)
        self._fe_cong_request_token += 1
        token = self._fe_cong_request_token
        key = (run_path or "", block or "")
        if (run_path in self._fp_ver_cache and key in self._cong_img_cache
                and key in self._cong_image_cache):
            self._apply_fe_congestion_lookup(
                token, run_path, self._fp_ver_cache.get(run_path, "-"),
                self._cong_img_cache.get(key, ""),
                self._cong_image_cache.get(key, QImage()))
            return
        self._current_cong_img_path = None
        self.fe_fp_ver_lbl.setText("FP_VER: loading...")
        self.fe_cong_img_lbl.setPixmap(QPixmap())
        self.fe_cong_img_lbl.setText("Loading congestion image...")
        self.fe_cong_img_lbl.setToolTip("")
        QTimer.singleShot(
            180,
            lambda t=token, p=run_path, b=block: self._start_fe_congestion_lookup(t, p, b))

    def _update_stage_screenshot_panel(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE" or not item.parent():
            self._hide_run_image_panel()
            return
        self._fe_cong_request_token += 1
        self.fe_cong_panel.setTitle("PNR Stage Screenshots")
        stage_name = item.text(0)
        stage_path = item.text(15)
        parent = item.parent()
        be_path = parent.text(15)
        block = item.data(0, Qt.UserRole + 2) or parent.data(0, Qt.UserRole + 2) or ""
        dark = (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"))
        self.fe_fp_ver_lbl.setStyleSheet("font-size: 10px; font-weight: bold; color: {};".format("#90caf9" if dark else "#1565c0"))
        self.fe_cong_img_lbl.setStyleSheet("border: 1px solid {}; background: {}; color: {}; font-size: 10px;".format("#555b64" if dark else "#9e9e9e", "#30343a" if dark else "#f5f5f5", "#dfe1e5" if dark else "#757575"))
        self.fe_fp_ver_lbl.setText("Stage: {}   Block: {}".format(stage_name, block or "-"))
        self.fe_cong_img_lbl.setPixmap(QPixmap())
        self.fe_cong_img_lbl.setText("Loading stage screenshot...")
        self.fe_cong_img_lbl.setToolTip("")
        self._hide_stage_map_links()
        self.fe_cong_panel.setVisible(True)
        self._current_cong_img_path = None
        self._stage_screenshot_request_token += 1
        token = self._stage_screenshot_request_token
        key = (be_path or "", stage_path or "", stage_name or "", block or "")
        if key in self._stage_screenshot_cache:
            found, img = self._stage_screenshot_cache.get(key, ({}, QImage()))
            self._apply_stage_screenshot_lookup(token, stage_name, block, found, img)
            return
        QTimer.singleShot(
            180,
            lambda t=token, bp=be_path, sp=stage_path, sn=stage_name, b=block:
                self._start_stage_screenshot_lookup(t, bp, sp, sn, b))

    def _start_stage_screenshot_lookup(self, token, be_path, stage_path, stage_name, block):
        if token != self._stage_screenshot_request_token:
            return
        worker = StageScreenshotLookupWorker(token, be_path, stage_path, stage_name, block)
        self._stage_screenshot_workers.append(worker)
        worker.finished.connect(self._on_stage_screenshot_lookup_done)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _on_stage_screenshot_lookup_done(self, token, be_path, stage_name, block, found, img):
        key = ""
        try:
            sel = self.tree.selectedItems()
            if sel and sel[0].data(0, Qt.UserRole) == "STAGE":
                key = (sel[0].parent().text(15) or "", sel[0].text(15) or "",
                       stage_name or "", block or "")
        except Exception:
            key = ""
        if key:
            self._stage_screenshot_cache[key] = (
                found if isinstance(found, dict) else {},
                img if isinstance(img, QImage) else QImage())
        self._stage_screenshot_workers = self._keep_running_workers(
            self._stage_screenshot_workers)
        if token != self._stage_screenshot_request_token:
            return
        self._apply_stage_screenshot_lookup(token, stage_name, block, found, img)

    def _apply_stage_screenshot_lookup(self, token, stage_name, block, found, img):
        if token != self._stage_screenshot_request_token:
            return
        found = found if isinstance(found, dict) else {}
        main_path = found.get("Congestion Map", "")
        self._current_cong_img_path = main_path or None
        if main_path and isinstance(img, QImage) and not img.isNull():
            px = QPixmap.fromImage(img)
            scaled = px.scaled(self.fe_cong_img_lbl.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.fe_cong_img_lbl.setPixmap(scaled)
            self.fe_cong_img_lbl.setText("")
            self.fe_cong_img_lbl.setToolTip("Click to open full screen\n" + main_path)
        elif main_path:
            self.fe_cong_img_lbl.setPixmap(QPixmap())
            self.fe_cong_img_lbl.setText("Main screenshot found but image load failed")
            self.fe_cong_img_lbl.setToolTip(main_path)
        else:
            self.fe_cong_img_lbl.setPixmap(QPixmap())
            self.fe_cong_img_lbl.setText("No {}.{}.jpg screenshot found".format(stage_name, block or "<block>"))
            self.fe_cong_img_lbl.setToolTip("")
        for label, btn in self.stage_map_links.items():
            path = found.get(label, "")
            btn.setVisible(True)
            btn.setEnabled(bool(path))
            btn.setText(label if path else label + " (missing)")
            btn.setProperty("_path", path)
            btn.setToolTip(path)

    def _start_fe_congestion_lookup(self, token, run_path, block):
        if token != self._fe_cong_request_token:
            return
        worker = FeCongestionLookupWorker(token, run_path, block)
        self._fe_cong_workers.append(worker)
        worker.finished.connect(self._on_fe_congestion_lookup_done)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _on_fe_congestion_lookup_done(self, token, run_path, block, fp_ver, img_path, img):
        key = (run_path or "", block or "")
        self._fp_ver_cache[run_path] = fp_ver or "-"
        self._cong_img_cache[key] = img_path or ""
        self._cong_image_cache[key] = img if isinstance(img, QImage) else QImage()
        self._fe_cong_workers = self._keep_running_workers(
            self._fe_cong_workers)
        if token != self._fe_cong_request_token:
            return
        self._apply_fe_congestion_lookup(token, run_path, fp_ver, img_path, img)

    def _apply_fe_congestion_lookup(self, token, run_path, fp_ver, img_path, img):
        if token != self._fe_cong_request_token:
            return
        self.fe_fp_ver_lbl.setText(
            "FP_VER: " + (fp_ver if fp_ver and fp_ver != "-" else "not found in compile_opt.log"))
        self._current_cong_img_path = img_path if img_path else None
        if img_path and isinstance(img, QImage) and not img.isNull():
            px = QPixmap.fromImage(img)
            scaled = px.scaled(self.fe_cong_img_lbl.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.fe_cong_img_lbl.setPixmap(scaled)
            self.fe_cong_img_lbl.setText("")
            self.fe_cong_img_lbl.setToolTip("Click to open full screen\n" + img_path)
        elif img_path:
            self.fe_cong_img_lbl.setPixmap(QPixmap())
            self.fe_cong_img_lbl.setText("Image load failed")
            self.fe_cong_img_lbl.setToolTip(img_path)
        else:
            self.fe_cong_img_lbl.setPixmap(QPixmap())
            self.fe_cong_img_lbl.setText("No congestion.window image found")
            self.fe_cong_img_lbl.setToolTip("Expected: reports/congestion.window.<block>.*.jpg")

    def _open_fe_congestion_fullscreen(self):
        path = getattr(self, "_current_cong_img_path", None)
        self._open_image_fullscreen(path, "Run Image")

    def _open_stage_map_link(self, label):
        btn = self.stage_map_links.get(label)
        path = btn.property("_path") if btn else ""
        if not path or not os.path.exists(path):
            QMessageBox.information(self, label, "No image is available for this selection.")
            return
        try:
            subprocess.Popen(
                ["xdg-open", path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL)
            return
        except Exception:
            self._open_image_fullscreen(path, label)

    def _open_image_fullscreen(self, path, title):
        if not path or not os.path.exists(path):
            QMessageBox.information(self, title, "No image is available for this selection.")
            return
        px = QPixmap(path)
        if px.isNull():
            QMessageBox.warning(self, title, "Could not load image:\n" + path)
            return
        dlg = QDialog(self)
        dlg.setWindowTitle(title + " - " + os.path.basename(path))
        layout = QVBoxLayout(dlg)
        view = QLabel()
        view.setAlignment(Qt.AlignCenter)
        try:
            avail = QApplication.desktop().availableGeometry(self)
            scaled = px.scaled(avail.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
        except Exception:
            scaled = px
        view.setPixmap(scaled)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(view)
        layout.addWidget(scroll)
        row = QHBoxLayout()
        path_lbl = QLabel(path)
        path_lbl.setTextInteractionFlags(Qt.TextSelectableByMouse)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.accept)
        row.addWidget(path_lbl, 1)
        row.addWidget(close_btn)
        layout.addLayout(row)
        dlg.showFullScreen()
        dlg.exec_()
    def _toggle_personal_note_box(self, checked):
        self.ins_note.setVisible(bool(checked))
        self.ins_save_btn.setVisible(bool(checked))

    def _shared_notes_text(self, note_id):
        notes = _note_lines(self.global_notes.get(note_id, []))
        return "\n".join(notes) if notes else ""

    def _note_display(self, note_id):
        parts = []
        personal = self.personal_notes.get(note_id, "") if hasattr(self, 'personal_notes') else ""
        if personal:
            first = personal.splitlines()[0]
            parts.append("Personal: " + first[:80])
        shared = _note_lines(self.global_notes.get(note_id, [])) if hasattr(self, 'global_notes') else []
        if shared:
            parts.append("Shared: " + " | ".join(shared))
        return " | ".join(parts)

    def _apply_note_display_to_item(self, item, note_id):
        note_text = self._note_display(note_id)
        item.setText(22, note_text)
        item.setToolTip(22, note_text)
        if note_text:
            item.setForeground(22, self._colors["note"])
            f = item.font(0); f.setItalic(True); item.setFont(0, f)
        else:
            f = item.font(0); f.setItalic(False); item.setFont(0, f)
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

    def _count_compile_error_lines(self, err_path):
        if not err_path:
            return 0
        try:
            if not os.path.exists(err_path):
                return 0
            count = 0
            with open(err_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if line.strip():
                        count += 1
            return count
        except Exception:
            return 0

    def save_inspector_note(self):
        if not hasattr(self, "_current_note_id"):
            return
        save_personal_note(self._current_note_id, self.ins_note.toPlainText())
        self.personal_notes = load_personal_notes()
        sel = self.tree.selectedItems()
        if sel:
            self._apply_note_display_to_item(sel[0], self._current_note_id)
        self._update_status_bar()

    def save_shared_inspector_note(self):
        if not hasattr(self, "_current_note_id"):
            return
        text = self.shared_note_input.toPlainText()
        if not text.strip():
            return
        if save_shared_note(self._current_note_id, text):
            self.global_notes = load_all_notes()
            self.shared_note_history.setPlainText(self._shared_notes_text(self._current_note_id))
            self.shared_note_input.clear()
            sel = self.tree.selectedItems()
            if sel:
                self._apply_note_display_to_item(sel[0], self._current_note_id)
        self._update_status_bar()

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
                QTableWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: {bg}; gridline-color: {fg}; }}
                QTableWidget::item:selected {{ background-color: {sel}; color: #ffffff; }}
                QListWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; }}
                QScrollArea, QAbstractScrollArea, QScrollBar {{ background-color: {bg}; color: {fg}; }}
                QLabel {{ color: {fg}; }}
                QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QTextEdit {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {bg}; color: {fg}; selection-background-color: {sel}; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px 12px; border-radius: 4px; }}
                QPushButton:hover, QToolButton:hover {{ border-color: {sel}; }}
                QPushButton:pressed {{ background-color: {sel}; color: #ffffff; }}
                QPushButton#linkBtn {{ border: none; background: transparent; color: {sel}; padding: 0px 4px; min-width: 0px; }}
                QPushButton#linkBtn:hover {{ text-decoration: underline; }}
                QMenu {{ border: 1px solid {fg}; background-color: {bg}; color: {fg}; }}
                QMenu::item:selected {{ background-color: {sel}; color: #ffffff; }}
                QStatusBar {{ background: {bg}; color: {fg}; border-top: 1px solid {fg}; }}
                QLabel#statusLink {{ color: {sel}; font-weight: bold; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: {sel}; color: #ffffff; }}
                QSpinBox::up-button, QSpinBox::down-button, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{ width: 18px; }}
                {cb_style}"""
        elif self.is_dark_mode:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #2b2d30; color: #dfe1e5; }}
                QTreeWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; }}
                QTableWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #43454a; }}
                QTableWidget::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QListWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; }}
                QScrollArea, QAbstractScrollArea {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; }}
                QLabel {{ color: #dfe1e5; }}
                QHeaderView::section {{ background-color: #2b2d30; color: #a9b7c6; border: 1px solid #1e1f22; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QDoubleSpinBox, QTextEdit {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 3px; }}
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
                QLabel#statusLink {{ color: #90caf9; font-weight: bold; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #2f65ca; color: #ffffff; }}
                QSplitter::handle {{ background-color: #43454a; }}
                QSpinBox::up-button, QSpinBox::down-button, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{ width: 18px; }}
                {cb_style}"""
        else:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #f5f5f5; color: #212121; }}
                QTreeWidget {{ background-color: #ffffff; color: #212121; alternate-background-color: #f9f9f9; }}
                QTableWidget {{ background-color: #ffffff; color: #212121; alternate-background-color: #f9f9f9; gridline-color: #d0d0d0; }}
                QTableWidget::item:selected {{ background-color: #1976D2; color: #ffffff; }}
                QListWidget {{ background-color: #ffffff; color: #212121; alternate-background-color: #f9f9f9; }}
                QScrollArea, QAbstractScrollArea {{ background-color: #ffffff; color: #212121; border: 1px solid #d0d0d0; }}
                QHeaderView::section {{ background-color: #e0e0e0; color: #212121; border: 1px solid #bdbdbd; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QDoubleSpinBox, QTextEdit {{ background-color: #ffffff; color: #212121; border: 1px solid #bdbdbd; padding: 4px; border-radius: 3px; }}
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
                QLabel#statusLink {{ color: #1565c0; font-weight: bold; }}
                QTreeView::item {{ padding: {pad}px; }} QListWidget::item {{ padding: {pad}px; }}
                QTreeView::item:selected, QListWidget::item:selected {{ background-color: #1976D2; color: #ffffff; }}
                QSplitter::handle {{ background-color: #bdbdbd; }}
                QSpinBox::up-button, QSpinBox::down-button, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{ width: 18px; }}
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
    def _collect_quick_refresh_tasks(self):
        tasks = []
        seen = set()
        for item in self._iter_tree_items():
            run = item.data(0, Qt.UserRole + 10)
            if not run or run.get("run_type") != "FE":
                continue
            path = run.get("path") or item.text(15)
            if not path or path in ("N/A", "-") or path in seen:
                continue
            status = run.get("fe_status") or item.text(3)
            if run.get("is_comp") or status == "COMPLETED":
                continue
            seen.add(path)
            tasks.append({
                "path": path,
                "source": run.get("source", item.text(2) or "WS"),
                "item": item,
            })
        return tasks

    def start_quick_refresh(self):
        if hasattr(self, 'worker') and self._worker_is_running(self.worker):
            return
        if self._worker_is_running(getattr(self, "_quick_refresh_worker", None)):
            return
        tasks = self._collect_quick_refresh_tasks()
        if not tasks:
            if not (self.ws_data or self.out_data):
                self.start_fs_scan()
                return
            self._smart_poll_running()
            self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
            self.sb_scan_time.setText("Last refresh: " + self._last_scan_time)
            return
        self.prog_container.setVisible(True)
        self.prog.setRange(0, len(tasks))
        self.prog.setValue(0)
        self.prog_lbl.setText(
            "Quick refresh: checking " + str(len(tasks)) + " in-progress run(s)...")
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Checking...")
        try:
            worker_cls = globals().get("QuickStatusRefreshWorker")
            if worker_cls is None:
                try:
                    from workers import QuickStatusRefreshWorker as worker_cls
                except Exception:
                    worker_cls = None
            if worker_cls is None:
                self.prog_container.setVisible(False)
                self.refresh_btn.setEnabled(True)
                self.refresh_btn.setText("Refresh")
                self.status_bar.showMessage(
                    "Quick refresh worker is unavailable. Running full rescan.",
                    5000)
                QTimer.singleShot(0, self.start_fs_scan)
                return
            self._quick_refresh_items = {}
            worker_tasks = []
            for task in tasks:
                path = task.get("path", "")
                self._quick_refresh_items[path] = task.get("item")
                worker_tasks.append({
                    "path": path,
                    "source": task.get("source", "WS"),
                })
            worker = worker_cls(worker_tasks)
            self._quick_refresh_worker = worker
            worker.progress.connect(
                self._on_quick_refresh_progress)
            worker.finished.connect(
                self._on_quick_refresh_finished)
            worker.finished.connect(
                lambda *_args, ww=worker:
                self._clear_worker_attr_if_current("_quick_refresh_worker", ww))
            worker.start()
        except Exception as e:
            self.prog_container.setVisible(False)
            self.refresh_btn.setEnabled(True)
            self.refresh_btn.setText("Refresh")
            QMessageBox.warning(self, "Quick Refresh", str(e))

    def _on_quick_refresh_progress(self, done, total):
        self.prog.setRange(0, total)
        self.prog.setValue(done)
        self.prog_lbl.setText(
            "Quick refresh: " + str(done) + "/" + str(total))

    def _on_quick_refresh_finished(self, rows):
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_quick_refresh_worker", None):
            return
        changed = False
        for row in rows:
            item = self._quick_refresh_items.get(row.get("path", ""))
            if item is None:
                continue
            run = item.data(0, Qt.UserRole + 10)
            if not run:
                continue
            status = row.get("fe_status", run.get("fe_status", item.text(3)))
            is_comp = bool(row.get("is_comp", run.get("is_comp", False)))
            info = row.get("info") or run.get("info", {})
            old_status = run.get("fe_status")
            run["fe_status"] = status
            run["is_comp"] = is_comp
            run["info"] = info
            item.setData(0, Qt.UserRole + 10, run)
            _dot_map = {
                "COMPLETED":   "#388e3c", "RUNNING":    "#1976d2",
                "NOT STARTED": "#9e9e9e", "INTERRUPTED":"#e65100",
                "FAILED":      "#d32f2f", "FATAL ERROR":"#b71c1c",
            }
            dc = _dot_map.get(status, "#9e9e9e")
            item.setIcon(3, self._create_dot_icon(dc, dc))
            item.setText(3, status)
            item.setText(4, "COMPLETED" if is_comp else info.get("last_stage", item.text(4)))
            item.setText(12, info.get("runtime", item.text(12)))
            start_raw = info.get("start", item.data(0, Qt.UserRole + 40) or "")
            end_raw = info.get("end", item.data(0, Qt.UserRole + 41) or "")
            self._set_item_time_data(item, start_raw, end_raw)
            item.setText(13, self._fmt_ts(start_raw))
            item.setText(14, self._fmt_ts(end_raw))
            item.setToolTip(13, start_raw)
            item.setToolTip(14, end_raw)
            self._apply_status_color(item, 3, status)
            if old_status != status:
                changed = True
        self._running_items = [
            item for item in getattr(self, "_running_items", [])
            if item is not None and item.text(3) == "RUNNING"
        ]
        for row in rows:
            item = self._quick_refresh_items.get(row.get("path", ""))
            if item is not None and item.text(3) == "RUNNING" and item not in self._running_items:
                self._running_items.append(item)
        self._quick_refresh_items = {}
        self.prog_container.setVisible(False)
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.sb_scan_time.setText("Last refresh: " + self._last_scan_time)
        if changed:
            self.refresh_view()
        else:
            visible = []
            for item in self._iter_tree_items():
                run = item.data(0, Qt.UserRole + 10)
                if run and not item.isHidden():
                    visible.append(run)
            self._update_status_bar(visible)

    def start_fs_scan(self):
        if hasattr(self, 'worker') and self._worker_is_running(self.worker):
            return
        if self._worker_is_running(getattr(self, "_quick_refresh_worker", None)):
            return
        clear_path_cache()
        self.size_workers = self._cancel_worker_list_keep_running(
            self.size_workers)
        self._stage_workers = self._stop_worker_list_now(self._stage_workers)
        self._fe_cong_workers = self._stop_worker_list_now(self._fe_cong_workers)
        self._stage_screenshot_workers = self._stop_worker_list_now(
            self._stage_screenshot_workers)
        self._stage_metric_workers = self._stop_worker_list_now(
            self._stage_metric_workers)
        self.item_map.clear()
        self._signoff_bg_done = False
        if self._worker_is_running(self._signoff_worker):
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
        self.personal_notes  = load_personal_notes()

        # FEAT 3+5: Record history for all completed runs
        all_runs_for_history = (self.ws_data.get("all_runs", []) +
                                self.out_data.get("all_runs", []))
        for r in all_runs_for_history:
            if r.get("run_type") == "FE" and r.get("is_comp"):
                self._record_run_history(r)
        self._save_run_history()
        self._save_lightweight_snapshot_async(stats)

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
            if view == 'BE Only':
                view = 'All Runs'
            sort_mode = prefs.get(
                'UI', 'last_sort', fallback='Start Date Old->New')
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
            self._tree_sort_mode = sort_mode
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
        running_items = [
            item for item in getattr(self, "_running_items", [])
            if item is not None and not item.isHidden()
            and item.text(3) == "RUNNING"
        ]
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
                    start_raw = item.data(0, Qt.UserRole + 40) or item.toolTip(13)
                    end_raw = info.get("end", item.text(14))
                    self._set_item_time_data(item, start_raw, end_raw)
                    item.setText(14, self._fmt_ts(end_raw))
                    item.setToolTip(14, end_raw)
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
        self._running_items = [
            item for item in getattr(self, "_running_items", [])
            if item is not None and item.text(3) == "RUNNING"
        ]

    def _update_live_runtimes(self):
        """Update elapsed time display for RUNNING FE runs every 60s."""
        import datetime
        month_map = {
            "Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
            "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        now = datetime.datetime.now()
        running_items = [
            item for item in getattr(self, "_running_items", [])
            if item is not None and item.text(3) == "RUNNING"
        ]
        for child in running_items:
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

    # ------------------------------------------------------------------
    # BUILD TREE
    # ------------------------------------------------------------------
    def _build_tree(self):
        """Build the full tree once. Filtering done by setHidden() only."""
        self._closure_pass_token = getattr(self, "_closure_pass_token", 0) + 1
        self.size_workers = self._cancel_worker_list_keep_running(
            self.size_workers)
        self._stage_workers = self._stop_worker_list_now(self._stage_workers)
        self._fe_cong_workers = self._stop_worker_list_now(self._fe_cong_workers)
        self._stage_screenshot_workers = self._stop_worker_list_now(
            self._stage_screenshot_workers)
        self._stage_metric_workers = self._stop_worker_list_now(
            self._stage_metric_workers)
        self._stop_worker_if_running(getattr(self, "_owner_lookup_worker", None))
        self._owner_lookup_worker = None
        self._owner_items_by_path = {}
        self.item_map.clear()
        self._signoff_items_by_path.clear()
        self._running_items = []
        self._visible_run_item_cache = None
        if self._worker_is_running(self._signoff_worker):
            if hasattr(self._signoff_worker, 'cancel'):
                self._signoff_worker.cancel()
            self._signoff_bg_done = False

        self._building_tree = True

        # Save expand state before clear so filter/ignore actions don't collapse tree
        def _collect_expanded(node, out):
            nt = node.data(0, Qt.UserRole)
            if nt in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT", "STANDALONE_ROOT"):
                if node.isExpanded():
                    out.add((nt, node.text(0)))
            for i in range(node.childCount()):
                _collect_expanded(node.child(i), out)
        _saved_expanded = set()
        _collect_expanded(self.tree.invisibleRootItem(), _saved_expanded)

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
                    be_item.setData(0, Qt.UserRole + 11, run)
                    if _build_be_only:
                        self._add_stages(be_item, run, ign_root)
                    else:
                        ph = QTreeWidgetItem(be_item)
                        ph.setText(0, "Loading stages...")
                        ph.setData(0, Qt.UserRole, "__PLACEHOLDER__")
                        ph.setFlags(Qt.NoItemFlags)

        if ign_root.childCount() == 0:
            root.removeChild(ign_root)
        if standalone_root.childCount() == 0:
            root.removeChild(standalone_root)

        self.tree.setSortingEnabled(True)
        self._apply_tree_sort()
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
            _save_mail_users_async(all_owners)

        self.refresh_view()

        # Fill real Unix owners after the tree is visible. This avoids using
        # unreliable run-name guesses while keeping startup responsive.
        QTimer.singleShot(250, self.start_bg_owner_lookup)

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

        # Optional scorecard/regression pass deferred until after the tree paints.
        if (self._closure_enabled or self._status_regression_enabled
                or self._qor_regression_enabled):
            QTimer.singleShot(300, self._run_closure_pass)
        if getattr(self, "_force_default_expand", False):
            self._force_default_expand = False
            QTimer.singleShot(50, self._expand_to_rtl_level)
        # Auto-expand to milestone level on first load only
        elif not hasattr(self, '_auto_expanded_once'):
            self._auto_expanded_once = True
            QTimer.singleShot(50, self._expand_to_rtl_level)
        # Pre-warm log paths later so it does not compete with the FM/VSLP
        # background scan immediately after tree build.
        QTimer.singleShot(5000, self._prefetch_log_paths)

    # ------------------------------------------------------------------
    # CREATE RUN ITEM
    # ------------------------------------------------------------------
    def _guess_owner_from_run_name(self, name):
        name = name or ""
        base = name.replace("-FE", "").replace("-BE", "")
        toks = [t for t in re.split(r'[-_]+', base) if t]
        skip = set(["EVT0", "EVT1", "ML0", "ML1", "ML2", "ML3", "ML4",
                    "DEV00", "DEV01", "DEV02", "DEV03", "DEV04",
                    "PRE", "SVP", "FFN", "SYN", "AUTOFP", "FP", "DP",
                    "TRIAL", "FINAL", "PHYSYN", "PRESVP", "POSTSVP"])
        for i, tok in enumerate(toks):
            low = tok.lower()
            if len(low) < 3:
                continue
            if low.upper() in skip:
                continue
            if re.match(r'^[a-z][a-z]+$', low):
                if i + 1 < len(toks) and re.match(r'^[a-z][a-z]+$', toks[i + 1].lower()):
                    return low + "." + toks[i + 1].lower()
                return low
        return "Unknown"

    def _display_owner_for_run(self, run):
        owner = (run.get("owner") or "").strip()
        if owner and owner != "Unknown":
            return owner
        return "Unknown"

    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(
            Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
        child.setCheckState(0, Qt.Unchecked)

        r_name = run["r_name"]
        display_owner = self._display_owner_for_run(run)
        child.setText(0, r_name)
        child.setText(1, run["rtl"])
        child.setText(2, run["source"])
        child.setText(5, display_owner)
        child.setToolTip(5, display_owner)
        child.setText(15, run["path"])
        child.setText(22, "")
        child.setData(0, Qt.UserRole + 2, run["block"])
        child.setData(0, Qt.UserRole + 4,
                      r_name.replace("-FE","").replace("-BE",""))

        note_id = f"{run['rtl']} : {r_name}"
        self._apply_note_display_to_item(child, note_id)

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
            if status_str == "RUNNING":
                self._running_items.append(child)
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
            self._set_item_time_data(child, start_raw, end_raw)
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
            self._set_item_time_data(child, be_start_raw, be_end_raw)
            child.setText(13, self._fmt_ts(be_start_raw))
            child.setText(14, self._fmt_ts(be_end_raw))

        child.setData(0, Qt.UserRole, "STAGE"
                      if run["run_type"] == "STAGE" else None)

        if self._run_in_filter_config(run):
            child.setText(23, "CONFIG")
            child.setForeground(0, QColor("#1565c0" if not self.is_dark_mode else "#90caf9"))
            tooltip_text += "\n[IN ACTIVE FILTER CONFIG]"
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

        # Apply pin icon at creation time - O(1), replaces post-build tree walk
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
            note_text = self._note_display(text)
            if note_text:
                p.setText(22, note_text); p.setToolTip(22, note_text)
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
            stage_owner = be_item.text(5) if be_item else "Unknown"
            s_item.setText(5, stage_owner)
            s_item.setToolTip(5, stage_owner)
            s_item.setText(7,  f"NONUPF - {stage.get('st_n', '')}")
            s_item.setText(8,  f"UPF - {stage.get('st_u', '')}")
            s_item.setText(9,  stage.get("vslp_status", ""))
            s_item.setText(12, stage.get("info", {}).get("runtime", ""))
            s_start_raw = stage.get("info", {}).get("start", "")
            s_end_raw   = stage.get("info", {}).get("end", "")
            self._set_item_time_data(s_item, s_start_raw, s_end_raw)
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
        def _start_stage_detail_worker(be_run):
            if not be_run or be_run.get("_stage_detail_loading"):
                return
            if not any(s.get("_lazy") for s in be_run.get("stages", [])):
                return
            from workers import StageDetailWorker
            be_run["_stage_detail_loading"] = True
            w = StageDetailWorker(be_run)
            w.finished.connect(self._on_stage_details_loaded)
            w.finished.connect(w.deleteLater)
            w.start()
            self._stage_workers.append(w)

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
                    _start_stage_detail_worker(be_run)
                    return
        be_run = item.data(0, Qt.UserRole + 11)
        if be_run:
            _start_stage_detail_worker(be_run)

    def _on_stage_details_loaded(self, be_path, run_name, enriched_stages):
        """Called by StageDetailWorker using stable identifiers only."""
        try:
            be_item = None
            for item in self._iter_tree_items():
                try:
                    be_run = item.data(0, Qt.UserRole + 11)
                    if be_run and be_run.get("path") == be_path:
                        if not run_name or be_run.get("r_name") == run_name or item.text(0) == run_name:
                            be_item = item
                            break
                except RuntimeError:
                    continue
                except Exception:
                    continue
            if be_item is None:
                return
            be_run = be_item.data(0, Qt.UserRole + 11)
            if not enriched_stages:
                if be_run:
                    be_run["_stage_detail_loading"] = False
                return
            if be_run:
                be_run["stages"] = enriched_stages
                be_run["_stage_detail_loading"] = False
                be_run["_stage_detail_loaded"] = True
            for i in range(be_item.childCount()):
                ch = be_item.child(i)
                if ch.data(0, Qt.UserRole) != "STAGE":
                    continue
                sname = ch.text(0)
                for s in enriched_stages:
                    if s["name"] == sname:
                        s_start = s.get("info", {}).get("start", "")
                        s_end = s.get("info", {}).get("end", "")
                        self._set_item_time_data(ch, s_start, s_end)
                        ch.setText(12, s.get("info", {}).get("runtime", "-"))
                        ch.setText(13, self._fmt_ts(s_start))
                        ch.setText(14, self._fmt_ts(s_end))
                        ch.setText(7, "NONUPF - " + s["st_n"])
                        ch.setText(8, "UPF - " + s["st_u"])
                        ch.setText(9, s["vslp_status"])
                        self._apply_fm_color(ch, 7, ch.text(7))
                        self._apply_fm_color(ch, 8, ch.text(8))
                        self._apply_vslp_color(ch, 9, ch.text(9))
                        break
        except RuntimeError:
            pass
        except Exception:
            pass
        self._stage_workers = self._keep_running_workers(self._stage_workers)

    # ------------------------------------------------------------------
    # REFRESH VIEW (pure hide/show -- zero item creation)
    # ------------------------------------------------------------------
    def _iter_tree_items(self):
        out = []
        def collect(node):
            for i in range(node.childCount()):
                child = node.child(i)
                out.append(child)
                collect(child)
        collect(self.tree.invisibleRootItem())
        return out

    def _cache_modified_times_for_sort(self):
        for item in self._iter_tree_items():
            role = item.data(0, Qt.UserRole)
            if role in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT",
                        "STANDALONE_ROOT", "__PLACEHOLDER__"):
                continue
            path = item.text(15)
            val = 0.0
            if path and path not in ("N/A", "-"):
                try:
                    val = os.path.getmtime(path)
                except Exception:
                    val = 0.0
            item.setData(0, Qt.UserRole + 60, val)

    def _move_special_roots_to_bottom(self):
        try:
            root = self.tree.invisibleRootItem()
            for role in ("STANDALONE_ROOT", "IGNORED_ROOT"):
                for i in range(root.childCount()):
                    child = root.child(i)
                    if child.data(0, Qt.UserRole) == role:
                        moved = root.takeChild(i)
                        root.addChild(moved)
                        break
        except Exception:
            pass

    def _rtl_release_sort_key(self, text):
        s = str(text or "")
        m = re.search(r'EVT(\d+)_ML(\d+)_DEV(\d+)', s)
        if m:
            return (0, int(m.group(1)), int(m.group(2)), int(m.group(3)), s)
        nums = [int(x) for x in re.findall(r'\d+', s)]
        return (1,) + tuple(nums[:6]) + (s,)

    def _reorder_milestones_by_map(self):
        label_order = {}
        try:
            for idx, label in enumerate(self._milestone_map.values()):
                if label not in label_order:
                    label_order[label] = idx
        except Exception:
            return

        def _walk(parent):
            count = parent.childCount()
            milestone_idxs = [
                i for i in range(count)
                if parent.child(i).data(0, Qt.UserRole) == "MILESTONE"
            ]
            if len(milestone_idxs) > 1:
                children = [parent.takeChild(0) for _ in range(count)]
                children.sort(key=lambda it: (
                    0 if it.data(0, Qt.UserRole) == "MILESTONE" else 1,
                    label_order.get(it.text(0), 999),
                    it.text(0)))
                for child in children:
                    parent.addChild(child)
            if parent.data(0, Qt.UserRole) == "MILESTONE" and parent.childCount() > 1:
                rtl_children = [parent.takeChild(0) for _ in range(parent.childCount())]
                rtl_children.sort(key=lambda it: self._rtl_release_sort_key(it.text(0)))
                for child in rtl_children:
                    parent.addChild(child)
            for i in range(parent.childCount()):
                _walk(parent.child(i))

        try:
            _walk(self.tree.invisibleRootItem())
        except Exception:
            pass

    def _refresh_group_start_sort_keys(self):
        label_order = {}
        try:
            for idx, label in enumerate(self._milestone_map.values()):
                if label not in label_order:
                    label_order[label] = idx
        except Exception:
            label_order = {}

        def _walk(item):
            best = None
            for i in range(item.childCount()):
                child = item.child(i)
                ck = _walk(child)
                if ck is not None and (best is None or ck < best):
                    best = ck
            role = item.data(0, Qt.UserRole)
            if role not in ("BLOCK", "MILESTONE", "RTL",
                            "IGNORED_ROOT", "STANDALONE_ROOT"):
                own = item.data(0, Qt.UserRole + 42)
                if own is not None:
                    best = own if best is None or own < best else best
            elif role == "MILESTONE" and item.text(0) in label_order:
                best = (0, label_order.get(item.text(0), 999))
            if best is not None:
                item.setData(0, Qt.UserRole + 42, best)
            return best
        try:
            root = self.tree.invisibleRootItem()
            for i in range(root.childCount()):
                _walk(root.child(i))
        except Exception:
            pass

    def _apply_tree_sort(self):
        self._refresh_group_start_sort_keys()
        mode = getattr(self, "_tree_sort_mode", "Start Date Old->New")
        self.tree.setProperty("flow_sort_mode", "")
        if mode == "Start Date New->Old":
            col, order = 13, Qt.DescendingOrder
        elif mode == "End Date Old->New":
            col, order = 14, Qt.AscendingOrder
        elif mode == "End Date New->Old":
            col, order = 14, Qt.DescendingOrder
        elif mode == "Modified Date Old->New":
            self.tree.setProperty("flow_sort_mode", "modified")
            self._cache_modified_times_for_sort()
            col, order = 0, Qt.AscendingOrder
        elif mode == "Modified Date New->Old":
            self.tree.setProperty("flow_sort_mode", "modified")
            self._cache_modified_times_for_sort()
            col, order = 0, Qt.DescendingOrder
        elif mode == "Run Name A-Z":
            self.tree.setProperty("flow_sort_mode", "")
            col, order = 0, Qt.AscendingOrder
        else:
            self.tree.setProperty("flow_sort_mode", "")
            col, order = 13, Qt.AscendingOrder
        self.tree.sortByColumn(col, order)
        self.tree.header().setSortIndicator(col, order)
        self._move_special_roots_to_bottom()
        self._reorder_milestones_by_map()

    def _set_tree_sort_mode(self, mode):
        self._tree_sort_mode = mode
        self._apply_tree_sort()
        self._fit_run_name_column()

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
        _completed_only = (preset == "Completed Only")
        _run_only      = (preset == "Running Only")
        _fail_only     = (preset == "Failed Only")
        _today_only    = (preset == "Today's Runs")
        _pinned_only   = (preset == "Pinned Only")
        _selected_only = (preset == "Selected Only")
        _checked_set   = self._checked_paths
        _pins          = self.user_pins
        _rfc           = None if self.ignore_run_filter else self.run_filter_config
        _notes         = self.global_notes
        _personal_notes = self.personal_notes
        _note_text_cache = {}
        visible_run_items = []
        self._visible_run_item_cache = None

        def _search_notes(note_id):
            if note_id in _note_text_cache:
                return _note_text_cache[note_id]
            notes = " | ".join(_note_lines(_notes.get(note_id, [])))
            if note_id in _personal_notes:
                notes += " | " + _personal_notes.get(note_id, "")
            _note_text_cache[note_id] = notes
            return notes

        _pinned_desc_items = set()
        if _pinned_only:
            def _mark_pinned_desc(item):
                has_pinned = False
                run = item.data(0, _UR10)
                if run:
                    for st in run.get("stages", []) or []:
                        sp = st.get("stage_path", "")
                        if sp and sp in _pins:
                            has_pinned = True
                            break
                for i in range(item.childCount()):
                    ch = item.child(i)
                    p = ch.text(15)
                    child_has = bool(p and p in _pins)
                    if _mark_pinned_desc(ch):
                        child_has = True
                    if child_has:
                        has_pinned = True
                if has_pinned:
                    _pinned_desc_items.add(id(item))
                return has_pinned

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
                    src_cfg = _rfc.get(src, {})
                    matched_rtls = [k for k in src_cfg
                                    if k == rr or (k and k in rr) or (rr and rr in k)]
                    if matched_rtls:
                        allowed = []
                        for cfg_rtl in matched_rtls:
                            allowed.extend(src_cfg.get(cfg_rtl, {}).get(rb, []) or [])
                        if not allowed:
                            return False
                        base_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                        if base_name not in allowed and run["r_name"] not in allowed:
                            return False
            rtl = run["rtl"]
            if not _sel_rtl_all:
                if rtl != sel_rtl and not rtl.startswith(_sel_rtl_sfx):
                    return False
            rt_type = run["run_type"]
            if _fe_only and rt_type != "FE": return False
            if _be_only and rt_type != "BE": return False
            if _completed_only and not (
                    rt_type == "FE" and run.get("is_comp")):
                return False
            if _run_only and not (
                    rt_type == "FE"
                    and run.get("fe_status", "") == "RUNNING"):
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
                notes    = _search_notes(note_id)
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

        _UR   = Qt.UserRole
        _UR10 = Qt.UserRole + 10
        _GROUP_TYPES = frozenset(
            ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STANDALONE_ROOT"))

        if _pinned_only:
            _mark_pinned_desc(self.tree.invisibleRootItem())

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
            # into children. Never auto-expand - preserve user's expand state.
            if node_type in _GROUP_TYPES or node_type == "MILESTONE":
                # Short-circuit: if this is a BLOCK node whose block is
                # entirely excluded by the block-list filter, hide it and
                # skip recursing all its children - big win when many blocks
                # are unchecked (skips 70-80% of tree walk).
                if node_type == "BLOCK" and item.text(0) not in checked_blks:
                    item.setHidden(True)
                    return False
                any_visible = False
                for i in range(item.childCount()):
                    if _update_visibility(item.child(i)):
                        any_visible = True
                item.setHidden(not any_visible)
                # No setExpanded() - user expand state is preserved
                return any_visible
            else:
                run         = item.data(0, _UR10)
                passes      = _passes(run)
                if _pinned_only and not passes and id(item) in _pinned_desc_items:
                    passes = True
                rt_type_run = run.get("run_type") if run else None
                item.setHidden(not passes)
                if passes and run:
                    visible_runs.append(run)
                    if run.get("run_type") == "FE":
                        visible_run_items.append(item)
                for i in range(item.childCount()):
                    ch = item.child(i)
                    if ch.data(0, _UR) == "__PLACEHOLDER__":
                        ch.setHidden(True)
                    elif ch.data(0, _UR) == "STAGE":
                        # When BE-only: hide synthesis stages of FE parent
                        hide_stage = not passes or (
                            _be_only and rt_type_run == "FE")
                        if _pinned_only:
                            parent_pinned = bool(run and run.get("path") in _pins)
                            stage_pinned = bool(ch.text(15) and ch.text(15) in _pins)
                            hide_stage = not (parent_pinned or stage_pinned)
                        ch.setHidden(hide_stage)
                    else:
                        # BE child run under FE item: hide when FE-only
                        child_run = ch.data(0, _UR10)
                        child_rt  = child_run.get("run_type") if child_run else None
                        hide_child = not passes or (_fe_only and child_rt == "BE")
                        if _pinned_only:
                            child_path = child_run.get("path") if child_run else ch.text(15)
                            hide_child = not (
                                (child_path and child_path in _pins)
                                or id(ch) in _pinned_desc_items)
                        ch.setHidden(hide_child)
                return passes

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            _update_visibility(root.child(i))

        self._visible_run_item_cache = list(visible_run_items)
        if self.active_col_filters:
            self.apply_tree_filters()
            self._visible_run_item_cache = None

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
        run_data_for_menu = item.data(0, Qt.UserRole + 10) or {}
        is_be_run = bool(run_data_for_menu.get("run_type") == "BE")

        target_item = item if not is_stage else item.parent()
        b_name      = target_item.data(0, Qt.UserRole + 2)
        r_rtl       = target_item.text(1)
        base_run    = target_item.data(0, Qt.UserRole + 4)
        run_source  = target_item.text(2)

        act_gold = act_good = act_red = act_later = act_clear = None
        gantt_act = None
        timeline_act = None
        sort_actions = {}

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
                gantt_act = m.addAction("Show Timeline Overview")
                m.addSeparator()
            else:
                timeline_act = m.addAction("Run Timeline Overview")
                m.addSeparator()

        sort_menu = m.addMenu("Sort Tree By")
        for label in [
                "Run Name A-Z",
                "Start Date Old->New", "Start Date New->Old",
                "End Date Old->New", "End Date New->Old",
                "Modified Date Old->New", "Modified Date New->Old"]:
            act = sort_menu.addAction(label)
            sort_actions[act] = label
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
        add_checked_config_act = None
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

        be_stage_table_act = None
        app_opt_paths_act = None
        if is_stage or is_be_run:
            be_stage_table_act = m.addAction(
                "Generate BE Stage Summary Table")
            if is_stage:
                app_opt_paths_act = m.addAction(
                    "Show App Options Search Paths")
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

        if be_stage_table_act and res == be_stage_table_act:
            self.show_be_stage_summary_table(item)
            return

        if app_opt_paths_act and res == app_opt_paths_act:
            self.show_stage_app_options_search_paths(item)
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

        elif res in sort_actions:
            self._set_tree_sort_mode(sort_actions[res])
            return

        elif gantt_act and res == gantt_act:
            self.show_timeline_overview(item)

        elif timeline_act and res == timeline_act:
            self.show_timeline_overview(item)

        elif edit_note_act and res == edit_note_act:
            dlg = EditNoteDialog(self.personal_notes.get(note_identifier, ""),
                                 note_identifier, self)
            if dlg.exec_():
                save_personal_note(note_identifier, dlg.get_text())
                self.personal_notes = load_personal_notes()
                self.refresh_view()

        elif add_checked_config_act and res == add_checked_config_act:
            self.add_checked_runs_to_filter_config()
            return

        elif add_config_act and res == add_config_act:
            if not self.current_config_path:
                path, _ = QFileDialog.getSaveFileName(
                    self, "Create New Config", "dashboard_filter.cfg",
                    "Config Files (*.cfg *.txt)")
                if not path:
                    return
                self.current_config_path = path
            added = self._add_run_to_filter_config(
                run_source, r_rtl, b_name, base_run)
            self._save_current_config()
            self.sb_config.setText(
                f"Config: {os.path.basename(self.current_config_path)}")
            self.status_bar.showMessage(
                "Added to active filter config: " + added, 5000)

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
            worker = SingleSizeWorker(item_id, run_path)
            worker.result.connect(self.update_item_size)
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
        One call per 50 results instead of one call per result - keeps UI fluid."""
        for item_id, size_str in batch:
            self.update_item_size(item_id, size_str)

    # ------------------------------------------------------------------
    # BACKGROUND OWNER LOOKUP
    # ------------------------------------------------------------------
    def start_bg_owner_lookup(self):
        if self._building_tree:
            return
        if self._worker_is_running(self._owner_lookup_worker):
            return
        tasks = []
        seen = set()
        self._owner_items_by_path = {}
        for item in self._iter_tree_items():
            run = item.data(0, Qt.UserRole + 10)
            if not run:
                continue
            if run.get("run_type") not in ("FE", "BE"):
                continue
            path = run.get("path") or item.text(15)
            if not path or path in ("N/A", "-"):
                continue
            owner = (run.get("owner") or item.text(5) or "").strip()
            if owner and owner != "Unknown":
                item.setToolTip(5, owner)
                continue
            self._owner_items_by_path.setdefault(path, []).append(item)
            if path not in seen:
                seen.add(path)
                tasks.append({"path": path})
        if not tasks:
            return
        self._owner_lookup_worker = OwnerLookupWorker(tasks)
        self._owner_lookup_worker.batch_ready.connect(self._on_owner_lookup_batch)
        self._owner_lookup_worker.finished.connect(self._on_owner_lookup_finished)
        self._owner_lookup_worker.start()

    def _set_item_owner_text(self, item, owner):
        if not item or not owner or owner == "Unknown":
            return
        try:
            item.setText(5, owner)
            item.setToolTip(5, owner)
            run = item.data(0, Qt.UserRole + 10)
            if run:
                run["owner"] = owner
            for i in range(item.childCount()):
                ch = item.child(i)
                if ch and ch.data(0, Qt.UserRole) == "STAGE":
                    ch.setText(5, owner)
                    ch.setToolTip(5, owner)
        except RuntimeError:
            pass
        except Exception:
            pass

    def _on_owner_lookup_batch(self, batch):
        owners = set()
        updated = False
        for row in batch or []:
            path = row.get("path")
            owner = (row.get("owner") or "").strip()
            if not path or not owner or owner == "Unknown":
                continue
            owners.add(owner)
            for item in list(self._owner_items_by_path.get(path, []) or []):
                self._set_item_owner_text(item, owner)
                updated = True
        if owners:
            _save_mail_users_async(owners)
        if updated and self.search.text().strip():
            self.refresh_view()

    def _on_owner_lookup_finished(self):
        self._owner_lookup_worker = None

    # ------------------------------------------------------------------
    # BACKGROUND FE SIGNOFF SCAN
    # ------------------------------------------------------------------
    def start_bg_signoff_scan(self):
        if self._building_tree:
            return
        if self._worker_is_running(self._signoff_worker):
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
                    self._set_item_owner_text(item, row["owner"])
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
                self, "FE Block Summary",
                "Please check (tick) the FE runs you want to include\n"
                "in the summary table, then click FE Block Summary Table.")
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
    def _run_in_filter_config(self, run):
        if not self.run_filter_config or not run:
            return False
        src = run.get("source", "")
        rtl = run.get("rtl", "")
        blk = run.get("block", "")
        allowed = self.run_filter_config.get(src, {}).get(rtl, {}).get(blk)
        if not allowed:
            return False
        base = run.get("r_name", "").replace("-FE", "").replace("-BE", "")
        return run.get("r_name", "") in allowed or base in allowed

    def _ensure_filter_config_path(self):
        if self.current_config_path:
            return True
        path, _ = QFileDialog.getSaveFileName(
            self, "Create New Config", "dashboard_filter.cfg",
            "Config Files (*.cfg *.txt)")
        if not path:
            return False
        self.current_config_path = path
        return True

    def _task_item_for_filter_config(self, item):
        if not item:
            return None
        if item.data(0, Qt.UserRole) == "STAGE":
            item = item.parent()
        run = item.data(0, Qt.UserRole + 10) if item else None
        if not run:
            return None
        return item

    def add_checked_runs_to_filter_config(self):
        items = []
        seen = set()
        for item in self._checked_run_items() + self._checked_stage_items():
            item = self._task_item_for_filter_config(item)
            if not item:
                continue
            path = item.text(15)
            if path in seen:
                continue
            seen.add(path)
            items.append(item)
        if not items:
            QMessageBox.information(
                self, "Filter Config", "Check one or more runs first.")
            return
        if not self._ensure_filter_config_path():
            return
        added_count = 0
        for item in items:
            run = item.data(0, Qt.UserRole + 10) or {}
            base_run = item.data(0, Qt.UserRole + 4) or item.text(0)
            before = list(self.run_filter_config.get(
                run.get("source", ""), {}).get(
                item.text(1), {}).get(
                run.get("block", item.data(0, Qt.UserRole + 2) or ""), [])) if self.run_filter_config else []
            self._add_run_to_filter_config(
                run.get("source", item.text(2)),
                item.text(1),
                run.get("block", item.data(0, Qt.UserRole + 2) or ""),
                base_run)
            after = self.run_filter_config.get(
                run.get("source", item.text(2)), {}).get(
                item.text(1), {}).get(
                run.get("block", item.data(0, Qt.UserRole + 2) or ""), [])
            if len(after) > len(before):
                added_count += 1
        self._save_current_config()
        self.ignore_run_filter = False
        if hasattr(self, "ignore_run_filter_act"):
            self.ignore_run_filter_act.setChecked(False)
        self.sb_config.setText(
            "Config: {}".format(os.path.basename(self.current_config_path)))
        self.status_bar.showMessage(
            "Added {} checked run(s) to active filter config".format(added_count),
            5000)
        self.refresh_view()

    def toggle_ignore_run_filter(self, checked):
        self.ignore_run_filter = bool(checked)
        if self.current_config_path:
            name = os.path.basename(self.current_config_path)
            self.sb_config.setText(
                "Config: {}{}".format(name, " (ignored)" if checked else ""))
        else:
            self.sb_config.setText("Config: None")
        self.refresh_view()

    def load_filter_config(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Run Filter Config", "",
            "Config Files (*.cfg *.txt)")
        if not path:
            return
        try:
            cfg = self.run_filter_config or {}
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line and '|' in line.split('=', 1)[0]:
                        key, runs_str = line.split('=', 1)
                        parts = [p.strip() for p in key.split('|')]
                    else:
                        parts = [p.strip() for p in line.split(':', 3)]
                        runs_str = parts[3] if len(parts) == 4 else ""
                    if len(parts) != 3 and len(parts) != 4:
                        continue
                    if len(parts) == 4:
                        source, rtl, block = parts[:3]
                    else:
                        source, rtl, block = parts
                    run_list = [r.strip() for r in runs_str.split(',')
                                if r.strip()]
                    current = cfg.setdefault(source, {}).setdefault(
                        rtl, {}).setdefault(block, [])
                    for run_name in run_list:
                        if run_name not in current:
                            current.append(run_name)
            self.run_filter_config  = cfg
            self.current_config_path = path
            self.ignore_run_filter = False
            if hasattr(self, "ignore_run_filter_act"):
                self.ignore_run_filter_act.setChecked(False)
            self.sb_config.setText(
                f"Config: {os.path.basename(path)}")
            self.refresh_view()
        except Exception as e:
            QMessageBox.warning(self, "Load Config Error", str(e))

    def clear_filter_config(self):
        self.run_filter_config  = None
        self.current_config_path = None
        self.ignore_run_filter = False
        if hasattr(self, "ignore_run_filter_act"):
            self.ignore_run_filter_act.setChecked(False)
        self.sb_config.setText("Config: None")
        self.refresh_view()

    def generate_sample_config(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Sample Config", "sample_filter.cfg",
            "Config Files (*.cfg *.txt)")
        if not path:
            return
        sample = (
            "# Format: SOURCE|RTL_RELEASE|BLOCK = run1,run2,...\n"
            "# Old source:rtl:block:run1,run2 format is still accepted.\n"
            "# Example:\n"
            "WS|S5K2P5SP_EVT0_ML4_DEV00_syn1|BLK_CMU = run1,run2\n"
            "OUTFEED|S5K2P5SP_EVT0_ML4_DEV00|BLK_CPU = run1\n")
        with open(path, 'w') as f:
            f.write(sample)
        QMessageBox.information(self, "Sample Config", f"Saved to:\n{path}")

    def _add_run_to_filter_config(self, source, rtl, block, run_name):
        if self.run_filter_config is None:
            self.run_filter_config = {}
        source = str(source or "WS").strip()
        rtl = str(rtl or "").strip()
        block = str(block or "").strip()
        run_name = str(run_name or "").strip()
        if not source or not rtl or not block or not run_name:
            return "-"
        runs = self.run_filter_config.setdefault(source, {}).setdefault(
            rtl, {}).setdefault(block, [])
        if run_name not in runs:
            runs.append(run_name)
        return "{}|{}|{} = {}".format(source, rtl, block, run_name)

    def _save_current_config(self):
        if not self.current_config_path or not self.run_filter_config:
            return
        with open(self.current_config_path, 'w',
                  encoding='utf-8') as f:
            f.write("# dashboard_filter.cfg\n")
            for src, rtl_dict in self.run_filter_config.items():
                for rtl, blk_dict in rtl_dict.items():
                    for blk, runs in blk_dict.items():
                        f.write("{}|{}|{} = {}\n".format(
                            src, rtl, blk, ",".join(runs)))

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
        closure_cb.setChecked(getattr(self, '_closure_enabled', False))
        gen_l.addRow("", closure_cb)

        status_reg_cb = QCheckBox("Enable Status Regression Detection")
        status_reg_cb.setChecked(getattr(self, '_status_regression_enabled', False))
        status_reg_cb.setToolTip(
            "Checks runtime, FM and VSLP regressions from run history. Default: off.")
        gen_l.addRow("", status_reg_cb)

        qor_reg_cb = QCheckBox("Enable QoR Regression Detection")
        qor_reg_cb.setChecked(getattr(self, '_qor_regression_enabled', False))
        qor_reg_cb.setToolTip(
            "Uses already cached metrics only. It does not parse QoR reports during scan. Default: off.")
        gen_l.addRow("", qor_reg_cb)

        gate_factor_spin = QDoubleSpinBox()
        gate_factor_spin.setDecimals(6)
        gate_factor_spin.setRange(0.000001, 100.0)
        gate_factor_spin.setSingleStep(0.0001)
        gate_factor_spin.setValue(getattr(self, 'gate_count_unit_area', 0.2419))
        gate_factor_spin.setToolTip(
            "Gate Count = Std Cell Area / this value. Default: 0.2419")
        gen_l.addRow("Gate count unit area:", gate_factor_spin)

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
            ("Ctrl+R",       "Quick refresh in-progress runs"),
            ("Ctrl+Shift+R", "Full rescan all workspaces"),
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
        current_mode_before_settings = self.mode_combo.currentText()
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
        prefs.set('UI', 'show_relative_time',
                  'true' if self.show_relative_time else 'false')
        prefs.set('UI', 'convert_to_ist',
                  'true' if self.convert_to_ist else 'false')
        if old_rel_time != self.show_relative_time or old_ist != self.convert_to_ist:
            QTimer.singleShot(50, self._refresh_timestamps)
        old_hide_blk = self.hide_block_nodes
        self.hide_block_nodes   = hide_blk_cb.isChecked()
        prefs.set('UI', 'hide_block_nodes',
                  'true' if self.hide_block_nodes else 'false')
        _need_rebuild = _need_rebuild or (old_hide_blk != self.hide_block_nodes)
        self._closure_enabled = closure_cb.isChecked()
        self._status_regression_enabled = status_reg_cb.isChecked()
        self._qor_regression_enabled = qor_reg_cb.isChecked()
        prefs.set('UI', 'closure_enabled',
                  'true' if self._closure_enabled else 'false')
        prefs.set('UI', 'status_regression_enabled',
                  'true' if self._status_regression_enabled else 'false')
        prefs.set('UI', 'qor_regression_enabled',
                  'true' if self._qor_regression_enabled else 'false')
        self.gate_count_unit_area = gate_factor_spin.value()
        prefs.set('UI', 'gate_count_unit_area',
                  "{:.6f}".format(self.gate_count_unit_area))

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
        _write_config_atomic(prefs, USER_PREFS_FILE)

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
        self._set_col_preset(
            {"Standard": 2, "Compact": 1, "Full": 3}.get(current_mode_before_settings, 2))

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
                and self._worker_is_running(self._disk_scan_worker)):
            return
        if force:
            self._stop_worker_if_running(getattr(self, "_disk_scan_worker", None))
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

        if not self._stop_worker_attr("_qor_worker"):
            QMessageBox.information(
                self, "QoR Compare",
                "Previous QoR compare is still running. Please try again in a moment.")
            return
        worker = QoRWorker(script, sel, _PYTHON_BIN)
        self._qor_worker = worker
        worker.finished.connect(self._on_qor_done)
        worker.finished.connect(
            lambda *_args, ww=worker:
            self._clear_worker_attr_if_current("_qor_worker", ww))
        worker.start()

    def _on_qor_done(self, html_path):
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_qor_worker", None):
            return
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

        if not self._stop_worker_attr("_qor_worker"):
            QMessageBox.information(
                self, "QoR Compare",
                "Previous QoR compare is still running. Please try again in a moment.")
            return
        worker = QoRWorker(script, [be_run_path, "-stage", stage_name],
                            _PYTHON_BIN)
        self._qor_worker = worker
        worker.finished.connect(self._on_qor_done)
        worker.finished.connect(
            lambda *_args, ww=worker:
            self._clear_worker_attr_if_current("_qor_worker", ww))
        worker.start()

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

    def _checked_run_items(self):
        return self._iter_checked_items()

    def _checked_stage_items(self):
        items = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (c.checkState(0) == Qt.Checked
                        and c.data(0, Qt.UserRole) == "STAGE"):
                    items.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())
        return items

    def deselect_all_checked_runs(self):
        self.tree.blockSignals(True)
        try:
            def walk(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    if c.checkState(0) == Qt.Checked:
                        c.setCheckState(0, Qt.Unchecked)
                    walk(c)
            walk(self.tree.invisibleRootItem())
        finally:
            self.tree.blockSignals(False)
        self._checked_paths.clear()
        self._update_status_bar()
        if self.view_combo.currentText() == "Selected Only":
            self.refresh_view()

    def show_selected_timeline_overview(self):
        item = None
        selected = self.tree.selectedItems()
        if selected:
            item = selected[0]
        else:
            checked = self._checked_run_items()
            if checked:
                item = checked[0]
        if not item:
            QMessageBox.information(
                self, "Timeline Overview",
                "Select or check one FE/BE run first.")
            return
        if item.data(0, Qt.UserRole) in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT", "__PLACEHOLDER__"):
            QMessageBox.information(
                self, "Timeline Overview",
                "Select or check a run row, not a grouping row.")
            return
        self.show_timeline_overview(item)
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
        """Click on Selected count label -> toggle Selected Only view."""
        if self.view_combo.currentText() == "Selected Only":
            self.view_combo.setCurrentText("All Runs")
        else:
            if not self._checked_paths:
                return  # nothing checked, ignore
            self.view_combo.setCurrentText("Selected Only")

    def _on_status_config_clicked(self):
        if self.current_config_path and os.path.exists(self.current_config_path):
            try:
                subprocess.Popen(['gvim', self.current_config_path])
            except Exception:
                QMessageBox.information(
                    self, "Config", self.current_config_path)
        else:
            self.load_filter_config()

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
        self._prepare_utility_dialog(dlg)
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
        self._make_table_user_adjustable(t1)
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
        self._make_table_user_adjustable(t2)
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
        self._make_table_user_adjustable(t3)
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
        self._make_table_user_adjustable(t4)
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
        self._prepare_utility_dialog(dlg)
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
        self._make_table_user_adjustable(tbl)
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
        self._prepare_utility_dialog(dlg)
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
            self._make_table_user_adjustable(tbl)
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
                "branch": "FE",
                "branch_index": -1,
                "seq": 0,
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

        for branch_idx, be_item in enumerate(be_items):
            be_run = be_item.data(0, Qt.UserRole + 10) or {}
            be_name = be_run.get("r_name", be_item.text(0))
            branch_events = []
            for st in be_run.get("stages", []) or []:
                info = self._stage_info_for_timeline(be_run, st)
                ev = {
                    "name": be_name + " / " + st.get("name", "-"),
                    "kind": "STAGE",
                    "branch": be_name,
                    "branch_index": branch_idx,
                    "seq": len(branch_events),
                    "start": info.get("start", "-"),
                    "end": info.get("end", "-"),
                    "runtime": info.get("runtime", "-"),
                }
                if self._valid_timeline_event(ev):
                    branch_events.append(ev)
            branch_events.sort(
                key=lambda ev: self._parse_dashboard_time(ev.get("start")) or datetime.datetime.max)
            for seq, ev in enumerate(branch_events):
                ev["seq"] = seq
                events.append(ev)
        return events

    def show_timeline_overview(self, item):
        events = self._timeline_events_for_item(item)
        if not events:
            QMessageBox.information(
                self, "Timeline",
                "No timed FE/stage rows found for this run. "
                "Rows without runtime/start/end are hidden from the timeline.")
            return
        dlg = QDialog(self)
        self._prepare_utility_dialog(dlg)
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
        summary.setStyleSheet(
            "color: {}; font-weight: bold;".format(
                "#64b5f6" if self.is_dark_mode else "#1976d2"))
        layout.addWidget(summary)

        chart = _TimelineChartWidget(events, self._parse_dashboard_time,
                                     self.is_dark_mode)
        chart.setMinimumSize(chart.preferred_width(), chart.preferred_height(dlg.width()))
        chart.event_clicked.connect(self._show_timeline_event_detail)
        chart_scroll = QScrollArea()
        chart_scroll.setWidgetResizable(False)
        chart_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        chart_scroll.setWidget(chart)
        chart_scroll.setMinimumHeight(min(430, chart.preferred_height(dlg.width()) + 20))
        chart_scroll.setMaximumHeight(min(540, chart.preferred_height(dlg.width()) + 30))
        layout.addWidget(chart_scroll)

        tbl = QTableWidget(0, 7)
        tbl.setHorizontalHeaderLabels(["Branch", "Step", "Type", "Start", "End", "Runtime", "Gap From Previous"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        for c in range(2, 7):
            tbl.horizontalHeader().setSectionResizeMode(c, QHeaderView.Interactive)
        tbl.setColumnWidth(0, 220)
        tbl.setColumnWidth(2, 70)
        tbl.setColumnWidth(3, 150)
        tbl.setColumnWidth(4, 150)
        tbl.setColumnWidth(5, 110)
        tbl.setColumnWidth(6, 155)
        tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._make_table_user_adjustable(tbl)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        prev_by_branch = {}
        table_events = sorted(events, key=lambda ev: (
            ev.get("branch_index", -1), ev.get("seq", 0), ev.get("kind", "")))
        for ev in table_events:
            r = tbl.rowCount(); tbl.insertRow(r)
            branch = ev.get("branch", "-") if ev.get("kind") != "FE" else "FE"
            start_dt = self._parse_dashboard_time(ev.get("start"))
            gap = self._fmt_gap(prev_by_branch.get(branch), start_dt)
            vals = [branch, ev.get("name", "-"), ev.get("kind", "-"),
                    ev.get("start", "-"), ev.get("end", "-"),
                    ev.get("runtime", "-"), gap]
            for c, val in enumerate(vals):
                it = QTableWidgetItem(str(val))
                if ev.get("kind") == "FE":
                    it.setBackground(QColor(
                        "#1f3b57" if self.is_dark_mode else "#e3f2fd"))
                    it.setForeground(QColor(
                        "#dfe1e5" if self.is_dark_mode else "#263238"))
                elif ev.get("kind") == "STAGE":
                    it.setBackground(QColor(
                        "#203828" if self.is_dark_mode else "#e8f5e9"))
                    it.setForeground(QColor(
                        "#dfe1e5" if self.is_dark_mode else "#263238"))
                tbl.setItem(r, c, it)
            prev_by_branch[branch] = self._parse_dashboard_time(ev.get("end")) or prev_by_branch.get(branch)
        layout.addWidget(tbl)

        btn_row = QHBoxLayout()
        max_btn = QPushButton("Maximize Window")
        full_btn = QPushButton("Full Screen Flowchart")
        close_btn = QPushButton("Close")

        def _toggle_maximize():
            self._toggle_dialog_maximize(
                dlg, max_btn, "Maximize Window", "Restore Window")

        def _show_full_chart():
            fd = QDialog(dlg)
            fd.setWindowTitle("Timeline Flowchart - Full Screen")
            fdl = QVBoxLayout(fd)
            full_chart = _TimelineChartWidget(events, self._parse_dashboard_time, self.is_dark_mode)
            full_chart.event_clicked.connect(self._show_timeline_event_detail)
            fs = QScrollArea()
            fs.setWidgetResizable(False)
            fs.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
            full_chart.setMinimumSize(full_chart.preferred_width(), full_chart.preferred_height(1400))
            fs.setWidget(full_chart)
            fdl.addWidget(fs)
            fr = QHBoxLayout()
            exit_btn = QPushButton("Exit Full Screen")
            exit_btn.clicked.connect(fd.accept)
            fr.addStretch(1)
            fr.addWidget(exit_btn)
            fdl.addLayout(fr)
            fd.showFullScreen()
            fd.exec_()

        max_btn.clicked.connect(_toggle_maximize)
        full_btn.clicked.connect(_show_full_chart)
        close_btn.clicked.connect(dlg.accept)
        btn_row.addWidget(max_btn)
        btn_row.addWidget(full_btn)
        btn_row.addStretch(1)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)
        dlg.exec_()

    def _show_timeline_event_detail(self, ev):
        msg = (
            "Step: {}\n"
            "Type: {}\n"
            "Branch: {}\n"
            "Start: {}\n"
            "End: {}\n"
            "Runtime: {}"
        ).format(
            ev.get("name", "-"), ev.get("kind", "-"),
            ev.get("branch", "-"), ev.get("start", "-"),
            ev.get("end", "-"), ev.get("runtime", "-"))
        QMessageBox.information(self, "Timeline Step", msg)

    def _stage_task_from_item(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE" or not item.parent():
            return None
        parent = item.parent()
        return {
            "name": parent.text(0) + " / " + item.text(0),
            "be_name": parent.text(0),
            "path": parent.text(15),
            "run_type": "BE",
            "stage_name": item.text(0),
            "stage_path": item.text(15),
            "runtime": item.text(12) or "-",
            "source": item.text(2) or parent.text(2) or "WS",
            "block": item.data(0, Qt.UserRole + 2) or parent.data(0, Qt.UserRole + 2) or "",
        }

    def _stage_tasks_from_be_item(self, item):
        tasks = []
        if not item:
            return tasks
        run = item.data(0, Qt.UserRole + 10) or {}
        if run.get("run_type") != "BE":
            return tasks
        if item.childCount() and item.child(0).data(0, Qt.UserRole) != "__PLACEHOLDER__":
            for i in range(item.childCount()):
                task = self._stage_task_from_item(item.child(i))
                if task:
                    tasks.append(task)
            return tasks
        be_path = item.text(15)
        block = run.get("block") or item.data(0, Qt.UserRole + 2) or ""
        source = run.get("source") or item.text(2) or "WS"
        for stage in run.get("stages", []):
            name = stage.get("name", "")
            if not name:
                continue
            info = stage.get("info", {}) or {}
            tasks.append({
                "name": item.text(0) + " / " + name,
                "be_name": item.text(0),
                "path": be_path,
                "run_type": "BE",
                "stage_name": name,
                "stage_path": stage.get("stage_path", ""),
                "runtime": info.get("runtime", "-"),
                "source": source,
                "block": block,
            })
        return tasks

    def _selected_stage_tasks(self):
        tasks = []
        seen = set()
        for item in self._checked_stage_items():
            task = self._stage_task_from_item(item)
            if not task:
                continue
            key = (task.get("path"), task.get("stage_name"), task.get("stage_path"))
            if key in seen:
                continue
            seen.add(key)
            tasks.append(task)
        return tasks

    def show_be_stage_summary_table(self, item=None):
        if item and item.data(0, Qt.UserRole) == "STAGE":
            tasks = self._selected_stage_tasks() or [self._stage_task_from_item(item)]
            title = "BE Stage Summary: selected stages"
        elif item and (item.data(0, Qt.UserRole + 10) or {}).get("run_type") == "BE":
            tasks = self._stage_tasks_from_be_item(item)
            title = "BE Stage Summary: " + item.text(0)
        else:
            tasks = self._selected_stage_tasks()
            title = "BE Stage Summary: selected stages"
        tasks = [t for t in tasks if t]
        if not tasks:
            QMessageBox.information(
                self, "BE Stage Summary",
                "Select or check BE stage rows, or right-click a BE run.")
            return
        dlg = BEStageSummaryDialog(title, tasks, self.is_dark_mode, self)
        dlg.exec_()

    def _metric_task_from_item(self, item):
        if not item:
            return None
        role = item.data(0, Qt.UserRole)
        if role == "STAGE":
            return self._stage_task_from_item(item)
        run = item.data(0, Qt.UserRole + 10) or {}
        path = item.text(15)
        if not path or path == "N/A":
            return None
        run_type = run.get("run_type") or ("FE" if item.text(0).endswith("-FE") else "")
        if run_type != "FE":
            return None
        return {
            "name": item.text(0),
            "path": path,
            "run_type": "FE",
            "stage_name": None,
            "source": run.get("source", item.text(2) or "WS"),
            "block": run.get("block", item.data(0, Qt.UserRole + 2) or ""),
        }

    def _num(self, value):
        if value is None:
            return None
        try:
            txt = str(value).replace(',', '').strip()
            m = re.search(r'[-+]?\d+(?:\.\d+)?', txt)
            return float(m.group(0)) if m else None
        except Exception:
            return None

    def _metric_value(self, metrics, key):
        metrics = metrics or {}
        area = metrics.get("area", {}) if isinstance(metrics.get("area", {}), dict) else {}
        vth = metrics.get("vth", {}) if isinstance(metrics.get("vth", {}), dict) else {}
        cong = metrics.get("congestion", {}) if isinstance(metrics.get("congestion", {}), dict) else {}
        if key == "std_cell_area":
            return area.get("std_cell_area", metrics.get("std_cell_area", "-"))
        if key == "instance_count":
            return area.get("instance_count", metrics.get("instance_count", "-"))
        if key == "gate_count":
            val = metrics.get("gate_count", "-")
            if val != "-":
                return val
            std_area = self._num(area.get("std_cell_area", "-"))
            factor = getattr(self, "gate_count_unit_area", 0.2419) or 0.2419
            return str(int(std_area / factor)) if std_area is not None and factor else "-"
        if key == "vth_area":
            if vth.get("stage_vt_area"):
                return vth.get("stage_vt_area")
            return vth.get("vt_area", vth.get("lvt_rvt_hvt_area", vth.get("lvt_rvt_area", "-")))
        if key == "vth_inst":
            if vth.get("stage_vt_inst"):
                return vth.get("stage_vt_inst")
            return vth.get("vt_inst", vth.get("lvt_rvt_hvt_inst", vth.get("lvt_rvt_inst", "-")))
        if key == "wns":
            return str(metrics.get("setup_r2r", metrics.get("r2r_setup", "-"))).split('/')[0]
        if key == "hold_wns":
            return str(metrics.get("hold_r2r", metrics.get("r2r_hold", metrics.get("hold_all", "-")))).split('/')[0]
        if key == "setup_total_wns":
            return str(metrics.get("setup_total", "-")).split('/')[0]
        if key == "hold_total_wns":
            return str(metrics.get("hold_total", metrics.get("hold_all", "-"))).split('/')[0]
        if key == "congestion":
            return cong.get("cong_both", metrics.get("congestion", "-"))
        if key == "std_count_area":
            return metrics.get("std_cell_count_area", "-")
        return metrics.get(key, "-")

    def _prepare_utility_dialog(self, dlg):
        try:
            dlg.setWindowFlags(
                dlg.windowFlags()
                | Qt.Window
                | Qt.WindowMaximizeButtonHint
                | Qt.WindowMinimizeButtonHint)
            dlg.setSizeGripEnabled(True)
        except Exception:
            pass

    def _make_table_user_adjustable(self, tbl, movable=True):
        try:
            hh = tbl.horizontalHeader()
            hh.setSectionsMovable(bool(movable))
            hh.setStretchLastSection(False)
            tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        except Exception:
            pass

    def _toggle_dialog_maximize(self, dlg, button=None,
                                max_text="Maximize",
                                restore_text="Restore"):
        """Maximize utility dialogs even on window managers that ignore
        showMaximized() for modal child dialogs."""
        try:
            if getattr(dlg, "_flowpulse_manual_maximized", False):
                geom = getattr(dlg, "_flowpulse_restore_geometry", None)
                try:
                    dlg.setWindowState(Qt.WindowNoState)
                except Exception:
                    pass
                if geom is not None:
                    dlg.setGeometry(geom)
                else:
                    dlg.showNormal()
                dlg._flowpulse_manual_maximized = False
                if button is not None:
                    button.setText(max_text)
                dlg.raise_()
                dlg.activateWindow()
                return
            dlg._flowpulse_restore_geometry = dlg.geometry()
            try:
                avail = QApplication.desktop().availableGeometry(dlg)
            except Exception:
                avail = QApplication.desktop().availableGeometry()
            try:
                dlg.setWindowState(Qt.WindowNoState)
            except Exception:
                pass
            dlg.setGeometry(avail)
            dlg._flowpulse_manual_maximized = True
            if button is not None:
                button.setText(restore_text)
            dlg.raise_()
            dlg.activateWindow()
        except Exception:
            try:
                if dlg.isMaximized():
                    dlg.showNormal()
                    if button is not None:
                        button.setText(max_text)
                else:
                    dlg.showMaximized()
                    if button is not None:
                        button.setText(restore_text)
            except Exception:
                pass

    def _add_standard_dialog_buttons(self, layout, dlg):
        row = QHBoxLayout()
        row.addStretch(1)
        max_btn = QPushButton("Maximize")
        close_btn = QPushButton("Close")
        def _toggle():
            self._toggle_dialog_maximize(dlg, max_btn, "Maximize", "Restore")
        max_btn.clicked.connect(_toggle)
        close_btn.clicked.connect(dlg.accept)
        row.addWidget(max_btn)
        row.addWidget(close_btn)
        layout.addLayout(row)
        return max_btn, close_btn

    def _clean_app_option_cell(self, value):
        text = str(value or "").strip()
        if not text:
            return ""
        # report_app_options sometimes inserts pipe/dash visual separators
        # inside long wrapped fields. They are layout artifacts, not values.
        text = re.sub(r'\s*[-]?\|[-]?\s*', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _show_metric_diff_dialog(self, title, rows, baseline_name=None):
        if not rows:
            QMessageBox.information(self, title, "No metrics were extracted.")
            return
        is_stage_diff = all(r.get("run_type") == "BE" for r in rows)
        if is_stage_diff:
            fields = [
                ("R2R Setup WNS", "wns"),
                ("Total Setup WNS", "setup_total_wns"),
                ("R2R Hold WNS", "hold_wns"),
                ("Total Hold WNS", "hold_total_wns"),
                ("Std Cell Area", "std_cell_area"),
                ("Gate Count", "gate_count"),
                ("Instance Count", "instance_count"),
                ("Runtime", "runtime"),
                ("Std Cell Count/Area", "std_count_area"),
                ("Std/StdOnly Util", "std_util_str"),
                ("Total Util", "total_util"),
                ("VT Inst %", "vth_inst"),
                ("VT Area %", "vth_area"),
                ("Congestion", "congestion"),
                ("Skew/Latency", "skew_latency"),
                ("Clock Repeater Count/Area", "clock_repeater_count_area"),
            ]
        else:
            fields = [
                ("Std Cell Area", "std_cell_area"),
                ("Gate Count", "gate_count"),
                ("Instance Count", "instance_count"),
                ("Runtime", "runtime"),
                ("R2R Setup WNS", "wns"),
                ("R2R Hold WNS", "hold_wns"),
                ("CGC %", "cgc"),
                ("MBIT %", "mbit"),
                ("Logic Depth", "logic_depth"),
                ("VT Area %", "vth_area"),
            ]
        dlg = QDialog(self)
        self._prepare_utility_dialog(dlg)
        dlg.setWindowTitle(title)
        dlg.resize(1100, 650)
        layout = QVBoxLayout(dlg)
        names = [r.get("name", "-") for r in rows]
        if baseline_name:
            layout.addWidget(QLabel("<b>Baseline:</b> " + baseline_name))
        layout.addWidget(QLabel("<b>Runs:</b> " + "  |  ".join(names)))

        col_count = 5 if len(rows) == 2 else len(rows) + 2
        tbl = QTableWidget(0, col_count)
        if len(rows) == 2:
            tbl.setHorizontalHeaderLabels(["Metric", names[0], names[1], "Delta", "Delta %"])
        else:
            tbl.setHorizontalHeaderLabels(["Metric"] + names + ["Worst Delta %"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        for c in range(1, tbl.columnCount()):
            tbl.horizontalHeader().setSectionResizeMode(c, QHeaderView.Stretch)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        self._make_table_user_adjustable(tbl)

        chart = _BarChartWidget("Metric Delta % (comparison vs baseline)")
        chart.setMinimumHeight(210)
        chart_labels, chart_values, chart_colors = [], [], []
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
                    delta = nums[1] - nums[0]
                    delta_txt = "{:+.4g}".format(delta)
                    if nums[0] != 0:
                        pct = (delta / abs(nums[0])) * 100.0
                        pct_txt = "{:+.2f}%".format(pct)
                        chart_labels.append(label.split()[0])
                        chart_values.append(pct)
                        chart_colors.append(QColor("#ef5350") if pct < 0 else QColor("#66bb6a"))
                tbl.setItem(r, 3, QTableWidgetItem(delta_txt))
                tbl.setItem(r, 4, QTableWidgetItem(pct_txt))
            else:
                worst = "-"
                base = nums[0]
                if base not in (None, 0):
                    pcts = [((n - base) / abs(base)) * 100.0 for n in nums[1:] if n is not None]
                    if pcts:
                        pct = max(pcts, key=lambda x: abs(x))
                        worst = "{:+.2f}%".format(pct)
                        chart_labels.append(label.split()[0])
                        chart_values.append(pct)
                        chart_colors.append(QColor("#ef5350") if pct < 0 else QColor("#66bb6a"))
                tbl.setItem(r, tbl.columnCount() - 1, QTableWidgetItem(worst))
        chart.set_data(chart_labels, chart_values, colors=chart_colors, is_dark=self.is_dark_mode)
        layout.addWidget(chart)
        layout.addWidget(tbl)
        self._add_standard_dialog_buttons(layout, dlg)
        dlg.exec_()

    def show_ror_metric_diff(self):
        tasks = []
        for item in self._checked_run_items() + self._checked_stage_items():
            task = self._metric_task_from_item(item)
            if task:
                tasks.append(task)
        if len(tasks) != 2:
            QMessageBox.information(
                self, "RoR Metric Diff",
                "Check exactly two FE runs or two stage rows for metric diff.")
            return
        if len(set(t.get("run_type") for t in tasks)) != 1:
            QMessageBox.information(
                self, "RoR Metric Diff",
                "Compare FE runs with FE runs, or PNR stage rows with PNR stage rows.")
            return
        self.status_bar.showMessage("Extracting metrics for RoR diff...")
        if not self._stop_worker_attr("_metric_batch_worker"):
            QMessageBox.information(
                self, "RoR Metric Diff",
                "Previous metric extraction is still running. Please try again in a moment.")
            return
        worker = MetricBatchWorker(tasks)
        self._metric_batch_worker = worker
        worker.finished.connect(self._on_ror_metric_done)
        worker.finished.connect(
            lambda *_args, ww=worker:
            self._clear_worker_attr_if_current("_metric_batch_worker", ww))
        worker.start()

    def _on_ror_metric_done(self, rows):
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_metric_batch_worker", None):
            return
        self.status_bar.showMessage("RoR metric diff ready", 3000)
        if not rows:
            QMessageBox.information(
                self, "RoR Metric Diff",
                "No metric rows were returned. The extraction may have been cancelled or no reports were found.")
            return
        self._show_metric_diff_dialog("RoR Metric Diff", rows)

    def _find_golden_item_for(self, target_item):
        target_run = target_item.data(0, Qt.UserRole + 10) or {}
        target_role = target_item.data(0, Qt.UserRole)
        target_block = (target_run.get("block")
                        or target_item.data(0, Qt.UserRole + 2)
                        or (target_item.parent().data(0, Qt.UserRole + 2)
                            if target_item.parent() else ""))
        target_metric = self._metric_task_from_item(target_item) or {}
        target_type = target_metric.get("run_type")
        found = [None]
        def walk(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if self.user_pins.get(c.text(15)) == "golden":
                    c_metric = self._metric_task_from_item(c) or {}
                    c_run = c.data(0, Qt.UserRole + 10) or {}
                    c_block = (c_run.get("block")
                               or c.data(0, Qt.UserRole + 2)
                               or (c.parent().data(0, Qt.UserRole + 2)
                                   if c.parent() else ""))
                    same_type = (not target_type
                                 or c_metric.get("run_type") == target_type)
                    same_block = (not target_block or c_block == target_block)
                    if same_type and (same_block or not found[0]):
                        found[0] = c
                walk(c)
        walk(self.tree.invisibleRootItem())
        return found[0]

    def show_golden_benchmark(self):
        checked = [i for i in (self._checked_run_items()
                               + self._checked_stage_items())
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
        if not self._stop_worker_attr("_metric_batch_worker"):
            QMessageBox.information(
                self, "Golden Benchmark",
                "Previous metric extraction is still running. Please try again in a moment.")
            return
        worker = MetricBatchWorker(tasks)
        self._metric_batch_worker = worker
        worker.finished.connect(self._on_golden_metric_done)
        worker.finished.connect(
            lambda *_args, ww=worker:
            self._clear_worker_attr_if_current("_metric_batch_worker", ww))
        worker.start()

    def _on_golden_metric_done(self, rows):
        sender = self.sender()
        if sender is not None and sender is not getattr(self, "_metric_batch_worker", None):
            return
        self.status_bar.showMessage("Golden benchmark ready", 3000)
        if not rows:
            QMessageBox.information(
                self, "Golden Benchmark",
                "No metric rows were returned. The extraction may have been cancelled or no reports were found.")
            return
        self._show_metric_diff_dialog(
            "Golden Benchmark", rows, baseline_name=rows[0].get("name", "Golden"))

    def _find_app_options_report(self, run_path, block):
        if not run_path or run_path == "N/A":
            return ""
        rpt_dir = os.path.join(run_path, "reports")
        if not os.path.isdir(rpt_dir):
            return ""
        patterns = []
        if block:
            patterns.append("report_app_options.full.{}.*.rpt".format(block))
        patterns.append("report_app_options.full.*.rpt")
        try:
            names = os.listdir(rpt_dir)
        except Exception:
            return ""
        hits = []
        for pat in patterns:
            for name in names:
                if fnmatch.fnmatch(name, pat):
                    hits.append(os.path.join(rpt_dir, name))
            if hits:
                break
        if not hits:
            return ""
        try:
            return sorted(hits, key=os.path.getmtime)[-1]
        except Exception:
            return sorted(hits)[-1]

    def _stage_report_dirs_for_item(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE" or not item.parent():
            return []
        stage = item.text(0)
        stage_path = item.text(15)
        be_path = item.parent().text(15)
        dirs = [
            os.path.join(be_path, "reports", stage),
            os.path.join(stage_path, "reports"),
            os.path.join(stage_path, "reports", stage),
            os.path.join(be_path, stage, "reports"),
            os.path.join(be_path, stage, "reports", stage),
            os.path.join(be_path, "reports"),
            stage_path,
        ]
        out = []
        for d in dirs:
            if d and d not in out:
                out.append(d)
        return out

    def _find_stage_app_options_report(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE":
            return ""
        stage = item.text(0)
        patterns = self._stage_app_options_patterns(stage)
        hits = []
        for d in self._stage_report_dirs_for_item(item):
            try:
                if not os.path.isdir(d):
                    continue
                names = os.listdir(d)
            except Exception:
                continue
            for pat in patterns:
                for name in names:
                    if fnmatch.fnmatch(name, pat):
                        hits.append(os.path.join(d, name))
                if hits:
                    break
            if hits:
                break
        if not hits:
            return ""
        try:
            return sorted(hits, key=os.path.getmtime)[-1]
        except Exception:
            return sorted(hits)[-1]

    def _stage_app_options_patterns(self, stage):
        return [
            "{}.opt.options.rpt".format(stage),
            "{}.env.app_options.full.rpt".format(stage),
            "{}env.app_options.full.rpt".format(stage),
            "*.opt.options.rpt",
            "*.env.app_options.full.rpt",
            "*env.app_options.full.rpt",
            "*.app_options.full.rpt",
        ]

    def show_stage_app_options_search_paths(self, item):
        if not item or item.data(0, Qt.UserRole) != "STAGE":
            QMessageBox.information(
                self, "App Options Search Paths",
                "Select or right-click a PNR stage row.")
            return
        stage = item.text(0)
        dirs = self._stage_report_dirs_for_item(item)
        patterns = self._stage_app_options_patterns(stage)
        found = self._find_stage_app_options_report(item)
        lines = [
            "Stage: {}".format(stage),
            "Found: {}".format(found if found else "NO MATCH"),
            "",
            "Directories tried:",
        ]
        lines.extend(["  " + d for d in dirs])
        lines.extend(["", "Filename patterns tried:"])
        lines.extend(["  " + p for p in patterns])
        QMessageBox.information(
            self, "App Options Search Paths", "\n".join(lines))

    def _parse_app_options_report(self, path):
        opts = {}
        if not path or not os.path.exists(path):
            return opts
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except Exception:
            return opts
        header = None
        positions = None
        for idx, line in enumerate(lines):
            if ("Name" in line and "Type" in line and "Value" in line
                    and "User-value" in line and "System-default" in line
                    and "Scope" in line and "Status" in line and "Source" in line):
                header = idx
                keys = ["Name", "Type", "Value", "User-value",
                        "User-default", "System-default",
                        "Scope", "Status", "Source"]
                pos = []
                for key in keys:
                    p = line.find(key)
                    if p < 0:
                        p = len(line)
                    pos.append(p)
                positions = pos
                break
        if header is not None and positions is not None:
            pos = positions + [None]
            for raw in lines[header + 1:]:
                line = raw.rstrip("\n")
                if not line.strip():
                    continue
                stripped = line.strip()
                if set(stripped) <= set("- "):
                    continue
                if stripped.startswith("*") or stripped.startswith("Report:"):
                    continue
                if len(line) <= pos[1]:
                    continue
                name = line[pos[0]:pos[1]].strip()
                typ = line[pos[1]:pos[2]].strip()
                if not name or not typ:
                    continue
                value = self._clean_app_option_cell(line[pos[2]:pos[3]])
                user_value = self._clean_app_option_cell(line[pos[3]:pos[4]])
                user_default = self._clean_app_option_cell(line[pos[4]:pos[5]])
                system_default = self._clean_app_option_cell(line[pos[5]:pos[6]])
                scope = self._clean_app_option_cell(line[pos[6]:pos[7]])
                status = self._clean_app_option_cell(line[pos[7]:pos[8]])
                source = self._clean_app_option_cell(line[pos[8]:])
                opts[name] = {
                    "type": typ,
                    "value": value,
                    "user_value": user_value,
                    "user_default": user_default,
                    "system_default": system_default,
                    "scope": scope,
                    "status": status,
                    "source": source,
                }
            if opts:
                return opts

        # Innovus stage report fallback:
        # Attribute Name | Data Type:Default | Current Value
        header = None
        positions = None
        for idx, line in enumerate(lines):
            if ("Attribute Name" in line and "Data Type:Default" in line
                    and "Current Value" in line):
                header = idx
                positions = [
                    line.find("Attribute Name"),
                    line.find("Data Type:Default"),
                    line.find("Current Value"),
                ]
                break
        if header is None or positions is None or min(positions) < 0:
            return opts
        p0, p1, p2 = positions
        for raw in lines[header + 1:]:
            line = raw.rstrip("\n")
            stripped = line.strip()
            if not stripped or set(stripped) <= set("- "):
                continue
            if stripped.startswith("*") or stripped.startswith("="):
                continue
            if len(line) <= p1:
                continue
            name = line[p0:p1].strip()
            typ_default = line[p1:p2].strip()
            value = self._clean_app_option_cell(line[p2:])
            if not name or not typ_default:
                continue
            typ = typ_default.split(":", 1)[0].strip() if ":" in typ_default else typ_default
            default = typ_default.split(":", 1)[1].strip() if ":" in typ_default else ""
            opts[name] = {
                "type": typ,
                "value": value,
                "user_value": value,
                "user_default": "-",
                "system_default": default,
                "scope": "-",
                "status": "-",
                "source": "-",
            }
        return opts

    def _app_option_value(self, opt, field_key):
        if not opt:
            return "-"
        val = opt.get(field_key, "")
        return val if str(val).strip() else "-"

    def show_app_options_diff(self):
        checked = []
        seen = set()
        for item in list(self._checked_run_items()) + list(self._checked_stage_items()):
            run = item.data(0, Qt.UserRole + 10) or {}
            path = item.text(15)
            role = item.data(0, Qt.UserRole)
            if role == "STAGE":
                parent = item.parent()
                key = (parent.text(15) if parent else "", item.text(0), path)
            else:
                key = (path, "", "")
            if not path or path == "N/A" or key in seen:
                continue
            seen.add(key)
            if role != "STAGE" and run.get("run_type") and run.get("run_type") != "FE":
                continue
            checked.append(item)
        if len(checked) < 2:
            QMessageBox.information(
                self, "App Options Diff",
                "Check 2 or more FE runs or BE stage rows, then open Utilities > App Options Diff.")
            return

        rows = []
        all_options = set()
        for item in checked:
            run = item.data(0, Qt.UserRole + 10) or {}
            is_stage = item.data(0, Qt.UserRole) == "STAGE"
            parent = item.parent() if is_stage else None
            block = (item.data(0, Qt.UserRole + 2)
                     or (parent.data(0, Qt.UserRole + 2) if parent else "")
                     or run.get("block") or "")
            rpt = (self._find_stage_app_options_report(item) if is_stage
                   else self._find_app_options_report(item.text(15), block))
            opts = self._parse_app_options_report(rpt) if rpt else {}
            all_options.update(opts.keys())
            rows.append({
                "name": (parent.text(0) + " / " + item.text(0)) if is_stage else item.text(0),
                "block": block,
                "path": item.text(15),
                "report": rpt,
                "options": opts,
            })
        if not all_options:
            missing = [r["name"] for r in rows if not r["report"]]
            msg = "No app-options reports were found for the selected runs/stages."
            if missing:
                msg += "\n\nMissing reports for:\n" + "\n".join(missing[:12])
            QMessageBox.information(self, "App Options Diff", msg)
            return

        dlg = QDialog(self)
        self._prepare_utility_dialog(dlg)
        dlg.setWindowTitle("App Options Diff  ({} item(s))".format(len(rows)))
        dlg.resize(min(520 + len(rows) * 190, 1600), 720)
        layout = QVBoxLayout(dlg)

        top = QHBoxLayout()
        diff_only_cb = QCheckBox("Show differences only")
        diff_only_cb.setChecked(True)
        field_combo = QComboBox()
        field_combo.addItem("Value", "value")
        field_combo.addItem("User-value", "user_value")
        field_combo.addItem("User-default", "user_default")
        field_combo.addItem("System-default", "system_default")
        field_combo.addItem("Scope", "scope")
        field_combo.addItem("Status", "status")
        field_combo.addItem("Source", "source")
        search = QLineEdit()
        search.setPlaceholderText("Search option name...")
        top.addWidget(diff_only_cb)
        top.addWidget(QLabel("Compare:"))
        top.addWidget(field_combo)
        top.addWidget(search, 1)
        layout.addLayout(top)

        info = QLabel("Reports: " + "  |  ".join(
            [os.path.basename(r["report"]) if r["report"] else r["name"] + ": MISSING"
             for r in rows[:5]]))
        info.setWordWrap(True)
        info.setStyleSheet("color: gray;")
        layout.addWidget(info)

        tbl = QTableWidget(0, len(rows) + 3)
        tbl.setHorizontalHeaderLabels(
            ["Option", "Type"] + [r["name"] for r in rows] + ["Status"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        for c in range(2, len(rows) + 2):
            tbl.horizontalHeader().setSectionResizeMode(c, QHeaderView.Interactive)
            tbl.setColumnWidth(c, 190)
        tbl.horizontalHeader().setSectionResizeMode(len(rows) + 2, QHeaderView.ResizeToContents)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)
        tbl.setSortingEnabled(False)
        tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._make_table_user_adjustable(tbl)
        layout.addWidget(tbl, 1)

        status_lbl = QLabel("")
        status_lbl.setStyleSheet("color: gray;")
        layout.addWidget(status_lbl)

        amber = QColor("#fff3e0")
        missing_bg = QColor("#eeeeee")
        dark_diff = QColor("#5a3c12")
        dark_missing = QColor("#3a3a3a")

        def _populate():
            field = field_combo.currentData()
            query = search.text().strip().lower()
            tbl.setRowCount(0)
            diff_count = 0
            same_count = 0
            missing_count = 0
            for opt_name in sorted(all_options):
                if query and query not in opt_name.lower():
                    continue
                vals = []
                types = []
                missing_here = False
                for r in rows:
                    opt = r["options"].get(opt_name)
                    if not opt:
                        missing_here = True
                    vals.append(self._app_option_value(opt, field))
                    if opt and opt.get("type"):
                        types.append(opt.get("type"))
                status = "MISSING" if missing_here else ("SAME" if len(set(vals)) == 1 else "DIFF")
                if status == "SAME":
                    same_count += 1
                elif status == "DIFF":
                    diff_count += 1
                else:
                    missing_count += 1
                if diff_only_cb.isChecked() and status == "SAME":
                    continue
                row = tbl.rowCount(); tbl.insertRow(row)
                tbl.setItem(row, 0, QTableWidgetItem(opt_name))
                tbl.setItem(row, 1, QTableWidgetItem(types[0] if types else "-"))
                for c, val in enumerate(vals):
                    cell = QTableWidgetItem(str(val))
                    cell.setToolTip(str(val))
                    if status == "DIFF":
                        cell.setBackground(dark_diff if self.is_dark_mode else amber)
                    elif status == "MISSING" and val == "-":
                        cell.setBackground(dark_missing if self.is_dark_mode else missing_bg)
                    tbl.setItem(row, c + 2, cell)
                st_item = QTableWidgetItem(status)
                if status == "DIFF":
                    st_item.setBackground(dark_diff if self.is_dark_mode else amber)
                elif status == "MISSING":
                    st_item.setBackground(dark_missing if self.is_dark_mode else missing_bg)
                tbl.setItem(row, len(rows) + 2, st_item)
            status_lbl.setText(
                "Shown: {} option(s). Different: {}. Missing: {}. Same: {}.".format(
                    tbl.rowCount(), diff_count, missing_count, same_count))

        diff_only_cb.toggled.connect(_populate)
        field_combo.currentIndexChanged.connect(_populate)
        search.textChanged.connect(_populate)
        _populate()

        self._add_standard_dialog_buttons(layout, dlg)
        dlg.exec_()
    def show_run_diff(self):
        """Compare N checked FE/BE run or PNR stage rows side-by-side."""
        checked = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (c.checkState(0) == Qt.Checked
                        and c.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
                         "__PLACEHOLDER__")):
                    checked.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())

        if len(checked) < 2:
            QMessageBox.information(
                self, "Compare Runs",
                "Please check 2 or more FE/BE run or PNR stage rows\n"
                "using the checkboxes, then click Compare Runs.")
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
        self._prepare_utility_dialog(dlg)
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
        self._make_table_user_adjustable(tbl)

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
        self._add_standard_dialog_buttons(layout, dlg)
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

# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Flow Pulse")
    window = PDDashboard()
    window.showMaximized()
    sys.exit(app.exec_())
