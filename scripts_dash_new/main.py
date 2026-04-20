# -*- coding: utf-8 -*-
# Singularity PD | Pro Edition -- main.py
# Pure ASCII comments only (Python 3.6 compatible on Linux)

import os
import re
import sys
import fnmatch
import subprocess
import csv
import getpass

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
from PyQt5.QtCore import Qt, QTimer, QDateTime, pyqtSignal, QThread
from PyQt5.QtGui import (QColor, QFont, QKeySequence, QBrush,
                         QPainter, QPen, QPixmap, QIcon)

from config import *
from utils import *
from workers import *
from widgets import *
from dialogs import *


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
        self.item_map               = {}
        self.ignored_paths          = set()
        self._checked_paths         = set()
        self.current_error_log_path = None
        self._building_tree         = False
        self._last_stylesheet       = ""
        self._columns_fitted_once   = False
        self._initial_size_calc_done= False
        self._last_scan_time        = ""
        self.run_filter_config      = None
        self.current_config_path    = None
        self.active_col_filters     = {}
        self._tree_builder          = None

        # -- milestone map (user-configurable in Settings > Milestones) --
        self._milestone_map = self._load_milestone_map()

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
        QTimer.singleShot(50, self.start_fs_scan)

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
        import json
        try:
            with open(self._history_file(), 'w') as f:
                json.dump(self._run_history, f, indent=2)
        except Exception:
            pass

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
    # CLOSURE PASS (deferred -- runs after tree is fully painted)
    # ------------------------------------------------------------------
    def _run_closure_pass(self):
        """Apply closure scorecard + regression detection to all FE items.
        Deferred via QTimer so it doesn't block the initial tree paint.
        Only walks run items (skips group nodes) for maximum speed."""
        GROUP = frozenset(("BLOCK","MILESTONE","RTL",
                           "IGNORED_ROOT","STAGE","__PLACEHOLDER__"))
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
        px = QPixmap(size, size)
        px.fill(Qt.transparent)
        p = QPainter(px)
        p.setRenderHint(QPainter.Antialiasing)
        p.setBrush(QBrush(QColor(fill)))
        p.setPen(QPen(QColor(border), 1.2))
        p.drawEllipse(1, 1, size - 2, size - 2)
        p.end()
        return QIcon(px)

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
            "Running Only", "Failed Only", "Today's Runs"])
        self.view_combo.setFixedWidth(120)
        self.view_combo.currentIndexChanged.connect(self.refresh_view)
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
        self.actions_menu.addAction("Compare Selected Runs",   self.show_run_diff)
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
        self.badge_notstart  = _badge("Not Started: 0", "#757575", "All Runs")
        self.badge_total     = _badge("Total FE: 0",  "#5c5c5c", "All Runs")
        for b in [self.badge_completed, self.badge_running, self.badge_failed,
                  self.badge_notstart, self.badge_total]:
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
            lambda: self.tree.resizeColumnToContents(0))
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
        GROUP = frozenset(("BLOCK","MILESTONE","RTL","IGNORED_ROOT",
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
            if log and log != "N/A" and os.path.exists(log):
                subprocess.Popen(['gvim', log])

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
        _restyle(self.badge_notstart,  f"Not Started: {not_started}", "#757575")
        _restyle(self.badge_total,     f"Total FE: {total}", "#5c5c5c")

    # ------------------------------------------------------------------
    # ITEM CHECK
    # ------------------------------------------------------------------
    def _on_item_check_changed(self, item, col=0):
        if self._building_tree:
            return
        if col != 0:
            return
        path = item.text(15)
        if not path or path == "N/A":
            return
        if item.checkState(0) == Qt.Checked:
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
        self.meta_log.setText(item.text(16))
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
            hist_txt = ""
            if run_data and self._run_history:
                hist_txt = self._get_run_history_text(run_data)
            reg_part = (
                f"<br><span style='color:#f57c00'>"
                f"[!] {reg_msg}</span>"
                if reg_msg else "")
            hist_part = (
                f"<br><small><b>History (last 5):</b><br>"
                + hist_txt.replace("\n", "<br>") + "</small>"
                if hist_txt and hist_txt != "No history yet." else "")
            self.ins_lbl.setText(
                f"<b>Run:</b> {run_name}<br><b>RTL:</b> {rtl}"
                f"{reg_part}{hist_part}")
            self._current_note_id = f"{rtl} : {run_name}"

        notes      = self.global_notes.get(self._current_note_id, [])
        clean_text = "\n".join(notes)
        tag        = f"[{getpass.getuser()}]"
        for line in notes:
            if line.startswith(tag):
                clean_text = line.replace(tag, "").strip()
                break
        self.ins_note.setPlainText(clean_text)

        # Read pre-cached error count (zero file I/O)
        if len(sel) == 1 and not is_stage and path and path != "N/A":
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

    def on_item_double_clicked(self, item, col):
        log = item.text(16)
        if log and log != "N/A" and os.path.exists(log):
            subprocess.Popen(['gvim', log])

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
        cb_style = """
            QTreeView::indicator:checked   { background-color: #4CAF50; border: 1px solid #388E3C; image: none; }
            QTreeView::indicator:unchecked { background-color: white;   border: 1px solid gray; }
        """
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

        self._building_tree = True
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
                for (blk, fe_base), fe_rtl in fe_info.items():
                    if run["block"] == blk and fe_name_from_be == fe_base:
                        run["rtl"] = fe_rtl
                        break

        root     = self.tree.invisibleRootItem()
        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

        # Pre-compute base_rtl and milestone per unique RTL string
        _rtl_cache = {}
        for run in runs_to_process:
            rtl = run["rtl"]
            if rtl not in _rtl_cache:
                base = re.sub(r'_syn\d+$', '', rtl)
                # Use user-configurable milestone map
                ms   = self.get_milestone_label(base)
                _rtl_cache[rtl] = (base, base != rtl, ms)

        _ignored  = self.ignored_paths
        _hide_blk = self.hide_block_nodes

        # CRITICAL: process FE runs first so FE tree items exist
        # before any BE/innovus run tries to find its FE parent.
        # Without this, BE runs processed before their FE run silently
        # attach to the RTL node instead of the FE item.
        runs_fe = [r for r in runs_to_process if r["run_type"] == "FE"]
        runs_be = [r for r in runs_to_process if r["run_type"] != "FE"]
        ordered_runs = runs_fe + runs_be

        _item_count = 0
        for run in ordered_runs:
            run_rtl = run["rtl"]
            base_rtl, has_syn, milestone = _rtl_cache.get(
                run_rtl, (run_rtl, False, None))
            if milestone is None:
                continue

            _item_count += 1
            if _item_count % 100 == 0:
                QApplication.processEvents()

            is_ignored  = run["path"] in _ignored
            attach_root = ign_root if is_ignored else root
            blk_name    = run["block"]

            base_attach = (attach_root if _hide_blk
                           else self._get_node(attach_root, blk_name, "BLOCK"))

            m_node = self._get_node(base_attach, milestone, "MILESTONE")
            # All syn* variants placed directly under base RTL node
            parent_for_run = self._get_node(m_node, base_rtl, "RTL")

            if run["run_type"] == "FE":
                run_item = self._create_run_item(parent_for_run, run)
                run_item.setData(0, Qt.UserRole + 10, run)

            elif run["run_type"] == "BE":
                # Pre-compute FE name from BE run name.
                # FE names have NO underscores -- split at first _ after EVT prefix.
                _r = re.sub(r'^EVT\d+_ML\d+_DEV\d+(?:_syn\d+)?_', '', run["r_name"])
                _idx = _r.find('_')
                if _idx == -1:
                    # Direct fc BE: run1-BE -> run1
                    fe_name_from_be = _r[:-3] if _r.endswith('-BE') else _r
                else:
                    fe_name_from_be = _r[:_idx]  # e.g. "M2D2S2-mohit-bhar-..."
                be_source = run["source"]
                be_block  = run["block"]

                def _find_fe_parent(search_node):
                    """Search for matching FE run.
                    Uses exact name match: after stripping EVT prefix,
                    split at first underscore to extract FE name,
                    then compare directly with FE item text."""
                    for i in range(search_node.childCount()):
                        c = search_node.child(i)
                        nt = c.data(0, Qt.UserRole)
                        if nt in ("RTL", "MILESTONE"):
                            found = _find_fe_parent(c)
                            if found:
                                return found
                        if nt in ("STAGE","__PLACEHOLDER__","BLOCK",
                                  "MILESTONE","RTL","IGNORED_ROOT"):
                            continue
                        # FE candidate -- must be from same source
                        fe_source = c.text(2).strip()
                        source_ok = (fe_source == be_source
                                     or not fe_source or not be_source)
                        if not source_ok:
                            continue
                        if c.data(0, Qt.UserRole + 2) != be_block:
                            continue
                        fe_text = c.text(0)
                        fe_base = fe_text[:-3] if fe_text.endswith("-FE") else fe_text
                        # Exact match using first-underscore split rule
                        if fe_name_from_be == fe_base:
                            return c
                    return None

                # First try the same RTL node (fast path)
                fe_parent = _find_fe_parent(parent_for_run)
                # If not found, search the entire block subtree
                # (handles innovus runs with different RTL than FE)
                if fe_parent is None:
                    fe_parent = _find_fe_parent(base_attach)

                actual_parent = fe_parent if fe_parent else parent_for_run
                be_item = self._create_run_item(actual_parent, run)
                be_item.setData(0, Qt.UserRole + 10, run)
                if run.get("stages"):
                    ph = QTreeWidgetItem(be_item)
                    ph.setText(0, "Loading stages...")
                    ph.setData(0, Qt.UserRole, "__PLACEHOLDER__")
                    ph.setFlags(Qt.NoItemFlags)
                    be_item.setData(0, Qt.UserRole + 11, run)

        if ign_root.childCount() == 0:
            root.removeChild(ign_root)

        # Apply pin icons
        def update_nodes(node):
            if node.data(0, Qt.UserRole) not in (
                    "BLOCK", "RTL", "MILESTONE", "IGNORED_ROOT"):
                pin_type = self.user_pins.get(node.text(15))
                if pin_type in self.icons:
                    node.setIcon(0, self.icons[pin_type])
                    node.setData(0, Qt.UserRole + 5, pin_type)
                else:
                    node.setIcon(0, QIcon())
                    node.setData(0, Qt.UserRole + 5, None)
            for i in range(node.childCount()):
                update_nodes(node.child(i))
        update_nodes(root)

        self.tree.setSortingEnabled(True)
        # Default sort: Run Name column A-Z ascending
        self.tree.sortByColumn(0, Qt.AscendingOrder)
        self.tree.header().setSortIndicator(0, Qt.AscendingOrder)
        self.tree.setUpdatesEnabled(True)
        self.tree.blockSignals(False)
        self._building_tree = False

        if not self._columns_fitted_once:
            self._columns_fitted_once = True
            self.fit_all_columns()
        if not self._initial_size_calc_done:
            self._initial_size_calc_done = True
            self.calculate_all_sizes()

        all_owners = set()
        for r in (self.ws_data.get("all_runs", []) +
                  self.out_data.get("all_runs", [])):
            if r.get("owner") and r["owner"] != "Unknown":
                all_owners.add(r["owner"])
        if all_owners:
            save_mail_users_config(all_owners)

        QApplication.processEvents()
        self.refresh_view()
        # Defer closure+regression scan -- runs after UI is responsive
        # Uses QTimer so tree is fully painted before the extra pass
        QTimer.singleShot(200, self._run_closure_pass)
        # Auto-expand to RTL level on first load only
        if not hasattr(self, '_auto_expanded_once'):
            self._auto_expanded_once = True
            QTimer.singleShot(50, self._expand_to_rtl_level)

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
            if self.convert_to_ist:
                start_raw = convert_kst_to_ist_str(start_raw)
                end_raw   = convert_kst_to_ist_str(end_raw)
            if self.show_relative_time:
                child.setText(13, relative_time(start_raw))
            else:
                child.setText(13, start_raw)
            child.setText(14, end_raw)
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
            child.setText(12, run.get("info", {}).get("runtime", "-"))
            child.setText(13, run.get("info", {}).get("start", "-"))
            child.setText(14, run.get("info", {}).get("end", "-"))

        child.setData(0, Qt.UserRole, "STAGE"
                      if run["run_type"] == "STAGE" else None)

        tooltip_text += f"\nSize: -\n"
        child.setToolTip(0, tooltip_text)
        child.setExpanded(False)

        # Pre-compute error log count at build time (zero I/O on selection)
        err_count = None
        if (run["run_type"] == "FE"
                and run.get("path") and run["path"] != "N/A"):
            err_file = os.path.join(run["path"], "logs",
                                    "compile_opt.error.log")
            if os.path.exists(err_file):
                try:
                    with open(err_file, 'r',
                              encoding='utf-8', errors='ignore') as _ef:
                        err_count = sum(1 for ln in _ef if ln.strip())
                except Exception:
                    err_count = 0
        child.setData(0, Qt.UserRole + 12, err_count)

        # Pre-compute path existence flags for context menu (no NFS on right-click)
        child.setData(0, Qt.UserRole + 20, {
            'run_path': bool(run.get("path") and run["path"] != "N/A"),
            'log':      bool(run.get("path") and run["path"] != "N/A"),
            'fm_n':     cached_exists(run.get("fm_n_path", "")),
            'fm_u':     cached_exists(run.get("fm_u_path", "")),
            'vslp':     cached_exists(run.get("vslp_rpt_path", "")),
        })

        if run["source"] == "OUTFEED":
            child.setForeground(
                2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))
        else:
            child.setForeground(
                2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))

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
            s_item.setText(0, stage.get("name", ""))
            s_item.setText(7,  f"NONUPF - {stage.get('st_n', '')}")
            s_item.setText(8,  f"UPF - {stage.get('st_u', '')}")
            s_item.setText(9,  stage.get("vslp_status", ""))
            s_item.setText(12, stage.get("info", {}).get("runtime", ""))
            s_item.setText(13, stage.get("info", {}).get("start",   ""))
            s_item.setText(14, stage.get("info", {}).get("end",     ""))
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
                    item.removeChild(ph)
                    self._add_stages(item, be_run, ign_root)

    # ------------------------------------------------------------------
    # REFRESH VIEW (pure hide/show -- zero item creation)
    # ------------------------------------------------------------------
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
        _fe_only     = (preset == "FE Only")
        _be_only     = (preset == "BE Only")
        _run_only    = (preset == "Running Only")
        _fail_only   = (preset == "Failed Only")
        _today_only  = (preset == "Today's Runs")
        _pins        = self.user_pins
        _rfc         = self.run_filter_config
        _notes       = self.global_notes

        def _passes(run):
            if run is None:
                return False
            src = run["source"]
            if _src_ws  and src != "WS":      return False
            if _src_out and src != "OUTFEED": return False
            path = run["path"]
            is_golden = (_pins.get(path) == "golden")
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
            if _fe_only  and rt_type != "FE": return False
            if _be_only  and rt_type != "BE": return False
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
                    f"{run.get('st_n','')} {run.get('st_u','')} "
                    f"{run.get('vslp_status','')} "
                    f"{run['info']['runtime']} {run['info']['start']} "
                    f"{run['info']['end']} {notes}").lower()
                if not fnmatch.fnmatch(combined, search_pattern):
                    if rt_type == "BE":
                        if not any(
                            fnmatch.fnmatch(
                                f"{s['name']} {s['st_n']} {s['st_u']} "
                                f"{s['vslp_status']} "
                                f"{s['info']['runtime']}".lower(),
                                search_pattern)
                            for s in run.get("stages",[])):
                            return False
                    else:
                        return False
            return True

        _GROUP_TYPES = frozenset(
            ("BLOCK","MILESTONE","RTL","IGNORED_ROOT"))
        _expand_when_filtered = (preset != "All Runs" or bool(raw_query))
        _UR   = Qt.UserRole
        _UR10 = Qt.UserRole + 10

        def _update_visibility(item):
            node_type = item.data(0, _UR)
            if node_type == "__PLACEHOLDER__":
                item.setHidden(True)
                return False
            if node_type in _GROUP_TYPES:
                any_visible = False
                for i in range(item.childCount()):
                    if _update_visibility(item.child(i)):
                        any_visible = True
                item.setHidden(not any_visible)
                if any_visible and _expand_when_filtered:
                    item.setExpanded(True)
                return any_visible
            else:
                run    = item.data(0, _UR10)
                passes = _passes(run)
                item.setHidden(not passes)
                if passes and run:
                    visible_runs.append(run)
                for i in range(item.childCount()):
                    ch = item.child(i)
                    if ch.data(0, _UR) == "__PLACEHOLDER__":
                        ch.setHidden(True)
                    else:
                        ch.setHidden(not passes)
                return passes

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            _update_visibility(root.child(i))

        if self.active_col_filters:
            self.apply_tree_filters()

        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)
        QApplication.processEvents()
        # FEAT 6: Show search result count when search is active
        if raw_query:
            fe_visible = sum(1 for r in visible_runs
                             if r.get("run_type") == "FE")
            self.search_count_lbl.setText(f"{fe_visible} found")
            self.search_count_lbl.setVisible(True)
        else:
            self.search_count_lbl.setVisible(False)

        self._update_status_bar(visible_runs)
        self.on_tree_selection_changed()

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

        ignore_checked_act = m.addAction("Hide All Checked Runs/Stages")
        m.addSeparator()

        ignore_act = restore_act = None
        target_path = item.text(15)
        if target_path and target_path != "N/A":
            if target_path in self.ignored_paths:
                restore_act = m.addAction("Restore (Unhide)")
            else:
                ignore_act = m.addAction("Hide/Ignore")
            m.addSeparator()

        _flags = target_item.data(0, Qt.UserRole + 20) or {}
        def _ex(path):
            return bool(path and path != "N/A" and cached_exists(path))

        calc_size_act = (m.addAction("Calculate Folder Size")
                         if run_path and run_path != "N/A"
                         and cached_exists(run_path) else None)
        if calc_size_act: m.addSeparator()

        fm_n_act    = m.addAction("Open NONUPF Formality Report") if (_flags.get('fm_n') or _ex(fm_n_path)) else None
        fm_u_act    = m.addAction("Open UPF Formality Report")    if (_flags.get('fm_u') or _ex(fm_u_path)) else None
        v_act       = m.addAction("Open VSLP Report")             if (_flags.get('vslp') or _ex(vslp_path)) else None
        sta_act     = m.addAction("Open PT STA Summary")          if _ex(sta_path)  else None
        ir_stat_act = m.addAction("Open Static IR Log")           if _ex(ir_path)   else None
        ir_dyn_act  = m.addAction("Open Dynamic IR Log")          if (is_stage and _ex(ir_path)) else None
        log_act     = m.addAction("Open Log File")                if (_flags.get('log') or _ex(log_path)) else None

        m.addSeparator()
        qor_act = None
        if is_stage:
            m.addSeparator()
            qor_act = m.addAction("Run Single Stage QoR")

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
                self.refresh_view()

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
            def ig(node):
                for i in range(node.childCount()):
                    c = node.child(i)
                    if c.checkState(0) == Qt.Checked:
                        p = c.text(15)
                        if p and p != "N/A":
                            self.ignored_paths.add(p)
                    ig(c)
            ig(self.tree.invisibleRootItem())
            self.refresh_view()

        elif res == ignore_act:
            self.ignored_paths.add(target_path)
            item.setHidden(True)
            parent = item.parent()
            if parent and all(
                    parent.child(i).isHidden()
                    for i in range(parent.childCount())):
                parent.setHidden(True)

        elif res == restore_act:
            self.ignored_paths.discard(target_path)
            item.setHidden(False)
            p = item.parent()
            while p and p.parent():
                p.setHidden(False)
                p = p.parent()

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

        elif fm_n_act     and res == fm_n_act:     subprocess.Popen(['gvim', fm_n_path])
        elif fm_u_act     and res == fm_u_act:     subprocess.Popen(['gvim', fm_u_path])
        elif v_act        and res == v_act:        subprocess.Popen(['gvim', vslp_path])
        elif sta_act      and res == sta_act:      subprocess.Popen(['gvim', sta_path])
        elif ir_stat_act  and res == ir_stat_act:  subprocess.Popen(['gvim', ir_path])
        elif ir_dyn_act   and res == ir_dyn_act:   subprocess.Popen(['gvim', ir_path])
        elif log_act      and res == log_act:      subprocess.Popen(['gvim', log_path])
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
            worker.size_calculated.connect(self.update_item_size)
            self.size_workers.append(worker)
            worker.finished.connect(
                lambda w=worker: self.size_workers.remove(w)
                if w in self.size_workers else None)
            worker.start()

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

    # ------------------------------------------------------------------
    # CSV EXPORT
    # ------------------------------------------------------------------
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
            with open(path) as f:
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
        with open(self.current_config_path, 'w') as f:
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
        from PyQt5.QtWidgets import QDateEdit
        from PyQt5.QtCore import QDate
        gen_l.addRow(QLabel("--- Tapeout ---"))
        tapeout_edit = QDateEdit()
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

        outer.addWidget(tabs)
        btn_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        outer.addWidget(btn_box)

        if not dlg.exec_():
            return

        # Apply general settings
        font = font_combo.currentFont()
        font.setPointSize(size_spin.value())
        QApplication.setFont(font)
        self.is_dark_mode       = theme_cb.isChecked()
        self.use_custom_colors  = use_custom_cb.isChecked()
        self.custom_bg_color    = _colors[0]
        self.custom_fg_color    = _colors[1]
        self.custom_sel_color   = _colors[2]
        self.row_spacing        = space_spin.value()
        self.show_relative_time = rel_time_cb.isChecked()
        self.convert_to_ist     = ist_cb.isChecked()
        self.hide_block_nodes   = hide_blk_cb.isChecked()

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
        self.refresh_view()
        current_mode = self.mode_combo.currentText()
        self._set_col_preset(
            {"Standard": 2, "Compact": 1, "Full": 3}.get(current_mode, 2))

    # ------------------------------------------------------------------
    # DISK USAGE
    # ------------------------------------------------------------------
    def open_disk_usage(self):
        if not hasattr(self, '_disk_data') or not self._disk_data:
            QMessageBox.information(
                self, "Disk Space",
                "Disk scan not yet complete. Please wait or press Refresh.")
            return
        dlg = DiskUsageDialog(self._disk_data, self.is_dark_mode, self)
        dlg.exec_()

    def start_bg_disk_scan(self, force=False):
        if (not force and hasattr(self, '_disk_scan_worker')
                and self._disk_scan_worker.isRunning()):
            return
        self._disk_scan_worker = DiskScannerWorker()
        self._disk_scan_worker.finished.connect(self._on_disk_scan_done)
        self._disk_scan_worker.start()

    def _on_disk_scan_done(self, data):
        self._disk_data = data

    # ------------------------------------------------------------------
    # QoR
    # ------------------------------------------------------------------
    def run_qor_comparison(self):
        checked_paths = [
            item.text(15)
            for item in self._iter_checked_items()
            if item.text(15) and item.text(15) != "N/A"]
        if len(checked_paths) < 2:
            QMessageBox.information(
                self, "QoR Compare",
                "Please check at least 2 runs first.")
            return
        try:
            script = QOR_SUMMARY_SCRIPT
        except NameError:
            QMessageBox.warning(
                self, "QoR Compare",
                "QOR_SUMMARY_SCRIPT is not defined in config.py.\n"
                "Please add it to config.py:\n\n"
                "QOR_SUMMARY_SCRIPT = '/path/to/summary.py'")
            return
        if not os.path.exists(script):
            QMessageBox.warning(
                self, "QoR Compare", f"Script not found:\n{script}")
            return
        worker = QoRWorker(script, checked_paths)
        worker.finished.connect(self._on_qor_done)
        worker.start()
        self._qor_worker = worker

    def _on_qor_done(self, html_path):
        if html_path and os.path.exists(html_path):
            subprocess.Popen(['firefox', html_path])
        else:
            QMessageBox.warning(
                self, "QoR Compare", "QoR script did not produce output.")

    def _run_single_stage_qor(self, item, b_name, r_rtl, base_run):
        stage_name = item.text(0)
        stage_path = item.text(21)
        # QOR_SUMMARY_SCRIPT may not be defined in config for all projects
        try:
            script = QOR_SUMMARY_SCRIPT
        except NameError:
            QMessageBox.warning(
                self, "QoR",
                "QOR_SUMMARY_SCRIPT is not defined in config.py.\n"
                "Please add it to config.py:\n\n"
                "QOR_SUMMARY_SCRIPT = '/path/to/summary.py'")
            return
        if not os.path.exists(script):
            QMessageBox.warning(
                self, "QoR", f"Script not found:\n{script}")
            return
        worker = QoRWorker(script, [stage_path])
        worker.finished.connect(self._on_qor_done)
        worker.start()
        self._qor_worker = worker

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
        checked = self._iter_checked_items()
        if not checked:
            QMessageBox.information(self, "Cleanup Mail",
                                    "Please check some runs first.")
            return
        paths = [c.text(15) for c in checked
                 if c.text(15) and c.text(15) != "N/A"
                 and self.user_pins.get(c.text(15)) != "golden"]
        if not paths:
            QMessageBox.information(self, "Cleanup Mail",
                                    "No non-golden runs selected.")
            return
        dlg = CleanupMailDialog(paths, self)
        dlg.exec_()

    def send_qor_mail_action(self):
        dlg = QoRMailDialog(self)
        dlg.exec_()

    def send_custom_mail_action(self):
        dlg = CustomMailDialog(self)
        dlg.exec_()

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

        # Filter bar -- Source and Run Type
        filter_bar = QHBoxLayout()
        filter_bar.addWidget(QLabel("<b>Source:</b>"))
        src_filter = QComboBox()
        src_filter.addItems(["ALL", "WS", "OUTFEED"])
        src_filter.setFixedWidth(100)
        filter_bar.addWidget(src_filter)
        filter_bar.addSpacing(20)
        filter_bar.addWidget(QLabel("<b>Run Type:</b>"))
        type_filter = QComboBox()
        type_filter.addItems(["FE + BE", "FE Only", "BE Only"])
        type_filter.setFixedWidth(100)
        filter_bar.addWidget(type_filter)
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


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Singularity PD")
    window = PDDashboard()
    window.show()
    sys.exit(app.exec_())
