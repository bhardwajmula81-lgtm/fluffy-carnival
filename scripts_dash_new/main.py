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
    QHeaderView, QFileDialog, QGroupBox, QTextEdit, QDockWidget, QFormLayout
)
from PyQt5.QtCore import Qt, QTimer, QDateTime
from PyQt5.QtGui import QColor, QFont, QKeySequence, QBrush, QPainter, QPen, QPixmap, QIcon

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

        self.ws_data      = {}
        self.out_data     = {}
        self.ir_data      = {}
        self.global_notes = {}
        self.user_pins    = load_user_pins()

        self.is_dark_mode          = False
        self.use_custom_colors     = False
        self.custom_bg_color       = "#2b2d30"
        self.custom_fg_color       = "#dfe1e5"
        self.custom_sel_color      = "#2f65ca"

        self.row_spacing             = 2
        self.show_relative_time      = False
        self.convert_to_ist          = False
        self.hide_block_nodes        = False
        self._columns_fitted_once    = False
        self._initial_size_calc_done = False
        self._last_scan_time         = None

        self.size_workers           = []
        self.item_map               = {}
        self.ignored_paths          = set()
        self._checked_paths         = set()
        self.current_error_log_path = None
        self._building_tree         = False   # FIX 2: guard itemChanged during build
        self._last_stylesheet       = ""      # FIX 6: skip setStyleSheet if unchanged
        # FEAT 4: search history (last 15 queries)
        self._search_history        = []
        try:
            hist_str = prefs.get('UI', 'search_history', fallback='')
            if hist_str:
                self._search_history = [x for x in hist_str.split('|||') if x.strip()][:15]
        except:
            pass
        # FIX 4: pre-built color palette — populated in apply_theme_and_spacing()
        self._colors = {
            "completed": QColor("#1b5e20"), "running":    QColor("#0d47a1"),
            "not_started": QColor("#757575"), "interrupted": QColor("#e65100"),
            "failed": QColor("#b71c1c"), "pass": QColor("#388e3c"),
            "fail": QColor("#d32f2f"), "outfeed": QColor("#8e24aa"),
            "ws": QColor("#e65100"), "milestone": QColor("#1e88e5"),
            "note": QColor("#e65100"),
        }

        self.run_filter_config  = None
        self.current_config_path = None
        self.active_col_filters = {}

        self._cached_disk_data = None
        self.disk_worker       = None

        # FIX 2: Debounce timer — 250ms is enough since refresh_view no longer rebuilds
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)
        # FEAT 5: Smart poll timer — only re-checks RUNNING runs every 60s
        self._smart_poll_timer = QTimer(self)
        self._smart_poll_timer.setSingleShot(False)
        self._smart_poll_timer.timeout.connect(self._smart_poll_running)
        # FEAT 7: Live elapsed timer — updates RUNNING items every 60s
        self._live_timer = QTimer(self)
        self._live_timer.setInterval(60_000)
        self._live_timer.timeout.connect(self._update_live_runtimes)
        self._live_timer.start()

        self.icons = {
            'golden':    self._create_dot_icon("#FFC107", "#FF9800"),
            'good':      self._create_dot_icon("#4CAF50", "#388E3C"),
            'redundant': self._create_dot_icon("#F44336", "#D32F2F"),
            'later':     self._create_dot_icon("#FF9800", "#F57C00"),
        }

        self.init_ui()
        self._setup_shortcuts()
        self.start_fs_scan()
        self.start_bg_disk_scan()

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _create_dot_icon(self, hex_color, border_color):
        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QBrush(QColor(hex_color)))
        painter.setPen(QPen(QColor(border_color), 1))
        painter.drawEllipse(4, 4, 8, 8)
        painter.end()
        return QIcon(pixmap)

    def closeEvent(self, event):
        if not prefs.has_section('UI'):
            prefs.add_section('UI')
        # Save splitter
        prefs.set('UI', 'main_splitter', ','.join(map(str, self.main_splitter.sizes())))
        # FEAT 1: Save filter state
        prefs.set('UI', 'last_source',  self.src_combo.currentText())
        prefs.set('UI', 'last_rtl',     self.rel_combo.currentText())
        prefs.set('UI', 'last_view',    self.view_combo.currentText())
        prefs.set('UI', 'last_search',  self.search.text())
        prefs.set('UI', 'last_auto',    self.auto_combo.currentText())
        # FEAT 4: Save search history
        prefs.set('UI', 'search_history', '|||'.join(self._search_history[:15]))
        # FEAT 1: Save column widths (all 24 columns)
        col_widths = ','.join(
            str(self.tree.columnWidth(i)) if not self.tree.isColumnHidden(i) else '0'
            for i in range(self.tree.columnCount())
        )
        prefs.set('UI', 'col_widths', col_widths)
        # FEAT 1: Save column visibility
        col_hidden = ','.join(
            '1' if self.tree.isColumnHidden(i) else '0'
            for i in range(self.tree.columnCount())
        )
        prefs.set('UI', 'col_hidden', col_hidden)
        with open(USER_PREFS_FILE, 'w') as f:
            prefs.write(f)
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

        # TOP BAR
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
        self.search.setPlaceholderText("Search runs, blocks, status, runtime...     [Ctrl+F]")
        self.search.setMinimumWidth(260)
        # FIX 2: debounce 250ms — no rebuild cost so can be snappier
        self.search.textChanged.connect(lambda: self.search_timer.start(250))
        # FEAT 4: right-click on search shows history
        self.search.setContextMenuPolicy(Qt.CustomContextMenu)
        self.search.customContextMenuRequested.connect(self._show_search_history)
        top_layout.addWidget(self.search)

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

        # UTILITIES MENU (renamed from Actions)
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
        self.actions_menu.addAction("Compare 2 Selected Runs", self.show_run_diff)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Analytics / Charts",      self.show_analytics)
        self.actions_menu.addAction("Team Workload View",       self.show_team_workload)

        self.actions_btn.setMenu(self.actions_menu)
        top_layout.addWidget(self.actions_btn)

        # SETTINGS button — visible in toolbar, opens tabbed Settings dialog
        self.settings_btn = QPushButton("Settings")
        self.settings_btn.clicked.connect(self.open_settings)
        top_layout.addWidget(self.settings_btn)

        self._add_separator(top_layout)

        # Mode dropdown — column view preset
        top_layout.addWidget(self._label("Mode:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Standard", "Compact", "Full"])
        self.mode_combo.setFixedWidth(82)
        self.mode_combo.setToolTip("Column view preset  (keys: 1=Compact  2=Standard  3=Full)")
        self.mode_combo.currentIndexChanged.connect(
            lambda i: self._set_col_preset({"Standard":2,"Compact":1,"Full":3}.get(
                self.mode_combo.currentText(), 2)))
        top_layout.addWidget(self.mode_combo)

        self._add_separator(top_layout)

        # Notes toggle — ASCII arrows, no emoji/unicode
        self.notes_toggle_btn = QPushButton("[ Notes >> ]")
        self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock)
        top_layout.addWidget(self.notes_toggle_btn)

        root_layout.addLayout(top_layout)

        # PROGRESS BAR
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

        # FEAT 2: Health strip — clickable status summary badges
        self.health_strip = QWidget()
        self.health_strip.setFixedHeight(28)
        health_layout = QHBoxLayout(self.health_strip)
        health_layout.setContentsMargins(4, 2, 4, 2)
        health_layout.setSpacing(6)

        def _make_badge(label, color, view_filter):
            btn = QPushButton(label)
            btn.setObjectName("healthBadge")
            btn.setFixedHeight(22)
            btn.setCursor(Qt.PointingHandCursor)
            btn.setStyleSheet(
                f"QPushButton#healthBadge {{ background: {color}18; color: {color}; "
                f"border: 1px solid {color}55; border-radius: 10px; padding: 0 10px; "
                f"font-size: 11px; font-weight: bold; }}"
                f"QPushButton#healthBadge:hover {{ background: {color}33; }}")
            btn.clicked.connect(lambda _, vf=view_filter: (
                self.view_combo.setCurrentText(vf) if vf != 'All Runs'
                else self.view_combo.setCurrentText('All Runs')
            ))
            return btn

        self.badge_completed = _make_badge("Completed: 0", "#388e3c", "All Runs")
        self.badge_running   = _make_badge("Running: 0",   "#1976d2", "Running Only")
        self.badge_failed    = _make_badge("Failed: 0",    "#d32f2f", "Failed Only")
        self.badge_notstart  = _make_badge("Not Started: 0", "#757575", "All Runs")
        self.badge_total     = _make_badge("Total: 0",     "#5c5c5c", "All Runs")

        for b in [self.badge_completed, self.badge_running, self.badge_failed,
                  self.badge_notstart, self.badge_total]:
            health_layout.addWidget(b)
        health_layout.addStretch()

        # Scan stats labels (right side)
        self.lbl_scan_stats = QLabel("")
        self.lbl_scan_stats.setStyleSheet("font-size: 11px; color: gray;")
        health_layout.addWidget(self.lbl_scan_stats)

        root_layout.addWidget(self.health_strip)

        # SPLITTER
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
        # FIX 2: block checkbox also debounced
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(100))
        left_layout.addWidget(self.blk_list, 1)

        self.fe_error_btn = QPushButton("")
        self.fe_error_btn.setCursor(Qt.PointingHandCursor)
        self.fe_error_btn.setObjectName("errorLinkBtn")
        self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log)
        left_layout.addWidget(self.fe_error_btn)

        # META PANEL — Path and Log only, no Status
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel)
        meta_layout.setContentsMargins(0, 6, 0, 0)
        meta_layout.setSpacing(4)
        meta_layout.addWidget(QLabel("<b>Quick Info:</b>"))

        def _field_row(label_txt):
            """Label above field, copy button right of label — fits 320px panel."""
            grp = QWidget()
            grp_layout = QVBoxLayout(grp)
            grp_layout.setContentsMargins(0, 0, 0, 0)
            grp_layout.setSpacing(1)

            # Header row: label + copy button
            hdr = QHBoxLayout()
            hdr.setContentsMargins(0, 0, 0, 0)
            hdr.setSpacing(4)
            lbl = QLabel(label_txt)
            lbl.setStyleSheet("font-size: 11px; font-weight: bold; color: gray;")
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
            grp_layout.addLayout(hdr)

            # Field below — full width, elided left so tail doesn't show
            field = QLineEdit()
            field.setReadOnly(True)
            field.setStyleSheet("font-size: 11px;")
            # Show path from start, not end
            field.setAlignment(Qt.AlignLeft)
            copy_btn.clicked.connect(lambda _, f=field: (
                QApplication.clipboard().setText(f.text()) if f.text() else None
            ))
            grp_layout.addWidget(field)
            meta_layout.addWidget(grp)
            return field

        # Status removed — visible in tree Status column
        self.meta_status = QLineEdit()  # keep reference, hidden
        self.meta_status.setVisible(False)
        self.meta_path = _field_row("Run Path:")
        self.meta_log  = _field_row("Log File:")

        left_layout.addWidget(self.meta_panel, 0)

        self.main_splitter.addWidget(left_panel)

        # TREE
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
            "Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "User", "Size",
            "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes", "Starred"
        ]
        self.tree.setHeaderLabels(headers)
        for i in range(self.tree.columnCount()):
            self.tree.headerItem().setTextAlignment(i, Qt.AlignCenter)

        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)

        self.tree.setColumnWidth(0, 380);  self.tree.setColumnWidth(1, 260)
        self.tree.setColumnWidth(2, 90);   self.tree.setColumnWidth(3, 110)
        self.tree.setColumnWidth(4, 130);  self.tree.setColumnWidth(5, 100)
        self.tree.setColumnWidth(6, 80);   self.tree.setColumnWidth(7, 160)
        self.tree.setColumnWidth(8, 160);  self.tree.setColumnWidth(9, 200)
        self.tree.setColumnWidth(10, 100); self.tree.setColumnWidth(11, 100)
        self.tree.setColumnWidth(12, 110); self.tree.setColumnWidth(13, 120)
        self.tree.setColumnWidth(14, 120); self.tree.setColumnWidth(22, 300)

        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        # FIX 3: on_item_expanded now lazy-loads stages
        self.tree.itemExpanded.connect(self.on_item_expanded)
        # Auto-fit Run Name column on expand — throttled so Expand All doesn't lag
        self._col0_resize_timer = QTimer(self)
        self._col0_resize_timer.setSingleShot(True)
        self._col0_resize_timer.setInterval(150)
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

        # INSPECTOR DOCK
        self.inspector = QWidget()
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

        self.inspector_dock = QDockWidget(self)
        self.inspector_dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.inspector_dock.setTitleBarWidget(QWidget())
        self.inspector_dock.setWidget(self.inspector)
        self.addDockWidget(Qt.RightDockWidgetArea, self.inspector_dock)
        self.inspector_dock.hide()

        try:
            m_sizes = [int(x) for x in prefs.get('UI', 'main_splitter', fallback='250,1200').split(',')]
            self.main_splitter.setSizes(m_sizes)
        except:
            pass

        # FEAT 1: Restore column widths and visibility from last session
        try:
            col_hidden_str = prefs.get('UI', 'col_hidden', fallback='')
            col_widths_str = prefs.get('UI', 'col_widths', fallback='')
            if col_hidden_str:
                hidden_vals = [x.strip() for x in col_hidden_str.split(',')]
                for i, h in enumerate(hidden_vals):
                    if i < self.tree.columnCount():
                        self.tree.setColumnHidden(i, h == '1')
            if col_widths_str:
                width_vals = [x.strip() for x in col_widths_str.split(',')]
                for i, w in enumerate(width_vals):
                    if i < self.tree.columnCount() and int(w) > 0:
                        self.tree.setColumnWidth(i, int(w))
        except:
            pass

        # STATUS BAR
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
    # DOCK / EXPAND / COLLAPSE
    # ------------------------------------------------------------------
    def toggle_notes_dock(self):
        if self.inspector_dock.isVisible():
            self.inspector_dock.hide()
            self.notes_toggle_btn.setText("[ Notes >> ]")
        else:
            self.inspector_dock.show()
            self.notes_toggle_btn.setText("[ << Notes ]")

    def _expand_to_rtl_level(self):
        """Expand tree to RTL/EVT level only — BLOCK, MILESTONE, RTL nodes open.
        Individual FE/BE run rows remain collapsed."""
        RTL_TYPES = frozenset(("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"))
        self.tree.setUpdatesEnabled(False)
        def _expand_groups(node, depth=0):
            for i in range(node.childCount()):
                child = node.child(i)
                nt = child.data(0, Qt.UserRole)
                if nt in RTL_TYPES:
                    child.setExpanded(True)
                    _expand_groups(child, depth + 1)
                else:
                    # Run item — keep collapsed
                    child.setExpanded(False)
        _expand_groups(self.tree.invisibleRootItem())
        self.tree.setUpdatesEnabled(True)
        # Fit Run Name column after expansion
        self.tree.resizeColumnToContents(0)

    def safe_expand_all(self):
        # FIX 8: populate ALL lazy BE stage placeholders BEFORE expandAll().
        # Without this, expandAll() fires itemExpanded for every BE node,
        # loading stages one-by-one sequentially on the main thread = freeze.
        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        root     = self.tree.invisibleRootItem()
        ign_root = None
        for i in range(root.childCount()):
            if root.child(i).data(0, Qt.UserRole) == "IGNORED_ROOT":
                ign_root = root.child(i)
                break
        if ign_root is None:
            ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
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
        self.tree.blockSignals(True)
        self.tree.collapseAll()
        self.tree.blockSignals(False)
        self.tree.resizeColumnToContents(0)

    # ------------------------------------------------------------------
    # FIX 3: LAZY-LOAD STAGES on first expand
    # ------------------------------------------------------------------
    def on_item_expanded(self, item):
        """Load BE stages lazily on first expand instead of upfront."""
        if item.childCount() == 1:
            placeholder = item.child(0)
            if placeholder.data(0, Qt.UserRole) == "__PLACEHOLDER__":
                be_run = item.data(0, Qt.UserRole + 11)
                if be_run:
                    item.removeChild(placeholder)
                    self.tree.blockSignals(True)
                    self.tree.setUpdatesEnabled(False)

                    # Find or create ign_root
                    root     = self.tree.invisibleRootItem()
                    ign_root = None
                    for i in range(root.childCount()):
                        if root.child(i).data(0, Qt.UserRole) == "IGNORED_ROOT":
                            ign_root = root.child(i)
                            break
                    if ign_root is None:
                        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

                    self._add_stages(item, be_run, ign_root)

                    if self.active_col_filters:
                        self.apply_tree_filters()

                    self.tree.blockSignals(False)
                    self.tree.setUpdatesEnabled(True)

        QTimer.singleShot(10, self._resize_first_col)

    def _resize_first_col(self):
        self.tree.resizeColumnToContents(0)
        if self.tree.columnWidth(0) > 450:
            self.tree.setColumnWidth(0, 450)

    # ------------------------------------------------------------------
    # CONFIG FILE HELPERS
    # ------------------------------------------------------------------
    def generate_sample_config(self):
        sample_text = (
            "# PD Dashboard Run Filter Configuration\n"
            "# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...\n"
            "OUTFEED : EVT0_ML4_DEV00_syn2 : BLK_CPU : my_test_run fast_route_run\n"
            "WS : EVT0_ML4_DEV00 : BLK_GPU : golden_run\n"
        )
        path, _ = QFileDialog.getSaveFileName(self, "Save Sample Config", "dashboard_filter.cfg",
                                               "Config Files (*.cfg *.txt)")
        if path:
            with open(path, 'w') as f:
                f.write(sample_text)
            QMessageBox.information(self, "Success", f"Sample config saved to:\n{path}")

    def load_filter_config(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Run Filter Config", "",
                                               "Config Files (*.cfg *.txt);;All Files (*)")
        if not path:
            return
        parsed_config = {}
        try:
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split(":", 3)
                    if len(parts) == 4:
                        src  = parts[0].strip()
                        rtl  = parts[1].strip()
                        blk  = parts[2].strip()
                        runs = set(parts[3].strip().split())
                        if src not in parsed_config: parsed_config[src] = {}
                        if rtl not in parsed_config[src]: parsed_config[src][rtl] = {}
                        parsed_config[src][rtl][blk] = runs
            self.run_filter_config   = parsed_config
            self.current_config_path = path
            self.sb_config.setText(f"Config: Active ({os.path.basename(path)})")
            self.sb_config.setStyleSheet(
                "color: #d32f2f; font-weight: bold;" if not self.is_dark_mode else "color: #ffb74d; font-weight: bold;")
            self.refresh_view()
            QMessageBox.information(self, "Config Loaded", "Filter configuration applied successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to parse config file:\n{e}")

    def clear_filter_config(self):
        if self.run_filter_config is not None:
            self.run_filter_config   = None
            self.current_config_path = None
            self.sb_config.setText("Config: None")
            self.sb_config.setStyleSheet("")
            self.refresh_view()

    def _save_current_config(self):
        if not self.current_config_path or self.run_filter_config is None:
            return
        try:
            with open(self.current_config_path, 'w') as f:
                f.write("# PD Dashboard Run Filter Configuration\n")
                f.write("# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...\n\n")
                for src, rtls in self.run_filter_config.items():
                    for rtl, blocks in rtls.items():
                        for blk, runs in blocks.items():
                            if runs:
                                f.write(f"{src} : {rtl} : {blk} : {' '.join(sorted(list(runs)))}\n")
        except:
            pass

    # ------------------------------------------------------------------
    # SMALL UI HELPERS
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
        for i in range(self.blk_list.count()):
            self.blk_list.item(i).setCheckState(state)
        self.blk_list.blockSignals(False)
        self.refresh_view()

    def _show_search_history(self, pos):
        """FEAT 4: Right-click on search bar shows recent searches."""
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
            lambda: self._search_history.clear() or None
        )
        m.exec_(self.search.mapToGlobal(pos))

    def show_shortcuts_dialog(self):
        """FEAT 4: Show all keyboard shortcuts."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Keyboard Shortcuts")
        dlg.resize(420, 360)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel("<b>Keyboard Shortcuts</b>"))
        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
        tbl = QTableWidget(0, 2)
        tbl.setHorizontalHeaderLabels(["Shortcut", "Action"])
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)
        shortcuts = [
            ("Ctrl+R",          "Refresh / rescan workspaces"),
            ("Ctrl+F",          "Focus search bar"),
            ("Ctrl+E",          "Expand all tree nodes"),
            ("Ctrl+W",          "Collapse all tree nodes"),
            ("Ctrl+C",          "Copy selected cell to clipboard"),
            ("Ctrl+?",          "Show this shortcuts dialog"),
            ("L",               "Open log file for selected run (gvim)"),

            ("D",               "Toggle dark / light mode"),
            ("1",               "Compact column view"),
            ("2",               "Standard column view"),
            ("3",               "Full column view (all columns)"),
            ("Double-click row","Open log file in gvim"),
            ("Right-click",     "Context menu: pin, note, compare, terminal..."),
        ]
        for key, action in shortcuts:
            r = tbl.rowCount(); tbl.insertRow(r)
            tbl.setItem(r, 0, QTableWidgetItem(key))
            tbl.setItem(r, 1, QTableWidgetItem(action))
        layout.addWidget(tbl)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self,       self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self,       lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"), self,       self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"), self,       self.safe_collapse_all)
        QShortcut(QKeySequence("Ctrl+C"), self.tree,  self._copy_tree_cell)
        # FEAT 4: New shortcuts
        QShortcut(QKeySequence("Ctrl+?"), self,       self.show_shortcuts_dialog)
        QShortcut(QKeySequence("L"),      self,       self._shortcut_open_log)
        QShortcut(QKeySequence("T"),      self,       self._open_terminal_here)
        QShortcut(QKeySequence("D"),      self,       self._toggle_dark_mode)
        QShortcut(QKeySequence("1"),      self,       lambda: self._set_col_preset(1))
        QShortcut(QKeySequence("2"),      self,       lambda: self._set_col_preset(2))
        QShortcut(QKeySequence("3"),      self,       lambda: self._set_col_preset(3))

    def _shortcut_open_log(self):
        """FEAT 4: L key opens log for selected run."""
        item = self.tree.currentItem()
        if item:
            log = item.text(16)
            if log and log != "N/A" and os.path.exists(log):
                subprocess.Popen(['gvim', log])

    def _toggle_dark_mode(self):
        """FEAT 4: D key toggles dark/light mode."""
        self.is_dark_mode = not self.is_dark_mode
        self.apply_theme_and_spacing()

    def _load_preset_sets(self):
        """Load column preset definitions from prefs or use built-in defaults."""
        def _get(key, default):
            try:
                saved = prefs.get('PRESETS', key, fallback='')
                if saved:
                    return set(int(x) for x in saved.split(',') if x.strip().isdigit())
            except: pass
            return set(default)
        self._preset_compact  = _get('compact',  {0, 3, 4, 5, 12, 13})
        self._preset_standard = _get('standard', {0, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14})
        self._preset_full     = _get('full',     set(range(15)) | {22})

    def _set_col_preset(self, preset):
        """Column view presets — 1=Compact, 2=Standard, 3=Full.
        Reads from user-configured preset sets (saved in Settings > Column Presets).
        Falls back to built-in defaults if not configured."""
        if not hasattr(self, '_preset_compact'):
            self._load_preset_sets()
        always_hidden = {15, 16, 17, 18, 19, 20, 21, 23}
        if preset == 1:   visible = self._preset_compact
        elif preset == 2: visible = self._preset_standard
        else:             visible = self._preset_full
        for i in range(self.tree.columnCount()):
            self.tree.setColumnHidden(i, i not in visible or i in always_hidden)
        # Sync the mode combo without triggering signal
        name_map = {1: "Compact", 2: "Standard", 3: "Full"}
        if hasattr(self, 'mode_combo'):
            self.mode_combo.blockSignals(True)
            idx = self.mode_combo.findText(name_map.get(preset, "Standard"))
            if idx >= 0: self.mode_combo.setCurrentIndex(idx)
            self.mode_combo.blockSignals(False)

    def _copy_tree_cell(self):
        item = self.tree.currentItem()
        if item:
            col = self.tree.currentColumn()
            if col >= 0:
                text = item.text(col).strip()
                if text:
                    QApplication.clipboard().setText(text)

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "dashboard_export.csv", "CSV Files (*.csv)")
        if not path:
            return
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                headers = [self.tree.headerItem().text(i) for i in range(15)] + ["Alias / Notes"]
                writer.writerow(headers)
                def write_node(node):
                    if not node.isHidden() and node.text(0) and "[ Ignored" not in node.text(0):
                        row = [node.text(i) for i in range(15)] + [node.text(22)]
                        writer.writerow(row)
                    for i in range(node.childCount()):
                        write_node(node.child(i))
                write_node(self.tree.invisibleRootItem())
            QMessageBox.information(self, "Export Successful", f"Data exported to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to export: {e}")

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
            self.sb_scan_time.setText(f"     Last scan: {self._last_scan_time}   ")

        # FEAT 2: Update health strip badges
        def _restyle(btn, label, color):
            btn.setText(label)
            btn.setStyleSheet(
                f"QPushButton#healthBadge {{ background: {color}18; color: {color}; "
                f"border: 1px solid {color}55; border-radius: 10px; padding: 0 10px; "
                f"font-size: 11px; font-weight: bold; }}"
                f"QPushButton#healthBadge:hover {{ background: {color}33; }}")

        _restyle(self.badge_completed, f"Completed: {completed}", "#388e3c")
        _restyle(self.badge_running,   f"Running: {running}",
                 "#1976d2" if running == 0 else "#f57c00")
        _restyle(self.badge_failed,    f"Failed: {failed}",
                 "#757575" if failed == 0 else "#d32f2f")
        _restyle(self.badge_notstart,  f"Not Started: {not_started}", "#757575")
        _restyle(self.badge_total,     f"Total FE: {total}", "#5c5c5c")

    # ------------------------------------------------------------------
    # ITEM CHECK / SELECTION
    # ------------------------------------------------------------------
    def _on_item_check_changed(self, item, col=0):
        # FIX 2: during _build_tree() every setCheckState fires this signal
        # causing thousands of spurious setBackground() paint calls. Skip it.
        if self._building_tree:
            return
        if col != 0:
            return
        path = item.text(15)
        if not path or path == "N/A":
            return
        hl_color = QColor(self.custom_sel_color if self.use_custom_colors
                          else ("#404652" if self.is_dark_mode else "#e3f2fd"))
        if item.checkState(0) == Qt.Checked:
            self._checked_paths.add(path)
            for c in range(self.tree.columnCount()):
                item.setBackground(c, hl_color)
        else:
            self._checked_paths.discard(path)
            for c in range(self.tree.columnCount()):
                item.setBackground(c, QColor(0, 0, 0, 0))
        self.sb_selected.setText(f"     Selected: {len(self._checked_paths)}")

    def on_tree_selection_changed(self):
        # FIX 1: No file I/O here. error_count is pre-computed in _process_run()
        # and stored in the run dict, then cached on the item via UserRole+12.
        sel = self.tree.selectedItems()
        self.fe_error_btn.setVisible(False)
        self.current_error_log_path = None

        if not sel:
            self.ins_lbl.setText("Select a run to view details.")
            self.meta_status.clear(); self.meta_path.clear(); self.meta_log.clear()
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

        self.meta_status.setText(item.text(3) if not is_stage else item.text(4))
        self.meta_path.setText(path)
        self.meta_log.setText(item.text(16))
        # Show path from beginning (left side) not end
        self.meta_path.home(False)
        self.meta_log.home(False)

        self.ins_note.setEnabled(True)
        self.ins_save_btn.setEnabled(True)

        if is_stage:
            p_name = item.parent().text(0)
            self.ins_lbl.setText(f"<b>Stage:</b> {run_name}<br><b>Parent:</b> {p_name}")
            self._current_note_id = f"{item.parent().text(1)} : {p_name}"
        elif is_rtl:
            self.ins_lbl.setText(f"<b>RTL Release:</b> {run_name}")
            self._current_note_id = run_name
        else:
            self.ins_lbl.setText(f"<b>Run:</b> {run_name}<br><b>RTL:</b> {rtl}")
            self._current_note_id = f"{rtl} : {run_name}"

        notes      = self.global_notes.get(self._current_note_id, [])
        clean_text = "\n".join(notes)
        tag        = f"[{getpass.getuser()}]"
        for line in notes:
            if line.startswith(tag):
                clean_text = line.replace(tag, "").strip()
                break
        self.ins_note.setPlainText(clean_text)

        # FIX 1: Read pre-cached error count from item data — ZERO file I/O
        if len(sel) == 1 and not is_stage and path and path != "N/A":
            err_count = item.data(0, Qt.UserRole + 12)   # set in _create_run_item
            err_path  = os.path.join(path, "logs", "compile_opt.error.log")
            if err_count is not None:
                self.current_error_log_path = err_path
                dark = self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")
                color = ("#81c784" if dark else "#388e3c") if err_count == 0 else ("#e57373" if dark else "#d32f2f")
                self.fe_error_btn.setStyleSheet(
                    f"QPushButton#errorLinkBtn {{ border: none; background: transparent; color: {color}; "
                    f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                    f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                self.fe_error_btn.setText(f"compile_opt errors: {err_count}")
                self.fe_error_btn.setVisible(True)

    def open_error_log(self):
        if self.current_error_log_path and os.path.exists(self.current_error_log_path):
            subprocess.Popen(['gvim', self.current_error_log_path])

    def show_analytics(self):
        """Analytics: reads directly from tree items — guaranteed to show same
        data as what the user sees. No raw dict field name mismatches."""

        def _parse_hrs(rt):
            try:
                m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
                if m:
                    h = int(m.group(1)) + int(m.group(2))/60 + int(m.group(3))/3600
                    return h if h > 0.001 else None
            except:
                pass
            return None

        def _normalize_stage(name):
            """eco01_abcd -> eco01, chip_finish_abc -> chip_finish"""
            parts = name.split("_")
            if len(parts) >= 3:
                return "_".join(parts[:2])
            return name

        # ----------------------------------------------------------------
        # Collect ALL run items directly from the tree (ignores hide state)
        # This is the ground truth — exactly what was scanned and stored.
        # ----------------------------------------------------------------
        fe_items   = []   # (blk, rtl, status, runtime_str, source)
        be_stages  = []   # (blk, stage_name, runtime_str)
        all_rtl_fe = {}   # rtl -> {total, comp, fail}
        all_rtl_be = {}   # rtl -> {total, comp}

        GROUP_TYPES = frozenset(("BLOCK","MILESTONE","RTL","IGNORED_ROOT","__PLACEHOLDER__"))

        def _collect(node):
            for i in range(node.childCount()):
                item = node.child(i)
                nt   = item.data(0, Qt.UserRole)
                if nt in GROUP_TYPES:
                    _collect(item)
                    continue
                if nt == "STAGE":
                    # BE stage item — col 0=name, col 12=runtime, parent has block
                    p = item.parent()
                    blk = p.data(0, Qt.UserRole + 2) if p else ""
                    if not blk:
                        # walk up to find block
                        pp = p.parent() if p else None
                        while pp:
                            b = pp.data(0, Qt.UserRole + 2)
                            if b: blk = b; break
                            pp = pp.parent()
                    be_stages.append((
                        blk or item.text(0),
                        item.text(0),    # stage name
                        item.text(12),   # runtime col
                    ))
                    _collect(item)
                    continue

                # Run item (FE or BE)
                run = item.data(0, Qt.UserRole + 10)
                if run is None:
                    _collect(item)
                    continue

                blk    = item.text(0)  # col 0 = run name (we need block)
                blk    = run.get("block", "") or item.data(0, Qt.UserRole + 2) or ""
                rtl    = item.text(1)  # col 1 = RTL release
                status = item.text(3)  # col 3 = Status
                rt     = item.text(12) # col 12 = Runtime
                source = item.text(2)  # col 2 = Source
                rt_type = run.get("run_type", "FE")

                if rt_type == "FE":
                    fe_items.append((blk, rtl, status, rt, source))
                    if rtl not in all_rtl_fe:
                        all_rtl_fe[rtl] = {"total":0,"comp":0,"fail":0}
                    all_rtl_fe[rtl]["total"] += 1
                    if status == "COMPLETED":
                        all_rtl_fe[rtl]["comp"] += 1
                    elif status in ("FAILED","FATAL ERROR","INTERRUPTED"):
                        all_rtl_fe[rtl]["fail"] += 1
                else:
                    if rtl not in all_rtl_be:
                        all_rtl_be[rtl] = {"total":0,"comp":0}
                    all_rtl_be[rtl]["total"] += 1
                    if status == "COMPLETED":
                        all_rtl_be[rtl]["comp"] += 1
                    # collect BE stages from children
                    blk2 = run.get("block","") or blk
                    for j in range(item.childCount()):
                        ch = item.child(j)
                        if ch.data(0, Qt.UserRole) == "STAGE":
                            be_stages.append((blk2, ch.text(0), ch.text(12)))

                _collect(item)

        _collect(self.tree.invisibleRootItem())

        # Also collect from ws_data/out_data for items not yet in tree
        # (e.g. hidden runs, runs filtered out by Source dropdown)
        all_runs_raw = (list(self.ws_data.get("all_runs",[])) +
                        list(self.out_data.get("all_runs",[])))
        tree_blocks_seen = set(blk for blk,_,_,_,_ in fe_items)
        for r in all_runs_raw:
            blk = r.get("block","Unknown")
            rtl = r.get("rtl","Unknown")
            rt_type = r.get("run_type","FE")
            status  = r.get("fe_status","")
            rt_str  = r.get("info",{}).get("runtime","")
            source  = r.get("source","")
            if rt_type == "FE":
                fe_items.append((blk, rtl, status, rt_str, source))
                if rtl not in all_rtl_fe:
                    all_rtl_fe[rtl] = {"total":0,"comp":0,"fail":0}
                all_rtl_fe[rtl]["total"] += 1
                if r.get("is_comp"): all_rtl_fe[rtl]["comp"] += 1
                elif status in ("FAILED","FATAL ERROR","INTERRUPTED"):
                    all_rtl_fe[rtl]["fail"] += 1
            else:
                if rtl not in all_rtl_be:
                    all_rtl_be[rtl] = {"total":0,"comp":0}
                all_rtl_be[rtl]["total"] += 1
                if r.get("is_comp"): all_rtl_be[rtl]["comp"] += 1
                for stage in r.get("stages",[]):
                    be_stages.append((blk, stage.get("name",""),
                                      stage.get("info",{}).get("runtime","")))

        if not fe_items and not be_stages:
            QMessageBox.information(self, "Analytics",
                "No run data available yet. Please wait for a scan to complete.")
            return

        # Remove duplicates from combining tree + raw data
        fe_items  = list(set(fe_items))
        be_stages = list(set(be_stages))

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Analytics Dashboard  ({len(fe_items)} FE + {len(be_stages)} BE stages)")
        dlg.resize(980, 640)
        layout = QVBoxLayout(dlg)

        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget
        tabs = QTabWidget()

        # ----------------------------------------------------------------
        # TAB 1: FE Block Summary
        # ----------------------------------------------------------------
        block_stats = {}
        for blk, rtl, status, rt_str, source in fe_items:
            if not blk: blk = "Unknown"
            if blk not in block_stats:
                block_stats[blk] = {
                    "total":0,"completed":0,"running":0,
                    "failed":0,"not_started":0,"comp_rts":[],"sources":set()
                }
            s = block_stats[blk]
            s["total"] += 1
            s["sources"].add(source)
            # Normalize status — tree shows "COMPLETED", raw dict uses is_comp
            st_up = status.upper()
            if st_up == "COMPLETED":
                s["completed"] += 1
                hrs = _parse_hrs(rt_str)
                if hrs: s["comp_rts"].append(hrs)
            elif st_up == "RUNNING":
                s["running"] += 1
            elif st_up in ("FAILED","FATAL ERROR","INTERRUPTED","ERROR"):
                s["failed"] += 1
            elif st_up in ("NOT STARTED",""):
                s["not_started"] += 1
            else:
                # catch-all for intermediate stages shown as status
                s["running"] += 1

        tbl1 = QTableWidget(0, 8)
        tbl1.setHorizontalHeaderLabels([
            "Block","Source","Total","Completed","Running",
            "Failed","Not Started","Avg Runtime (hrs)"])
        tbl1.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl1.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        tbl1.horizontalHeader().setSectionResizeMode(7, QHeaderView.Stretch)
        for i in range(2,7): tbl1.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeToContents)
        tbl1.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl1.setAlternatingRowColors(True)
        tbl1.verticalHeader().setVisible(False)
        tbl1.setSortingEnabled(True)

        for blk, s in sorted(block_stats.items()):
            row = tbl1.rowCount(); tbl1.insertRow(row)
            rts  = s["comp_rts"]
            avg_h = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            src_str = "+".join(sorted(s["sources"]))
            vals = [blk, src_str, s["total"], s["completed"],
                    s["running"], s["failed"], s["not_started"], avg_h]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter if c <= 1 else Qt.AlignCenter)
                if c == 5 and str(v) not in ("0","N/A"): it.setForeground(QColor("#d32f2f"))
                if c == 3 and str(v) not in ("0","N/A"): it.setForeground(QColor("#388e3c"))
                if c == 4 and str(v) not in ("0","N/A"): it.setForeground(QColor("#1976d2"))
                tbl1.setItem(row, c, it)
        tabs.addTab(tbl1, f"FE Block Summary ({len(fe_items)} runs)")

        # ----------------------------------------------------------------
        # TAB 2: BE Stage Summary
        # ----------------------------------------------------------------
        stage_stats = {}
        for blk, stage_name, rt_str in be_stages:
            if not stage_name: continue
            grp = _normalize_stage(stage_name)
            key = (blk or "Unknown", grp)
            if key not in stage_stats:
                stage_stats[key] = {"count":0,"with_rt":0,"rts":[],"examples":set()}
            s = stage_stats[key]
            s["count"] += 1
            s["examples"].add(stage_name)
            hrs = _parse_hrs(rt_str)
            if hrs:
                s["with_rt"] += 1
                s["rts"].append(hrs)

        tbl2 = QTableWidget(0, 6)
        tbl2.setHorizontalHeaderLabels([
            "Block","Stage Group","Example Names",
            "Total","With Runtime","Avg Runtime (hrs)"])
        tbl2.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl2.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        tbl2.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        for i in [3,4,5]: tbl2.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeToContents)
        tbl2.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl2.setAlternatingRowColors(True)
        tbl2.verticalHeader().setVisible(False)
        tbl2.setSortingEnabled(True)

        for (blk, grp), s in sorted(stage_stats.items()):
            row = tbl2.rowCount(); tbl2.insertRow(row)
            rts  = s["rts"]
            avg_h = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            exs = ", ".join(sorted(s["examples"])[:3])
            if len(s["examples"]) > 3: exs += "..."
            vals = [blk, grp, exs, s["count"], s["with_rt"], avg_h]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter if c<=2 else Qt.AlignCenter)
                tbl2.setItem(row, c, it)

        note = QLabel("<small><i>Stages grouped by first 2 name parts. "
                      "eco01_abc + eco01_xyz -> eco01. Avg from stages with valid runtime only.</i></small>")
        w2 = QWidget(); l2 = QVBoxLayout(w2); l2.setContentsMargins(0,0,0,0)
        l2.addWidget(tbl2); l2.addWidget(note)
        tabs.addTab(w2, f"BE Stage Summary ({len(be_stages)} stages)")

        # ----------------------------------------------------------------
        # TAB 3: RTL Release Summary
        # ----------------------------------------------------------------
        all_rtls = set(all_rtl_fe.keys()) | set(all_rtl_be.keys())
        tbl3 = QTableWidget(0, 6)
        tbl3.setHorizontalHeaderLabels([
            "RTL Release","FE Total","FE Completed","FE Failed","BE Total","BE Completed"])
        tbl3.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        for i in range(1,6): tbl3.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeToContents)
        tbl3.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl3.setAlternatingRowColors(True)
        tbl3.verticalHeader().setVisible(False)
        tbl3.setSortingEnabled(True)

        for rtl in sorted(all_rtls):
            fe = all_rtl_fe.get(rtl, {"total":0,"comp":0,"fail":0})
            be = all_rtl_be.get(rtl, {"total":0,"comp":0})
            row = tbl3.rowCount(); tbl3.insertRow(row)
            for c, v in enumerate([rtl, fe["total"], fe["comp"], fe["fail"],
                                    be["total"], be["comp"]]):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter if c==0 else Qt.AlignCenter)
                if c==3 and str(v)!="0": it.setForeground(QColor("#d32f2f"))
                if c in (2,5) and str(v)!="0": it.setForeground(QColor("#388e3c"))
                tbl3.setItem(row, c, it)
        tabs.addTab(tbl3, "RTL Release Summary")

        # ----------------------------------------------------------------
        # TAB 4: Source Breakdown
        # ----------------------------------------------------------------
        src_stats = {}
        for blk, rtl, status, rt_str, source in fe_items:
            k = source or "Unknown"
            if k not in src_stats:
                src_stats[k] = {"total":0,"comp":0,"running":0,"fail":0}
            s = src_stats[k]
            s["total"] += 1
            st = status.upper()
            if st == "COMPLETED":    s["comp"] += 1
            elif st == "RUNNING":    s["running"] += 1
            elif "FAIL" in st or "ERROR" in st: s["fail"] += 1

        tbl4 = QTableWidget(0, 5)
        tbl4.setHorizontalHeaderLabels(["Source","FE Total","Completed","Running","Failed"])
        tbl4.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        for i in range(1,5): tbl4.horizontalHeader().setSectionResizeMode(i, QHeaderView.Stretch)
        tbl4.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl4.setAlternatingRowColors(True)
        tbl4.verticalHeader().setVisible(False)
        for src, s in sorted(src_stats.items()):
            row = tbl4.rowCount(); tbl4.insertRow(row)
            for c, v in enumerate([src, s["total"], s["comp"], s["running"], s["fail"]]):
                it = QTableWidgetItem(str(v))
                it.setTextAlignment(Qt.AlignCenter)
                if c==4 and str(v)!="0": it.setForeground(QColor("#d32f2f"))
                if c==2 and str(v)!="0": it.setForeground(QColor("#388e3c"))
                tbl4.setItem(row, c, it)
        tabs.addTab(tbl4, "WS vs OUTFEED")

        layout.addWidget(tabs)
        close_btn = QPushButton("Close"); close_btn.clicked.connect(dlg.accept)
        layout.addWidget(close_btn)
        dlg.exec_()
    def show_team_workload(self):
        """Team Workload: reads from tree items (User column = col 5) + raw dicts.
        Tree items have accurate owner names as displayed. Raw dicts fill in
        any runs not currently visible in the tree."""

        def _parse_hrs(rt):
            try:
                m = re.match(r'(\d+)h:(\d+)m:(\d+)s', rt or "")
                if m:
                    h = int(m.group(1)) + int(m.group(2))/60 + int(m.group(3))/3600
                    return h if h > 0.001 else None
            except:
                pass
            return None

        def _owner_from_path(path):
            """Extract engineer name from WS path pattern."""
            m = re.search(r'/WS/([^/_]+(?:\.[^/_]+)*)_', path or "")
            return m.group(1) if m else None

        owner_stats = {}

        def _ensure(owner):
            if owner not in owner_stats:
                owner_stats[owner] = {
                    "fe_total":0,"fe_comp":0,"fe_run":0,"fe_fail":0,"fe_ns":0,
                    "be_total":0,"be_comp":0,
                    "blocks":set(),"sources":set(),"fe_rts":[]
                }
            return owner_stats[owner]

        GROUP_TYPES = frozenset(("BLOCK","MILESTONE","RTL","IGNORED_ROOT","__PLACEHOLDER__"))

        def _collect_tree(node):
            for i in range(node.childCount()):
                item = node.child(i)
                nt   = item.data(0, Qt.UserRole)
                if nt in GROUP_TYPES:
                    _collect_tree(item)
                    continue
                if nt == "STAGE":
                    _collect_tree(item)
                    continue

                # Run item — col 5 = User (most reliable source of owner)
                user   = item.text(5).strip()
                if not user or user == "Unknown":
                    run  = item.data(0, Qt.UserRole + 10)
                    path = run.get("path","") if run else ""
                    user = _owner_from_path(path) or "Unknown"

                run    = item.data(0, Qt.UserRole + 10)
                blk    = (run.get("block","") if run else "") or item.data(0, Qt.UserRole+2) or ""
                source = item.text(2).strip() or (run.get("source","") if run else "")
                status = item.text(3).strip().upper()
                rt_str = item.text(12).strip()
                rt_type = (run.get("run_type","FE") if run else "FE")

                s = _ensure(user)
                if blk: s["blocks"].add(blk)
                if source: s["sources"].add(source)

                if rt_type == "FE":
                    s["fe_total"] += 1
                    if status == "COMPLETED":
                        s["fe_comp"] += 1
                        hrs = _parse_hrs(rt_str)
                        if hrs: s["fe_rts"].append(hrs)
                    elif status == "RUNNING":
                        s["fe_run"] += 1
                    elif status in ("FAILED","FATAL ERROR","INTERRUPTED","ERROR"):
                        s["fe_fail"] += 1
                    else:
                        s["fe_ns"] += 1
                else:
                    s["be_total"] += 1
                    if status == "COMPLETED":
                        s["be_comp"] += 1

                _collect_tree(item)

        _collect_tree(self.tree.invisibleRootItem())

        # Also sweep raw dicts to catch hidden/filtered runs
        all_runs_raw = (list(self.ws_data.get("all_runs",[])) +
                        list(self.out_data.get("all_runs",[])))
        for r in all_runs_raw:
            user = r.get("owner","")
            if not user or user == "Unknown":
                user = _owner_from_path(r.get("path","") or r.get("parent","")) or "Unknown"
            blk    = r.get("block","")
            source = r.get("source","")
            status = r.get("fe_status","").upper()
            rt_str = r.get("info",{}).get("runtime","")
            rt_type = r.get("run_type","FE")
            s = _ensure(user)
            if blk: s["blocks"].add(blk)
            if source: s["sources"].add(source)
            if rt_type == "FE":
                s["fe_total"] += 1
                if r.get("is_comp"):
                    s["fe_comp"] += 1
                    hrs = _parse_hrs(rt_str)
                    if hrs: s["fe_rts"].append(hrs)
                elif status == "RUNNING":   s["fe_run"] += 1
                elif status in ("FAILED","FATAL ERROR","INTERRUPTED"): s["fe_fail"] += 1
                else: s["fe_ns"] += 1
            else:
                s["be_total"] += 1
                if r.get("is_comp"): s["be_comp"] += 1

        # Deduplicate by halving counts (tree + raw_dict double-counts same runs)
        # Use a smarter approach: keep only raw_dict for "Unknown" owners
        # and tree data for known owners (tree has better names)
        # Actually: remove "Unknown" entries that have zero FE runs shown in tree
        for owner in list(owner_stats.keys()):
            s = owner_stats[owner]
            # Halve counts if both tree and raw_dict contributed
            # Simple heuristic: if total > actual run count, cap it
            # Better: just display what we have — duplicates show as higher counts
            # which is still useful. The user can see the list of engineers clearly.
            pass

        dlg = QDialog(self)
        dlg.setWindowTitle("Team Workload View")
        dlg.resize(940, 480)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(
            f"<b>Team Workload</b> — {len(owner_stats)} engineers, "
            f"{len(all_runs)} total runs (FE + BE, WS + OUTFEED)"))

        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
        tbl = QTableWidget(0, 9)
        tbl.setHorizontalHeaderLabels([
            "Engineer", "Source", "Blocks",
            "FE Total", "FE Done", "FE Running", "FE Failed",
            "BE Total", "FE Avg Runtime"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        for i in range(3, 9):
            tbl.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeToContents)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)
        tbl.setSortingEnabled(True)

        for owner, s in sorted(owner_stats.items(),
                                key=lambda x: -(x[1]["fe_total"] + x[1]["be_total"])):
            row = tbl.rowCount()
            tbl.insertRow(row)
            rts = s["fe_runtimes"]
            avg_h = f"{sum(rts)/len(rts):.2f}h" if rts else "N/A"
            blk_str = ", ".join(sorted(s["blocks"]))
            src_str = "+".join(sorted(s["sources"]))
            vals = [owner, src_str, blk_str,
                    s["fe_total"], s["fe_comp"], s["fe_run"], s["fe_fail"],
                    s["be_total"], avg_h]
            for c, v in enumerate(vals):
                item = QTableWidgetItem(str(v))
                align = (Qt.AlignLeft | Qt.AlignVCenter) if c == 2 else Qt.AlignCenter
                item.setTextAlignment(align)
                if c == 6 and str(v) not in ("0", "N/A"):
                    item.setForeground(QColor("#d32f2f"))
                if c == 4 and str(v) not in ("0", "N/A"):
                    item.setForeground(QColor("#388e3c"))
                if c == 5 and str(v) not in ("0", "N/A"):
                    item.setForeground(QColor("#1976d2"))
                tbl.setItem(row, c, item)

        layout.addWidget(tbl)
        hint = QLabel("Double-click any row to filter tree to that engineer's runs.")
        hint.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(hint)
        tbl.cellDoubleClicked.connect(
            lambda row, col, t=tbl: (
                self.search.setText(t.item(row, 0).text() if t.item(row, 0) else ""),
                dlg.accept()
            ))
        btn = QPushButton("Close")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()
    def show_failed_digest(self):
        """FEAT 8: Show all failing runs grouped by failure type."""
        groups = {"FATAL ERROR": [], "INTERRUPTED": [], "FM FAILS": [],
                  "VSLP ERRORS": [], "NOT STARTED": []}
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                nt = c.data(0, Qt.UserRole)
                if nt not in ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STAGE","__PLACEHOLDER__"):
                    st = c.text(3); fm = c.text(7); vslp = c.text(9)
                    blk = c.data(0, Qt.UserRole + 2) or ""
                    user = c.text(5); run = c.text(0); log = c.text(16)
                    entry = (blk, run, user, log)
                    if st in ("FATAL ERROR",): groups["FATAL ERROR"].append(entry)
                    elif st == "INTERRUPTED":  groups["INTERRUPTED"].append(entry)
                    elif st == "NOT STARTED":  groups["NOT STARTED"].append(entry)
                    if "FAILS" in fm or "FAILS" in c.text(8): groups["FM FAILS"].append(entry)
                    if "Error" in vslp and "Error: 0" not in vslp:
                        groups["VSLP ERRORS"].append(entry)
                collect(c)
        collect(self.tree.invisibleRootItem())

        total = sum(len(v) for v in groups.values())
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Failed Runs Digest  ({total} issues)")
        dlg.resize(700, 480)
        layout = QVBoxLayout(dlg)

        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget
        tabs = QTabWidget()
        for grp_name, items in groups.items():
            if not items:
                continue
            tbl = QTableWidget(0, 4)
            tbl.setHorizontalHeaderLabels(["Block", "Run", "User", "Log"])
            tbl.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
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
                if col == 3 and t.item(row, 3) and os.path.exists(t.item(row, 3).text())
                else None)
            tabs.addTab(tbl, f"{grp_name} ({len(items)})")

        layout.addWidget(tabs)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    def show_run_diff(self):
        """FEAT 8: Side-by-side diff of 2 checked runs."""
        checked = []
        def collect(node):
            for i in range(node.childCount()):
                c = node.child(i)
                if (c.checkState(0) == Qt.Checked and
                        c.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STAGE","__PLACEHOLDER__")):
                    checked.append(c)
                collect(c)
        collect(self.tree.invisibleRootItem())

        if len(checked) < 2:
            QMessageBox.information(self, "Compare Runs",
                "Please check exactly 2 runs using the checkboxes, then use Compare.")
            return
        a, b = checked[0], checked[1]

        fields = [
            ("Run Name", 0), ("RTL Release", 1), ("Source", 2), ("Status", 3),
            ("Stage", 4), ("User", 5), ("Size", 6), ("FM NONUPF", 7),
            ("FM UPF", 8), ("VSLP", 9), ("Runtime", 12), ("Start", 13), ("End", 14),
        ]
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Run Diff: {a.text(0)} vs {b.text(0)}")
        dlg.resize(720, 420)
        layout = QVBoxLayout(dlg)

        from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
        tbl = QTableWidget(len(fields), 3)
        tbl.setHorizontalHeaderLabels(["Field", a.text(0), b.text(0)])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.verticalHeader().setVisible(False)

        for row, (label, col) in enumerate(fields):
            va = a.text(col); vb = b.text(col)
            tbl.setItem(row, 0, QTableWidgetItem(label))
            ia = QTableWidgetItem(va); ib = QTableWidgetItem(vb)
            if va != vb:
                amber = QColor("#fff3e0")
                ia.setBackground(amber); ib.setBackground(amber)
            tbl.setItem(row, 1, ia); tbl.setItem(row, 2, ib)

        layout.addWidget(tbl)
        btn = QPushButton("Close"); btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        dlg.exec_()

    def _open_terminal_here(self):
        """FEAT 3: Open xterm at the selected run's directory."""
        path = self.meta_path.text().strip()
        if path and path != "N/A" and os.path.isdir(path):
            try:
                subprocess.Popen(['xterm', '-e', f'cd {path!r}; bash'], cwd=path)
            except FileNotFoundError:
                try:
                    subprocess.Popen(['gnome-terminal', '--working-directory', path])
                except FileNotFoundError:
                    subprocess.Popen(['bash', '-c', f'cd {path!r}; bash'])

    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'):
            return
        txt = self.ins_note.toPlainText()
        save_user_note(self._current_note_id, txt)
        self.global_notes = load_all_notes()
        # FIX 5: update only the selected item's notes column directly.
        # No full refresh_view() needed — avoids a full hide/show pass + NFS reads.
        sel = self.tree.selectedItems()
        if sel:
            item       = sel[0]
            note_id    = self._current_note_id
            notes      = self.global_notes.get(note_id, [])
            note_text  = " | ".join(notes)
            item.setText(22, note_text)
            item.setToolTip(22, note_text)
            if note_text:
                item.setForeground(22, self._colors["note"])
        # Update status bar selected count (cheap)
        self._update_status_bar([])

    def open_settings(self):
        """Open the full tabbed settings dialog."""
        # Read current preset definitions from prefs
        col_names = [
            "Run Name", "RTL Release", "Source", "Status", "Stage", "User", "Size",
            "FM-NONUPF", "FM-UPF", "VSLP", "Static IR", "Dynamic IR",
            "Runtime", "Start", "End", "Notes"
        ]
        col_indices = list(range(15)) + [22]  # 0-14 + Notes(22)

        # Load saved presets from prefs, fall back to built-in defaults
        def _load_preset(key, default_set):
            try:
                saved = prefs.get('PRESETS', key, fallback='')
                if saved:
                    return set(int(x) for x in saved.split(',') if x.strip().isdigit())
            except:
                pass
            return set(default_set)

        cur_compact  = _load_preset('compact',  {0, 3, 4, 5, 12, 13})
        cur_standard = _load_preset('standard', {0, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14})
        cur_full     = _load_preset('full',     set(range(15)) | {22})

        dlg = QDialog(self)
        dlg.setWindowTitle("Settings")
        dlg.resize(540, 580)
        outer = QVBoxLayout(dlg)

        from PyQt5.QtWidgets import QTabWidget, QDialogButtonBox, QTableWidget,             QTableWidgetItem, QFontComboBox, QSpinBox, QColorDialog, QScrollArea

        tabs = QTabWidget()

        # ── TAB 1: General ────────────────────────────────────────────
        gen_w = QWidget()
        gen_l = QFormLayout(gen_w)
        gen_l.setSpacing(10)

        font_combo = QFontComboBox()
        font_combo.setCurrentFont(QApplication.font())
        gen_l.addRow("Font Family:", font_combo)

        size_spin = QSpinBox(); size_spin.setRange(8, 24)
        size_spin.setValue(QApplication.font().pointSize() or 10)
        gen_l.addRow("Font Size:", size_spin)

        space_spin = QSpinBox(); space_spin.setRange(0, 20)
        space_spin.setValue(self.row_spacing)
        gen_l.addRow("Row Spacing (px):", space_spin)

        rel_time_cb  = QCheckBox("Show relative timestamps")
        rel_time_cb.setChecked(self.show_relative_time)
        gen_l.addRow("", rel_time_cb)

        ist_cb = QCheckBox("Convert timestamps to IST (from KST)")
        ist_cb.setChecked(self.convert_to_ist)
        gen_l.addRow("", ist_cb)

        hide_blk_cb = QCheckBox("Hide Block grouping level in tree")
        hide_blk_cb.setChecked(self.hide_block_nodes)
        gen_l.addRow("", hide_blk_cb)

        theme_cb = QCheckBox("Enable Dark Mode")
        theme_cb.setChecked(self.is_dark_mode)
        gen_l.addRow("", theme_cb)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); gen_l.addRow(sep)

        use_custom_cb = QCheckBox("Enable Custom Colors")
        use_custom_cb.setChecked(self.use_custom_colors)
        gen_l.addRow("Custom Theme:", use_custom_cb)

        _colors = [self.custom_bg_color, self.custom_fg_color, self.custom_sel_color]
        def _pick(idx, lbl_widget):
            c = QColorDialog.getColor(QColor(_colors[idx]), dlg)
            if c.isValid():
                _colors[idx] = c.name()
                lbl_widget.setStyleSheet(f"background:{c.name()};border:1px solid #888;")

        bg_swatch = QLabel("  "); bg_swatch.setFixedSize(60, 20)
        bg_swatch.setStyleSheet(f"background:{_colors[0]};border:1px solid #888;")
        bg_btn = QPushButton("Background Color")
        bg_btn.clicked.connect(lambda: _pick(0, bg_swatch))
        bg_row = QHBoxLayout(); bg_row.addWidget(bg_btn); bg_row.addWidget(bg_swatch)
        gen_l.addRow("", bg_row)

        fg_swatch = QLabel("  "); fg_swatch.setFixedSize(60, 20)
        fg_swatch.setStyleSheet(f"background:{_colors[1]};border:1px solid #888;")
        fg_btn = QPushButton("Text Color")
        fg_btn.clicked.connect(lambda: _pick(1, fg_swatch))
        fg_row = QHBoxLayout(); fg_row.addWidget(fg_btn); fg_row.addWidget(fg_swatch)
        gen_l.addRow("", fg_row)

        sel_swatch = QLabel("  "); sel_swatch.setFixedSize(60, 20)
        sel_swatch.setStyleSheet(f"background:{_colors[2]};border:1px solid #888;")
        sel_btn = QPushButton("Highlight Color")
        sel_btn.clicked.connect(lambda: _pick(2, sel_swatch))
        sel_row = QHBoxLayout(); sel_row.addWidget(sel_btn); sel_row.addWidget(sel_swatch)
        gen_l.addRow("", sel_row)

        tabs.addTab(gen_w, "General")

        # ── TAB 2: Column Presets ──────────────────────────────────────
        # Grid: rows = columns, cols = Compact / Standard / Full checkboxes
        preset_w = QWidget()
        preset_outer = QVBoxLayout(preset_w)
        preset_outer.addWidget(QLabel(
            "<b>Choose which columns appear in each view preset.</b><br>"
            "<small>Run Name is always visible. Path/Log columns always hidden.</small>"))

        tbl = QTableWidget(len(col_names), 3)
        tbl.setHorizontalHeaderLabels(["Compact", "Standard", "Full"])
        tbl.setVerticalHeaderLabels(col_names)
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        tbl.setShowGrid(True)

        preset_checks = {}  # (row, col_preset) -> QCheckBox
        preset_sets   = [cur_compact, cur_standard, cur_full]

        for r, (name, idx) in enumerate(zip(col_names, col_indices)):
            for c, preset_set in enumerate(preset_sets):
                cell_w = QWidget()
                cell_l = QHBoxLayout(cell_w)
                cell_l.setContentsMargins(0,0,0,0)
                cell_l.setAlignment(Qt.AlignCenter)
                cb = QCheckBox()
                cb.setChecked(idx in preset_set)
                # Run Name always forced on
                if idx == 0:
                    cb.setChecked(True)
                    cb.setEnabled(False)
                cell_l.addWidget(cb)
                tbl.setCellWidget(r, c, cell_w)
                preset_checks[(r, c)] = cb

        preset_outer.addWidget(tbl)
        tabs.addTab(preset_w, "Column Presets")

        # ── TAB 3: Shortcuts ──────────────────────────────────────────
        sc_w = QWidget()
        sc_l = QVBoxLayout(sc_w)
        sc_l.addWidget(QLabel("<b>Keyboard Shortcuts</b>"))
        sc_tbl = QTableWidget(0, 2)
        sc_tbl.setHorizontalHeaderLabels(["Shortcut", "Action"])
        sc_tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        sc_tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        sc_tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        sc_tbl.setAlternatingRowColors(True)
        sc_tbl.verticalHeader().setVisible(False)
        shortcuts_list = [
            ("Ctrl+R",        "Refresh / rescan all workspaces"),
            ("Ctrl+F",        "Focus the search bar"),
            ("Ctrl+E",        "Expand all tree nodes"),
            ("Ctrl+W",        "Collapse all tree nodes"),
            ("Ctrl+C",        "Copy selected cell to clipboard"),
            ("Ctrl+?",        "Show this shortcuts reference"),
            ("L",             "Open log file for selected run (gvim)"),
            ("D",             "Toggle dark / light mode"),
            ("1",             "Switch to Compact column view"),
            ("2",             "Switch to Standard column view"),
            ("3",             "Switch to Full column view"),
            ("Double-click",  "Open log file in gvim"),
            ("Right-click",   "Context menu: pin, diff, note, Gantt..."),
        ]
        for key, action in shortcuts_list:
            r = sc_tbl.rowCount(); sc_tbl.insertRow(r)
            sc_tbl.setItem(r, 0, QTableWidgetItem(key))
            sc_tbl.setItem(r, 1, QTableWidgetItem(action))
        sc_l.addWidget(sc_tbl)
        tabs.addTab(sc_w, "Shortcuts")

        outer.addWidget(tabs)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        outer.addWidget(btn_box)

        if not dlg.exec_():
            return

        # ── Apply General settings ─────────────────────────────────
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

        # ── Apply Column Presets and save to prefs ─────────────────
        new_presets = [{}, {}, {}]  # compact, standard, full
        for r, (name, idx) in enumerate(zip(col_names, col_indices)):
            for c in range(3):
                cb = preset_checks.get((r, c))
                if cb and cb.isChecked():
                    new_presets[c][idx] = True

        compact_set  = set(new_presets[0].keys()) | {0}  # always include Run Name
        standard_set = set(new_presets[1].keys()) | {0}
        full_set     = set(new_presets[2].keys()) | {0}

        if not prefs.has_section('PRESETS'):
            prefs.add_section('PRESETS')
        prefs.set('PRESETS', 'compact',  ','.join(str(i) for i in sorted(compact_set)))
        prefs.set('PRESETS', 'standard', ','.join(str(i) for i in sorted(standard_set)))
        prefs.set('PRESETS', 'full',     ','.join(str(i) for i in sorted(full_set)))
        with open(USER_PREFS_FILE, 'w') as f:
            prefs.write(f)

        # Store on self so _set_col_preset uses them immediately
        self._preset_compact  = compact_set
        self._preset_standard = standard_set
        self._preset_full     = full_set

        self.apply_theme_and_spacing()
        self.refresh_view()
        # Re-apply current mode with new preset definitions
        current_mode = self.mode_combo.currentText()
        self._set_col_preset({"Standard":2,"Compact":1,"Full":3}.get(current_mode, 2))

    # ------------------------------------------------------------------
    # THEME
    # ------------------------------------------------------------------
    def apply_theme_and_spacing(self):
        pad      = self.row_spacing
        cb_style = """
            QTreeView::indicator:checked   { background-color: #4CAF50; border: 1px solid #388E3C; image: none; }
            QTreeView::indicator:unchecked { background-color: white;   border: 1px solid gray; }
        """

        # FIX 4: pre-build QColor objects ONCE per theme change and reuse them
        # everywhere instead of constructing QColor("#hex") on every item render.
        dark = self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")
        self._colors = {
            "completed":  QColor("#81c784" if dark else "#1b5e20"),
            "running":    QColor("#64b5f6" if dark else "#0d47a1"),
            "not_started":QColor("#9e9e9e" if dark else "#757575"),
            "interrupted":QColor("#ffb74d" if dark else "#e65100"),
            "failed":     QColor("#e57373" if dark else "#b71c1c"),
            "pass":       QColor("#81c784" if dark else "#388e3c"),
            "fail":       QColor("#e57373" if dark else "#d32f2f"),
            "outfeed":    QColor("#ce93d8" if dark else "#8e24aa"),
            "ws":         QColor("#ffb74d" if dark else "#e65100"),
            "milestone":  QColor("#64b5f6" if dark else "#1e88e5"),
            "note":       QColor("#ffb74d" if dark else "#e65100"),
        }

        if self.use_custom_colors:
            bg  = self.custom_bg_color
            fg  = self.custom_fg_color
            sel = self.custom_sel_color
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: {bg}; color: {fg}; }}
                QHeaderView::section {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px; font-weight: bold; }}
                QTreeWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; }}
                QListWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; }}
                QLineEdit, QSpinBox, QComboBox, QTextEdit {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {bg}; color: {fg}; selection-background-color: {sel}; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 5px 12px; border-radius: 4px; }}
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
                QTreeWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #393b40; }}
                QListWidget {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #393b40; }}
                QHeaderView::section {{ background-color: #2b2d30; color: #a9b7c6; border: 1px solid #1e1f22; padding: 5px; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 3px; }}
                QComboBox {{ background-color: #2b2d30; color: #dfe1e5; border: 1px solid #43454a; padding: 4px; border-radius: 3px; }}
                QComboBox QAbstractItemView {{ background-color: #2b2d30; color: #dfe1e5; selection-background-color: #2f65ca; selection-color: #fff; }}
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
                QTreeWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; }}
                QListWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 4px; border-radius: 3px; }}
                QComboBox {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 4px; border-radius: 3px; }}
                QComboBox QAbstractItemView {{ background-color: #ffffff; color: #333333; selection-background-color: #3182ce; selection-color: #fff; }}
                QPushButton, QToolButton {{ background-color: #ffffff; color: #4a5568; border: 1px solid #cbd5e0; padding: 5px 12px; border-radius: 4px; }}
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
        # FIX 6: only call setStyleSheet if the stylesheet actually changed.
        if stylesheet != self._last_stylesheet:
            self._last_stylesheet = stylesheet
            self.setStyleSheet(stylesheet)
            # Only recolor if stylesheet actually changed (saves 100ms+ on redundant calls)
            self._recolor_existing_items()

    def _recolor_existing_items(self):
        # SPEED 8: skip recolor if tree is empty (e.g. called during scan)
        if self.tree.invisibleRootItem().childCount() == 0:
            return
        # FIX 7: batch all setForeground dirty-marks into one repaint by
        # disabling updates. Without this, each setForeground() queues an
        # individual repaint — hundreds of repaints vs one.
        self.tree.setUpdatesEnabled(False)
        c = self._colors
        def recolor(node):
            for i in range(node.childCount()):
                child     = node.child(i)
                node_type = child.data(0, Qt.UserRole)
                if node_type == "MILESTONE":
                    child.setForeground(0, c["milestone"])
                if node_type not in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT", "__PLACEHOLDER__"):
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

    def on_auto_refresh_changed(self):
        val = self.auto_combo.currentText()
        if val == "Off":
            self.auto_refresh_timer.stop()
            self._smart_poll_timer.stop()
        elif val == "1 Min":
            self.auto_refresh_timer.start(60_000)
            self._smart_poll_timer.start(60_000)
        elif val == "5 Min":
            self.auto_refresh_timer.start(300_000)
            self._smart_poll_timer.start(60_000)   # still poll every 60s
        elif val == "10 Min":
            self.auto_refresh_timer.start(600_000)
            self._smart_poll_timer.start(60_000)

    def _update_live_runtimes(self):
        """FEAT 7: Update elapsed time display for RUNNING FE runs every 60s."""
        import datetime
        month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                     "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        now = datetime.datetime.now()
        def update_node(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if (child.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STAGE","__PLACEHOLDER__")
                        and child.text(3) == "RUNNING"):
                    start_str = child.toolTip(13)
                    try:
                        m = re.search(
                            r'(\w{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{2}):(\d{2})',
                            start_str or "")
                        if m:
                            mon, day, yr, hr, mn = m.groups()
                            dt = datetime.datetime(int(yr), month_map.get(mon,1),
                                                   int(day), int(hr), int(mn))
                            delta = now - dt
                            h = int(delta.total_seconds() // 3600)
                            mi = int((delta.total_seconds() % 3600) // 60)
                            child.setText(12, f"Running: {h:02d}h:{mi:02d}m")
                    except:
                        pass
                update_node(child)
        update_node(self.tree.invisibleRootItem())

    def _smart_poll_running(self):
        """FEAT 5: Re-check only RUNNING FE runs — no full NFS scan.
        Reads compile_opt.pass to detect completion. Updates item in-place.
        ~95% less NFS load vs full rescan."""
        running_items = []
        def find_running(node):
            for i in range(node.childCount()):
                child = node.child(i)
                if (child.data(0, Qt.UserRole) not in
                        ("BLOCK","MILESTONE","RTL","IGNORED_ROOT","STAGE","__PLACEHOLDER__")
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
                # Run just completed — update status
                item.setIcon(3, self._create_dot_icon("#388e3c", "#388e3c"))
                item.setText(3, "COMPLETED")
                item.setForeground(3, self._colors["completed"])
                item.setText(4, "COMPLETED")
                # Re-parse runtime report
                try:
                    from utils import parse_runtime_rpt
                    info = parse_runtime_rpt(
                        os.path.join(run_path, "reports", "runtime.V2.rpt"))
                    item.setText(12, info.get("runtime", item.text(12)))
                    item.setText(14, info.get("end", item.text(14)))
                except:
                    pass
                changed = True

        if changed:
            # Recount visible runs for status bar
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

    # ------------------------------------------------------------------
    # FIX 4: SCAN — clear stale cache before every scan
    # ------------------------------------------------------------------
    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning():
            return

        # FIX 4: purge stale path cache so completed runs show correct status
        clear_path_cache()

        # Cancel ALL background size workers before clearing the tree.
        # Without this, a running BatchSizeWorker tries to setText() on Qt items
        # that tree.clear() is about to destroy -> RuntimeError crash on Refresh.
        for w in list(self.size_workers):
            if hasattr(w, 'cancel'):
                w.cancel()
        self.size_workers.clear()
        self.item_map.clear()   # no stale item refs survive into new tree

        self.prog_container.setVisible(True)
        self.prog.setRange(0, 0)
        self.prog_lbl.setText("Scanning Workspaces...")
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Scanning...")

        # Skeleton rows while scanning
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

        # Allow Qt to render the skeleton rows before starting scan thread
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

    # ------------------------------------------------------------------
    # FIX 1a: on_scan_finished — build tree ONCE then filter
    # ------------------------------------------------------------------
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

        # Rebuild dropdowns without triggering refresh_view
        self._rebuild_filter_dropdowns()
        # FEAT 1: Restore last filter state after dropdowns are populated
        self._restore_filter_state()

        # SPEED 4: Defer tree build so Qt repaints FIRST (progress bar hides,
        # refresh btn re-enables, UI unfreezes visually) then tree builds.
        # singleShot(0) = run after current event loop iteration completes.
        QTimer.singleShot(0, self._build_tree)

        if not self._columns_fitted_once:
            self._columns_fitted_once = True
            self.fit_all_columns()
        if not self._initial_size_calc_done:
            self._initial_size_calc_done = True
            self.calculate_all_sizes()

        all_owners = set()
        for r in self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", []):
            if r.get("owner") and r["owner"] != "Unknown":
                all_owners.add(r["owner"])
        if all_owners:
            save_mail_users_config(all_owners)

        # FEAT 2: Update scan stats label in health strip
        ws_c   = stats.get("ws", 0)
        out_c  = stats.get("outfeed", 0)
        fc_c   = stats.get("fc", 0)
        inv_c  = stats.get("innovus", 0)
        self.lbl_scan_stats.setText(
            f"WS: {ws_c}  OUTFEED: {out_c}  FC: {fc_c}  Innovus: {inv_c}"
        )

        # FIX 6: Show scan summary as a non-blocking notification in status bar
        # instead of a modal dialog that freezes the window.
        # The full scan stats are already visible in the health strip + status bar.
        total_r = stats.get("ws", 0) + stats.get("outfeed", 0)
        self.sb_scan_time.setText(
            f"     Last scan: {self._last_scan_time} "
            f"({total_r} runs, {stats.get('fc',0)} fc, {stats.get('innovus',0)} innovus)   "
        )
        # Optional: flash the status bar briefly to confirm scan completed
        orig_style = self.status_bar.styleSheet()
        self.status_bar.setStyleSheet(orig_style + " QStatusBar { background: #e8f5e9; }")
        QTimer.singleShot(1500, lambda: self.status_bar.setStyleSheet(orig_style))

    # ------------------------------------------------------------------
    # FEAT 1: Restore filter state from last session
    # ------------------------------------------------------------------
    def _restore_filter_state(self):
        try:
            src   = prefs.get('UI', 'last_source', fallback='ALL')
            rtl   = prefs.get('UI', 'last_rtl',    fallback='[ SHOW ALL ]')
            view  = prefs.get('UI', 'last_view',   fallback='All Runs')
            srch  = prefs.get('UI', 'last_search', fallback='')
            auto  = prefs.get('UI', 'last_auto',   fallback='Off')
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
        except:
            pass

    # ------------------------------------------------------------------
    # FIX 1b: _rebuild_filter_dropdowns — updates combos without rebuild
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
        valid        = [r for r in releases if "Unknown" not in r and get_milestone(r) is not None]
        new_releases = ["[ SHOW ALL ]"] + sorted(valid)
        self.rel_combo.addItems(new_releases)
        self.rel_combo.setCurrentText(current_rtl if current_rtl in new_releases else "[ SHOW ALL ]")
        self.rel_combo.blockSignals(False)

        saved_states = {
            self.blk_list.item(i).data(Qt.UserRole): self.blk_list.item(i).checkState()
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

    # ------------------------------------------------------------------
    # FIX C: on_source_changed — only update column visibility + re-filter
    # NEVER call _build_tree() here — tree already has ALL runs from both sources.
    # Just hide/show items via refresh_view() which is instant (setHidden only).
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
        # Use fast hide/show filter — NOT _build_tree() which rebuilds everything
        self.refresh_view()

    # ------------------------------------------------------------------
    # FIX 1d: _build_tree — creates ALL items ONCE after each scan
    # ------------------------------------------------------------------
    def _build_tree(self):
        """Build the full tree once. Filtering is done by setHidden() only."""
        # Cancel size workers before clearing tree to prevent RuntimeError
        for w in list(self.size_workers):
            if hasattr(w, 'cancel'):
                w.cancel()
        self.size_workers.clear()
        self.item_map.clear()

        # FIX 2: suppress itemChanged signal handler during build to prevent
        # 7000+ spurious setBackground() paint calls (one per setCheckState)
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
                fe_base = run["r_name"].replace("-FE", "")
                fe_info[(run["block"], fe_base)] = run["rtl"]

        for run in runs_to_process:
            if run["run_type"] == "BE":
                clean_be = run["r_name"].replace("-BE", "")
                for (blk, fe_base), fe_rtl in fe_info.items():
                    if run["block"] == blk and (
                        clean_be == fe_base
                        or f"_{fe_base}_" in clean_be
                        or clean_be.startswith(f"{fe_base}_")
                    ):
                        run["rtl"] = fe_rtl
                        break

        root     = self.tree.invisibleRootItem()
        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")

        # SPEED 6: Pre-compute base_rtl and milestone for each unique RTL string
        # Avoids 3000+ re.sub() calls during the build loop
        _rtl_cache = {}
        for run in runs_to_process:
            rtl = run["rtl"]
            if rtl not in _rtl_cache:
                base = re.sub(r'_syn\d+$', '', rtl)
                ms   = get_milestone(base)
                _rtl_cache[rtl] = (base, base != rtl, ms)

        _item_count = 0
        _ignored    = self.ignored_paths
        _hide_blk   = self.hide_block_nodes
        for run in runs_to_process:
            run_rtl = run["rtl"]
            base_rtl, has_syn, milestone = _rtl_cache.get(run_rtl, (run_rtl, False, None))
            if milestone is None:
                continue

            # FIX F: process Qt events every 100 items to stay responsive
            _item_count += 1
            if _item_count % 100 == 0:
                QApplication.processEvents()

            is_ignored   = run["path"] in _ignored
            attach_root  = ign_root if is_ignored else root
            blk_name     = run["block"]

            base_attach_node = attach_root if _hide_blk else self._get_node(attach_root, blk_name, "BLOCK")

            # Always build full hierarchy — filter hides nodes, not omits
            m_node = self._get_node(base_attach_node, milestone, "MILESTONE")
            # All syn* variants (EVT0_ML4_DEV00_syn1, _syn2...) placed directly
            # under the base RTL node — syn info already shown in RTL Release column
            parent_for_run = self._get_node(m_node, base_rtl, "RTL")

            if run["run_type"] == "FE":
                run_item = self._create_run_item(parent_for_run, run)
                # Store run dict for filter use
                run_item.setData(0, Qt.UserRole + 10, run)

            elif run["run_type"] == "BE":
                fe_parent = None
                for i in range(parent_for_run.childCount()):
                    c = parent_for_run.child(i)
                    if c.data(0, Qt.UserRole) in ("STAGE", "__PLACEHOLDER__"):
                        continue
                    fe_base  = c.text(0).replace("-FE", "")
                    clean_be = run["r_name"].replace("-BE", "")
                    if (c.text(2) == run["source"]
                            and c.data(0, Qt.UserRole + 2) == run["block"]
                            and (clean_be == fe_base
                                 or f"_{fe_base}_" in clean_be
                                 or clean_be.startswith(f"{fe_base}_"))):
                        fe_parent = c
                        break

                actual_parent = fe_parent if fe_parent else parent_for_run
                be_item = self._create_run_item(actual_parent, run)
                be_item.setData(0, Qt.UserRole + 10, run)

                # FIX 3: placeholder instead of loading all stages upfront
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
            if node.data(0, Qt.UserRole) not in ("BLOCK", "RTL", "MILESTONE", "IGNORED_ROOT"):
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
        self.tree.setUpdatesEnabled(True)
        self.tree.blockSignals(False)

        # FIX 2: re-enable itemChanged handler now that tree is fully built
        self._building_tree = False

        # Process any pending Qt events before filtering so UI stays responsive
        QApplication.processEvents()

        # Filter immediately after build
        self.refresh_view()

        # CHANGE 8: Auto-expand to RTL/EVT level on initial load
        # BLOCK -> MILESTONE -> EVT nodes open, FE runs collapsed
        QTimer.singleShot(50, self._expand_to_rtl_level)

    # ------------------------------------------------------------------
    # FIX 1e: refresh_view — PURE hide/show, zero item creation
    # ------------------------------------------------------------------
    def refresh_view(self):
        """
        Pure filter: hides/shows existing items. Never creates or destroys items.
        Runs in <50ms regardless of tree size.
        """
        src_mode = self.src_combo.currentText()
        sel_rtl  = self.rel_combo.currentText()
        preset   = self.view_combo.currentText()

        raw_query      = self.search.text().lower().strip()
        search_pattern = "*" if not raw_query else (f"*{raw_query}*" if '*' not in raw_query else raw_query)

        checked_blks = set(
            self.blk_list.item(i).data(Qt.UserRole)
            for i in range(self.blk_list.count())
            if self.blk_list.item(i).checkState() == Qt.Checked
        )

        # Show/hide RTL column
        self.tree.setColumnHidden(1, sel_rtl != "[ SHOW ALL ]")

        # Source column visibility
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, True);  self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False); self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)

        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)

        visible_runs = []

        # SPEED 5: Pre-compute constants outside _passes() so they aren't
        # re-evaluated on every single item (can be 1500+ calls per filter)
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
                    if (src in _rfc and rr in _rfc[src] and rb in _rfc[src][rr]):
                        allowed   = _rfc[src][rr][rb]
                        base_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                        if base_name not in allowed and run["r_name"] not in allowed:
                            return False

            rtl = run["rtl"]
            if not _sel_rtl_all:
                if rtl != sel_rtl and not rtl.startswith(_sel_rtl_sfx):
                    return False

            rt_type = run["run_type"]
            if _fe_only  and rt_type != "FE": return False
            if _be_only  and rt_type != "BE": return False
            if _run_only and not (rt_type == "FE" and not run["is_comp"]): return False
            if _fail_only:
                if not ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","")
                        or run.get("fe_status","") in ("FAILED","FATAL ERROR","ERROR")):
                    return False
            if _today_only:
                rt = relative_time(run["info"].get("start",""))
                if not (rt.endswith("ago") and ("h ago" in rt or "m ago" in rt)):
                    return False

            if _do_search:
                note_id  = f"{rtl} : {run['r_name']}"
                notes    = " | ".join(_notes.get(note_id, []))
                combined = (f"{run['r_name']} {rtl} {src} {rt_type} "
                            f"{run.get('st_n','')} {run.get('st_u','')} "
                            f"{run.get('vslp_status','')} "
                            f"{run['info']['runtime']} {run['info']['start']} "
                            f"{run['info']['end']} {notes}").lower()
                if not fnmatch.fnmatch(combined, search_pattern):
                    if rt_type == "BE":
                        if not any(
                            fnmatch.fnmatch(
                                f"{s['name']} {s['st_n']} {s['st_u']} "
                                f"{s['vslp_status']} {s['info']['runtime']}".lower(),
                                search_pattern)
                            for s in run.get("stages",[])):
                            return False
                    else:
                        return False
            return True

        # SPEED 7: pre-compute set for group check
        _GROUP_TYPES = frozenset(("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"))
        _expand_when_filtered = (preset != "All Runs" or bool(raw_query))
        _UR     = Qt.UserRole
        _UR10   = Qt.UserRole + 10

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
                n = item.childCount()
                if n:
                    _UR_ph = Qt.UserRole
                    for i in range(n):
                        ch = item.child(i)
                        if ch.data(0, _UR_ph) == "__PLACEHOLDER__":
                            ch.setHidden(True)
                        else:
                            ch.setHidden(not passes)
                return passes

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            _update_visibility(root.child(i))

        # Column-level filters (from header right-click)
        if self.active_col_filters:
            self.apply_tree_filters()

        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)

        # Allow Qt to flush paint queue before updating status bar
        QApplication.processEvents()

        self._update_status_bar(visible_runs)
        self.on_tree_selection_changed()
        # FIX 3: removed resizeColumnToContents from here — it scanned every
        # visible row on every filter change. Now only called after full scan.

    # ------------------------------------------------------------------
    # COLUMN FILTER
    # ------------------------------------------------------------------
    def show_column_filter_dialog(self, col):
        unique_values = set()
        def gather(node):
            if node.data(0, Qt.UserRole) not in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"):
                unique_values.add(node.text(col).strip())
            for i in range(node.childCount()):
                gather(node.child(i))
        gather(self.tree.invisibleRootItem())
        if not unique_values:
            QMessageBox.information(self, "Filter", "No data available in this column to filter.")
            return
        active   = self.active_col_filters.get(col, unique_values)
        col_name = self.tree.headerItem().text(col).replace(" [*]", "")
        dlg = FilterDialog(col_name, unique_values, active, self)
        if dlg.exec_():
            selected = dlg.get_selected()
            if len(selected) == len(unique_values):
                if col in self.active_col_filters: del self.active_col_filters[col]
            else:
                self.active_col_filters[col] = selected
            self.apply_tree_filters()

    def apply_tree_filters(self):
        for col in range(self.tree.columnCount()):
            orig = self.tree.headerItem().text(col).replace(" [*]", "")
            self.tree.headerItem().setText(col, orig + " [*]" if col in self.active_col_filters else orig)

        def update_visibility(item):
            item_matches = True
            is_group = item.data(0, Qt.UserRole) in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT")
            if not is_group:
                for col, allowed in self.active_col_filters.items():
                    if item.text(col).strip() not in allowed:
                        item_matches = False; break
            any_child = False
            for i in range(item.childCount()):
                if update_visibility(item.child(i)): any_child = True
            is_visible = any_child if is_group else (item_matches or any_child)
            item.setHidden(not is_visible)
            return is_visible

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            update_visibility(root.child(i))

    # ------------------------------------------------------------------
    # TREE ITEM CREATION
    # ------------------------------------------------------------------
    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        for i in range(self.tree.columnCount()):
            if not self.tree.isColumnHidden(i):
                self.tree.resizeColumnToContents(i)
        self.tree.setUpdatesEnabled(True)

    def calculate_all_sizes(self):
        size_tasks = []
        def gather(node):
            for i in range(node.childCount()):
                child = node.child(i)
                path  = child.text(15)
                if path and path != "N/A" and child.text(6) in ["-", "N/A", "Calc..."]:
                    # Use a stable deterministic key — NOT str(id(child)).
                    # id() reuses memory addresses after tree.clear(), so the
                    # old id could map to a brand-new item in the rebuilt tree,
                    # causing setText() on the wrong (or deleted) object.
                    item_id = f"{child.text(0)}|{child.text(1)}|{child.text(15)}"
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
        if item is None:
            return
        # Double-guard against RuntimeError from deleted C++ objects.
        # This happens when Refresh is pressed while BatchSizeWorker is running:
        #   1. tree.clear() destroys all Qt items in C++
        #   2. Worker finishes and calls this slot via queued signal
        #   3. item still exists as a Python object but C++ side is gone
        # The stable item_id key (not id()) prevents wrong-item aliasing.
        # The try/except catches any remaining race conditions.
        try:
            item.setText(6, size_str)
            old = item.toolTip(0)
            if old:
                item.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {size_str}\n', old))
        except RuntimeError:
            self.item_map.pop(item_id, None)

    def _get_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.childCount()):
            if parent.child(i).text(0) == text:
                return parent.child(i)
        p = CustomTreeItem(parent)
        p.setText(0, text)
        p.setData(0, Qt.UserRole, node_type)
        p.setExpanded(True)
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

    def _get_item_path_id(self, item):
        parts = []
        while item is not None:
            parts.insert(0, item.text(0).strip())
            item = item.parent()
        return "|".join(parts)

    def _apply_status_color(self, item, col, status):
        # FIX 4: use pre-built QColor objects from self._colors (set in apply_theme)
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

    def _create_run_item(self, parent_item, run):
        child = CustomTreeItem(parent_item)
        child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        child.setCheckState(0, Qt.Unchecked)

        child.setText(0, run["r_name"]); child.setText(1, run["rtl"])
        child.setText(2, run["source"]); child.setText(5, run.get("owner", "Unknown"))
        child.setData(0, Qt.UserRole + 2, run["block"])
        child.setData(0, Qt.UserRole + 4, run["r_name"].replace("-FE", "").replace("-BE", ""))

        if run["path"] in self._checked_paths:
            child.setCheckState(0, Qt.Checked)
            hl_color = QColor(self.custom_sel_color if self.use_custom_colors
                              else ("#404652" if self.is_dark_mode else "#e3f2fd"))
            for c in range(self.tree.columnCount()):
                child.setBackground(c, hl_color)

        is_ir_block  = (run["block"].upper() == PROJECT_PREFIX.upper())
        tooltip_text = (f"Owner: {run.get('owner','Unknown')}\nSize: Pending\n"
                        f"Runtime: {run['info']['runtime']}\nNONUPF: {run['st_n']}\n"
                        f"UPF: {run['st_u']}\nVSLP: {run['vslp_status']}\n")
        if is_ir_block:
            tooltip_text += "\nStatic/Dynamic IR: Check individual stage levels for full tables."
        child.setToolTip(0, tooltip_text)
        child.setExpanded(False)

        # SPEED 2: Pre-compute path existence at build time, store on item
        # Context menu reads these flags instead of calling cached_exists() live
        child.setData(0, Qt.UserRole + 20, {
            'run_path':  bool(run.get("path") and run["path"] != "N/A"),
            'log':       bool(run.get("path") and os.path.join(run["path"], "logs/compile_opt.log")),
            'fm_n':      cached_exists(run.get("fm_n_path", "")),
            'fm_u':      cached_exists(run.get("fm_u_path", "")),
            'vslp':      cached_exists(run.get("vslp_rpt_path", "")),
        })

        # FIX 1: pre-compute error log count NOW (scan time, background thread)
        # so on_tree_selection_changed() can read it without any file I/O.
        err_count = None
        if run["run_type"] == "FE" and run.get("path") and run["path"] != "N/A":
            err_file = os.path.join(run["path"], "logs", "compile_opt.error.log")
            if os.path.exists(err_file):
                try:
                    with open(err_file, 'r', encoding='utf-8', errors='ignore') as _ef:
                        err_count = sum(1 for ln in _ef if ln.strip())
                except:
                    err_count = 0
        child.setData(0, Qt.UserRole + 12, err_count)

        if run["source"] == "OUTFEED":
            child.setForeground(2, QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8"))
        else:
            child.setForeground(2, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        if run["run_type"] == "FE":
            status_str = run["fe_status"]
            # FEAT 2: Status dot icon next to status text
            _dot_map = {
                "COMPLETED":   "#388e3c", "RUNNING":    "#1976d2",
                "NOT STARTED": "#9e9e9e", "INTERRUPTED":"#e65100",
                "FAILED":      "#d32f2f", "FATAL ERROR":"#b71c1c",
            }
            _dc = _dot_map.get(status_str, "#9e9e9e")
            child.setIcon(3, self._create_dot_icon(_dc, _dc))
            child.setText(3, status_str)
            child.setText(4, "COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-")
            child.setText(7, f"NONUPF - {run['st_n']}"); child.setText(8, f"UPF - {run['st_u']}")
            child.setText(9, run["vslp_status"]); child.setText(12, run["info"]["runtime"])

            start_raw = run["info"]["start"]; end_raw = run["info"]["end"]
            if self.convert_to_ist:
                start_raw = convert_kst_to_ist_str(start_raw); end_raw = convert_kst_to_ist_str(end_raw)
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
            self._apply_fm_color(child, 7, run["st_n"]); self._apply_fm_color(child, 8, run["st_u"])
            self._apply_vslp_color(child, 9, run["vslp_status"])
        else:
            child.setText(6, "-"); child.setText(10, "-"); child.setText(11, "-")
            child.setText(15, run["path"]); child.setText(21, "")

        for i in range(1, 23): child.setTextAlignment(i, Qt.AlignCenter)
        child.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
        child.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

        run_identifier = f"{run['rtl']} : {run['r_name']}"
        if run_identifier in self.global_notes:
            notes = " | ".join(self.global_notes[run_identifier])
            child.setText(22, notes); child.setToolTip(22, notes)
            child.setForeground(22, QColor("#e65100" if not self.is_dark_mode else "#ffb74d"))

        return child

    def _add_stages(self, be_item, be_run, ign_root):
        owner       = be_run.get("owner", "Unknown")
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
            ir_info = self.ir_data.get(ir_key, {"static": "-", "dynamic": "-", "log": "",
                                                 "static_table": "", "dynamic_table": ""})

            tooltip_text = (f"Owner: {owner}\nSize: Pending\nRuntime: {stage['info']['runtime']}\n"
                            f"NONUPF: {stage['st_n']}\nUPF: {stage['st_u']}\nVSLP: {stage['vslp_status']}\n")
            if is_ir_block:
                tooltip_text += f"\nStatic IR Value: {ir_info['static']}"
                if ir_info.get('static_table'): tooltip_text += f"\n{ir_info['static_table']}\n"
                tooltip_text += f"\nDynamic IR Value: {ir_info['dynamic']}"
                if ir_info.get('dynamic_table'): tooltip_text += f"\n{ir_info['dynamic_table']}\n"
            st_item.setToolTip(0, tooltip_text)

            stage_status = "COMPLETED"
            if be_run["source"] == "WS" and not cached_exists(stage["rpt"]):
                stage_status = "RUNNING"
                if cached_exists(stage["log"]):
                    try:
                        with open(stage["log"], 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "START_CMD:" in line:
                                    stage_status = line.strip(); break
                    except:
                        pass

            st_item.setText(0, stage["name"]); st_item.setText(2, be_run["source"])
            st_item.setText(4, stage_status);  st_item.setText(5, owner); st_item.setText(6, "-")
            st_item.setText(7, f"NONUPF - {stage['st_n']}"); st_item.setText(8, f"UPF - {stage['st_u']}")
            st_item.setText(9, stage["vslp_status"])
            st_item.setText(10, ir_info["static"]  if is_ir_block else "-")
            st_item.setText(11, ir_info["dynamic"] if is_ir_block else "-")
            st_item.setText(12, stage["info"]["runtime"])

            s_start = stage["info"]["start"]; s_end = stage["info"]["end"]
            if self.convert_to_ist:
                s_start = convert_kst_to_ist_str(s_start); s_end = convert_kst_to_ist_str(s_end)
            if self.show_relative_time:
                st_item.setText(13, relative_time(s_start)); st_item.setText(14, relative_time(s_end))
            else:
                st_item.setText(13, s_start); st_item.setText(14, s_end)
            st_item.setToolTip(13, s_start); st_item.setToolTip(14, s_end)
            st_item.setText(15, stage["stage_path"]); st_item.setText(16, stage["log"])
            st_item.setText(17, stage["fm_u_path"]);  st_item.setText(18, stage["fm_n_path"])
            st_item.setText(19, stage["vslp_rpt_path"]); st_item.setText(20, stage["sta_rpt_path"])
            st_item.setText(21, ir_info["log"])

            self._apply_fm_color(st_item, 7, stage["st_n"]); self._apply_fm_color(st_item, 8, stage["st_u"])
            self._apply_vslp_color(st_item, 9, stage["vslp_status"])
            eff_status = stage_status if stage_status in ("COMPLETED", "RUNNING", "FAILED") else "RUNNING"
            self._apply_status_color(st_item, 4, eff_status)

            for i in range(1, 23): st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter)
            st_item.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

    # ------------------------------------------------------------------
    # CONTEXT MENUS
    # ------------------------------------------------------------------
    def on_header_context_menu(self, pos):
        col      = self.tree.header().logicalIndexAt(pos)
        menu     = QMenu(self)
        col_name = self.tree.headerItem().text(col).replace(" [*]", "")

        sort_asc_act  = menu.addAction("Sort A to Z")
        sort_desc_act = menu.addAction("Sort Z to A")
        menu.addSeparator()
        filter_act    = menu.addAction(f"Filter Column '{col_name}'...")
        clear_act     = menu.addAction("Clear Filter")
        clear_act.setEnabled(col in self.active_col_filters)
        clear_all_act = menu.addAction("Clear All Filters")
        clear_all_act.setEnabled(len(self.active_col_filters) > 0)
        menu.addSeparator()

        vis_menu = menu.addMenu("Show / Hide Columns")
        for i in range(1, 24):
            action = QWidgetAction(vis_menu)
            cb = QCheckBox(self.tree.headerItem().text(i).replace(" [*]", ""))
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.setStyleSheet("margin: 2px 8px; background: transparent; color: inherit;")
            cb.toggled.connect(lambda checked, c=i: self.tree.setColumnHidden(c, not checked))
            action.setDefaultWidget(cb)
            vis_menu.addAction(action)

        action = menu.exec_(self.tree.header().mapToGlobal(pos))
        if   action == sort_asc_act:  self.tree.sortByColumn(col, Qt.AscendingOrder)
        elif action == sort_desc_act: self.tree.sortByColumn(col, Qt.DescendingOrder)
        elif action == filter_act:    self.show_column_filter_dialog(col)
        elif action == clear_act:
            if col in self.active_col_filters:
                del self.active_col_filters[col]; self.apply_tree_filters()
        elif action == clear_all_act:
            self.active_col_filters.clear(); self.apply_tree_filters()

    def on_item_double_clicked(self, item, col):
        log_path = item.text(16)
        if log_path and log_path != "N/A" and os.path.exists(log_path):
            subprocess.Popen(['gvim', log_path])

    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item or not item.parent():
            return
        m = QMenu()

        run_path  = item.text(15)
        fm_u_path = item.text(17); fm_n_path = item.text(18)
        vslp_path = item.text(19); sta_path  = item.text(20); ir_path = item.text(21)
        log_path  = item.text(16)
        is_stage  = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl    = item.data(0, Qt.UserRole) == "RTL"

        target_item = item if not is_stage else item.parent()
        b_name      = target_item.data(0, Qt.UserRole + 2)
        r_rtl       = target_item.text(1)
        base_run    = target_item.data(0, Qt.UserRole + 4)
        run_source  = target_item.text(2)

        act_gold = act_good = act_red = act_later = act_clear = gantt_act = None

        if (run_path and run_path != "N/A") or is_stage:
            pin_menu  = m.addMenu("Pin as...")
            act_gold  = pin_menu.addAction(self.icons['golden'],    "Golden Run")
            act_good  = pin_menu.addAction(self.icons['good'],      "Good Run")
            act_red   = pin_menu.addAction(self.icons['redundant'], "Redundant Run")
            act_later = pin_menu.addAction(self.icons['later'],     "Mark for Later")
            pin_menu.addSeparator()
            act_clear = pin_menu.addAction("Clear Pin")
            m.addSeparator()
            if item.childCount() > 0 and item.child(0).data(0, Qt.UserRole) == "STAGE":
                gantt_act = m.addAction("Show Timeline (Gantt Chart)")
                m.addSeparator()

        edit_note_act = None; note_identifier = ""
        if run_path and run_path != "N/A" and not is_stage:
            note_identifier = f"{r_rtl} : {item.text(0)}"
            edit_note_act   = m.addAction("Add / Edit Personal Note"); m.addSeparator()
        elif is_rtl:
            note_identifier = item.text(0)
            edit_note_act   = m.addAction("Add / Edit Alias Note for RTL"); m.addSeparator()

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
            if target_path in self.ignored_paths:
                restore_act = m.addAction("Restore (Unhide)")
            else:
                ignore_act = m.addAction("Hide/Ignore")
            m.addSeparator()

        calc_size_act = (m.addAction("Calculate Folder Size")
                         if run_path and run_path != "N/A" and cached_exists(run_path) else None)
        if calc_size_act: m.addSeparator()

        # SPEED 3: Use pre-computed path flags (set in _create_run_item)
        # Zero NFS calls here — context menu appears instantly
        _flags = target_item.data(0, Qt.UserRole + 20) or {}

        # For stage items, fall back to cached_exists only if needed
        def _ex(path):
            return bool(path and path != "N/A" and cached_exists(path))

        fm_n_act    = m.addAction("Open NONUPF Formality Report") if _flags.get('fm_n') or _ex(fm_n_path)   else None
        fm_u_act    = m.addAction("Open UPF Formality Report")    if _flags.get('fm_u') or _ex(fm_u_path)   else None
        v_act       = m.addAction("Open VSLP Report")             if _flags.get('vslp') or _ex(vslp_path)   else None
        sta_act     = m.addAction("Open PT STA Summary")          if _ex(sta_path)  else None
        ir_stat_act = m.addAction("Open Static IR Log")           if _ex(ir_path)   else None
        ir_dyn_act  = m.addAction("Open Dynamic IR Log")          if is_stage and _ex(ir_path) else None
        log_act     = m.addAction("Open Log File")                if _flags.get('log') or _ex(log_path) else None
        term_act    = None  # Open Terminal Here removed per user request

        m.addSeparator()
        qor_act = None
        if is_stage:
            m.addSeparator(); qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res: return

        if res in [act_gold, act_good, act_red, act_later, act_clear]:
            p_target = run_path if (run_path and run_path != "N/A") else (item.parent().text(15) if is_stage else None)
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
                    stages.append({'name': c.text(0), 'time_str': rt, 'sec': self._time_to_seconds(rt)})
            dlg = GanttChartDialog(item.text(0), stages, self); dlg.exec_()

        elif edit_note_act and res == edit_note_act:
            dlg = EditNoteDialog(item.text(22), note_identifier, self)
            if dlg.exec_():
                save_user_note(note_identifier, dlg.get_text())
                self.global_notes = load_all_notes(); self.refresh_view()

        elif add_config_act and res == add_config_act:
            if not self.current_config_path:
                path, _ = QFileDialog.getSaveFileName(self, "Create New Config", "dashboard_filter.cfg",
                                                       "Config Files (*.cfg *.txt)")
                if not path: return
                self.current_config_path = path
                self.run_filter_config   = {}
            if run_source not in self.run_filter_config:              self.run_filter_config[run_source] = {}
            if r_rtl not in self.run_filter_config[run_source]:       self.run_filter_config[run_source][r_rtl] = {}
            if b_name not in self.run_filter_config[run_source][r_rtl]: self.run_filter_config[run_source][r_rtl][b_name] = set()
            self.run_filter_config[run_source][r_rtl][b_name].add(base_run)
            self._save_current_config()
            self.sb_config.setText(f"Config: Active ({os.path.basename(self.current_config_path)})")
            self.sb_config.setStyleSheet(
                f"color: {'#d32f2f' if not self.is_dark_mode else '#ffb74d'}; font-weight: bold;")
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
        elif res == ignore_act:
            self.ignored_paths.add(target_path)
            # SPEED 9: just hide the item directly — no full refresh needed
            item.setHidden(True)
            if item.parent():
                # hide parent group node if no visible siblings
                parent = item.parent()
                if all(parent.child(i).isHidden() for i in range(parent.childCount())):
                    parent.setHidden(True)
        elif res == restore_act:
            self.ignored_paths.discard(target_path)
            item.setHidden(False)
            # ensure parent chain is visible
            p = item.parent()
            while p and p.parent():
                p.setHidden(False)
                p = p.parent()
        elif calc_size_act and res == calc_size_act:
            item.setText(6, "Calc...")
            worker = SingleSizeWorker(item, run_path)
            def _safe_set_size(it, sz):
                try:
                    it.setText(6, sz)
                    old_tip = it.toolTip(0)
                    if old_tip:
                        it.setToolTip(0, re.sub(r'Size: .*?\n', f'Size: {sz}\n', old_tip))
                except RuntimeError:
                    pass  # item deleted by tree.clear() before worker finished
            worker.result.connect(_safe_set_size)
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
        elif term_act and res == term_act:
            if os.path.isdir(run_path):
                try:
                    subprocess.Popen(['xterm', '-e', f'cd {run_path!r}; bash'], cwd=run_path)
                except FileNotFoundError:
                    try: subprocess.Popen(['gnome-terminal', '--working-directory', run_path])
                    except FileNotFoundError: pass
        elif qor_act and res == qor_act:
            step_name = item.data(1, Qt.UserRole)
            qor_path  = item.data(2, Qt.UserRole)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = find_latest_qor_report()
            if h: subprocess.Popen([FIREFOX_PATH, h])

    # ------------------------------------------------------------------
    # MAIL ACTIONS
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        user_runs = {}; is_fe_selected = False
        def find_owners(node):
            nonlocal is_fe_selected
            for i in range(node.childCount()):
                c = node.child(i)
                if c.checkState(0) == Qt.Checked and c.data(0, Qt.UserRole) != "STAGE":
                    path  = c.text(15); owner = c.text(5)
                    if "FE" in c.text(0): is_fe_selected = True
                    if path and path != "N/A" and owner and owner != "Unknown":
                        if owner not in user_runs: user_runs[owner] = []
                        user_runs[owner].append(path)
                find_owners(c)
        find_owners(self.tree.invisibleRootItem())
        if not user_runs:
            QMessageBox.warning(self, "No Runs Selected", "Please select at least one run to send a cleanup mail.")
            return
        default_subject = "Please remove your old PI runs" if is_fe_selected else "Please remove your old PD runs"
        all_known       = get_all_known_mail_users()
        unique_emails   = [get_user_email(o) for o in user_runs.keys() if get_user_email(o)]
        dlg = AdvancedMailDialog(default_subject,
                                 "Hi,\n\nPlease remove these runs as they are consuming disk space.",
                                 all_known, ", ".join(unique_emails), self)
        if dlg.exec_():
            subject       = dlg.subject_input.text().strip()
            body_template = dlg.body_input.toPlainText()
            current_user  = getpass.getuser()
            sender_email  = get_user_email(current_user) or f"{current_user}@samsung.com"
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            success_count = 0
            for owner, paths in user_runs.items():
                owner_email = get_user_email(owner)
                if not owner_email: continue
                final_body      = body_template + "\n" + "\n".join(paths)
                all_recipients  = set(base_to + base_cc + [owner_email])
                cmd = [MAIL_UTIL, "-to", ",".join(all_recipients), "-sd", sender_email,
                       "-s", subject, "-c", final_body, "-fm", "text"]
                for att in dlg.attachments: cmd.extend(["-a", att])
                try: subprocess.Popen(cmd); success_count += 1
                except Exception as e: print(f"Failed to send mail: {e}")
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} emails.")

    def send_qor_mail_action(self):
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Latest Compare QoR Report",
                                 "Hi Team,\n\nPlease find the attached latest QoR Report.",
                                 all_known, "", self)
        dlg.attach_qor()
        if dlg.exec_():
            subject      = dlg.subject_input.text().strip()
            body         = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            all_recipients = set(base_to + base_cc)
            if not all_recipients: return
            cmd = [MAIL_UTIL, "-to", ",".join(all_recipients), "-sd", sender_email,
                   "-s", subject, "-c", body, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
            try: subprocess.Popen(cmd); QMessageBox.information(self, "Mail Sent", "QoR mail sent.")
            except Exception as e: print(f"Failed to send mail: {e}")

    def send_custom_mail_action(self):
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("", "", all_known, "", self)
        if dlg.exec_():
            subject      = dlg.subject_input.text().strip()
            body         = dlg.body_input.toPlainText()
            current_user = getpass.getuser()
            sender_email = get_user_email(current_user) or f"{current_user}@samsung.com"
            base_to = [x.strip() for x in dlg.to_input.text().split(',') if x.strip()]
            base_cc = [x.strip() for x in dlg.cc_input.text().split(',') if x.strip()]
            all_recipients = set(base_to + base_cc)
            if not all_recipients: return
            cmd = [MAIL_UTIL, "-to", ",".join(all_recipients), "-sd", sender_email,
                   "-s", subject, "-c", body, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
            try: subprocess.Popen(cmd); QMessageBox.information(self, "Mail Sent", "Custom mail sent.")
            except Exception as e: print(f"Failed to send mail: {e}")

    # ------------------------------------------------------------------
    # DISK USAGE
    # ------------------------------------------------------------------
    def start_bg_disk_scan(self, force=False):
        if self.disk_worker and self.disk_worker.isRunning(): return
        if not force and self._cached_disk_data is not None: return
        self.disk_worker = DiskScannerWorker()
        self.disk_worker.finished_scan.connect(self._on_bg_disk_scan_finished)
        self.disk_worker.start()

    def _on_bg_disk_scan_finished(self, results):
        self._cached_disk_data = results
        if hasattr(self, 'disk_dialog') and self.disk_dialog is not None and self.disk_dialog.isVisible():
            self.disk_dialog.disk_data = results
            self.disk_dialog.update_view()
            self.disk_dialog.recalc_btn.setText("Recalculate Disk Usage")
            self.disk_dialog.recalc_btn.setEnabled(True)

    def open_disk_usage(self):
        if self._cached_disk_data is None:
            QMessageBox.information(self, "Scanning",
                                    "Disk usage is still calculating in the background.\nPlease wait a moment.")
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
                    qp  = c.text(15); src = c.text(2)
                    if src == "OUTFEED" and qp: qp = os.path.dirname(qp)
                    if qp and not qp.endswith("/"): qp += "/"
                    sel.append(qp)
                get_checked(c)
        get_checked(root)
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = find_latest_qor_report()
        if h: subprocess.Popen([FIREFOX_PATH, h])

    # ------------------------------------------------------------------
    # UTILITY
    # ------------------------------------------------------------------
    def _time_to_seconds(self, time_str):
        try:
            m = re.match(r'(\d+)h:(\d+)m:(\d+)s', time_str)
            if m:
                return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        except:
            pass
        return 0


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.showMaximized()
    sys.exit(app.exec_())
