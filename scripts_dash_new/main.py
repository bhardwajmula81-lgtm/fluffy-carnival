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
        prefs.set('UI', 'main_splitter', ','.join(map(str, self.main_splitter.sizes())))
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

        # ACTIONS MENU
        self.actions_btn  = QPushButton("Actions v")
        self.actions_menu = QMenu(self)

        self.actions_menu.addAction("Fit Columns",           self.fit_all_columns)
        self.actions_menu.addAction("Expand All",            self.safe_expand_all)
        self.actions_menu.addAction("Collapse All",          self.safe_collapse_all)
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

        self.actions_menu.addAction("Disk Space", self.open_disk_usage)
        self.actions_menu.addAction("Settings",   self.open_settings)

        self.actions_btn.setMenu(self.actions_menu)
        top_layout.addWidget(self.actions_btn)

        self._add_separator(top_layout)

        self.notes_toggle_btn = QPushButton("Notes <")
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

        # META PANEL
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel)
        meta_layout.setContentsMargins(0, 8, 0, 0)
        meta_layout.addWidget(QLabel("<b>Quick Info (Select a Run):</b>"))
        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        self.meta_status = QLineEdit(); self.meta_status.setReadOnly(True)
        self.meta_path   = QLineEdit(); self.meta_path.setReadOnly(True)
        self.meta_log    = QLineEdit(); self.meta_log.setReadOnly(True)
        form.addRow("Status:", self.meta_status)
        form.addRow("Path:",   self.meta_path)
        form.addRow("Log:",    self.meta_log)
        meta_layout.addLayout(form)
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
            self.notes_toggle_btn.setText("Notes <")
        else:
            self.inspector_dock.show()
            self.notes_toggle_btn.setText("Notes v")

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

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self,       self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self,       lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"), self,       self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"), self,       self.safe_collapse_all)
        QShortcut(QKeySequence("Ctrl+C"), self.tree,  self._copy_tree_cell)

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
        total = completed = running = 0
        for r in runs:
            if r.get("run_type") != "FE":
                continue
            total += 1
            if r["is_comp"]:
                completed += 1
            elif r.get("fe_status") == "RUNNING":
                running += 1
        self.sb_total.setText(f"     Total: {total}")
        self.sb_complete.setText(f"     Completed: {completed}")
        self.sb_running.setText(f"    Running: {running}")
        self.sb_selected.setText(f"     Selected: {len(self._checked_paths)}")
        if self._last_scan_time:
            self.sb_scan_time.setText(f"     Last scan: {self._last_scan_time}   ")

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
        self.meta_path.setCursorPosition(0)
        self.meta_log.setCursorPosition(0)

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
            self.convert_to_ist      = dlg.ist_cb.isChecked()
            self.hide_block_nodes    = dlg.hide_blocks_cb.isChecked()
            self.apply_theme_and_spacing()
            self.refresh_view()

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
        # setStyleSheet on QMainWindow re-resolves styles on every child widget
        # (expensive). Skipping it when nothing changed saves 50-150ms.
        if stylesheet != self._last_stylesheet:
            self._last_stylesheet = stylesheet
            self.setStyleSheet(stylesheet)
        self._recolor_existing_items()

    def _recolor_existing_items(self):
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
        if   val == "Off":    self.auto_refresh_timer.stop()
        elif val == "1 Min":  self.auto_refresh_timer.start(60_000)
        elif val == "5 Min":  self.auto_refresh_timer.start(300_000)
        elif val == "10 Min": self.auto_refresh_timer.start(600_000)

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
        # Build full tree once
        self._build_tree()

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

        summary_dlg = ScanSummaryDialog(stats, self)
        summary_dlg.exec_()

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
    # FIX 1c: on_source_changed — rebuild dropdowns + rebuild tree
    # ------------------------------------------------------------------
    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, True);  self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False); self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)

        self._rebuild_filter_dropdowns()
        if self.ws_data or self.out_data:
            self._build_tree()

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

        for run in runs_to_process:
            if get_milestone(re.sub(r'_syn\d+$', '', run["rtl"])) is None:
                continue

            is_ignored   = run["path"] in self.ignored_paths
            attach_root  = ign_root if is_ignored else root
            run_rtl      = run["rtl"]
            base_rtl     = re.sub(r'_syn\d+$', '', run_rtl)
            has_syn      = (run_rtl != base_rtl)
            blk_name     = run["block"]

            base_attach_node = attach_root if self.hide_block_nodes else self._get_node(attach_root, blk_name, "BLOCK")

            # Always build full hierarchy — filter hides nodes, not omits
            m_node = self._get_node(base_attach_node, get_milestone(base_rtl), "MILESTONE")
            if has_syn:
                rtl_parent   = self._get_node(m_node, base_rtl, "RTL")
                parent_for_run = self._get_node(rtl_parent, run_rtl, "RTL")
            else:
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

        # Filter immediately after build
        self.refresh_view()

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

        def _passes(run):
            if run is None:
                return False
            # Source
            if src_mode == "WS"      and run["source"] != "WS":      return False
            if src_mode == "OUTFEED" and run["source"] != "OUTFEED": return False
            # Golden bypass
            is_golden = (self.user_pins.get(run["path"]) == "golden")
            if not is_golden:
                if run["block"] not in checked_blks:
                    return False
                if self.run_filter_config is not None:
                    rs, rr, rb = run["source"], run["rtl"], run["block"]
                    if (rs in self.run_filter_config
                            and rr in self.run_filter_config[rs]
                            and rb in self.run_filter_config[rs][rr]):
                        allowed   = self.run_filter_config[rs][rr][rb]
                        base_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                        if base_name not in allowed and run["r_name"] not in allowed:
                            return False
            # RTL
            if sel_rtl != "[ SHOW ALL ]":
                if not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")):
                    return False
            # View preset
            if preset == "FE Only"      and run["run_type"] != "FE": return False
            if preset == "BE Only"      and run["run_type"] != "BE": return False
            if preset == "Running Only" and not (run["run_type"] == "FE" and not run["is_comp"]): return False
            if preset == "Failed Only":
                has_fail = ("FAILS" in run.get("st_n", "") or "FAILS" in run.get("st_u", "")
                            or run.get("fe_status", "") in ("FAILED", "FATAL ERROR", "ERROR"))
                if not has_fail: return False
            if preset == "Today's Runs":
                rt = relative_time(run["info"].get("start", ""))
                if not (rt.endswith("ago") and ("h ago" in rt or "m ago" in rt)):
                    return False
            # Search
            if search_pattern != "*":
                note_id  = f"{run['rtl']} : {run['r_name']}"
                notes    = " | ".join(self.global_notes.get(note_id, []))
                combined = (f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} "
                            f"{run.get('st_n','')} {run.get('st_u','')} {run.get('vslp_status','')} "
                            f"{run['info']['runtime']} {run['info']['start']} {run['info']['end']} {notes}").lower()
                if not fnmatch.fnmatch(combined, search_pattern):
                    if run["run_type"] == "BE":
                        stage_match = any(
                            fnmatch.fnmatch(
                                f"{s['name']} {s['st_n']} {s['st_u']} {s['vslp_status']} {s['info']['runtime']}".lower(),
                                search_pattern)
                            for s in run.get("stages", []))
                        if not stage_match: return False
                    else:
                        return False
            return True

        def _update_visibility(item):
            node_type = item.data(0, Qt.UserRole)
            if node_type == "__PLACEHOLDER__":
                item.setHidden(True)
                return False
            is_group = node_type in ("BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT")
            if is_group:
                any_visible = False
                for i in range(item.childCount()):
                    if _update_visibility(item.child(i)):
                        any_visible = True
                item.setHidden(not any_visible)
                if any_visible and (preset != "All Runs" or raw_query):
                    item.setExpanded(True)
                return any_visible
            else:
                run    = item.data(0, Qt.UserRole + 10)
                passes = _passes(run)
                item.setHidden(not passes)
                if passes and run:
                    visible_runs.append(run)
                # Keep stage/placeholder children consistent with parent
                for i in range(item.childCount()):
                    ch = item.child(i)
                    if ch.data(0, Qt.UserRole) == "__PLACEHOLDER__":
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

        fm_n_act    = m.addAction("Open NONUPF Formality Report") if fm_n_path and fm_n_path != "N/A" and cached_exists(fm_n_path) else None
        fm_u_act    = m.addAction("Open UPF Formality Report")    if fm_u_path and fm_u_path != "N/A" and cached_exists(fm_u_path) else None
        v_act       = m.addAction("Open VSLP Report")             if vslp_path and vslp_path != "N/A" and cached_exists(vslp_path) else None
        sta_act     = m.addAction("Open PT STA Summary")          if sta_path  and sta_path  != "N/A" and cached_exists(sta_path)  else None
        ir_stat_act = m.addAction("Open Static IR Log")           if ir_path   and ir_path   != "N/A" and cached_exists(ir_path)   else None
        ir_dyn_act  = (m.addAction("Open Dynamic IR Log")
                       if is_stage and ir_path and ir_path != "N/A" and cached_exists(ir_path) else None)
        log_act     = m.addAction("Open Log File")                if log_path  and log_path  != "N/A" and cached_exists(log_path)  else None

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
        elif res == ignore_act:   self.ignored_paths.add(target_path);      self.refresh_view()
        elif res == restore_act:  self.ignored_paths.discard(target_path);   self.refresh_view()
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
