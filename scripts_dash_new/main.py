# -*- coding: utf-8 -*-
import sys
import os
import re
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

        self.ws_data  = {}
        self.out_data = {}
        self.ir_data  = {}
        self.global_notes = {}
        self.user_pins = load_user_pins()

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

        # Ultra-fast hide/show filters only need 150ms timeout now
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.refresh_view)

        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.start_fs_scan)

        self.icons = {
            'golden': self._create_dot_icon("#FFC107", "#FF9800"),
            'good': self._create_dot_icon("#4CAF50", "#388E3C"),
            'redundant': self._create_dot_icon("#F44336", "#D32F2F"),
            'later': self._create_dot_icon("#FF9800", "#F57C00")
        }

        self.init_ui()
        self._setup_shortcuts()
        
        self.start_fs_scan()
        self.start_bg_disk_scan()

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
        if not prefs.has_section('UI'): prefs.add_section('UI')
        prefs.set('UI', 'main_splitter', ','.join(map(str, self.main_splitter.sizes())))
        with open(USER_PREFS_FILE, 'w') as f: prefs.write(f)
        os._exit(0)

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(8, 8, 8, 4)
        root_layout.setSpacing(6)

        # TOP BAR - MODULAR LOOK
        top_layout = QHBoxLayout()
        top_layout.setSpacing(8)

        top_layout.addWidget(self._label("<b>Source:</b>"))
        self.src_combo = QComboBox()
        self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.setFixedWidth(100)
        self.src_combo.currentIndexChanged.connect(self.on_source_changed)
        top_layout.addWidget(self.src_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("<b>RTL Release:</b>"))
        self.rel_combo = QComboBox()
        self.rel_combo.setMinimumWidth(220)
        self.rel_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.rel_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("<b>View:</b>"))
        self.view_combo = QComboBox()
        self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Running Only", "Failed Only", "Today's Runs"])
        self.view_combo.setFixedWidth(120)
        self.view_combo.currentIndexChanged.connect(self.refresh_view)
        top_layout.addWidget(self.view_combo)

        self._add_separator(top_layout)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search runs, blocks, status, runtime...   [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.textChanged.connect(lambda: self.search_timer.start(150))
        top_layout.addWidget(self.search)

        top_layout.addStretch()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setCursor(Qt.PointingHandCursor)
        self.refresh_btn.clicked.connect(self.start_fs_scan)
        top_layout.addWidget(self.refresh_btn)

        self.auto_combo = QComboBox()
        self.auto_combo.addItems(["Off", "1 Min", "5 Min", "10 Min"])
        self.auto_combo.setFixedWidth(75)
        self.auto_combo.currentIndexChanged.connect(self.on_auto_refresh_changed)
        top_layout.addWidget(self.auto_combo)

        self._add_separator(top_layout)

        # EXTRACTED MODULAR BUTTONS
        self.qor_btn = QPushButton("Compare QoR")
        self.qor_btn.setCursor(Qt.PointingHandCursor)
        self.qor_btn.clicked.connect(self.run_qor_comparison)
        top_layout.addWidget(self.qor_btn)
        
        self.settings_btn = QPushButton("Settings")
        self.settings_btn.setCursor(Qt.PointingHandCursor)
        self.settings_btn.clicked.connect(self.open_settings)
        top_layout.addWidget(self.settings_btn)

        # ACTIONS MENU
        self.actions_btn = QPushButton("Actions v")
        self.actions_btn.setCursor(Qt.PointingHandCursor)
        self.actions_menu = QMenu(self)
        
        self.actions_menu.addAction("Fit Columns", self.fit_all_columns)
        self.actions_menu.addAction("Expand All", self.safe_expand_all)
        self.actions_menu.addAction("Collapse All", self.safe_collapse_all)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Calculate All Run Sizes", self.calculate_all_sizes)
        self.actions_menu.addAction("Export to CSV", self.export_csv)
        self.actions_menu.addSeparator()
        
        mail_menu = self.actions_menu.addMenu("Send Mail...")
        mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        mail_menu.addAction("Send Compare QoR Mail", self.send_qor_mail_action)
        mail_menu.addAction("Send Custom Mail", self.send_custom_mail_action)
        self.actions_menu.addSeparator()
        
        filt_menu = self.actions_menu.addMenu("Filter Configs...")
        filt_menu.addAction("Load Run Filter Config...", self.load_filter_config)
        filt_menu.addAction("Clear Run Filter Config", self.clear_filter_config)
        filt_menu.addAction("Generate Sample Config", self.generate_sample_config)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Disk Space", self.open_disk_usage)
        
        self.actions_btn.setMenu(self.actions_menu)
        top_layout.addWidget(self.actions_btn)
        
        self._add_separator(top_layout)
        
        # NOTES TOGGLER
        self.notes_toggle_btn = QPushButton("Notes <")
        self.notes_toggle_btn.setCursor(Qt.PointingHandCursor)
        self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock)
        top_layout.addWidget(self.notes_toggle_btn)

        root_layout.addLayout(top_layout)

        # Loading Progress Bar
        self.prog_container = QWidget()
        self.prog_container.setFixedHeight(30)
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
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(50))
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
        self.meta_path = QLineEdit(); self.meta_path.setReadOnly(True)
        self.meta_log = QLineEdit(); self.meta_log.setReadOnly(True)
        
        form.addRow("Status:", self.meta_status)
        form.addRow("Path:", self.meta_path)
        form.addRow("Log:", self.meta_log)
        meta_layout.addLayout(form)
        left_layout.addWidget(self.meta_panel, 0)

        self.main_splitter.addWidget(left_panel)

        # MAIN TREE
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

        # Restored missing Custom Context Menu logic
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
        self.tree.itemExpanded.connect(self.on_item_expanded)

        for i in [15, 16, 17, 18, 19, 20, 21, 23]: self.tree.setColumnHidden(i, True)

        # Restored missing Tree Context Menu logic
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        
        # Double Click restores!
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.tree.itemChanged.connect(self._on_item_check_changed)

        self.main_splitter.addWidget(self.tree)
        root_layout.addWidget(self.main_splitter)

        # RIGHT PANEL (Inspector Dock Widget - Stacked Chat Format)
        self.inspector = QWidget()
        ins_layout = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view details.")
        self.ins_lbl.setWordWrap(True)
        
        self.ins_history = QTextEdit()
        self.ins_history.setReadOnly(True)
        
        self.ins_note = QTextEdit()
        self.ins_note.setPlaceholderText("Enter your new note or update your existing note here...")
        self.ins_note.setMaximumHeight(100)
        
        self.ins_save_btn = QPushButton("Save / Update Note")
        self.ins_save_btn.clicked.connect(self.save_inspector_note)
        
        ins_layout.addWidget(self.ins_lbl)
        ins_layout.addWidget(QLabel("<b>Shared Notes History:</b>"))
        ins_layout.addWidget(self.ins_history, 1)
        ins_layout.addWidget(QLabel("<b>Your Contribution:</b>"))
        ins_layout.addWidget(self.ins_note, 0)
        ins_layout.addWidget(self.ins_save_btn)
        
        self.inspector_dock = QDockWidget(self)
        self.inspector_dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.inspector_dock.setTitleBarWidget(QWidget()) # Hide default dock title bar
        self.inspector_dock.setWidget(self.inspector)
        self.addDockWidget(Qt.RightDockWidgetArea, self.inspector_dock)
        self.inspector_dock.hide()

        try:
            m_sizes = [int(x) for x in prefs.get('UI', 'main_splitter', fallback='250,1200').split(',')]
            self.main_splitter.setSizes(m_sizes)
        except: pass

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
    # CORE UI BEHAVIORS
    # ------------------------------------------------------------------
    def toggle_notes_dock(self):
        if self.inspector_dock.isVisible():
            self.inspector_dock.hide()
            self.notes_toggle_btn.setText("Notes <")
        else:
            self.inspector_dock.show()
            self.notes_toggle_btn.setText("Notes v")

    def safe_expand_all(self):
        self.tree.blockSignals(True)
        self.tree.expandAll()
        self.tree.blockSignals(False)
        self.tree.resizeColumnToContents(0)

    def safe_collapse_all(self):
        self.tree.blockSignals(True)
        self.tree.collapseAll()
        self.tree.blockSignals(False)
        self.tree.resizeColumnToContents(0)
        
    def on_item_expanded(self, item):
        QTimer.singleShot(10, self._resize_first_col)

    def _resize_first_col(self):
        self.tree.resizeColumnToContents(0)
        if self.tree.columnWidth(0) > 450: self.tree.setColumnWidth(0, 450)

    # ------------------------------------------------------------------
    # CONFIG & EXPORT
    # ------------------------------------------------------------------
    def generate_sample_config(self):
        sample_text = """# PD Dashboard Run Filter Configuration
# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...
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
                        src = parts[0].strip(); rtl = parts[1].strip(); blk = parts[2].strip()
                        runs = set(parts[3].strip().split())
                        if src not in parsed_config: parsed_config[src] = {}
                        if rtl not in parsed_config[src]: parsed_config[src][rtl] = {}
                        parsed_config[src][rtl][blk] = runs
            
            self.run_filter_config = parsed_config
            self.current_config_path = path
            self.sb_config.setText(f"Config: Active ({os.path.basename(path)})")
            self.sb_config.setStyleSheet("color: #d32f2f; font-weight: bold;" if not self.is_dark_mode else "color: #ffb74d; font-weight: bold;")
            self.refresh_view()
            QMessageBox.information(self, "Config Loaded", "Filter configuration applied successfully.")
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
        except: pass

    # ------------------------------------------------------------------
    # UTILS
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

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"),       self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"),       self, lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"),       self, self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"),       self, self.safe_collapse_all)
        QShortcut(QKeySequence("Ctrl+C"),       self.tree, self._copy_tree_cell)

    def _copy_tree_cell(self):
        item = self.tree.currentItem()
        if item:
            col = self.tree.currentColumn()
            if col >= 0:
                text = item.text(col).strip()
                if text: QApplication.clipboard().setText(text)

    def on_item_double_clicked(self, item, col):
        if item.parent():
            log = item.text(16)
            if log and cached_exists(log): 
                subprocess.Popen(['gvim', log])

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

    # ------------------------------------------------------------------
    # SELECTION & NOTES
    # ------------------------------------------------------------------
    def on_tree_selection_changed(self):
        sel = self.tree.selectedItems()
        self.fe_error_btn.setVisible(False)
        self.current_error_log_path = None
        
        if not sel:
            self.ins_lbl.setText("Select a run to view details.")
            self.meta_status.clear(); self.meta_path.clear(); self.meta_log.clear()
            self.ins_history.clear(); self.ins_note.clear()
            self.ins_note.setEnabled(False); self.ins_save_btn.setEnabled(False)
            return

        item = sel[0]
        run_name = item.text(0); rtl = item.text(1)
        is_stage = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl = item.data(0, Qt.UserRole) == "RTL"
        path = item.text(15)
        
        self.meta_status.setText(item.text(3) if not is_stage else item.text(4))
        self.meta_path.setText(path)
        self.meta_log.setText(item.text(16))
        self.meta_path.setCursorPosition(0); self.meta_log.setCursorPosition(0)

        self.ins_note.setEnabled(True); self.ins_save_btn.setEnabled(True)

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

        # POPULATE STACKED CHAT
        notes = self.global_notes.get(self._current_note_id, [])
        full_history = "\n\n".join(notes) if notes else "No notes have been saved for this run yet."
        self.ins_history.setPlainText(full_history)
        
        my_note = ""
        tag_prefix = f"[{getpass.getuser()} -"
        for n in notes:
            if n.startswith(tag_prefix):
                parts = n.split("] ", 1)
                if len(parts) == 2: my_note = parts[1]
                break
        self.ins_note.setPlainText(my_note)
        
        if len(sel) == 1:
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
                        color = ("#e57373" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")) else "#d32f2f")
                        if count == 0: color = ("#81c784" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")) else "#388e3c")
                        self.fe_error_btn.setStyleSheet(
                            f"QPushButton#errorLinkBtn {{ border: none; background: transparent; color: {color}; "
                            f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                            f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                        self.fe_error_btn.setText(f"compile_opt errors: {count}")
                        self.fe_error_btn.setVisible(True)

    def open_error_log(self):
        if self.current_error_log_path and os.path.exists(self.current_error_log_path):
            subprocess.Popen(['gvim', self.current_error_log_path])
            
    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'): return
        txt = self.ins_note.toPlainText()
        save_user_note(self._current_note_id, txt)
        self.global_notes = load_all_notes()
        self.on_tree_selection_changed() # Updates the history text instantly
        
        # Trigger a quick update for column 22
        for i in range(self.tree.topLevelItemCount()):
            self._update_note_labels(self.tree.topLevelItem(i))

    def _update_note_labels(self, node):
        node_type = node.data(0, Qt.UserRole)
        if node_type == "RTL":
            note_id = node.text(0)
            if note_id in self.global_notes:
                node.setText(22, " | ".join(self.global_notes[note_id]))
        elif node_type == "DEFAULT":
            note_id = f"{node.text(1)} : {node.text(0)}"
            if note_id in self.global_notes:
                node.setText(22, " | ".join(self.global_notes[note_id]))
        for i in range(node.childCount()):
            self._update_note_labels(node.child(i))

    # ------------------------------------------------------------------
    # SETTINGS & THEMING
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
                QHeaderView::section {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 6px; font-weight: bold; }}
                QTreeWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; border: 1px solid {fg}; }}
                QListWidget {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; border: 1px solid {fg}; font-weight: bold; }}
                QLineEdit, QSpinBox, QComboBox, QTextEdit {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 6px; border-radius: 6px; }}
                QComboBox QAbstractItemView {{ background-color: {bg}; color: {fg}; selection-background-color: {sel}; selection-color: #ffffff; border-radius: 6px; }}
                QPushButton, QToolButton {{ background-color: {bg}; color: {fg}; border: 1px solid {fg}; padding: 6px 14px; border-radius: 6px; font-weight: bold; }}
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
                QHeaderView::section {{ background-color: #2b2d30; color: #a9b7c6; border: 1px solid #1e1f22; padding: 6px; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #1e1f22; color: #dfe1e5; border: 1px solid #43454a; padding: 6px; border-radius: 6px; }}
                QComboBox {{ background-color: #2b2d30; color: #dfe1e5; border: 1px solid #43454a; padding: 6px; border-radius: 6px; }}
                QComboBox QAbstractItemView {{ background-color: #2b2d30; color: #dfe1e5; selection-background-color: #2f65ca; selection-color: #ffffff; border-radius: 6px; }}
                QPushButton, QToolButton {{ background-color: #393b40; color: #dfe1e5; border: 1px solid #43454a; padding: 6px 14px; border-radius: 6px; }}
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
                QHeaderView::section {{ background-color: #e4e7eb; color: #4a5568; border: 1px solid #cbd5e0; padding: 6px; font-weight: bold; }}
                QTreeWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; border: 1px solid #cbd5e0; }}
                QListWidget {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; border: 1px solid #cbd5e0; font-weight: bold; }}
                QLineEdit, QSpinBox, QTextEdit {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 6px; border-radius: 6px; }}
                QComboBox {{ background-color: #ffffff; color: #333333; border: 1px solid #cbd5e0; padding: 6px; border-radius: 6px; }}
                QComboBox QAbstractItemView {{ background-color: #ffffff; color: #333333; selection-background-color: #3182ce; selection-color: #ffffff; border-radius: 6px; }}
                QPushButton, QToolButton {{ background-color: #ffffff; color: #4a5568; border: 1px solid #cbd5e0; padding: 6px 14px; border-radius: 6px; font-weight: bold; }}
                QPushButton:hover, QToolButton:hover {{ background-color: #edf2f7; border-color: #a0aec0; }}
                QPushButton:pressed, QToolButton:pressed {{ background-color: #e2e8f0; border-color: #a0aec0; }}
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
    # SCANNING & CORE DATA HANDLING
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
            for col in range(24): skel.setForeground(col, skel_color)
                
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
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.prog_container.setVisible(False)
        self.refresh_btn.setEnabled(True)
        self.refresh_btn.setText("Refresh")
        self.tree.setEnabled(True) 
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.global_notes = load_all_notes()
        
        self.update_combos()
        self.build_tree() # Tree is only built once per scan
        
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
        
        summary_dlg = ScanSummaryDialog(stats, self)
        summary_dlg.exec_()

    def update_combos(self):
        releases, blocks = set(), set()
        if self.ws_data:
            releases.update(self.ws_data.get("releases", {}).keys())
            blocks.update(self.ws_data.get("blocks", set()))
        if self.out_data:
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
            it = QListWidgetItem(b)
            it.setData(Qt.UserRole, b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)

    def on_source_changed(self):
        src_mode = self.src_combo.currentText()
        if src_mode == "WS":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)
        elif src_mode == "OUTFEED":
            self.tree.setColumnHidden(2, True);  self.tree.setColumnHidden(3, True);  self.tree.setColumnHidden(4, True)
        else:
            self.tree.setColumnHidden(2, False); self.tree.setColumnHidden(3, False); self.tree.setColumnHidden(4, False)
        self.refresh_view()

    # --- THE NEW LIGHTNING-FAST HIDE/SHOW FILTER METHOD ---
    def refresh_view(self):
        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        
        src_mode = self.src_combo.currentText()
        sel_rtl = self.rel_combo.currentText()
        preset = self.view_combo.currentText()
        raw_query = self.search.text().lower().strip()
        search_pattern = "*" if not raw_query else (f"*{raw_query}*" if '*' not in raw_query else raw_query)
        checked_blks = [self.blk_list.item(i).data(Qt.UserRole) for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]
        
        is_actively_filtered = (preset != "All Runs") or (raw_query != "") or (sel_rtl != "[ SHOW ALL ]") or (src_mode != "ALL")

        def check_run(run):
            if self.user_pins.get(run["path"]) == "golden": return True
            if src_mode != "ALL" and run["source"] != src_mode: return False
            if run["block"] not in checked_blks: return False
            
            if self.run_filter_config is not None:
                r_src, r_rtl, r_blk = run["source"], run["rtl"], run["block"]
                if r_src in self.run_filter_config and r_rtl in self.run_filter_config[r_src] and r_blk in self.run_filter_config[r_src][r_rtl]:
                    allowed = self.run_filter_config[r_src][r_rtl][r_blk]
                    b_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                    if b_name not in allowed and run["r_name"] not in allowed: return False
                    
            base_rtl_filter = re.sub(r'_syn\d+$', '', run["rtl"])
            if sel_rtl != "[ SHOW ALL ]":
                if not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")): return False
                
            if preset == "FE Only" and run["run_type"] != "FE": return False
            if preset == "BE Only" and run["run_type"] != "BE": return False
            if preset == "Running Only" and not (run["run_type"] == "FE" and not run["is_comp"]): return False
            if preset == "Failed Only" and not ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","") or run.get("fe_status") == "FATAL ERROR"): return False
            if preset == "Today's Runs":
                start = run["info"].get("start","")
                rt = relative_time(start)
                if not (rt.endswith("ago") and ("h ago" in rt or "m ago" in rt)): return False
                
            if search_pattern != "*":
                note_id = f"{run['rtl']} : {run['r_name']}"
                notes = " | ".join(self.global_notes.get(note_id, []))
                combined = (f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} "
                            f"{run['st_n']} {run['st_u']} {run['vslp_status']} "
                            f"{run['info']['runtime']} {run['info']['start']} {run['info']['end']} {notes}").lower()
                if not fnmatch.fnmatch(combined, search_pattern): return False
            return True
            
        def check_stage(stage, run):
            if self.user_pins.get(run["path"]) == "golden": return True
            if src_mode != "ALL" and run["source"] != src_mode: return False
            if run["block"] not in checked_blks: return False
            if sel_rtl != "[ SHOW ALL ]" and not (run["rtl"] == sel_rtl or run["rtl"].startswith(sel_rtl + "_")): return False
            if preset == "FE Only": return False
            if preset == "Failed Only" and not ("FAILS" in stage.get("st_n","") or "FAILS" in stage.get("st_u","")): return False
            if search_pattern != "*":
                sc = f"{stage['name']} {stage['st_n']} {stage['st_u']} {stage['vslp_status']} {stage['info']['runtime']}".lower()
                if not fnmatch.fnmatch(sc, search_pattern): return False
            return True

        visible_runs = []
        
        def update_vis(node):
            node_type = node.data(0, Qt.UserRole)
            is_group = node_type in ["BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"]
            
            if node_type == "DEFAULT":
                run = node.data(0, Qt.UserRole + 10)
                matches = check_run(run)
                child_matches = False
                for i in range(node.childCount()):
                    if update_vis(node.child(i)): child_matches = True
                is_vis = matches or child_matches
                node.setHidden(not is_vis)
                if is_vis: visible_runs.append(run)
                return is_vis
            elif node_type == "STAGE":
                stage = node.data(0, Qt.UserRole + 10)
                parent_run = node.parent().data(0, Qt.UserRole + 10)
                matches = check_stage(stage, parent_run)
                node.setHidden(not matches)
                return matches
            else:
                any_vis = False
                for i in range(node.childCount()):
                    if update_vis(node.child(i)): any_vis = True
                if is_actively_filtered and any_vis: node.setExpanded(True)
                elif not is_actively_filtered and node_type in ["MILESTONE", "RTL"]: node.setExpanded(False)
                node.setHidden(not any_vis)
                return any_vis
                
        update_vis(self.tree.invisibleRootItem())
        
        self.apply_tree_filters()
        self._update_status_bar(visible_runs)
        
        self.tree.setUpdatesEnabled(True)
        self.tree.blockSignals(False)

    # --- THE NEW CORE TREE BUILDER (Runs exactly once per scan) ---
    def build_tree(self):
        self.tree.blockSignals(True)
        self.tree.setUpdatesEnabled(False)
        self.tree.setSortingEnabled(False)
        self.tree.clear()
        
        runs_to_process = []
        if self.ws_data: runs_to_process.extend(self.ws_data.get("all_runs", []))
        if self.out_data: runs_to_process.extend(self.out_data.get("all_runs", []))
        
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
                        
        root = self.tree.invisibleRootItem()
        ign_root = self._get_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
        
        fe_runs = [r for r in runs_to_process if r["run_type"] == "FE"]
        be_runs = [r for r in runs_to_process if r["run_type"] == "BE"]
        
        for run in fe_runs:
            target_root = ign_root if run["path"] in self.ignored_paths else root
            base_attach_node = target_root if self.hide_block_nodes else self._get_node(target_root, run["block"], "BLOCK")
            base_rtl = re.sub(r'_syn\d+$', '', run["rtl"])
            m_node = self._get_node(base_attach_node, get_milestone(base_rtl) or "UNKNOWN", "MILESTONE")
            parent_for_run = self._get_node(m_node, base_rtl, "RTL") 
            
            item = self._create_run_item(parent_for_run, run)
            item.setData(0, Qt.UserRole + 10, run)
            
        for run in be_runs:
            target_root = ign_root if run["path"] in self.ignored_paths else root
            base_attach_node = target_root if self.hide_block_nodes else self._get_node(target_root, run["block"], "BLOCK")
            base_rtl = re.sub(r'_syn\d+$', '', run["rtl"])
            m_node = self._get_node(base_attach_node, get_milestone(base_rtl) or "UNKNOWN", "MILESTONE")
            parent_for_run = self._get_node(m_node, base_rtl, "RTL")
            
            fe_parent = None
            for i in range(parent_for_run.childCount()):
                c = parent_for_run.child(i)
                if c.data(0, Qt.UserRole) != "STAGE":
                    fe_base = c.text(0).replace("-FE", "")
                    clean_be = run["r_name"].replace("-BE", "")
                    if c.text(2) == run["source"] and c.data(0, Qt.UserRole + 2) == run["block"] and (clean_be == fe_base or f"_{fe_base}_" in clean_be or clean_be.startswith(f"{fe_base}_") or clean_be.endswith(f"_{fe_base}")):
                        fe_parent = c; break
                        
            actual_parent = fe_parent if fe_parent else parent_for_run
            be_item = self._create_run_item(actual_parent, run)
            be_item.setData(0, Qt.UserRole + 10, run)
            self._add_stages(be_item, run, ign_root)
            
        if ign_root.childCount() == 0: root.removeChild(ign_root)
        
        def update_pins(node):
            if node.data(0, Qt.UserRole) not in ["BLOCK", "RTL", "MILESTONE", "IGNORED_ROOT"]:
                pin_type = self.user_pins.get(node.text(15))
                if pin_type in self.icons:
                    node.setIcon(0, self.icons[pin_type])
                    node.setData(0, Qt.UserRole + 5, pin_type)
                else:
                    node.setIcon(0, QIcon())
                    node.setData(0, Qt.UserRole + 5, None)
            for i in range(node.childCount()): update_pins(node.child(i))
        update_pins(root)
        
        self.tree.blockSignals(False)
        self.tree.setUpdatesEnabled(True)
        self.tree.setSortingEnabled(True)
        self.refresh_view()

    # ------------------------------------------------------------------
    # ITEM CREATION
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
                if col in self.active_col_filters: del self.active_col_filters[col]
            else: self.active_col_filters[col] = selected
            self.apply_tree_filters()

    def apply_tree_filters(self):
        for col in range(self.tree.columnCount()):
            orig_text = self.tree.headerItem().text(col).replace(" [*]", "")
            if col in self.active_col_filters: self.tree.headerItem().setText(col, orig_text + " [*]")
            else: self.tree.headerItem().setText(col, orig_text)

        def update_visibility(item):
            item_matches = True
            is_group_node = item.data(0, Qt.UserRole) in ["BLOCK", "MILESTONE", "RTL", "IGNORED_ROOT"]
            if not is_group_node:
                for col, allowed in self.active_col_filters.items():
                    val = item.text(col).strip()
                    if val not in allowed:
                        item_matches = False; break

            any_child_visible = False
            for i in range(item.childCount()):
                if update_visibility(item.child(i)): any_child_visible = True

            is_visible = any_child_visible if is_group_node else (item_matches or any_child_visible)
            item.setHidden(not is_visible)
            return is_visible

        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()): update_visibility(root.child(i))

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
            hl_color = QColor(self.custom_sel_color if self.use_custom_colors else ("#404652" if self.is_dark_mode else "#e3f2fd"))
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
            
            child.setText(15, run["path"]); child.setText(16, os.path.join(run["path"], "logs/compile_opt.log"))
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
            st_item.setData(0, Qt.UserRole + 10, stage)

            ir_key  = f"{be_run['r_name']}/{stage['name']}"
            ir_info = self.ir_data.get(ir_key, {"static": "-", "dynamic": "-", "log": "", "static_table": "", "dynamic_table": ""})

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
                                if "START_CMD:" in line: stage_status = line.strip(); break
                    except: pass

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
            self._apply_status_color(st_item, 4, stage_status if stage_status in ("COMPLETED","RUNNING","FAILED") else "RUNNING")

            for i in range(1, 23): st_item.setTextAlignment(i, Qt.AlignCenter)
            st_item.setTextAlignment(0, Qt.AlignLeft | Qt.AlignVCenter); st_item.setTextAlignment(22, Qt.AlignLeft | Qt.AlignVCenter)

    # ------------------------------------------------------------------
    # RESTORED MISSING CONTEXT MENUS (The cause of your errors!)
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
        for i in range(1, 24):
            action = QWidgetAction(vis_menu)
            cb = QCheckBox(self.tree.headerItem().text(i).replace(" [*]", ""))
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.setStyleSheet("margin: 2px 8px; background: transparent; color: inherit;")
            cb.toggled.connect(lambda checked, c=i: self.tree.setColumnHidden(c, not checked))
            action.setDefaultWidget(cb)
            vis_menu.addAction(action)

        action = menu.exec_(self.tree.header().mapToGlobal(pos))
        if action == sort_asc_act: self.tree.sortByColumn(col, Qt.AscendingOrder)
        elif action == sort_desc_act: self.tree.sortByColumn(col, Qt.DescendingOrder)
        elif action == filter_act: self.show_column_filter_dialog(col)
        elif action == clear_act:
            if col in self.active_col_filters:
                del self.active_col_filters[col]; self.apply_tree_filters()
        elif action == clear_all_act:
            self.active_col_filters.clear(); self.apply_tree_filters()

    def on_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item or not item.parent(): return
        col = self.tree.columnAt(pos.x())
        m   = QMenu()

        run_path  = item.text(15)
        fm_u_path = item.text(17); fm_n_path = item.text(18)
        vslp_path = item.text(19); sta_path  = item.text(20); ir_path = item.text(21)
        log_path  = item.text(16)
        is_stage  = item.data(0, Qt.UserRole) == "STAGE"
        is_rtl    = item.data(0, Qt.UserRole) == "RTL"
        
        target_item = item if not is_stage else item.parent()
        b_name = target_item.data(0, Qt.UserRole + 2)
        r_rtl = target_item.text(1)
        base_run = target_item.data(0, Qt.UserRole + 4)
        run_source = target_item.text(2)
        
        act_gold = act_good = act_red = act_later = act_clear = None
        gantt_act = None
        
        if (run_path and run_path != "N/A") or is_stage:
            pin_menu = m.addMenu("Pin as...")
            act_gold = pin_menu.addAction(self.icons['golden'], "Golden Run")
            act_good = pin_menu.addAction(self.icons['good'], "Good Run")
            act_red = pin_menu.addAction(self.icons['redundant'], "Redundant Run")
            act_later = pin_menu.addAction(self.icons['later'], "Mark for Later")
            pin_menu.addSeparator()
            act_clear = pin_menu.addAction("Clear Pin")
            m.addSeparator()
            
            if item.childCount() > 0 and item.child(0).data(0, Qt.UserRole) == "STAGE":
                gantt_act = m.addAction("Show Timeline (Gantt Chart)")
                m.addSeparator()
        
        edit_note_act = None; note_identifier = ""
        if run_path and run_path != "N/A" and not is_stage:
            note_identifier = f"{r_rtl} : {item.text(0)}"
            edit_note_act = m.addAction("Add / Edit Personal Note"); m.addSeparator()
        elif is_rtl:
            note_identifier = item.text(0)
            edit_note_act = m.addAction("Add / Edit Alias Note for RTL"); m.addSeparator()
        
        add_config_act = None
        if b_name and r_rtl and base_run and run_source:
            if self.current_config_path: add_config_act = m.addAction("Add Run to Active Filter Config")
            else: add_config_act = m.addAction("Create New Filter Config & Add Run")
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
        qor_act = None
        if is_stage: m.addSeparator(); qor_act = m.addAction("Run Single Stage QoR")

        res = m.exec_(self.tree.viewport().mapToGlobal(pos))
        if not res: return
        
        if res in [act_gold, act_good, act_red, act_later, act_clear]:
            p_target = run_path if run_path and run_path != "N/A" else (item.parent().text(15) if is_stage else None)
            if p_target:
                if res == act_gold: self.user_pins[p_target] = 'golden'
                elif res == act_good: self.user_pins[p_target] = 'good'
                elif res == act_red: self.user_pins[p_target] = 'redundant'
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
            current_note = item.text(22)
            dlg = EditNoteDialog(current_note, note_identifier, self)
            if dlg.exec_():
                save_user_note(note_identifier, dlg.get_text())
                self.global_notes = load_all_notes(); self.refresh_view()

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
            self.sb_config.setStyleSheet(f"color: {'#d32f2f' if not self.is_dark_mode else '#ffb74d'}; font-weight: bold;")
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
        elif qor_act and res == qor_act:
            step_name = item.data(1, Qt.UserRole); qor_path  = item.data(2, Qt.UserRole)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = find_latest_qor_report()
            if h: subprocess.Popen([FIREFOX_PATH, h])

    # ------------------------------------------------------------------
    # EMAILS & DISK USAGE
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

        default_subject = "Action Required: Please clean up heavy disk usage runs"
        if user_runs: default_subject = "Please remove your old PI runs" if is_fe_selected else "Please remove your old PD runs"
            
        all_known = get_all_known_mail_users()
        unique_emails = [get_user_email(owner) for owner in user_runs.keys() if get_user_email(owner)]
            
        dlg = AdvancedMailDialog(default_subject, "Hi,\n\nPlease remove these runs as they are consuming disk space and are no longer needed:\n\n",
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
                except Exception as e: print(f"Failed to send mail: {e}")
                    
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
            
            cmd = [MAIL_UTIL, "-to", ",".join(all_recipients), "-sd", sender_email, "-s", subject, "-c", body_template, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
                
            try:
                subprocess.Popen(cmd); QMessageBox.information(self, "Mail Sent", "Successfully sent the Compare QoR mail.")
            except Exception as e: print(f"Failed to send mail: {e}")

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
            
            cmd = [MAIL_UTIL, "-to", ",".join(all_recipients), "-sd", sender_email, "-s", subject, "-c", body_template, "-fm", "text"]
            for att in dlg.attachments: cmd.extend(["-a", att])
                
            try:
                subprocess.Popen(cmd); QMessageBox.information(self, "Mail Sent", "Successfully sent the custom mail.")
            except Exception as e: print(f"Failed to send mail: {e}")

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
            QMessageBox.information(self, "Scanning", "Disk usage is still calculating in the background.\nPlease wait a moment and try again.")
            return
            
        self.disk_dialog = DiskUsageDialog(
            self._cached_disk_data,
            self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888"),
            self)
        self.disk_dialog.exec_()

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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.showMaximized()
    sys.exit(app.exec_())