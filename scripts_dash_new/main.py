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
    QLabel, QComboBox, QLineEdit, QTreeView, QPushButton, QMessageBox, 
    QListWidget, QListWidgetItem, QProgressBar, QMenu, QSplitter, 
    QWidgetAction, QCheckBox, QStatusBar, QFrame, QShortcut, QHeaderView, 
    QFileDialog, QTextEdit, QDockWidget, QFormLayout
)
from PyQt5.QtCore import Qt, QTimer, QDateTime, QSortFilterProxyModel, QModelIndex
from PyQt5.QtGui import QColor, QFont, QKeySequence, QBrush, QPainter, QPen, QPixmap, QIcon, QStandardItemModel, QStandardItem

from config import *
from utils import *
from workers import *
from widgets import *
from dialogs import *

# =====================================================================
# HIGH-PERFORMANCE C++ PROXY MODEL FOR INSTANT FILTERING
# =====================================================================
class RunFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.src_mode = "ALL"
        self.sel_rtl = "[ SHOW ALL ]"
        self.preset = "All Runs"
        self.search_text = ""
        self.checked_blks = set()
        self.filter_config = None
        self.user_pins = {}
        self.global_notes = {}
        
        if hasattr(self, 'setRecursiveFilteringEnabled'):
            self.setRecursiveFilteringEnabled(True)

    def update_filters(self, src, rtl, preset, search, blks, config, pins, notes):
        self.src_mode = src
        self.sel_rtl = rtl
        self.preset = preset
        self.search_text = search.lower().strip()
        self.checked_blks = set(blks)
        self.filter_config = config
        self.user_pins = pins
        self.global_notes = notes
        self.invalidateFilter() 

    def filterAcceptsRow(self, source_row, source_parent):
        idx0 = self.sourceModel().index(source_row, 0, source_parent)
        node_type = self.sourceModel().data(idx0, Qt.UserRole)
        
        if not hasattr(self, 'setRecursiveFilteringEnabled'):
            for i in range(self.sourceModel().rowCount(idx0)):
                if self.filterAcceptsRow(i, idx0): return True

        if node_type == "DEFAULT":
            run_data = self.sourceModel().data(idx0, Qt.UserRole + 10)
            if not run_data: return True
            return self._check_run(run_data)
            
        elif node_type == "STAGE":
            stage_data = self.sourceModel().data(idx0, Qt.UserRole + 10)
            parent_run = self.sourceModel().data(source_parent, Qt.UserRole + 10)
            if not stage_data or not parent_run: return True
            return self._check_stage(stage_data, parent_run)
            
        return False if hasattr(self, 'setRecursiveFilteringEnabled') else True

    def _check_run(self, run):
        if self.user_pins.get(run["path"]) == "golden": return True
        if self.src_mode != "ALL" and run["source"] != self.src_mode: return False
        if run["block"] not in self.checked_blks: return False
        
        if self.filter_config is not None:
            r_src, r_rtl, r_blk = run["source"], run["rtl"], run["block"]
            if r_src in self.filter_config and r_rtl in self.filter_config[r_src] and r_blk in self.filter_config[r_src][r_rtl]:
                allowed = self.filter_config[r_src][r_rtl][r_blk]
                b_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                if b_name not in allowed and run["r_name"] not in allowed: return False
                
        if self.sel_rtl != "[ SHOW ALL ]" and not (run["rtl"] == self.sel_rtl or run["rtl"].startswith(self.sel_rtl + "_")): return False
        if self.preset == "FE Only" and run["run_type"] != "FE": return False
        if self.preset == "BE Only" and run["run_type"] != "BE": return False
        if self.preset == "Running Only" and not (run["run_type"] == "FE" and not run["is_comp"]): return False
        if self.preset == "Failed Only" and not ("FAILS" in run.get("st_n","") or "FAILS" in run.get("st_u","") or run.get("fe_status") == "FATAL ERROR"): return False
        if self.preset == "Today's Runs":
            start = run["info"].get("start","")
            rt = relative_time(start)
            if not (rt.endswith("ago") and ("h ago" in rt or "m ago" in rt)): return False
            
        if self.search_text != "*":
            note_id = f"{run['rtl']} : {run['r_name']}"
            notes = " | ".join(self.global_notes.get(note_id, []))
            combined = (f"{run['r_name']} {run['rtl']} {run['source']} {run['run_type']} "
                        f"{run['st_n']} {run['st_u']} {run['vslp_status']} "
                        f"{run['info']['runtime']} {run['info']['start']} {run['info']['end']} {notes}").lower()
            if not fnmatch.fnmatch(combined, self.search_text): return False
        return True

    def _check_stage(self, stage, run):
        if self.user_pins.get(run["path"]) == "golden": return True
        if self.src_mode != "ALL" and run["source"] != self.src_mode: return False
        if run["block"] not in self.checked_blks: return False
        if self.sel_rtl != "[ SHOW ALL ]" and not (run["rtl"] == self.sel_rtl or run["rtl"].startswith(self.sel_rtl + "_")): return False
        if self.preset == "FE Only": return False
        if self.preset == "Failed Only" and not ("FAILS" in stage.get("st_n","") or "FAILS" in stage.get("st_u","")): return False
        if self.search_text != "*":
            sc = f"{stage['name']} {stage['st_n']} {stage['st_u']} {stage['vslp_status']} {stage['info']['runtime']}".lower()
            if not fnmatch.fnmatch(sc, self.search_text): return False
        return True

# =====================================================================
# MAIN DASHBOARD CLASS
# =====================================================================
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

        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.apply_proxy_filters)

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

        # TOP BAR
        top_layout = QHBoxLayout()
        top_layout.setSpacing(8)

        top_layout.addWidget(self._label("<b>Source:</b>"))
        self.src_combo = QComboBox()
        self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.setFixedWidth(100)
        self.src_combo.currentIndexChanged.connect(self.apply_proxy_filters)
        top_layout.addWidget(self.src_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("<b>RTL Release:</b>"))
        self.rel_combo = QComboBox()
        self.rel_combo.setMinimumWidth(220)
        self.rel_combo.currentIndexChanged.connect(self.apply_proxy_filters)
        top_layout.addWidget(self.rel_combo)

        self._add_separator(top_layout)
        top_layout.addWidget(self._label("<b>View:</b>"))
        self.view_combo = QComboBox()
        self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Running Only", "Failed Only", "Today's Runs"])
        self.view_combo.setFixedWidth(120)
        self.view_combo.currentIndexChanged.connect(self.apply_proxy_filters)
        top_layout.addWidget(self.view_combo)

        self._add_separator(top_layout)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search runs, blocks, status, runtime...   [Ctrl+F]")
        self.search.setMinimumWidth(260)
        self.search.textChanged.connect(lambda: self.search_timer.start(100))
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

        # BUTTONS
        self.qor_btn = QPushButton("Compare QoR")
        self.qor_btn.setCursor(Qt.PointingHandCursor)
        self.qor_btn.clicked.connect(self.run_qor_comparison)
        top_layout.addWidget(self.qor_btn)
        
        self.settings_btn = QPushButton("Settings")
        self.settings_btn.setCursor(Qt.PointingHandCursor)
        self.settings_btn.clicked.connect(self.open_settings)
        top_layout.addWidget(self.settings_btn)

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
        
        self.notes_toggle_btn = QPushButton("Notes <")
        self.notes_toggle_btn.setCursor(Qt.PointingHandCursor)
        self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock)
        top_layout.addWidget(self.notes_toggle_btn)

        root_layout.addLayout(top_layout)

        self.prog_container = QWidget(); self.prog_container.setFixedHeight(30); self.prog_container.setVisible(False)
        self.prog_layout = QHBoxLayout(self.prog_container); self.prog_layout.setContentsMargins(4, 0, 4, 0)
        self.prog_lbl = QLabel("Initializing Scanner...")
        self.prog = QProgressBar(); self.prog.setFixedHeight(6); self.prog.setTextVisible(False)
        self.prog_layout.addWidget(self.prog_lbl); self.prog_layout.addWidget(self.prog, 1)
        root_layout.addWidget(self.prog_container)

        self.main_splitter = QSplitter(Qt.Horizontal)

        left_panel = QWidget()
        left_panel.setMaximumWidth(320)
        left_layout = QVBoxLayout(left_panel); left_layout.setContentsMargins(0, 0, 4, 0); left_layout.setSpacing(6)

        blk_header = QHBoxLayout()
        blk_header.addWidget(self._label("<b>Blocks</b>")); blk_header.addStretch()
        all_btn = QPushButton("All"); all_btn.setObjectName("linkBtn"); all_btn.clicked.connect(lambda: self._set_all_blocks(True))
        none_btn = QPushButton("None"); none_btn.setObjectName("linkBtn"); none_btn.clicked.connect(lambda: self._set_all_blocks(False))
        blk_header.addWidget(all_btn); blk_header.addWidget(self._label("|")); blk_header.addWidget(none_btn)
        left_layout.addLayout(blk_header)

        self.blk_list = QListWidget()
        self.blk_list.itemChanged.connect(lambda: self.search_timer.start(50))
        left_layout.addWidget(self.blk_list, 1)

        self.fe_error_btn = QPushButton("")
        self.fe_error_btn.setObjectName("errorLinkBtn"); self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log)
        left_layout.addWidget(self.fe_error_btn)
        
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel); meta_layout.setContentsMargins(0, 8, 0, 0)
        meta_layout.addWidget(QLabel("<b>Quick Info (Select a Run):</b>"))
        form = QFormLayout(); form.setContentsMargins(0, 0, 0, 0)
        self.meta_status = QLineEdit(); self.meta_status.setReadOnly(True)
        self.meta_path = QLineEdit(); self.meta_path.setReadOnly(True)
        self.meta_log = QLineEdit(); self.meta_log.setReadOnly(True)
        form.addRow("Status:", self.meta_status); form.addRow("Path:", self.meta_path); form.addRow("Log:", self.meta_log)
        meta_layout.addLayout(form)
        left_layout.addWidget(self.meta_panel, 0)

        self.main_splitter.addWidget(left_panel)

        # -------------------------------------------------------
        # QTREEVIEW & DATA MODELS
        # -------------------------------------------------------
        self.tree = QTreeView()
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)
        self.tree.setAnimated(False)
        self.tree.setExpandsOnDoubleClick(False)
        self.tree.setSortingEnabled(True)

        self.model = QStandardItemModel(0, 24)
        headers = [
            "Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "User", "Size",
            "FM - NONUPF", "FM - UPF", "VSLP Status", "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes", "Starred"
        ]
        self.model.setHorizontalHeaderLabels(headers)

        self.proxy = RunFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.tree.setModel(self.proxy)

        self.tree.setColumnWidth(0, 380); self.tree.setColumnWidth(1, 260)
        self.tree.setColumnWidth(2, 90);  self.tree.setColumnWidth(3, 110)
        self.tree.setColumnWidth(4, 130); self.tree.setColumnWidth(5, 100)
        self.tree.setColumnWidth(6, 80);  self.tree.setColumnWidth(7, 160)
        self.tree.setColumnWidth(8, 160); self.tree.setColumnWidth(9, 200)
        self.tree.setColumnWidth(10, 100); self.tree.setColumnWidth(11, 100)
        self.tree.setColumnWidth(12, 110); self.tree.setColumnWidth(13, 120)
        self.tree.setColumnWidth(14, 120); self.tree.setColumnWidth(22, 300)

        for i in [15, 16, 17, 18, 19, 20, 21, 23]: self.tree.setColumnHidden(i, True)

        self.tree.selectionModel().selectionChanged.connect(self.on_tree_selection_changed)
        self.tree.expanded.connect(self.on_item_expanded)
        self.tree.doubleClicked.connect(self.on_item_double_clicked)
        self.model.itemChanged.connect(self._on_item_check_changed)

        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)

        self.main_splitter.addWidget(self.tree)
        root_layout.addWidget(self.main_splitter)

        # RIGHT PANEL (Inspector Dock)
        self.inspector = QWidget()
        ins_layout = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view details."); self.ins_lbl.setWordWrap(True)
        self.ins_history = QTextEdit(); self.ins_history.setReadOnly(True)
        self.ins_note = QTextEdit(); self.ins_note.setPlaceholderText("Enter your new note here..."); self.ins_note.setMaximumHeight(100)
        self.ins_save_btn = QPushButton("Save / Update Note")
        self.ins_save_btn.clicked.connect(self.save_inspector_note)
        
        ins_layout.addWidget(self.ins_lbl)
        ins_layout.addWidget(QLabel("<b>Shared Notes History:</b>")); ins_layout.addWidget(self.ins_history, 1)
        ins_layout.addWidget(QLabel("<b>Your Contribution:</b>")); ins_layout.addWidget(self.ins_note, 0)
        ins_layout.addWidget(self.ins_save_btn)
        
        self.inspector_dock = QDockWidget(self); self.inspector_dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.inspector_dock.setTitleBarWidget(QWidget()); self.inspector_dock.setWidget(self.inspector)
        self.addDockWidget(Qt.RightDockWidgetArea, self.inspector_dock); self.inspector_dock.hide()

        try:
            m_sizes = [int(x) for x in prefs.get('UI', 'main_splitter', fallback='250,1200').split(',')]
            self.main_splitter.setSizes(m_sizes)
        except: pass

        self.status_bar = QStatusBar(); self.status_bar.setFixedHeight(26); self.setStatusBar(self.status_bar)
        self.sb_total = QLabel("Total: 0"); self.sb_complete = QLabel("Completed: 0"); self.sb_running = QLabel("Running: 0")
        self.sb_selected = QLabel("Selected: 0"); self.sb_scan_time = QLabel(""); self.sb_config = QLabel("Config: None")
        for lbl in [self.sb_total, self.sb_complete, self.sb_running, self.sb_selected, self.sb_scan_time, self.sb_config]:
            lbl.setContentsMargins(8, 0, 8, 0); self.status_bar.addPermanentWidget(lbl); self.status_bar.addPermanentWidget(self._vsep())

        self.apply_theme_and_spacing()

    def toggle_notes_dock(self):
        if self.inspector_dock.isVisible(): self.inspector_dock.hide(); self.notes_toggle_btn.setText("Notes <")
        else: self.inspector_dock.show(); self.notes_toggle_btn.setText("Notes v")

    def safe_expand_all(self): self.tree.expandAll(); self.tree.resizeColumnToContents(0)
    def safe_collapse_all(self): self.tree.collapseAll(); self.tree.resizeColumnToContents(0)
    def on_item_expanded(self, index): QTimer.singleShot(10, self._resize_first_col)
    def _resize_first_col(self):
        self.tree.resizeColumnToContents(0)
        if self.tree.columnWidth(0) > 450: self.tree.setColumnWidth(0, 450)

    # ------------------------------------------------------------------
    # DATA & SCANNING
    # ------------------------------------------------------------------
    def on_auto_refresh_changed(self):
        val = self.auto_combo.currentText()
        if val == "Off": self.auto_refresh_timer.stop()
        elif val == "1 Min": self.auto_refresh_timer.start(60_000)
        elif val == "5 Min": self.auto_refresh_timer.start(300_000)

    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning(): return
        self.prog_container.setVisible(True); self.prog.setRange(0, 0)
        self.prog_lbl.setText("Scanning Workspaces...")
        self.refresh_btn.setEnabled(False); self.refresh_btn.setText("Scanning...")
        self.model.removeRows(0, self.model.rowCount()) 
        
        self.worker = ScannerWorker()
        self.worker.progress_update.connect(lambda c, t: (self.prog.setRange(0, t), self.prog.setValue(c)))
        self.worker.status_update.connect(self.prog_lbl.setText)
        self.worker.finished.connect(self.on_scan_finished)
        self.worker.start()

    def on_scan_finished(self, ws, out, ir, stats):
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.prog_container.setVisible(False)
        self.refresh_btn.setEnabled(True); self.refresh_btn.setText("Refresh")
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.global_notes = load_all_notes()
        
        self.update_combos()
        self.build_model()
        self.apply_proxy_filters() 
        
        if not self._columns_fitted_once:
            self._columns_fitted_once = True; self.fit_all_columns()
            
        if not self._initial_size_calc_done:
            self._initial_size_calc_done = True
            self.calculate_all_sizes()

        all_owners = set()
        for r in self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", []):
            if r.get("owner") and r["owner"] != "Unknown": all_owners.add(r["owner"])
        if all_owners: save_mail_users_config(all_owners)
        ScanSummaryDialog(stats, self).exec_()

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
            it = QListWidgetItem(b); it.setData(Qt.UserRole, b)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
            it.setCheckState(saved_states.get(b, Qt.Checked))
            self.blk_list.addItem(it)
        self.blk_list.blockSignals(False)

    # ------------------------------------------------------------------
    # CORE MODEL BUILDING
    # ------------------------------------------------------------------
    def build_model(self):
        self.tree.setUpdatesEnabled(False)
        self.model.removeRows(0, self.model.rowCount())
        
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
                        
        root = self.model.invisibleRootItem()
        ign_root_items = self._get_model_node(root, "[ Ignored Runs ]", "IGNORED_ROOT")
        ign_root = ign_root_items[0]
        
        fe_runs = [r for r in runs_to_process if r["run_type"] == "FE"]
        be_runs = [r for r in runs_to_process if r["run_type"] == "BE"]
        
        for run in fe_runs:
            target_root = ign_root if run["path"] in self.ignored_paths else root
            base_attach_node = target_root if self.hide_block_nodes else self._get_model_node(target_root, run["block"], "BLOCK")[0]
            base_rtl = re.sub(r'_syn\d+$', '', run["rtl"])
            m_node = self._get_model_node(base_attach_node, get_milestone(base_rtl) or "UNKNOWN", "MILESTONE")[0]
            parent_node = self._get_model_node(m_node, base_rtl, "RTL")[0]
            self._create_model_run(parent_node, run)
            
        for run in be_runs:
            target_root = ign_root if run["path"] in self.ignored_paths else root
            base_attach_node = target_root if self.hide_block_nodes else self._get_model_node(target_root, run["block"], "BLOCK")[0]
            base_rtl = re.sub(r'_syn\d+$', '', run["rtl"])
            m_node = self._get_model_node(base_attach_node, get_milestone(base_rtl) or "UNKNOWN", "MILESTONE")[0]
            parent_node = self._get_model_node(m_node, base_rtl, "RTL")[0]
            
            fe_parent = None
            for i in range(parent_node.rowCount()):
                c = parent_node.child(i, 0)
                if c.data(Qt.UserRole) != "STAGE":
                    fe_base = c.text().replace("-FE", "")
                    clean_be = run["r_name"].replace("-BE", "")
                    if c.data(Qt.UserRole + 12) == run["source"] and c.data(Qt.UserRole + 2) == run["block"] and (clean_be == fe_base or f"_{fe_base}_" in clean_be or clean_be.startswith(f"{fe_base}_") or clean_be.endswith(f"_{fe_base}")):
                        fe_parent = c; break
                        
            actual_parent = fe_parent if fe_parent else parent_node
            be_items = self._create_model_run(actual_parent, run)
            self._add_model_stages(be_items[0], run, ign_root)
            
        if ign_root.rowCount() == 0: root.removeRow(ign_root.row())
        
        self.tree.setUpdatesEnabled(True)

    def _get_model_node(self, parent, text, node_type="DEFAULT"):
        for i in range(parent.rowCount()):
            if parent.child(i, 0).text() == text: 
                return [parent.child(i, col) for col in range(24)]
                
        row_items = [QStandardItem() for _ in range(24)]
        row_items[0].setText(text)
        row_items[0].setData(node_type, Qt.UserRole)
        
        if node_type == "MILESTONE":
            row_items[0].setForeground(QBrush(QColor("#1e88e5" if not self.is_dark_mode else "#64b5f6")))
            f = row_items[0].font(); f.setBold(True); row_items[0].setFont(f)
        elif node_type == "RTL":
            f = row_items[0].font(); f.setItalic(True); row_items[0].setFont(f)
            if text in self.global_notes:
                notes = " | ".join(self.global_notes[text])
                row_items[22].setText(notes); row_items[22].setToolTip(notes)
                row_items[22].setForeground(QBrush(QColor("#e65100" if not self.is_dark_mode else "#ffb74d")))
                
        parent.appendRow(row_items)
        return row_items

    def _create_model_run(self, parent_node, run):
        row = [QStandardItem() for _ in range(24)]
        row[0].setCheckable(True); row[0].setCheckState(Qt.Unchecked)
        
        row[0].setText(run["r_name"]); row[1].setText(run["rtl"]); row[2].setText(run["source"])
        row[5].setText(run.get("owner", "Unknown"))
        
        row[0].setData("DEFAULT", Qt.UserRole)
        row[0].setData(run["block"], Qt.UserRole + 2)
        row[0].setData(run["source"], Qt.UserRole + 12)
        row[0].setData(run, Qt.UserRole + 10) 
        
        pin_type = self.user_pins.get(run["path"])
        if pin_type in self.icons: row[0].setIcon(self.icons[pin_type])

        if run["path"] in self._checked_paths: row[0].setCheckState(Qt.Checked)

        if run["source"] == "OUTFEED": row[2].setForeground(QBrush(QColor("#8e24aa" if not self.is_dark_mode else "#ce93d8")))
        else: row[2].setForeground(QBrush(QColor("#e65100" if not self.is_dark_mode else "#ffb74d")))

        if run["run_type"] == "FE":
            row[3].setText(run["fe_status"])
            row[4].setText("COMPLETED" if run["is_comp"] else run["info"]["last_stage"])
            row[6].setText("-"); row[10].setText("-"); row[11].setText("-")
            row[7].setText(f"NONUPF - {run['st_n']}"); row[8].setText(f"UPF - {run['st_u']}")
            row[9].setText(run["vslp_status"]); row[12].setText(run["info"]["runtime"])
            
            s, e = run["info"]["start"], run["info"]["end"]
            row[13].setText(s); row[14].setText(e)
            row[15].setText(run["path"]); row[16].setText(os.path.join(run["path"], "logs/compile_opt.log"))
            row[17].setText(run["fm_u_path"]); row[18].setText(run["fm_n_path"])
            row[19].setText(run["vslp_rpt_path"])
            
            self._apply_status_color(row[3], run["fe_status"])
            self._apply_fm_color(row[7], run["st_n"]); self._apply_fm_color(row[8], run["st_u"])
            self._apply_vslp_color(row[9], run["vslp_status"])
        else:
            row[6].setText("-"); row[10].setText("-"); row[11].setText("-")
            row[15].setText(run["path"])

        run_id = f"{run['rtl']} : {run['r_name']}"
        if run_id in self.global_notes:
            row[22].setText(" | ".join(self.global_notes[run_id]))
            row[22].setForeground(QBrush(QColor("#e65100" if not self.is_dark_mode else "#ffb74d")))

        for i in range(1, 23): row[i].setTextAlignment(Qt.AlignCenter)
        parent_node.appendRow(row)
        return row

    def _add_model_stages(self, parent_run_item, be_run, ign_root):
        is_ir = (be_run["block"].upper() == PROJECT_PREFIX.upper())
        for stage in be_run["stages"]:
            target_parent = ign_root if stage["stage_path"] in self.ignored_paths else parent_run_item
            row = [QStandardItem() for _ in range(24)]
            row[0].setCheckable(True)
            
            row[0].setData("STAGE", Qt.UserRole)
            row[0].setData(stage["name"], Qt.UserRole + 1)
            row[0].setData(stage.get("qor_path", ""), Qt.UserRole + 2)
            row[0].setData(stage, Qt.UserRole + 10)
            
            ir_info = self.ir_data.get(f"{be_run['r_name']}/{stage['name']}", {"static": "-", "dynamic": "-"})
            row[0].setText(stage["name"]); row[2].setText(be_run["source"])
            row[4].setText("COMPLETED"); row[5].setText(be_run.get("owner", "Unknown"))
            row[6].setText("-"); row[7].setText(f"NONUPF - {stage['st_n']}"); row[8].setText(f"UPF - {stage['st_u']}")
            row[9].setText(stage["vslp_status"])
            row[10].setText(ir_info["static"] if is_ir else "-")
            row[11].setText(ir_info["dynamic"] if is_ir else "-")
            row[12].setText(stage["info"]["runtime"])
            row[13].setText(stage["info"]["start"]); row[14].setText(stage["info"]["end"])
            row[15].setText(stage["stage_path"]); row[16].setText(stage["log"])
            row[17].setText(stage["fm_u_path"]); row[18].setText(stage["fm_n_path"])
            row[19].setText(stage["vslp_rpt_path"]); row[20].setText(stage["sta_rpt_path"])
            row[21].setText(ir_info.get("log", ""))
            
            self._apply_fm_color(row[7], stage["st_n"]); self._apply_fm_color(row[8], stage["st_u"])
            self._apply_vslp_color(row[9], stage["vslp_status"])
            self._apply_status_color(row[4], "COMPLETED")
            
            for i in range(1, 23): row[i].setTextAlignment(Qt.AlignCenter)
            target_parent.appendRow(row)

    def _apply_status_color(self, item, status):
        c = "#b71c1c"
        if status == "COMPLETED":   c = "#1b5e20" if not self.is_dark_mode else "#81c784"
        elif status == "RUNNING":   c = "#0d47a1" if not self.is_dark_mode else "#64b5f6"
        elif status == "NOT STARTED": c = "#757575" if not self.is_dark_mode else "#9e9e9e"
        item.setForeground(QBrush(QColor(c)))

    def _apply_fm_color(self, item, val):
        if "FAILS" in val: item.setForeground(QBrush(QColor("#d32f2f" if not self.is_dark_mode else "#e57373")))
        elif "PASS" in val: item.setForeground(QBrush(QColor("#388e3c" if not self.is_dark_mode else "#81c784")))

    def _apply_vslp_color(self, item, val):
        if "Error: 0" in val: item.setForeground(QBrush(QColor("#388e3c" if not self.is_dark_mode else "#81c784")))
        elif "Error" in val: item.setForeground(QBrush(QColor("#d32f2f" if not self.is_dark_mode else "#e57373")))

    # ------------------------------------------------------------------
    # PROXY FILTERING DELEGATE
    # ------------------------------------------------------------------
    def apply_proxy_filters(self):
        blks = [self.blk_list.item(i).data(Qt.UserRole) for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]
        self.proxy.update_filters(
            self.src_combo.currentText(), self.rel_combo.currentText(), self.view_combo.currentText(),
            self.search.text(), blks, self.run_filter_config, self.user_pins, self.global_notes
        )
        if self.search.text() != "" or self.view_combo.currentText() != "All Runs": self.tree.expandAll()
        else: self.tree.collapseAll()
        self._update_status_bar()

    def _update_status_bar(self):
        total = completed = running = 0
        def count_runs(parent_idx):
            nonlocal total, completed, running
            for i in range(self.proxy.rowCount(parent_idx)):
                idx = self.proxy.index(i, 0, parent_idx)
                if self.proxy.data(idx, Qt.UserRole) == "DEFAULT":
                    run_data = self.proxy.data(idx, Qt.UserRole + 10)
                    if run_data and run_data.get("run_type") == "FE":
                        total += 1
                        if run_data.get("is_comp"): completed += 1
                        elif run_data.get("fe_status") == "RUNNING": running += 1
                if self.proxy.hasChildren(idx): count_runs(idx)
        count_runs(QModelIndex())
        
        self.sb_total.setText(f"  Total: {total}")
        self.sb_complete.setText(f"  Completed: {completed}")
        self.sb_running.setText(f"  Running: {running}")
        self.sb_selected.setText(f"  Selected: {len(self._checked_paths)}")
        if self._last_scan_time: self.sb_scan_time.setText(f"  Last scan: {self._last_scan_time}  ")

    # ------------------------------------------------------------------
    # EVENTS & INTERACTION
    # ------------------------------------------------------------------
    def _on_item_check_changed(self, item):
        if item.column() != 0: return
        path = self.model.itemFromIndex(item.index().siblingAtColumn(15)).text()
        if not path or path == "N/A": return
        
        c = QColor(self.custom_sel_color if self.use_custom_colors else ("#404652" if self.is_dark_mode else "#e3f2fd"))
        if item.checkState() == Qt.Checked:
            self._checked_paths.add(path)
            for i in range(24): self.model.itemFromIndex(item.index().siblingAtColumn(i)).setBackground(QBrush(c))
        else:
            self._checked_paths.discard(path)
            for i in range(24): self.model.itemFromIndex(item.index().siblingAtColumn(i)).setBackground(QBrush(Qt.transparent))
        self.sb_selected.setText(f"  Selected: {len(self._checked_paths)}")

    def on_tree_selection_changed(self):
        indexes = self.tree.selectionModel().selectedRows()
        self.fe_error_btn.setVisible(False)
        self.current_error_log_path = None
        
        if not indexes:
            self.ins_lbl.setText("Select a run to view details.")
            self.ins_history.clear(); self.ins_note.clear(); self.ins_note.setEnabled(False); self.ins_save_btn.setEnabled(False)
            self.meta_status.clear(); self.meta_path.clear(); self.meta_log.clear()
            return

        idx = self.proxy.mapToSource(indexes[0])
        item0 = self.model.itemFromIndex(idx.siblingAtColumn(0))
        run_name = item0.text()
        rtl = self.model.itemFromIndex(idx.siblingAtColumn(1)).text()
        node_type = item0.data(Qt.UserRole)
        
        self.meta_status.setText(self.model.itemFromIndex(idx.siblingAtColumn(3)).text())
        self.meta_path.setText(self.model.itemFromIndex(idx.siblingAtColumn(15)).text())
        self.meta_log.setText(self.model.itemFromIndex(idx.siblingAtColumn(16)).text())
        self.meta_path.setCursorPosition(0); self.meta_log.setCursorPosition(0)
        
        self.ins_note.setEnabled(True); self.ins_save_btn.setEnabled(True)

        if node_type == "STAGE":
            p_name = item0.parent().text()
            self.ins_lbl.setText(f"<b>Stage:</b> {run_name}<br><b>Parent:</b> {p_name}")
            self._current_note_id = f"{item0.parent().parent().text()} : {p_name}"
        elif node_type == "RTL":
            self.ins_lbl.setText(f"<b>RTL Release:</b> {run_name}")
            self._current_note_id = run_name
        else:
            self.ins_lbl.setText(f"<b>Run:</b> {run_name}<br><b>RTL:</b> {rtl}")
            self._current_note_id = f"{rtl} : {run_name}"

        notes = self.global_notes.get(self._current_note_id, [])
        self.ins_history.setPlainText("\n\n".join(notes) if notes else "No notes have been saved yet.")
        my_note = ""
        tag_prefix = f"[{getpass.getuser()} -"
        for n in notes:
            if n.startswith(tag_prefix):
                parts = n.split("] ", 1)
                if len(parts) == 2: my_note = parts[1]
                break
        self.ins_note.setPlainText(my_note)
        
        if len(indexes) == 1 and node_type != "STAGE":
            run_path = self.model.itemFromIndex(idx.siblingAtColumn(15)).text()
            if run_path and run_path != "N/A":
                err_file = os.path.join(run_path, "logs", "compile_opt.error.log")
                if os.path.exists(err_file):
                    count = sum(1 for line in open(err_file, 'r', encoding='utf-8', errors='ignore') if line.strip())
                    self.current_error_log_path = err_file
                    color = ("#e57373" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")) else "#d32f2f")
                    if count == 0: color = ("#81c784" if (self.is_dark_mode or (self.use_custom_colors and self.custom_bg_color < "#888888")) else "#388e3c")
                    self.fe_error_btn.setStyleSheet(
                        f"QPushButton#errorLinkBtn {{ border: none; background: transparent; color: {color}; "
                        f"font-weight: bold; text-align: left; padding: 6px 0px; }} "
                        f"QPushButton#errorLinkBtn:hover {{ text-decoration: underline; }}")
                    self.fe_error_btn.setText(f"compile_opt errors: {count}")
                    self.fe_error_btn.setVisible(True)

    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'): return
        save_user_note(self._current_note_id, self.ins_note.toPlainText())
        self.global_notes = load_all_notes()
        self.on_tree_selection_changed()
        self.build_model() 

    def on_item_double_clicked(self, index):
        src_idx = self.proxy.mapToSource(index)
        log = self.model.itemFromIndex(src_idx.siblingAtColumn(16)).text()
        if log and cached_exists(log): subprocess.Popen(['gvim', log])
        
    def open_error_log(self):
        if self.current_error_log_path and os.path.exists(self.current_error_log_path):
            subprocess.Popen(['gvim', self.current_error_log_path])

    # ------------------------------------------------------------------
    # UTILITIES AND ACTIONS
    # ------------------------------------------------------------------
    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        for i in range(self.model.columnCount()):
            if not self.tree.isColumnHidden(i): self.tree.resizeColumnToContents(i)
        self.tree.setUpdatesEnabled(True)

    def _label(self, text): return QLabel(text)
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
        self.apply_proxy_filters()

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self, lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"), self, self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"), self, self.safe_collapse_all)
        QShortcut(QKeySequence("Ctrl+C"), self.tree, self._copy_tree_cell)

    def _copy_tree_cell(self):
        idx = self.tree.currentIndex()
        if idx.isValid():
            text = str(idx.data(Qt.DisplayRole)).strip()
            if text: QApplication.clipboard().setText(text)

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export to CSV", "dashboard_export.csv", "CSV Files (*.csv)")
        if not path: return
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                headers = [self.model.horizontalHeaderItem(i).text() for i in range(15)] + ["Alias / Notes"]
                writer.writerow(headers)
                
                def export_node(proxy_index):
                    if proxy_index.isValid():
                        src_idx = self.proxy.mapToSource(proxy_index)
                        node_text = self.model.itemFromIndex(src_idx.siblingAtColumn(0)).text()
                        if "[ Ignored" not in node_text:
                            row_data = [self.model.itemFromIndex(src_idx.siblingAtColumn(i)).text() for i in range(15)]
                            row_data.append(self.model.itemFromIndex(src_idx.siblingAtColumn(22)).text())
                            writer.writerow(row_data)
                    for r in range(self.proxy.rowCount(proxy_index)):
                        export_node(self.proxy.index(r, 0, proxy_index))
                
                export_node(QModelIndex())
            QMessageBox.information(self, "Export Successful", f"Data exported to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to export: {e}")

    def generate_sample_config(self):
        sample_text = """# PD Dashboard Run Filter Configuration
# Format: SOURCE : RTL_NAME : BLOCK_NAME : run1 run2 run3 ...
OUTFEED : EVT0_ML4_DEV00_syn2 : BLK_CPU : my_test_run fast_route_run
WS : EVT0_ML4_DEV00 : BLK_GPU : golden_run
"""
        path, _ = QFileDialog.getSaveFileName(self, "Save Sample Config", "dashboard_filter.cfg", "Config Files (*.cfg *.txt)")
        if path:
            with open(path, 'w') as f: f.write(sample_text)
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
            self.apply_proxy_filters()
            QMessageBox.information(self, "Config Loaded", "Filter configuration applied successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to parse config file:\n{e}")

    def clear_filter_config(self):
        if self.run_filter_config is not None:
            self.run_filter_config = None
            self.current_config_path = None
            self.sb_config.setText("Config: None")
            self.sb_config.setStyleSheet("")
            self.apply_proxy_filters()

    # ------------------------------------------------------------------
    # FULL CONTEXT MENU RESTORED (Open Logs, QOR, Gantt)
    # ------------------------------------------------------------------
    def on_context_menu(self, pos):
        idx = self.tree.indexAt(pos)
        if not idx.isValid(): return
        src_idx = self.proxy.mapToSource(idx)
        item0 = self.model.itemFromIndex(src_idx.siblingAtColumn(0))
        
        m = QMenu()
        run_path = self.model.itemFromIndex(src_idx.siblingAtColumn(15)).text()
        log_path = self.model.itemFromIndex(src_idx.siblingAtColumn(16)).text()
        fm_u_path = self.model.itemFromIndex(src_idx.siblingAtColumn(17)).text()
        fm_n_path = self.model.itemFromIndex(src_idx.siblingAtColumn(18)).text()
        vslp_path = self.model.itemFromIndex(src_idx.siblingAtColumn(19)).text()
        sta_path  = self.model.itemFromIndex(src_idx.siblingAtColumn(20)).text()
        ir_path   = self.model.itemFromIndex(src_idx.siblingAtColumn(21)).text()
        
        node_type = item0.data(Qt.UserRole)
        is_stage  = node_type == "STAGE"
        
        act_gold = act_good = act_red = act_later = act_clear = None
        gantt_act = None
        
        if run_path and run_path != "N/A" and node_type in ["DEFAULT", "STAGE"]:
            pin_menu = m.addMenu("Pin as...")
            act_gold = pin_menu.addAction(self.icons['golden'], "Golden Run")
            act_good = pin_menu.addAction(self.icons['good'], "Good Run")
            act_red = pin_menu.addAction(self.icons['redundant'], "Redundant Run")
            act_later = pin_menu.addAction(self.icons['later'], "Mark for Later")
            pin_menu.addSeparator()
            act_clear = pin_menu.addAction("Clear Pin")
            m.addSeparator()
            
            if item0.hasChildren() and item0.child(0, 0).data(Qt.UserRole) == "STAGE":
                gantt_act = m.addAction("Show Timeline (Gantt Chart)")
                m.addSeparator()
                
        ignore_checked_act = m.addAction("Hide All Checked Runs/Stages")
        m.addSeparator()

        ignore_act = restore_act = None
        if run_path and run_path != "N/A":
            if run_path in self.ignored_paths: restore_act = m.addAction("Restore (Unhide)")
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
            if res == act_gold: self.user_pins[run_path] = 'golden'
            elif res == act_good: self.user_pins[run_path] = 'good'
            elif res == act_red: self.user_pins[run_path] = 'redundant'
            elif res == act_later: self.user_pins[run_path] = 'later'
            elif res == act_clear: self.user_pins.pop(run_path, None)
            save_user_pins(self.user_pins); self.build_model()
            
        elif gantt_act and res == gantt_act:
            stages = []
            for i in range(item0.rowCount()):
                c = item0.child(i, 0)
                if c.data(Qt.UserRole) == "STAGE":
                    rt = item0.child(i, 12).text()
                    stages.append({'name': c.text(), 'time_str': rt, 'sec': self._time_to_seconds(rt)})
            dlg = GanttChartDialog(item0.text(), stages, self); dlg.exec_()
            
        elif res == ignore_checked_act:
            for r in self._checked_paths: self.ignored_paths.add(r)
            self.build_model()
            
        elif res == ignore_act:  self.ignored_paths.add(run_path); self.build_model()
        elif res == restore_act: self.ignored_paths.discard(run_path); self.build_model()
        
        elif calc_size_act and res == calc_size_act:
            item6 = self.model.itemFromIndex(src_idx.siblingAtColumn(6))
            item6.setText("Calc...")
            worker = SingleSizeWorker(item6, run_path)
            worker.result.connect(lambda it, sz: it.setText(sz))
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
            step_name = item0.data(Qt.UserRole + 1); qor_path  = item0.data(Qt.UserRole + 2)
            subprocess.run(["python3.6", SUMMARY_SCRIPT, qor_path, "-stage", step_name])
            h = find_latest_qor_report()
            if h: subprocess.Popen([FIREFOX_PATH, h])

    def on_header_context_menu(self, pos):
        col = self.tree.header().logicalIndexAt(pos)
        m = QMenu()
        m.addAction("Sort A to Z").triggered.connect(lambda: self.tree.sortByColumn(col, Qt.AscendingOrder))
        m.addAction("Sort Z to A").triggered.connect(lambda: self.tree.sortByColumn(col, Qt.DescendingOrder))
        m.addSeparator()
        
        vis_menu = m.addMenu("Show / Hide Columns")
        for i in range(1, 24):
            action = QWidgetAction(vis_menu)
            cb = QCheckBox(self.model.horizontalHeaderItem(i).text())
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.setStyleSheet("margin: 2px 8px; background: transparent; color: inherit;")
            cb.toggled.connect(lambda checked, c=i: self.tree.setColumnHidden(c, not checked))
            action.setDefaultWidget(cb)
            vis_menu.addAction(action)
            
        m.exec_(self.tree.header().mapToGlobal(pos))

    # ------------------------------------------------------------------
    # EMAILS & DISK USAGE & QOR 
    # ------------------------------------------------------------------
    def run_qor_comparison(self):
        sel = list(self._checked_paths)
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = find_latest_qor_report()
        if h: subprocess.Popen([FIREFOX_PATH, h])

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

    def send_cleanup_mail_action(self):
        user_runs = {}; is_fe_selected = False
        def find_owners(parent_item):
            nonlocal is_fe_selected
            for i in range(parent_item.rowCount()):
                c = parent_item.child(i, 0)
                if c and c.checkState() == Qt.Checked and c.data(Qt.UserRole) != "STAGE":
                    path = parent_item.child(i, 15).text()
                    owner = parent_item.child(i, 5).text()
                    if "FE" in c.text(): is_fe_selected = True
                    if path and path != "N/A" and owner and owner != "Unknown":
                        if owner not in user_runs: user_runs[owner] = []
                        user_runs[owner].append(path)
                if c and c.hasChildren(): find_owners(c)
        find_owners(self.model.invisibleRootItem())

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
            try: subprocess.Popen(cmd)
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
            try: subprocess.Popen(cmd)
            except Exception as e: print(f"Failed to send mail: {e}")

    # ------------------------------------------------------------------
    # SETTINGS & THEME
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
                QTreeView {{ background-color: {bg}; color: {fg}; alternate-background-color: transparent; gridline-color: {fg}; border: 1px solid {fg}; }}
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
                QTreeView {{ background-color: #1e1f22; color: #dfe1e5; alternate-background-color: #26282b; gridline-color: #393b40; border: 1px solid #393b40; }}
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
                {cb_style}"""
        else:
            stylesheet = f"""
                QMainWindow, QWidget, QDialog {{ background-color: #f5f7fa; color: #333333; }}
                QHeaderView::section {{ background-color: #e4e7eb; color: #4a5568; border: 1px solid #cbd5e0; padding: 6px; font-weight: bold; }}
                QTreeView {{ background-color: #ffffff; color: #333333; alternate-background-color: #f8fafc; gridline-color: #e2e8f0; border: 1px solid #cbd5e0; }}
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
                {cb_style}"""

        self.setStyleSheet(stylesheet)
        self.build_model() 

    def calculate_all_sizes(self):
        size_tasks = []
        def gather(parent_index):
            for i in range(self.model.rowCount(parent_index)):
                idx = self.model.index(i, 0, parent_index)
                path = self.model.data(idx.siblingAtColumn(15))
                if path and path != "N/A":
                    item6 = self.model.itemFromIndex(idx.siblingAtColumn(6))
                    if item6.text() in ["-", "N/A", "Calc..."]:
                        size_tasks.append((str(id(item6)), path))
                        self.item_map[str(id(item6))] = item6
                        item6.setText("Calc...")
                gather(idx)
        gather(QModelIndex())
        
        if size_tasks:
            worker = BatchSizeWorker(size_tasks)
            worker.size_calculated.connect(self.update_item_size)
            self.size_workers.append(worker)
            worker.finished.connect(lambda w=worker: self.size_workers.remove(w) if w in self.size_workers else None)
            worker.start()

    def update_item_size(self, item_id, size_str):
        item = self.item_map.get(item_id)
        if item: item.setText(size_str)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.showMaximized()
    sys.exit(app.exec_())
