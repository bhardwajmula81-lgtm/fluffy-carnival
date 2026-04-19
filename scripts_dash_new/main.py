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
        
        # Enables recursive filtering (Qt 5.10+). If a child matches, parents are kept.
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
        self.invalidateFilter() # Instantly triggers C++ re-filter

    def filterAcceptsRow(self, source_row, source_parent):
        idx0 = self.sourceModel().index(source_row, 0, source_parent)
        node_type = self.sourceModel().data(idx0, Qt.UserRole)
        
        # Always evaluate children if recursive filtering isn't supported natively
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
            
        # For Group nodes (BLOCK, RTL, etc.), show them if they contain matching children
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
        self.search.textChanged.connect(lambda: self.search_timer.start(100)) # Ultra low latency typing
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

        # MODULAR BUTTONS
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
        self.actions_menu.addAction("Export to CSV", self.export_csv)
        self.actions_menu.addSeparator()
        
        mail_menu = self.actions_menu.addMenu("Send Mail...")
        mail_menu.addAction("Cleanup Mail (Selected Runs)", self.send_cleanup_mail_action)
        mail_menu.addAction("Send Compare QoR Mail", self.send_qor_mail_action)
        mail_menu.addAction("Send Custom Mail", self.send_custom_mail_action)
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
        self.model.removeRows(0, self.model.rowCount()) # Clear model instantly
        
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
        self.build_model() # Construct QStandardItemModel ONCE
        self.apply_proxy_filters() # Trigger lightning fast C++ view
        
        if not self._columns_fitted_once:
            self._columns_fitted_once = True; self.fit_all_columns()
            
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
    # CORE MODEL BUILDING (The secret to QTreeView speed)
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
        row[0].setData(run, Qt.UserRole + 10) # Inject full dict for Proxy to read instantly
        
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
        
        # Determine Expansion state dynamically post-filter
        if self.search.text() != "" or self.view_combo.currentText() != "All Runs": self.tree.expandAll()
        else: self.tree.collapseAll()

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
        if not indexes:
            self.ins_lbl.setText("Select a run to view details.")
            self.ins_history.clear(); self.ins_note.clear(); self.ins_note.setEnabled(False); self.ins_save_btn.setEnabled(False)
            return

        idx = self.proxy.mapToSource(indexes[0])
        item0 = self.model.itemFromIndex(idx.siblingAtColumn(0))
        run_name = item0.text()
        rtl = self.model.itemFromIndex(idx.siblingAtColumn(1)).text()
        node_type = item0.data(Qt.UserRole)
        
        self.meta_status.setText(self.model.itemFromIndex(idx.siblingAtColumn(3)).text())
        self.meta_path.setText(self.model.itemFromIndex(idx.siblingAtColumn(15)).text())
        self.meta_log.setText(self.model.itemFromIndex(idx.siblingAtColumn(16)).text())
        
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

    def save_inspector_note(self):
        if not hasattr(self, '_current_note_id'): return
        save_user_note(self._current_note_id, self.ins_note.toPlainText())
        self.global_notes = load_all_notes()
        self.on_tree_selection_changed()
        self.build_model() # Refresh model to show note in column 22

    def on_item_double_clicked(self, index):
        src_idx = self.proxy.mapToSource(index)
        log = self.model.itemFromIndex(src_idx.siblingAtColumn(16)).text()
        if log and cached_exists(log): subprocess.Popen(['gvim', log])

    # ------------------------------------------------------------------
    # REST OF UTILITIES (Same functionality, adapted for QTreeView)
    # ------------------------------------------------------------------
    def on_context_menu(self, pos):
        idx = self.tree.indexAt(pos)
        if not idx.isValid(): return
        src_idx = self.proxy.mapToSource(idx)
        item0 = self.model.itemFromIndex(src_idx.siblingAtColumn(0))
        
        m = QMenu()
        run_path = self.model.itemFromIndex(src_idx.siblingAtColumn(15)).text()
        log_path = self.model.itemFromIndex(src_idx.siblingAtColumn(16)).text()
        node_type = item0.data(Qt.UserRole)
        
        if run_path and node_type in ["DEFAULT", "STAGE"]:
            pin_menu = m.addMenu("Pin as...")
            act_gold = pin_menu.addAction(self.icons['golden'], "Golden Run")
            act_clear = pin_menu.addAction("Clear Pin")
            m.addSeparator()
            
            res = m.exec_(self.tree.viewport().mapToGlobal(pos))
            if res == act_gold: self.user_pins[run_path] = 'golden'; save_user_pins(self.user_pins); self.build_model()
            elif res == act_clear: self.user_pins.pop(run_path, None); save_user_pins(self.user_pins); self.build_model()

    def on_header_context_menu(self, pos):
        col = self.tree.header().logicalIndexAt(pos)
        m = QMenu()
        m.addAction("Sort A to Z").triggered.connect(lambda: self.tree.sortByColumn(col, Qt.AscendingOrder))
        m.addAction("Sort Z to A").triggered.connect(lambda: self.tree.sortByColumn(col, Qt.DescendingOrder))
        m.exec_(self.tree.header().mapToGlobal(pos))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = PDDashboard()
    w.showMaximized()
    sys.exit(app.exec_())
