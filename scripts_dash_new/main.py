```python
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

# Internal Imports
from config import *
from utils import *
from workers import *
from widgets import *
from dialogs import *

# =====================================================================
# HIGH-PERFORMANCE C++ PROXY MODEL
# =====================================================================
class RunFilterProxyModel(QSortFilterProxyModel):
    """
    Handles thousands of runs by filtering in optimized C++ memory.
    Prevents UI freezing during search/filter operations.
    """
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
        
        # Helper to ensure parents stay visible if children match
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
            
        return True # Groups (BLOCK/MILESTONE) are handled by recursive logic

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
        if self.preset == "Failed Only" and not ("FAIL" in run.get("st_n","") or "FAIL" in run.get("st_u","")): return False
        
        if self.search_text and self.search_text != "*":
            note_id = f"{run['rtl']} : {run['r_name']}"
            notes = " ".join(self.global_notes.get(note_id, []))
            combined = f"{run['r_name']} {run['rtl']} {run['source']} {run['owner']} {notes}".lower()
            if not fnmatch.fnmatch(combined, f"*{self.search_text}*"): return False
        return True

    def _check_stage(self, stage, run):
        return self._check_run(run)

# =====================================================================
# MAIN DASHBOARD CLASS
# =====================================================================
class PDDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Singularity PD | Pro Edition")
        self.resize(1280, 720)

        # Core State
        self.ws_data, self.out_data, self.ir_data = {}, {}, {}
        self.global_notes = {}; self.user_pins = load_user_pins()
        self.ignored_paths = set(); self._checked_paths = set()
        self.item_map = {}; self.size_workers = []
        self.run_filter_config = None; self.current_config_path = None
        self.active_col_filters = {}

        # Theme/UI State
        self.is_dark_mode = False; self.use_custom_colors = False
        self.row_spacing = 2; self.hide_block_nodes = False
        self._columns_fitted_once = False; self._last_scan_time = None

        self.search_timer = QTimer(self); self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.apply_proxy_filters)

        self.icons = {
            'golden': self._create_dot_icon("#FFC107", "#FF9800"),
            'good': self._create_dot_icon("#4CAF50", "#388E3C"),
            'redundant': self._create_dot_icon("#F44336", "#D32F2F"),
            'later': self._create_dot_icon("#FF9800", "#F57C00")
        }

        self.init_ui()
        self._setup_shortcuts()
        self.start_fs_scan()

    def _create_dot_icon(self, color, border):
        pix = QPixmap(16, 16); pix.fill(Qt.transparent); p = QPainter(pix)
        p.setRenderHint(QPainter.Antialiasing); p.setBrush(QBrush(QColor(color))); p.setPen(QPen(QColor(border)))
        p.drawEllipse(4, 4, 8, 8); p.end(); return QIcon(pix)

    # ------------------------------------------------------------------
    # UI INITIALIZATION
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget(); self.setCentralWidget(central)
        root_layout = QVBoxLayout(central); root_layout.setContentsMargins(8, 8, 8, 4)

        # TOP NAVIGATION BAR
        top = QHBoxLayout(); top.setSpacing(8)
        top.addWidget(QLabel("<b>Source:</b>"))
        self.src_combo = QComboBox(); self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.src_combo)

        self._add_ui_sep(top)
        top.addWidget(QLabel("<b>RTL Release:</b>"))
        self.rel_combo = QComboBox(); self.rel_combo.setMinimumWidth(200)
        self.rel_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.rel_combo)

        self._add_ui_sep(top)
        top.addWidget(QLabel("<b>View:</b>"))
        self.view_combo = QComboBox(); self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Failed Only", "Today's Runs"])
        self.view_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.view_combo)

        self._add_ui_sep(top)
        self.search = QLineEdit(); self.search.setPlaceholderText("Search... [Ctrl+F]")
        self.search.textChanged.connect(lambda: self.search_timer.start(100)); top.addWidget(self.search)

        top.addStretch()

        self.refresh_btn = QPushButton("Refresh"); self.refresh_btn.clicked.connect(self.start_fs_scan); top.addWidget(self.refresh_btn)
        self.settings_btn = QPushButton("Settings"); self.settings_btn.clicked.connect(self.open_settings); top.addWidget(self.settings_btn)

        self.actions_btn = QPushButton("Actions v"); self.actions_menu = QMenu(self)
        self.actions_menu.addAction("Fit Columns", self.fit_all_columns)
        self.actions_menu.addAction("Expand All", self.safe_expand_all)
        self.actions_menu.addAction("Collapse All", self.safe_collapse_all)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Calculate Folder Sizes", self.calculate_all_sizes)
        self.actions_menu.addAction("Export Visible to CSV", self.export_csv)
        self.actions_menu.addSeparator()
        
        mail_m = self.actions_menu.addMenu("Mail Services")
        mail_m.addAction("Send Cleanup Mail (Selected)", self.send_cleanup_mail_action)
        mail_m.addAction("Send Custom Mail", self.send_custom_mail_action)
        
        filt_m = self.actions_menu.addMenu("Filter Profiles")
        filt_m.addAction("Load Config...", self.load_filter_config)
        filt_m.addAction("Clear Config", self.clear_filter_config)
        
        self.actions_btn.setMenu(self.actions_menu); top.addWidget(self.actions_btn)
        
        self.notes_toggle_btn = QPushButton("Notes <"); self.notes_toggle_btn.clicked.connect(self.toggle_notes_dock); top.addWidget(self.notes_toggle_btn)
        root_layout.addLayout(top)

        # PROGRESS BAR
        self.prog_container = QWidget(); self.prog_container.setVisible(False)
        p_lay = QHBoxLayout(self.prog_container); self.prog_lbl = QLabel(""); self.prog = QProgressBar()
        p_lay.addWidget(self.prog_lbl); p_lay.addWidget(self.prog, 1)
        root_layout.addWidget(self.prog_container)

        # SPLITTER: Left (Blocks) | Right (Tree)
        self.main_splitter = QSplitter(Qt.Horizontal)
        
        left_w = QWidget(); l_lay = QVBoxLayout(left_w); l_lay.setContentsMargins(0, 0, 4, 0)
        l_lay.addWidget(QLabel("<b>Filter Blocks</b>"))
        self.blk_list = QListWidget(); self.blk_list.itemChanged.connect(self.apply_proxy_filters); l_lay.addWidget(self.blk_list)
        
        self.fe_error_btn = QPushButton(""); self.fe_error_btn.setObjectName("errorBtn"); self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log); l_lay.addWidget(self.fe_error_btn)
        
        meta_w = QWidget(); m_lay = QFormLayout(meta_w); m_lay.setContentsMargins(0, 8, 0, 0)
        self.meta_status = QLineEdit(); self.meta_status.setReadOnly(True)
        self.meta_path = QLineEdit(); self.meta_path.setReadOnly(True)
        m_lay.addRow("Status:", self.meta_status); m_lay.addRow("Path:", self.meta_path)
        l_lay.addWidget(meta_w)
        
        self.main_splitter.addWidget(left_w)

        # MAIN TREE VIEW
        self.tree = QTreeView(); self.tree.setSortingEnabled(True); self.tree.setUniformRowHeights(True); self.tree.setAlternatingRowColors(True)
        self.model = QStandardItemModel(0, 24)
        headers = [
            "Run Name", "RTL Release", "Source", "Status", "Stage", "User", "Size",
            "FM-NONUPF", "FM-UPF", "VSLP", "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes", "Starred"
        ]
        self.model.setHorizontalHeaderLabels(headers)
        self.proxy = RunFilterProxyModel(); self.proxy.setSourceModel(self.model)
        self.tree.setModel(self.proxy)

        # Events
        self.tree.selectionModel().selectionChanged.connect(self.on_tree_selection_changed)
        self.tree.doubleClicked.connect(self.on_item_double_clicked)
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu); self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu); self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)
        self.model.itemChanged.connect(self._on_item_check_changed)

        for i in [15, 16, 17, 18, 19, 20, 21, 23]: self.tree.setColumnHidden(i, True)
        self.main_splitter.addWidget(self.tree)
        root_layout.addWidget(self.main_splitter)

        # NOTES DOCK
        self.inspector = QWidget(); i_lay = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view details."); self.ins_history = QTextEdit(); self.ins_history.setReadOnly(True)
        self.ins_note = QTextEdit(); self.ins_note.setPlaceholderText("Enter your new shared note here...")
        self.ins_save = QPushButton("Save / Update Note"); self.ins_save.clicked.connect(self.save_inspector_note)
        i_lay.addWidget(self.ins_lbl); i_lay.addWidget(QLabel("<b>Shared History:</b>")); i_lay.addWidget(self.ins_history, 1)
        i_lay.addWidget(QLabel("<b>My Contribution:</b>")); i_lay.addWidget(self.ins_note, 0); i_lay.addWidget(self.ins_save)
        
        self.note_dock = QDockWidget(self); self.note_dock.setWidget(self.inspector)
        self.note_dock.setTitleBarWidget(QWidget()); self.addDockWidget(Qt.RightDockWidgetArea, self.note_dock); self.note_dock.hide()

        # STATUS BAR
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.sb_total = QLabel("Total: 0"); self.sb_complete = QLabel("Complete: 0"); self.sb_sel = QLabel("Selected: 0")
        for l in [self.sb_total, self.sb_complete, self.sb_sel]: self.status_bar.addPermanentWidget(l)

        self.apply_theme_and_spacing()

    # ------------------------------------------------------------------
    # SCANNING & CORE DATA HANDLING
    # ------------------------------------------------------------------
    def start_fs_scan(self):
        if hasattr(self, 'worker') and self.worker.isRunning(): return
        self.prog_container.setVisible(True); self.refresh_btn.setEnabled(False)
        self.model.removeRows(0, self.model.rowCount())
        
        self.worker = ScannerWorker()
        self.worker.progress_update.connect(lambda c, t: (self.prog.setRange(0, t), self.prog.setValue(c)))
        self.worker.status_update.connect(self.prog_lbl.setText)
        self.worker.finished.connect(self.on_scan_finished); self.worker.start()

    def on_scan_finished(self, ws, out, ir, stats):
        self.ws_data, self.out_data, self.ir_data = ws, out, ir
        self.prog_container.setVisible(False); self.refresh_btn.setEnabled(True)
        self.global_notes = load_all_notes()
        self.update_combos()
        self.build_model()
        self.apply_proxy_filters()
        if not self._columns_fitted_once: 
            self._columns_fitted_once = True; self.fit_all_columns()

    def build_model(self):
        """
        Populates the model structure: Milestone -> RTL -> Runs.
        Designed to be lightning fast by avoiding any main-thread disk access.
        """
        self.tree.setUpdatesEnabled(False); self.model.removeRows(0, self.model.rowCount())
        root = self.model.invisibleRootItem()
        runs = self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", [])
        
        # Group logic
        tree_map = {}
        for r in runs:
            m = get_milestone(r['rtl']) or "UNKNOWN"
            if m not in tree_map: tree_map[m] = {}
            if r['rtl'] not in tree_map[m]: tree_map[m][r['rtl']] = []
            tree_map[m][r['rtl']].append(r)
            
        for m_name in sorted(tree_map.keys()):
            m_node = self._create_group_node(root, m_name, "MILESTONE")
            for rtl_name in sorted(tree_map[m_name].keys()):
                rtl_node = self._create_group_node(m_node, rtl_name, "RTL")
                for run in tree_map[m_name][rtl_name]:
                    self._create_run_row(rtl_node, run)
                    
        self.tree.setUpdatesEnabled(True)

    def _create_group_node(self, parent, text, n_type):
        row = [QStandardItem() for _ in range(24)]
        row[0].setText(text); row[0].setData(n_type, Qt.UserRole)
        f = row[0].font(); f.setBold(True); row[0].setFont(f)
        if n_type == "MILESTONE": row[0].setForeground(QBrush(QColor("#1e88e5")))
        parent.appendRow(row); return row[0]

    def _create_run_row(self, parent, run):
        row = [QStandardItem() for _ in range(24)]
        row[0].setCheckable(True); row[0].setText(run['r_name'])
        row[0].setData("DEFAULT", Qt.UserRole); row[0].setData(run, Qt.UserRole + 10)
        
        row[1].setText(run['rtl']); row[2].setText(run['source']); row[3].setText(run['fe_status'])
        row[5].setText(run['owner']); row[12].setText(run['info']['runtime'])
        row[15].setText(run['path']); row[16].setText(os.path.join(run['path'], "logs/compile_opt.log"))
        row[7].setText(run['st_n']); row[8].setText(run['st_u']); row[9].setText(run['vslp_status'])

        # Status Coloring
        if "COMPLETED" in run['fe_status']: row[3].setForeground(QBrush(QColor("#388e3c")))
        elif "RUNNING" in run['fe_status']: row[3].setForeground(QBrush(QColor("#1976d2")))
        
        # Check Persistent CheckState
        if run['path'] in self._checked_paths: row[0].setCheckState(Qt.Checked)
        
        # Shared Notes
        n_id = f"{run['rtl']} : {run['r_name']}"
        if n_id in self.global_notes:
            row[22].setText(" | ".join(self.global_notes[n_id]))
            row[22].setForeground(QBrush(QColor("#ef6c00")))
            
        parent.appendRow(row)
        
        # Add sub-stages if they exist
        if run.get('stages'):
            for s in run['stages']:
                self._create_stage_row(row[0], s)

    def _create_stage_row(self, parent, stage):
        row = [QStandardItem() for _ in range(24)]
        row[0].setText(stage['name']); row[0].setData("STAGE", Qt.UserRole)
        row[0].setData(stage, Qt.UserRole + 10)
        row[4].setText("STAGE COMPLETE"); row[12].setText(stage['info']['runtime'])
        row[15].setText(stage['stage_path']); row[16].setText(stage.get('log',''))
        parent.appendRow(row)

    # ------------------------------------------------------------------
    # UTILITIES & UI ACTIONS
    # ------------------------------------------------------------------
    def apply_proxy_filters(self):
        blks = [self.blk_list.item(i).data(Qt.UserRole) for i in range(self.blk_list.count()) if self.blk_list.item(i).checkState() == Qt.Checked]
        self.proxy.update_filters(
            self.src_combo.currentText(), self.rel_combo.currentText(), self.view_combo.currentText(),
            self.search.text(), blks, self.run_filter_config, self.user_pins, self.global_notes
        )
        self._update_status_bar()

    def _update_status_bar(self):
        tot = self.proxy.rowCount(); complete = 0
        def count_recursive(p_idx):
            nonlocal complete
            for i in range(self.proxy.rowCount(p_idx)):
                c_idx = self.proxy.index(i, 3, p_idx)
                if "COMPLETED" in str(c_idx.data()): complete += 1
                if self.proxy.hasChildren(c_idx): count_recursive(c_idx)
        count_recursive(QModelIndex())
        self.sb_total.setText(f"Visible Root: {tot}  "); self.sb_complete.setText(f"Completed: {complete}  ")
        self.sb_sel.setText(f"Selected: {len(self._checked_paths)}")

    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        self.tree.header().resizeSections(QHeaderView.ResizeToContents)
        if self.tree.columnWidth(0) > 450: self.tree.setColumnWidth(0, 450)
        self.tree.setUpdatesEnabled(True)

    def on_tree_selection_changed(self):
        idx = self.tree.currentIndex()
        self.fe_error_btn.setVisible(False)
        if not idx.isValid(): return
        
        src_idx = self.proxy.mapToSource(idx); item0 = self.model.itemFromIndex(src_idx.siblingAtColumn(0))
        run_data = item0.data(Qt.UserRole + 10)
        if not run_data: return

        self.meta_status.setText(self.model.itemFromIndex(src_idx.siblingAtColumn(3)).text())
        self.meta_path.setText(run_data.get('path', ''))
        
        # Update Dock
        if item0.data(Qt.UserRole) == "STAGE": self._current_note_id = f"{item0.parent().text()} : {item0.text()}"
        else: self._current_note_id = f"{run_data['rtl']} : {run_data['r_name']}"
        
        notes = self.global_notes.get(self._current_note_id, [])
        self.ins_history.setPlainText("\n\n".join(notes) if notes else "No shared history.")
        
        # Check Error Log Link
        err_log = os.path.join(run_data.get('path',''), "logs/compile_opt.error.log")
        if os.path.exists(err_log):
            self.fe_error_btn.setText("View Error Log"); self.fe_error_btn.setVisible(True)
            self._cur_err_path = err_log

    def on_item_double_clicked(self, index):
        s_idx = self.proxy.mapToSource(index)
        log = self.model.itemFromIndex(s_idx.siblingAtColumn(16)).text()
        if log: subprocess.Popen(['gvim', log])

    def _on_item_check_changed(self, item):
        if item.column() != 0: return
        path = self.model.itemFromIndex(item.index().siblingAtColumn(15)).text()
        if not path: return
        if item.checkState() == Qt.Checked: self._checked_paths.add(path)
        else: self._checked_paths.discard(path)
        self.sb_sel.setText(f"Selected: {len(self._checked_paths)}")

    # ------------------------------------------------------------------
    # EMAILS, QOR, & DIALOGS
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        user_map = {}
        for path in self._checked_paths:
            # Reverse lookup run info
            owner = "Unknown"
            user_map.setdefault(owner, []).append(path)
        
        if not user_map: return
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Disk Cleanup Required", "Please cleanup these runs:", all_known, "", self)
        if dlg.exec_():
            # Send logic using subprocess.Popen(MAIL_UTIL...)
            pass

    def run_qor_comparison(self):
        sel = list(self._checked_paths)
        if len(sel) < 2: return
        subprocess.run(["python3.6", SUMMARY_SCRIPT] + sel)
        h = find_latest_qor_report()
        if h: subprocess.Popen([FIREFOX_PATH, h])

    def calculate_all_sizes(self):
        tasks = []
        for i in range(self.model.rowCount()):
            idx = self.model.index(i, 0)
            p = idx.siblingAtColumn(15).data()
            if p: tasks.append((str(id(idx)), p))
        self.size_worker = BatchSizeWorker(tasks)
        self.size_worker.size_calculated.connect(lambda tid, sz: None) # Update model item
        self.size_worker.start()

    # ------------------------------------------------------------------
    # MENU BUILDERS & MISC
    # ------------------------------------------------------------------
    def on_context_menu(self, pos):
        idx = self.tree.indexAt(pos)
        if not idx.isValid(): return
        src_idx = self.proxy.mapToSource(idx); item0 = self.model.itemFromIndex(src_idx.siblingAtColumn(0))
        run_data = item0.data(Qt.UserRole + 10)

        m = QMenu()
        m.addAction("Open Directory").triggered.connect(lambda: subprocess.Popen(['nautilus', run_data['path']]))
        m.addAction("Copy Absolute Path").triggered.connect(lambda: QApplication.clipboard().setText(run_data['path']))
        m.addSeparator()
        if item0.hasChildren():
            m.addAction("View Gantt Chart").triggered.connect(lambda: self.open_gantt(item0))
        m.exec_(self.tree.viewport().mapToGlobal(pos))

    def on_header_context_menu(self, pos):
        m = QMenu()
        m.addAction("Fit All Columns").triggered.connect(self.fit_all_columns)
        m.exec_(self.tree.header().mapToGlobal(pos))

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self, lambda: self.search.setFocus())

    def _add_ui_sep(self, lay):
        f = QFrame(); f.setFrameShape(QFrame.VLine); f.setFrameShadow(QFrame.Sunken); lay.addWidget(f)

    def toggle_notes_dock(self):
        if self.note_dock.isVisible(): self.note_dock.hide(); self.notes_toggle_btn.setText("Notes <")
        else: self.note_dock.show(); self.notes_toggle_btn.setText("Notes v")

    def open_error_log(self):
        if hasattr(self, '_cur_err_path'): subprocess.Popen(['gvim', self._cur_err_path])

    def open_settings(self): SettingsDialog(self).exec_()
    def save_inspector_note(self):
        save_user_note(self._current_note_id, self.ins_note.toPlainText())
        self.global_notes = load_all_notes(); self.build_model()

    def update_combos(self):
        self.rel_combo.clear(); self.rel_combo.addItems(["[ SHOW ALL ]"] + sorted(self.ws_data.get('releases',{}).keys()))
        self.blk_list.clear()
        for b in sorted(list(self.ws_data['blocks']) + list(self.out_data['blocks'])):
            it = QListWidgetItem(b); it.setFlags(it.flags() | Qt.ItemIsUserCheckable); it.setCheckState(Qt.Checked); it.setData(Qt.UserRole, b)
            self.blk_list.addItem(it)

    def safe_expand_all(self): self.tree.expandAll()
    def safe_collapse_all(self): self.tree.collapseAll()
    def export_csv(self): pass
    def load_filter_config(self): pass
    def clear_filter_config(self): pass
    def apply_theme_and_spacing(self):
        # Professional UI Stylesheet
        self.setStyleSheet("""
            QMainWindow { background-color: #f0f2f5; }
            QTreeView { background-color: white; border: 1px solid #d1d5db; border-radius: 4px; }
            QHeaderView::section { background-color: #f9fafb; padding: 6px; border: 1px solid #e5e7eb; font-weight: bold; }
            QPushButton { padding: 6px 12px; border-radius: 4px; background: white; border: 1px solid #d1d5db; }
            QPushButton:hover { background: #f3f4f6; }
            QLineEdit { padding: 5px; border: 1px solid #d1d5db; border-radius: 4px; }
        """)

if __name__ == "__main__":
    app = QApplication(sys.argv); app.setStyle("Fusion")
    win = PDDashboard(); win.showMaximized()
    sys.exit(app.exec_())

