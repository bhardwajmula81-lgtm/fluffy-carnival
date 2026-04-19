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

# Internal Project Imports
from config import *
from utils import *
from workers import *
from widgets import *
from dialogs import *

# =====================================================================
# HIGH-PERFORMANCE C++ PROXY MODEL (FOR INSTANT FILTERING)
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
        
        # Enable Qt's native recursive filtering if available
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
        self.invalidateFilter() # Triggers the C++ engine to re-scan visible items

    def filterAcceptsRow(self, source_row, source_parent):
        idx0 = self.sourceModel().index(source_row, 0, source_parent)
        node_type = self.sourceModel().data(idx0, Qt.UserRole)
        
        # If recursive filtering is not supported by this Qt version, we check children manually
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
            
        return True # Blocks, Milestones, and RTL nodes are kept (they'll hide if no children match)

    def _check_run(self, run):
        # 1. Pinned Runs Always Show
        if self.user_pins.get(run["path"]) == "golden": return True
        
        # 2. Basic Filters
        if self.src_mode != "ALL" and run["source"] != self.src_mode: return False
        if run["block"] not in self.checked_blks: return False
        
        # 3. External Config Filter
        if self.filter_config is not None:
            r_src, r_rtl, r_blk = run["source"], run["rtl"], run["block"]
            if r_src in self.filter_config and r_rtl in self.filter_config[r_src] and r_blk in self.filter_config[r_src][r_rtl]:
                allowed = self.filter_config[r_src][r_rtl][r_blk]
                b_name = run["r_name"].replace("-FE", "").replace("-BE", "")
                if b_name not in allowed and run["r_name"] not in allowed: return False
                
        # 4. RTL Release Specifics
        if self.sel_rtl != "[ SHOW ALL ]" and not (run["rtl"] == self.sel_rtl or run["rtl"].startswith(self.sel_rtl + "_")): return False
        
        # 5. Dashboard Presets
        if self.preset == "FE Only" and run["run_type"] != "FE": return False
        if self.preset == "BE Only" and run["run_type"] != "BE": return False
        if self.preset == "Failed Only" and not ("FAIL" in run.get("st_n","") or "FAIL" in run.get("st_u","")): return False
        if self.preset == "Today's Runs":
            start = run["info"].get("start","")
            if not ("hours ago" in relative_time(start) or "minutes ago" in relative_time(start)): return False
            
        # 6. Global Search (Case Insensitive)
        if self.search_text and self.search_text != "*":
            note_id = f"{run['rtl']} : {run['r_name']}"
            notes = " ".join(self.global_notes.get(note_id, []))
            combined = f"{run['r_name']} {run['rtl']} {run['source']} {run['owner']} {notes} {run['st_n']} {run['st_u']}".lower()
            if self.search_text not in combined: return False
            
        return True

    def _check_stage(self, stage, run):
        # Stages follow the same visibility rules as their parent run
        return self._check_run(run)

# =====================================================================
# MAIN DASHBOARD CLASS
# =====================================================================
class PDDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Singularity PD | Pro Edition")
        self.resize(1300, 800)

        # Persistence & Memory
        self.ws_data, self.out_data, self.ir_data = {}, {}, {}
        self.global_notes = {}; self.user_pins = load_user_pins()
        self.ignored_paths = set(); self._checked_paths = set()
        self.item_map = {}; self.size_workers = []
        self.run_filter_config = None; self.current_config_path = None

        # Theme Configuration
        self.is_dark_mode = False; self.use_custom_colors = False
        self.row_spacing = 2; self.hide_block_nodes = False
        self._columns_fitted_once = False; self._last_scan_time = None

        # Timer for typing searches
        self.search_timer = QTimer(self); self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.apply_proxy_filters)

        # Asset Icons
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
    # UI ASSEMBLY
    # ------------------------------------------------------------------
    def init_ui(self):
        central = QWidget(); self.setCentralWidget(central)
        root_layout = QVBoxLayout(central); root_layout.setContentsMargins(10, 10, 10, 5)

        # 1. TOP CONTROL BAR
        top = QHBoxLayout(); top.setSpacing(10)
        top.addWidget(QLabel("<b>Source:</b>"))
        self.src_combo = QComboBox(); self.src_combo.addItems(["ALL", "WS", "OUTFEED"])
        self.src_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.src_combo)

        self._add_ui_sep(top)
        top.addWidget(QLabel("<b>RTL Release:</b>"))
        self.rel_combo = QComboBox(); self.rel_combo.setMinimumWidth(220)
        self.rel_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.rel_combo)

        self._add_ui_sep(top)
        top.addWidget(QLabel("<b>View:</b>"))
        self.view_combo = QComboBox(); self.view_combo.addItems(["All Runs", "FE Only", "BE Only", "Failed Only", "Today's Runs"])
        self.view_combo.currentIndexChanged.connect(self.apply_proxy_filters); top.addWidget(self.view_combo)

        self._add_ui_sep(top)
        self.search = QLineEdit(); self.search.setPlaceholderText("Search runs, users, notes... [Ctrl+F]")
        self.search.textChanged.connect(lambda: self.search_timer.start(150)); top.addWidget(self.search)

        top.addStretch()

        self.refresh_btn = QPushButton("Refresh (Ctrl+R)"); self.refresh_btn.clicked.connect(self.start_fs_scan); top.addWidget(self.refresh_btn)
        
        self.actions_btn = QPushButton("Actions v"); self.actions_menu = QMenu(self)
        self.actions_menu.addAction("Fit All Columns", self.fit_all_columns)
        self.actions_menu.addAction("Expand All Rows (Ctrl+E)", self.safe_expand_all)
        self.actions_menu.addAction("Collapse All Rows (Ctrl+W)", self.safe_collapse_all)
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Calculate All Folder Sizes", self.calculate_all_sizes)
        self.actions_menu.addAction("Export Visible View to CSV", self.export_csv)
        self.actions_menu.addSeparator()
        
        mail_m = self.actions_menu.addMenu("Mail Utilities")
        mail_m.addAction("Send Cleanup Mail (To Owners of Selected)", self.send_cleanup_mail_action)
        mail_m.addAction("Send QoR Comparison Mail", self.send_qor_mail_action)
        mail_m.addAction("Send Custom Formatted Mail", self.send_custom_mail_action)
        
        filt_m = self.actions_menu.addMenu("Filter Profiles")
        filt_m.addAction("Load Run Filter Config (.cfg)...", self.load_filter_config)
        filt_m.addAction("Clear Active Config", self.clear_filter_config)
        filt_m.addAction("Generate Sample Config", self.generate_sample_config)
        
        self.actions_menu.addSeparator()
        self.actions_menu.addAction("Dashboard Settings", self.open_settings)
        self.actions_btn.setMenu(self.actions_menu); top.addWidget(self.actions_btn)
        
        self.notes_btn = QPushButton("Notes Panel <"); self.notes_btn.clicked.connect(self.toggle_notes_dock); top.addWidget(self.notes_btn)
        root_layout.addLayout(top)

        # 2. PROGRESS OVERLAY
        self.prog_container = QWidget(); self.prog_container.setVisible(False)
        p_lay = QHBoxLayout(self.prog_container); self.prog_lbl = QLabel("Scanning..."); self.prog = QProgressBar()
        p_lay.addWidget(self.prog_lbl); p_lay.addWidget(self.prog, 1)
        root_layout.addWidget(self.prog_container)

        # 3. SPLITTER FOR MAIN LAYOUT
        self.main_splitter = QSplitter(Qt.Horizontal)
        
        # Left Dashboard (Filters & Meta)
        left_w = QWidget(); l_lay = QVBoxLayout(left_w); l_lay.setContentsMargins(0, 0, 4, 0)
        blk_head = QHBoxLayout(); blk_head.addWidget(QLabel("<b>Blocks</b>")); blk_head.addStretch()
        a_btn = QPushButton("All"); a_btn.clicked.connect(lambda: self._set_all_blocks(True))
        n_btn = QPushButton("None"); n_btn.clicked.connect(lambda: self._set_all_blocks(False))
        blk_head.addWidget(a_btn); blk_head.addWidget(n_btn); l_lay.addLayout(blk_head)
        
        self.blk_list = QListWidget(); self.blk_list.itemChanged.connect(self.apply_proxy_filters); l_lay.addWidget(self.blk_list)
        
        self.fe_error_btn = QPushButton(""); self.fe_error_btn.setObjectName("errorLinkBtn"); self.fe_error_btn.setVisible(False)
        self.fe_error_btn.clicked.connect(self.open_error_log); l_lay.addWidget(self.fe_error_btn)
        
        meta_w = QWidget(); m_lay = QFormLayout(meta_w); m_lay.setContentsMargins(0, 8, 0, 0)
        self.meta_status = QLineEdit(); self.meta_status.setReadOnly(True)
        self.meta_path = QLineEdit(); self.meta_path.setReadOnly(True)
        self.meta_log = QLineEdit(); self.meta_log.setReadOnly(True)
        m_lay.addRow("Status:", self.meta_status); m_lay.addRow("Run Path:", self.meta_path); m_lay.addRow("Main Log:", self.meta_log)
        l_lay.addWidget(meta_w)
        
        self.main_splitter.addWidget(left_w)

        # 4. HIGH-SPEED TREE VIEW
        self.tree = QTreeView(); self.tree.setSortingEnabled(True); self.tree.setUniformRowHeights(True); self.tree.setAlternatingRowColors(True)
        self.model = QStandardItemModel(0, 24)
        self.model.setHorizontalHeaderLabels([
            "Run Name (Select)", "RTL Release Version", "Source", "Status", "Stage", "User", "Size",
            "FM-NONUPF", "FM-UPF", "VSLP Status", "Static IR", "Dynamic IR", "Runtime", "Start", "End",
            "Path", "Log", "UPF_RPT", "NONUPF_RPT", "VSLP_RPT", "STA_RPT", "IR_LOG", "Alias / Notes", "Starred"
        ])
        self.proxy = RunFilterProxyModel(); self.proxy.setSourceModel(self.model)
        self.tree.setModel(self.proxy)
        
        # Tree Interaction
        self.tree.selectionModel().selectionChanged.connect(self.on_tree_selection_changed)
        self.tree.doubleClicked.connect(self.on_item_double_clicked)
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu); self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.header().setContextMenuPolicy(Qt.CustomContextMenu); self.tree.header().customContextMenuRequested.connect(self.on_header_context_menu)
        self.model.itemChanged.connect(self._on_item_check_changed)
        
        # Column Management
        for i in [15, 16, 17, 18, 19, 20, 21, 23]: self.tree.setColumnHidden(i, True)
        self.main_splitter.addWidget(self.tree)
        root_layout.addWidget(self.main_splitter)

        # 5. SIDE PANEL (NOTES DOCK)
        self.inspector = QWidget(); i_lay = QVBoxLayout(self.inspector)
        self.ins_lbl = QLabel("Select a run to view historical notes."); self.ins_lbl.setWordWrap(True)
        self.ins_history = QTextEdit(); self.ins_history.setReadOnly(True)
        self.ins_note = QTextEdit(); self.ins_note.setPlaceholderText("Enter your new note here...")
        self.ins_save = QPushButton("Save / Update Note"); self.ins_save.clicked.connect(self.save_inspector_note)
        i_lay.addWidget(self.ins_lbl); i_lay.addWidget(QLabel("<b>Shared History:</b>")); i_lay.addWidget(self.ins_history, 1)
        i_lay.addWidget(QLabel("<b>My Entry:</b>")); i_lay.addWidget(self.ins_note, 0); i_lay.addWidget(self.ins_save)
        
        self.note_dock = QDockWidget(self); self.note_dock.setWidget(self.inspector)
        self.note_dock.setTitleBarWidget(QWidget()); self.addDockWidget(Qt.RightDockWidgetArea, self.note_dock)
        self.note_dock.hide()

        # 6. STATUS BAR
        self.status_bar = QStatusBar(); self.setStatusBar(self.status_bar)
        self.sb_total = QLabel("Total Runs: 0"); self.sb_comp = QLabel("Completed: 0"); self.sb_sel = QLabel("Selected: 0")
        for l in [self.sb_total, self.sb_comp, self.sb_sel]: 
            l.setContentsMargins(10, 0, 10, 0)
            self.status_bar.addPermanentWidget(l)

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
        self._last_scan_time = QDateTime.currentDateTime().toString("hh:mm:ss")
        self.global_notes = load_all_notes()
        self.update_combos()
        self.build_model()
        self.apply_proxy_filters()
        if not self._columns_fitted_once: 
            self._columns_fitted_once = True; self.fit_all_columns()

    def build_model(self):
        """
        Organizes raw run data into a hierarchical tree structure.
        Crucially: Milestone -> RTL Version -> Run Name -> Stages.
        """
        self.tree.setUpdatesEnabled(False); self.model.removeRows(0, self.model.rowCount())
        root = self.model.invisibleRootItem()
        runs = self.ws_data.get("all_runs", []) + self.out_data.get("all_runs", [])
        
        # Hierarchy Map: {Milestone: {RTL: [Runs]}}
        tree_map = {}
        for r in runs:
            m = get_milestone(r['rtl']) or "UNKNOWN_MILESTONE"
            tree_map.setdefault(m, {}).setdefault(r['rtl'], []).append(r)
            
        for m_name in sorted(tree_map.keys()):
            m_node = self._create_group_node(root, m_name, "MILESTONE")
            for rtl_name in sorted(tree_map[m_name].keys()):
                rtl_node = self._create_group_node(m_node, rtl_name, "RTL")
                for run in sorted(tree_map[m_name][rtl_name], key=lambda x: x['r_name']):
                    self._create_run_row(rtl_node, run)
                    
        self.tree.setUpdatesEnabled(True)

    def _create_group_node(self, parent, text, n_type):
        row = [QStandardItem() for _ in range(24)]
        row[0].setText(text); row[0].setData(n_type, Qt.UserRole)
        f = row[0].font(); f.setBold(True); row[0].setFont(f)
        if n_type == "MILESTONE": row[0].setForeground(QBrush(QColor("#1e88e5")))
        elif n_type == "RTL": row[0].setForeground(QBrush(QColor("#666666")))
        parent.appendRow(row); return row[0]

    def _create_run_row(self, parent, run):
        row = [QStandardItem() for _ in range(24)]
        row[0].setCheckable(True); row[0].setText(run['r_name'])
        row[0].setData("DEFAULT", Qt.UserRole); row[0].setData(run, Qt.UserRole + 10)
        
        row[1].setText(run['rtl']); row[2].setText(run['source']); row[3].setText(run['fe_status'])
        row[5].setText(run['owner']); row[12].setText(run['info']['runtime'])
        row[13].setText(run['info']['start']); row[14].setText(run['info']['end'])
        
        row[15].setText(run['path']); row[16].setText(os.path.join(run['path'], "logs/compile_opt.log"))
        row[7].setText(run['st_n']); row[8].setText(run['st_u']); row[9].setText(run['vslp_status'])

        # Status Coloring logic
        st = run['fe_status'].upper()
        if "COMPLETED" in st: row[3].setForeground(QBrush(QColor("#388e3c")))
        elif "RUNNING" in st: row[3].setForeground(QBrush(QColor("#1976d2")))
        elif "FAIL" in st or "FATAL" in st: row[3].setForeground(QBrush(QColor("#d32f2f")))

        # Check existing pins
        if self.user_pins.get(run['path']) in self.icons:
            row[0].setIcon(self.icons[self.user_pins[run['path']]])
        
        # Check selection state
        if run['path'] in self._checked_paths: row[0].setCheckState(Qt.Checked)
        
        # Apply Notes
        n_id = f"{run['rtl']} : {run['r_name']}"
        if n_id in self.global_notes:
            note_str = " | ".join(self.global_notes[n_id])
            row[22].setText(note_str); row[22].setForeground(QBrush(QColor("#ef6c00")))
            
        parent.appendRow(row)
        
        # Populate Stages if Backend
        if run.get('stages'):
            for s in run['stages']:
                self._create_stage_row(row[0], s)

    def _create_stage_row(self, parent, stage):
        row = [QStandardItem() for _ in range(24)]
        row[0].setText(stage['name']); row[0].setData("STAGE", Qt.UserRole)
        row[0].setData(stage, Qt.UserRole + 10)
        row[4].setText("COMPLETED"); row[12].setText(stage['info']['runtime'])
        row[7].setText(stage['st_n']); row[8].setText(stage['st_u']); row[9].setText(stage['vslp_status'])
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
        total = 0; complete = 0
        def traverse(p_idx):
            nonlocal total, complete
            for i in range(self.proxy.rowCount(p_idx)):
                c_idx = self.proxy.index(i, 0, p_idx)
                if self.proxy.data(c_idx, Qt.UserRole) == "DEFAULT":
                    total += 1
                    if "COMPLETED" in str(self.proxy.index(i, 3, p_idx).data()): complete += 1
                traverse(c_idx)
        traverse(QModelIndex())
        self.sb_total.setText(f"Total Visible: {total}"); self.sb_comp.setText(f"Completed: {complete}")
        self.sb_sel.setText(f"Selected: {len(self._checked_paths)}")

    def fit_all_columns(self):
        self.tree.setUpdatesEnabled(False)
        self.tree.header().resizeSections(QHeaderView.ResizeToContents)
        if self.tree.columnWidth(0) > 480: self.tree.setColumnWidth(0, 480)
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
        self.meta_log.setText(self.model.itemFromIndex(src_idx.siblingAtColumn(16)).text())
        
        # Update Side Panel
        if item0.data(Qt.UserRole) == "STAGE": self._current_note_id = f"{item0.parent().text()} : {item0.text()}"
        else: self._current_note_id = f"{run_data['rtl']} : {run_data['r_name']}"
        
        notes = self.global_notes.get(self._current_note_id, [])
        self.ins_history.setPlainText("\n\n".join(notes) if notes else "No notes found for this entry.")
        
        # Check Error Log Link
        err_log = os.path.join(run_data.get('path',''), "logs/compile_opt.error.log")
        if os.path.exists(err_log):
            self.fe_error_btn.setText("View compile_opt Error Log"); self.fe_error_btn.setVisible(True)
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
    # EMAILS & CSV EXPORT
    # ------------------------------------------------------------------
    def send_cleanup_mail_action(self):
        """Groups selected runs by their owner and opens the mail dialog."""
        user_runs = {}
        # We must look through the actual model to find owners of checked paths
        def gather_checked(parent):
            for i in range(parent.rowCount()):
                c = parent.child(i, 0)
                if c.checkState() == Qt.Checked:
                    path = parent.child(i, 15).text()
                    owner = parent.child(i, 5).text()
                    if owner and owner != "Unknown":
                        user_runs.setdefault(owner, []).append(path)
                gather_checked(c)
        gather_checked(self.model.invisibleRootItem())
        
        if not user_runs:
            QMessageBox.warning(self, "No Runs Selected", "Select runs via checkboxes first.")
            return

        all_known = get_all_known_mail_users()
        all_emails = [get_user_email(o) for o in user_runs.keys() if get_user_email(o)]
        
        dlg = AdvancedMailDialog("Action Required: Cleanup PD Runs", "Hi,\n\nPlease remove these runs to save disk space:\n\n", all_known, ", ".join(all_emails), self)
        if dlg.exec_():
            subject = dlg.subject_input.text()
            body = dlg.body_input.toPlainText()
            for owner, paths in user_runs.items():
                dest = get_user_email(owner)
                if not dest: continue
                final_body = f"{body}\n" + "\n".join(paths)
                # Subprocess call to internal mailer
                subprocess.Popen([MAIL_UTIL, "-to", dest, "-s", subject, "-c", final_body])
            QMessageBox.information(self, "Success", "Cleanup emails dispatched.")

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Visible Data", "dashboard_export.csv", "CSV Files (*.csv)")
        if not path: return
        try:
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                headers = [self.model.horizontalHeaderItem(i).text() for i in range(15)]
                writer.writerow(headers)
                
                def export_node(p_idx):
                    for i in range(self.proxy.rowCount(p_idx)):
                        row_data = [self.proxy.index(i, col, p_idx).data() for col in range(15)]
                        writer.writerow(row_data)
                        export_node(self.proxy.index(i, 0, p_idx))
                export_node(QModelIndex())
            QMessageBox.information(self, "Export", f"CSV saved to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to export: {e}")

    # ------------------------------------------------------------------
    # CONFIG LOADERS & SETTINGS
    # ------------------------------------------------------------------
    def load_filter_config(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Filter Profile", "", "Config Files (*.cfg);;Text Files (*.txt)")
        if not path: return
        parsed = {}
        try:
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    # Format: SRC : RTL : BLK : RUN1 RUN2 ...
                    p = line.split(":", 3)
                    if len(p) == 4:
                        src, rtl, blk, runs = [x.strip() for x in p]
                        parsed.setdefault(src, {}).setdefault(rtl, {})[blk] = set(runs.split())
            self.run_filter_config = parsed
            self.apply_proxy_filters()
            QMessageBox.information(self, "Loaded", f"Filter config '{os.path.basename(path)}' is now active.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Invalid config format: {e}")

    def clear_filter_config(self):
        self.run_filter_config = None
        self.apply_proxy_filters()
        QMessageBox.information(self, "Cleared", "Static filter config removed.")

    def generate_sample_config(self):
        sample = "# Filter Profile\n# SOURCE : RTL : BLOCK : RUN_NAMES\nWS : RTL_V1 : CPU_CORE : run_golden run_test\nOUTFEED : * : * : *"
        path, _ = QFileDialog.getSaveFileName(self, "Save Sample", "sample.cfg", "Config Files (*.cfg)")
        if path:
            with open(path, 'w') as f: f.write(sample)

    def calculate_all_sizes(self):
        tasks = []
        def gather(parent):
            for i in range(parent.rowCount()):
                c = parent.child(i, 0)
                p = parent.child(i, 15).text()
                if p: tasks.append((str(id(c)), p)); self.item_map[str(id(c))] = parent.child(i, 6)
                gather(c)
        gather(self.model.invisibleRootItem())
        
        self.size_worker = BatchSizeWorker(tasks)
        self.size_worker.size_calculated.connect(lambda tid, sz: self.item_map[tid].setText(sz) if tid in self.item_map else None)
        self.size_worker.start()

    # ------------------------------------------------------------------
    # EVENT OVERRIDES & MISC
    # ------------------------------------------------------------------
    def on_context_menu(self, pos):
        idx = self.tree.indexAt(pos)
        if not idx.isValid(): return
        src_idx = self.proxy.mapToSource(idx); item0 = self.model.itemFromIndex(src_idx.siblingAtColumn(0))
        run_data = item0.data(Qt.UserRole + 10)
        
        m = QMenu()
        m.addAction("Open Directory (Nautilus)").triggered.connect(lambda: subprocess.Popen(['nautilus', run_data['path']]))
        m.addAction("Copy Absolute Path").triggered.connect(lambda: QApplication.clipboard().setText(run_data['path']))
        m.addSeparator()
        
        pin_m = m.addMenu("Pin Run as...")
        for name, ico in self.icons.items():
            pin_m.addAction(ico, f"Mark as {name.title()}").triggered.connect(lambda n=name: self._pin_run(run_data['path'], n))
        
        if item0.hasChildren():
            m.addAction("View Run Timeline (Gantt)").triggered.connect(lambda: self._show_gantt(item0))
        
        m.exec_(self.tree.viewport().mapToGlobal(pos))

    def on_header_context_menu(self, pos):
        m = QMenu()
        m.addAction("Auto-Fit All Columns").triggered.connect(self.fit_all_columns)
        m.addSeparator()
        for i in range(self.model.columnCount()):
            act = QWidgetAction(m)
            cb = QCheckBox(self.model.horizontalHeaderItem(i).text())
            cb.setChecked(not self.tree.isColumnHidden(i))
            cb.toggled.connect(lambda chk, c=i: self.tree.setColumnHidden(c, not chk))
            act.setDefaultWidget(cb); m.addAction(act)
        m.exec_(self.tree.header().mapToGlobal(pos))

    def _setup_shortcuts(self):
        QShortcut(QKeySequence("Ctrl+R"), self, self.start_fs_scan)
        QShortcut(QKeySequence("Ctrl+F"), self, lambda: self.search.setFocus())
        QShortcut(QKeySequence("Ctrl+E"), self, self.safe_expand_all)
        QShortcut(QKeySequence("Ctrl+W"), self, self.safe_collapse_all)

    def _pin_run(self, path, status):
        self.user_pins[path] = status; save_user_pins(self.user_pins); self.build_model()

    def _set_all_blocks(self, state):
        for i in range(self.blk_list.count()): self.blk_list.item(i).setCheckState(Qt.Checked if state else Qt.Unchecked)

    def toggle_notes_dock(self):
        if self.note_dock.isVisible(): self.note_dock.hide(); self.notes_btn.setText("Notes Panel <")
        else: self.note_dock.show(); self.notes_btn.setText("Notes Panel v")

    def open_error_log(self):
        if hasattr(self, '_cur_err_path'): subprocess.Popen(['gvim', self._cur_err_path])

    def save_inspector_note(self):
        save_user_note(self._current_note_id, self.ins_note.toPlainText())
        self.global_notes = load_all_notes(); self.build_model()

    def update_combos(self):
        self.rel_combo.clear(); self.rel_combo.addItems(["[ SHOW ALL ]"] + sorted(self.ws_data.get('releases',{}).keys()))
        self.blk_list.clear()
        for b in sorted(list(self.ws_data.get('blocks',set())) + list(self.out_data.get('blocks',set()))):
            it = QListWidgetItem(b); it.setFlags(it.flags() | Qt.ItemIsUserCheckable); it.setCheckState(Qt.Checked); it.setData(Qt.UserRole, b)
            self.blk_list.addItem(it)

    def safe_expand_all(self): self.tree.expandAll(); self.fit_all_columns()
    def safe_collapse_all(self): self.tree.collapseAll()
    def _add_ui_sep(self, lay):
        f = QFrame(); f.setFrameShape(QFrame.VLine); f.setFrameShadow(QFrame.Sunken); lay.addWidget(f)
    def open_settings(self): SettingsDialog(self).exec_()
    
    def send_qor_mail_action(self): pass # Implement from utils as needed
    def send_custom_mail_action(self): pass

    def apply_theme_and_spacing(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #f3f4f6; }
            QTreeView { background-color: white; border: 1px solid #d1d5db; alternate-background-color: #f9fafb; outline: 0; }
            QTreeView::item { padding: 4px; border-bottom: 1px solid #f3f4f6; }
            QTreeView::item:selected { background-color: #3b82f6; color: white; }
            QHeaderView::section { background-color: #e5e7eb; padding: 6px; border: 1px solid #d1d5db; font-weight: bold; }
            QPushButton { padding: 6px 12px; border-radius: 4px; background: white; border: 1px solid #d1d5db; font-weight: 500; }
            QPushButton:hover { background: #f9fafb; border-color: #9ca3af; }
            QLineEdit { padding: 6px; border: 1px solid #d1d5db; border-radius: 4px; background: white; }
            QStatusBar { background: #e5e7eb; color: #4b5563; }
        """)

if __name__ == "__main__":
    app = QApplication(sys.argv); app.setStyle("Fusion")
    win = PDDashboard(); win.showMaximized()
    sys.exit(app.exec_())