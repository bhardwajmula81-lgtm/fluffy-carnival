import os
import subprocess
import getpass
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
                             QLineEdit, QLabel, QPushButton, QTextEdit,
                             QDialogButtonBox, QMessageBox, QFileDialog,
                             QTableWidget, QTableWidgetItem, QHeaderView,
                             QFrame, QListWidget, QListWidgetItem,
                             QApplication, QFontComboBox, QSpinBox, QCheckBox,
                             QColorDialog, QComboBox, QTreeWidget, QTreeWidgetItem)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor
from config import *
from utils import *
from widgets import MultiCompleterLineEdit, PieChartWidget

class AdvancedMailDialog(QDialog):
    def __init__(self, default_subject, default_body, all_users, prefill_to="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Send EMail Message")
        self.resize(700, 550)
        self.attachments = []
        
        layout = QVBoxLayout(self)
        
        form_layout = QFormLayout()
        self.to_input = MultiCompleterLineEdit()
        self.to_input.setModel(all_users)
        
        self.cc_input = MultiCompleterLineEdit()
        self.cc_input.setModel(all_users)
        
        try:
            always_to = mail_config.get('PERMANENT_MEMBERS', 'always_to', fallback='').strip()
            always_cc = mail_config.get('PERMANENT_MEMBERS', 'always_cc', fallback='').strip()
            
            final_to = always_to
            if prefill_to:
                final_to = always_to + (", " if always_to else "") + prefill_to
                
            if final_to: self.to_input.setText(final_to + (', ' if not final_to.endswith(',') else ' '))
            if always_cc: self.cc_input.setText(always_cc + (', ' if not always_cc.endswith(',') else ' '))
        except: pass

        self.subject_input = QLineEdit(default_subject)
        
        form_layout.addRow("<b>To:</b>", self.to_input)
        form_layout.addRow("<b>CC:</b>", self.cc_input)
        form_layout.addRow("<b>Subject:</b>", self.subject_input)
        layout.addLayout(form_layout)

        attach_layout = QHBoxLayout()
        attach_layout.addWidget(QLabel("<b>Attachments:</b>"))
        self.attach_lbl = QLabel("None")
        self.attach_lbl.setStyleSheet("color: #1976D2;")
        attach_layout.addWidget(self.attach_lbl)
        attach_layout.addStretch()
        
        btn_qor = QPushButton("Attach Latest QoR Report")
        btn_qor.clicked.connect(self.attach_qor)
        btn_browse = QPushButton("Browse Files...")
        btn_browse.clicked.connect(self.browse_files)
        attach_layout.addWidget(btn_qor)
        attach_layout.addWidget(btn_browse)
        layout.addLayout(attach_layout)

        self.body_input = QTextEdit()
        self.body_input.setPlainText(default_body)
        layout.addWidget(self.body_input)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Send Mail")
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def update_attach_lbl(self):
        if not self.attachments: self.attach_lbl.setText("None")
        else: self.attach_lbl.setText(f"{len(self.attachments)} file(s) attached: " + ", ".join([os.path.basename(p) for p in self.attachments]))

    def attach_qor(self):
        report = find_latest_qor_report()
        if report:
            if report not in self.attachments:
                self.attachments.append(report)
                self.update_attach_lbl()
        else:
            QMessageBox.warning(self, "Not Found", "No QoR summary.html found in the current environment.")

    def browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Attachments", "", "All Files (*)")
        if files:
            for f in files:
                if f not in self.attachments: self.attachments.append(f)
            self.update_attach_lbl()

class ScanSummaryDialog(QDialog):
    def __init__(self, stats, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scan Complete")
        self.resize(500, 450)
        layout = QVBoxLayout(self)

        header = QLabel("<b>Scan Summary</b>")
        font = header.font()
        font.setPointSize(14)
        header.setFont(font)
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

        grid = QFormLayout()
        grid.addRow("<b>Total Workspace Runs Found:</b>", QLabel(str(stats['ws'])))
        grid.addRow("<b>Total Outfeed Runs Found:</b>", QLabel(str(stats['outfeed'])))
        grid.addRow("", QLabel(""))
        grid.addRow("<b>Total FC Runs:</b>", QLabel(str(stats['fc'])))
        grid.addRow("<b>Total Innovus Runs:</b>", QLabel(str(stats['innovus'])))
        layout.addLayout(grid)

        layout.addWidget(QLabel("<b>Runs per Block Breakdown:</b>"))
        table = QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["Block Name", "Number of Runs"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.setRowCount(len(stats['blocks']))
        
        row = 0
        for blk, count in sorted(stats['blocks'].items(), key=lambda x: x[1], reverse=True):
            table.setItem(row, 0, QTableWidgetItem(blk))
            count_item = QTableWidgetItem(str(count))
            count_item.setTextAlignment(Qt.AlignCenter)
            table.setItem(row, 1, count_item)
            row += 1
            
        layout.addWidget(table)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok)
        btn_box.accepted.connect(self.accept)
        layout.addWidget(btn_box)

class EditNoteDialog(QDialog):
    def __init__(self, current_text, identifier_name, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Alias / Personal Note")
        self.resize(400, 250)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<b>Edit Note for:</b><br>{identifier_name}"))
        
        self.text_edit = QTextEdit()
        clean_text = current_text
        user_tag = f"[{getpass.getuser()}]"
        if user_tag in clean_text:
            clean_text = clean_text.split(user_tag)[-1].strip()
        self.text_edit.setPlainText(clean_text)
        layout.addWidget(self.text_edit)
        
        layout.addWidget(QLabel("<i>Notes are visible to all dashboard users.</i>"))
        
        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def get_text(self):
        return self.text_edit.toPlainText().strip()

class FilterDialog(QDialog):
    def __init__(self, col_name, unique_values, active_values, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Filter Column: {col_name}")
        self.resize(320, 420)
        layout = QVBoxLayout(self)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search items...")
        self.search.textChanged.connect(self.filter_list)
        layout.addWidget(self.search)

        self.list_widget = QListWidget()
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()
        sel_all = QPushButton("Select All")
        desel_all = QPushButton("Clear All")
        sel_all.clicked.connect(lambda: self.set_all(True))
        desel_all.clicked.connect(lambda: self.set_all(False))
        btn_layout.addWidget(sel_all)
        btn_layout.addWidget(desel_all)
        layout.addLayout(btn_layout)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        for val in sorted(unique_values):
            item = QListWidgetItem(val)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if val in active_values else Qt.Unchecked)
            self.list_widget.addItem(item)

    def set_all(self, state):
        s = Qt.Checked if state else Qt.Unchecked
        for i in range(self.list_widget.count()):
            if not self.list_widget.item(i).isHidden():
                self.list_widget.item(i).setCheckState(s)

    def filter_list(self, text):
        text = text.lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setHidden(text not in item.text().lower())

    def get_selected(self):
        return [self.list_widget.item(i).text() for i in range(self.list_widget.count())
                if self.list_widget.item(i).checkState() == Qt.Checked]

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Settings")
        self.resize(400, 420)
        layout = QFormLayout(self)
        layout.setSpacing(12)

        self.font_combo = QFontComboBox()
        self.font_combo.setCurrentFont(QApplication.font())
        layout.addRow("Font Family:", self.font_combo)

        self.size_spin = QSpinBox()
        self.size_spin.setRange(8, 24)
        current_size = QApplication.font().pointSize()
        self.size_spin.setValue(current_size if current_size > 0 else 10)
        layout.addRow("Font Size:", self.size_spin)

        self.space_spin = QSpinBox()
        self.space_spin.setRange(0, 20)
        self.space_spin.setValue(parent.row_spacing if parent else 2)
        layout.addRow("Row Spacing (px):", self.space_spin)

        self.rel_time_cb = QCheckBox("Show relative timestamps")
        self.rel_time_cb.setChecked(parent.show_relative_time if parent else False)
        layout.addRow("", self.rel_time_cb)

        self.ist_cb = QCheckBox("Convert Timestamps to IST (from KST)")
        self.ist_cb.setChecked(parent.convert_to_ist if parent else False)
        layout.addRow("", self.ist_cb)
        
        self.hide_blocks_cb = QCheckBox("Hide Block grouping in Tree")
        self.hide_blocks_cb.setChecked(parent.hide_block_nodes if parent else False)
        layout.addRow("", self.hide_blocks_cb)

        self.theme_cb = QCheckBox("Enable Dark Mode")
        self.theme_cb.setChecked(parent.is_dark_mode if parent else False)
        layout.addRow("", self.theme_cb)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setFrameShadow(QFrame.Sunken)
        layout.addRow(sep)

        self.use_custom_cb = QCheckBox("Enable Custom Colors")
        self.use_custom_cb.setChecked(parent.use_custom_colors if parent else False)
        layout.addRow("Theme:", self.use_custom_cb)

        self.bg_color  = parent.custom_bg_color  if parent else "#2b2d30"
        self.fg_color  = parent.custom_fg_color  if parent else "#dfe1e5"
        self.sel_color = parent.custom_sel_color if parent else "#2f65ca"

        self.bg_btn  = QPushButton("Pick Background Color");  self.bg_btn.clicked.connect(self.pick_bg)
        self.fg_btn  = QPushButton("Pick Text Color");        self.fg_btn.clicked.connect(self.pick_fg)
        self.sel_btn = QPushButton("Pick Selection Color");   self.sel_btn.clicked.connect(self.pick_sel)

        layout.addRow("Background:", self.bg_btn)
        layout.addRow("Text:", self.fg_btn)
        layout.addRow("Highlight:", self.sel_btn)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addRow(self.buttons)

    def pick_bg(self):
        c = QColorDialog.getColor(QColor(self.bg_color), self)
        if c.isValid(): self.bg_color = c.name()
    def pick_fg(self):
        c = QColorDialog.getColor(QColor(self.fg_color), self)
        if c.isValid(): self.fg_color = c.name()
    def pick_sel(self):
        c = QColorDialog.getColor(QColor(self.sel_color), self)
        if c.isValid(): self.sel_color = c.name()

class DiskUsageDialog(QDialog):
    def __init__(self, disk_data, is_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Disk Space Usage (Advanced Drill-Down)")
        self.resize(1200, 700)
        self.disk_data = disk_data
        self.is_dark = is_dark
        self.parent_window = parent
        layout = QVBoxLayout(self)
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("<b>Select Directory Scope:</b>"))
        self.combo = QComboBox()
        self.combo.addItems(["WS (FE)", "WS (BE)", "OUTFEED"])
        self.combo.currentIndexChanged.connect(self.update_view)
        top_row.addWidget(self.combo)
        top_row.addSpacing(30)
        self.partition_lbl = QLabel("")
        self.partition_lbl.setStyleSheet("color: #d32f2f; font-weight: bold;" if not is_dark else "color: #e57373; font-weight: bold;")
        top_row.addWidget(self.partition_lbl)
        top_row.addStretch()
        layout.addLayout(top_row)
        main_body = QHBoxLayout()
        self.pie = PieChartWidget()
        main_body.addWidget(self.pie, 1)
        self.tree = QTreeWidget()
        self.tree.setColumnCount(2)
        self.tree.setHeaderLabels(["User / Directory Path", "Size (GB)"])
        self.tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.tree.setAlternatingRowColors(True)
        self.tree.setSelectionMode(QTreeWidget.ExtendedSelection)
        self.tree.itemChanged.connect(self.handle_item_changed)
        main_body.addWidget(self.tree, 2)
        layout.addLayout(main_body, 1)
        
        bottom_row = QHBoxLayout()
        self.recalc_btn = QPushButton("Recalculate Disk Usage")
        self.recalc_btn.setMinimumHeight(35)
        self.recalc_btn.clicked.connect(self.trigger_recalc)
        bottom_row.addWidget(self.recalc_btn)
        
        bottom_row.addStretch()
        
        self.mail_btn = QPushButton("Send Cleanup Mail to Selected")
        self.mail_btn.setMinimumHeight(35)
        self.mail_btn.clicked.connect(self.send_disk_mail)
        btn_color = "#2f65ca" if self.is_dark else "#3182ce"
        self.mail_btn.setStyleSheet(f"QPushButton {{ background-color: {btn_color}; color: white; font-weight: bold; padding: 5px 15px; border-radius: 4px; }}")
        bottom_row.addWidget(self.mail_btn)
        layout.addLayout(bottom_row)
        self.update_view()

    def update_view(self):
        self.tree.blockSignals(True)
        self.tree.clear()
        cat = self.combo.currentText()
        data = self.disk_data.get(cat, {})
        if cat == "WS (FE)": p_path = BASE_WS_FE_DIR
        elif cat == "WS (BE)": p_path = BASE_WS_BE_DIR
        else: p_path = BASE_OUTFEED_DIR
        self.partition_lbl.setText(get_partition_space(p_path))
        pie_data = {user: info["total"] for user, info in data.items()}
        self.pie.set_data(pie_data, self.is_dark)
        sorted_data = sorted(data.items(), key=lambda item: item[1]["total"], reverse=True)
        for i, (user, info) in enumerate(sorted_data):
            user_item = QTreeWidgetItem(self.tree)
            user_item.setFlags(user_item.flags() | Qt.ItemIsUserCheckable)
            user_item.setCheckState(0, Qt.Unchecked)
            user_item.setText(0, user)
            user_item.setText(1, f"{info['total']:.2f} GB")
            color = QColor(self.pie.colors[i % len(self.pie.colors)])
            user_item.setForeground(0, color)
            font = user_item.font(0); font.setBold(True)
            user_item.setFont(0, font); user_item.setFont(1, font)
            for dir_path, dir_sz in info["dirs"]:
                dir_item = QTreeWidgetItem(user_item)
                dir_item.setFlags(dir_item.flags() | Qt.ItemIsUserCheckable)
                dir_item.setCheckState(0, Qt.Unchecked)
                dir_item.setText(0, os.path.basename(dir_path))
                dir_item.setToolTip(0, dir_path)
                dir_item.setText(1, f"{dir_sz:.2f} GB")
                dir_item.setData(0, Qt.UserRole, user)
                dir_item.setData(0, Qt.UserRole + 1, dir_path)
        self.tree.blockSignals(False)

    def trigger_recalc(self):
        if self.parent_window:
            self.recalc_btn.setText("Calculating...")
            self.recalc_btn.setEnabled(False)
            self.parent_window.start_bg_disk_scan(force=True)

    def handle_item_changed(self, item, column):
        if column != 0: return
        self.tree.blockSignals(True)
        state = item.checkState(0)
        for i in range(item.childCount()): item.child(i).setCheckState(0, state)
        self.tree.blockSignals(False)

    def send_disk_mail(self):
        user_runs = {}
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            user_item = root.child(i)
            for j in range(user_item.childCount()):
                dir_item = user_item.child(j)
                if dir_item.checkState(0) == Qt.Checked:
                    owner = dir_item.data(0, Qt.UserRole)
                    path  = dir_item.data(0, Qt.UserRole + 1)
                    if owner not in user_runs: user_runs[owner] = []
                    user_runs[owner].append(path)
        if not user_runs:
            QMessageBox.warning(self, "No Directories Selected", "Please check at least one directory to request cleanup.")
            return
            
        all_known = get_all_known_mail_users()
        dlg = AdvancedMailDialog("Action Required: Please clean up heavy disk usage runs",
                                 "Hi,\n\nPlease remove these runs as they are consuming disk space and are no longer needed:\n\n",
                                 all_known, "", self)
        
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
                for att in dlg.attachments:
                    cmd.extend(["-a", att])
                    
                try:
                    subprocess.Popen(cmd); success_count += 1
                except Exception as e:
                    print(f"Failed to send mail: {e}")
            QMessageBox.information(self, "Mail Sent", f"Successfully triggered {success_count} cleanup emails.")
