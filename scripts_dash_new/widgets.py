import os
import math
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QScrollArea, QPlainTextEdit, QTreeWidgetItem,
                             QLineEdit, QCompleter, QDialog)
from PyQt5.QtCore import Qt, QTimer, QRectF, QStringListModel
from PyQt5.QtGui import QColor, QBrush, QPainter, QPen, QFont

class GanttChartDialog(QDialog):
    def __init__(self, run_name, stages_data, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Timeline: {run_name}")
        self.resize(800, 400)
        layout = QVBoxLayout(self)
        self.scene = QWidget()
        self.scene.setMinimumHeight(max(200, len(stages_data) * 40 + 50))
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.scene)
        layout.addWidget(scroll)
        self.stages_data = stages_data
        self.is_dark = parent.is_dark_mode if parent else False

    def paintEvent(self, event):
        super().paintEvent(event)
        if not self.stages_data: return
        painter = QPainter(self.scene)
        painter.setRenderHint(QPainter.Antialiasing)
        w = self.scene.width() - 40
        x_start = 120
        usable_w = w - x_start
        max_sec = max([d['sec'] for d in self.stages_data if d['sec'] > 0] + [1])
        scale = usable_w / max_sec
        y = 30
        for data in self.stages_data:
            painter.setPen(QPen(Qt.white if self.is_dark else Qt.black))
            painter.drawText(10, y + 15, data['name'])
            bar_w = data['sec'] * scale
            color = QColor("#4CAF50") if data['sec'] > 0 else QColor("#9E9E9E")
            painter.setBrush(QBrush(color))
            painter.setPen(Qt.NoPen)
            painter.drawRect(x_start, y, int(bar_w), 20)
            painter.setPen(QPen(Qt.white if self.is_dark else Qt.black))
            painter.drawText(x_start + int(bar_w) + 10, y + 15, data['time_str'])
            y += 40

class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        
        # --- Golden Run Pin to Top Logic ---
        if col == 0:
            pin1 = self.data(0, Qt.UserRole + 5)
            pin2 = other.data(0, Qt.UserRole + 5)
            asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
            if pin1 == 'golden' and pin2 != 'golden': return asc
            if pin2 == 'golden' and pin1 != 'golden': return not asc

        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""

        if col in [3, 7, 8, 9]:
            def score(val):
                v_up = val.upper()
                if "PASS" in v_up or "ERROR: 0" in v_up or "COMPLETED" in v_up: return 4
                if "RUNNING" in v_up: return 3
                if "FAILS" in v_up or "ERROR:" in v_up or "FATAL" in v_up: return 2
                if "INTERRUPTED" in v_up or "NOT STARTED" in v_up: return 1
                return 0
            s1, s2 = score(t1), score(t2)
            if s1 != s2:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return s1 < s2 if asc else s1 > s2

        if col == 0:
            if t1 == "[ Ignored Runs ]": return False
            if t2 == "[ Ignored Runs ]": return True
            m_order = {"INITIAL RELEASE": 1, "PRE-SVP": 2, "SVP": 3, "FFN": 4}
            if t1 in m_order and t2 in m_order:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return m_order[t1] < m_order[t2] if asc else m_order[t1] > m_order[t2]
        return t1 < t2

class MultiCompleterLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.completer = QCompleter()
        self.completer.setWidget(self)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.activated.connect(self.insertCompletion)
        self.words = []

    def setModel(self, string_list):
        self.words = string_list
        model = QStringListModel(self.words, self.completer)
        self.completer.setModel(model)

    def insertCompletion(self, completion):
        text = self.text()
        parts = text.split(',')
        if len(parts) > 1: text = ','.join(parts[:-1]) + ', ' + completion + ', '
        else: text = completion + ', '
        self.setText(text)

    def keyPressEvent(self, e):
        if self.completer.popup().isVisible():
            if e.key() in (Qt.Key_Enter, Qt.Key_Return):
                e.ignore()
                return
        super().keyPressEvent(e)
        cr = self.cursorRect()
        cr.setWidth(self.completer.popup().sizeHintForColumn(0) + self.completer.popup().verticalScrollBar().sizeHint().width())
        
        text = self.text()
        current_word = text.split(',')[-1].strip()
        
        if current_word:
            self.completer.setCompletionPrefix(current_word)
            if self.completer.completionCount() > 0:
                self.completer.complete(cr)
            else:
                self.completer.popup().hide()
        else:
            self.completer.popup().hide()
