"""On-demand FM/VCLP results and LDRC reports, off the GUI thread."""
import copy
import fnmatch
import os
import re
import subprocess
from PyQt5.QtGui import QColor
from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                            QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox)
from workers import get_outfeed_evt_base, get_dynamic_evt_path


def _unique(paths):
    return list(dict.fromkeys(p for p in paths if p and p != 'N/A'))


def check_specs(run, selected_stage=None):
    """Use the dashboard's canonical event, run and stage layout only."""
    path = run.get('path', '')
    source = run.get('source', '')
    base = (get_outfeed_evt_base(path) if source == 'OUTFEED' else
            get_dynamic_evt_path(run.get('rtl', ''), run.get('block', '')))
    block = run.get('block', '')
    name = run.get('r_name', '')
    checks = []

    def add(stage, label, roots, report_name, pattern=None, log_roots=None):
        roots = _unique(roots)
        checks.append(dict(stage=stage, check=label, roots=roots,
                           report_name=report_name, pattern=pattern,
                           log_roots=_unique(log_roots or roots)))

    if run.get('run_type') == 'FE':
        clean = name.replace('-FE', '').replace('-BE', '')
        for label, mode in (('FM NONUPF', 'r2n'), ('FM UPF', 'r2upf')):
            add('FE', label, [os.path.join(base, 'fm', clean, mode)] if base else [],
                '{}_{}.failpoint.rpt'.format(block, mode), '*.failpoint.rpt')
        roots = [os.path.join(base, 'vslp', clean, 'pre')] if base else []
        log_roots = [os.path.join(base, 'vslp', clean), roots[0]] if roots else []
        add('FE', 'VCLP', roots, 'report_lp.rpt', log_roots=log_roots)
    else:
        stages = [selected_stage] if selected_stage else run.get('stages', [])
        for stage in stages:
            step = stage.get('_fm_step') or stage.get('name', '')
            origin = stage.get('_origin_be_path') or path
            origin_source = stage.get('_origin_source') or stage.get('source') or source
            event = (get_outfeed_evt_base(origin) if origin_source == 'OUTFEED'
                     else stage.get('_fm_base') or base)
            dirs = stage.get('_fm_dirs') or [os.path.basename(origin)]
            for label, mode in (('FM NONUPF', 'n2n_func'), ('FM UPF', 'n2upf_func')):
                roots = [os.path.join(event, 'fm', d, step, mode) for d in dirs] if event else []
                add(stage.get('name', step), label, roots,
                    '{}_{}.failpoint.rpt'.format(block, mode), '*.failpoint.rpt')
            roots = [os.path.join(event, 'fm', d, step, 'pgnet') for d in dirs] if event else []
            add(stage.get('name', step), 'VCLP', roots, 'report_lp.rpt')
    # LDRC is run-level, with no invented stage subdirectory. A selected stage
    # can belong to a different BE origin in the ALL-merged view.
    ldrc_base, ldrc_name = base, name
    if selected_stage and selected_stage.get('_origin_be_path'):
        origin = selected_stage['_origin_be_path']
        ldrc_name = os.path.basename(os.path.normpath(origin))
        if (selected_stage.get('_origin_source') or selected_stage.get('source') or source) == 'OUTFEED':
            ldrc_base = get_outfeed_evt_base(origin)
        else:
            ldrc_base = selected_stage.get('_fm_base') or base
    clean = re.sub(r'-(FE|BE)$', '', ldrc_name)
    add('Run', 'LDRC', [os.path.join(ldrc_base, 'vcspyglass', clean)]
        if ldrc_base and clean else [], 'summary.rpt')
    return checks


def read_check_status(path, check, cancelled=lambda: False):
    """Read conclusive report evidence; an existing report is never a pass."""
    if not path:
        return 'Not available', ''
    in_summary = False
    fm_failures = None
    ldrc_summary = False
    ldrc_table = False
    ldrc_counts = dict(fatal=0, error=0, warning=0, info=0)
    ldrc_rows = 0
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as report:
            for line in report:
                if cancelled():
                    return 'Unknown', ''
                if check == 'LDRC':
                    if re.search(r'^\s*SUMMARY\s+REPORT\s*:', line, re.I):
                        ldrc_summary = True
                    elif ldrc_summary and re.search(r'Severity\s+Rule\s+Name\s+Count', line, re.I):
                        ldrc_table = True
                    elif ldrc_table:
                        match = re.match(r'^\s*(fatal|error|warning|info)\s+\S+\s+([\d,]+)\b',
                                         line.replace('|', ' '), re.I)
                        if match:
                            ldrc_counts[match.group(1).lower()] += int(match.group(2).replace(',', ''))
                            ldrc_rows += 1
                elif check.startswith('FM'):
                    if re.search(r'No\s+failing\s+compare\s+points?', line, re.I):
                        fm_failures = max(fm_failures or 0, 0)
                    match = re.search(r'([\d,]+)\s+Failing\s+compare\s+points?', line, re.I)
                    if match:
                        fm_failures = max(fm_failures or 0, int(match.group(1).replace(',', '')))
                else:
                    if 'management summary' in line.lower():
                        in_summary = True
                    elif in_summary:
                        match = re.match(r'^\s*Total\s+([\d,]+)\s+([\d,]+)(?:\s|$)',
                                         line.replace('|', ' '), re.I)
                        if match:
                            errors, warnings = [int(n.replace(',', '')) for n in match.groups()]
                            return '{} - Error: {}, Warning: {}'.format(
                                'FAIL' if errors else 'PASS', errors, warnings), ''
    except (OSError, ValueError) as exc:
        return 'Unavailable', '{}: {}'.format(path, exc)
    if check == 'LDRC' and ldrc_rows:
        text = 'Error: {error}  Warning: {warning}  Info: {info}'.format(**ldrc_counts)
        if ldrc_counts['fatal']:
            text += '\nFatal: {}'.format(ldrc_counts['fatal'])
        return text, ''
    if fm_failures is not None:
        return ('{} FAILS'.format(fm_failures) if fm_failures else 'PASS'), ''
    return 'Unknown', ''


def resolve_check(spec, cancelled=lambda: False):
    """A report proves an export; a log alone never proves an export."""
    errors = []
    def files(directory, pattern):
        found = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if cancelled():
                        return []
                    if fnmatch.fnmatchcase(entry.name, pattern) and entry.is_file():
                        found.append((entry.stat().st_mtime, entry.path))
        except FileNotFoundError:
            pass
        except OSError as exc:
            errors.append('{}: {}'.format(directory, exc))
        return sorted(found, reverse=True)

    reports = []
    for root in spec['roots']:
        directory = os.path.join(root, 'reports')
        hits = files(directory, spec['report_name'])
        if not hits and spec.get('pattern'):
            hits = files(directory, spec['pattern'])
        if hits:
            reports = hits
            break
        if cancelled():
            return None
    report_errors = list(errors)
    logs = []
    for root in spec['log_roots']:
        if spec['check'].startswith('FM'):
            logs.extend(files(os.path.join(root, 'logs'), '*.log'))
        else:
            # Only run.log in the VSLP directory, never a generic log fallback.
            logs.extend(files(root, 'run.log'))
        if cancelled():
            return None
    logs.sort(reverse=True)
    status, status_error = read_check_status(reports[0][1] if reports else '', spec['check'], cancelled)
    if cancelled():
        return None
    if status_error:
        errors.append(status_error)
    if not reports and report_errors:
        status = 'Unavailable'
    return dict(stage=spec['stage'], check=spec['check'],
                status=status,
                exported=('Exported' if reports else 'Unavailable' if report_errors else
                          'Path unavailable' if not spec['roots'] else 'Not exported'),
                report=reports[0][1] if reports else '',
                log=logs[0][1] if logs else '', errors='\n'.join(errors),
                roots='\n'.join(spec['roots']))


class SignoffExportWorker(QThread):
    row_ready = pyqtSignal(int, dict)
    prepared = pyqtSignal(list)
    failed = pyqtSignal(str)

    def __init__(self, run, stage=None):
        super().__init__()
        self.run_data, self.stage = copy.deepcopy(run), copy.deepcopy(stage)

    def cancel(self):
        self.requestInterruption()

    def run(self):
        try:
            specs = check_specs(self.run_data, self.stage)
            self.prepared.emit(specs)
            for index, spec in enumerate(specs):
                if self.isInterruptionRequested():
                    break
                result = resolve_check(spec, self.isInterruptionRequested)
                if result is not None:
                    self.row_ready.emit(index, result)
        except Exception as exc:
            self.failed.emit(str(exc))


class SignoffCheckDialog(QDialog):
    def __init__(self, run, stage, parent):
        super().__init__(parent)
        self.run_data, self.stage = copy.deepcopy(run), copy.deepcopy(stage)
        self._worker = None
        self._closing = False
        self.setWindowTitle('Signoff Check Status: ' + run.get('r_name', ''))
        self.setWindowFlags(Qt.Window | Qt.WindowMinMaxButtonsHint | Qt.WindowCloseButtonHint)
        self.setWindowModality(Qt.NonModal)
        self.resize(980, 500)
        layout = QVBoxLayout(self)
        header = QLabel('{} | {} | {}'.format(run.get('r_name', ''), run.get('source', ''), run.get('rtl', '')))
        header.setTextFormat(Qt.PlainText)
        layout.addWidget(header)
        header.setWordWrap(True)
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(['Stage', 'Check', 'Report export', 'Status', 'Report', 'Log'])
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.horizontalHeader().setMinimumSectionSize(90)
        self.table.verticalHeader().hide()
        layout.addWidget(self.table)
        controls = QHBoxLayout()
        self.message = QLabel('')
        controls.addWidget(self.message, 1)
        self.refresh_button = QPushButton('Refresh')
        self.refresh_button.clicked.connect(self.refresh)
        controls.addWidget(self.refresh_button)
        close = QPushButton('Close')
        close.clicked.connect(self.close)
        controls.addWidget(close)
        layout.addLayout(controls)
        QTimer.singleShot(0, self.refresh)

    def refresh(self):
        if self._closing or self._worker_is_running():
            return
        if getattr(self.parent(), '_closing_wait_for_workers', False):
            return
        self.table.setRowCount(0)
        self.refresh_button.setEnabled(False)
        self.message.setText('Checking report and log files...')
        worker = SignoffExportWorker(self.run_data, self.stage)
        self._worker = worker
        worker.prepared.connect(self._prepared)
        worker.row_ready.connect(self._row)
        worker.failed.connect(self._failed)
        worker.finished.connect(self._finished)
        try:
            self.parent()._workers.start('signoff_export', worker)
        except Exception as exc:
            self.message.setText('Check failed: ' + str(exc))
            if self._worker_is_running():
                worker.cancel()
            else:
                self._worker = None
                self.refresh_button.setEnabled(True)
                try:
                    worker.deleteLater()
                except RuntimeError:
                    pass

    def _worker_is_running(self):
        worker = self._worker
        if worker is None:
            return False
        try:
            return worker.isRunning()
        except RuntimeError:
            # The dashboard registry owns/deletes QThreads. A Python wrapper
            # can survive its C++ object; never call it again after this point.
            self._worker = None
            return False

    def _accept_worker_result(self):
        return (not self._closing and self._worker is not None
                and self.sender() is self._worker)

    def _prepared(self, specs):
        if not self._accept_worker_result():
            return
        self.table.setRowCount(len(specs))
        for index, spec in enumerate(specs):
            for col, text in enumerate((spec['stage'], spec['check'], 'Checking...', 'Checking...')):
                self.table.setItem(index, col, QTableWidgetItem(text))

    def _row(self, index, result):
        if not self._accept_worker_result():
            return
        cell = QTableWidgetItem(result['exported'])
        cell.setToolTip(result['errors'] or result['roots'])
        self.table.setItem(index, 2, cell)
        status = QTableWidgetItem(result['status'])
        status.setToolTip(result['errors'] or result['status'])
        if 'FAIL' in result['status']:
            status.setForeground(QColor('#d32f2f'))
        elif result['status'].startswith('PASS'):
            status.setForeground(QColor('#388e3c'))
        elif result['check'] == 'LDRC' and result['status'].startswith('Error:'):
            failed = re.search(r'(?:Error|Fatal):\s*[1-9]\d*', result['status'])
            status.setForeground(QColor('#d32f2f' if failed else '#388e3c'))
        self.table.setItem(index, 3, status)
        for column, key, title in ((4, 'report', 'Open report'), (5, 'log', 'Open log')):
            button = QPushButton(title if result[key] else 'Not found')
            button.setEnabled(bool(result[key]))
            button.setToolTip(result[key] or result['errors'] or 'No file found')
            button.clicked.connect(lambda checked=False, path=result[key]: self._open(path))
            self.table.setCellWidget(index, column, button)
        self.table.resizeRowToContents(index)

    def _failed(self, error):
        if self._accept_worker_result():
            self.message.setText('Check failed: ' + error)

    def _finished(self):
        if self.sender() is not self._worker:
            return
        # Clear the dialog reference at native QThread completion, before the
        # dashboard registry processes deleteLater(). Do not own deletion here.
        self._worker = None
        if self._closing:
            self.deleteLater()
            return
        self.refresh_button.setEnabled(True)
        if not self.message.text().startswith('Check failed:'):
            self.message.setText('Checks updated.')

    def _open(self, path):
        if path:
            try:
                subprocess.Popen(['gvim', path])
            except OSError as exc:
                QMessageBox.warning(self, 'Open file', str(exc))

    def accept(self):
        self.close()

    def reject(self):
        # Escape calls reject(), bypassing closeEvent unless routed here.
        self.close()

    def closeEvent(self, event):
        self._closing = True
        if self._worker_is_running():
            self._worker.cancel()
        else:
            self._worker = None
            self.deleteLater()
        event.accept()
