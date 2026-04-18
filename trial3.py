config.py

import os
import configparser
import threading
import json
import getpass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "project_config.ini")
MAIL_USERS_FILE = os.path.join(SCRIPT_DIR, "mail_users.ini")
NOTES_DIR = os.path.join(SCRIPT_DIR, "dashboard_notes")
USER_PREFS_FILE = os.path.join(SCRIPT_DIR, "user_prefs.ini")

if not os.path.exists(NOTES_DIR):
    try: os.makedirs(NOTES_DIR)
    except: pass

config = configparser.ConfigParser()
DEFAULT_CONFIG = {
    'PROJECT': {
        'PROJECT_PREFIX': 'S5K2P5SP',
        'BASE_WS_FE_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/WS',
        'BASE_WS_BE_DIR': '/user/s5k2p5sp.be1/s5k2p5sp/WS',
        'BASE_OUTFEED_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/outfeed',
        'BASE_IR_DIR': '/user/s5k2p5sx.be1/LAYOUT/IR/ /user/s5k2p5sx.be1/LAYOUT/IR2/'
    },
    'TOOLS': {
        'PNR_TOOL_NAMES': 'fc innovus',
        'SUMMARY_SCRIPT': '/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py',
        'FIREFOX_PATH': '/usr/bin/firefox',
        'MAIL_UTIL': '/user/vwpmailsystem/MAIL/send_mail_for_rhel7',
        'USER_INFO_UTIL': '/usr/local/bin/user_info'
    }
}

if not os.path.exists(CONFIG_FILE):
    config.read_dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_FILE, 'w') as f:
            config.write(f)
    except: pass
else:
    config.read(CONFIG_FILE)

# Map Global Variables
PROJECT_PREFIX   = config.get('PROJECT', 'PROJECT_PREFIX', fallback='S5K2P5SP')
BASE_WS_FE_DIR   = config.get('PROJECT', 'BASE_WS_FE_DIR', fallback='')
BASE_WS_BE_DIR   = config.get('PROJECT', 'BASE_WS_BE_DIR', fallback='')
BASE_OUTFEED_DIR = config.get('PROJECT', 'BASE_OUTFEED_DIR', fallback='')
BASE_IR_DIR      = config.get('PROJECT', 'BASE_IR_DIR', fallback='')

PNR_TOOL_NAMES   = config.get('TOOLS', 'PNR_TOOL_NAMES', fallback='fc innovus')
SUMMARY_SCRIPT   = config.get('TOOLS', 'SUMMARY_SCRIPT', fallback='')
FIREFOX_PATH     = config.get('TOOLS', 'FIREFOX_PATH', fallback='/usr/bin/firefox')
MAIL_UTIL        = config.get('TOOLS', 'MAIL_UTIL', fallback='')
USER_INFO_UTIL   = config.get('TOOLS', 'USER_INFO_UTIL', fallback='')

prefs = configparser.ConfigParser()
if os.path.exists(USER_PREFS_FILE): prefs.read(USER_PREFS_FILE)

mail_config = configparser.ConfigParser()
DEFAULT_MAIL_CONFIG = {
    'PERMANENT_MEMBERS': {'always_to': '', 'always_cc': 'mohit.bhar'},
    'KNOWN_USERS': {'users': ''}
}
if not os.path.exists(MAIL_USERS_FILE):
    mail_config.read_dict(DEFAULT_MAIL_CONFIG)
    try:
        with open(MAIL_USERS_FILE, 'w') as f:
            mail_config.write(f)
    except: pass
else:
    mail_config.read(MAIL_USERS_FILE)

# Thread-safe cache
_path_cache = {}
_path_cache_lock = threading.Lock()

def cached_exists(path):
    with _path_cache_lock:
        if path in _path_cache: return _path_cache[path]
    result = os.path.exists(path)
    with _path_cache_lock: _path_cache[path] = result
    return result

def clear_path_cache():
    with _path_cache_lock: _path_cache.clear()

def prefetch_path_cache(paths):
    import concurrent.futures
    unique_paths = [p for p in set(paths) if p]
    if not unique_paths: return
    max_w = min(30, len(unique_paths))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
        results = list(ex.map(os.path.exists, unique_paths))
    with _path_cache_lock:
        for path, exists in zip(unique_paths, results):
            _path_cache[path] = exists



utils.py

import os
import re
import pwd
import glob
import json
import shutil
import getpass
import datetime
import subprocess
from config import *

def convert_kst_to_ist_str(time_str):
    if not time_str or time_str == "N/A": return time_str
    formats = ["%a %b %d, %Y - %H:%M:%S", "%b %d, %Y - %H:%M", "%b %d, %Y - %H:%M:%S"]
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(time_str, fmt)
            dt_ist = dt - datetime.timedelta(hours=3, minutes=30)
            return dt_ist.strftime(fmt)
        except ValueError: continue
    return time_str

def relative_time(date_str):
    if not date_str or date_str == "N/A": return date_str
    try:
        m = re.search(r'(\w{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{2}):(\d{2})', str(date_str))
        if not m: return date_str
        month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        mon, day, year, hour, minute = m.groups()
        dt = datetime.datetime(int(year), month_map.get(mon, 1), int(day), int(hour), int(minute))
        delta = datetime.datetime.now() - dt
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0: return date_str
        if total_seconds < 3600: return f"{total_seconds // 60}m ago"
        if total_seconds < 86400: return f"{total_seconds // 3600}h {(total_seconds % 3600) // 60}m ago"
        return f"{total_seconds // 86400}d ago"
    except: return date_str

def get_user_email(username):
    username = username.strip()
    if not username or username == "Unknown": return ""
    if "@" in username: return username
    try:
        res = subprocess.check_output([USER_INFO_UTIL, '-a', username], stderr=subprocess.DEVNULL).decode('utf-8')
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', res)
        if match: return match.group(0)
    except: pass
    return f"{username}@samsung.com"

def get_partition_space(path_str):
    try:
        total, used, free = shutil.disk_usage(path_str)
        t_gb = total / (1024**3); u_gb = used / (1024**3); f_gb = free / (1024**3)
        perc = (used / total) * 100 if total > 0 else 0
        return f"Total: {t_gb:.1f} GB | Used: {u_gb:.1f} GB ({perc:.1f}%) | Free: {f_gb:.1f} GB"
    except: return "Partition Space Information Unavailable"

def get_owner(path):
    if not path or not cached_exists(path): return "Unknown"
    try: return pwd.getpwuid(os.stat(path).st_uid).pw_name
    except: return "Unknown"

def normalize_rtl(rtl_str):
    if rtl_str and rtl_str.startswith("EVT"): return f"{PROJECT_PREFIX}_{rtl_str}"
    return rtl_str

def get_milestone(rtl_str):
    if "_ML1_" in rtl_str: return "INITIAL RELEASE"
    if "_ML2_" in rtl_str: return "PRE-SVP"
    if "_ML3_" in rtl_str: return "SVP"
    if "_ML4_" in rtl_str: return "FFN"
    return None

def format_log_date(date_str):
    match = re.search(r'([A-Z][a-z]{2})\s+([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})', str(date_str))
    if match: return f"{match.group(1)} {match.group(2)} {match.group(3)}, {match.group(5)} - {match.group(4)}"
    return str(date_str).strip()

def get_dynamic_evt_path(rtl_tag, block_name):
    match = re.search(r'(EVT\d+_ML\d+_DEV\d+)', str(rtl_tag))
    if not match: return ""
    return os.path.join(BASE_OUTFEED_DIR, block_name, match.group(1))

def get_fm_info(report_path):
    if not report_path or not cached_exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            for line in f:
                if "No failing compare points" in line: return "PASS"
                m = re.search(r'(\d+)\s+Failing compare points', line)
                if m: return f"{m.group(1)} FAILS"
    except: pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not cached_exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            in_summary = False
            for line in f:
                if "Management Summary" in line: in_summary = True; continue
                if in_summary and line.strip().startswith("Total"):
                    parts = line.strip().split()
                    if len(parts) >= 3: return f"Error: {parts[1]}, Warning: {parts[2]}"
                    break
    except: pass
    return "Not Found"

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not cached_exists(file_path): return d
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if "TOTAL_START" in line and "Load :" in line:
                    d["start"] = format_log_date(line.split("Load :")[-1].strip())
                m = re.search(r'TimeStamp\s*:\s*(\S+)', line)
                if m and m.group(1) not in ["TOTAL", "TOTAL_START"]: d["last_stage"] = m.group(1)
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(r'Total\s*:\s*(\d+)h:(\d+)m:(\d+)s', line)
                    if rt: d["runtime"] = f"{int(rt.group(1)):02}h:{int(rt.group(2)):02}m:{int(rt.group(3)):02}s"
                    if "Load :" in line: d["end"] = format_log_date(line.split("Load :")[-1].strip())
    except: pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not cached_exists(file_path): return d
    try:
        first_ts, last_ts, final_time_str = None, None, None
        months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    ts_match = re.search(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})', line)
                    time_matches = re.findall(r'(\d+)d:(\d+)h:(\d+)m:(\d+)s', line)
                    if ts_match and time_matches:
                        if not first_ts: first_ts = ts_match
                        last_ts = ts_match
                        target_match = time_matches[1] if len(time_matches) > 1 else time_matches[0]
                        days, hours, mins, secs = map(int, target_match)
                        final_time_str = f"{days*24+hours:02}h:{mins:02}m:{secs:02}s"
                        if len(parts) > 1 and not parts[1].isdigit(): d["last_stage"] = parts[1]
        if first_ts:
            y, mo, day, H, M = first_ts.groups()
            d["start"] = f"{months[int(mo)-1]} {int(day):02d}, {y} - {H}:{M}"
        if last_ts:
            y, mo, day, H, M = last_ts.groups()
            d["end"] = f"{months[int(mo)-1]} {int(day):02d}, {y} - {H}:{M}"
        if final_time_str: d["runtime"] = final_time_str
    except: pass
    return d

def extract_rtl(run_dir):
    f = glob.glob(os.path.join(run_dir, "reports", "dump_variables.user_defined.*.rpt"))
    if not f: return "Unknown"
    try:
        with open(f[0], 'r') as file:
            for line in file:
                m = re.search(r'^\s*all\s*=\s*"(.*?)"', line)
                if m: return normalize_rtl(m.group(1))
    except: pass
    return "Unknown"

def find_latest_qor_report():
    h = glob.glob(os.path.join(os.getcwd(), "qor_metrices/**/summary.html"), recursive=True)
    if h: return sorted(h, key=os.path.getmtime)[-1]
    return None

def save_mail_users_config(new_users):
    try:
        mail_config.read(MAIL_USERS_FILE)
        existing_str = mail_config.get('KNOWN_USERS', 'users', fallback='')
        existing = set([u.strip() for u in existing_str.split(',') if u.strip()])
        existing.update(new_users)
        if not mail_config.has_section('KNOWN_USERS'): mail_config.add_section('KNOWN_USERS')
        mail_config.set('KNOWN_USERS', 'users', ', '.join(sorted(existing)))
        with open(MAIL_USERS_FILE, 'w') as f:
            mail_config.write(f)
    except: pass

def get_all_known_mail_users():
    try:
        mail_config.read(MAIL_USERS_FILE)
        existing_str = mail_config.get('KNOWN_USERS', 'users', fallback='')
        return sorted(list(set([u.strip() for u in existing_str.split(',') if u.strip()])))
    except: return []

def load_all_notes():
    global_notes = {}
    if not os.path.exists(NOTES_DIR): return global_notes
    for file in os.listdir(NOTES_DIR):
        if file.endswith(".json"):
            try:
                with open(os.path.join(NOTES_DIR, file), 'r') as f:
                    data = json.load(f)
                    for key, val in data.items():
                        if key not in global_notes: global_notes[key] = []
                        global_notes[key].append(val)
            except: pass
    return global_notes

def save_user_note(identifier, note_text):
    current_user = getpass.getuser()
    user_file = os.path.join(NOTES_DIR, f"notes_{current_user}.json")
    user_data = {}
    if os.path.exists(user_file):
        try:
            with open(user_file, 'r') as f:
                user_data = json.load(f)
        except: pass
    
    if note_text.strip():
        user_data[identifier] = f"[{current_user}] {note_text.strip()}"
    else:
        if identifier in user_data: del user_data[identifier]
        
    try:
        with open(user_file, 'w') as f:
            json.dump(user_data, f, indent=4)
    except Exception as e:
        print(f"Failed to save note: {e}")



workers.py


import os
import glob
import re
import subprocess
import pwd
import concurrent.futures
from PyQt5.QtCore import QThread, pyqtSignal
from config import *
from utils import *

class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self._is_cancelled = False

    def run(self):
        max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self.get_size, path): item_id for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled: break
                item_id = futures[future]
                try: self.size_calculated.emit(item_id, future.result())
                except: self.size_calculated.emit(item_id, "N/A")

    def get_size(self, path):
        if not path or not os.path.exists(path): return "N/A"
        total_size = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total_size += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total_size += self._calc_dir(entry.path)
        except: return "N/A"
        
        if total_size == 0: return "N/A"
        for unit in ['K', 'M', 'G']:
            total_size /= 1024.0
            if total_size < 1024.0: return f"{total_size:.1f}{unit}"
        return f"{total_size:.1f}T"
        
    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False): total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False): total += self._calc_dir(entry.path)
        except: pass
        return total

    def cancel(self): self._is_cancelled = True


class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)

    def __init__(self, item, path):
        super().__init__()
        self.item = item; self.path = path; self._is_cancelled = False

    def run(self):
        if self._is_cancelled or not self.path or not os.path.exists(self.path):
            self.result.emit(self.item, "N/A"); return
        
        total_size = 0
        try:
            for entry in os.scandir(self.path):
                if entry.is_file(follow_symlinks=False): total_size += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False): total_size += self._calc_dir(entry.path)
            
            if total_size == 0: 
                self.result.emit(self.item, "N/A")
                return
                
            for unit in ['K', 'M', 'G']:
                total_size /= 1024.0
                if total_size < 1024.0:
                    self.result.emit(self.item, f"{total_size:.1f}{unit}")
                    return
            self.result.emit(self.item, f"{total_size:.1f}T")
        except:
            if not self._is_cancelled: self.result.emit(self.item, "N/A")
            
    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False): total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False): total += self._calc_dir(entry.path)
        except: pass
        return total

    def cancel(self): self._is_cancelled = True


class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def _get_batch_dir_info(self, paths):
        results = []
        if not paths: return results
        try:
            cmd = ['du', '-sk'] + paths
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode('utf-8')
            for line in output.strip().split('\n'):
                if not line: continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    sz_kb = int(parts[0])
                    full_path = parts[1]
                    try: owner = pwd.getpwuid(os.stat(full_path).st_uid).pw_name
                    except: owner = "Unknown"
                    results.append((owner, sz_kb, full_path))
        except: pass
        return results

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}
        outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "fc", "*"))
        outfeed_targets.extend(glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "innovus", "*")))
        if not outfeed_targets:
            outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*"))

        targets_map = {
            "WS (FE)":  glob.glob(os.path.join(BASE_WS_FE_DIR, "*")),
            "WS (BE)":  glob.glob(os.path.join(BASE_WS_BE_DIR, "*")),
            "OUTFEED":  outfeed_targets
        }

        tasks = []
        for cat, paths in targets_map.items():
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                chunk = valid_paths[i:i+50]
                tasks.append((cat, chunk))

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                cat = future_to_cat[future]
                try:
                    batch_results = future.result()
                    for owner, sz_kb, full_path in batch_results:
                        if sz_kb > 0:
                            gb_sz = sz_kb / (1024**2)
                            if gb_sz > 0.01:
                                if owner not in results[cat]: results[cat][owner] = {"total": 0, "dirs": []}
                                results[cat][owner]["total"] += gb_sz
                                results[cat][owner]["dirs"].append((full_path, gb_sz))
                except: pass

        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)

        self.finished_scan.emit(results)


class ScannerWorker(QThread):
    finished        = pyqtSignal(dict, dict, dict, dict)
    progress_update = pyqtSignal(int, int)
    status_update   = pyqtSignal(str)

    def scan_ir_dir(self):
        ir_data = {}
        target_lef = f"{PROJECT_PREFIX}.lef.list"
        ir_dirs = BASE_IR_DIR.split()

        for ir_base in ir_dirs:
            if not os.path.exists(ir_base): continue
            for root_dir, dirs, files in os.walk(ir_base):
                for f_name in files:
                    if not f_name.startswith("redhawk.log"): continue
                    log_path = os.path.join(root_dir, f_name)
                    
                    run_be_name = step_name = None
                    static_val = dynamic_val = "-"
                    in_static = in_dynamic = False
                    static_lines, dynamic_lines = [], []
                    
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m: run_be_name = m.group(1); step_name = m.group(2)

                                if "Worst Static IR Drop:" in line:
                                    in_static = True; in_dynamic = False
                                    static_lines.append(line.rstrip())
                                    continue
                                if "Worst Dynamic Voltage Drop:" in line:
                                    in_dynamic = True; in_static = False
                                    dynamic_lines.append(line.rstrip())
                                    continue

                                if in_static:
                                    if line.startswith("****") or line.startswith("Finish"):
                                        in_static = False
                                    elif line.strip():
                                        static_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and static_val == "-":
                                                static_val = parts[1]

                                if in_dynamic:
                                    if line.startswith("****") or line.startswith("Finish"):
                                        in_dynamic = False
                                    elif line.strip():
                                        dynamic_lines.append(line.rstrip())
                                        if not line.startswith("-") and not line.startswith("Type"):
                                            parts = line.split()
                                            if len(parts) >= 2 and parts[0] != "WIRE" and dynamic_val == "-":
                                                dynamic_val = parts[1]

                        if run_be_name and step_name:
                            key = f"{run_be_name}/{step_name}"
                            if key not in ir_data:
                                ir_data[key] = {"static": "-", "dynamic": "-", "log": log_path, "static_table": "", "dynamic_table": ""}
                            if static_val != "-": ir_data[key]["static"] = static_val
                            if dynamic_val != "-": ir_data[key]["dynamic"] = dynamic_val
                            if static_lines: ir_data[key]["static_table"] = "\n".join(static_lines)
                            if dynamic_lines: ir_data[key]["dynamic_table"] = "\n".join(dynamic_lines)
                    except: pass
        return ir_data

    def _scan_single_workspace(self, ws_base, ws_name, tools_to_scan):
        tasks = []
        releases_found = {}
        ws_path = os.path.join(ws_base, ws_name)
        if not os.path.isdir(ws_path): return tasks, releases_found

        current_rtl = "Unknown"
        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            try:
                with open(sf, 'r') as f:
                    lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                    for l in set(lbls):
                        current_rtl = normalize_rtl(l)
                        if current_rtl not in releases_found: releases_found[current_rtl] = []
                        releases_found[current_rtl].append(ws_path)
            except: pass

        for ent_path in glob.glob(os.path.join(ws_path, "IMPLEMENTATION", "*", "SOC", "*")):
            ent_name = os.path.basename(ent_path)
            if ws_base == BASE_WS_FE_DIR:
                for rd in glob.glob(os.path.join(ent_path, "fc", "*-FE")):
                    tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "FE", None))
            if "fc" in tools_to_scan:
                for pat in ["*-BE", "EVT*_ML*_DEV*_*_*-BE"]:
                    for rd in glob.glob(os.path.join(ent_path, "fc", pat)):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))
            if "innovus" in tools_to_scan:
                for rd in glob.glob(os.path.join(ent_path, "innovus", "EVT*_ML*_DEV*_*")):
                    if os.path.isdir(rd):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

        return tasks, releases_found

    def run(self):
        clear_path_cache()
        self.status_update.emit("Discovering Workspaces...")
        
        ws_data  = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data = {"releases": {}, "blocks": set(), "all_runs": []}
        scan_stats = {'ws': 0, 'outfeed': 0, 'blocks': {}, 'fc': 0, 'innovus': 0}
        
        tasks = []
        tools_to_scan = PNR_TOOL_NAMES.split()

        disc_futures = []
        disc_max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=disc_max_w) as disc_ex:
            for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
                if not os.path.exists(ws_base): continue
                try: ws_names = os.listdir(ws_base)
                except: continue
                for ws_name in ws_names:
                    disc_futures.append(
                        (disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan))
                    )

            for future in concurrent.futures.as_completed(disc_futures):
                try:
                    new_tasks, new_releases = future.result()
                    tasks.extend(new_tasks)
                    for rtl, paths in new_releases.items():
                        for p in paths: self._map_release(ws_data, rtl, p)
                except: pass

        self.status_update.emit("Discovering OUTFEED directories...")
        if os.path.exists(BASE_OUTFEED_DIR):
            for ent_name in os.listdir(BASE_OUTFEED_DIR):
                ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
                if not os.path.isdir(ent_path): continue
                for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                    phys_evt = os.path.basename(evt_dir)
                    for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                        tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                    if "fc" in tools_to_scan:
                        be_runs = (glob.glob(os.path.join(evt_dir, "fc", "*-BE")) +
                                   glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")))
                        for rd in be_runs:
                            tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))
                    if "innovus" in tools_to_scan:
                        for rd in glob.glob(os.path.join(evt_dir, "innovus", "*")):
                            if os.path.isdir(rd):
                                tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

        paths_to_prefetch = []
        for t in tasks:
            rd = t[1]
            paths_to_prefetch.append(os.path.join(rd, "pass/compile_opt.pass"))
            paths_to_prefetch.append(os.path.join(rd, "logs/compile_opt.log"))
            paths_to_prefetch.append(os.path.join(rd, "reports/runtime.V2.rpt"))
            
        self.status_update.emit("Prefetching file metadata...")
        prefetch_path_cache(paths_to_prefetch)

        total_tasks = len(tasks)
        completed_tasks = 0
        max_w = min(40, (os.cpu_count() or 4) * 6)

        self.status_update.emit("Processing run data and parsing reports...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            ir_future = executor.submit(self.scan_ir_dir)
            future_to_task = {executor.submit(self._thread_process_run, t): t for t in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        if result["source"] == "WS":
                            ws_data["blocks"].add(result["block"])
                            ws_data["all_runs"].append(result)
                            scan_stats['ws'] += 1
                            if result["run_type"] == "BE":
                                self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            scan_stats['outfeed'] += 1
                            self._map_release(out_data, result["rtl"], result["path"])
                            
                        blk = result["block"]
                        if blk not in scan_stats['blocks']: scan_stats['blocks'][blk] = 0
                        scan_stats['blocks'][blk] += 1
                        
                        if "/fc/" in result["path"]: scan_stats['fc'] += 1
                        elif "/innovus/" in result["path"]: scan_stats['innovus'] += 1
                            
                except Exception: pass
                completed_tasks += 1
                self.progress_update.emit(completed_tasks, total_tasks)
                if completed_tasks % 20 == 0:
                    self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")

            ir_data = ir_future.result()

        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

    def _thread_process_run(self, task_tuple):
        b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
        if source == "OUTFEED": rtl = self._resolve_outfeed_rtl(rd, phys_evt)
        else:
            rtl = extract_rtl(rd) if run_type == "BE" else base_rtl
            if rtl == "Unknown": rtl = base_rtl
        return self._process_run(b_name, rd, parent_path, rtl, source, run_type)

    def _resolve_outfeed_rtl(self, rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl): rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif rtl == "Unknown": rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    def _process_run(self, b_name, rd, parent_path, rtl, source, run_type):
        r_name       = os.path.basename(rd)
        clean_run    = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)
        evt_base     = get_dynamic_evt_path(rtl, b_name)
        owner        = get_owner(rd)

        fm_n     = os.path.join(evt_base, "fm", clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")
        info     = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))

        is_comp   = True if source == "OUTFEED" else cached_exists(os.path.join(rd, "pass/compile_opt.pass"))
        fe_status = "RUNNING"

        if run_type == "FE":
            if is_comp:
                fe_status = "COMPLETED"
            else:
                log_file = os.path.join(rd, "logs/compile_opt.log")
                if not cached_exists(log_file):
                    fe_status = "NOT STARTED"
                else:
                    fe_status = "RUNNING"
                    try:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as lf:
                            for line in lf:
                                if "Stack trace for crashing thread" in line:
                                    fe_status = "FATAL ERROR"; break
                                if "Information: Process terminated by interrupt. (INT-4)" in line:
                                    fe_status = "INTERRUPTED"; break
                    except: pass

        stages = []
        if run_type == "BE":
            search_glob = os.path.join(rd, "outputs", "*") if source == "WS" else os.path.join(rd, "*")
            for s_dir in glob.glob(search_glob):
                if not os.path.isdir(s_dir): continue
                step_name = os.path.basename(s_dir)
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]: continue

                if source == "WS":
                    rpt = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt")
                    log = os.path.join(rd, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, "outputs", step_name)
                else:
                    rpt = os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                    log = os.path.join(s_dir, "logs", f"{step_name}.log")
                    stage_path = os.path.join(rd, step_name)

                fm_u_glob    = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                fm_n_glob    = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                st_vslp_rpt  = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                sta_rpt      = os.path.join(evt_base, "pt",   r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                qor_path     = rd if rd.endswith("/") else rd + "/"

                stages.append({
                    "name": step_name, "rpt": rpt, "log": log,
                    "info": parse_pnr_runtime_rpt(rpt),
                    "st_n": get_fm_info(st_fm_n_path), "st_u": get_fm_info(st_fm_u_path),
                    "vslp_status": get_vslp_info(st_vslp_rpt),
                    "fm_u_path": st_fm_u_path, "fm_n_path": st_fm_n_path,
                    "vslp_rpt_path": st_vslp_rpt, "sta_rpt_path": sta_rpt,
                    "qor_path": qor_path, "stage_path": stage_path
                })

        return {
            "block": b_name, "path": rd, "parent": parent_path, "rtl": rtl,
            "r_name": r_name, "run_type": run_type, "stages": stages,
            "source": source, "owner": owner, "is_comp": is_comp, "fe_status": fe_status,
            "st_n": get_fm_info(fm_n), "st_u": get_fm_info(fm_u),
            "vslp_status": get_vslp_info(vslp_rpt),
            "info": info, "fm_n_path": fm_n, "fm_u_path": fm_u, "vslp_rpt_path": vslp_rpt
        }

    def _map_release(self, data_obj, rtl_str, path):
        if rtl_str not in data_obj["releases"]: data_obj["releases"][rtl_str] = []
        if path not in data_obj["releases"][rtl_str]: data_obj["releases"][rtl_str].append(path)
        base = re.sub(r'_syn\d+$', '', rtl_str)
        if base != rtl_str:
            if base not in data_obj["releases"]: data_obj["releases"][base] = []
            if path not in data_obj["releases"][base]: data_obj["releases"][base].append(path)



widgets.py



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

class MiniPieChart(QWidget):
    def __init__(self):
        super().__init__()
        self.setMinimumSize(200, 150)
        self.data = {'Passed': 0, 'Failed': 0, 'Running': 0}
        self.colors = {'Passed': QColor("#4CAF50"), 'Failed': QColor("#F44336"), 'Running': QColor("#2196F3")}
        self.bg_col = "#ffffff"

    def update_data(self, p, f, r, is_dark):
        self.data = {'Passed': p, 'Failed': f, 'Running': r}
        self.bg_col = "#2b2d30" if is_dark else "#f5f7fa"
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        rect = self.rect()
        margin = 10
        min_dim = min(rect.width(), rect.height()) - 2 * margin
        if min_dim <= 0: return
        center_x = margin + min_dim/2
        center_y = rect.center().y()
        pie_rect = QRectF(center_x - min_dim/2, center_y - min_dim/2, min_dim, min_dim)
        total = sum(self.data.values())
        if total == 0:
            painter.setPen(QColor("#888888"))
            painter.drawText(rect, Qt.AlignCenter, "No Data")
            return
        start_angle = 0
        for name, val in self.data.items():
            if val == 0: continue
            span_angle = (val / total) * 360 * 16
            painter.setBrush(QBrush(self.colors[name]))
            painter.setPen(QPen(QColor(self.bg_col), 1))
            painter.drawPie(pie_rect, int(start_angle), int(span_angle))
            start_angle += span_angle
        leg_x = center_x + min_dim/2 + 15
        leg_y = center_y - 20
        font = painter.font(); font.setPointSize(8); painter.setFont(font)
        for name in ['Passed', 'Running', 'Failed']:
            painter.setBrush(QBrush(self.colors[name]))
            painter.drawRect(int(leg_x), int(leg_y), 10, 10)
            painter.setPen(QPen(Qt.white if self.bg_col == "#2b2d30" else Qt.black))
            painter.drawText(int(leg_x) + 15, int(leg_y) + 10, f"{name}: {self.data[name]}")
            leg_y += 18

class LogTailer(QWidget):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0,0,0,0)
        header = QHBoxLayout()
        self.lbl = QLabel("<b>Select a log file to view...</b>")
        header.addWidget(self.lbl); header.addStretch()
        self.layout.addLayout(header)
        self.text = QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setFont(QFont("Courier", 9))
        self.layout.addWidget(self.text)
        self.timer = QTimer()
        self.timer.timeout.connect(self.read_log)
        self.current_file = None
        self.last_pos = 0

    def tail_file(self, path):
        self.current_file = path
        self.lbl.setText(f"<b>Tailing:</b> {path}")
        self.text.clear()
        self.last_pos = 0
        self.show()
        self.timer.start(1000)
        self.read_log()

    def read_log(self):
        if not self.current_file or not os.path.exists(self.current_file): return
        try:
            with open(self.current_file, 'r') as f:
                f.seek(self.last_pos)
                new_data = f.read()
                if new_data:
                    self.text.appendPlainText(new_data)
                    self.last_pos = f.tell()
                    bar = self.text.verticalScrollBar()
                    bar.setValue(bar.maximum())
        except: pass

    def hideEvent(self, event):
        self.timer.stop(); super().hideEvent(event)

class CustomTreeItem(QTreeWidgetItem):
    def __lt__(self, other):
        col = self.treeWidget().sortColumn()
        t1 = self.text(col).strip() if self.text(col) else ""
        t2 = other.text(col).strip() if other.text(col) else ""

        if col == 0:
            s1 = bool(self.data(0, Qt.UserRole + 5))
            s2 = bool(other.data(0, Qt.UserRole + 5))
            if s1 != s2:
                asc = self.treeWidget().header().sortIndicatorOrder() == Qt.AscendingOrder
                return s1 if asc else not s1

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

class PieChartWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setMinimumSize(450, 450)
        self.data = {}
        self.colors = [
            QColor("#ef5350"), QColor("#42a5f5"), QColor("#66bb6a"), QColor("#ffa726"),
            QColor("#ab47bc"), QColor("#26c6da"), QColor("#8d6e63"), QColor("#78909c"),
            QColor("#d4e157"), QColor("#ec407a")
        ]
        self.bg_col = "#ffffff"

    def set_data(self, data, is_dark):
        self.data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
        self.bg_col = "#2b2d30" if is_dark else "#ffffff"
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        rect = self.rect()
        margin = 30
        min_dim = min(rect.width(), rect.height()) - 2 * margin
        if min_dim <= 0: return
        center = rect.center()
        pie_rect = QRectF(center.x() - min_dim/2, center.y() - min_dim/2, min_dim, min_dim)
        total = sum(self.data.values())
        if total == 0:
            painter.setPen(QColor("#888888"))
            painter.drawText(rect, Qt.AlignCenter, "No Data Available")
            return
        start_angle = 0
        for i, (name, val) in enumerate(self.data.items()):
            span_angle = (val / total) * 360 * 16
            painter.setBrush(QBrush(self.colors[i % len(self.colors)]))
            painter.setPen(QPen(QColor(self.bg_col), 2))
            painter.drawPie(pie_rect, int(start_angle), int(span_angle))
            if (val / total) > 0.03:
                mid_angle_deg = (start_angle + span_angle / 2) / 16.0
                mid_angle_rad = math.radians(mid_angle_deg)
                text_x = center.x() + (min_dim / 2 * 0.65) * math.cos(mid_angle_rad)
                text_y = center.y() - (min_dim / 2 * 0.65) * math.sin(mid_angle_rad)
                perc = (val / total) * 100
                text = f"{name}\n{perc:.1f}%"
                font = painter.font(); font.setBold(True); font.setPointSize(9)
                painter.setFont(font)
                fm = painter.fontMetrics()
                lines = text.split('\n'); th = fm.height()
                y_offset = text_y - (th * len(lines)) / 2
                for line in lines:
                    tw = fm.horizontalAdvance(line)
                    painter.setPen(QPen(QColor(0, 0, 0, 180)))
                    painter.drawText(int(text_x - tw/2 + 1), int(y_offset + th + 1), line)
                    painter.setPen(QPen(Qt.white))
                    painter.drawText(int(text_x - tw/2), int(y_offset + th), line)
                    y_offset += th
            start_angle += span_angle



dialogs.py


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



