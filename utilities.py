
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
        fields = res.split(',')
        if len(fields) >= 9:
            email = fields[8].strip()
            if '@' in email:
                return email
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', res)
        if match: return match.group(0)
    except: pass
    return ""

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
        with open(report_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if "No failing compare points" in line: return "PASS"
                m = re.search(r'(\d+)\s+Failing compare points', line)
                if m: return f"{m.group(1)} FAILS"
    except: pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not cached_exists(report_path): return "N/A"
    try:
        with open(report_path, 'r', encoding='utf-8', errors='ignore') as f:
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
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
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
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
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
        with open(f[0], 'r', encoding='utf-8', errors='ignore') as file:
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
        if file.endswith(".json") and not file.startswith("pins_"):
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
        # Adds the stacked chat timestamp
        ts = datetime.datetime.now().strftime("%b %d, %H:%M")
        user_data[identifier] = f"[{current_user} - {ts}] {note_text.strip()}"
    else:
        if identifier in user_data: del user_data[identifier]
        
    try:
        with open(user_file, 'w') as f:
            json.dump(user_data, f, indent=4)
    except Exception as e:
        print(f"Failed to save note: {e}")

def get_pins_file():
    return os.path.join(NOTES_DIR, f"pins_{getpass.getuser()}.json")

def load_user_pins():
    fpath = get_pins_file()
    if os.path.exists(fpath):
        try:
            with open(fpath, 'r') as f: return json.load(f)
        except: pass
    return {}

def save_user_pins(pins_dict):
    try:
        with open(get_pins_file(), 'w') as f: json.dump(pins_dict, f, indent=4)
    except: pass
