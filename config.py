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
