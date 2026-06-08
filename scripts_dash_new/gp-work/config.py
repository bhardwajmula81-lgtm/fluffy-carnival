import os
import configparser
import threading
import json
import getpass

try:
    from debug_log import debug_log
except Exception:
    def debug_log(context, exc=None):
        pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "project_config.ini")
MAIL_USERS_FILE = os.path.join(SCRIPT_DIR, "mail_users.ini")
NOTES_DIR = os.path.join(SCRIPT_DIR, "dashboard_notes")

def _safe_user_name():
    try:
        name = getpass.getuser()
    except Exception:
        name = "user"
    import re
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', name or "user")

USER_PREFS_FILE = os.path.join(NOTES_DIR, "user_prefs_{}.ini".format(_safe_user_name()))

if not os.path.exists(NOTES_DIR):
    try: os.makedirs(NOTES_DIR)
    except Exception as e: debug_log("config: create notes dir failed", e)


_PROJECT_INI_DOCS = [
    ("PROJECT", [
        ("PROJECT_PREFIX", "S5K2P5SP", ["Project/top-block prefix."]),
        ("BASE_WS_FE_DIR", "", ["Front-end workspace root."]),
        ("BASE_WS_BE_DIR", "", ["Back-end workspace root."]),
        ("BASE_OUTFEED_DIR", "", ["Published outfeed root."]),
        ("BASE_IR_DIR", "", ["Space-separated RedHawk IR log roots."]),
        ("BLOCKS", "", ["Comma-separated block whitelist. Empty scans all blocks."]),
    ]),
    ("PERFORMANCE", [
        ("SCAN_IR_ON_START", "false", ["true/false. Scan IR logs at startup."]),
        ("SCAN_OWNER_ON_START", "false", ["true/false. Resolve owner names at startup."]),
        ("SCAN_SIGNOFF_ON_START", "false", ["true/false. Scan FE signoff at startup."]),
        ("AUTO_SIZE_ON_START", "false", ["true/false. Calculate disk size at startup."]),
        ("BACKGROUND_SIGNOFF_AFTER_SCAN", "true", ["true/false. Queue background signoff after scan."]),
        ("SIGNOFF_BG_WORKERS", "6", ["Worker count for background signoff scans."]),
    ]),
    ("SCAN_IGNORE", [
        ("FE_RUN_PATTERNS", "", ["Comma-separated fnmatch patterns for FE runs to ignore."]),
        ("BE_RUN_PATTERNS", "", ["Comma-separated fnmatch patterns for BE runs to ignore."]),
        ("PNR_STAGE_PATTERNS", "backup_*", ["Comma-separated fnmatch patterns for PNR stages to ignore."]),
    ]),
    ("TOOLS", [
        ("PNR_TOOL_NAMES", "fc innovus", ["Space-separated PNR tool directory names."]),
        ("SUMMARY_SCRIPT", "", ["summary.py path used for Compare QoR."]),
        ("FIREFOX_PATH", "/usr/bin/firefox", ["Browser executable used for HTML reports."]),
        ("MAIL_UTIL", "/user/vwpmailsystem/MAIL/send_mail_for_rhel7", ["HTML mail command path."]),
        ("USER_INFO_UTIL", "/usr/local/bin/user_info", ["User lookup command path."]),
        ("PYTHON_BIN", "python3.6", ["Python executable for helper scripts."]),
        ("QOR_TIMEOUT_SEC", "600", ["Compare QoR timeout in seconds."]),
        ("USER_INFO_TIMEOUT_SEC", "5", ["user_info timeout in seconds."]),
    ]),
]


_MAIL_INI_DOCS = [
    ("PERMANENT_MEMBERS", [
        ("always_to", "", ["Comma-separated users/emails always added to To."]),
        ("always_cc", "", ["Comma-separated users/emails always added to CC."]),
    ]),
    ("KNOWN_USERS", [
        ("users", "", ["Comma-separated cached users/emails for autocomplete."]),
    ]),
]


def _ini_docs_for_path(path):
    name = os.path.basename(path or "").lower()
    if name == "project_config.ini":
        return _PROJECT_INI_DOCS
    if name == "mail_users.ini":
        return _MAIL_INI_DOCS
    return None


def _ini_needs_doc_refresh(path):
    if not _ini_docs_for_path(path):
        return False
    try:
        with open(path, "r") as f:
            first = f.readline().strip()
        return first != "# Flow Pulse generated configuration."
    except Exception:
        return True


def _cfg_get_case_insensitive(config_obj, section, option, default=""):
    try:
        if not config_obj.has_section(section):
            return default
        target = option.lower()
        for opt in config_obj.options(section):
            if opt.lower() == target:
                return config_obj.get(section, opt, raw=True)
    except Exception:
        pass
    return default


def _render_ini_value(value):
    text = str(value if value is not None else "")
    if "\n" not in text:
        return text
    return "\n\t".join(text.splitlines())


def _render_documented_config(config_obj, docs):
    if not docs:
        import io
        buf = io.StringIO()
        config_obj.write(buf)
        return buf.getvalue()
    lines = [
        "# Flow Pulse generated configuration.",
        "# Edit values below as needed. Comments are regenerated on save.",
        "",
    ]
    known_sections = set()
    for section, entries in docs:
        known_sections.add(section.lower())
        if not config_obj.has_section(section):
            try:
                config_obj.add_section(section)
            except Exception:
                pass
        lines.append("[{}]".format(section))
        known_options = set()
        for option, default, comments in entries:
            known_options.add(option.lower())
            for comment in comments:
                lines.append("# {}: {}".format(option, comment))
            value = _cfg_get_case_insensitive(config_obj, section, option, default)
            lines.append("{} = {}".format(option, _render_ini_value(value)))
            lines.append("")
        try:
            custom = []
            for opt in config_obj.options(section):
                if opt.lower() not in known_options:
                    custom.append(opt)
            if custom:
                lines.append("# Custom options in this section.")
                for opt in sorted(custom):
                    lines.append("{} = {}".format(
                        opt, _render_ini_value(config_obj.get(section, opt, raw=True))))
                lines.append("")
        except Exception:
            pass
        lines.append("")
    unknown_sections = []
    try:
        for section in config_obj.sections():
            if section.lower() not in known_sections:
                unknown_sections.append(section)
    except Exception:
        unknown_sections = []
    if unknown_sections:
        lines.append("# ----------------------------------------------------------------------")
        lines.append("# Unknown/custom options")
        lines.append("# These sections are preserved but are not documented by Flow Pulse.")
        lines.append("# ----------------------------------------------------------------------")
        lines.append("")
        for section in sorted(unknown_sections):
            lines.append("[{}]".format(section))
            try:
                for opt in sorted(config_obj.options(section)):
                    lines.append("{} = {}".format(
                        opt, _render_ini_value(config_obj.get(section, opt, raw=True))))
            except Exception:
                pass
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _atomic_write_config(config_obj, path):
    """Atomically write a ConfigParser object to disk."""
    tmp = "{}.tmp.{}".format(path, os.getpid())
    try:
        with open(tmp, 'w') as f:
            f.write(_render_documented_config(config_obj, _ini_docs_for_path(path)))
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, path)
        return True
    except Exception as e:
        debug_log("config: atomic write failed for {}".format(path), e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False

config = configparser.ConfigParser()
DEFAULT_CONFIG = {
    'PROJECT': {
        'PROJECT_PREFIX': 'S5K2P5SP',
        'BASE_WS_FE_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/WS',
        'BASE_WS_BE_DIR': '/user/s5k2p5sx.be1/s5k2p5sp/WS',
        'BASE_OUTFEED_DIR': '/user/s5k2p5sx.fe1/s5k2p5sp/outfeed',
        'BASE_IR_DIR': '/user/s5k2p5sx.be1/LAYOUT/IR/ /user/s5k2p5sx.be1/LAYOUT/IR2/',
        'BLOCKS': ''
    },
    'PERFORMANCE': {
        'SCAN_IR_ON_START': 'false',
        'SCAN_OWNER_ON_START': 'false',
        'SCAN_SIGNOFF_ON_START': 'false',
        'AUTO_SIZE_ON_START': 'false',
        'BACKGROUND_SIGNOFF_AFTER_SCAN': 'true',
        'SIGNOFF_BG_WORKERS': '6'
    },
    'SCAN_IGNORE': {
        'FE_RUN_PATTERNS': '',
        'BE_RUN_PATTERNS': '',
        'PNR_STAGE_PATTERNS': 'backup_*'
    },
    'TOOLS': {
        'PNR_TOOL_NAMES': 'fc innovus',
        'SUMMARY_SCRIPT': '/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py',
        'FIREFOX_PATH': '/usr/bin/firefox',
        'MAIL_UTIL': '/user/vwpmailsystem/MAIL/send_mail_for_rhel7',
        'USER_INFO_UTIL': '/usr/local/bin/user_info',
        'PYTHON_BIN': 'python3.6',
        'QOR_TIMEOUT_SEC': '600',
        'USER_INFO_TIMEOUT_SEC': '5'
    }
}

if not os.path.exists(CONFIG_FILE):
    config.read_dict(DEFAULT_CONFIG)
    _atomic_write_config(config, CONFIG_FILE)
else:
    config.read(CONFIG_FILE)
    changed = False
    for sec, vals in DEFAULT_CONFIG.items():
        if not config.has_section(sec):
            config.add_section(sec)
            changed = True
        for key, val in vals.items():
            if not config.has_option(sec, key):
                config.set(sec, key, val)
                changed = True
    if changed or _ini_needs_doc_refresh(CONFIG_FILE):
        _atomic_write_config(config, CONFIG_FILE)

# Map Global Variables
PROJECT_PREFIX   = config.get('PROJECT', 'PROJECT_PREFIX', fallback='S5K2P5SP')
BASE_WS_FE_DIR   = config.get('PROJECT', 'BASE_WS_FE_DIR', fallback='')
BASE_WS_BE_DIR   = config.get('PROJECT', 'BASE_WS_BE_DIR', fallback='')
BASE_OUTFEED_DIR = config.get('PROJECT', 'BASE_OUTFEED_DIR', fallback='')
BASE_IR_DIR      = config.get('PROJECT', 'BASE_IR_DIR', fallback='')
# BLOCKS: comma-separated whitelist of block names to scan.
# If empty, all blocks are scanned. If set, only listed blocks are included.
_blocks_raw      = config.get('PROJECT', 'BLOCKS', fallback='')
BLOCKS           = set(b.strip() for b in _blocks_raw.split(',') if b.strip())

PNR_TOOL_NAMES   = config.get('TOOLS', 'PNR_TOOL_NAMES', fallback='fc innovus')
SUMMARY_SCRIPT   = config.get('TOOLS', 'SUMMARY_SCRIPT', fallback='')
FIREFOX_PATH     = config.get('TOOLS', 'FIREFOX_PATH', fallback='/usr/bin/firefox')
MAIL_UTIL        = config.get('TOOLS', 'MAIL_UTIL', fallback='')
USER_INFO_UTIL   = config.get('TOOLS', 'USER_INFO_UTIL', fallback='')
QOR_TIMEOUT_SEC  = config.getint('TOOLS', 'QOR_TIMEOUT_SEC', fallback=600)
USER_INFO_TIMEOUT_SEC = config.getint('TOOLS', 'USER_INFO_TIMEOUT_SEC', fallback=5)

prefs = configparser.ConfigParser()
if os.path.exists(USER_PREFS_FILE): prefs.read(USER_PREFS_FILE)

mail_config = configparser.ConfigParser()
DEFAULT_MAIL_CONFIG = {
    'PERMANENT_MEMBERS': {'always_to': '', 'always_cc': 'mohit.bhar'},
    'KNOWN_USERS': {'users': ''}
}
if not os.path.exists(MAIL_USERS_FILE):
    mail_config.read_dict(DEFAULT_MAIL_CONFIG)
    _atomic_write_config(mail_config, MAIL_USERS_FILE)
else:
    mail_config.read(MAIL_USERS_FILE)
    if _ini_needs_doc_refresh(MAIL_USERS_FILE):
        _atomic_write_config(mail_config, MAIL_USERS_FILE)

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
