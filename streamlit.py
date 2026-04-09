import os
import glob
import re
import subprocess
import fnmatch
import concurrent.futures
import streamlit as st
import pandas as pd

# =====================================================================
# --- CONFIGURATION BLOCK ---
# =====================================================================
PROJECT_PREFIX = "S5K2P5SP"

BASE_WS_FE_DIR = "/user/s5k2p5sx.fe1/s5k2p5sp/WS"
BASE_WS_BE_DIR = "/user/s5k2p5sp.be1/s5k2p5sp/WS"
BASE_OUTFEED_DIR = "/user/s5k2p5sx.fe1/s5k2p5sp/outfeed"

SUMMARY_SCRIPT = "/user/s5k2p5sx.fe1/s5k2p5sp/WS/scripts/summary/summary.py"
FIREFOX_PATH = "/usr/bin/firefox"

st.set_page_config(page_title="Unified PD Dashboard", layout="wide")

# =====================================================================
# --- LOGIC HELPERS ---
# =====================================================================

def normalize_rtl(rtl_str):
    if rtl_str and rtl_str.startswith("EVT"):
        return f"{PROJECT_PREFIX}_{rtl_str}"
    return rtl_str

def format_log_date(date_str):
    match = re.search(r'([A-Z][a-z]{2})\s+([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})', str(date_str))
    if match:
        day_of_week, mon, day, time, year = match.groups()
        return f"{day_of_week} {mon} {day}, {year} - {time}"
    return str(date_str).strip()

def get_dynamic_evt_path(rtl_tag, block_name):
    match = re.search(r'(EVT\d+_ML\d+_DEV\d+)', str(rtl_tag))
    if not match: return ""
    folder_part = match.group(1) 
    return os.path.join(BASE_OUTFEED_DIR, block_name, folder_part)

def get_fm_info(report_path):
    if not report_path or not os.path.exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            for line in f:
                if "No failing compare points" in line: return "PASS"
                m = re.search(r'(\d+)\s+Failing compare points', line)
                if m: return f"{m.group(1)} FAILS"
    except: pass
    return "ERR"

def get_vslp_info(report_path):
    if not report_path or not os.path.exists(report_path): return "N/A"
    try:
        with open(report_path, 'r') as f:
            in_summary = False
            for line in f:
                if "Management Summary" in line:
                    in_summary = True
                    continue
                if in_summary and line.strip().startswith("Total"):
                    parts = line.strip().split()
                    if len(parts) >= 3: return f"Error: {parts[1]}, Warning: {parts[2]}"
                    break
    except: pass
    return "Not Found"

def parse_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path): return d
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if "TOTAL_START" in line and "Load :" in line:
                    d["start"] = format_log_date(line.split("Load :")[-1].strip())
                m = re.search(r'TimeStamp\s*:\s*(\S+)', line)
                if m and m.group(1) not in ["TOTAL", "TOTAL_START"]: 
                    d["last_stage"] = m.group(1)
                if "TimeStamp : TOTAL" in line and "TOTAL_START" not in line:
                    rt = re.search(r'Total\s*:\s*(\d+)h:(\d+)m:(\d+)s', line)
                    if rt: d["runtime"] = f"{int(rt.group(1)):02}h:{int(rt.group(2)):02}m:{int(rt.group(3)):02}s"
                    if "Load :" in line: d["end"] = format_log_date(line.split("Load :")[-1].strip())
    except Exception: pass
    return d

def parse_pnr_runtime_rpt(file_path):
    d = {"start": "N/A", "end": "N/A", "runtime": "00h:00m:00s", "last_stage": "N/A"}
    if not os.path.exists(file_path): return d
    try:
        first_ts, last_ts, final_time_str = None, None, None
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
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
                        total_hours = days * 24 + hours
                        final_time_str = f"{total_hours:02}h:{mins:02}m:{secs:02}s"
                        
                        if len(parts) > 1 and not parts[1].isdigit():
                            d["last_stage"] = parts[1]

        if first_ts:
            y, m, day, H, M = first_ts.groups()
            d["start"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if last_ts:
            y, m, day, H, M = last_ts.groups()
            d["end"] = f"{months[int(m)-1]} {int(day):02d}, {y} - {H}:{M}"
        if final_time_str:
            d["runtime"] = final_time_str
    except Exception: pass
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

def _resolve_outfeed_rtl(rd, phys_evt):
    rtl = extract_rtl(rd)
    if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl):
        rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
    elif rtl == "Unknown":
        rtl = normalize_rtl(phys_evt)
    return normalize_rtl(rtl)

def _process_run(task_tuple):
    b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
    
    if source == "OUTFEED":
        rtl = _resolve_outfeed_rtl(rd, phys_evt)
    else:
        if run_type == "BE":
            extracted = extract_rtl(rd)
            rtl = extracted if extracted != "Unknown" else base_rtl
        else:
            rtl = base_rtl

    r_name = os.path.basename(rd)
    clean_run = r_name.replace("-FE", "").replace("-BE", "")
    clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name) 
    evt_base = get_dynamic_evt_path(rtl, b_name)
    
    fm_n = os.path.join(evt_base, "fm", clean_run, "r2n", "reports", f"{b_name}_r2n.failpoint.rpt")
    fm_u = os.path.join(evt_base, "fm", clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
    vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre", "reports", "report_lp.rpt")
    info = parse_runtime_rpt(os.path.join(rd, "reports/runtime.V2.rpt"))
    
    stages = []
    if run_type == "BE":
        search_dir = os.path.join(rd, "outputs", "*") if source == "WS" else os.path.join(rd, "*")
        for s_dir in glob.glob(search_dir):
            if os.path.isdir(s_dir):
                step_name = os.path.basename(s_dir)
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]: continue
                
                rpt = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt") if source == "WS" else os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                log = os.path.join(rd, "logs", f"{step_name}.log") if source == "WS" else os.path.join(s_dir, "logs", f"{step_name}.log")
                stage_path = os.path.join(rd, "outputs", step_name) if source == "WS" else os.path.join(rd, step_name)
                
                fm_u_glob = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                fm_n_glob = glob.glob(os.path.join(evt_base, "fm", clean_be_run, step_name, "n2n_func", "reports", "*.failpoint.rpt"))
                st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                st_vslp_rpt = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                sta_rpt = os.path.join(evt_base, "pt", r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                
                stage_status = "COMPLETED"
                if source == "WS" and not os.path.exists(rpt):
                    stage_status = "RUNNING"
                    if os.path.exists(log):
                        try:
                            with open(log, 'r', encoding='utf-8', errors='ignore') as f:
                                for line in f:
                                    if "START_CMD:" in line: stage_status = line.strip()
                        except: pass

                stages.append({
                    "Stage Name": step_name,
                    "Status": stage_status,
                    "NONUPF": get_fm_info(st_fm_n_path), 
                    "UPF": get_fm_info(st_fm_u_path),
                    "VSLP": get_vslp_info(st_vslp_rpt),
                    "Runtime": parse_pnr_runtime_rpt(rpt)["runtime"],
                    "Path": stage_path
                })

    is_comp = True if source == "OUTFEED" else os.path.exists(os.path.join(rd, "pass/compile_opt.pass"))
    
    return {
        "Block": b_name, 
        "Run Name": r_name, 
        "RTL": rtl, 
        "Source": source, 
        "Run Type": run_type,
        "Status": "COMPLETED" if is_comp else "RUNNING",
        "NONUPF": get_fm_info(fm_n) if run_type == "FE" else "-",
        "UPF": get_fm_info(fm_u) if run_type == "FE" else "-",
        "VSLP": get_vslp_info(vslp_rpt) if run_type == "FE" else "-",
        "Runtime": info["runtime"],
        "Path": rd,
        "Stages": stages
    }

# =====================================================================
# --- DATA FETCHING (CACHED) ---
# =====================================================================
@st.cache_data(show_spinner="Scanning Network Filesystem...", ttl=300)
def scan_network():
    tasks = []
    
    # SCAN WS
    for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
        if not os.path.exists(ws_base): continue
        for ws_name in os.listdir(ws_base):
            ws_path = os.path.join(ws_base, ws_name)
            if not os.path.isdir(ws_path): continue
            
            current_rtl = "Unknown"
            for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
                try:
                    with open(sf, 'r') as f:
                        lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                        if lbls: current_rtl = normalize_rtl(lbls[0])
                except: pass
            
            for ent_path in glob.glob(os.path.join(ws_path, "IMPLEMENTATION", "*", "SOC", "*")):
                ent_name = os.path.basename(ent_path)
                if ws_base == BASE_WS_FE_DIR:
                    for rd in glob.glob(os.path.join(ent_path, "fc", "*-FE")):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "FE", None))
                for pat in ["*-BE", "EVT*_ML*_DEV*_*_*-BE"]:
                    for rd in glob.glob(os.path.join(ent_path, "fc", pat)):
                        tasks.append((ent_name, rd, ws_path, current_rtl, "WS", "BE", None))

    # SCAN OUTFEED
    if os.path.exists(BASE_OUTFEED_DIR):
        for ent_name in os.listdir(BASE_OUTFEED_DIR):
            ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
            if not os.path.isdir(ent_path): continue
            for evt_dir in glob.glob(os.path.join(ent_path, "EVT*")):
                phys_evt = os.path.basename(evt_dir) 
                for rd in glob.glob(os.path.join(evt_dir, "fc", "*", "*-FE")):
                    tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "FE", phys_evt))
                for rd in glob.glob(os.path.join(evt_dir, "fc", "*-BE")) + glob.glob(os.path.join(evt_dir, "fc", "*", "*-BE")):
                    tasks.append((ent_name, rd, rd, "UNKNOWN", "OUTFEED", "BE", phys_evt))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        for result in executor.map(_process_run, tasks):
            if result: results.append(result)
            
    # Flatten Data for Pandas
    flat_data = []
    for r in results:
        # Add Main Run
        flat_data.append({
            "Block": r["Block"],
            "Run Name": r["Run Name"],
            "Stage": "(Main Run)",
            "RTL": r["RTL"],
            "Source": r["Source"],
            "Status": r["Status"],
            "NONUPF": r["NONUPF"],
            "UPF": r["UPF"],
            "VSLP": r["VSLP"],
            "Runtime": r["Runtime"],
            "Path": r["Path"]
        })
        # Add Stages
        for s in r["Stages"]:
            flat_data.append({
                "Block": r["Block"],
                "Run Name": r["Run Name"],
                "Stage": s["Stage Name"],
                "RTL": r["RTL"],
                "Source": r["Source"],
                "Status": s["Status"],
                "NONUPF": s["NONUPF"],
                "UPF": s["UPF"],
                "VSLP": s["VSLP"],
                "Runtime": s["Runtime"],
                "Path": s["Path"]
            })
            
    return pd.DataFrame(flat_data)


# =====================================================================
# --- STREAMLIT UI ---
# =====================================================================

st.title("Unified PD Dashboard 📊")
st.markdown("*(Read-Only Trial Port)*")

# Fetch Data
df = scan_network()

if df.empty:
    st.warning("No runs found or still scanning...")
else:
    # Sidebar Filters
    st.sidebar.header("Filters")
    
    src_options = ["ALL", "WS", "OUTFEED"]
    sel_src = st.sidebar.selectbox("Source", src_options)
    
    all_rtls = ["ALL"] + sorted(df["RTL"].unique().tolist())
    sel_rtl = st.sidebar.selectbox("RTL Release", all_rtls)
    
    all_blocks = sorted(df["Block"].unique().tolist())
    sel_blocks = st.sidebar.multiselect("Blocks", all_blocks, default=all_blocks)
    
    search_txt = st.sidebar.text_input("Search Run Name (e.g. *run1*)")

    # Apply Filters
    filtered_df = df.copy()
    
    if sel_src != "ALL":
        filtered_df = filtered_df[filtered_df["Source"] == sel_src]
        
    if sel_rtl != "ALL":
        filtered_df = filtered_df[filtered_df["RTL"] == sel_rtl]
        
    if sel_blocks:
        filtered_df = filtered_df[filtered_df["Block"].isin(sel_blocks)]
        
    if search_txt:
        # Convert glob syntax to regex for pandas filtering
        regex_pattern = fnmatch.translate(search_txt)
        filtered_df = filtered_df[filtered_df["Run Name"].str.match(regex_pattern, case=False)]

    # Display Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows Displayed", len(filtered_df))
    col2.metric("Total Blocks", len(filtered_df["Block"].unique()))
    col3.metric("Total Unique Runs", len(filtered_df["Run Name"].unique()))

    # Display Interactive Dataframe
    st.dataframe(
        filtered_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Path": st.column_config.TextColumn("Path (Double click to copy)"),
            "Status": st.column_config.TextColumn("Status")
        }
    )
    
    # Utilities Section below table
    st.divider()
    st.subheader("🛠️ Utilities")
    st.info("Note: Because Streamlit runs as a web server, traditional PyQt right-click menus (like opening `gvim` directly) don't translate natively to the browser without custom components. In this trial, you can copy the Path from the table above.")
    
    # Local terminal command generator
    target_path = st.text_input("Paste a Path here to generate terminal commands:")
    if target_path:
        st.code(f"cd {target_path}\nls -lah", language="bash")
        st.code(f"gvim {target_path}/logs/*.log", language="bash")
        st.code(f"du -sh {target_path}", language="bash")
        
    # Refresh Cache Button
    if st.button("🔄 Force Rescan (Clear Cache)"):
        st.cache_data.clear()
        st.rerun()
