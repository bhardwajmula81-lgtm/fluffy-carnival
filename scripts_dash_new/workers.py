import os
import glob
import re
import subprocess
import pwd
import concurrent.futures
from PyQt5.QtCore import QThread, pyqtSignal
from config import *
from utils import *


# ===========================================================================
# BatchSizeWorker — calculates folder sizes for multiple items in background
# ===========================================================================
class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self._is_cancelled = False

    def run(self):
        max_w = min(20, (os.cpu_count() or 4) * 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
            futures = {executor.submit(self.get_size, path): item_id
                       for item_id, path in self.tasks}
            for future in concurrent.futures.as_completed(futures):
                if self._is_cancelled:
                    break
                item_id = futures[future]
                try:
                    self.size_calculated.emit(item_id, future.result())
                except:
                    self.size_calculated.emit(item_id, "N/A")

    def get_size(self, path):
        if not path or not os.path.exists(path):
            return "N/A"
        total_size = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total_size += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total_size += self._calc_dir(entry.path)
        except:
            return "N/A"
        if total_size == 0:
            return "N/A"
        for unit in ['K', 'M', 'G']:
            total_size /= 1024.0
            if total_size < 1024.0:
                return f"{total_size:.1f}{unit}"
        return f"{total_size:.1f}T"

    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True


# ===========================================================================
# SingleSizeWorker — calculates folder size for one item on demand
# ===========================================================================
class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)

    def __init__(self, item, path):
        super().__init__()
        self.item = item
        self.path = path
        self._is_cancelled = False

    def run(self):
        if self._is_cancelled or not self.path or not os.path.exists(self.path):
            self.result.emit(self.item, "N/A")
            return
        total_size = 0
        try:
            for entry in os.scandir(self.path):
                if entry.is_file(follow_symlinks=False):
                    total_size += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total_size += self._calc_dir(entry.path)
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
            if not self._is_cancelled:
                self.result.emit(self.item, "N/A")

    def _calc_dir(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._calc_dir(entry.path)
        except:
            pass
        return total

    def cancel(self):
        self._is_cancelled = True


# ===========================================================================
# DiskScannerWorker — scans workspace/outfeed disk usage in background
# ===========================================================================
class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)

    def _get_batch_dir_info(self, paths):
        results = []
        if not paths:
            return results
        try:
            cmd    = ['du', '-sk'] + paths
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode('utf-8')
            for line in output.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    sz_kb     = int(parts[0])
                    full_path = parts[1]
                    try:
                        owner = pwd.getpwuid(os.stat(full_path).st_uid).pw_name
                    except:
                        owner = "Unknown"
                    results.append((owner, sz_kb, full_path))
        except:
            pass
        return results

    def run(self):
        results = {"WS (FE)": {}, "WS (BE)": {}, "OUTFEED": {}}

        outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "fc", "*"))
        outfeed_targets.extend(glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*", "innovus", "*")))
        if not outfeed_targets:
            outfeed_targets = glob.glob(os.path.join(BASE_OUTFEED_DIR, "*", "EVT*"))

        targets_map = {
            "WS (FE)": glob.glob(os.path.join(BASE_WS_FE_DIR, "*")),
            "WS (BE)": glob.glob(os.path.join(BASE_WS_BE_DIR, "*")),
            "OUTFEED":  outfeed_targets,
        }

        tasks = []
        for cat, paths in targets_map.items():
            valid_paths = [p for p in paths if os.path.isdir(p)]
            for i in range(0, len(valid_paths), 50):
                chunk = valid_paths[i:i + 50]
                tasks.append((cat, chunk))

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_cat = {executor.submit(self._get_batch_dir_info, t[1]): t[0] for t in tasks}
            for future in concurrent.futures.as_completed(future_to_cat):
                cat = future_to_cat[future]
                try:
                    batch_results = future.result()
                    for owner, sz_kb, full_path in batch_results:
                        if sz_kb > 0:
                            gb_sz = sz_kb / (1024 ** 2)
                            if gb_sz > 0.01:
                                if owner not in results[cat]:
                                    results[cat][owner] = {"total": 0, "dirs": []}
                                results[cat][owner]["total"] += gb_sz
                                results[cat][owner]["dirs"].append((full_path, gb_sz))
                except:
                    pass

        for cat in results:
            for owner in results[cat]:
                results[cat][owner]["dirs"].sort(key=lambda x: x[1], reverse=True)

        self.finished_scan.emit(results)


# ===========================================================================
# ScannerWorker — main workspace/outfeed scanner
# FIX 5: IR scan runs in PARALLEL with workspace scans via concurrent.futures
# ===========================================================================
class ScannerWorker(QThread):
    finished        = pyqtSignal(dict, dict, dict, dict)
    progress_update = pyqtSignal(int, int)
    status_update   = pyqtSignal(str)

    # -----------------------------------------------------------------------
    # IR directory scanner — called as a parallel future inside run()
    # -----------------------------------------------------------------------
    def scan_ir_dir(self):
        ir_data    = {}
        target_lef = f"{PROJECT_PREFIX}.lef.list"
        ir_dirs    = BASE_IR_DIR.split()

        for ir_base in ir_dirs:
            if not os.path.exists(ir_base):
                continue
            for root_dir, dirs, files in os.walk(ir_base):
                for f_name in files:
                    if not f_name.startswith("redhawk.log"):
                        continue
                    log_path = os.path.join(root_dir, f_name)

                    run_be_name = step_name = None
                    static_val  = dynamic_val = "-"
                    in_static   = in_dynamic  = False
                    static_lines, dynamic_lines = [], []

                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if line.startswith("Parsing ") and target_lef in line:
                                    m = re.search(r'/fc/([^/]+-BE)/(?:outputs/)?([^/]+)/', line)
                                    if m:
                                        run_be_name = m.group(1)
                                        step_name   = m.group(2)

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
                                ir_data[key] = {
                                    "static": "-", "dynamic": "-",
                                    "log": log_path,
                                    "static_table": "", "dynamic_table": ""
                                }
                            if static_val  != "-": ir_data[key]["static"]  = static_val
                            if dynamic_val != "-": ir_data[key]["dynamic"] = dynamic_val
                            if static_lines:  ir_data[key]["static_table"]  = "\n".join(static_lines)
                            if dynamic_lines: ir_data[key]["dynamic_table"] = "\n".join(dynamic_lines)
                    except:
                        pass

        return ir_data

    # -----------------------------------------------------------------------
    # Workspace discovery helper
    # -----------------------------------------------------------------------
    def _scan_single_workspace(self, ws_base, ws_name, tools_to_scan):
        tasks           = []
        releases_found  = {}
        ws_path         = os.path.join(ws_base, ws_name)
        if not os.path.isdir(ws_path):
            return tasks, releases_found

        current_rtl = "Unknown"
        for sf in glob.glob(os.path.join(ws_path, "*.p4_sync")):
            try:
                with open(sf, 'r') as f:
                    lbls = re.findall(r'/([^/]+_syn\d*)\.config', f.read())
                    for l in set(lbls):
                        current_rtl = normalize_rtl(l)
                        if current_rtl not in releases_found:
                            releases_found[current_rtl] = []
                        releases_found[current_rtl].append(ws_path)
            except:
                pass

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

    # -----------------------------------------------------------------------
    # Main run — FIX 5: IR scan launched in parallel with workspace scans
    # -----------------------------------------------------------------------
    def run(self):
        clear_path_cache()
        self.status_update.emit("Discovering Workspaces...")

        ws_data    = {"releases": {}, "blocks": set(), "all_runs": []}
        out_data   = {"releases": {}, "blocks": set(), "all_runs": []}
        scan_stats = {"ws": 0, "outfeed": 0, "blocks": {}, "fc": 0, "innovus": 0}

        tasks          = []
        tools_to_scan  = PNR_TOOL_NAMES.split()

        # --- Workspace discovery (parallel) ---
        disc_max_w = min(20, (os.cpu_count() or 4) * 4)
        disc_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=disc_max_w) as disc_ex:
            for ws_base in [BASE_WS_FE_DIR, BASE_WS_BE_DIR]:
                if not os.path.exists(ws_base):
                    continue
                try:
                    ws_names = os.listdir(ws_base)
                except:
                    continue
                for ws_name in ws_names:
                    disc_futures.append(
                        disc_ex.submit(self._scan_single_workspace, ws_base, ws_name, tools_to_scan)
                    )

            for future in concurrent.futures.as_completed(disc_futures):
                try:
                    new_tasks, new_releases = future.result()
                    tasks.extend(new_tasks)
                    for rtl, paths in new_releases.items():
                        for p in paths:
                            self._map_release(ws_data, rtl, p)
                except:
                    pass

        # --- Outfeed discovery ---
        self.status_update.emit("Discovering OUTFEED directories...")
        if os.path.exists(BASE_OUTFEED_DIR):
            for ent_name in os.listdir(BASE_OUTFEED_DIR):
                ent_path = os.path.join(BASE_OUTFEED_DIR, ent_name)
                if not os.path.isdir(ent_path):
                    continue
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

        # --- Prefetch path cache ---
        paths_to_prefetch = []
        for t in tasks:
            rd = t[1]
            paths_to_prefetch.append(os.path.join(rd, "pass/compile_opt.pass"))
            paths_to_prefetch.append(os.path.join(rd, "logs/compile_opt.log"))
            paths_to_prefetch.append(os.path.join(rd, "reports/runtime.V2.rpt"))
        self.status_update.emit("Prefetching file metadata...")
        prefetch_path_cache(paths_to_prefetch)

        # --- Process runs + IR scan in PARALLEL ---
        total_tasks     = len(tasks)
        completed_tasks = 0
        max_w           = min(40, (os.cpu_count() or 4) * 6)

        self.status_update.emit("Processing run data and parsing reports...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:

            # FIX 5: submit IR scan immediately — runs alongside workspace processing
            ir_future = executor.submit(self.scan_ir_dir)

            future_to_task = {executor.submit(self._thread_process_run, t): t for t in tasks}

            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    if result:
                        if result["source"] == "WS":
                            ws_data["blocks"].add(result["block"])
                            ws_data["all_runs"].append(result)
                            scan_stats["ws"] += 1
                            if result["run_type"] == "BE":
                                self._map_release(ws_data, result["rtl"], result["parent"])
                        else:
                            out_data["blocks"].add(result["block"])
                            out_data["all_runs"].append(result)
                            scan_stats["outfeed"] += 1
                            self._map_release(out_data, result["rtl"], result["path"])

                        blk = result["block"]
                        if blk not in scan_stats["blocks"]:
                            scan_stats["blocks"][blk] = 0
                        scan_stats["blocks"][blk] += 1

                        if "/fc/" in result["path"]:
                            scan_stats["fc"] += 1
                        elif "/innovus/" in result["path"]:
                            scan_stats["innovus"] += 1
                except:
                    pass

                completed_tasks += 1
                # Throttle UI updates — emit every 20 tasks to avoid flooding event loop
                if completed_tasks % 20 == 0 or completed_tasks == total_tasks:
                    self.progress_update.emit(completed_tasks, total_tasks)
                    self.status_update.emit(f"Processing runs... ({completed_tasks}/{total_tasks})")

            # Collect IR results — usually already done by now since it ran in parallel
            try:
                ir_data = ir_future.result()
            except:
                ir_data = {}

        self.finished.emit(ws_data, out_data, ir_data, scan_stats)

    # -----------------------------------------------------------------------
    # Per-task run processor (called in thread pool)
    # -----------------------------------------------------------------------
    def _thread_process_run(self, task_tuple):
        b_name, rd, parent_path, base_rtl, source, run_type, phys_evt = task_tuple
        if source == "OUTFEED":
            rtl = self._resolve_outfeed_rtl(rd, phys_evt)
        else:
            per_run_rtl = extract_rtl(rd)
            rtl = per_run_rtl if (per_run_rtl and per_run_rtl != "Unknown") else base_rtl
            if rtl == "Unknown":
                rtl = base_rtl
        return self._process_run(b_name, rd, parent_path, rtl, source, run_type)

    def _resolve_outfeed_rtl(self, rd, phys_evt):
        rtl = extract_rtl(rd)
        if re.search(r'EVT\d+_ML\d+_DEV\d+', rtl):
            rtl = re.sub(r'EVT\d+_ML\d+_DEV\d+', phys_evt, rtl)
        elif rtl == "Unknown":
            rtl = normalize_rtl(phys_evt)
        return normalize_rtl(rtl)

    def _process_run(self, b_name, rd, parent_path, rtl, source, run_type):
        r_name       = os.path.basename(rd)
        clean_run    = r_name.replace("-FE", "").replace("-BE", "")
        clean_be_run = re.sub(r'^EVT\d+_ML\d+_DEV\d+(_syn\d+)?_', '', r_name)
        evt_base     = get_dynamic_evt_path(rtl, b_name)
        owner        = get_owner(rd)

        fm_n     = os.path.join(evt_base, "fm",   clean_run, "r2n",   "reports", f"{b_name}_r2n.failpoint.rpt")
        fm_u     = os.path.join(evt_base, "fm",   clean_run, "r2upf", "reports", f"{b_name}_r2upf.failpoint.rpt")
        vslp_rpt = os.path.join(evt_base, "vslp", clean_run, "pre",   "reports", "report_lp.rpt")
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
                    except:
                        pass

        stages = []
        if run_type == "BE":
            search_glob = (os.path.join(rd, "outputs", "*") if source == "WS"
                           else os.path.join(rd, "*"))
            for s_dir in glob.glob(search_glob):
                if not os.path.isdir(s_dir):
                    continue
                step_name = os.path.basename(s_dir)
                if source == "OUTFEED" and step_name in ["reports", "logs", "pass", "fail", "outputs"]:
                    continue

                if source == "WS":
                    rpt        = os.path.join(rd, "reports", step_name, f"{step_name}.runtime.rpt")
                    log        = os.path.join(rd, "logs",    f"{step_name}.log")
                    stage_path = os.path.join(rd, "outputs", step_name)
                else:
                    rpt        = os.path.join(s_dir, "reports", step_name, f"{step_name}.runtime.rpt")
                    log        = os.path.join(s_dir, "logs",    f"{step_name}.log")
                    stage_path = os.path.join(rd,    step_name)

                fm_u_glob    = glob.glob(os.path.join(evt_base, "fm",   clean_be_run, step_name, "n2upf_func", "reports", "*.failpoint.rpt"))
                fm_n_glob    = glob.glob(os.path.join(evt_base, "fm",   clean_be_run, step_name, "n2n_func",   "reports", "*.failpoint.rpt"))
                st_fm_u_path = fm_u_glob[0] if fm_u_glob else ""
                st_fm_n_path = fm_n_glob[0] if fm_n_glob else ""
                st_vslp_rpt  = os.path.join(evt_base, "vslp", clean_be_run, "pgnet", step_name, "reports", "report_lp.rpt")
                sta_rpt      = os.path.join(evt_base, "pt",   r_name, step_name, "reports", "sta", "summary", "summary.rpt")
                qor_path     = rd if rd.endswith("/") else rd + "/"

                stages.append({
                    "name":          step_name,
                    "rpt":           rpt,
                    "log":           log,
                    "info":          parse_pnr_runtime_rpt(rpt),
                    "st_n":          get_fm_info(st_fm_n_path),
                    "st_u":          get_fm_info(st_fm_u_path),
                    "vslp_status":   get_vslp_info(st_vslp_rpt),
                    "fm_u_path":     st_fm_u_path,
                    "fm_n_path":     st_fm_n_path,
                    "vslp_rpt_path": st_vslp_rpt,
                    "sta_rpt_path":  sta_rpt,
                    "qor_path":      qor_path,
                    "stage_path":    stage_path,
                })

        return {
            "block":        b_name,
            "path":         rd,
            "parent":       parent_path,
            "rtl":          rtl,
            "r_name":       r_name,
            "run_type":     run_type,
            "stages":       stages,
            "source":       source,
            "owner":        owner,
            "is_comp":      is_comp,
            "fe_status":    fe_status,
            "st_n":         get_fm_info(fm_n),
            "st_u":         get_fm_info(fm_u),
            "vslp_status":  get_vslp_info(vslp_rpt),
            "info":         info,
            "fm_n_path":    fm_n,
            "fm_u_path":    fm_u,
            "vslp_rpt_path": vslp_rpt,
        }

    def _map_release(self, data_obj, rtl_str, path):
        if rtl_str not in data_obj["releases"]:
            data_obj["releases"][rtl_str] = []
        if path not in data_obj["releases"][rtl_str]:
            data_obj["releases"][rtl_str].append(path)
        base = re.sub(r'_syn\d+$', '', rtl_str)
        if base != rtl_str:
            if base not in data_obj["releases"]:
                data_obj["releases"][base] = []
            if path not in data_obj["releases"][base]:
                data_obj["releases"][base].append(path)
