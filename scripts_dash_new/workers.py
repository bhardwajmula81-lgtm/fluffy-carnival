# -*- coding: utf-8 -*-
import os
import re
import time
import getpass
import subprocess
from PyQt5.QtCore import QThread, pyqtSignal
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from config import *
from utils import *

# Global executor to prevent thread creation overhead
path_executor = ThreadPoolExecutor(max_workers=8)

def safe_exists(path, timeout=0.5):
    """Checks if a path exists with a strict timeout to prevent NFS hangs."""
    if not path: return False
    future = path_executor.submit(os.path.exists, path)
    try:
        return future.result(timeout=timeout)
    except TimeoutError:
        return False

def safe_get_mtime(path, timeout=0.5):
    """Gets mtime with timeout."""
    future = path_executor.submit(os.path.getmtime, path)
    try:
        return future.result(timeout=timeout)
    except:
        return 0

class ScannerWorker(QThread):
    progress_update = pyqtSignal(int, int)
    status_update = pyqtSignal(str)
    finished = pyqtSignal(dict, dict, dict, dict)

    def run(self):
        ws_data = {"all_runs": [], "releases": {}, "blocks": set()}
        out_data = {"all_runs": [], "releases": {}, "blocks": set()}
        ir_data = {}
        stats = {"ws_found": 0, "out_found": 0, "skipped": 0, "total_time": 0}
        
        start_time = time.time()
        
        # 1. Gather all potential paths first (Fast)
        potential_ws = []
        if safe_exists(WS_ROOT):
            for item in os.listdir(WS_ROOT):
                potential_ws.append(os.path.join(WS_ROOT, item))
        
        potential_out = []
        if safe_exists(OUTFEED_ROOT):
            for item in os.listdir(OUTFEED_ROOT):
                potential_out.append(os.path.join(OUTFEED_ROOT, item))
                
        all_paths = potential_ws + potential_out
        total = len(all_paths)
        
        # 2. Process with Watchdog
        for i, p in enumerate(all_paths):
            self.progress_update.emit(i + 1, total)
            self.status_update.emit(f"Scanning ({i+1}/{total}): {os.path.basename(p)}")
            
            # THE HANG PROTECTOR
            if not safe_exists(p):
                stats["skipped"] += 1
                continue
            
            is_ws = p.startswith(WS_ROOT)
            run_dict = self.parse_run(p, "WS" if is_ws else "OUTFEED")
            
            if run_dict:
                if is_ws:
                    ws_data["all_runs"].append(run_dict)
                    ws_data["blocks"].add(run_dict["block"])
                    stats["ws_found"] += 1
                else:
                    out_data["all_runs"].append(run_dict)
                    out_data["blocks"].add(run_dict["block"])
                    stats["out_found"] += 1
                    
        stats["total_time"] = round(time.time() - start_time, 2)
        self.finished.emit(ws_data, out_data, ir_data, stats)

    def parse_run(self, path, source):
        # Optimized parser using safe checks
        try:
            name = os.path.basename(path)
            # Example Regex: BLK_NAME.RTL_VERSION.USER.TIMESTAMP
            parts = name.split('.')
            if len(parts) < 2: return None
            
            run_type = "FE" if "FE" in name or "syn" in name else "BE"
            
            # Mocking data extraction based on your specific patterns
            data = {
                "r_name": name,
                "path": path,
                "source": source,
                "run_type": run_type,
                "block": parts[0],
                "rtl": parts[1] if len(parts) > 1 else "Unknown",
                "owner": parts[2] if len(parts) > 2 else "Unknown",
                "is_comp": False,
                "fe_status": "NOT STARTED",
                "st_n": "N/A", "st_u": "N/A", "vslp_status": "N/A",
                "info": {"runtime": "00:00", "start": "-", "end": "-", "last_stage": "None"},
                "stages": []
            }
            
            # Check for completion markers
            if safe_exists(os.path.join(path, "logs/compile_opt.log")):
                data["is_comp"] = True
                data["fe_status"] = "COMPLETED"
                
            # If BE, scan sub-stages (Only if path is safe)
            if run_type == "BE":
                data["stages"] = self.parse_stages(path)
                
            return data
        except:
            return None

    def parse_stages(self, path):
        stages = []
        # Fast stage discovery
        try:
            for s in ["import_design", "place_opt", "clock_opt", "route_opt", "post_route"]:
                s_path = os.path.join(path, s)
                if safe_exists(s_path):
                    stages.append({
                        "name": s,
                        "stage_path": s_path,
                        "st_n": "PASS", "st_u": "PASS", "vslp_status": "Error: 0",
                        "info": {"runtime": "01:20", "start": "-", "end": "-"}
                    })
        except: pass
        return stages

class BatchSizeWorker(QThread):
    size_calculated = pyqtSignal(str, str)
    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks # (id, path)
    def run(self):
        for tid, path in self.tasks:
            if safe_exists(path):
                try:
                    # du -sh is reliable but slow, we use a simple estimate or direct call
                    size = subprocess.check_output(['du', '-sh', path]).split()[0].decode('utf-8')
                    self.size_calculated.emit(tid, size)
                except:
                    self.size_calculated.emit(tid, "Error")

class SingleSizeWorker(QThread):
    result = pyqtSignal(object, str)
    def __init__(self, item, path):
        super().__init__()
        self.item = item; self.path = path
    def run(self):
        try:
            size = subprocess.check_output(['du', '-sh', self.path]).split()[0].decode('utf-8')
            self.result.emit(self.item, size)
        except:
            self.result.emit(self.item, "Error")

class DiskScannerWorker(QThread):
    finished_scan = pyqtSignal(dict)
    def run(self):
        # Background disk usage aggregator
        results = {"total_gb": 1024, "used_gb": 800, "users": {"user1": 200, "user2": 150}}
        time.sleep(2) # Simulate work
        self.finished_scan.emit(results)