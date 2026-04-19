class ScannerWorker_run_replacement:
    """
    Replace the run() method body in your ScannerWorker class with this logic.
    This is shown as a standalone function for clarity.
    """
 
    def run(self):
        import concurrent.futures as cf
 
        clear_path_cache()
 
        ws_data  = {"all_runs": [], "releases": {}, "blocks": set()}
        out_data = {"all_runs": [], "releases": {}, "blocks": set()}
        ir_data  = {}
        stats    = {"ws_scanned": 0, "out_scanned": 0, "total_runs": 0, "errors": []}
 
        # --- Discover all workspaces ---
        fe_workspaces, be_workspaces, out_dirs = [], [], []
 
        if os.path.isdir(BASE_WS_FE_DIR):
            fe_workspaces = sorted([
                d for d in os.listdir(BASE_WS_FE_DIR)
                if os.path.isdir(os.path.join(BASE_WS_FE_DIR, d))
                   and PROJECT_PREFIX in d
            ])
 
        if os.path.isdir(BASE_WS_BE_DIR):
            be_workspaces = sorted([
                d for d in os.listdir(BASE_WS_BE_DIR)
                if os.path.isdir(os.path.join(BASE_WS_BE_DIR, d))
                   and PROJECT_PREFIX in d
            ])
 
        if os.path.isdir(BASE_OUTFEED_DIR):
            out_dirs = sorted([
                d for d in os.listdir(BASE_OUTFEED_DIR)
                if os.path.isdir(os.path.join(BASE_OUTFEED_DIR, d))
            ])
 
        total = len(fe_workspaces) + len(be_workspaces) + len(out_dirs)
        completed = [0]
 
        def progress_inc():
            completed[0] += 1
            self.progress_update.emit(completed[0], total)
 
        # FIX 5: Launch IR scan in parallel with workspace scans
        executor = cf.ThreadPoolExecutor(max_workers=min(32, total + 2))
        futures  = {}
 
        # Submit IR scan immediately — runs in background while WS scans proceed
        ir_future = executor.submit(self.scan_ir_dir)
 
        # Submit all FE workspace scans
        for ws_name in fe_workspaces:
            ws_base = os.path.join(BASE_WS_FE_DIR, ws_name)
            f = executor.submit(self._scan_single_workspace, ws_base, ws_name, "FE")
            futures[f] = ("FE", ws_name)
 
        # Submit all BE workspace scans
        for ws_name in be_workspaces:
            ws_base = os.path.join(BASE_WS_BE_DIR, ws_name)
            f = executor.submit(self._scan_single_workspace, ws_base, ws_name, "BE")
            futures[f] = ("BE", ws_name)
 
        # Submit outfeed scans
        for out_dir in out_dirs:
            out_base = os.path.join(BASE_OUTFEED_DIR, out_dir)
            f = executor.submit(self._scan_outfeed_dir, out_base, out_dir)
            futures[f] = ("OUT", out_dir)
 
        # Collect workspace results as they complete
        for future in cf.as_completed(futures):
            scan_type, name = futures[future]
            try:
                result = future.result()
                if result is None:
                    progress_inc()
                    continue
 
                if scan_type in ("FE", "BE"):
                    ws_data["all_runs"].extend(result.get("runs", []))
                    for rtl, blks in result.get("releases", {}).items():
                        if rtl not in ws_data["releases"]:
                            ws_data["releases"][rtl] = set()
                        ws_data["releases"][rtl].update(blks)
                    ws_data["blocks"].update(result.get("blocks", set()))
                    stats["ws_scanned"] += 1
 
                elif scan_type == "OUT":
                    out_data["all_runs"].extend(result.get("runs", []))
                    for rtl, blks in result.get("releases", {}).items():
                        if rtl not in out_data["releases"]:
                            out_data["releases"][rtl] = set()
                        out_data["releases"][rtl].update(blks)
                    out_data["blocks"].update(result.get("blocks", set()))
                    stats["out_scanned"] += 1
 
            except Exception as e:
                stats["errors"].append(f"{name}: {e}")
 
            progress_inc()
            self.status_update.emit(f"Scanning {name}...")
 
        # Wait for IR scan to complete (it ran in parallel — usually done by now)
        try:
            ir_data = ir_future.result()
        except Exception as e:
            stats["errors"].append(f"IR scan: {e}")
            ir_data = {}
 
        executor.shutdown(wait=False)
 
        stats["total_runs"] = len(ws_data["all_runs"]) + len(out_data["all_runs"])
        self.finished.emit(ws_data, out_data, ir_data, stats)
