"""
Flow Pulse Archive Layer  (archive.py)
---------------------------------------
Level 1 - Lightweight snapshot : SQLite storing extracted run metadata + metrics.
Level 2 - Evidence archive      : Copies key report/log files to an archive dir.

Directory layout (evidence):
  <ARCHIVE_BASE>/
    <PROJECT>/
      <BLOCK>/
        <RTL>/
          <RUN_NAME>/
            manifest.json
            metrics.json
            reports/
            logs/
            screenshots/

SQLite DB  :  <ARCHIVE_BASE>/flow_pulse.db
"""

import os
import re
import glob
import json
import shutil
import sqlite3
import threading
import datetime
import getpass

from PyQt5.QtCore import QThread, pyqtSignal

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------
_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS schema_version (version INTEGER);

CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    project        TEXT,
    block          TEXT,
    rtl            TEXT,
    run_name       TEXT,
    source_path    TEXT UNIQUE,
    source_type    TEXT,
    run_type       TEXT,
    status         TEXT,
    owner          TEXT,
    start_time     TEXT,
    end_time       TEXT,
    runtime_sec    INTEGER,
    archived_at    TEXT,
    archived_by    TEXT,
    has_evidence   INTEGER DEFAULT 0,
    snapshot_json  TEXT
);

CREATE TABLE IF NOT EXISTS be_stages (
    run_id       INTEGER REFERENCES runs(id) ON DELETE CASCADE,
    stage_name   TEXT,
    status       TEXT,
    runtime_sec  INTEGER,
    start_time   TEXT,
    end_time     TEXT,
    metrics_json TEXT,
    PRIMARY KEY (run_id, stage_name)
);

CREATE TABLE IF NOT EXISTS archived_files (
    run_id         INTEGER REFERENCES runs(id) ON DELETE CASCADE,
    rel_path       TEXT,
    source_mtime   REAL,
    size_bytes     INTEGER,
    PRIMARY KEY (run_id, rel_path)
);

CREATE INDEX IF NOT EXISTS idx_runs_block  ON runs(block);
CREATE INDEX IF NOT EXISTS idx_runs_rtl    ON runs(rtl);
CREATE INDEX IF NOT EXISTS idx_runs_path   ON runs(source_path);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
"""

DB_FILENAME      = "flow_pulse.db"
SCHEMA_VERSION   = 1

# ---------------------------------------------------------------------------
# Files captured per run type
# ---------------------------------------------------------------------------

# Relative glob patterns under the FE run directory
FE_CAPTURE_GLOBS = [
    "reports/runtime.V2.rpt",
    "reports/area.*.rpt",
    "reports/qor.*.rpt",
    "reports/congestion.*.rpt",
    "reports/utilization.*.rpt",
    "reports/cell_usage.summary.*.rpt",
    "reports/report_app_options.full.*.rpt",
    "reports/congestion.window.*.jpg",
    "logs/compile_opt.log",
    "pass/compile_opt.pass",
]

# Templates with {s} = stage name, relative to BE run directory
BE_STAGE_CAPTURE_GLOBS = [
    "reports/{s}/{s}.runtime.rpt",
    "reports/{s}/{s}.qor_sum.rpt",
    "reports/{s}/{s}_p*.summary.gz",
    "reports/{s}/{s}.qor.snap.rpt",
    "reports/{s}/{s}.grc.rpt",
    "reports/{s}/{s}.sec_get_area.rpt",
    "reports/{s}/{s}.sec_vth_use.rpt",
    "reports/{s}/{s}.cts.qor.final.rpt",
    "reports/{s}/{s}.env.app_options.full.rpt",
    "reports/{s}/{s}.opt.options.rpt",
    "screenshot/*.jpg",
]

_SAFE_RE = re.compile(r"[^\w\-.]")

def _safe(s):
    """Sanitize a string for use as a path component."""
    return _SAFE_RE.sub("_", str(s)) if s else "_"

def _current_user():
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"

def _run_archive_dir(archive_base, project, block, rtl, run_name):
    """Canonical on-disk path for one archived run."""
    return os.path.join(archive_base,
                        _safe(project), _safe(block),
                        _safe(rtl), _safe(run_name))

def _atomic_json_write(path, data):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp, path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ArchiveDB  --  thread-safe SQLite wrapper
# ---------------------------------------------------------------------------
class ArchiveDB:
    """
    Thread-safe SQLite wrapper for the lightweight snapshot store.
    Each public method opens its own connection; a lock serializes writes.
    """

    def __init__(self, db_path):
        self._db_path  = db_path
        self._lock     = threading.Lock()
        self._init_db()

    # ── internal ─────────────────────────────────────────────────────────────
    def _connect(self):
        con = sqlite3.connect(self._db_path, check_same_thread=False,
                              timeout=30)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self):
        with self._lock:
            con = self._connect()
            con.executescript(_SCHEMA)
            if not con.execute(
                    "SELECT version FROM schema_version").fetchone():
                con.execute(
                    "INSERT INTO schema_version VALUES (?)",
                    (SCHEMA_VERSION,))
            con.commit()
            con.close()

    # ── write ─────────────────────────────────────────────────────────────────
    def upsert_run(self, run_dict, be_stages=None, has_evidence=False):
        """
        Insert or update a run in the snapshot DB.
        run_dict  : the run dict from the scan (or a subset of it).
        be_stages : list of stage dicts [{name, status, runtime_sec, ...}]
        Returns   : run_id (int)
        """
        r        = run_dict
        now      = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        snap     = json.dumps(r, default=str)
        src_path = r.get("path", "")

        with self._lock:
            con = self._connect()
            cur = con.execute("""
                INSERT INTO runs
                  (project, block, rtl, run_name, source_path, source_type,
                   run_type, status, owner, start_time, end_time, runtime_sec,
                   archived_at, archived_by, has_evidence, snapshot_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_path) DO UPDATE SET
                  status        = excluded.status,
                  owner         = excluded.owner,
                  start_time    = excluded.start_time,
                  end_time      = excluded.end_time,
                  runtime_sec   = excluded.runtime_sec,
                  archived_at   = excluded.archived_at,
                  archived_by   = excluded.archived_by,
                  has_evidence  = MAX(runs.has_evidence, excluded.has_evidence),
                  snapshot_json = excluded.snapshot_json
            """, (
                r.get("project", ""),
                r.get("block",   r.get("ent_name", "")),
                r.get("rtl",     r.get("current_rtl", "")),
                r.get("run_name", os.path.basename(src_path)),
                src_path,
                r.get("source", "WS"),
                r.get("run_type", "FE"),
                r.get("status", ""),
                r.get("owner", ""),
                r.get("start_time", ""),
                r.get("end_time", ""),
                r.get("runtime_sec"),
                now,
                _current_user(),
                1 if has_evidence else 0,
                snap,
            ))
            run_id = cur.lastrowid
            # ON CONFLICT branch leaves lastrowid as 0 -- look it up
            if not run_id:
                row = con.execute(
                    "SELECT id FROM runs WHERE source_path=?",
                    (src_path,)).fetchone()
                run_id = row["id"] if row else None

            if run_id and be_stages:
                for stage in be_stages:
                    con.execute("""
                        INSERT INTO be_stages
                          (run_id, stage_name, status, runtime_sec,
                           start_time, end_time, metrics_json)
                        VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(run_id, stage_name) DO UPDATE SET
                          status       = excluded.status,
                          runtime_sec  = excluded.runtime_sec,
                          start_time   = excluded.start_time,
                          end_time     = excluded.end_time,
                          metrics_json = excluded.metrics_json
                    """, (
                        run_id,
                        stage.get("name", ""),
                        stage.get("status", ""),
                        stage.get("runtime_sec"),
                        stage.get("start_time", ""),
                        stage.get("end_time", ""),
                        json.dumps(stage.get("metrics", {}), default=str),
                    ))
            con.commit()
            con.close()
        return run_id

    def mark_evidence(self, source_path, files_info):
        """Record archived file list after an evidence copy completes."""
        with self._lock:
            con = self._connect()
            row = con.execute(
                "SELECT id FROM runs WHERE source_path=?",
                (source_path,)).fetchone()
            if not row:
                con.close()
                return
            run_id = row["id"]
            for fi in files_info:
                con.execute("""
                    INSERT OR REPLACE INTO archived_files
                      (run_id, rel_path, source_mtime, size_bytes)
                    VALUES (?,?,?,?)
                """, (run_id, fi.get("rel_path", ""),
                      fi.get("mtime"), fi.get("size")))
            con.execute(
                "UPDATE runs SET has_evidence=1 WHERE id=?", (run_id,))
            con.commit()
            con.close()

    # ── read ──────────────────────────────────────────────────────────────────
    def is_archived(self, source_path):
        """
        Returns (has_evidence: bool, archived_at: str|None)
        has_evidence=True means evidence files were physically copied.
        has_evidence=False means only a lightweight snapshot was saved.
        """
        with self._lock:
            con = self._connect()
            row = con.execute(
                "SELECT has_evidence, archived_at FROM runs "
                "WHERE source_path=?", (source_path,)).fetchone()
            con.close()
        if not row:
            return False, None
        return bool(row["has_evidence"]), row["archived_at"]

    def snapshot_exists(self, source_path):
        """True if any record (snapshot or evidence) exists for this path."""
        with self._lock:
            con = self._connect()
            row = con.execute(
                "SELECT 1 FROM runs WHERE source_path=?",
                (source_path,)).fetchone()
            con.close()
        return row is not None

    def get_snapshot(self, source_path):
        """Return the full run_dict stored at archive time, or None."""
        with self._lock:
            con = self._connect()
            row = con.execute(
                "SELECT snapshot_json FROM runs WHERE source_path=?",
                (source_path,)).fetchone()
            con.close()
        if not row:
            return None
        try:
            return json.loads(row["snapshot_json"])
        except Exception:
            return None

    def get_archived_files(self, source_path):
        """Return list of rel_path strings for a run's archived evidence."""
        with self._lock:
            con = self._connect()
            rows = con.execute("""
                SELECT af.rel_path FROM archived_files af
                JOIN runs r ON r.id = af.run_id
                WHERE r.source_path=?
            """, (source_path,)).fetchall()
            con.close()
        return [r["rel_path"] for r in rows]

    def query_runs(self, block=None, rtl=None, run_type=None,
                   has_evidence=None, limit=500):
        """Return list of run record dicts matching optional filters."""
        clauses, params = [], []
        if block:
            clauses.append("block=?"); params.append(block)
        if rtl:
            clauses.append("rtl=?"); params.append(rtl)
        if run_type:
            clauses.append("run_type=?"); params.append(run_type)
        if has_evidence is not None:
            clauses.append("has_evidence=?")
            params.append(1 if has_evidence else 0)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._lock:
            con = self._connect()
            rows = con.execute(
                f"SELECT * FROM runs {where} "
                f"ORDER BY archived_at DESC LIMIT ?",
                params + [limit]).fetchall()
            con.close()
        return [dict(r) for r in rows]

    def stats(self):
        """Return dict of aggregate stats for the status bar / about dialog."""
        with self._lock:
            con = self._connect()
            total    = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            evidence = con.execute(
                "SELECT COUNT(*) FROM runs WHERE has_evidence=1").fetchone()[0]
            files    = con.execute(
                "SELECT COUNT(*) FROM archived_files").fetchone()[0]
            con.close()
        return {"snapshots": total, "with_evidence": evidence,
                "files_archived": files}


# ---------------------------------------------------------------------------
# Evidence copy helpers
# ---------------------------------------------------------------------------

def _collect_fe_files(run_path):
    """Return [(rel_path, abs_path)] for FE capture patterns that exist."""
    results = []
    for pattern in FE_CAPTURE_GLOBS:
        for abs_path in glob.glob(os.path.join(run_path, pattern)):
            if os.path.isfile(abs_path):
                rel = os.path.relpath(abs_path, run_path)
                results.append((rel, abs_path))
    return results


def _collect_be_files(run_path, stage_names):
    """Return [(rel_path, abs_path)] for BE stage capture patterns."""
    results = []
    for s in (stage_names or []):
        for tpl in BE_STAGE_CAPTURE_GLOBS:
            for abs_path in glob.glob(
                    os.path.join(run_path, tpl.format(s=s))):
                if os.path.isfile(abs_path):
                    rel = os.path.relpath(abs_path, run_path)
                    results.append((rel, abs_path))
    return results


def archive_evidence(run_dict, archive_base, db,
                     stage_names=None, progress_cb=None):
    """
    Copy key report/log files for one run to archive_base and update DB.

    run_dict     : run dict from scan
    archive_base : root of the archive tree
    db           : ArchiveDB instance (or None to skip DB update)
    stage_names  : list of BE stage name strings (BE runs only)
    progress_cb  : callable(done_idx, total, rel_path) for progress updates

    Returns list of file_info dicts that were written.
    """
    run_path  = run_dict.get("path", "")
    run_type  = run_dict.get("run_type", "FE")
    run_name  = run_dict.get("run_name",
                             os.path.basename(run_path))
    project   = run_dict.get("project", "")
    block     = run_dict.get("block", run_dict.get("ent_name", ""))
    rtl       = run_dict.get("rtl",   run_dict.get("current_rtl", ""))

    dest_root = _run_archive_dir(archive_base, project, block, rtl, run_name)
    os.makedirs(dest_root, exist_ok=True)

    # Collect files to copy
    if run_type == "FE":
        pairs = _collect_fe_files(run_path)
    else:
        pairs = _collect_be_files(run_path, stage_names or [])

    files_info = []
    for i, (rel, src) in enumerate(pairs):
        if progress_cb:
            progress_cb(i, len(pairs), rel)
        dest = os.path.join(dest_root, rel)
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            stat = os.stat(src)
            tmp  = dest + ".tmp"
            shutil.copy2(src, tmp)
            os.replace(tmp, dest)
            files_info.append({
                "rel_path": rel,
                "source":   src,
                "archive":  dest,
                "mtime":    stat.st_mtime,
                "size":     stat.st_size,
            })
        except Exception:
            pass  # missing / unreadable file -- skip silently

    # Write manifest.json  (always, even if no files copied)
    manifest = {
        "project":     project,
        "block":       block,
        "rtl":         rtl,
        "run_name":    run_name,
        "run_type":    run_type,
        "source_path": run_path,
        "archived_at": datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"),
        "archived_by": _current_user(),
        "files": [
            {"source": fi["source"],
             "archive": fi["rel_path"],
             "mtime":   fi["mtime"],
             "size":    fi["size"]}
            for fi in files_info
        ],
    }
    _atomic_json_write(os.path.join(dest_root, "manifest.json"), manifest)

    # Write metrics snapshot (whatever was extracted at archive time)
    _atomic_json_write(os.path.join(dest_root, "metrics.json"),
                       run_dict.get("metrics", {}))

    # Update DB
    if db:
        db.upsert_run(run_dict, has_evidence=True)
        db.mark_evidence(run_path, files_info)

    return files_info


# ---------------------------------------------------------------------------
# ArchiveWorker  --  background QThread
# ---------------------------------------------------------------------------
class ArchiveWorker(QThread):
    """
    Archives a list of run_dicts in a background thread.

    Signals:
      progress(done, total, run_name)  -- emitted after each run
      finished(count_ok, errors)       -- emitted when done
    """
    progress = pyqtSignal(int, int, str)   # done, total, current run name
    finished = pyqtSignal(int, list)       # count_ok, list of error strings

    def __init__(self, run_dicts, archive_base, db,
                 evidence=True, parent=None):
        super().__init__(parent)
        self._runs         = run_dicts
        self._archive_base = archive_base
        self._db           = db
        self._evidence     = evidence   # False = snapshot-only (faster)
        self._cancelled    = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        errors = []
        total  = len(self._runs)
        done   = 0

        for run_dict in self._runs:
            if self._cancelled:
                break

            run_name = run_dict.get(
                "run_name",
                os.path.basename(run_dict.get("path", "run")))
            self.progress.emit(done, total, run_name)

            try:
                run_path = run_dict.get("path", "")

                if self._evidence and run_path and os.path.isdir(run_path):
                    # Level 2: copy evidence files
                    stage_names = [
                        s["name"] for s in run_dict.get("stages", [])
                        if isinstance(s, dict) and s.get("name")
                    ]
                    archive_evidence(run_dict, self._archive_base,
                                     self._db, stage_names=stage_names)
                else:
                    # Level 1: snapshot only
                    self._db.upsert_run(run_dict)

            except Exception as exc:
                errors.append(f"{run_name}: {exc}")

            done += 1

        self.progress.emit(done, total, "")
        self.finished.emit(done, errors)


# ---------------------------------------------------------------------------
# Module-level DB singleton
# ---------------------------------------------------------------------------
_db_instance      = None
_db_instance_lock = threading.Lock()


def get_archive_db(archive_base):
    """
    Return (and lazily initialise) the module-level ArchiveDB.
    Calling this multiple times with the same archive_base is safe.
    Call reset_archive_db() first if archive_base changes at runtime.
    """
    global _db_instance
    with _db_instance_lock:
        if _db_instance is None:
            os.makedirs(archive_base, exist_ok=True)
            _db_instance = ArchiveDB(
                os.path.join(archive_base, DB_FILENAME))
    return _db_instance


def reset_archive_db():
    """Force re-creation of the DB singleton (e.g. after config change)."""
    global _db_instance
    with _db_instance_lock:
        _db_instance = None
