"""Small, Qt-independent helpers shared by dashboard workers (Python 3.6)."""
import os
import subprocess
import threading
import time


_DISK_SLOT = threading.Semaphore(1)


def format_disk_bytes(value):
    if value is None or value < 0:
        return "N/A"
    if value == 0:
        return "0B"
    value = float(value)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if value < 1024.0 or unit == "P":
            return "{:.1f}{}".format(value, unit)
        value /= 1024.0


def disk_record_fresh(record, max_age=3600, now=None):
    if not record or record.get("size_status") != "updated":
        return False
    if record.get("size_bytes") is None:
        return False
    age = (time.time() if now is None else now) - float(record.get("measured_at", 0))
    return 0 <= age < max_age


def measure_disk_usage(path, cancelled=lambda: False, timeout=60):
    """Allocated bytes from du only. Failures never masquerade as exact sizes.

    One recursive disk job runs at a time, across all dashboard size workers.
    No Python traversal fallback: that both doubled NFS work and changed units.
    """
    result = {"path": path, "size_bytes": None, "size_status": "error", "error": ""}
    if not path or path in ("N/A", "-"):
        result["error"] = "No run path"
        return result
    while not _DISK_SLOT.acquire(timeout=0.1):
        if cancelled():
            result["size_status"] = "cancelled"
            return result
    process = None
    try:
        if cancelled():
            result["size_status"] = "cancelled"
            return result
        process = subprocess.Popen(["du", "-sk", "--", path],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.monotonic() + timeout
        while True:
            if cancelled() or time.monotonic() >= deadline:
                result["size_status"] = "cancelled" if cancelled() else "timeout"
                result["error"] = "Cancelled" if cancelled() else "Disk scan timed out"
                process.kill()
                process.communicate()
                return result
            try:
                output, error = process.communicate(timeout=min(0.2, max(0.001, deadline - time.monotonic())))
                break
            except subprocess.TimeoutExpired:
                continue
        if process.returncode != 0:
            result["error"] = error.decode("utf-8", "replace").strip()[:300] or "du failed"
            return result
        kb = int(output.split(None, 1)[0])
        if kb < 0:
            raise ValueError("Negative disk usage")
        result.update(size_bytes=kb * 1024, size_status="updated",
                      measured_at=time.time(), error="")
        return result
    except Exception as exc:
        result["error"] = str(exc)[:300]
        return result
    finally:
        _DISK_SLOT.release()


def nonoverlapping_disk_records(cache):
    """Exclude nested run paths from totals; retain each record for tree cells.

    This is lexical containment, not symlink/hardlink resolution. Totals describe
    the selected run directories, not unique blocks across an entire filesystem.
    """
    accepted = set()
    for path, record in sorted(cache.items(), key=lambda pair: (len(pair[0]), pair[0])):
        norm = os.path.normpath(path)
        parent = os.path.dirname(norm)
        nested = False
        while parent and parent != os.path.dirname(parent):
            if parent in accepted:
                nested = True
                break
            parent = os.path.dirname(parent)
        if nested or norm in accepted:
            continue
        if record.get("size_bytes") is None and record.get("size_gb") is None:
            continue
        accepted.add(norm)
        yield path, record
