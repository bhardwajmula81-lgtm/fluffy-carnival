# -*- coding: utf-8 -*-
# Best-effort debug logging for Flow Pulse.

import os
import time
import traceback
import threading

_LOG_LOCK = threading.Lock()


def _debug_log_path():
    base = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base, "dashboard_notes", "debug")
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    except Exception:
        return None
    return os.path.join(log_dir, "flow_pulse_debug.log")


def debug_log(context, exc=None):
    """Write non-fatal diagnostic context to the Flow Pulse debug log."""
    try:
        path = _debug_log_path()
        if not path:
            return
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = "[{}] {}".format(stamp, context)
        if exc is not None:
            line += " | {}: {}".format(exc.__class__.__name__, exc)
        with _LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                if exc is not None:
                    f.write(traceback.format_exc() + "\n")
    except Exception:
        pass
