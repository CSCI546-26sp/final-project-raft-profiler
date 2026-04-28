
from __future__ import annotations

import functools
import inspect
import logging
import os
import sys
import time
from typing import Any, Callable, Optional

import ray

_TRACKER_NAME = "_raft_user_fn_tracker"
_TRACKER_NAMESPACE = "raft_profiler"
_log = logging.getLogger(__name__)
_DEBUG = bool(os.environ.get("RAFT_PROFILER_DEBUG"))


def _dbg(msg: str) -> None:
    if _DEBUG:
        print(f"[raft-profiler] {msg}", file=sys.stderr, flush=True)
_WRAPS_ASSIGN = tuple(a for a in functools.WRAPPER_ASSIGNMENTS if a != "__module__")


@ray.remote(num_cpus=0)
class _UserFnTracker:
    """Collects per-call records from `@track_function`-decorated callables."""

    def __init__(self) -> None:
        self._records: list[dict] = []

    def record(
        self,
        fn_name: str,
        source: str,
        start_ms: float,
        end_ms: float,
        pid: int,
    ) -> None:
        self._records.append({
            "fn_name":    fn_name,
            "source":     source,
            "start_ms":   start_ms,
            "end_ms":     end_ms,
            "elapsed_ms": end_ms - start_ms,
            "pid":        pid,
        })

    def get_all(self) -> list[dict]:
        return list(self._records)

    def ping(self) -> bool:
        return True
_driver_handle: Optional[Any] = None


def ensure_tracker() -> Optional[Any]:
    
    global _driver_handle
    if not ray.is_initialized():
        _dbg("ensure_tracker: ray not initialized")
        return None
    if _driver_handle is not None:
        return _driver_handle
    try:
        _driver_handle = ray.get_actor(_TRACKER_NAME, namespace=_TRACKER_NAMESPACE)
        _dbg(f"ensure_tracker: found existing actor {_TRACKER_NAME}")
        return _driver_handle
    except Exception:
        pass
    try:
        _driver_handle = _UserFnTracker.options(
            name=_TRACKER_NAME,
            namespace=_TRACKER_NAMESPACE,
            get_if_exists=True,
        ).remote()
                                                                           
                                                                          
        try:
            ray.get(_driver_handle.ping.remote(), timeout=10)
            _dbg(f"ensure_tracker: created actor {_TRACKER_NAME} "
                 f"(namespace={_TRACKER_NAMESPACE})")
        except Exception as e:
            _dbg(f"ensure_tracker: ping failed: {e!r}")
        return _driver_handle
    except Exception as e:
        _log.debug("could not create user-fn tracker: %s", e)
        _dbg(f"ensure_tracker: create failed: {e!r}")
        _driver_handle = None
        return None


def enable_pickle_by_value() -> None:
    import sys
    me = sys.modules[__name__]
    for mod_path in ("cloudpickle", "ray.cloudpickle"):
        try:
            cp = __import__(mod_path, fromlist=["register_pickle_by_value"])
            cp.register_pickle_by_value(me)
        except Exception as e:
            _log.debug("register-by-value (%s) skipped: %s", mod_path, e)


def _get_tracker() -> Optional[Any]:
    if not ray.is_initialized():
        return None                                                       
    if _driver_handle is not None:
        return _driver_handle
    try:
        return ray.get_actor(_TRACKER_NAME, namespace=_TRACKER_NAMESPACE)
    except Exception:
        return None


def fetch_records() -> list[dict]:
    """Pulls every record the tracker has seen so far. Returns [] on error."""
    h = _get_tracker()
    if h is None:
        _dbg("fetch_records: tracker actor not found")
        return []
    try:
        recs = ray.get(h.get_all.remote(), timeout=10)
        _dbg(f"fetch_records: pulled {len(recs)} record(s)")
        return recs
    except Exception as e:
        _dbg(f"fetch_records: failed: {e!r}")
        return []


def is_tracker_task(name: Optional[str]) -> bool:
    if not name:
        return False
    return name.startswith("_UserFnTracker.") or name == "_UserFnTracker"


def _source_of(fn: Callable) -> str:
    try:
        path = inspect.getsourcefile(fn) or inspect.getfile(fn)
        line = fn.__code__.co_firstlineno
        return f"{os.path.basename(path)}:{line}"
    except Exception:
        return "<unknown>"


def track_function(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
):
    def decorator(f: Callable) -> Callable:
        fn_name = name or getattr(f, "__qualname__", None) or f.__name__
        source = _source_of(f)                           
        @functools.wraps(f, assigned=_WRAPS_ASSIGN)
        def wrapper(*args, **kwargs):
            t0 = time.time() * 1000.0
            try:
                return f(*args, **kwargs)
            finally:
                t1 = time.time() * 1000.0
                tracker = _get_tracker()
                if tracker is None:
                    _dbg(f"wrapper[{fn_name}]: no tracker actor visible "
                         f"from pid={os.getpid()}")
                else:
                    try:
                                                                            
                                                                             
                        ray.get(
                            tracker.record.remote(
                                fn_name, source, t0, t1, os.getpid()
                            ),
                            timeout=2,
                        )
                        _dbg(f"wrapper[{fn_name}]: recorded "
                             f"{t1 - t0:.1f}ms (pid={os.getpid()})")
                    except Exception as e:
                        _dbg(f"wrapper[{fn_name}]: record failed: {e!r}")

        wrapper.__raft_user_fn__ = True
        wrapper.__raft_fn_name__ = fn_name
        wrapper.__raft_source__ = source
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator
