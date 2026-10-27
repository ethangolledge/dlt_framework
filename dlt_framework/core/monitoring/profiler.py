"""On-demand profiling: which functions a run spends its time in, across all threads."""

import sys
import threading
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

from dlt_framework.core.errors import code_path

# Innermost frames of a thread with nothing to do: an idle pool worker (blocked in C inside
# concurrent/futures/thread.py), or a thread waiting on another. They are left out, or they
# would outweigh the work.
_IDLE = ("threading.py", "queue.py", "selectors.py", "thread.py")
_TOP = 100  # functions kept per step, by total samples

Key = tuple[str, str, int]  # function, file, line


class StackSampler:
    """Samples the stack of every thread every ``interval_s``, from a thread of its own, and
    counts each function's own and total samples under the step ``phase()`` names then.

    Sampling stacks (``sys._current_frames``) sees every thread, where ``cProfile`` sees only
    the one that enabled it, and dlt extracts in many. It costs one stack walk per thread per
    interval, so it is off unless a run asks for it.
    """

    def __init__(self, interval_s: float, phase: Callable[[], str | None]) -> None:
        self.interval_s = interval_s
        self.phase = phase
        self.own: Counter[tuple[str | None, Key]] = Counter()
        self.total: Counter[tuple[str | None, Key]] = Counter()
        self.samples: Counter[str | None] = Counter()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="stack-sampler", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()

    def top(self) -> list[dict[str, Any]]:
        """The ``_TOP`` functions of each step by total samples, with their counts and shares."""
        rows = []
        for step, count in self.samples.items():
            keys = sorted(
                (key for phase, key in self.total if phase == step),
                key=lambda key: -self.total[step, key],
            )[:_TOP]
            for key in keys:
                function, file, line = key
                own, total = self.own[step, key], self.total[step, key]
                rows.append(
                    {
                        "step": step,
                        "function": function,
                        "file": file,
                        "line": line,
                        "own_samples": own,
                        "total_samples": total,
                        "own_share": own / count,
                        "total_share": total / count,
                    }
                )
        return rows

    def _loop(self) -> None:
        skip = {threading.get_ident()}
        while not self._stop.wait(self.interval_s):
            step = self.phase()
            for ident, frame in sys._current_frames().items():
                if ident in skip or _idle(frame):
                    continue
                self.samples[step] += 1
                self.own[step, _key(frame)] += 1
                seen: set[Key] = set()
                current: FrameType | None = frame
                while current is not None:
                    key = _key(current)
                    if key not in seen:  # recursion counts once
                        seen.add(key)
                        self.total[step, key] += 1
                    current = current.f_back


def _idle(frame: FrameType) -> bool:
    return Path(frame.f_code.co_filename).name in _IDLE


def _key(frame: FrameType) -> Key:
    code = frame.f_code
    module = frame.f_globals.get("__name__", "?")
    return f"{module}:{code.co_qualname}", code_path(code.co_filename), code.co_firstlineno
