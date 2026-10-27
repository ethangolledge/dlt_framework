"""Background sampling of this process's memory and CPU."""

import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import psutil

_MB = 1024**2
_UNLIMITED = 2**60  # cgroup v1 reports "no limit" as a huge number


class ResourceSampler:
    """Calls ``on_sample(measures)`` every ``interval_s`` seconds while active.

    Memory is the process RSS; the limit is the container's cgroup limit, never host RAM, so
    ``rss_mb / memory_limit_mb`` is how close the run is to an OOM kill, and outside a container
    ``system_available_mb`` says how much the host could still give. CPU is the percentage of
    one core averaged over the interval (four busy cores read 400); IO is bytes read and written
    by the process per second since the previous sample, where the platform reports it. The
    first sample comes one interval in, and a last one is taken on exit, so every run has at
    least one. All are this process's: pipelines running in one process share them.
    ``cpu_seconds`` counts from entry.
    """

    def __init__(self, interval_s: float, on_sample: Callable[[dict[str, Any]], None]) -> None:
        self.interval_s = interval_s
        self.on_sample = on_sample
        self.peak_rss_mb = 0.0
        self._process = psutil.Process()
        self._cpu_at_start = 0.0
        self._io_at: tuple[float, int, int] | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="resource-sampler", daemon=True)

    def __enter__(self) -> Self:
        self._process.cpu_percent()  # first call only primes the measurement
        self._cpu_at_start = self.cpu_used()
        self._io_rates()  # primes IO rates the same way
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        """Stop sampling and take the final sample; safe to call more than once."""
        if self._stop.is_set():
            return
        self._stop.set()
        self._thread.join()
        self.sample()

    @property
    def cpu_seconds(self) -> float:
        """CPU time used since sampling started."""
        return self.cpu_used() - self._cpu_at_start

    def cpu_used(self) -> float:
        """This process's CPU time so far, all threads, user and system."""
        times = self._process.cpu_times()
        return float(times.user + times.system)

    @staticmethod
    def host_memory_mb() -> float:
        return float(psutil.virtual_memory().total / _MB)

    @staticmethod
    def memory_limit_mb(root: Path = Path("/sys/fs/cgroup")) -> float | None:
        """The cgroup (v2, then v1) memory limit, or ``None`` when unlimited or not in a cgroup."""
        for limit in (root / "memory.max", root / "memory" / "memory.limit_in_bytes"):
            try:
                text = limit.read_text().strip()
            except OSError:
                continue
            if text == "max" or int(text) >= _UNLIMITED:
                return None
            return int(text) / _MB
        return None

    def sample(self) -> None:
        rss_mb = self._process.memory_info().rss / _MB
        self.peak_rss_mb = max(self.peak_rss_mb, rss_mb)
        system = psutil.virtual_memory()
        self.on_sample(
            {
                "rss_mb": rss_mb,
                "cpu_percent": self._process.cpu_percent(),
                "threads": self._process.num_threads(),
                **self._io_rates(),
                "system_available_mb": system.available / _MB,
                "system_total_mb": system.total / _MB,
            }
        )

    def _io_rates(self) -> dict[str, float | None]:
        """MB/s read and written since the previous sample; null on the first, or where the
        platform has no per-process IO counters (macOS)."""
        counters = getattr(self._process, "io_counters", None)
        if counters is None:
            return {"read_mb_s": None, "write_mb_s": None}
        now, io = time.monotonic(), counters()
        previous, self._io_at = self._io_at, (now, io.read_bytes, io.write_bytes)
        if previous is None or now <= previous[0]:
            return {"read_mb_s": None, "write_mb_s": None}
        elapsed = now - previous[0]
        return {
            "read_mb_s": (io.read_bytes - previous[1]) / _MB / elapsed,
            "write_mb_s": (io.write_bytes - previous[2]) / _MB / elapsed,
        }

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            self.sample()
