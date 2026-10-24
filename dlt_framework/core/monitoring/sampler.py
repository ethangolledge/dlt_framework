"""Background sampling of this process's memory and CPU."""

import threading
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Self

import psutil

_MB = 1024**2
_UNLIMITED = 2**60  # cgroup v1 reports "no limit" as a huge number


class ResourceSampler:
    """Calls ``on_sample(rss_mb, cpu_percent)`` every ``interval_s`` seconds while active.

    Memory is the process RSS; the limit is the container's cgroup limit, never host RAM, so
    ``rss_mb / memory_limit_mb`` is how close the run is to an OOM kill. CPU is the percentage of
    one core averaged over the interval (four busy cores read 400). The first sample comes one
    interval in, and a last one is taken on exit, so every run has at least one.
    """

    def __init__(self, interval_s: float, on_sample: Callable[[float, float], None]) -> None:
        self.interval_s = interval_s
        self.on_sample = on_sample
        self.peak_rss_mb = 0.0
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="resource-sampler", daemon=True)

    def __enter__(self) -> Self:
        self._process.cpu_percent()  # first call only primes the measurement
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
        times = self._process.cpu_times()
        return float(times.user + times.system)

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
        self.on_sample(rss_mb, self._process.cpu_percent())

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            self.sample()
