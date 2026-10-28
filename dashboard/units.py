"""Numbers as a reader wants them: ``12.4M`` rows, ``4m05s``, ``▲12%`` against before."""

from datetime import datetime, timedelta


def change(now: float, before: float) -> str:
    """``▲12%`` against the period before; ``new`` when there was nothing then."""
    if not before:
        return "new" if now else "-"
    delta = (now - before) / before
    return f"{'▲' if delta > 0 else '▼' if delta < 0 else '='}{abs(delta):.0%}"


def human(count: float) -> str:
    """``12.4M``, ``3.1K``, ``42``."""
    for size, unit in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(count) >= size:
            return f"{count / size:.1f}{unit}"
    return f"{count:.0f}"


def seconds(value: float | None) -> str:
    """``3h12m``, ``4m05s``, ``12s``, or ``-``."""
    if value is None:
        return "-"
    whole = int(value)
    if whole >= 3600:
        return f"{whole // 3600}h{whole % 3600 // 60:02d}m"
    if whole >= 60:
        return f"{whole // 60}m{whole % 60:02d}s"
    return f"{value:.1f}s" if value < 10 else f"{whole}s"


def age(delta: timedelta | None) -> str:
    return "never" if delta is None else seconds(delta.total_seconds())


def mb(value: float | None) -> str:
    """``4.4G``, ``2.25G``, or ``512M``; ``-`` when unknown."""
    if value is None or value != value:  # NaN
        return "-"
    if value < 1024:
        return f"{value:.0f}M"
    return f"{value / 1024:.2f}".rstrip("0").rstrip(".") + "G"  # 2.25G is not rounded to 2.2G


def first_line(text: str | None) -> str:
    """The first line of a message, which says what happened; the rest is detail."""
    lines = (text or "").splitlines()
    return lines[0] if lines else ""


def edge(text: str | None) -> str:
    """A window edge or cursor value, short: ``2026-09-30 00:00`` for an ISO instant (UTC), the
    text itself otherwise; ``-`` for none."""
    if text is None:
        return "-"
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return f"{moment:%Y-%m-%d %H:%M}" + (f":{moment:%S}" if moment.second else "")
