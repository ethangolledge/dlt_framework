"""Numbers as a reader wants them: ``4m05s``, ``4.4G``, ``3d ago``."""

from datetime import datetime, timedelta


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


def ago(delta: timedelta) -> str:
    """``3d ago``, ``5h ago``, or ``just now``."""
    for size, unit in ((86400, "d"), (3600, "h"), (60, "m")):
        if delta.total_seconds() >= size:
            return f"{int(delta.total_seconds() // size)}{unit} ago"
    return "just now"


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
