"""Styled text for the terminal: exact widths, and colour only where asked."""

from dashboard.render.encode import ansi
from dashboard.render.text import fit, join, plain, span, width


def test_lines_fit_crop_and_pad_to_their_width() -> None:
    assert plain([fit(span("abc"), 5)]) == "abc  "
    assert plain([fit(span("abc"), 5, "right")]) == "  abc"
    assert plain([fit(span("abcdef"), 4)]) == "abc…"
    assert width(fit(join(span("x"), span("y", "ok")), 7, "center")) == 7


def test_ansi_colours_known_styles_or_writes_plain_text() -> None:
    assert ansi([[("ok", "ok"), (" x", "nope")]]) == "\x1b[38;2;57;255;20mok\x1b[0m x"
    assert ansi([[("ok", "ok|tip")]], colour=False) == "ok"
