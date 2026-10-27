"""The ASCII primitives: exact text, fixed widths, and both encoders."""

from datetime import UTC, datetime, timedelta

import pytest

from dashboard.render import primitives as draw
from dashboard.render.encode import ansi, html
from dashboard.render.text import block_width, fit, hstack, plain, span, width

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def test_lines_fit_crop_and_pad_to_their_width() -> None:
    assert plain([fit(span("abc"), 5)]) == "abc  "
    assert plain([fit(span("abc"), 5, "right")]) == "  abc"
    assert plain([fit(span("abcdef"), 4)]) == "abc…"
    assert width(fit(span("x"), 7, "center")) == 7


def test_a_panel_is_exactly_as_wide_as_asked_whatever_its_body() -> None:
    block = draw.panel("TITLE", [span("short"), span("x" * 200)], 30)
    assert {width(line) for line in block} == {30}
    top = plain(block).splitlines()[0]
    assert top.startswith("┌─ TITLE ──") and top.endswith("─┐")


def test_sparklines_scale_to_their_range_and_keep_peaks_when_shrinking() -> None:
    assert plain([draw.sparkline([0, 1, 2, 3, 4, 5, 6, 7], 8)]) == "▁▂▃▄▅▆▇█"
    assert plain([draw.sparkline([1, None, 1], 3)]) == "▁ ▁"
    assert draw.resample([1, 9, 1, 1], 2) == [9, 1]  # the spike survives


def test_bars_fill_in_eighths_against_the_largest() -> None:
    assert plain([draw.bar(0.5, 8)]) == "████░░░░"
    assert plain([draw.bar(1 / 16, 8)]) == "▌░░░░░░░"
    rows = draw.bars([("a", 2.0, "2", "ok"), ("b", 1.0, "1", "ok")], 30, label=4)
    assert block_width(rows) == 30 and plain(rows).count("█") > plain(rows[1:]).count("█")


def test_a_gantt_places_intervals_on_a_shared_time_axis() -> None:
    lanes = [
        ("a", [(T0, T0 + timedelta(hours=1), "█", "ok")]),
        ("b", [(T0 + timedelta(hours=3), T0 + timedelta(hours=4), "✖", "error")]),
    ]
    block = draw.gantt(lanes, T0, T0 + timedelta(hours=4), 20, label=2)
    first, second = plain(block).splitlines()[:2]
    assert first.index("█") < second.index("✖")
    assert "00:00" in plain(block).splitlines()[-1]


@pytest.mark.parametrize(
    ("seconds", "tick"), [(30, "+15.0s"), (3600, "00:30:00"), (172800, "01-02 00:00")]
)
def test_time_axes_label_by_span(seconds, tick) -> None:
    assert tick in plain([draw.time_axis(T0, T0 + timedelta(seconds=seconds), 40)])


def test_braille_charts_draw_lines_a_limit_and_say_when_empty() -> None:
    block = draw.chart([([0, 1, 2, 3, 4], "ok")], 30, 4, limit=(5.0, "error"))
    text = plain(block)
    assert any("⠀" < char <= "⣿" for char in text) and "·" in text
    assert plain(draw.chart([([None], "ok")], 20, 3)).strip() == "NO SIGNAL"


def test_tables_align_and_say_when_empty() -> None:
    columns = [draw.Column("name", 6), draw.Column("n", 3, "right")]
    assert plain(draw.table(columns, [["ab", "1"]])).splitlines() == [
        "NAME     N ",
        "───────────",
        "ab       1 ",
    ]
    assert "NO SIGNAL" in plain(draw.table(columns, []))


def test_wrap_breaks_between_words_with_an_indent() -> None:
    assert plain(draw.wrap("one two three four", 9, indent="  ")) == "one two\n  three\n  four"


def test_side_by_side_falls_back_to_stacked_when_too_wide() -> None:
    left, right = [span("a" * 10)], [span("b" * 10)]
    assert plain(draw.side_by_side([left, right], 30)) == "a" * 10 + " " + "b" * 10
    assert plain(draw.side_by_side([left, right], 15)) == "a" * 10 + "\n" + "b" * 10
    assert plain(hstack([[span("x")], [span("y"), span("z")]])) == "x y\n  z"


def test_html_escapes_styles_and_shows_tooltips_from_the_style() -> None:
    out = html([[("<b>", "ok|run 1 & more"), (" plain", None)]])
    assert out.startswith('<pre class="term"')
    assert '<span class="t-ok" title="run 1 &amp; more">&lt;b&gt;</span> plain' in out


def test_ansi_colours_known_styles_or_writes_plain_text() -> None:
    assert ansi([[("ok", "ok"), (" x", "nope")]]) == "\x1b[38;2;57;255;20mok\x1b[0m x"
    assert ansi([[("ok", "ok|tip")]], colour=False) == "ok"
