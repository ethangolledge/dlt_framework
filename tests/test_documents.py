"""Documents: stated layouts read locally, other layouts by a model, files released only once
their rows are committed."""

import io
import json
import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import BaseModel

from dlt_framework.cli import main
from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    AwareDatetime,
    DuckDb,
    Merge,
    Pipeline,
    Resource,
    TimeCursor,
)
from dlt_framework.core.errors import GuardError, TerminalRunError
from dlt_framework.core.run import IncrementalRun
from dlt_framework.sources.filesystem import Files, FilesystemSource
from dlt_framework.sources.filesystem.documents import (
    Archive,
    Delete,
    Documents,
    Keep,
    Model,
    Ocr,
    Table,
    Template,
    Text,
)
from dlt_framework.sources.scrape import ScrapeSource, ScrapeSpider


class Line(BaseModel):
    item: str
    amount: Decimal


class Invoice(BaseModel):
    number: str
    total: Decimal
    lines: list[Line] = []


ACME = Template(
    name="acme",
    match="ACME Ltd",
    fields={
        "number": Text(r"Invoice (\S+)"),
        "total": Text(r"Total (\S+)"),
        "lines": Table(columns={"item": 50, "amount": 200}, key="amount", starts=r"[\d.]+"),
    },
)
KEEP = Keep()
MODEL = Model(name="claude-haiku-5-5", instructions="Extract the invoice.")


def pdf(lines: list[str], table: list[list[str]] | None = None) -> bytes:
    """A one-page PDF: ``lines`` of text from the top, then ``table`` as a ruled grid."""
    ops = ["BT /F1 11 Tf 14 TL 50 780 Td", *(f"({line}) Tj T*" for line in lines), "ET"]
    for r, row in enumerate(table or []):
        for c, cell in enumerate(row):
            x, y = 50 + c * 150, 600 - r * 20
            ops += [f"{x} {y} 150 20 re S", f"BT /F1 10 Tf {x + 4} {y + 6} Td ({cell}) Tj ET"]
    stream = "\n".join(ops)
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 842] "
        "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
    ]
    out, offsets = "%PDF-1.4\n", []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n"
    out += "".join(f"{offset:010} 00000 n \n" for offset in offsets)
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    return out.encode()


def scan(lines: list[str]) -> bytes:
    """A one-page PDF holding only an image of ``lines``: no text layer, as a scanner makes."""
    from PIL import Image, ImageDraw, ImageFont

    image = Image.new("L", (1240, 1754), "white")
    font = ImageFont.load_default(size=40)
    for number, line in enumerate(lines):
        ImageDraw.Draw(image).text((100, 100 + number * 70), line, fill="black", font=font)
    out = io.BytesIO()
    image.save(out, "PDF", resolution=150)
    return out.getvalue()


ACME_PDF = pdf(
    ["ACME Ltd", "Invoice A-1", "Total 30.50"],
    [["item", "amount"], ["bolts", "10.50"], ["nuts", "20.00"], ["(steel)", ""]],
)
GLOBEX_PDF = pdf(["Globex Corporation", "Bill number G-7, amount due 99.00"])


def pipeline(folder, extract, on_invalid="raise", once_loaded=KEEP, ocr=None) -> Pipeline:
    resource = Resource(
        name="invoices",
        write=Merge(primary_key=("document",), strategy="delete-insert"),
        cursor=TimeCursor(
            path="modification_date",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            format=AwareDatetime(),
            on_missing="raise",
        ),
        empty="warn",
    )
    reader = Documents(
        schema=Invoice, extract=extract, on_invalid=on_invalid, once_loaded=once_loaded, ocr=ocr
    )
    files = Files(bucket_url=folder.as_uri(), file_glob="*.pdf", reader=reader)
    return Pipeline(
        name="docs",
        target=DuckDb(dataset="raw_docs"),
        schema_contract=FREEZE_DATA_TYPES,
        source=FilesystemSource(name="docs", resources={resource: files}),
    )


@pytest.fixture
def folder(tmp_path):
    path = tmp_path / "landing"
    path.mkdir()
    return path


@pytest.fixture
def claude(site, monkeypatch):
    """A local Messages API answering every request with the Globex invoice."""
    invoice = {"number": "G-7", "total": "99.00", "lines": []}
    site.pages["/v1/messages"] = (
        200,
        json.dumps(
            {
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": MODEL.name,
                "content": [{"type": "text", "text": json.dumps(invoice)}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        ),
    )
    monkeypatch.setenv("ANTHROPIC_BASE_URL", site.url)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    return site


def test_stated_layouts_are_read_locally_and_others_by_the_model(folder, claude, query) -> None:
    (folder / "acme.pdf").write_bytes(ACME_PDF)
    (folder / "globex.pdf").write_bytes(GLOBEX_PDF)
    (folder / "scan.pdf").write_bytes(scan(["Globex Corporation"]))  # no text: sent as pages
    assert IncrementalRun(pipeline(folder, (ACME, MODEL)))() == {"invoices": 3}
    assert query("SELECT document, total, extracted_by FROM raw_docs.invoices ORDER BY 1") == [
        ("acme.pdf", Decimal("30.50"), "acme"),
        ("globex.pdf", Decimal("99.00"), MODEL.name),
        ("scan.pdf", Decimal("99.00"), MODEL.name),
    ]
    assert query("SELECT item FROM raw_docs.invoices__lines ORDER BY item") == [
        ("bolts",),
        ("nuts (steel)",),  # a wrapped cell continues its row
    ]
    assert len(claude.paths("POST")) == 2  # only the layouts no template read were sent


def test_an_unreadable_document_fails_the_run_unless_stated_otherwise(folder, query) -> None:
    (folder / "acme.pdf").write_bytes(ACME_PDF)
    (folder / "globex.pdf").write_bytes(GLOBEX_PDF)
    with pytest.raises(TerminalRunError):
        IncrementalRun(pipeline(folder, (ACME,)))()

    Pipeline.registry.clear()
    assert IncrementalRun(pipeline(folder, (ACME,), on_invalid="warn"))() == {"invoices": 1}
    assert (folder / "globex.pdf").exists()


def test_read_documents_are_deleted_only_once_their_rows_are_committed(folder, query) -> None:
    document = folder / "acme.pdf"
    document.write_bytes(ACME_PDF)
    future = time.time() + 7 * 86400  # a modification date the future-cursor guard refuses
    os.utime(document, (future, future))
    definition = pipeline(folder, (ACME,), once_loaded=Delete())
    with pytest.raises(GuardError):
        IncrementalRun(definition)()
    assert document.exists()

    os.utime(document)
    assert IncrementalRun(definition)() == {"invoices": 1}
    assert not document.exists()


def test_archived_documents_move_to_the_archive_once_committed(folder, tmp_path, query) -> None:
    (folder / "acme.pdf").write_bytes(ACME_PDF)
    archive = tmp_path / "archive"
    definition = pipeline(folder, (ACME,), once_loaded=Archive(archive.as_uri()))
    assert IncrementalRun(definition)() == {"invoices": 1}
    assert not (folder / "acme.pdf").exists()
    assert (archive / "acme.pdf").read_bytes() == ACME_PDF


def test_a_crawl_downloads_documents_that_are_then_extracted_and_removed(
    site, folder, query
) -> None:
    """The whole flow: a spider saves the PDFs it finds, a Documents reader loads them."""
    site.pages["/filings"] = (200, '<a class="pdf" href="/files/acme.pdf">ACME</a>')
    site.pages["/files/acme.pdf"] = (200, ACME_PDF.decode())

    class Filings(ScrapeSpider):
        name = "filings"
        start_urls = (f"{site.url}/filings",)
        robots_txt_obey = True
        failed_requests = "raise"
        documents = folder.as_uri()

        async def parse(self, response):
            for href in response.css("a.pdf::attr(href)").getall():
                yield response.follow(href, callback=self.download)

        async def download(self, response):
            yield {"url": response.url, "saved_as": await self.save(response)}

    crawl = Pipeline(
        name="filings",
        target=DuckDb(dataset="raw_filings"),
        schema_contract=FREEZE_DATA_TYPES,
        source=ScrapeSource(
            name="filings",
            resources={
                Resource(name="filings", write=Append(), cursor=None, empty="fail"): Filings
            },
        ),
    )
    assert IncrementalRun(crawl)() == {"filings": 1}
    assert IncrementalRun(pipeline(folder, (ACME,), once_loaded=Delete()))() == {"invoices": 1}
    assert query("SELECT number FROM raw_docs.invoices") == [("A-1",)]
    assert list(folder.iterdir()) == []


# Where the tesseract language packages install English, on Arch and on Debian/Ubuntu.
TESSDATA = next(
    (
        d
        for d in ("/usr/share/tessdata", "/usr/share/tesseract-ocr/5/tessdata")
        if os.path.exists(f"{d}/eng.traineddata")
    ),
    None,
)


@pytest.mark.skipif(TESSDATA is None, reason="needs English tesseract language data")
def test_ocr_lets_templates_read_printed_scans(folder, query) -> None:
    fields = {"number": Text(r"Invoice (\S+)"), "total": Text(r"Total (\S+)")}
    header = Template(name="acme_scan", match="ACME", fields=fields)
    (folder / "scan.pdf").write_bytes(scan(["ACME Ltd", "Invoice A-1", "Total 30.50"]))
    definition = pipeline(folder, (header,), ocr=Ocr(languages=("eng",), tessdata=TESSDATA))
    assert IncrementalRun(definition)() == {"invoices": 1}
    assert query("SELECT number, total FROM raw_docs.invoices") == [("A-1", Decimal("30.50"))]


def test_a_refused_model_request_fails_the_run_without_retrying(folder, claude) -> None:
    claude.pages["/v1/messages"] = (401, '{"type": "error", "error": {"type": "auth"}}')
    (folder / "globex.pdf").write_bytes(GLOBEX_PDF)
    with pytest.raises(TerminalRunError):
        IncrementalRun(pipeline(folder, (MODEL,)))()
    assert len(claude.paths("POST")) == 1


def sharing(name: str, folders: dict[str, Path], reader: Documents) -> Pipeline:
    """A pipeline whose resources, one per folder, all read through ``reader``."""
    cursor = TimeCursor(
        path="modification_date",
        start=datetime(2000, 1, 1, tzinfo=UTC),
        format=AwareDatetime(),
        on_missing="raise",
    )
    files = {
        Resource(
            name=resource,
            write=Merge(primary_key=("document",), strategy="delete-insert"),
            cursor=cursor,
            empty="fail",
        ): Files(bucket_url=folder.as_uri(), file_glob="*.pdf", reader=reader)
        for resource, folder in folders.items()
    }
    return Pipeline(
        name=name,
        target=DuckDb(dataset=f"raw_{name}"),
        schema_contract=FREEZE_DATA_TYPES,
        source=FilesystemSource(name=name, resources=files),
    )


def test_a_failed_run_keeps_its_files_while_another_sharing_its_reader_commits(tmp_path) -> None:
    reader = Documents(schema=Invoice, extract=(ACME,), on_invalid="raise", once_loaded=Delete())
    good, bad = tmp_path / "good", tmp_path / "bad"
    for folder in (good, bad):
        folder.mkdir()
        (folder / "acme.pdf").write_bytes(ACME_PDF)
    future = time.time() + 7 * 86400  # read, then refused by the future-cursor guard before load
    os.utime(bad / "acme.pdf", (future, future))
    sharing("good", {"good": good}, reader)
    sharing("bad", {"bad": bad}, reader)
    assert main(["run", "good", "bad"]) == GuardError.exit_code
    assert list(good.iterdir()) == []
    assert [path.name for path in bad.iterdir()] == ["acme.pdf"]
