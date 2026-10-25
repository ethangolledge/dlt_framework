"""Web pages through Scrapling spiders, loaded raw. Needs the ``scrape`` extra.

Scrapling owns fetching: sessions, concurrency, throttling, robots.txt, and retrying blocked
pages. dlt owns loading and the cursor. The default session is plain HTTP (``FetcherSession``),
which runs in the standard image; a browser session (``StealthySession``) also needs
``scrapling install`` and an image with a browser.

Scrapling's crawl checkpoint (``crawldir``) is not used: dlt state and the monitoring ``loads``
frame are the record, so runs stay stateless outside dlt and work on ephemeral runners.
"""

import asyncio
import hashlib
import logging
import posixpath
import queue
import threading
from collections import Counter
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, ClassVar, Literal
from urllib.parse import urlparse

from dlt.common.exceptions import TerminalException
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource
from scrapling.spiders import Request, Response, Spider

from dlt_framework.core import buckets
from dlt_framework.core.errors import DefinitionError
from dlt_framework.core.resource import Resource
from dlt_framework.core.source import Extraction, Source
from dlt_framework.core.windows import RowWindow, TimeWindow, ValueWindow, Window

LOGGER = logging.getLogger(__name__)
FailedRequests = Literal["raise", "warn"]
STATED = ("robots_txt_obey", "failed_requests")
_BUFFER = 1000  # items held between the crawl and dlt before the crawl waits


class ScrapeError(RuntimeError, TerminalException):
    """Requests failed during a crawl whose spider states ``failed_requests = "raise"``, or the
    site rate-limited it.

    Terminal: Scrapling has already retried each failed request, and re-running the whole crawl
    would re-request every page, multiplying the load on a site that may be limiting us. The run
    fails, nothing is committed, and the next run resumes from the stored cursor.
    """


class ScrapeSpider(Spider):
    """Base for spiders loaded by ``ScrapeSource``.

    A subclass states ``name``, ``start_urls`` (or ``start_requests``), ``parse``, and two choices
    the framework does not default: ``robots_txt_obey``, and ``failed_requests``. With ``"raise"``
    a failed request fails the extract, so nothing is committed and the run retries or exits
    non-zero; with ``"warn"`` what was scraped loads and the failures are logged.

    ``self.incremental`` is the dlt cursor, or ``None`` without one: read its ``start_value`` to
    stop following pagination once pages are older. ``self.window`` is the backfill window, or
    ``None`` on runs: turn its edges into page numbers or date filters.

    Scrapling only logs some failures: request errors, ``parse`` errors, pages still blocked after
    ``max_blocked_retries``, and requests skipped by robots.txt (disallowed, or robots.txt itself
    unreachable, which Scrapling treats the same). They are collected here. Its log lines go
    through the framework's handler, so they share the log format and the masking of URL secrets.

    A spider that downloads documents states ``documents``, a folder or bucket URL, and calls
    ``await self.save(response)``; a ``FilesystemSource`` with a ``Documents`` reader extracts
    them from there.
    """

    failed_requests: ClassVar[FailedRequests]
    documents: ClassVar[str]
    logging_level = logging.WARNING

    def __init__(self, incremental: Incremental[Any] | None, window: Window | None) -> None:
        super().__init__()
        self.incremental, self.window = incremental, window
        self.failures: list[str] = []
        self.rate_limited = False
        self._blocked: Counter[str] = Counter()
        for handler in [h for h in self.logger.handlers if type(h) is logging.StreamHandler]:
            self.logger.removeHandler(handler)
        self.logger.propagate = True

    async def save(self, response: Response, name: str | None = None) -> str:
        """Store a downloaded document under ``documents`` and return its URL.

        Named ``name`` (plus the URL's suffix) when the upstream has a stable id for it, so a
        spider can check ``buckets.exists`` before downloading it again; otherwise by content
        hash. Either way a document saved again while still stored is not rewritten (nor
        re-extracted). The write runs off the event loop, so a slow bucket does not stall the
        crawl.
        """
        documents = getattr(self, "documents", None)
        if documents is None:
            raise DefinitionError(f"{type(self).__name__} saves documents: state `documents`")
        body = response.body
        suffix = posixpath.splitext(urlparse(response.url).path)[1]
        stem = name or hashlib.sha256(body).hexdigest()[:16]
        url = f"{documents.rstrip('/')}/{stem}{suffix}"
        if not await asyncio.to_thread(buckets.exists, url):
            await asyncio.to_thread(buckets.write, url, body)
        return url

    async def on_error(self, request: Request, error: Exception) -> None:
        self.failures.append(f"{request.url}: {type(error).__name__}: {error}")

    async def is_blocked(self, response: Response) -> bool:
        if response.status == 429 and not self.rate_limited:
            # Rate limited: stop now. Retrying into the limit is what extends a site's block.
            self.rate_limited = True
            self.failures.append(f"{response.url}: rate limited (HTTP 429); the crawl stopped")
            self.pause()
        blocked = await super().is_blocked(response)
        if blocked and not self.rate_limited:
            url = (response.request or response).url
            self._blocked[url] += 1
            if self._blocked[url] > self.max_blocked_retries:
                self.failures.append(
                    f"{url}: still blocked (HTTP {response.status}) after "
                    f"{self.max_blocked_retries} retries"
                )
        return blocked

    async def on_close(self) -> None:
        await super().on_close()
        if skipped := self.stats.robots_disallowed_count:
            self.failures.append(
                f"{skipped} requests skipped by robots.txt (disallowed, or robots.txt unreachable)"
            )

    def raise_for_failures(self) -> None:
        if not self.failures:
            return
        summary = f"{self.name}: {len(self.failures)} failed requests, first: {self.failures[0]}"
        if self.failed_requests == "raise" or self.rate_limited:  # a cut-short crawl never lands
            raise ScrapeError(summary)
        LOGGER.warning("%s; the rest were loaded", summary)


@dataclass(frozen=True, kw_only=True)
class ScrapeSource(Source[type[ScrapeSpider]]):
    """Each resource maps to a ``ScrapeSpider`` subclass; every item it yields is a row.

    Rows land as scraped: shape, deduplicate, and type them downstream, or with ``Merge`` on a
    URL key. A cursor is applied by dlt to what the spider yields; a newest-first listing with
    ``row_order="desc"`` stops the crawl at the stored cursor. Windows reach the spider as
    ``self.window``, so each spider decides what a time, value, or row window means for its site.
    """

    windows: ClassVar[tuple[type[Window], ...]] = (TimeWindow, ValueWindow, RowWindow)

    def requires(self) -> dict[str, str]:
        needed: dict[str, str] = {}
        for spider in self.resources.values():
            if documents := getattr(spider, "documents", None):
                needed |= buckets.requires(documents)
        return needed

    def validate(self, resource: Resource, config: type[ScrapeSpider]) -> None:
        super().validate(resource, config)
        if not (isinstance(config, type) and issubclass(config, ScrapeSpider)):
            raise DefinitionError(f"{resource.name!r}: map it to a ScrapeSpider subclass")
        own = config.__mro__[: config.__mro__.index(ScrapeSpider)]
        if missing := [name for name in STATED if not any(name in vars(k) for k in own)]:
            raise DefinitionError(f"{config.__name__} must state {missing}")

    def resource(
        self, resource: Resource, config: type[ScrapeSpider], extraction: Extraction
    ) -> DltResource:
        def crawl(incremental: Incremental[Any] | None) -> Iterator[Any]:
            spider = config(incremental, extraction.window)
            yield from _items(spider)
            spider.raise_for_failures()

        return self._generator(resource, extraction, crawl)


@dataclass(frozen=True, slots=True)
class _Ended:
    error: BaseException | None


def _items(spider: ScrapeSpider) -> Iterator[Any]:
    """The spider's items, from a crawl that runs whole on its own event loop in a worker thread.

    dlt advances an async generator one step per asyncio task, and Scrapling's stream holds an
    anyio task group open across steps, which must close in the task that opened it: iterated by
    dlt directly, the crawl deadlocks. Here it runs as one task and hands items over a bounded
    queue. Closing this generator, as dlt does when a cursor stops the resource, stops the crawl:
    through Scrapling's graceful stop, so in-flight pages finish, their items are dropped, and the
    stream ends in its own task. Leaving the stream mid-iteration instead makes asyncio close it
    from another task, after Scrapling has cleared the crawl that ``on_close`` reads.
    """
    handoff: queue.Queue[Any] = queue.Queue(maxsize=_BUFFER)
    stop = threading.Event()

    async def crawl() -> None:
        stopping = False
        async for item in spider.stream():
            if not stop.is_set():
                await asyncio.to_thread(handoff.put, item)
            elif not stopping:
                spider.pause()
                stopping = True

    def run() -> None:
        try:
            asyncio.run(crawl())
        except BaseException as error:
            handoff.put(_Ended(error))
        else:
            handoff.put(_Ended(None))

    worker = threading.Thread(target=run, name=f"scrape-{spider.name}", daemon=True)
    worker.start()
    try:
        while not isinstance(item := handoff.get(), _Ended):
            yield item
        if item.error is not None:
            raise item.error
    finally:
        stop.set()
        while worker.is_alive():  # unblock a crawl waiting on a full queue
            with suppress(queue.Empty):
                handoff.get(timeout=0.1)
