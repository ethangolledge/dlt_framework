"""ScrapeSource against a local site: crawls load raw, cursors stop crawls, failures are loud."""

import pytest
from conftest import Site

from dlt_framework.core import (
    FREEZE_DATA_TYPES,
    Append,
    Integer,
    Pipeline,
    Resource,
    RowBackfill,
    Target,
    ValueCursor,
)
from dlt_framework.core.errors import TransientRunError
from dlt_framework.core.run import BackfillRun, IncrementalRun
from dlt_framework.sources.scrape import ScrapeSource, ScrapeSpider


def listing(site: Site, posts: list[int], per_page: int = 3) -> None:
    """Newest-first pages of posts, ``/page/1`` onwards, each linking to the next."""
    pages = [posts[start : start + per_page] for start in range(0, len(posts), per_page)]
    for number, page in enumerate(pages, start=1):
        items = "".join(f'<li class="post" data-id="{post}">post {post}</li>' for post in page)
        more = f'<a class="next" href="/page/{number + 1}">next</a>' if number < len(pages) else ""
        site.pages[f"/page/{number}"] = (200, f"<ul>{items}</ul>{more}")


def spider(site: Site, **stated):
    class Posts(ScrapeSpider):
        name = "posts"
        start_urls = [f"{site.url}/page/1"]
        robots_txt_obey = True
        failed_requests = "raise"
        max_blocked_retries = 1
        locals().update(stated)

        async def parse(self, response):
            for post in response.css("li.post"):
                yield {"id": int(post.attrib["data-id"]), "text": post.css("::text").get()}
            if next_page := response.css("a.next::attr(href)").get():
                yield response.follow(next_page)

    return Posts


def pipeline(posts_spider, cursor=None, backfill=None) -> Pipeline:
    resource = Resource(
        name="posts", write=Append(), cursor=cursor, empty="warn", backfill=backfill
    )
    return Pipeline(
        name="site",
        target=Target("duckdb", dataset="raw_site"),
        schema_contract=FREEZE_DATA_TYPES,
        source=ScrapeSource(name="site", resources={resource: posts_spider}),
    )


def test_a_crawl_follows_pages_and_loads_items_raw(site, query) -> None:
    listing(site, list(range(9, 0, -1)))  # three pages
    assert IncrementalRun(pipeline(spider(site)))() == {"posts": 9}
    assert query("SELECT count(*), max(id) FROM raw_site.posts") == [(9, 9)]


def test_a_newest_first_cursor_stops_the_crawl_at_stored_state(site, query) -> None:
    """With row_order="desc", dlt closes the crawl at the first post at or below the cursor."""
    cursor = ValueCursor(path="id", start=0, format=Integer(), on_missing="raise", row_order="desc")
    definition = pipeline(spider(site), cursor=cursor)
    listing(site, list(range(9, 0, -1)))
    IncrementalRun(definition)()

    listing(site, list(range(11, 0, -1)))  # two new posts on page 1
    assert IncrementalRun(definition)() == {"posts": 2}
    assert query("SELECT count(*), max(id) FROM raw_site.posts") == [(11, 11)]


def test_failed_requests_fail_the_extract_unless_stated_otherwise(site, query) -> None:
    listing(site, [3, 2, 1])
    site.pages["/page/1"] = (200, site.pages["/page/1"][1] + '<a class="next" href="/gone">x</a>')
    site.pages["/gone"] = (503, "down")
    with pytest.raises(TransientRunError):
        IncrementalRun(pipeline(spider(site)))()

    Pipeline.registry.clear()
    assert IncrementalRun(pipeline(spider(site, failed_requests="warn")))() == {"posts": 3}
    assert query("SELECT count(*) FROM raw_site.posts") == [(3,)]


@pytest.mark.parametrize("robots", [False, True])
def test_unreachable_pages_are_failures_not_silence(site, robots) -> None:
    """Scrapling skips a request whose robots.txt cannot be fetched without calling on_error."""
    unreachable = spider(site, start_urls=["http://127.0.0.1:9/page/1"], robots_txt_obey=robots)
    with pytest.raises(TransientRunError):
        IncrementalRun(pipeline(unreachable))()


def test_pages_disallowed_by_robots_txt_are_reported(site) -> None:
    listing(site, [6, 5, 4, 3, 2, 1])
    site.pages["/robots.txt"] = (200, "User-agent: *\nDisallow: /page/2")
    with pytest.raises(TransientRunError):
        IncrementalRun(pipeline(spider(site)))()


def test_row_windows_reach_the_spider_as_pages(site) -> None:
    """A spider turns window offsets into page numbers; the backfill stops at a short page."""
    listing(site, list(range(8, 0, -1)))

    class Paged(spider(site)):
        async def start_requests(self):
            from scrapling.spiders import Request

            page = self.window.offset // self.window.size + 1
            yield Request(f"{site.url}/page/{page}", sid=self._session_manager.default_session_id)

        async def parse(self, response):
            for post in response.css("li.post"):
                yield {"id": int(post.attrib["data-id"]), "text": post.css("::text").get()}

    assert BackfillRun(pipeline(Paged, backfill=RowBackfill(size=3)))() == {"posts": 8}
