"""Example: quotes.toscrape.com, a sandbox built for scraping practice, loaded raw."""

from collections.abc import AsyncGenerator
from typing import Any

from scrapling.spiders import Request, Response

from dlt_framework.core import FREEZE_DATA_TYPES, Append, DuckDb, Pipeline, Resource
from dlt_framework.sources.scrape import ScrapeSource, ScrapeSpider


class QuotesSpider(ScrapeSpider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]
    robots_txt_obey = True
    failed_requests = "raise"

    async def parse(
        self, response: Response
    ) -> AsyncGenerator[dict[str, Any] | Request | None, None]:
        for quote in response.css(".quote"):
            yield {
                "text": quote.css(".text::text").get(),
                "author": quote.css(".author::text").get(),
                "tags": quote.css(".tag::text").getall(),
                "page": response.url,
            }
        if next_page := response.css("li.next a::attr(href)").get():
            yield response.follow(next_page)


Pipeline(
    name="quotes",
    target=DuckDb(dataset="raw_quotes"),
    schema_contract=FREEZE_DATA_TYPES,
    source=ScrapeSource(
        name="quotes",
        resources={
            Resource(name="quotes", write=Append(), cursor=None, empty="fail"): QuotesSpider
        },
    ),
)
