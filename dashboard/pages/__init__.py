"""Every page, registered in the order a reader flips through them."""

from dashboard.pages import overview, runs, backfills, incidents, tuning  # noqa: I001, F401 - flip order
from dashboard.pages.base import PAGES

__all__ = ["PAGES"]
