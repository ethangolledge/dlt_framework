"""Every page, registered in the order a reader flips through them: from the overview down to
one run, then the pages for digging in across pipelines."""

from dashboard.pages import overview, pipeline, run  # noqa: I001, F401 - registers in flip order
from dashboard.pages import analysis  # noqa: F401
from dashboard.pages.base import PAGES

__all__ = ["PAGES"]
