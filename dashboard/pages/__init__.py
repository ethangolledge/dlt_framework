"""Every page, registered in the order a reader flips through them."""

from dashboard.pages import overview, runs  # noqa: I001, F401 - registers in flip order
from dashboard.pages import analysis  # noqa: F401
from dashboard.pages.base import PAGES

__all__ = ["PAGES"]
