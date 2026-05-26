"""Date utilities for financial-year windows."""

from datetime import date


def get_last_financial_year_range(reference: date | None = None) -> tuple[date, date]:
    """
    Return the Last Financial Year range: April 1 of the previous calendar year
    through March 31 of the current calendar year (inclusive), relative to *reference*.
    """
    ref = reference or date.today()
    start = date(ref.year - 1, 4, 1)
    end = date(ref.year, 3, 31)
    return start, end
