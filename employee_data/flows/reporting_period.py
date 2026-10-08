"""Previous calendar month for employee attendance pipelines (Asia/Dubai)."""

from datetime import datetime
from zoneinfo import ZoneInfo

REPORTING_TIMEZONE = ZoneInfo("Asia/Dubai")


def reporting_datetime(now=None):
    """Wall clock in Asia/Dubai (UTC+4) for monthly attendance windows."""
    if now is None:
        return datetime.now(REPORTING_TIMEZONE)
    if now.tzinfo is None:
        return now.replace(tzinfo=REPORTING_TIMEZONE)
    return now.astimezone(REPORTING_TIMEZONE)


def previous_calendar_period(now=None):
    """Return (year, month) for the completed month before `now` in Dubai time."""
    now = reporting_datetime(now)
    if now.month == 1:
        return now.year - 1, 12
    return now.year, now.month - 1
