from datetime import date, datetime, timedelta, timezone

# IESO uses fixed EST (UTC-5) all year, no daylight saving.
EST = timezone(timedelta(hours=-5))


def hour_ending_to_utc(day: date, hour_ending: int) -> datetime:
    """HE17 means 16:00-17:00 EST, so the interval starts at hour_ending - 1."""
    if not 1 <= hour_ending <= 24:
        raise ValueError(f"hour ending must be 1..24, got {hour_ending}")
    start = datetime(day.year, day.month, day.day, tzinfo=EST) + timedelta(hours=hour_ending - 1)
    return start.astimezone(timezone.utc)
