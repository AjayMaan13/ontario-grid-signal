import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime

from producer.sources import sort_key


@dataclass
class Change:
    report: str
    name: str
    published_at: datetime
    kind: str  # "new_group": a file we never saw (the live path missed it)
    #            "new_version": a later version of a file we already hold (IESO re-issued it)


def group_of(name: str) -> str:
    """PUB_RealtimeTotals_2026092808_v12.xml -> PUB_RealtimeTotals_2026092808.xml: all versions of one hour."""
    return re.sub(r"_v\d+(\.\w+)$", r"\1", name)


def find_changes(report, listed, done_names):
    """The files IESO lists that were never processed, oldest first.

    listed:     SourceFile objects IESO shows for this report in the window
    done_names: file names already processed, by the producer or by an earlier reconciliation
    Pure on purpose: no network, no clock, so it is easy to test with golden inputs.
    """
    done_groups = {group_of(name) for name in done_names}
    return [Change(report, f.name, f.published_at, "new_version" if group_of(f.name) in done_groups else "new_group")
            for f in sorted(listed, key=sort_key) if f.name not in done_names]


def summarize(changes) -> dict:
    """{'RealtimeTotals': {'new_group': 12, 'new_version': 1}, ...}"""
    counts = Counter((c.report, c.kind) for c in changes)
    out = {}
    for (report, kind), n in counts.items():
        out.setdefault(report, {})[kind] = n
    return out


def to_dict(change: Change) -> dict:
    """JSON-safe form, small enough for Airflow's XCom."""
    return {**asdict(change), "published_at": change.published_at.isoformat()}


def from_dict(data: dict) -> Change:
    return Change(**{**data, "published_at": datetime.fromisoformat(data["published_at"])})
