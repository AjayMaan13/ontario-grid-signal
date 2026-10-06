import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from parsers.common import EST

# The one file format the archiver keeps per report.
EXTENSION = {"RealtimeTotals": "xml", "PredispTotals": "xml", "ICIDemand": "csv"}
BASE = "https://reports-public.ieso.ca/public"
USER_AGENT = "ontario-grid-signal-producer/0.1 (personal project)"


@dataclass
class SourceFile:
    name: str
    published_at: datetime  # UTC
    uri: str


def is_wanted(report: str, name: str) -> bool:
    """Versioned files of the right format only. Base files change; versioned files never do."""
    return re.search(rf"_v\d+\.{EXTENSION[report]}$", name) is not None


def sort_key(file: SourceFile):
    """Files of one series together, oldest version first, so first appearances stay in order."""
    group = re.sub(r"_v\d+(\.\w+)$", r"\1", file.name)
    return group, int(re.search(r"_v(\d+)\.", file.name).group(1))


def parse_listing(html: str) -> list[tuple[str, datetime]]:
    """(file name, published time in UTC) for every file on an IESO directory page. Page times are EST."""
    rows = re.findall(r'<a href="(PUB_[^"]+)">[^<]*</a>\s+(\d{2}-\w{3}-\d{4} \d{2}:\d{2})', html)
    return [(name, datetime.strptime(when, "%d-%b-%Y %H:%M").replace(tzinfo=EST).astimezone(timezone.utc)) for name, when in rows]


def _fetch(url: str) -> bytes:
    for attempt in range(4):  # IESO sometimes resets connections
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}), timeout=30) as response:
                return response.read()
        except OSError:
            time.sleep(2**attempt)
    raise RuntimeError(f"giving up on {url}")


class IESOSource:
    """Live: reads straight from IESO, only files published in the last `lookback_hours`."""

    def __init__(self, lookback_hours=48):
        self.lookback = timedelta(hours=lookback_hours)

    def list_files(self, report, since=None, until=None):
        """Files published in [since, until). Default: the last `lookback_hours` up to now."""
        html = _fetch(f"{BASE}/{report}/").decode("latin-1")
        since = since or datetime.now(timezone.utc) - self.lookback
        return [SourceFile(name, when, f"{BASE}/{report}/{name}") for name, when in parse_listing(html)
                if is_wanted(report, name) and when >= since and (until is None or when < until)]

    def read(self, report, name):
        return _fetch(f"{BASE}/{report}/{name}").decode("utf-8")


class GCSSource:
    """Backfill: reads the files the archiver copied into the raw bucket."""

    def __init__(self, bucket_name):
        from google.cloud import storage

        self.name = bucket_name
        self.bucket = storage.Client().bucket(bucket_name)

    def list_files(self, report):
        files = []
        for blob in self.bucket.list_blobs(prefix=f"{report}/"):
            name = blob.name.split("/", 1)[1]
            if is_wanted(report, name):
                # the archiver saved IESO's Last-Modified on every file: that is our published_at
                saved = (blob.metadata or {}).get("ieso_last_modified")
                published = parsedate_to_datetime(saved) if saved else blob.updated
                files.append(SourceFile(name, published, f"gs://{self.name}/{blob.name}"))
        return files

    def read(self, report, name):
        return self.bucket.blob(f"{report}/{name}").download_as_text()


class DirectorySource:
    """Local folder laid out like the bucket (root/report/file). For tests and dry runs."""

    def __init__(self, root):
        self.root = Path(root)

    def list_files(self, report):
        folder = self.root / report
        files = sorted(folder.glob("PUB_*")) if folder.exists() else []
        return [SourceFile(p.name, datetime.fromtimestamp(p.stat().st_mtime, timezone.utc), str(p)) for p in files if is_wanted(report, p.name)]

    def read(self, report, name):
        return (self.root / report / name).read_text()
