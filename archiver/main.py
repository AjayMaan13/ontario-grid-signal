"""Copy new IESO files into the raw GCS bucket. Runs as a Cloud Run job, once an hour.

Only versioned files (..._vN.xml) are copied. They never change once published,
unlike the unversioned base file, so "already in the bucket" means "done".
Re-running is always safe: files already archived are skipped.
"""

import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://reports-public.ieso.ca/public"
USER_AGENT = "ontario-grid-signal-archiver/0.1 (personal project, hourly)"

# report directory -> the one file format we keep
REPORTS = {
    "ICIDemand": "csv",
    "ICIPeakTracker": "xml",
    "RealtimeTotals": "xml",  # the CSV has no Ontario demand column
    "PredispTotals": "xml",
}


def parse_listing(html: str) -> list[str]:
    """File names from an IESO directory page."""
    return re.findall(r'href="(PUB_[^"]+)"', html)


def select_files(names: list[str], extension: str) -> list[str]:
    """Keep only versioned files of the wanted format."""
    return [n for n in names if re.search(rf"_v\d+\.{extension}$", n)]


def fetch(url: str) -> tuple[bytes, str]:
    """Return (body, Last-Modified). Retries because IESO sometimes resets connections."""
    for attempt in range(4):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read(), response.headers.get("Last-Modified", "")
        except OSError:  # covers HTTP errors, resets and timeouts
            time.sleep(2**attempt)
    raise RuntimeError(f"giving up on {url}")


def main() -> None:
    from google.cloud import storage  # imported here so the tests do not need it

    bucket = storage.Client().bucket(os.environ["BUCKET"])

    for report, extension in REPORTS.items():
        page, _ = fetch(f"{BASE}/{report}/")
        wanted = select_files(parse_listing(page.decode("latin-1")), extension)
        already = {blob.name for blob in bucket.list_blobs(prefix=f"{report}/")}
        missing = [name for name in wanted if f"{report}/{name}" not in already]
        print(f"{report}: {len(wanted)} on IESO, {len(already)} archived, copying {len(missing)}")

        def copy(name: str) -> None:
            body, last_modified = fetch(f"{BASE}/{report}/{name}")
            blob = bucket.blob(f"{report}/{name}")
            blob.metadata = {"ieso_last_modified": last_modified}  # this is our published_at
            blob.upload_from_string(body)
            time.sleep(0.1)  # be polite to a public server

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(copy, missing))  # list() makes any error stop the job


if __name__ == "__main__":
    main()
