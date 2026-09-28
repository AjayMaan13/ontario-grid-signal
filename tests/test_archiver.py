from pathlib import Path

from main import parse_listing, select_files

LISTING = (Path(__file__).parent / "fixtures" / "ICIPeakTracker_listing.html").read_text(encoding="latin-1")


def test_listing_contains_base_and_versioned_files():
    names = parse_listing(LISTING)
    assert "PUB_ICIPeakTracker.xml" in names
    assert "PUB_ICIPeakTracker_2022_v8501.xml" in names


def test_only_versioned_files_of_the_wanted_format_are_selected():
    selected = select_files(parse_listing(LISTING), "xml")
    assert "PUB_ICIPeakTracker_2022_v8501.xml" in selected
    assert "PUB_ICIPeakTracker.xml" not in selected  # base file changes, so it is skipped
    assert "PUB_ICIPeakTracker_2022.xml" not in selected  # same file as _v8501
    assert all("_v" in name for name in selected)


def test_wrong_format_selects_nothing():
    assert select_files(parse_listing(LISTING), "csv") == []
