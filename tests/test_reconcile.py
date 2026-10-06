import json
from datetime import datetime, timedelta, timezone

import pytest

from producer.checkpoint import Checkpoint
from producer.sources import DirectorySource, SourceFile
from reconcile.compare import Change, find_changes, from_dict, group_of, summarize, to_dict
from reconcile.steps import archive, detect, republish
from test_producer import FakeProducer, make_archive

NOW = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


def listed(*names):
    return [SourceFile(n, NOW, "https://x/" + n) for n in names]


# ---- compare: pure, golden inputs ----

def test_a_file_that_was_never_processed_is_a_new_group():
    changes = find_changes("RealtimeTotals", listed("PUB_RealtimeTotals_2026100508_v1.xml"), set())
    assert [(c.name, c.kind) for c in changes] == [("PUB_RealtimeTotals_2026100508_v1.xml", "new_group")]


def test_a_later_version_of_an_hour_we_hold_is_a_new_version():
    held = {"PUB_RealtimeTotals_2026100508_v12.xml"}
    changes = find_changes("RealtimeTotals", listed("PUB_RealtimeTotals_2026100508_v12.xml", "PUB_RealtimeTotals_2026100508_v13.xml"), held)
    assert [(c.name, c.kind) for c in changes] == [("PUB_RealtimeTotals_2026100508_v13.xml", "new_version")]


def test_processed_files_are_never_reported_again():
    held = {"PUB_PredispTotals_20261005_v3.xml"}
    assert find_changes("PredispTotals", listed("PUB_PredispTotals_20261005_v3.xml"), held) == []


def test_versions_are_reported_oldest_first_and_numerically():
    names = ["PUB_RealtimeTotals_2026100508_v10.xml", "PUB_RealtimeTotals_2026100508_v9.xml", "PUB_RealtimeTotals_2026100507_v1.xml"]
    changes = find_changes("RealtimeTotals", listed(*names), set())
    assert [c.name for c in changes] == ["PUB_RealtimeTotals_2026100507_v1.xml", "PUB_RealtimeTotals_2026100508_v9.xml", "PUB_RealtimeTotals_2026100508_v10.xml"]


def test_a_whole_missed_hour_counts_as_new_group_in_the_summary():
    names = [f"PUB_RealtimeTotals_2026100508_v{n}.xml" for n in range(1, 13)]
    assert summarize(find_changes("RealtimeTotals", listed(*names), set())) == {"RealtimeTotals": {"new_group": 12}}


def test_group_of_drops_only_the_version():
    assert group_of("PUB_ICIDemand_2026_v3571.csv") == "PUB_ICIDemand_2026.csv"


def test_a_change_survives_the_trip_through_json_that_airflow_uses():
    change = Change("ICIDemand", "PUB_ICIDemand_2026_v3571.csv", NOW, "new_version")
    assert from_dict(json.loads(json.dumps(to_dict(change)))) == change


# ---- detect: compares IESO's listing with what was processed, per report ----

class FakeIESO:
    def __init__(self, files):
        self.files, self.asked = files, []

    def list_files(self, report, since, until):
        self.asked.append((report, since, until))
        return self.files.get(report, [])


def test_detect_counts_listed_files_and_only_looks_at_each_reports_own_processed_names():
    ieso = FakeIESO({"RealtimeTotals": listed("PUB_RealtimeTotals_2026100508_v1.xml", "PUB_RealtimeTotals_2026100508_v2.xml"),
                     "PredispTotals": listed("PUB_PredispTotals_20261005_v1.xml")})
    known = {"RealtimeTotals/PUB_RealtimeTotals_2026100508_v1.xml", "ICIDemand/PUB_RealtimeTotals_2026100508_v2.xml"}  # second entry belongs to another report
    changes, counts = detect(ieso, known, set(), NOW - timedelta(days=30), NOW)
    assert counts == {"ICIDemand": 0, "PredispTotals": 1, "RealtimeTotals": 2}
    assert sorted((c.name, c.kind) for c in changes) == [("PUB_PredispTotals_20261005_v1.xml", "new_group"), ("PUB_RealtimeTotals_2026100508_v2.xml", "new_version")]
    assert all(until == NOW and since == NOW - timedelta(days=30) for _, since, until in ieso.asked)


def test_files_handled_by_an_earlier_reconciliation_are_not_found_again():
    ieso = FakeIESO({"PredispTotals": listed("PUB_PredispTotals_20261005_v1.xml")})
    changes, _ = detect(ieso, set(), {"PredispTotals/PUB_PredispTotals_20261005_v1.xml"}, NOW - timedelta(days=30), NOW)
    assert changes == []


# ---- archive ----

class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name, self.metadata = store, name, None

    def exists(self):
        return self.name in self.store

    def upload_from_string(self, body):
        self.store[self.name] = (body, self.metadata)


class FakeBucket:
    def __init__(self, existing=()):
        self.store = {name: (b"old", None) for name in existing}

    def blob(self, name):
        return FakeBlob(self.store, name)


def test_archive_copies_only_files_the_bucket_lacks_and_saves_ieso_s_publish_time():
    changes = [to_dict(Change("RealtimeTotals", "a_v1.xml", NOW, "new_group")), to_dict(Change("RealtimeTotals", "b_v1.xml", NOW, "new_group"))]
    bucket = FakeBucket(existing=["RealtimeTotals/a_v1.xml"])
    assert archive(changes, bucket, fetch=lambda url: b"body") == 1
    assert bucket.store["RealtimeTotals/b_v1.xml"] == (b"body", {"ieso_last_modified": "Mon, 05 Oct 2026 12:00:00 GMT"})
    assert bucket.store["RealtimeTotals/a_v1.xml"] == (b"old", None)  # untouched


# ---- republish: reuses the producer's code, tagged reason=reconciliation ----

def changes_for(archive_dir, *pairs):
    return [to_dict(Change(report, name, NOW, "new_group")) for report, name in pairs]


def test_republished_events_are_tagged_reconciliation_and_the_checkpoint_is_left_alone(tmp_path):
    archive_dir = make_archive(tmp_path)
    producer, checkpoint = FakeProducer(), Checkpoint()
    result = republish(changes_for(archive_dir, ("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v12.xml")),
                       DirectorySource(archive_dir), producer, checkpoint, lambda report, name: f"gs://raw/{report}/{name}")
    assert result["events_sent"] == 12 and result["files_with_new_values"] == 1
    assert {e["reason"] for _, _, e in producer.sent} == {"reconciliation"}
    assert len(result["processed"]) == 1 and result["failed"] == []
    assert len(result["events"]) == 12 and set(result["events"][0]) == {"report", "interval_start", "version", "value_mw"}


def test_a_reissued_file_with_identical_values_is_processed_but_sends_nothing(tmp_path):
    archive_dir = make_archive(tmp_path)
    checkpoint = Checkpoint()  # a checkpoint that already knows the values, as the live producer's would
    first = FakeProducer()
    uri = lambda report, name: f"gs://raw/{report}/{name}"
    republish(changes_for(archive_dir, ("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v12.xml")), DirectorySource(archive_dir), first, checkpoint, uri)
    # IESO re-issues the hour as v13 with the same values
    import shutil
    shutil.copy(archive_dir / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v12.xml", archive_dir / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v13.xml")
    second = FakeProducer()
    result = republish(changes_for(archive_dir, ("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v13.xml")), DirectorySource(archive_dir), second, checkpoint, uri)
    assert second.sent == [] and result["events_sent"] == 0 and len(result["processed"]) == 1


def test_a_broken_file_is_reported_as_failed_not_hidden(tmp_path):
    archive_dir = make_archive(tmp_path)
    (archive_dir / "RealtimeTotals/PUB_RealtimeTotals_2026092809_v1.xml").write_text("<broken")
    result = republish(changes_for(archive_dir, ("RealtimeTotals", "PUB_RealtimeTotals_2026092809_v1.xml")),
                       DirectorySource(archive_dir), FakeProducer(), Checkpoint(), lambda report, name: name)
    assert result["processed"] == [] and len(result["failed"]) == 1
