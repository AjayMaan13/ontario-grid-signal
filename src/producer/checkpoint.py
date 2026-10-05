import json
import logging

log = logging.getLogger("producer")


class Checkpoint:
    """Remembers the last (version, value) sent for every interval, so a restart sends nothing twice.

    Call is_new() before producing and mark() only AFTER Kafka confirmed delivery.
    A crash in between means the event is sent again: at-least-once, never lost.
    """

    def __init__(self, seen=None, files=None):
        self.seen = seen or {}
        self.files = set(files or [])  # "report/name" of every file fully sent, so it is never downloaded twice

    @staticmethod
    def _id(event):
        return f"{event['report']}|{event['zone']}|{event['interval_start']}"

    def is_new(self, event) -> bool:
        previous = self.seen.get(self._id(event))
        if previous is None:
            return True
        version, value = previous
        if value == event["value_mw"]:
            return False  # same value again (e.g. v2 repeats v1's interval): nothing new
        if version == event["version"]:
            log.warning("same version, different value: %s", self._id(event))
        return True

    def mark(self, event):
        previous = self.seen.get(self._id(event))
        if previous is None or event["version"] >= previous[0]:  # an old version never overwrites a newer one
            self.seen[self._id(event)] = [event["version"], event["value_mw"]]

    def is_file_done(self, report, name) -> bool:
        return f"{report}/{name}" in self.files

    def mark_file_done(self, report, name):
        self.files.add(f"{report}/{name}")

    def to_json(self) -> str:
        return json.dumps({"intervals": self.seen, "files": sorted(self.files)})

    @classmethod
    def from_json(cls, text: str):
        data = json.loads(text)
        return cls(data["intervals"], data["files"])
