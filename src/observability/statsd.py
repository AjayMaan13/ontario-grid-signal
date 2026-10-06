import os
import socket


class Statsd:
    """A minimal DogStatsD client: one small UDP packet per metric, sent to the Datadog agent on the node.

    With no agent configured it does nothing, so tests and local runs need no setup, and a metrics problem can never
    break the pipeline.
    """

    def __init__(self, host=None, port=8125, prefix="grid.", tags=()):
        host = host if host is not None else os.environ.get("DD_AGENT_HOST")  # host="" means: send nothing
        self._address = (host, port) if host else None
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) if host else None
        self._prefix, self._tags = prefix, list(tags)

    def _send(self, name, value, kind, tags):
        if not self._address:
            return
        all_tags = self._tags + list(tags)
        line = f"{self._prefix}{name}:{value}|{kind}" + (f"|#{','.join(all_tags)}" if all_tags else "")
        try:
            self._socket.sendto(line.encode(), self._address)
        except OSError:
            pass  # metrics must never break the pipeline

    def incr(self, name, value=1, tags=()):
        self._send(name, value, "c", tags)

    def gauge(self, name, value, tags=()):
        self._send(name, value, "g", tags)

    def histogram(self, name, value, tags=()):
        self._send(name, value, "h", tags)
