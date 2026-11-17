from __future__ import annotations

import json
from typing import Any


class Msg:
    def __init__(
        self, topic: str, value: Any, offset: int = 0, headers: list[tuple[str, bytes]] | None = None
    ):
        self._topic, self._offset, self._headers = topic, offset, headers or []
        self._value = value if isinstance(value, bytes) else json.dumps(value).encode()

    def topic(self):
        return self._topic

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def key(self):
        return b"k"

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return None


class Consumer:
    def __init__(self, messages):
        self.messages, self.committed, self.seeks, self.subscribed, self.closed = (
            list(messages),
            [],
            [],
            [],
            False,
        )

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self.messages[0] if self.messages else None

    def commit(self, message, asynchronous):
        self.committed.append(message.offset())
        self.messages.pop(0)

    def seek(self, tp):
        self.seeks.append(tp.offset)

    def close(self):
        self.closed = True


class Producer:
    def __init__(self, fail: bool = False):
        self.sent, self.fail, self.pending = [], fail, []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": dict(headers or [])})
        if on_delivery:
            self.pending.append(on_delivery)

    def flush(self, timeout):
        for cb in self.pending:
            cb("broker down" if self.fail else None, None)
        self.pending = []
        return 0
