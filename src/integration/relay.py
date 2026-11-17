"""The outbox relay: events written with an accepted webhook delivery, published to Kafka.

Polls the outbox, publishes each event (keyed, with its headers and the trace), waits for the
broker to acknowledge (acks=all, idempotent producer), then marks them published. A crash
between the two publishes the batch again: consumers see each event's id and drop duplicates.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

from .contracts import Contracts, count
from .store import Store

log = logging.getLogger("integration.relay")


class Relay(threading.Thread):
    def __init__(self, store: Store, contracts: Contracts, producer: Any, *, idle_s: float = 1.0) -> None:
        super().__init__(daemon=True, name="outbox-relay")
        self.store, self.contracts, self.producer, self.idle_s = store, contracts, producer, idle_s
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                if not self.step():
                    self.stop.wait(self.idle_s)
            except Exception:
                log.exception("outbox relay: publishing failed; retrying")
                self.stop.wait(5)

    def step(self) -> int:
        batch = self.store.claim(100)
        if not batch:
            return 0
        failed: list[str] = []

        def done(err: Any, _msg: Any) -> None:
            if err is not None:
                failed.append(str(err))

        for q in batch:
            self.producer.produce(
                q.topic,
                key=q.key.encode(),
                value=json.dumps(q.value).encode(),
                headers=[(k, v.encode()) for k, v in q.headers.items()],
                on_delivery=done,
            )
        remaining = self.producer.flush(30)
        if failed or remaining:
            raise RuntimeError(f"{len(failed) + remaining} of {len(batch)} events weren't acknowledged")
        self.store.published([q.id for q in batch])
        for q in batch:
            count(self.contracts.dataset(q.topic), "published")
        return len(batch)


def kafka_producer(conf: dict[str, Any]) -> Any:
    from confluent_kafka import Producer

    return Producer({**conf, "acks": "all", "enable.idempotence": True})


ProducerFactory = Callable[[dict[str, Any]], Any]
