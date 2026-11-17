"""The example mapping: a lab vendor's results in, lab orders out. The coder replaces it with the
mapping the design names (HLD "Integrations"), keeping the shape: ids and codes, never more of a
record than the product's contract allows.

Everything here is synthetic: example-labs is not a real vendor.
"""

from __future__ import annotations

from typing import Any

from .handlers import Emit, Permanent, inbound, outbound

STATUSES = {"preliminary": "preliminary", "final": "final", "corrected": "amended", "cancelled": "cancelled"}


@inbound("lab-results")
def lab_result(body: dict[str, Any]) -> list[Emit]:
    status = STATUSES.get(str(body.get("status", "")).lower())
    if status is None:
        raise ValueError("unknown result status")
    return [
        Emit(
            "lab-result",
            key=str(body["order_ref"]),
            data={
                "order_id": str(body["order_ref"]),
                "status": status,
                "result_code": str(body.get("loinc", "")),
                "reported_at": str(body["reported_at"]),
            },
        )
    ]


@outbound("lab-order")
def send_order(event: dict[str, Any], vendor: Any) -> None:
    data = event["data"]
    if not data.get("tests"):
        raise Permanent("an order needs at least one test")
    vendor.post(
        "/v1/orders",
        idempotency_key=str(event["id"]),
        json={
            "order_ref": data["order_id"],
            "tests": [{"code": t} for t in data["tests"]],
            "priority": data.get("priority", "routine"),
        },
    )
