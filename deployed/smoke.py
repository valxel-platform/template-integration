"""Checks a deployed connector from outside, and writes JUnit for the post-deploy report.

What the outside can reach is the point: every webhook refuses a delivery with no signature and
one signed with a wrong secret (401, nothing published), and /healthz, /readyz and /metrics are
not routed outside the cluster (404). Sends no valid delivery, so it is safe in production.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx

from integration.config import load
from integration.webhooks import sign


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--base-url",
        required=True,
        help="the connector's public URL, e.g. https://labs-hooks.dev.example.com",
    )
    ap.add_argument("--config", default="integration.yaml")
    ap.add_argument("--junit", default="reports/deployed-smoke.xml")
    args = ap.parse_args(argv)
    base = args.base_url.rstrip("/")
    cfg = load(args.config)
    checks: dict[str, Callable[[], bool]] = {}
    for hook in cfg.webhooks:
        url = f"{base}{hook.path}"

        def unsigned(url: str = url) -> bool:
            return httpx.post(url, content=b"{}", timeout=10).status_code == 401

        def wrong_secret(url: str = url, h=hook) -> bool:  # type: ignore[no-untyped-def]
            stamp = str(int(time.time()))
            headers = {
                h.timestamp_header: stamp,
                h.header: sign("not-the-secret", stamp, b"{}"),
                h.delivery_id_header: f"smoke-{stamp}",
            }
            return httpx.post(url, content=b"{}", headers=headers, timeout=10).status_code == 401

        checks[f"{hook.name}: unsigned deliveries are refused"] = unsigned
        checks[f"{hook.name}: wrongly signed deliveries are refused"] = wrong_secret
    for path in ("/healthz", "/readyz", "/metrics"):
        checks[f"{path} isn't reachable from outside"] = lambda p=path: (  # type: ignore[misc]
            httpx.get(f"{base}{p}", timeout=10).status_code in (403, 404)
        )
    suite = ET.Element("testsuite", name="smoke")
    failed = 0
    for name, check in checks.items():
        t0 = time.monotonic()
        try:
            ok = check()
        except Exception:  # noqa: BLE001  (an unreachable connector is a failed check)
            ok = False
        tc = ET.SubElement(
            suite, "testcase", classname="smoke", name=name, time=f"{time.monotonic() - t0:.3f}"
        )
        if not ok:
            failed += 1
            ET.SubElement(tc, "failure", message=f"{name} failed at {base}")
        print(f"{'pass' if ok else 'FAIL'}  {name}")
    suite.set("tests", str(len(checks)))
    suite.set("failures", str(failed))
    Path(args.junit).parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(args.junit, encoding="utf-8", xml_declaration=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
