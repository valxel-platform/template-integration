# template-integration

The Valxel template for an **integration connector**: the one component that talks to a third
party (a lab, a payer, a CRM, a payment processor) on the product's behalf. The rest of the
product never calls the vendor and the vendor never calls the product: they meet here, as data
contracts on Kafka.

```
vendor ──signed webhook──▶ /webhooks/<name> ─▶ inbox + outbox ─▶ relay ─▶ Kafka (product events)
vendor ◀──https, allowlisted── vendor client ◀── outbound consumer ◀── Kafka (product events)
source ◀──poll on a schedule── pull sync ─▶ @pull ─▶ outbox + cursor (one transaction) ─▶ relay ─▶ Kafka
```

## What you write

1. **`integration.yaml`** — the vendor, the data class (`phi` needs `baa: true`), each webhook
   (path, signature header and secret name, tolerance, limits) and the outbound connection (base
   URL, allowed hosts, auth, timeout, retries, rate, circuit breaker). Names of secrets only. G4
   reviews it with the design's *Integrations* section.
2. **`src/integration/mapping.py`** — `@inbound("<webhook>")` turns a verified delivery into
   `Emit(dataset, key, data)`; `@outbound("<dataset>")` sends one product event through `vendor`.
   Raise `Permanent` for an event the vendor will never accept. The example (a synthetic lab
   vendor) shows the shape.
3. **Pull sources** (optional), when the source can't call you: a `pull:` list in
   `integration.yaml` (the shape is in `config.py`) and a `@pull("<source>")` handler per source
   that turns one record into `Emit(...)`, or `Delete(dataset, key)` for a record the source says
   is gone. Two adapters: `http-cursor` (a paged API, read through `outbound`, so the same hosts,
   auth, limits and circuit breaker apply) and `object-store` (files in an S3 bucket, read with
   the pod's workload identity; add `boto3` to the dependencies). A new adapter is the platform
   team's to approve: the SDLC tool refuses any other name.

```python
@pull("test-catalog")
def catalog_item(record: dict) -> list[Emit | Delete]:
    if record.get("retired"):
        return [Delete("lab-test", key=record["code"])]
    return [Emit("lab-test", key=record["code"], data={"code": record["code"], "name": record["name"]})]
```

The SDLC tool writes `contracts/` from the product's dataset catalog. Events you emit are checked
against their contract before they're published; events you read are checked before they're sent.

## What the template does for you

| Concern | How |
| --- | --- |
| Authenticity of inbound | HMAC-SHA256 over `timestamp.body`, constant-time, previous secret accepted during rotation, timestamp window against replays, bare 401 |
| Exactly-once effects | inbox by delivery id (duplicates answered, never republished); outbox written in the same transaction; `sent` table plus `Idempotency-Key` for outbound |
| Egress | https to `allowed_hosts` only, no redirects, no proxy from the environment |
| Vendor trouble | retries with jittered backoff and `Retry-After`, token bucket, circuit breaker; the consumer pauses and retries the same event while the vendor is down |
| Bad data | contract checks both ways; refused deliveries kept as failed (422); unsendable events parked on `<topic>.dlt` naming the field, never the value |
| Secrets | mounted files under `/var/run/secrets/valxel`, re-read on change |
| Pull sources | each page's events and the cursor after it saved in one transaction (a crash re-reads a page, never skips one); `backfill_from` on the first run, then incremental; one run per source across replicas; deletions as `<dataset>.deleted` carrying only the key (by tombstone, or a full listing that never deletes on a partial one); `permissions_field` empty, so visible to nobody, when the handler gives no principals; a completeness record per run (seen, published, deleted, parked, cursor from and to) the SDLC tool reconciles; `integration_pull_failing{source}` and a "needs attention" log after 3 failed runs in a row |
| Audit | `integration.received` (per webhook delivery, or per pulled page with the record keys) and `integration.sent`, ids only; required for PHI and payments |
| Observability | OpenTelemetry traces (the webhook's trace continues through Kafka to the vendor call), JSON logs, Sentry with scrubbing, `/metrics` with dataset counts, product events and flag cohorts |

## Running

```sh
uv sync
mkdir -p .secrets && for s in lab-results-signing lab-client-id lab-client-secret; do echo "dev-$s" > .secrets/$s; done
INTEGRATION_SECRETS_DIR=.secrets uv run integration
uv run pytest            # VALXEL_TEST_DATABASE_URL=postgresql://... also runs the Postgres store tests
```

Without `INTEGRATION_DATABASE_URL` and `INTEGRATION_KAFKA_BOOTSTRAP` the connector runs with an
in-memory store and keeps events in the outbox; staging and production refuse to start that way.

## Settings

`INTEGRATION_` prefix: `CONFIG_PATH`, `CONTRACTS_DIR`, `SECRETS_DIR`, `ENVIRONMENT`, `PRODUCT`,
`DATABASE_URL`, `KAFKA_BOOTSTRAP`, `KAFKA_CONFIG` (JSON), `AUDIT_SINK`, `AUDIT_KAFKA_BOOTSTRAP`,
`AUDIT_TOPIC`, `AUDIT_KAFKA_CONFIG`, `AUDIT_REQUIRED`, `FLAGS_URL` and friends, `PORT`. Plus the
standard `OTEL_*`, `SENTRY_DSN`, `ENVIRONMENT` and `SENTRY_RELEASE`.

## After deploy

`post-deploy.yml` checks the deployed connector from outside: each webhook refuses unsigned and
wrongly signed deliveries, and `/healthz`, `/readyz` and `/metrics` aren't reachable. It never
sends a valid delivery, so it is safe in production.
