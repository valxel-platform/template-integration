# template-integration

The Valxel template for an **integration connector**: the one component that talks to a third
party (a lab, a payer, a CRM, a payment processor) on the product's behalf. The rest of the
product never calls the vendor and the vendor never calls the product: they meet here, as data
contracts on Kafka.

```
vendor ──signed webhook──▶ /webhooks/<name> ─▶ inbox + outbox ─▶ relay ─▶ Kafka (product events)
vendor ◀──https, allowlisted── vendor client ◀── outbound consumer ◀── Kafka (product events)
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
| Audit | `integration.received` and `integration.sent`, ids only; required for PHI and payments |
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
