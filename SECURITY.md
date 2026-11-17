# Security policy

## Reporting a vulnerability

Do not open a public issue. Use **Security > Report a vulnerability** on this repository
(GitHub private vulnerability reporting), or email the product's security team. Include what you
found, how to reproduce it, and the impact you expect. We acknowledge reports within two
business days.

## What this repository does by default

- Secret scanning with push protection; Dependabot alerts and security updates.
- CodeQL code scanning on every pull request; merges are blocked on new high or critical alerts.
- Dependency review on pull requests; vulnerable additions at the platform's threshold block.
- The default branch is protected: pull requests only, code-owner review, required checks.

## Handling sensitive data

This repository may belong to a product that handles PHI or payment data. Never commit real
patient or cardholder data, including in tests, fixtures, logs or screenshots. Use synthetic
data. Do not log request bodies, identifiers or tokens.

## Access controls in this connector

- **Inbound:** only `/webhooks/*` is routed outside the cluster. Every delivery must carry an
  HMAC-SHA256 signature over its timestamp and raw body, within the timestamp tolerance; anything
  else gets a bare 401. Deliveries are deduplicated by delivery id, size-capped and rate-limited.
- **Outbound:** calls go over https to `allowed_hosts` only, never follow redirects, and
  authenticate with OAuth client credentials, an API key or mTLS. The pod's egress policy allows
  the same hosts.
- **Secrets** are files the platform mounts (current and previous value during a rotation), never
  environment variables, settings, logs or error reports.
- **Kafka:** the connector has its own identity, allowed to write the topics it publishes and read
  the topics it sends on, nothing else. PHI and payments products must use TLS.
- **PHI** needs `baa: true` in `integration.yaml`: a business associate agreement with the vendor.
- **Audit:** every delivery received (accepted, refused or rejected) and every event sent is
  recorded by id; for PHI and payments, data isn't accepted or sent without its record.
