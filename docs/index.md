# pgcat

pgcat is the PostgreSQL connection pooler and proxy used at OneSignal between application services and the database. It handles transaction pooling, load balancing across read replicas, automatic failover, and sharding — replacing PgBouncer in environments that need these capabilities.

## OneSignal context

This repo is a deployment of the open-source [pgcat project](../README.md) (originally from PostgresML). OneSignal runs it as infrastructure between services and PostgreSQL. For configuration reference (pool modes, sharding settings, auth passthrough, etc.), see the upstream README.

For OneSignal-specific deployment config (K8s manifests, pgcat.toml overrides, resource limits), see the `infra` repo.

## Observability

- [Grafana Dashboard](https://dashboards.onesignal.io) — search for "pgcat" in the dashboard list
- Prometheus metrics are exposed via pgcat's built-in HTTP endpoint (`:9930/metrics` by default)

## Owner

**eng-platform** — [#team-platform-eng](https://onesignal.slack.com/archives/C024RA5EF3Q)
