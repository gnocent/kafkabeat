*(Version française : [CHANGELOG_fr.md](CHANGELOG_fr.md))*

# Changelog

One entry per published version, most recent first; "Unreleased" gathers what waits for the next tag.

## v0.1.1 (2026-10-28)

- `kafkabeat -version` shows the module version when the binary was installed with `go install` (it showed `dev`).

## v0.1.0 (2026-10-28)

First public version. How it works and how it is validated: [DESIGN.md](DESIGN.md).

- Time lag of every topic + consumer group pair every 10 s, as a lower and an upper bound read from the end-offset
  history, without interpolation: states (`caught_up`, `lagging`, `stalled`, `unassigned`, `no_commit`, `gone`),
  production status, rates, flags, loss risk (`retention_used`, `commit_before_log_start`), topic owner
  (`kafka.topic.owner`).
- Committed offsets read from the `__consumer_offsets` stream with reconciliation polling and per-group fallback
  (`hybrid` mode), or by polling only.
- Metricbeat-compatible documents (`kafka.partition.*`, `kafka.consumergroup.*`, `kafka.broker.*` through Jolokia) every
  60 s; one document per rebalance of a classic consumer group (`kafkabeat-events`); a health document every cycle.
- Lag and metrics data streams as TSDS, downsampled by ILM (7 days raw, then one value per hour up to 90 days);
  templates, mappings and policies installed at startup.
- Example Kibana dashboards and alert rules (`kibana/`), Docker test bench (Kafka 3.9 and 4.4, Elastic 8.19 and 9.5) and
  an automated acceptance suite of 16 criteria.
- Static binaries for Linux (x86-64), macOS (Apple Silicon) and Windows (x86-64); hardened systemd unit (`DynamicUser`,
  `LoadCredential`); `scratch` container image running as non-root; HashiCorp Nomad job example.
