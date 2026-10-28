*(Version française : [SECURITY_fr.md](SECURITY_fr.md))*

# Security

## Reporting a vulnerability

Do not open a public issue. Use GitHub's private reporting (**Security** tab → **Report a vulnerability** of the
[gnocent/kafkabeat](https://github.com/gnocent/kafkabeat) repository), describing the impact, the version
(`kafkabeat -version`) and, if possible, a reproduction on the test bench (`make bench-up`).

The project is maintained on its author's free time: acknowledgement within about two weeks at best, with no commitment
on a fix date. Only the latest published version is supported.

## What the agent touches

- **Kafka**: metadata, end and start offsets, consumer groups and committed offsets; in `hybrid` mode, reads the internal
  `__consumer_offsets` topic only. No application topic is read; nothing is written to Kafka.
- **Elasticsearch**: bulk writes to the `<prefix>-*` data streams, and installation of their templates and ILM policies
  at startup (can be turned off: `setup: false`).
- **Jolokia** (optional): reads MBeans on each broker, over HTTP or HTTPS, with Basic Auth.
- **Disk**: its state directory (`state.path`), files in `0640`.

Credentials are read only from the configuration file (keep it `0600`; the provided systemd unit hands it to the
service through `LoadCredential`) and are never logged. TLS 1.2 at least; no option disables certificate verification.
