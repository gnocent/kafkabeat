*(Version française : [README_fr.md](README_fr.md))*

# Local test bench

The bench runs two Kafka clusters and two Elastic stacks in Docker, for the quick start
([INSTALL.md](../INSTALL.md#2-quick-start-on-the-test-bench)) and the acceptance suite (`make recette`,
[DESIGN.md](../DESIGN.md#7-validation)).

## Starting and stopping

`make bench-up` (about 1.5 min cold, images already pulled; needs Docker Engine 25 and Compose 2.20 or later, for the
healthchecks' `start_interval`); `make bench-down` stops it and **deletes its data**. Nothing is authenticated: every
port is published on `127.0.0.1` only.

| Service | Address | Notes |
|---|---|---|
| Kafka 3.9 (KRaft) | `localhost:19092` | Jolokia 1.7.2 on `localhost:8778` (it reports itself as 1.7.1). |
| Kafka 4.4 (KRaft) | `localhost:19094` | Jolokia 2.6.3 on `localhost:8779`. |
| Elasticsearch / Kibana 8.19 | `localhost:9200` / `localhost:5601` | No security, Basic license. |
| Elasticsearch / Kibana 9.5 | `localhost:9201` / `localhost:5602` | Same; used for dashboard import and rendering checks. |
| Metricbeat | | `partition` and `consumergroup` metricsets of both clusters every 30 s into `metricbeat-*`, `broker` every 10 s; field `bench_cluster` = `kafka39` or `kafka44`. |

- Brokers apply retention every 5 s (`log.retention.check.interval.ms`), for the loss-risk criterion (14).
- Jolokia agents run inside each broker JVM (`KAFKA_OPTS`); `make bench-up` downloads them once from Maven Central and
  checks their SHA-256 (`bench/jolokia/`, not tracked by git). Tools run with `docker exec` inside a Kafka container print
  "Could not start Jolokia agent" (the port is taken by the broker) and work normally.
- With Jolokia 2.x (kafka44), Metricbeat only publishes the per-topic broker documents: its document of fixed MBeans is
  rejected by Elasticsearch (ZooKeeper MBeans are missing in KRaft and come back as error objects) and only appears in
  `/usr/share/metricbeat/logs/metricbeat-events-data-*.ndjson`, where criterion 11 reads it. With Jolokia 1.x (kafka39)
  it is published normally.

## Harness

`go build -o /tmp/harness ./bench/harness`. Every command accepts `--bootstrap` (default `localhost:19092`).

```sh
/tmp/harness topics create --name t1 --partitions 6                     # also: delete, recreate (new topic id)
/tmp/harness topics create --bootstrap localhost:19094 --name t2 --config retention.ms=15000,segment.ms=2000
/tmp/harness produce --topic t1 --phases "steady:50:10s,silence:0:5s,burst:2000:5s" --journal t1.journal   # --repeat, --size
/tmp/harness consume --group g1 --topics t1 --commit auto --commit-interval 2s --control 127.0.0.1:8181   # --commit manual, --max-rate 10
#   --balancer range|roundrobin|sticky|cooperative-sticky|omit=t1/3 (round robin that never assigns t1/3; same balancer for every member)
#   --session-timeout 6s (departure seen quickly after a kill), --revoke-delay 25s (holds each rebalance),
#   --assign (all partitions without joining the group, commits for --group)
#   --server-side (KIP-848 consumer protocol, Kafka 4.0+, range balancer by default; on 3.9 franz-go falls back to classic)
curl -X POST 127.0.0.1:8181/pause      # stops consuming and committing without leaving the group; /resume resumes
curl -X POST 127.0.0.1:8181/commit     # commits the consumed offsets now
curl -X POST "127.0.0.1:8181/reset?to=earliest"   # or latest, offset=<n>: leaves the group, rewrites the offsets, rejoins
curl 127.0.0.1:8181/status             # state, consumed and committed offsets per partition, member, generation, assignment
/tmp/harness groups delete --group g1  # the group must have no member
```

The journal holds one JSON line per produced message (`topic`, `partition`, `offset`, `produced_at`); the package
`bench/harness/journal` reads it back (`Reader.ProducedAt`).

Checking on the Kafka side:
`docker exec kafka39 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group g1`.
On the Elasticsearch side:
`curl "localhost:9200/metricbeat-*/_search?q=metricset.name:consumergroup&size=1&sort=@timestamp:desc"`.

## Rendering a dashboard

`node bench/render/render.mjs http://localhost:5601 kafkabeat-dashboard [from] [to] [capture.png]` prints the load and
refresh times and the panels in error, in Chromium headless. Chromium comes from the Playwright cache
(`~/.cache/ms-playwright`), which `make recette` fills (`npm ci`, then `npx playwright-core install chromium` in
`bench/render`: `playwright-core` alone ships no browser); on a fresh Ubuntu,
`npx playwright-core install --with-deps chromium` adds the system libraries.

## Acceptance suite

`make recette` replays the 16 criteria of [DESIGN.md](../DESIGN.md#7-validation). Variables:
`KB_KAFKA39`, `KB_KAFKA44`, `KB_ES`, `KB_ES9`, `KB_KIBANA`, `KB_KIBANA9`, `GO`, `KB_LONG=1` (2-hour scenario for
criterion 1), `KB_NO_SANDBOX=1` (Chromium without sandbox). The suite still assumes this bench: it pauses the `kafka39`
and `kafka44` containers and reads Metricbeat's logs with `docker exec`.

Criterion 5 injects a record with an unknown version into partition 49 of `__consumer_offsets` on both clusters, which
the coordinators can no longer load (4.x at once, 3.9 at restart); the bench's groups avoid that partition. Reset the
bench afterwards (`make bench-down`).
