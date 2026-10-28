*(Version française : [DESIGN_fr.md](DESIGN_fr.md))*

# kafkabeat — Design

How kafkabeat measures what it measures, the technical choices behind it with their benefits and limits, and how it is
validated. To install it: [INSTALL.md](INSTALL.md); to use it day to day: [USAGE.md](USAGE.md).

## 1. Goal and scope

For every topic + consumer group pair of a very large Kafka estate, a measure of the delay **in seconds** every 10 s,
published to Elasticsearch, that keeps existing when consumption stops. The target is Kafka used as a buffer between
high-throughput producers and consumers, with thousands of topics and tens of thousands of partitions: what matters is
the overall picture and trustworthy near-real-time signals (is this group keeping up, stuck, losing data?), not
per-message latency.

Around that measure, kafkabeat also replaces Metricbeat's `partition`, `consumergroup` and `broker` metricsets for the
clusters (same document names), records rebalances, checks that every partition has a consumer, and estimates the risk
of losing messages to retention.

**Out of scope**: client-side JMX metrics (producer and consumer metricsets stay with Metricbeat, next to the
applications); Kerberos; KIP-932 share groups; reading application topics (hence no exact timestamp of the message at
the committed offset); emitting only on change (one document per pair and per cycle is the rule); active/passive high
availability; OTLP export; splitting a cluster between several agents; per-topic log sizes (DescribeLogDirs).

## 2. Measuring the time lag

### 2.1 Bracketing instead of estimating

The lag of a pair is the age of the first message the group has not consumed: the message at its committed offset. The
agent never reads messages, so it cannot read that message's timestamp. Instead, it records for each partition the
history of its **end offset** (LEO) as seen at each cycle, as entries meaning "the LEO was `L` from time `first` to time
`last`".

For a committed offset `c` with `L_i ≤ c < L_{i+1}`, message `c` was appended to the log after `last_i` and no later
than `first_{i+1}`. Hence:

- **lower bound** = now − `first_{i+1}` (the message already existed then);
- **upper bound** = now − `last_i` (it did not exist yet before).

When `c` equals the current end offset, the partition is caught up and its lag is 0. When `c` is older than the oldest
entry kept, the upper bound is unknown (`bracket_open`). A pair's lag is that of its worst partition.

**Benefit**: the true lag is always between the two bounds; nothing is interpolated, no average rate is assumed. A
consumer stuck on a quiet topic shows an upper bound that grows by 10 s every cycle, as it should; a burst after a long
silence is not blamed on the silence. Alerts use the upper bound.
**Limit**: the bounds are as tight as the history. Within the fine tier the bracket is at most one cycle wide; in older,
coarser tiers it widens (up to 10 min after 3 h).

### 2.2 History tiers and persistence

Default tiers: one entry every 10 s over 15 min, every minute over 3 h, every 10 min over 7 days. Removing entries only
widens brackets, so compaction is always safe. The history and the per-pair state are saved to disk atomically (every
5 min and at shutdown) and reloaded at startup: after a restart the bounds are immediately available again, with
`resumed_after_gap` raised and no rate computed over the gap.

## 3. Reading consumer groups

### 3.1 Committed offsets: stream plus polling

Two ways to know each group's committed offsets:

- **`hybrid`** (default): the agent consumes the internal `__consumer_offsets` topic, which receives every commit, and
  reconciles with a full OffsetFetch poll every 2 min (and at startup). A group whose records use an unknown format
  version falls back to polling on its own; while the stream is in error, every group is polled at every cycle (otherwise
  frozen commits would look `stalled`).
- **`polling`**: OffsetFetch for every group at every cycle.

**Benefit**: on a large estate, polling every group every 10 s is a constant load on group coordinators; the stream
costs one consumer and also dates each commit (`last_commit_age_s`). **Limit**: the internal format of
`__consumer_offsets` is not a public API. The parser is generated from Kafka's message schemas, counts the records it
cannot read, and falls back per group; it should be tested before each Kafka upgrade. Verified on Kafka 3.9 and 4.4.

### 3.2 Members, coverage and rebalances

For classic consumer groups, members (subscriptions, assigned partitions, generation) come from the GroupMetadata records
of the same stream, written by the coordinator at each generation. DescribeGroups (every 60 s) gives the reference at
startup and the fallback when the stream is unavailable.

- **Rebalances**: one `kafkabeat-events` document per generation, with members joined and left and the observed
  duration; in fallback, one document per change of members seen every 60 s, without generation.
- **Coverage**: expected partitions are those of the topics members subscribe to; a partition with no assigned member
  for 60 s, group `Stable`, makes the pair `unassigned`. A pair also exists for a subscription without any commit
  (`no_commit`).
- **Groups without members**: if their commits advance (manual assignment: Spark, Flink, `assign()`), they are classified
  `manual` and raise nothing; if commits are frozen, `none`.

**Limit**: KIP-848 groups (the broker-side `consumer` protocol) are tracked by their state only: no members, coverage or
rebalances. Their type is published (`kafka.consumergroup.type`) so they can be spotted. Full parity is left for later,
when such groups appear in production.

### 3.3 Broker metrics through Jolokia

With `jolokia.enabled`, the agent queries the Jolokia agent of each broker found in the metadata, every 60 s and outside
the 10 s cycle, with a bounded timeout, and publishes the same `kafka.broker.*` documents as Metricbeat's `broker`
metricset. An unreachable broker is reported in the health document; the others are still published. Verified with
Jolokia 1.7 and 2.6.

## 4. States, production status and flags

A pair's state describes **consumption** and is that of its worst partition: `caught_up`, `lagging` (commit advancing),
`stalled` (committed offset frozen for 60 s with lag, whether or not the consumer recommits the same offset and whether
or not production continues), `unassigned`, `no_commit`, `gone` (pair no longer seen: emitted a few cycles, then
forgotten). **Production** has its own status per topic: `active` or `stopped` (no message for 60 s). Keeping both apart
is what tells a producer problem from a consumer problem.

Flags stay raised for 6 cycles: offset reset, log truncated, topic recreated, group recreated, resumed after a gap,
commit before the log start (messages deleted by retention before being read), incomplete cycle.

**Loss risk**: `retention_used` = (LEO − commit) / (LEO − log start), from 0 (caught up) to 1 (commit at the start of the
retained log). The log start is read every 60 s, so a loss is certain when seen, but may be seen up to 60 s late. Not
computed while the log start is 0 (compacted topic, or retention never reached).

Exact definitions of every state, flag and field: [USAGE.md](USAGE.md#3-states-and-flags).

## 5. Storage in Elasticsearch

- **Data streams**: `kafkabeat-lag` (10 s), `kafkabeat-metrics` (60 s), `kafkabeat-events`, `kafkabeat-health`. The agent
  installs templates, mappings and ILM policies at startup (`setup: false` leaves this to the operator) and never modifies
  an existing ILM policy: it belongs to the operator.
- **Time series (TSDS)** for lag and metrics: dimensions are the identifiers of each document (cluster, topic, group,
  partition, broker). `@timestamp` is aligned on the 10 s grid; a document rejected with 409 (same series, same
  timestamp: restart within the same slot, two agents on one cluster) is counted as a duplicate, not an error.
- **Retention**: raw for 7 days, then downsampled to one value per hour, deleted at 90 days; health and events: 30 days.
  No field is marked as a metric, so downsampling keeps the **last** value of each hour, the only form ES|QL 8.19 can read.
  Beyond 7 days, a peak shorter than an hour may be missed.
- **Size**: with a Basic license the source is stored (no synthetic source), so raw TSDS documents are not smaller than
  classic ones; the saving comes from retention. Measured on the bench (8.19): over the whole retention, about 0.25× (lag)
  and 0.40× (metrics) of a classic 30-day index, assuming a downsampled document is the size of a raw one (not measured).
- The per-partition detail of a pair (`kafka.lag.partitions`) is kept in `_source` but not indexed.

## 6. Technical choices

| Choice | Benefit | Limit |
|---|---|---|
| Go, [franz-go](https://github.com/twmb/franz-go) Kafka client, vendored and pinned dependencies | One static binary, no runtime; full control of the Kafka protocol versions (OffsetFetch, ListGroups, `__consumer_offsets`). | Builds need Go 1.27. |
| One agent per cluster, state on local disk | No coordination, no external store; restarts resume from disk. | No high availability: a stopped agent leaves a gap, reported by the health document and the "silent component" rule. |
| Kafka metadata only (ACL: Describe, plus Read on `__consumer_offsets`) | Safe to grant on any cluster; no load on application topics. | No exact timestamp of a message: hence the bracketing. |
| One document per pair and per cycle | A silent or stuck pair stays visible; queries need no gap filling. | Volume proportional to pairs × cycles, mitigated by TSDS and downsampling. |
| Dashboards in ES|QL (tiles, totals) and Lens (series) | Exact counts (max per topic, then sum): a topic read by two groups is counted once. | ES|QL syntax differs slightly between 8.19 and 9.x; both are checked by the acceptance suite. |
| Elasticsearch sink behind an interface, strict YAML configuration | Another output can be added; a typo in the configuration is an error, not a silent default. | — |

## 7. Validation

The acceptance suite (`make recette`, in [recette/](recette/)) replays the criteria below on the Docker test bench:
Kafka 3.9 and 4.4 (KRaft, with Jolokia), Elasticsearch and Kibana 8.19 and 9.5, Metricbeat alongside, and a harness
that produces with irregular rates, drives consumers (pause, commit, reset, custom assignors) and journals the
production time of every message. Test `TestC<n>` checks criterion `n`.

1. The true lag of the last committed offset lies between the lower and upper bounds on 100% of cycles; width ≤ 10 s in
   the fine tier.
2. Consumer stuck, then production stopped: the upper bound grows by 10 s per cycle, one document per cycle. Silence,
   then a burst consumed at once: upper bound ≤ 20 s (the silence is not counted as lag).
3. Offset reset, recreated topic, deleted group, Kafka unreachable for a cycle: respectively `offset_reset_suspected`,
   `topic_recreated`, `gone` (a few cycles then purged), state kept with the error in the health document.
4. Restart after a stop: bounds resumed from disk, `resumed_after_gap` raised, no rate over the gap.
5. Unknown version key injected into `__consumer_offsets`: the group falls back to polling, the parser counter
   increments, values equal to polling within a cycle; on 3.9 and 4.x.
6. Offsets of the 60 s documents equal to Metricbeat's within a cycle. At 30,000 partitions: cycle < 10 s, memory < 2 GB
   (measured by `make load`, on a simulator).
7. Harness scenario (stuck pairs, stopped production, topic without group, topic with two groups): each overview tile
   equals the expected count; total throughput equals the per-topic sum within 5%.
8. Dashboards import and render without errors on Kibana 8.19 and 9.x.
9. Overview rendered in < 5 s over 15 simulated minutes at 30,000 partitions.
10. N rebalances caused by the harness (join, leave, hard kill) on 3.9 and 4.4: N documents, consecutive generations,
    correct members, timestamp within a cycle; stream cut: fallback reported, no invented rebalance.
11. Jolokia on the bench brokers: `kafka.broker.*` equal to Metricbeat's within a period, same mapping field by field; one
    Jolokia cut: error in the health document, other brokers published, 10 s cycle kept.
12. Two-member group on a 4-partition topic, one member frozen by an assignor that omits a partition:
    `partitions_unassigned` = 1 and pair KO after 60 s, not during a rebalance; subscribed group without commit:
    `no_commit` pair; `assign()` consumer that commits: no alert.
13. Agent stopped 3 min: "silent component" fires for that cluster only; Kafka cut 3 min with the agent alive: same;
    production stopped beyond the (lowered) threshold: the rule fires for that topic only.
14. Topic with short retention, consumer stopped: `retention_used` rises to 1, then `commit_before_log_start` with the
    right number of lost offsets and the pair KO; compacted topic behind: no risk reported.
15. Volume over the whole retention lower than a classic 30-day index (measured, then extrapolated); forced
    downsampling: trends dashboard renders beyond 7 days on 8.19 and 9.x; cycle rewritten at the same `@timestamp`: no
    error in the health document.
16. On 4.4, a group using the `consumer` protocol next to classic groups: only it is counted in the KIP-848 tile and
    listed; on 3.9, the tile shows 0.

**Where the suite is weaker than the text**, to fit in one session: criterion 2 checked over 40 s instead of 30 min, its
silence over 60 s instead of 10 min; criterion 4 with a 15 s stop instead of 5 min, Elasticsearch still reachable;
criterion 1 runs 3 min by default (`KB_LONG=1` for the 2-hour scenario); criterion 5 skips itself if the broker refuses
the injection; the 30,000-partition half of criterion 6 is measured by `make load`, outside the suite; criterion 9
asserts the refresh time, the initial load is only logged. Human checks (readability of the dashboards) are not
automated.

**Not verified**: a secured Elasticsearch cluster (the bench has no security; the privileges in INSTALL.md are derived
from the API calls), SASL and mTLS against a real cluster, a multi-broker cluster (the bench clusters have one broker
each; partial ListGroups failures are covered by unit tests), downsampled document sizes, the systemd sandbox on a real
host.

## 8. Known limits and later

- **Risks**: the `__consumer_offsets` and GroupMetadata formats in future Kafka versions (fallback to polling, test before
  upgrading); share groups listed by ListGroups on 4.x and then queried by OffsetFetch would probably produce per-group
  errors and incomplete cycles (not verified: no share group on the bench); during a partial ListGroups failure, the pairs
  of unlisted groups stay unknown (neither `gone` nor purged) and the cycle is reported incomplete.
- **Alert rule limit**: a rule grouped by cluster only sees clusters it has seen alive; a cluster removed for good may
  stay in alert until the rule is disabled and re-enabled (not verified).
- **TSDS**: after a long Elasticsearch outage, documents older than the TSDS time window are rejected.
- **Later**: full KIP-848 parity (members, assignments, rebalances by epoch), when the first such group is seen in
  production; throughput and lag fields marked as gauges (exact min, max and average per hour beyond 7 days) once every
  cluster runs Elasticsearch 9.x.
