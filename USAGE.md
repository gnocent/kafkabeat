*(Version française : [USAGE_fr.md](USAGE_fr.md))*

# Using kafkabeat day to day

This guide starts once the agent runs and writes to Elasticsearch ([INSTALL.md](INSTALL.md)). It walks through the
example dashboards and alert rules, then explains every state, flag and field, and how to check the agent itself. How
the values are computed: [DESIGN.md](DESIGN.md).

## 1. Setting up Kibana

1. **Import the examples**: Stack Management → Saved Objects → Import, choose `kibana/kafkabeat-examples.ndjson`, allow
   overwriting. Or:

   ```bash
   curl -X POST -H 'kbn-xsrf: true' "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
        --form file=@kibana/kafkabeat-examples.ndjson
   ```

   This creates the data views, four dashboards and four alert rules. Requirements: Kibana 8.19 or 9.x (checked on
   8.19.23 and 9.5.5, Basic license) and data streams created by the agent (it creates them at its first start).
2. **Check the data**: open the dashboard **kafkabeat: overview**. The "Last cycle (s)" tile should stay under 20; if it
   is empty, see [§7](#7-checking-the-agent).
3. **Name the teams** (optional): the "Team" filter of every dashboard reads `kafka.topic.owner`, which the agent sets
   from the `output.owners` rules of its configuration (first matching glob pattern wins):

   ```yaml
   output:
     owners:
       - {topic: "payments-*", owner: payments}
       - {topic: "*-logs",     owner: platform}
   ```

4. **Enable the alert rules** you want ([§6](#6-alert-rules)).

Every dashboard filters by cluster, team, topic and group (broker health: by cluster only).

## 2. The dashboards

### 2.1 Overview: the supervision screen

15 minutes, refreshed every 10 s. **Read it top to bottom:**

1. **Tiles** describe *now* (the latest cycles), whatever time range is selected:
   - topics tracked, consumed, and without any group;
   - **Consumption OK / not OK** per topic. Not OK = at least one pair of the topic `stalled`, `unassigned`, `no_commit`, more than
     300 s behind (upper bound, or lower bound when the upper one is unknown), or with lost offsets;
   - **Production active / stopped** per topic (stopped = no message for 60 s or more);
   - groups rebalancing, partitions unassigned, groups without members (commits frozen; manual assignment is not
     counted), pairs at risk of loss (`retention_used` ≥ 0.8, or lost offsets), KIP-848 groups;
   - total produce and consume throughput, worst lag, age of the agent's last cycle.
2. **State per pair**: which pairs are not caught up, and since when.
3. **Throughput per team** and **pairs per state** over the time range: did something change, and when?
4. **10 largest topics** and **10 worst pairs**: where to look first.
5. **kafkabeat health**: cycle duration and errors (see [§7](#7-checking-the-agent)).

Tiles and totals are exact (ES|QL: maximum per topic, then count or sum): the throughput of a topic read by two groups is
counted once. Pairs that are `gone` only count in "topics without group".

### 2.2 Topic + group zoom: on-call diagnosis

1 hour. Pick a topic and a group in the filters, then:

1. **Lag bounds**: the true lag lies between the two lines. A steadily rising upper bound with a flat consume rate is a
   stuck consumer.
2. **Produce and consume rates**: is the group slower than production, or has production stopped?
3. **Lag per partition** and **commit age**: one partition behind points to one member or a hot key; all partitions
   behind points to the whole group.
4. **Group state**, **consumers with their partitions and lag**, **partitions without an assigned member**.
5. **Rebalances per group** and the **membership log** (joined, left, generation): a rebalance storm explains a lag that
   comes and goes.
6. **Per-partition detail** with the assigned consumer and `retention_used` (share of the retained log not read yet).

### 2.3 Trends (up to 90 days): capacity

7 days by default: throughput and worst lag per team, topics ranked by volume produced. Beyond 7 days the data is
downsampled to one value per hour per series (the last one): a peak shorter than an hour may not show.

### 2.4 Broker health

1 hour, requires `jolokia.enabled` in the agent: brokers read and unreachable through Jolokia, under-replicated
partitions, bytes per second, failed requests, leader elections, and a table per broker.

## 3. States and flags

### 3.1 Consumption state (`kafka.lag.state`)

One value per pair: that of its worst partition. The dashboards show it with the label in the second column.

| Value | Label | Meaning |
|---|---|---|
| `caught_up` | Caught up | Every partition is consumed to the end. |
| `lagging` | Lagging | Lag on at least one partition, but the committed offset advances. |
| `unassigned` | Unassigned | A partition of a topic the group subscribes to has had no assigned member for `stalled_after` (60 s), group `Stable` (never during a rebalance). Takes precedence over `stalled`, which it often causes. |
| `stalled` | Stalled | On at least one partition, lag and a committed offset frozen for `stalled_after` (60 s), whether or not the consumer recommits that offset and whether or not production continues. |
| `no_commit` | No commit | No lag elsewhere, but at least one partition without a committed offset. |
| `gone` | Gone | The pair is no longer seen (topic or group deleted, offsets expired): emitted `gone_cycles` times, then forgotten. A group its coordinator cannot list (loading after a broker restart, unreachable broker) stays unknown during the outage, not `gone`. |

### 3.2 Production status (`kafka.lag.production`)

Same for every pair of a topic.

| Value | Label | Meaning |
|---|---|---|
| `active` | Active | A message arrived on the topic less than `stalled_after` (60 s) ago. |
| `stopped` | Stopped | No message on the topic for at least `stalled_after`. |
| absent | | At startup, until an advance is seen and `stalled_after` has elapsed. |

### 3.3 Flags (`kafka.lag.flags.*`)

Each flag stays raised for `flag_hold_cycles` cycles (6 by default).

| Flag | Meaning | What to do |
|---|---|---|
| `offset_reset_suspected` | The committed offset went back on a partition. | Expected after a deliberate reset; otherwise ask the group's owner. |
| `log_truncated_suspected` | The end offset went back (log truncated); the partition's history restarts from zero. | Check the broker (unclean leader election, deleted records). |
| `topic_recreated` | The topic changed id (deleted and recreated). Kafka then drops the group's offsets: the flag also comes with the pair's `gone` document, then its return. | Check the group restarted where intended. |
| `group_recreated` | The pair reappears after being `gone`. | Usually informational. |
| `resumed_after_gap` | The agent restarted after more than two cycles: no rate over the gap. | Check why the agent stopped. |
| `commit_before_log_start` | Retention went past the group's commit: messages were deleted before being read (`kafka.lag.offsets_lost`). | Data loss for that group: alert its owner. |
| `cycle_incomplete` | Part of the collection failed this cycle (details in the health document). | See [§7](#7-checking-the-agent). |

### 3.4 Main fields of a pair document (`kafkabeat-lag`)

| Field | Meaning |
|---|---|
| `kafka.lag.seconds_lower`, `seconds_upper` | Lag bounds in seconds, worst partition. `seconds_upper` absent when the committed offset is older than the history (`kafka.lag.method: bracket_open`). |
| `kafka.lag.offsets_sum` | Lag in messages, summed over partitions. |
| `kafka.lag.produce_rate`, `consume_rate`, `consume_rate_smoothed` | Messages per second over the last cycle; smoothed over `smoothing_window`. |
| `kafka.lag.last_commit_age_max_s` | Age of the last commit of a lagging partition. In `hybrid` mode the stream dates each commit: a stuck consumer that recommits the same offset stays `stalled` with a small age (alive, but not advancing). In `polling` mode, age of the last offset change. |
| `kafka.lag.last_produce_age_s` | Seconds since the last message seen on the topic (a lower bound until the agent has watched long enough). |
| `kafka.lag.retention_used` | (LEO − commit) / (LEO − log start), worst partition, from 0 (caught up) to 1 (commit at the start of the retained log). Absent while the log start is 0 (compacted topic, retention never reached). |
| `kafka.lag.offsets_lost` | Messages deleted before being read (log start − commit, summed). |
| `kafka.lag.partitions_unassigned`, `partitions_unassigned_ids`, `unassigned_age_max_s` | Partitions of subscribed topics with no member (group `Stable` only). |
| `kafka.consumergroup.members`, `assignment` | `members` (at least one member), `manual` (no member, commits advancing: `assign()`, Spark, Flink; no alert), `none` (no member, commits frozen). Absent for two cycles after the last member leaves. |
| `kafka.consumergroup.type` | `classic`, or `consumer` for KIP-848 groups (tracked by state only). |
| `kafka.topic.owner` | Team, from `output.owners`. |
| `kafka.lag.partitions` | Detail of lagging partitions, in `_source` only (not searchable). |

Rebalance documents (`kafkabeat-events`) carry `kafka.consumergroup.generation`, `kafka.rebalance.previous_generation`,
`kafka.rebalance.joined`, `left`, the subscribed topics and `duration_s` (a lower bound observed by the 10 s cycles). In
fallback (`describe` method), one document per change of members seen every 60 s, without generation. Rebalances that
happen while the agent is stopped are not reported.

## 4. Investigating a pair, step by step

**A pair is `lagging`, its upper bound keeps rising.**
1. Zoom on the pair. Compare produce and consume rates: if consumption is below production, the group lacks capacity
   (more members, up to the number of partitions) or a member is slow.
2. Look at lag per partition: a single partition behind points to one member (consumers table) or a hot key.
3. Check rebalances: frequent generations mean members keep leaving (timeouts, crashes).

**A pair is `stalled`.**
1. Check `kafka.lag.production`: if `stopped`, the backlog is not growing, but nobody consumes it.
2. Look at `last_commit_age_max_s`: small in `hybrid` mode means the consumer is alive and recommits the same offset (it
   is stuck on a message, often a processing error); large means the consumer stopped committing.
3. Check the consumers table: no member means the application is down (`assignment: none`).

**A pair is `unassigned`.** A partition has no member while the group is stable: usually a custom assignor, or more
subscriptions than members can take. The zoom lists the partitions; the group's owner must fix the assignment.

**A pair is `no_commit`.** The group subscribes to the topic but never committed on some partitions: a new group still
starting, or an application that does not commit (auto-commit off).

**A pair is at risk of loss.** `retention_used` close to 1 means retention is about to delete messages the group has
not read. Speed the consumer up, or extend the topic's retention. `commit_before_log_start` means it already happened:
`offsets_lost` messages were never read.

**Production is `stopped`.** The problem is upstream: check the producing application, not the consumer.

## 5. Querying the data directly

The data views `kafkabeat-lag`, `kafkabeat-metrics`, `kafkabeat-events` and `kafkabeat-health` work in Discover. A few
ES|QL queries:

```esql
// Worst pairs right now
FROM kafkabeat-lag
| WHERE @timestamp > NOW() - 1 minute
| STATS upper = MAX(kafka.lag.seconds_upper) BY kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| SORT upper DESC | LIMIT 20

// Pairs not caught up, by state
FROM kafkabeat-lag
| WHERE @timestamp > NOW() - 1 minute AND kafka.lag.state != "caught_up"
| STATS pairs = COUNT_DISTINCT(kafka.consumergroup.id) BY kafka.lag.state

// Rebalances of a group over the last day
FROM kafkabeat-events
| WHERE @timestamp > NOW() - 1 day AND kafka.consumergroup.id == "my-group"
| KEEP @timestamp, kafka.consumergroup.generation, kafka.rebalance.joined.client_id, kafka.rebalance.left.client_id
| SORT @timestamp DESC
```

## 6. Alert rules

All four are created **disabled and without actions**:

| Rule | Type | Fires, per group of |
|---|---|---|
| kafkabeat: time lag > 5 min (upper bound) | Elasticsearch query | Upper bound > 300 s over the last minute, per cluster, topic and group. |
| kafkabeat: silent component (no successful cycle for 2 min, per cluster) | Custom threshold | No successful cycle for 2 min, per cluster: agent stopped ("alert on missing group" option) or Kafka unreachable on every cycle (`kafkabeat.cycle_failed`). |
| kafkabeat: production stopped for 15 min (consumed topic) | Elasticsearch query | `last_produce_age_s` > 15 min on a consumed topic, per cluster and topic. |
| kafkabeat: loss risk (80% of the retained log unread, or offsets lost) | Elasticsearch query | `retention_used` ≥ 0.8 or `commit_before_log_start`, per cluster, topic and group. |

**To enable them:**
1. Kibana needs a permanent `xpack.encryptedSavedObjects.encryptionKey` in `kibana.yml`.
2. Stack Management → Rules: open a rule, add a connector (email, Slack, webhook…), adjust the threshold, enable it.
3. Check its last execution in the rules list. Seen on Kibana 8.19: a rule enabled once, then re-imported, says it is
   active at the first enabling without ever running; disable it and enable it again.

"Silent component" only sees disappear the clusters it has seen alive: enable it while the agents run. A cluster removed
for good may stay in alert until the rule is disabled and re-enabled (not verified).

Thresholds (300 s, 0.8, 15 min, and the tile definitions) are constants of `kibana/examples.py`. To change them, edit the
constants, then regenerate the export against a Kibana 8.19 (`make kibana`; variables `KIBANA`,
`KIBANA_USER`/`KIBANA_PASSWORD` or `KIBANA_API_KEY`; `--export-only` only exports).

## 7. Checking the agent

**Health document** (`kafkabeat-health`, one per cycle):

| Field | Look for |
|---|---|
| `kafkabeat.cycle_duration_ms` | Must stay well below the interval (10,000 ms). |
| `kafkabeat.cycle_failed` | `true`: the cycle was abandoned (Kafka unreachable); only this document was sent. |
| `kafkabeat.cycle_incomplete`, `errors`, `partial_errors`, `group_errors` | Part of the collection failed; `errors` says what. |
| `kafkabeat.commits_mode`, `stream.ready`, `stream.errors`, `stream.last_error`, `stream.groups_fallback` | State of the `__consumer_offsets` stream; groups that fell back to polling. |
| `kafkabeat.stream.unknown_key_versions`, `unknown_value_versions` | Record versions the parser does not know (after a Kafka upgrade). |
| `kafkabeat.sink.failed`, `sink.last_error`, `sink.duplicates` | Writes refused by Elasticsearch; duplicates (409) are expected after a restart within the same slot. |
| `kafkabeat.jolokia.brokers_failed` | Brokers whose Jolokia did not answer. |
| `kafkabeat.last_snapshot` | Last state saved to disk. |

**Logs**: the agent logs to standard error (with systemd: `journalctl -u kafkabeat -f`). At startup it logs one line
(version, cluster, brokers, cycle, commit mode) and the state it restored; afterwards it only logs problems.

**Common problems**

| Symptom | Likely cause |
|---|---|
| No document at all | The agent cannot reach Elasticsearch: check `output.elasticsearch.urls`, credentials, and the logs. |
| Health documents with `cycle_failed: true` | Kafka unreachable: brokers, TLS, SASL, ACLs. |
| `errors` mentions authorization | Missing ACL: Describe on cluster, topics and groups; Read on `__consumer_offsets` in `hybrid` mode ([INSTALL.md](INSTALL.md#7-required-permissions)). |
| Pairs `gone` then back after a broker restart | Normal only for a few cycles; otherwise check `errors` for ListGroups failures. |
| `resumed_after_gap` on every start | The state directory is not persisted (container without a volume). |
| Two agents on one cluster | `sink.duplicates` keeps rising: run one instance per cluster. |
