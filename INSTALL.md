*(Version française : [INSTALL_fr.md](INSTALL_fr.md))*

# Installing kafkabeat

This guide goes from a first try on a local test bench to a production install, then details the configuration and the
permissions the agent needs. For an overview of the product, see the [README](README.md); for day-to-day use,
[USAGE.md](USAGE.md).

## 1. What you need

- **Kafka** 3.9 or 4.x in KRaft mode (checked on 3.9 and 4.4; older versions are not tested). PLAINTEXT, TLS, mTLS,
  SASL PLAIN or SCRAM.
- **Elasticsearch and Kibana** 8.19 or 9.x (checked on 8.19 and 9.5, Basic license).
- **One agent per Kafka cluster**, on any host that reaches the brokers and Elasticsearch: a Linux server (systemd), a
  container, or a Nomad job. Memory: plan up to 2 GB for 30,000 partitions (the acceptance target, measured on a
  simulator).
- **Permissions**: Describe on the Kafka cluster, topics and groups, Read on `__consumer_offsets`; an Elasticsearch role
  that can write its data streams ([§7](#7-required-permissions)).

## 2. Quick start on the test bench

Ten minutes on a Linux workstation (or Windows with WSL2) with Docker Engine 25+ and Compose 2.20+, Go 1.27+, `make`
and `curl`. Nothing on the bench is authenticated; its ports listen on `127.0.0.1` only.

1. **Get the sources and start the bench** (about 1.5 min once the images are pulled):

   ```bash
   git clone https://github.com/gnocent/kafkabeat.git && cd kafkabeat
   make bench-up
   ```

   You get Kafka 3.9 on `localhost:19092`, Kafka 4.4 on `localhost:19094`, Elasticsearch and Kibana 8.19 on
   `localhost:9200` and `localhost:5601`, Elasticsearch and Kibana 9.5 on `localhost:9201` and `localhost:5602`.
2. **Build and start the agent** in a second terminal. `kafkabeat.example.yml` already points to the bench (Kafka 3.9,
   Elasticsearch 8.19):

   ```bash
   make check                                            # vet, unit tests, bin/kafkabeat
   ./bin/kafkabeat -config kafkabeat.example.yml
   ```

   It installs its templates and ILM policies, then writes one cycle every 10 s. To see the documents without
   Elasticsearch: `./bin/kafkabeat -config kafkabeat.example.yml -stdout -once`.
3. **Import the dashboards**:

   ```bash
   curl -X POST -H 'kbn-xsrf: true' "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
        --form file=@kibana/kafkabeat-examples.ndjson
   ```

4. **Create traffic** with the bench harness, in a third terminal:

   ```bash
   go build -o /tmp/harness ./bench/harness
   /tmp/harness topics create --name orders --partitions 6
   /tmp/harness produce --topic orders --phases "steady:200:60s,burst:2000:10s" --repeat --journal /tmp/orders.journal &
   /tmp/harness consume --group billing --topics orders --control 127.0.0.1:8181 &
   ```

5. **Watch**: open <http://localhost:5601>, dashboard **kafkabeat: overview**. After a cycle or two, the pair
   `orders` + `billing` shows **Caught up** or **Lagging**.
6. **Stop the consumer without leaving the group**, and watch the pair turn **Stalled** after 60 s, its upper bound
   growing by 10 s per cycle; then resume it:

   ```bash
   curl -X POST 127.0.0.1:8181/pause
   curl -X POST 127.0.0.1:8181/resume
   ```

   Open **kafkabeat: topic + group zoom** on `orders` and `billing` to see the bounds, rates and partitions.
7. **Clean up**: stop the harness (`kill %1 %2`), the agent (`Ctrl+C`), then `make bench-down` (deletes the bench data).

More harness commands (rebalances, resets, custom assignors): [bench/README.md](bench/README.md).

## 3. Linux server with systemd

1. **Download** the archive and `SHA256SUMS` from the [Releases](https://github.com/gnocent/kafkabeat/releases) page,
   then check and unpack:

   ```bash
   sha256sum -c SHA256SUMS --ignore-missing
   tar -xzf kafkabeat-<version>-linux-amd64.tar.gz && cd kafkabeat-<version>
   ```

2. **Install the binary, the configuration and the unit**:

   ```bash
   sudo install -m 0755 kafkabeat /usr/local/bin/kafkabeat
   sudo install -d -m 0750 /etc/kafkabeat
   sudo install -m 0600 kafkabeat.example.yml /etc/kafkabeat/kafkabeat.yml
   sudo install -m 0644 kafkabeat.service /etc/systemd/system/kafkabeat.service
   ```

3. **Edit** `/etc/kafkabeat/kafkabeat.yml` ([§8](#8-configuration)): at least `cluster`, `kafka.brokers`, security,
   and `output.elasticsearch`.
4. **Check** the configuration with one cycle printed to the terminal, then **start** the service:

   ```bash
   sudo /usr/local/bin/kafkabeat -config /etc/kafkabeat/kafkabeat.yml -stdout -once | head
   sudo systemctl daemon-reload && sudo systemctl enable --now kafkabeat
   journalctl -u kafkabeat -f
   ```

The unit (systemd 247 or later) runs under a transient user (`DynamicUser`, nothing to create), receives the
configuration through `LoadCredential` (readable by the service only) and writes its state to
`/var/lib/kafkabeat/state` (`StateDirectory`). TLS certificates and keys are declared the same way (see the comments in
the unit). Its sandbox is checked for syntax only: validate it on the target host at first start
(`systemd-analyze security kafkabeat`, and the journal).

**Several clusters on one host**: copy the unit to `kafkabeat-<cluster>.service` with its own configuration file,
credential name and `StateDirectory`.

## 4. Container

The `Dockerfile` builds a `scratch` image (static binary, CA certificates, non-root user 65532). The image only carries
the example configuration: mount the real one, and a volume for the state.

```bash
make image                                   # kafkabeat:<version>, from the sources
docker run -d --name kafkabeat \
  -v "$PWD/kafkabeat.yml:/etc/kafkabeat/kafkabeat.yml:ro" \
  -v kafkabeat-state:/var/lib/kafkabeat \
  kafkabeat:<version>
```

The working directory is `/var/lib/kafkabeat`, so the default `state.path: ./state` lands on the volume. To use the image
elsewhere (Nomad, Kubernetes), tag and push it to your registry:
`docker tag kafkabeat:<version> registry.example.com/kafkabeat:<version> && docker push registry.example.com/kafkabeat:<version>`.

## 5. HashiCorp Nomad

[deploy/kafkabeat.nomad.hcl](deploy/kafkabeat.nomad.hcl) runs one agent with the Docker driver. The whole configuration
file, credentials included, is stored as a Nomad Variable, never in the job file.

1. **Push the image** to a registry your Nomad clients can pull from ([§4](#4-container)).
2. **Store the configuration** at the job's variable path, with `state.path: /alloc/data/state`:

   ```bash
   nomad var put nomad/jobs/kafkabeat config=@kafkabeat.yml
   ```

   The job's tasks can read `nomad/jobs/<job name>` with no extra ACL policy (workload identity).
3. **Run the job**:

   ```bash
   nomad job run -var image=registry.example.com/kafkabeat:<version> deploy/kafkabeat.nomad.hcl
   nomad alloc logs -f -job kafkabeat
   ```

**Several clusters**: one job per cluster. Copy the file, change the job name (`job "kafkabeat-prod-eu"`), and store
that cluster's configuration at `nomad/jobs/kafkabeat-prod-eu`.

The job keeps a single allocation, restarts it on failure, and re-renders the configuration (restarting the task) when
the variable changes. The configuration file is rendered for the image's user (65532), which needs a Nomad client
running as root, the usual setup for the Docker driver. The state lives on the allocation's sticky ephemeral disk: it
survives restarts and is migrated with the allocation when possible. If it is lost, the agent starts fresh: bounds are
open until the history fills again. Adjust `datacenters`, `resources` (memory) and constraints to your cluster.

## 6. From source

With Go 1.27 or later:

```bash
go install github.com/gnocent/kafkabeat/cmd/kafkabeat@latest      # binary only, in $(go env GOPATH)/bin
```

Or from a clone, with `make`: `make check` (vet, tests, `bin/kafkabeat`), `make dist` (release archives for Linux,
macOS and Windows, and `SHA256SUMS`, in `dist/`), `make image` (container image). Dependencies are vendored: the build
needs no network access.

## 7. Required permissions

**Kafka ACLs** for the agent's principal: `Describe` on the cluster, on all topics and on all groups; `Read` on the topic
`__consumer_offsets` in `hybrid` mode (commits and group members). The agent never reads application topics and never
writes to Kafka. For instance:

```bash
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Describe --cluster
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Describe --topic '*' --group '*'
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Read --topic __consumer_offsets
```

**Elasticsearch role**: the agent installs ILM policies and index templates, creates its data streams, rolls them over
when a template version changes, and writes with bulk `create`.

```json
PUT _security/role/kafkabeat_writer
{
  "cluster": ["manage_ilm", "manage_index_templates"],
  "indices": [{
    "names": ["kafkabeat-*"],
    "privileges": ["create_doc", "create_index", "manage", "view_index_metadata"]
  }]
}
```

With `setup: false` (templates and policies installed by the operator), `create_doc` and `view_index_metadata` on
`kafkabeat-*` are enough. Use the `prefix` you configured instead of `kafkabeat`. Not checked against a secured cluster:
the test bench has no security.

**Jolokia** (optional): read access (`read` operation) to the broker MBeans. Basic Auth travels in clear text unless
`jolokia.tls` is enabled.

## 8. Configuration

The commented reference is [kafkabeat.example.yml](kafkabeat.example.yml). The configuration is strict: an unknown key is
an error. The keys you will usually change:

| Key | Default | Meaning |
|---|---|---|
| `cluster` | (required) | Cluster name in every document (`kafka.cluster.name`). One agent per cluster. |
| `kafka.brokers` | (required) | Bootstrap brokers. |
| `kafka.tls.*`, `kafka.sasl.*` | off | TLS (`ca_file`, and `cert_file` + `key_file` for mTLS); SASL `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, to be used over TLS. |
| `collect.topics`, `collect.groups` | all | `include` / `exclude` glob patterns. |
| `collect.stalled_after` | `60s` | Frozen commit with lag → `stalled`; no message → production `stopped`. |
| `collect.commits.mode` | `hybrid` | `hybrid` (stream + reconciliation every `reconcile_interval`) or `polling`. |
| `state.path` | `./state` | State directory, relative to the working directory. |
| `output.elasticsearch.urls` | | Several URLs: the next one is tried on a transport error. |
| `output.elasticsearch.username`, `password` or `api_key` | | Credentials. |
| `output.elasticsearch.setup` | `true` | Install templates, mappings and ILM policies at startup. |
| `output.elasticsearch.prefix` | `kafkabeat` | Data stream prefix. |
| `output.owners` | none | Topic owner rules (glob → team) for the "Team" filter. |
| `output.fields` | none | Static fields added to every document. |
| `jolokia.enabled` | `false` | Broker metrics through Jolokia (`port`, `path`, credentials, `tls`). |

The configuration file holds credentials: keep it `0600`, readable by the agent only. Command-line options:
`-config <file>`, `-stdout` (print documents instead of sending them), `-once` (one cycle, then exit), `-version`.

## 9. Upgrading and uninstalling

**Upgrading**: replace the binary (or the image) and restart. The state on disk is kept; a newer template version is
installed at startup and the data streams roll over to it, without touching the documents already written. Read the
[CHANGELOG](CHANGELOG.md) first.

**Uninstalling** (systemd):

```bash
sudo systemctl disable --now kafkabeat
sudo systemctl clean --what=state kafkabeat          # removes /var/lib/kafkabeat
sudo rm /etc/systemd/system/kafkabeat.service /usr/local/bin/kafkabeat && sudo rm -r /etc/kafkabeat
sudo systemctl daemon-reload
```

The data in Elasticsearch expires with its ILM policies (90 days at most); delete the `kafkabeat-*` data streams,
templates and policies to remove it at once.

Troubleshooting: [USAGE.md, §7](USAGE.md#7-checking-the-agent).
