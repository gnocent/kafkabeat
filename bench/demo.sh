#!/usr/bin/env bash
# Enregistre demokafkabeat.gif (démo du README) sur le banc démarré (make bench-up) :
# quatre topics produits et consommés, puis le consumer « billing » mis en pause
# 150 s (la paire orders + billing passe Lagging puis Stalled) et repris.
# Usage : bench/demo.sh  (variables : KIBANA, défaut http://localhost:5601 ; GO)
set -euo pipefail
cd "$(dirname "$0")/.."
KIBANA=${KIBANA:-http://localhost:5601}
GO=${GO:-go}
work=$(mktemp -d)
pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  for t in orders payments clicks audit-logs; do "$work/harness" topics delete --name "$t" >/dev/null 2>&1 || true; done
  rm -rf "$work"
}
trap cleanup EXIT

$GO build -o "$work/kafkabeat" ./cmd/kafkabeat
$GO build -o "$work/harness" ./bench/harness
curl -sf -X POST -H 'kbn-xsrf: true' "$KIBANA/api/saved_objects/_import?overwrite=true" \
  --form file=@kibana/kafkabeat-examples.ndjson >/dev/null

sed -e 's|^cluster: .*|cluster: demo|' -e "s|path: ./state|path: $work/state|" \
    -e 's|^  owners: \[\]|  owners: [{topic: "orders", owner: sales}, {topic: "payments", owner: payments}, {topic: "*", owner: platform}]|' \
    kafkabeat.example.yml > "$work/kafkabeat.yml"

for spec in orders:6 payments:4 clicks:8 audit-logs:2; do
  "$work/harness" topics create --name "${spec%:*}" --partitions "${spec#*:}" >/dev/null
done
"$work/harness" produce --topic orders --phases "steady:120:60s,burst:400:20s" --repeat --journal "$work/orders.journal" >/dev/null 2>&1 & pids+=($!)
"$work/harness" produce --topic payments --phases "steady:40:60s" --repeat --journal "$work/payments.journal" >/dev/null 2>&1 & pids+=($!)
"$work/harness" produce --topic clicks --phases "steady:300:30s,steady:500:30s" --repeat --journal "$work/clicks.journal" >/dev/null 2>&1 & pids+=($!)
"$work/harness" produce --topic audit-logs --phases "steady:10:60s" --repeat --journal "$work/audit-logs.journal" >/dev/null 2>&1 & pids+=($!)
"$work/harness" consume --group billing --topics orders --control 127.0.0.1:8191 >/dev/null 2>&1 & pids+=($!)
"$work/harness" consume --group ledger --topics payments --control 127.0.0.1:8192 >/dev/null 2>&1 & pids+=($!)
"$work/harness" consume --group analytics --topics clicks --control 127.0.0.1:8193 >/dev/null 2>&1 & pids+=($!)
"$work/harness" consume --group archiver --topics audit-logs --control 127.0.0.1:8194 >/dev/null 2>&1 & pids+=($!)
"$work/kafkabeat" -config "$work/kafkabeat.yml" 2>"$work/agent.log" & pids+=($!)

echo "warming up (120 s)"; sleep 120
for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null || { echo "a bench process stopped early"; cat "$work/agent.log"; exit 1; }; done
node bench/render/demo.mjs "$KIBANA" kafkabeat-dashboard "$work/frames" 240 4 'kafka.cluster.name : "demo"' & rec=$!
sleep 20; echo "pausing billing"; curl -sf -X POST 127.0.0.1:8191/pause
sleep 150; echo "resuming billing"; curl -sf -X POST 127.0.0.1:8191/resume
wait "$rec"

# 60 images toutes les 4 s, jouées à 5 images/s : 4 min en 12 s
ffmpeg -loglevel error -y -framerate 5 -i "$work/frames/frame-%04d.png" \
  -vf "scale=1000:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=128[p];[b][p]paletteuse=dither=bayer" \
  demokafkabeat.gif
ls -l demokafkabeat.gif
