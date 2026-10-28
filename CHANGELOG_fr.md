*(English version: [CHANGELOG.md](CHANGELOG.md))*

# Changements

Une entrée par version publiée, la plus récente en tête ; « à paraître » regroupe ce qui attend le prochain tag.

## v0.1.0 (2026-10-28)

Première version publique. Fonctionnement et validation : [DESIGN_fr.md](DESIGN_fr.md).

- Lag temporel de chaque paire topic + consumer group toutes les 10 s, par une borne basse et une borne haute lues dans
  l'historique des offsets de fin, sans interpolation : états (`caught_up`, `lagging`, `stalled`, `unassigned`,
  `no_commit`, `gone`), statut de production, taux, flags, risque de perte (`retention_used`,
  `commit_before_log_start`), propriétaire du topic (`kafka.topic.owner`).
- Offsets commités lus dans le flux `__consumer_offsets` avec polling de réconciliation et repli par groupe (mode
  `hybrid`), ou par polling seul.
- Documents compatibles Metricbeat (`kafka.partition.*`, `kafka.consumergroup.*`, `kafka.broker.*` par Jolokia) toutes
  les 60 s ; un document par rebalance d'un consumer group classique (`kafkabeat-events`) ; un document de santé à chaque
  cycle.
- Data streams de lag et de métriques en TSDS, sous-échantillonnés par ILM (7 jours bruts, puis une valeur par heure
  jusqu'à 90 jours) ; templates, mappings et politiques installés au démarrage.
- Dashboards et règles d'alerte Kibana d'exemple (`kibana/`), banc de test Docker (Kafka 3.9 et 4.4, Elastic 8.19 et
  9.5) et recette automatisée de 16 critères.
- Binaires statiques pour Linux (x86-64), macOS (Apple Silicon) et Windows (x86-64) ; unité systemd durcie
  (`DynamicUser`, `LoadCredential`) ; image conteneur `scratch` sans root ; exemple de job HashiCorp Nomad.
