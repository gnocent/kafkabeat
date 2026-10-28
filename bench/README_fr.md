*(English version: [README.md](README.md))*

# Banc de test local

Le banc fait tourner deux clusters Kafka et deux stacks Elastic dans Docker, pour le démarrage rapide
([INSTALL_fr.md](../INSTALL_fr.md#2-démarrage-rapide-sur-le-banc-de-test)) et la recette (`make recette`,
[DESIGN_fr.md](../DESIGN_fr.md#7-validation)).

## Démarrage et arrêt

`make bench-up` (environ 1,5 min à froid, images déjà tirées ; exige Docker Engine 25 et Compose 2.20 au moins, pour
`start_interval` des healthchecks) ; `make bench-down` l'arrête et **efface ses données**. Rien n'est authentifié : tous
les ports sont publiés sur `127.0.0.1` seulement.

| Service | Adresse | Notes |
|---|---|---|
| Kafka 3.9 (KRaft) | `localhost:19092` | Jolokia 1.7.2 sur `localhost:8778` (il s'annonce en 1.7.1). |
| Kafka 4.4 (KRaft) | `localhost:19094` | Jolokia 2.6.3 sur `localhost:8779`. |
| Elasticsearch / Kibana 8.19 | `localhost:9200` / `localhost:5601` | Sans sécurité, licence Basic. |
| Elasticsearch / Kibana 9.5 | `localhost:9201` / `localhost:5602` | Idem ; sert à vérifier l'import et le rendu des dashboards. |
| Metricbeat | | Metricsets `partition` et `consumergroup` des deux clusters toutes les 30 s dans `metricbeat-*`, `broker` toutes les 10 s ; champ `bench_cluster` = `kafka39` ou `kafka44`. |

- Les brokers appliquent la rétention toutes les 5 s (`log.retention.check.interval.ms`), pour le critère du risque de
  perte (14).
- Les agents Jolokia tournent dans la JVM de chaque broker (`KAFKA_OPTS`) ; `make bench-up` les télécharge une fois depuis
  Maven Central et vérifie leur somme SHA-256 (`bench/jolokia/`, hors git). Les outils lancés par `docker exec` dans un
  conteneur Kafka affichent « Could not start Jolokia agent » (le port est pris par le broker) et fonctionnent
  normalement.
- Avec Jolokia 2.x (kafka44), Metricbeat ne publie que les documents broker par topic : son document des MBeans fixes est
  rejeté par Elasticsearch (les MBeans ZooKeeper sont absents en KRaft et reviennent en objets d'erreur) et n'apparaît
  que dans `/usr/share/metricbeat/logs/metricbeat-events-data-*.ndjson`, où le critère 11 le relit. Avec Jolokia 1.x
  (kafka39), il est publié normalement.

## Harnais

`go build -o /tmp/harness ./bench/harness`. Toutes les commandes acceptent `--bootstrap` (défaut `localhost:19092`).

```sh
/tmp/harness topics create --name t1 --partitions 6                     # aussi : delete, recreate (nouvel id de topic)
/tmp/harness topics create --bootstrap localhost:19094 --name t2 --config retention.ms=15000,segment.ms=2000
/tmp/harness produce --topic t1 --phases "steady:50:10s,silence:0:5s,burst:2000:5s" --journal t1.journal   # --repeat, --size
/tmp/harness consume --group g1 --topics t1 --commit auto --commit-interval 2s --control 127.0.0.1:8181   # --commit manual, --max-rate 10
#   --balancer range|roundrobin|sticky|cooperative-sticky|omit=t1/3 (tourniquet qui n'assigne jamais t1/3 ; même balancer pour tous les membres)
#   --session-timeout 6s (départ vu vite après un kill), --revoke-delay 25s (retient chaque rebalance),
#   --assign (toutes les partitions sans rejoindre le groupe, commits pour --group)
#   --server-side (protocole consumer KIP-848, Kafka 4.0+, balancer range par défaut ; sur 3.9 franz-go revient au protocole classique)
curl -X POST 127.0.0.1:8181/pause      # stoppe consommation et commits sans quitter le groupe ; /resume reprend
curl -X POST 127.0.0.1:8181/commit     # commite maintenant les offsets consommés
curl -X POST "127.0.0.1:8181/reset?to=earliest"   # ou latest, offset=<n> : quitte le groupe, réécrit les offsets, rejoint
curl 127.0.0.1:8181/status             # état, offsets consommés et commités par partition, membre, génération, partitions assignées
/tmp/harness groups delete --group g1  # le groupe doit être sans membre
```

Le journal contient une ligne JSON par message produit (`topic`, `partition`, `offset`, `produced_at`) ; le package
`bench/harness/journal` le relit (`Reader.ProducedAt`).

Vérification côté Kafka :
`docker exec kafka39 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group g1`.
Côté Elasticsearch :
`curl "localhost:9200/metricbeat-*/_search?q=metricset.name:consumergroup&size=1&sort=@timestamp:desc"`.

## Rendu d'un dashboard

`node bench/render/render.mjs http://localhost:5601 kafkabeat-dashboard [from] [to] [capture.png]` affiche les durées de
chargement et de rafraîchissement et les panneaux en erreur, dans Chromium headless. Le Chromium vient du cache Playwright
(`~/.cache/ms-playwright`), que `make recette` remplit (`npm ci`, puis `npx playwright-core install chromium` dans
`bench/render` : `playwright-core` seul n'embarque aucun navigateur) ; sur un Ubuntu neuf,
`npx playwright-core install --with-deps chromium` ajoute les bibliothèques système.

## Recette

`make recette` rejoue les 16 critères de [DESIGN_fr.md](../DESIGN_fr.md#7-validation). Variables :
`KB_KAFKA39`, `KB_KAFKA44`, `KB_ES`, `KB_ES9`, `KB_KIBANA`, `KB_KIBANA9`, `GO`, `KB_LONG=1` (scénario de 2 heures pour
le critère 1), `KB_NO_SANDBOX=1` (Chromium sans bac à sable). La recette suppose toujours ce banc : elle met en pause les
conteneurs `kafka39` et `kafka44` et lit les journaux de Metricbeat avec `docker exec`.

Le critère 5 injecte un enregistrement en version inconnue dans la partition 49 de `__consumer_offsets` des deux
clusters, que les coordinateurs ne savent plus charger (4.x aussitôt, 3.9 au redémarrage) ; les groupes du banc évitent
cette partition. Remettre le banc à neuf ensuite (`make bench-down`).
