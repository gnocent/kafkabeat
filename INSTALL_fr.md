*(English version: [INSTALL.md](INSTALL.md))*

# Installer kafkabeat

Ce guide va d'un premier essai sur un banc de test local à une installation en production, puis détaille la configuration
et les droits dont l'agent a besoin. Pour une présentation du produit, voir le [README](README_fr.md) ; pour l'usage au
quotidien, [USAGE_fr.md](USAGE_fr.md).

## 1. Ce qu'il faut

- **Kafka** 3.9 ou 4.x en mode KRaft (vérifié sur 3.9 et 4.4 ; les versions antérieures ne sont pas testées). PLAINTEXT,
  TLS, mTLS, SASL PLAIN ou SCRAM.
- **Elasticsearch et Kibana** 8.19 ou 9.x (vérifié sur 8.19 et 9.5, licence Basic).
- **Un agent par cluster Kafka**, sur n'importe quel hôte qui joint les brokers et Elasticsearch : un serveur Linux
  (systemd), un conteneur ou un job Nomad. Mémoire : prévoir jusqu'à 2 Go pour 30 000 partitions (la cible des critères
  d'acceptation, mesurée sur un simulateur).
- **Droits** : Describe sur le cluster Kafka, les topics et les groupes, Read sur `__consumer_offsets` ; un rôle
  Elasticsearch qui peut écrire dans ses data streams ([§7](#7-droits-nécessaires)).

## 2. Démarrage rapide sur le banc de test

Dix minutes sur un poste Linux (ou Windows avec WSL2) avec Docker Engine 25+ et Compose 2.20+, Go 1.27+, `make` et
`curl`. Rien n'est authentifié sur le banc ; ses ports n'écoutent que sur `127.0.0.1`.

1. **Récupérer les sources et démarrer le banc** (environ 1,5 min une fois les images tirées) :

   ```bash
   git clone https://github.com/gnocent/kafkabeat.git && cd kafkabeat
   make bench-up
   ```

   Vous obtenez Kafka 3.9 sur `localhost:19092`, Kafka 4.4 sur `localhost:19094`, Elasticsearch et Kibana 8.19 sur
   `localhost:9200` et `localhost:5601`, Elasticsearch et Kibana 9.5 sur `localhost:9201` et `localhost:5602`.
2. **Construire et lancer l'agent** dans un deuxième terminal. `kafkabeat.example.yml` pointe déjà vers le banc
   (Kafka 3.9, Elasticsearch 8.19) :

   ```bash
   make check                                            # vet, tests unitaires, bin/kafkabeat
   ./bin/kafkabeat -config kafkabeat.example.yml
   ```

   Il installe ses templates et ses politiques ILM, puis écrit un cycle toutes les 10 s. Pour voir les documents sans
   Elasticsearch : `./bin/kafkabeat -config kafkabeat.example.yml -stdout -once`.
3. **Importer les dashboards** :

   ```bash
   curl -X POST -H 'kbn-xsrf: true' "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
        --form file=@kibana/kafkabeat-examples.ndjson
   ```

4. **Créer du trafic** avec le harnais du banc, dans un troisième terminal :

   ```bash
   go build -o /tmp/harness ./bench/harness
   /tmp/harness topics create --name orders --partitions 6
   /tmp/harness produce --topic orders --phases "steady:200:60s,burst:2000:10s" --repeat --journal /tmp/orders.journal &
   /tmp/harness consume --group billing --topics orders --control 127.0.0.1:8181 &
   ```

5. **Regarder** : ouvrir <http://localhost:5601>, dashboard **kafkabeat: overview** (vue d'ensemble). Après un cycle ou
   deux, la paire `orders` + `billing` affiche **Caught up** ou **Lagging**.
6. **Arrêter le consumer sans quitter le groupe**, et regarder la paire passer en **Stalled** au bout de 60 s, sa borne
   haute croissant de 10 s par cycle ; puis le relancer :

   ```bash
   curl -X POST 127.0.0.1:8181/pause
   curl -X POST 127.0.0.1:8181/resume
   ```

   Ouvrir **kafkabeat: topic + group zoom** (zoom topic + groupe) sur `orders` et `billing` pour voir les bornes, les
   taux et les partitions.
7. **Nettoyer** : arrêter le harnais (`kill %1 %2`), l'agent (`Ctrl+C`), puis `make bench-down` (efface les données du
   banc).

Autres commandes du harnais (rebalances, resets, assignateurs sur mesure) : [bench/README_fr.md](bench/README_fr.md).

## 3. Serveur Linux avec systemd

1. **Télécharger** l'archive et `SHA256SUMS` depuis la page [Releases](https://github.com/gnocent/kafkabeat/releases),
   puis vérifier et décompresser :

   ```bash
   sha256sum -c SHA256SUMS --ignore-missing
   tar -xzf kafkabeat-<version>-linux-amd64.tar.gz && cd kafkabeat-<version>
   ```

2. **Installer le binaire, la configuration et l'unité** :

   ```bash
   sudo install -m 0755 kafkabeat /usr/local/bin/kafkabeat
   sudo install -d -m 0750 /etc/kafkabeat
   sudo install -m 0600 kafkabeat.example.yml /etc/kafkabeat/kafkabeat.yml
   sudo install -m 0644 kafkabeat.service /etc/systemd/system/kafkabeat.service
   ```

3. **Adapter** `/etc/kafkabeat/kafkabeat.yml` ([§8](#8-configuration)) : au moins `cluster`, `kafka.brokers`, la sécurité
   et `output.elasticsearch`.
4. **Vérifier** la configuration avec un cycle affiché dans le terminal, puis **démarrer** le service :

   ```bash
   sudo /usr/local/bin/kafkabeat -config /etc/kafkabeat/kafkabeat.yml -stdout -once | head
   sudo systemctl daemon-reload && sudo systemctl enable --now kafkabeat
   journalctl -u kafkabeat -f
   ```

L'unité (systemd 247 ou plus) tourne sous un utilisateur éphémère (`DynamicUser`, rien à créer), reçoit la configuration
par `LoadCredential` (lisible du seul service) et écrit son état dans `/var/lib/kafkabeat/state` (`StateDirectory`).
Certificats et clés TLS se déclarent de la même façon (voir les commentaires de l'unité). Son bac à sable n'est vérifié
qu'en syntaxe : à valider sur l'hôte cible au premier démarrage (`systemd-analyze security kafkabeat`, et le journal).

**Plusieurs clusters sur un hôte** : copier l'unité en `kafkabeat-<cluster>.service`, avec son propre fichier de
configuration, son propre nom d'identifiant et son propre `StateDirectory`.

## 4. Conteneur

Le `Dockerfile` construit une image `scratch` (binaire statique, certificats d'autorités, utilisateur non root 65532).
L'image n'embarque que la configuration d'exemple : monter la vraie, et un volume pour l'état.

```bash
make image                                   # kafkabeat:<version>, depuis les sources
docker run -d --name kafkabeat \
  -v "$PWD/kafkabeat.yml:/etc/kafkabeat/kafkabeat.yml:ro" \
  -v kafkabeat-state:/var/lib/kafkabeat \
  kafkabeat:<version>
```

Le répertoire de travail est `/var/lib/kafkabeat` : le `state.path: ./state` par défaut tombe donc sur le volume. Pour
utiliser l'image ailleurs (Nomad, Kubernetes), l'étiqueter et la pousser dans votre registre :
`docker tag kafkabeat:<version> registry.example.com/kafkabeat:<version> && docker push registry.example.com/kafkabeat:<version>`.

## 5. HashiCorp Nomad

[deploy/kafkabeat.nomad.hcl](deploy/kafkabeat.nomad.hcl) fait tourner un agent avec le driver Docker. Tout le fichier de
configuration, identifiants compris, est stocké dans une Nomad Variable, jamais dans le fichier du job.

1. **Pousser l'image** dans un registre accessible à vos clients Nomad ([§4](#4-conteneur)).
2. **Stocker la configuration** au chemin de variable du job, avec `state.path: /alloc/data/state` :

   ```bash
   nomad var put nomad/jobs/kafkabeat config=@kafkabeat.yml
   ```

   Les tâches du job peuvent lire `nomad/jobs/<nom du job>` sans politique ACL supplémentaire (workload identity).
3. **Lancer le job** :

   ```bash
   nomad job run -var image=registry.example.com/kafkabeat:<version> deploy/kafkabeat.nomad.hcl
   nomad alloc logs -f -job kafkabeat
   ```

**Plusieurs clusters** : un job par cluster. Copier le fichier, changer le nom du job (`job "kafkabeat-prod-eu"`) et
stocker la configuration de ce cluster dans `nomad/jobs/kafkabeat-prod-eu`.

Le job garde une seule allocation, la relance en cas d'échec et régénère la configuration (en relançant la tâche) quand
la variable change. Le fichier de configuration est écrit pour l'utilisateur de l'image (65532), ce qui demande un client
Nomad lancé en root, le cas habituel avec le driver Docker. L'état vit sur le disque éphémère persistant (sticky) de
l'allocation : il survit aux relances et suit l'allocation quand c'est possible. S'il est perdu, l'agent repart de zéro :
les bornes restent ouvertes jusqu'à ce que l'historique se remplisse de nouveau. Adapter `datacenters`, `resources`
(mémoire) et les contraintes à votre cluster.

## 6. Depuis les sources

Avec Go 1.27 ou plus :

```bash
go install github.com/gnocent/kafkabeat/cmd/kafkabeat@latest      # binaire seul, dans $(go env GOPATH)/bin
```

Ou depuis un clone, avec `make` : `make check` (vet, tests, `bin/kafkabeat`), `make dist` (archives de version pour Linux,
macOS et Windows, et `SHA256SUMS`, dans `dist/`), `make image` (image conteneur). Les dépendances sont vendorisées : la
construction n'a besoin d'aucun accès réseau.

## 7. Droits nécessaires

**ACL Kafka** pour le principal de l'agent : `Describe` sur le cluster, sur tous les topics et sur tous les groupes ;
`Read` sur le topic `__consumer_offsets` en mode `hybrid` (commits et membres des groupes). L'agent ne lit jamais les
topics applicatifs et n'écrit jamais dans Kafka. Par exemple :

```bash
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Describe --cluster
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Describe --topic '*' --group '*'
kafka-acls.sh --bootstrap-server <broker> --add --allow-principal User:kafkabeat --operation Read --topic __consumer_offsets
```

**Rôle Elasticsearch** : l'agent installe politiques ILM et index templates, crée ses data streams, les bascule (rollover)
quand une version de template change, et écrit en bulk `create`.

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

Avec `setup: false` (templates et politiques installés par l'exploitant), `create_doc` et `view_index_metadata` sur
`kafkabeat-*` suffisent. Remplacer `kafkabeat` par le `prefix` configuré. Non vérifié contre un cluster sécurisé : le banc
de test n'a pas de sécurité.

**Jolokia** (option) : accès en lecture (opération `read`) aux MBeans des brokers. La Basic Auth circule en clair si
`jolokia.tls` n'est pas activé.

## 8. Configuration

La référence commentée est [kafkabeat.example.yml](kafkabeat.example.yml). La configuration est stricte : une clé inconnue
est une erreur. Les clés qu'on change le plus souvent :

| Clé | Défaut | Signification |
|---|---|---|
| `cluster` | (obligatoire) | Nom du cluster dans chaque document (`kafka.cluster.name`). Un agent par cluster. |
| `kafka.brokers` | (obligatoire) | Brokers d'amorçage. |
| `kafka.tls.*`, `kafka.sasl.*` | désactivés | TLS (`ca_file`, et `cert_file` + `key_file` pour mTLS) ; SASL `PLAIN`, `SCRAM-SHA-256` ou `SCRAM-SHA-512`, à utiliser sur TLS. |
| `collect.topics`, `collect.groups` | tous | Motifs glob `include` / `exclude`. |
| `collect.stalled_after` | `60s` | Commit figé avec du lag → `stalled` ; aucun message → production `stopped`. |
| `collect.commits.mode` | `hybrid` | `hybrid` (flux + réconciliation toutes les `reconcile_interval`) ou `polling`. |
| `state.path` | `./state` | Répertoire d'état, relatif au répertoire de travail. |
| `output.elasticsearch.urls` | | Plusieurs URL : la suivante est essayée sur une erreur de transport. |
| `output.elasticsearch.username`, `password` ou `api_key` | | Identifiants. |
| `output.elasticsearch.setup` | `true` | Installer templates, mappings et politiques ILM au démarrage. |
| `output.elasticsearch.prefix` | `kafkabeat` | Préfixe des data streams. |
| `output.owners` | aucune | Règles de propriétaire des topics (glob → équipe) pour le filtre « Team ». |
| `output.fields` | aucun | Champs statiques ajoutés à chaque document. |
| `jolokia.enabled` | `false` | Métriques des brokers par Jolokia (`port`, `path`, identifiants, `tls`). |

Le fichier de configuration porte des identifiants : le garder en `0600`, lisible du seul agent. Options de la ligne de
commande : `-config <fichier>`, `-stdout` (affiche les documents au lieu de les envoyer), `-once` (un cycle, puis
sortie), `-version`.

## 9. Mise à jour et désinstallation

**Mise à jour** : remplacer le binaire (ou l'image) et relancer. L'état sur disque est conservé ; une version de template
plus récente est installée au démarrage et les data streams basculent dessus, sans toucher aux documents déjà écrits.
Lire d'abord le [CHANGELOG](CHANGELOG_fr.md).

**Désinstallation** (systemd) :

```bash
sudo systemctl disable --now kafkabeat
sudo systemctl clean --what=state kafkabeat          # supprime /var/lib/kafkabeat
sudo rm /etc/systemd/system/kafkabeat.service /usr/local/bin/kafkabeat && sudo rm -r /etc/kafkabeat
sudo systemctl daemon-reload
```

Les données dans Elasticsearch expirent avec leurs politiques ILM (90 jours au plus) ; supprimer les data streams,
templates et politiques `kafkabeat-*` pour les effacer d'un coup.

Dépannage : [USAGE_fr.md, §7](USAGE_fr.md#7-vérifier-lagent).
