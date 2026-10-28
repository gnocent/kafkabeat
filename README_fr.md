*(English version: [README.md](README.md))*

# kafkabeat

Le **lag temporel** de chaque paire topic + consumer group d'un cluster Kafka, en secondes, toutes les 10 secondes, publié
dans Elasticsearch, avec des dashboards et des règles d'alerte Kibana d'exemple. Conçu pour les grands parcs Kafka à fort
débit où Kafka sert de tampon entre producteurs et consommateurs : des milliers de topics et des dizaines de milliers de
partitions, où comptent la vue d'ensemble et des signaux fiables en quasi temps réel, pas la latence à la milliseconde.

## Démo

<p align="center"><img src="demokafkabeat.gif" alt="Démo animée : le dashboard de vue d'ensemble de kafkabeat sur un cluster de test ; un consumer group cesse de commiter, sa paire passe en Stalled et son lag temporel croît de 10 secondes par cycle, puis le consumer reprend et la paire est de nouveau à jour"></p>

> [!NOTE]
> **Comment ce projet est fait.** Je ne suis pas développeur : j'écris les spécifications ([DESIGN_fr.md](DESIGN_fr.md))
> et je vérifie le résultat ; le code est écrit par [Claude Code](https://claude.com/claude-code). Ce qui compense : des
> tests unitaires lancés avec le détecteur de concurrence (race detector), et une recette automatisée de 16 critères
> rejouée sur un banc de test Docker avec deux versions de Kafka (3.9 et 4.4, KRaft) et deux stacks Elastic (8.19 et 9.5).
> C'est une **première version publique (0.1.0)**, encore peu utilisée : ce qui a été vérifié, et ce qui ne l'a pas été,
> est dans [DESIGN_fr.md](DESIGN_fr.md#7-validation) et sous [Sécurité](#sécurité).

## Pourquoi kafkabeat

Le lag en offsets (« 42 000 messages de retard ») dit peu de chose à lui seul : 42 000 messages, c'est une seconde sur un
topic et une journée sur un autre. kafkabeat répond à la question que se pose vraiment l'astreinte, **de combien de
secondes ce consumer group est-il en retard**, pour chaque paire d'un cluster, à chaque cycle, y compris quand la
consommation s'est complètement arrêtée.

- **Des secondes, encadrées, jamais devinées.** L'agent garde un historique de l'offset de fin de chaque partition et du
  moment où il a été vu. Le lag est donné par une borne basse et une borne haute, lues dans cet historique sans
  interpolation ni débit moyen. Les alertes se déclenchent sur la borne haute.
- **Fait pour le volume.** Un seul binaire statique par cluster, qui ne lit que des métadonnées Kafka (aucun topic
  applicatif n'est jamais lu). Un document par paire et par cycle, lag ou pas, pour qu'une paire muette reste visible.
  Mesuré sur un simulateur : un cycle à 30 000 partitions prend bien moins que l'intervalle de 10 s.
- **Des états, pas seulement des chiffres.** Chaque paire dit ce qui se passe : `caught_up`, `lagging`, `stalled` (offset
  commité figé avec du lag), `unassigned` (une partition qu'aucun membre ne lit), `no_commit`, `gone`. La production a son
  propre statut (`active`, `stopped`) : un problème de production se distingue d'un coup d'œil d'un problème de
  consommation.
- **Les signaux autour du lag.** Rebalances (un document par génération de groupe, membres entrés et sortis), partitions
  sans membre assigné, risque de perte (part du journal retenu qu'un groupe n'a pas encore lue, et offsets réellement
  perdus par la rétention), resets d'offset, topics recréés, et un document de santé pour l'agent lui-même.
- **S'insère dans un parc Elastic.** Des métriques aux mêmes noms que celles du module Kafka de Metricbeat
  (`kafka.partition.*`, `kafka.consumergroup.*`, et `kafka.broker.*` par Jolokia), qui peuvent donc remplacer ces
  metricsets. Les data streams sont des séries temporelles (TSDS) sous-échantillonnées par ILM : 7 jours bruts, puis une
  valeur par heure jusqu'à 90 jours.

Ce qu'il n'est pas : une sonde de latence par message (il ne lit jamais les messages), un collecteur de métriques côté
clients (le JMX des producteurs et des consommateurs reste à Metricbeat, à côté des applications), ni un service haute
disponibilité (une instance par cluster, relancée par son superviseur ; un trou est signalé par le document de santé). Malgré son nom, ce n'est pas un Beat Elastic (il
n'utilise pas libbeat) et il est sans lien avec d'anciens projets nommés kafkabeat.

## Ce que vous obtenez

| Data stream | Contenu |
|---|---|
| `kafkabeat-lag` | Un document par paire topic + groupe toutes les 10 s : bornes du lag en secondes, pire partition, état, statut de production, taux, risque de perte, flags. |
| `kafkabeat-metrics` | Toutes les 60 s, des documents partition, consumer group et (en option) broker compatibles Metricbeat. |
| `kafkabeat-events` | Un document par rebalance d'un consumer group classique. |
| `kafkabeat-health` | Un document par cycle sur l'agent : durée du cycle, erreurs, échecs partiels. |

Et, dans [kibana/](kibana/), quatre dashboards (vue d'ensemble, zoom topic + groupe, tendances jusqu'à 90 jours, santé
des brokers) et quatre règles d'alerte (lag temporel, agent muet, production arrêtée, risque de perte), filtrés par
cluster, équipe, topic et groupe.

## Installation

Les fichiers de chaque version sont sur la page [Releases](https://github.com/gnocent/kafkabeat/releases) :

| Plateforme | Fichier | Usage |
|---|---|---|
| Linux (x86-64) | `kafkabeat-<version>-linux-amd64.tar.gz` | Production : binaire, unité systemd, configuration d'exemple. |
| macOS (Apple Silicon) | `kafkabeat-<version>-darwin-arm64.tar.gz` | Essai. |
| Windows (x86-64) | `kafkabeat-<version>-windows-amd64.zip` | Essai. |

```bash
sha256sum -c SHA256SUMS --ignore-missing     # vérifie l'archive téléchargée
```

Ou, avec [Go](https://go.dev/) 1.27 ou plus : `go install github.com/gnocent/kafkabeat/cmd/kafkabeat@latest`. Un
`Dockerfile` construit une image conteneur minimale, et [deploy/](deploy/) contient une unité systemd et un job HashiCorp
Nomad. Pas à pas, pour chacun : le **[guide d'installation](INSTALL_fr.md)**.

## Démarrage rapide

Sur un poste avec Docker, le banc de test du dépôt fournit un cluster Kafka, Elasticsearch et Kibana en quelques
minutes :

1. **Démarrer le banc** : `make bench-up` (Kafka 3.9 et 4.4, Elasticsearch et Kibana 8.19 et 9.5, ports locaux seulement).
2. **Lancer l'agent** dessus : `make check`, puis `./bin/kafkabeat -config kafkabeat.example.yml`.
3. **Importer les dashboards** dans Kibana (`http://localhost:5601`) depuis `kibana/kafkabeat-examples.ndjson`.
4. **Créer du trafic** avec le harnais du banc : un topic, un producteur, un consumer group, puis mettre le consumer en pause.
5. **Regarder** la paire passer de Caught up à Lagging, puis Stalled, sur le dashboard de vue d'ensemble.

Chaque commande, pas à pas : [guide d'installation, §2](INSTALL_fr.md#2-démarrage-rapide-sur-le-banc-de-test).

## Documentation

- **[INSTALL_fr.md](INSTALL_fr.md)** : démarrage rapide, puis installation en production (archive + systemd, conteneur +
  Nomad, depuis les sources), configuration et droits nécessaires.
- **[USAGE_fr.md](USAGE_fr.md)** : l'usage au quotidien, pas à pas : lire les dashboards, ce que signifie chaque état et
  chaque flag, règles d'alerte, examiner un groupe en retard, vérifier la santé de l'agent.
- **[DESIGN_fr.md](DESIGN_fr.md)** : comment il fonctionne et pourquoi : la méthode d'encadrement, la lecture des commits
  et des membres des groupes, le stockage, les choix techniques avec leurs avantages et leurs limites, et la validation.
- **[CHANGELOG_fr.md](CHANGELOG_fr.md)**, **[SECURITY_fr.md](SECURITY_fr.md)**, banc de test :
  [bench/README_fr.md](bench/README_fr.md).

## Sécurité

**Ce que l'agent touche**

- **Kafka** : métadonnées, offsets de fin et de début, consumer groups et leurs offsets commités ; en mode `hybrid`, il lit
  aussi le topic interne `__consumer_offsets`. Il ne lit jamais un topic applicatif et n'écrit jamais dans Kafka.
- **Elasticsearch** : écriture en bulk dans ses propres data streams, et installation de leurs templates et politiques
  ILM au démarrage (désactivable).
- **Jolokia** (option) : requêtes de MBeans en lecture seule sur chaque broker.
- **Les identifiants** ne sont lus que dans le fichier de configuration (à garder en `0600` ; l'unité systemd le remet
  par `LoadCredential`) et ne sont jamais journalisés. TLS 1.2 au minimum ; aucune option ne désactive la vérification
  des certificats.

**Ce qui a été vérifié, et ce qui ne l'a pas été**

- Tests unitaires avec le détecteur de concurrence, `go vet`, `govulncheck` en CI ; la recette sur le banc de test.
- Le banc de test n'a aucune sécurité activée : les privilèges Elasticsearch listés dans [INSTALL_fr.md](INSTALL_fr.md)
  ont été déduits des appels d'API, pas vérifiés contre un cluster sécurisé. SASL et mTLS sont implémentés mais pas
  exercés par le banc.
- Le bac à sable systemd n'est vérifié qu'en syntaxe : à valider sur l'hôte cible (`systemd-analyze security kafkabeat`).
- **Aucun audit externe indépendant.** Signaler une vulnérabilité : [SECURITY_fr.md](SECURITY_fr.md).

## Licence

Ce projet est distribué sous licence **[GNU Affero General Public License v3.0](LICENSE)** (AGPLv3) : vous êtes libre de
l'utiliser, l'étudier, le modifier et le redistribuer, à condition que le code source (y compris vos modifications) reste
disponible dans les mêmes termes — y compris si le logiciel est exposé via un réseau.

> **Note d'intention (non contraignante juridiquement)** : l'esprit de ce projet est de rester un outil communautaire et
> amélioré collectivement, pas un produit revendu tel quel. L'AGPLv3 n'interdit pas formellement un usage commercial, mais
> c'est l'usage que son auteur espère en voir fait.

## Avertissement / limitation de responsabilité

kafkabeat est publié **tel quel** ("as is"), sans garantie d'aucune sorte, explicite ou implicite — y compris, sans s'y
limiter, les garanties de qualité marchande, d'adéquation à un usage particulier et d'absence de contrefaçon (voir les
articles 15 à 17 de la [licence AGPLv3](LICENSE), qui font foi).

En particulier :

- Ce projet est développé et maintenu **sur le temps libre de son auteur**, sans engagement de disponibilité, de
  maintenance, de correctif de sécurité ou d'évolution future.
- L'auteur et les contributeurs **déclinent toute responsabilité** pour les conséquences directes ou indirectes de
  l'utilisation de cet outil — y compris, sans s'y limiter, une perte de données, une interruption de service, ou la
  charge imposée à un cluster Kafka ou Elasticsearch.
- L'agent lit des métadonnées Kafka et écrit dans Elasticsearch avec les droits qu'on lui donne : **essayez-le d'abord sur
  un cluster de test**. Son utilisation contre un cluster de production reste sous l'entière responsabilité de la personne
  qui l'utilise.
