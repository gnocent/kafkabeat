*(English version: [DESIGN.md](DESIGN.md))*

# kafkabeat — Conception

Comment kafkabeat mesure ce qu'il mesure, les choix techniques qui le sous-tendent avec leurs avantages et leurs limites,
et comment il est validé. Pour l'installer : [INSTALL_fr.md](INSTALL_fr.md) ; pour l'usage au quotidien :
[USAGE_fr.md](USAGE_fr.md).

## 1. Objectif et périmètre

Pour chaque paire topic + consumer group d'un parc Kafka très volumineux, une mesure du retard **en secondes** toutes les
10 s, publiée dans Elasticsearch, qui continue d'exister quand la consommation s'arrête. La cible est Kafka utilisé comme
tampon entre producteurs et consommateurs à fort débit, avec des milliers de topics et des dizaines de milliers de
partitions : comptent la vue d'ensemble et des signaux fiables en quasi temps réel (ce groupe suit-il, est-il bloqué,
perd-il des données ?), pas la latence par message.

Autour de cette mesure, kafkabeat remplace aussi les metricsets `partition`, `consumergroup` et `broker` de Metricbeat
pour les clusters (mêmes noms de documents), enregistre les rebalances, vérifie que chaque partition a un consumer et
estime le risque de perdre des messages par la rétention.

**Hors périmètre** : métriques JMX côté clients (les metricsets producer et consumer restent à Metricbeat, à côté des
applications) ; Kerberos ; share groups KIP-932 ; lecture des topics applicatifs (donc pas d'horodatage exact du message
à l'offset commité) ; émission sur changement seulement (un document par paire et par cycle reste la règle) ; haute
disponibilité actif/passif ; export OTLP ; découpage d'un cluster entre plusieurs agents ; taille des logs par topic
(DescribeLogDirs).

## 2. Mesurer le lag temporel

### 2.1 Encadrer plutôt qu'estimer

Le lag d'une paire est l'âge du premier message que le groupe n'a pas consommé : le message à son offset commité.
L'agent ne lit jamais les messages, il ne peut donc pas lire l'horodatage de ce message. À la place, il enregistre pour
chaque partition l'historique de son **offset de fin** (LEO) tel que vu à chaque cycle, sous forme d'entrées signifiant
« le LEO valait `L` de l'instant `first` à l'instant `last` ».

Pour un offset commité `c` avec `L_i ≤ c < L_{i+1}`, le message `c` a été ajouté au log après `last_i` et au plus tard à
`first_{i+1}`. D'où :

- **borne basse** = maintenant − `first_{i+1}` (le message existait déjà à cet instant) ;
- **borne haute** = maintenant − `last_i` (il n'existait pas encore avant).

Quand `c` égale l'offset de fin courant, la partition est à jour et son lag vaut 0. Quand `c` est plus ancien que la plus
ancienne entrée gardée, la borne haute est inconnue (`bracket_open`). Le lag d'une paire est celui de sa pire partition.

**Avantage** : le vrai lag est toujours entre les deux bornes ; rien n'est interpolé, aucun débit moyen n'est supposé. Un
consumer bloqué sur un topic calme montre une borne haute qui croît de 10 s à chaque cycle, comme il se doit ; une rafale
après un long silence n'est pas imputée au silence. Les alertes portent sur la borne haute.
**Limite** : les bornes sont aussi serrées que l'historique. Dans le palier fin, l'encadrement fait au plus un cycle de
large ; dans les paliers plus anciens et plus grossiers, il s'élargit (jusqu'à 10 min au-delà de 3 h).

### 2.2 Paliers d'historique et persistance

Paliers par défaut : une entrée toutes les 10 s sur 15 min, toutes les minutes sur 3 h, toutes les 10 min sur 7 jours.
Retirer des entrées ne fait qu'élargir les encadrements : le compactage est donc toujours sûr. L'historique et l'état par
paire sont sauvegardés sur disque de façon atomique (toutes les 5 min et à l'arrêt) et rechargés au démarrage : après un
redémarrage, les bornes sont aussitôt de nouveau disponibles, avec `resumed_after_gap` levé et aucun taux calculé sur
l'intervalle.

## 3. Lire les consumer groups

### 3.1 Offsets commités : flux plus polling

Deux façons de connaître les offsets commités de chaque groupe :

- **`hybrid`** (par défaut) : l'agent consomme le topic interne `__consumer_offsets`, qui reçoit chaque commit, et
  réconcilie par un polling OffsetFetch complet toutes les 2 min (et au démarrage). Un groupe dont les enregistrements
  utilisent une version de format inconnue repasse seul en polling ; tant que le flux est en erreur, tous les groupes sont
  interrogés à chaque cycle (sinon des commits figés passeraient pour `stalled`).
- **`polling`** : OffsetFetch pour chaque groupe à chaque cycle.

**Avantage** : sur un grand parc, interroger chaque groupe toutes les 10 s est une charge constante sur les
coordinateurs de groupe ; le flux coûte un consommateur et date en plus chaque commit (`last_commit_age_s`). **Limite** :
le format interne de `__consumer_offsets` n'est pas une API publique. Le parseur est généré depuis les schémas de messages
de Kafka, compte les enregistrements qu'il ne sait pas lire et se replie par groupe ; il est à tester avant chaque mise à
jour de Kafka. Vérifié sur Kafka 3.9 et 4.4.

### 3.2 Membres, couverture et rebalances

Pour les consumer groups classiques, les membres (abonnements, partitions assignées, génération) viennent des
enregistrements GroupMetadata du même flux, écrits par le coordinateur à chaque génération. DescribeGroups (toutes les
60 s) fournit la référence au démarrage et le repli quand le flux est indisponible.

- **Rebalances** : un document `kafkabeat-events` par génération, avec les membres entrés et sortis et la durée
  observée ; en repli, un document par changement de membres vu toutes les 60 s, sans génération.
- **Couverture** : les partitions attendues sont celles des topics auxquels les membres sont abonnés ; une partition sans
  membre assigné depuis 60 s, groupe `Stable`, fait passer la paire en `unassigned`. Une paire existe aussi pour un
  abonnement sans aucun commit (`no_commit`).
- **Groupes sans membre** : si leurs commits avancent (assignation manuelle : Spark, Flink, `assign()`), ils sont classés
  `manual` et ne lèvent rien ; si les commits sont figés, `none`.

**Limite** : les groupes KIP-848 (le protocole `consumer`, côté broker) ne sont suivis que par leur état : ni membres, ni
couverture, ni rebalances. Leur type est publié (`kafka.consumergroup.type`) pour qu'on puisse les repérer. La parité
complète est remise à plus tard, quand de tels groupes apparaîtront en production.

### 3.3 Métriques des brokers par Jolokia

Avec `jolokia.enabled`, l'agent interroge l'agent Jolokia de chaque broker trouvé dans les métadonnées, toutes les 60 s et
hors du cycle de 10 s, avec un délai borné, et publie les mêmes documents `kafka.broker.*` que le metricset `broker` de
Metricbeat. Un broker injoignable est signalé dans le document de santé ; les autres sont quand même publiés. Vérifié
avec Jolokia 1.7 et 2.6.

## 4. États, statut de production et flags

L'état d'une paire décrit la **consommation** et c'est celui de sa pire partition : `caught_up`, `lagging` (commit qui
avance), `stalled` (offset commité figé depuis 60 s avec du lag, que le consumer recommite le même offset ou non et que la
production continue ou non), `unassigned`, `no_commit`, `gone` (paire plus vue : émise quelques cycles, puis oubliée). La
**production** a son propre statut par topic : `active` ou `stopped` (aucun message depuis 60 s). C'est en gardant les
deux séparés qu'on distingue un problème de production d'un problème de consommation.

Les flags restent levés 6 cycles : reset d'offset, log tronqué, topic recréé, groupe recréé, reprise après un trou,
commit avant le début du log (messages supprimés par la rétention avant d'être lus), cycle incomplet.

**Risque de perte** : `retention_used` = (LEO − commit) / (LEO − log start), de 0 (à jour) à 1 (commit au début du
journal retenu). Le log start est lu toutes les 60 s : une perte vue est donc certaine, mais elle peut être vue jusqu'à
60 s en retard. Non calculé tant que le log start vaut 0 (topic compacté, ou rétention jamais atteinte).

Définitions exactes de chaque état, flag et champ : [USAGE_fr.md](USAGE_fr.md#3-états-et-flags).

## 5. Stockage dans Elasticsearch

- **Data streams** : `kafkabeat-lag` (10 s), `kafkabeat-metrics` (60 s), `kafkabeat-events`, `kafkabeat-health`. L'agent
  installe templates, mappings et politiques ILM au démarrage (`setup: false` laisse ce soin à l'exploitant) et ne modifie
  jamais une politique ILM existante : elle appartient à l'exploitant.
- **Séries temporelles (TSDS)** pour le lag et les métriques : les dimensions sont les identifiants de chaque document
  (cluster, topic, groupe, partition, broker). `@timestamp` est aligné sur la grille de 10 s ; un document refusé en 409
  (même série, même horodatage : redémarrage dans le même créneau, deux agents sur un cluster) est compté comme doublon,
  pas comme erreur.
- **Rétention** : bruts pendant 7 jours, puis sous-échantillonnés à une valeur par heure, supprimés à 90 jours ; santé et
  événements : 30 jours. Aucun champ n'est marqué métrique : le sous-échantillonnage garde donc la **dernière** valeur de
  chaque heure, seule forme que lit ES|QL 8.19. Au-delà de 7 jours, un pic de moins d'une heure peut manquer.
- **Taille** : en licence Basic, la source est stockée (pas de source synthétique) : les documents TSDS bruts ne sont pas
  plus petits que les classiques ; le gain vient de la rétention. Mesuré sur le banc (8.19) : sur toute la rétention,
  environ 0,25× (lag) et 0,40× (métriques) d'un index classique de 30 jours, en supposant un document sous-échantillonné de
  la taille d'un document brut (non mesuré).
- Le détail par partition d'une paire (`kafka.lag.partitions`) est gardé dans `_source` sans être indexé.

## 6. Choix techniques

| Choix | Avantage | Limite |
|---|---|---|
| Go, client Kafka [franz-go](https://github.com/twmb/franz-go), dépendances vendorisées et figées | Un seul binaire statique, aucun runtime ; maîtrise complète des versions du protocole Kafka (OffsetFetch, ListGroups, `__consumer_offsets`). | La construction exige Go 1.27. |
| Un agent par cluster, état sur disque local | Aucune coordination, aucun stockage externe ; les redémarrages reprennent depuis le disque. | Pas de haute disponibilité : un agent arrêté laisse un trou, signalé par le document de santé et la règle « silent component ». |
| Métadonnées Kafka seulement (ACL : Describe, plus Read sur `__consumer_offsets`) | Sans risque à accorder sur n'importe quel cluster ; aucune charge sur les topics applicatifs. | Pas d'horodatage exact d'un message : d'où l'encadrement. |
| Un document par paire et par cycle | Une paire muette ou bloquée reste visible ; les requêtes n'ont pas à combler de trous. | Volume proportionnel à paires × cycles, atténué par TSDS et sous-échantillonnage. |
| Dashboards en ES|QL (tuiles, totaux) et Lens (séries) | Comptes exacts (maximum par topic, puis somme) : un topic lu par deux groupes n'est compté qu'une fois. | La syntaxe ES|QL diffère un peu entre 8.19 et 9.x ; les deux sont vérifiées par la recette. |
| Sink Elasticsearch derrière une interface, configuration YAML stricte | Une autre sortie peut s'ajouter ; une faute de frappe dans la configuration est une erreur, pas un défaut silencieux. | — |

## 7. Validation

La recette (`make recette`, dans [recette/](recette/)) rejoue les critères ci-dessous sur le banc de test Docker :
Kafka 3.9 et 4.4 (KRaft, avec Jolokia), Elasticsearch et Kibana 8.19 et 9.5, Metricbeat en parallèle, et un harnais qui
produit à débit irrégulier, pilote des consumers (pause, commit, reset, assignateurs sur mesure) et journalise l'heure de
production de chaque message. Le test `TestC<n>` vérifie le critère `n`.

1. Le vrai lag du dernier offset commité est compris entre la borne basse et la borne haute sur 100 % des cycles ;
   largeur ≤ 10 s dans le palier fin.
2. Consumer bloqué, puis production arrêtée : la borne haute croît de 10 s par cycle, un document par cycle. Silence,
   puis rafale consommée aussitôt : borne haute ≤ 20 s (le silence n'est pas compté comme du lag).
3. Reset d'offset, topic recréé, groupe supprimé, Kafka injoignable un cycle : respectivement `offset_reset_suspected`,
   `topic_recreated`, `gone` (quelques cycles puis purge), état conservé avec l'erreur dans le document de santé.
4. Redémarrage après un arrêt : bornes reprises depuis le disque, `resumed_after_gap` levé, aucun taux sur l'intervalle.
5. Clé de version inconnue injectée dans `__consumer_offsets` : le groupe repasse en polling, le compteur du parseur
   s'incrémente, valeurs égales au polling à un cycle près ; sur 3.9 et 4.x.
6. Offsets des documents 60 s égaux à ceux de Metricbeat à un cycle près. À 30 000 partitions : cycle < 10 s, mémoire
   < 2 Go (mesuré par `make load`, sur un simulateur).
7. Scénario du harnais (paires bloquées, production arrêtée, topic sans groupe, topic à deux groupes) : chaque tuile de
   la vue d'ensemble égale le compte attendu ; débit total égal à la somme par topic à 5 % près.
8. Les dashboards s'importent et se rendent sans erreur sur Kibana 8.19 et 9.x.
9. Vue d'ensemble rendue en < 5 s sur 15 minutes simulées à 30 000 partitions.
10. N rebalances provoqués par le harnais (entrée, sortie, arrêt brutal) sur 3.9 et 4.4 : N documents, générations
    consécutives, membres corrects, horodatage à un cycle près ; flux coupé : repli signalé, aucun rebalance inventé.
11. Jolokia sur les brokers du banc : `kafka.broker.*` égaux à ceux de Metricbeat à une période près, même mapping champ à
    champ ; un Jolokia coupé : erreur dans le document de santé, autres brokers publiés, cycle de 10 s tenu.
12. Groupe à deux membres sur un topic de 4 partitions, un membre figé par un assignateur qui omet une partition :
    `partitions_unassigned` = 1 et paire KO au-delà de 60 s, pas pendant un rebalance ; groupe abonné sans commit : paire
    `no_commit` ; consumer en `assign()` qui commite : aucune alerte.
13. Agent arrêté 3 min : « silent component » se déclenche pour ce cluster seul ; Kafka coupé 3 min avec l'agent vivant :
    idem ; production arrêtée au-delà du seuil (abaissé) : la règle se déclenche pour ce topic seul.
14. Topic à rétention courte, consumer arrêté : `retention_used` monte jusqu'à 1, puis `commit_before_log_start` avec le
    bon nombre d'offsets perdus et la paire KO ; topic compacté en retard : aucun risque signalé.
15. Volume sur toute la rétention inférieur à un index classique de 30 jours (mesuré, puis extrapolé) ;
    sous-échantillonnage forcé : le dashboard des tendances se rend au-delà de 7 jours sur 8.19 et 9.x ; cycle réécrit au
    même `@timestamp` : aucune erreur dans le document de santé.
16. Sur 4.4, un groupe au protocole `consumer` à côté de groupes classiques : lui seul est compté dans la tuile KIP-848 et
    listé ; sur 3.9, la tuile affiche 0.

**Là où la recette est plus faible que le texte**, pour tenir en une session : critère 2 vérifié sur 40 s au lieu de
30 min, son silence sur 60 s au lieu de 10 min ; critère 4 avec un arrêt de 15 s au lieu de 5 min, Elasticsearch toujours
joignable ; critère 1 sur 3 min par défaut (`KB_LONG=1` pour le scénario de 2 heures) ; le critère 5 se saute lui-même si
le broker refuse l'injection ; la moitié à 30 000 partitions du critère 6 est mesurée par `make load`, hors recette ; le
critère 9 vérifie le temps de rafraîchissement, le chargement initial n'est que journalisé. Les vérifications humaines
(lisibilité des dashboards) ne sont pas automatisées.

**Non vérifié** : un cluster Elasticsearch sécurisé (le banc n'a pas de sécurité ; les privilèges d'INSTALL_fr.md sont
déduits des appels d'API), SASL et mTLS contre un vrai cluster, un cluster à plusieurs brokers (les clusters du banc ont
un broker chacun ; les échecs partiels de ListGroups sont couverts par des tests unitaires), la taille des documents
sous-échantillonnés, le bac à sable systemd sur un vrai hôte.

## 8. Limites connues et plus tard

- **Risques** : les formats `__consumer_offsets` et GroupMetadata des futures versions de Kafka (repli sur le polling,
  tester avant la mise à jour) ; des share groups listés par ListGroups sur 4.x puis interrogés par OffsetFetch
  produiraient probablement des erreurs par groupe et des cycles incomplets (non vérifié : aucun share group sur le banc) ;
  pendant un échec partiel de ListGroups, les paires des groupes non listés restent inconnues (ni `gone` ni purgées) et le
  cycle est signalé incomplet.
- **Limite des règles d'alerte** : une règle groupée par cluster ne voit que les clusters qu'elle a vus vivants ; un
  cluster retiré pour de bon peut rester en alerte jusqu'à ce que la règle soit désactivée puis réactivée (non vérifié).
- **TSDS** : après une longue panne d'Elasticsearch, les documents plus anciens que la fenêtre temporelle du TSDS sont
  rejetés.
- **Plus tard** : parité complète des groupes KIP-848 (membres, assignations, rebalances par époque), quand le premier
  groupe de ce type sera vu en production ; champs de débit et de lag marqués gauge (minimum, maximum et moyenne exacts
  par heure au-delà de 7 jours) quand tous les clusters seront en Elasticsearch 9.x.
