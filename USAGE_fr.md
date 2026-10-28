*(English version: [USAGE.md](USAGE.md))*

# kafkabeat au quotidien

Ce guide commence une fois l'agent lancé et en train d'écrire dans Elasticsearch ([INSTALL_fr.md](INSTALL_fr.md)). Il
parcourt les dashboards et les règles d'alerte d'exemple, puis explique chaque état, chaque flag et chaque champ, et
comment vérifier l'agent lui-même. Comment les valeurs sont calculées : [DESIGN_fr.md](DESIGN_fr.md).

## 1. Préparer Kibana

1. **Importer les exemples** : Stack Management → Saved Objects → Import, choisir `kibana/kafkabeat-examples.ndjson`,
   autoriser l'écrasement. Ou :

   ```bash
   curl -X POST -H 'kbn-xsrf: true' "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
        --form file=@kibana/kafkabeat-examples.ndjson
   ```

   L'import crée les data views, quatre dashboards et quatre règles d'alerte. Prérequis : Kibana 8.19 ou 9.x (vérifié
   sur 8.19.23 et 9.5.5, licence Basic) et des data streams créés par l'agent (il les crée à son premier démarrage).
2. **Vérifier les données** : ouvrir le dashboard **kafkabeat: overview** (vue d'ensemble). La tuile « Last cycle (s) »
   (âge du dernier cycle) doit rester sous 20 ; si elle est vide, voir le [§7](#7-vérifier-lagent).
3. **Nommer les équipes** (option) : le filtre « Team » (équipe) de chaque dashboard lit `kafka.topic.owner`, que l'agent
   renseigne d'après les règles `output.owners` de sa configuration (le premier motif glob qui correspond l'emporte) :

   ```yaml
   output:
     owners:
       - {topic: "payments-*", owner: payments}
       - {topic: "*-logs",     owner: platform}
   ```

4. **Activer les règles d'alerte** voulues ([§6](#6-règles-dalerte)).

Chaque dashboard se filtre par cluster, équipe, topic et groupe (santé des brokers : par cluster seulement).

## 2. Les dashboards

### 2.1 Vue d'ensemble : l'écran de supervision

15 minutes, rafraîchi toutes les 10 s. **À lire de haut en bas :**

1. Les **tuiles** décrivent *maintenant* (les derniers cycles), quelle que soit la plage de temps choisie :
   - topics suivis, consommés, et sans aucun groupe ;
   - **Consumption OK / not OK** (consommation OK / KO) par topic. KO = au moins une paire du topic `stalled`, `unassigned`,
     `no_commit`, en retard de plus de 300 s (borne haute, ou basse quand la haute est inconnue), ou avec des offsets
     perdus ;
   - **Production active / stopped** (production active / arrêtée) par topic (arrêtée = aucun message depuis 60 s ou
     plus) ;
   - groupes en rebalance, partitions non assignées, groupes sans membre (commits figés ; l'assignation manuelle n'est
     pas comptée), paires à risque de perte (`retention_used` ≥ 0,8, ou offsets perdus), groupes KIP-848 ;
   - débits totaux produit et consommé, pire retard, âge du dernier cycle de l'agent.
2. **État par paire** : quelles paires ne sont pas à jour, et depuis quand.
3. **Débit par équipe** et **paires par état** sur la plage de temps : quelque chose a-t-il changé, et quand ?
4. **10 plus gros topics** et **10 pires paires** : où regarder d'abord.
5. **Santé de kafkabeat** : durée et erreurs des cycles (voir le [§7](#7-vérifier-lagent)).

Tuiles et totaux sont exacts (ES|QL : maximum par topic, puis comptage ou somme) : le débit d'un topic lu par deux groupes
n'est compté qu'une fois. Les paires disparues (`gone`) ne comptent que dans « topics without group » (topics sans
groupe).

### 2.2 Zoom topic + groupe : le diagnostic d'astreinte

1 heure. Choisir un topic et un groupe dans les filtres, puis :

1. **Bornes du lag** : le vrai lag est entre les deux courbes. Une borne haute qui monte régulièrement avec un débit
   consommé plat, c'est un consumer bloqué.
2. **Débits produit et consommé** : le groupe est-il plus lent que la production, ou la production s'est-elle arrêtée ?
3. **Lag par partition** et **âge des commits** : une seule partition en retard désigne un membre ou une clé chaude ;
   toutes les partitions en retard désignent tout le groupe.
4. **État du groupe**, **consumers avec leurs partitions et leur retard**, **partitions sans membre assigné**.
5. **Rebalances par groupe** et **journal des membres** (entrés, sortis, génération) : une tempête de rebalances explique
   un lag qui va et vient.
6. **Détail par partition** avec le consumer assigné et `retention_used` (part du journal retenu pas encore lue).

### 2.3 Tendances (90 jours au plus) : la capacité

7 jours par défaut : débit et pire retard par équipe, topics classés par volume produit. Au-delà de 7 jours, les données
sont sous-échantillonnées à une valeur par heure et par série (la dernière) : un pic de moins d'une heure peut ne pas
apparaître.

### 2.4 Santé des brokers

1 heure, exige `jolokia.enabled` dans l'agent : brokers lus et injoignables par Jolokia, partitions sous-répliquées,
octets par seconde, requêtes en échec, élections de leader, et un tableau par broker.

## 3. États et flags

### 3.1 État de la consommation (`kafka.lag.state`)

Une valeur par paire : celle de sa pire partition. Les dashboards l'affichent avec le libellé de la deuxième colonne.

| Valeur | Libellé | Signification |
|---|---|---|
| `caught_up` | Caught up | Toutes les partitions sont consommées jusqu'au bout. |
| `lagging` | Lagging | Du lag sur au moins une partition, mais l'offset commité avance. |
| `unassigned` | Unassigned | Une partition d'un topic auquel le groupe est abonné n'a aucun membre assigné depuis `stalled_after` (60 s), groupe `Stable` (jamais pendant un rebalance). Prime sur `stalled`, dont elle est souvent la cause. |
| `stalled` | Stalled | Sur au moins une partition, du lag et un offset commité figé depuis `stalled_after` (60 s), que le consumer recommite cet offset ou non et que la production continue ou non. |
| `no_commit` | No commit | Pas de lag ailleurs, mais au moins une partition sans offset commité. |
| `gone` | Gone | La paire n'est plus vue (topic ou groupe supprimé, offsets expirés) : émise `gone_cycles` fois, puis oubliée. Un groupe que son coordinateur ne peut pas lister (chargement après la reprise d'un broker, broker injoignable) reste inconnu le temps de la panne, sans `gone`. |

### 3.2 Statut de la production (`kafka.lag.production`)

Le même pour toutes les paires d'un topic.

| Valeur | Libellé | Signification |
|---|---|---|
| `active` | Active | Un message est arrivé sur le topic depuis moins de `stalled_after` (60 s). |
| `stopped` | Stopped | Aucun message sur le topic depuis au moins `stalled_after`. |
| absent | | Au démarrage, tant qu'aucune avance n'a été vue et que `stalled_after` n'est pas écoulé. |

### 3.3 Flags (`kafka.lag.flags.*`)

Chaque flag reste levé `flag_hold_cycles` cycles (6 par défaut).

| Flag | Signification | Que faire |
|---|---|---|
| `offset_reset_suspected` | L'offset commité a reculé sur une partition. | Attendu après un reset volontaire ; sinon, interroger le propriétaire du groupe. |
| `log_truncated_suspected` | L'offset de fin a reculé (log tronqué) ; l'historique de la partition repart de zéro. | Vérifier le broker (élection de leader non propre, enregistrements supprimés). |
| `topic_recreated` | Le topic a changé d'identifiant (supprimé puis recréé). Kafka efface alors les offsets du groupe : le flag accompagne aussi le document `gone` de la paire, puis son retour. | Vérifier que le groupe est reparti là où il le fallait. |
| `group_recreated` | La paire réapparaît après être passée en `gone`. | En général pour information. |
| `resumed_after_gap` | L'agent a redémarré après plus de deux cycles d'arrêt : pas de taux sur l'intervalle. | Chercher pourquoi l'agent s'est arrêté. |
| `commit_before_log_start` | La rétention a dépassé le commit du groupe : des messages ont été supprimés avant d'être lus (`kafka.lag.offsets_lost`). | Perte de données pour ce groupe : prévenir son propriétaire. |
| `cycle_incomplete` | Une partie de la collecte a échoué pendant ce cycle (détail dans le document de santé). | Voir le [§7](#7-vérifier-lagent). |

### 3.4 Principaux champs d'un document de paire (`kafkabeat-lag`)

| Champ | Signification |
|---|---|
| `kafka.lag.seconds_lower`, `seconds_upper` | Bornes du lag en secondes, pire partition. `seconds_upper` absent quand l'offset commité est plus ancien que l'historique (`kafka.lag.method: bracket_open`). |
| `kafka.lag.offsets_sum` | Lag en messages, sommé sur les partitions. |
| `kafka.lag.produce_rate`, `consume_rate`, `consume_rate_smoothed` | Messages par seconde sur le dernier cycle ; lissés sur `smoothing_window`. |
| `kafka.lag.last_commit_age_max_s` | Âge du dernier commit d'une partition en retard. En mode `hybrid`, le flux date chaque commit : un consumer bloqué qui recommite le même offset reste `stalled` avec un âge faible (vivant, mais n'avance pas). En mode `polling`, âge du dernier changement d'offset. |
| `kafka.lag.last_produce_age_s` | Secondes depuis le dernier message vu sur le topic (un minorant tant que l'agent n'a pas observé assez longtemps). |
| `kafka.lag.retention_used` | (LEO − commit) / (LEO − log start), pire partition, de 0 (à jour) à 1 (commit au début du journal retenu). Absent tant que le log start vaut 0 (topic compacté, rétention jamais atteinte). |
| `kafka.lag.offsets_lost` | Messages supprimés avant d'être lus (log start − commit, sommé). |
| `kafka.lag.partitions_unassigned`, `partitions_unassigned_ids`, `unassigned_age_max_s` | Partitions des topics abonnés sans membre (groupe `Stable` seulement). |
| `kafka.consumergroup.members`, `assignment` | `members` (au moins un membre), `manual` (aucun membre, commits qui avancent : `assign()`, Spark, Flink ; sans alerte), `none` (aucun membre, commits figés). Absent pendant les deux cycles qui suivent le départ du dernier membre. |
| `kafka.consumergroup.type` | `classic`, ou `consumer` pour les groupes KIP-848 (suivis par leur état seul). |
| `kafka.topic.owner` | Équipe, d'après `output.owners`. |
| `kafka.lag.partitions` | Détail des partitions en retard, dans `_source` seulement (non interrogeable). |

Les documents de rebalance (`kafkabeat-events`) portent `kafka.consumergroup.generation`,
`kafka.rebalance.previous_generation`, `kafka.rebalance.joined`, `left`, les topics abonnés et `duration_s` (un minorant
observé par les cycles de 10 s). En repli (méthode `describe`), un document par changement de membres vu toutes les 60 s,
sans génération. Les rebalances survenus pendant un arrêt de l'agent ne sont pas rapportés.

## 4. Examiner une paire, pas à pas

**Une paire est `lagging`, sa borne haute ne cesse de monter.**
1. Zoomer sur la paire. Comparer débits produit et consommé : si la consommation est sous la production, le groupe
   manque de capacité (plus de membres, jusqu'au nombre de partitions) ou un membre est lent.
2. Regarder le lag par partition : une seule partition en retard désigne un membre (tableau des consumers) ou une clé
   chaude.
3. Vérifier les rebalances : des générations fréquentes signifient que des membres partent sans cesse (timeouts,
   plantages).

**Une paire est `stalled`.**
1. Vérifier `kafka.lag.production` : si `stopped`, le retard ne grossit pas, mais personne ne le consomme.
2. Regarder `last_commit_age_max_s` : faible en mode `hybrid`, le consumer est vivant et recommite le même offset (il est
   bloqué sur un message, souvent une erreur de traitement) ; élevé, le consumer a cessé de commiter.
3. Vérifier le tableau des consumers : aucun membre signifie que l'application est arrêtée (`assignment: none`).

**Une paire est `unassigned`.** Une partition n'a aucun membre alors que le groupe est stable : en général un assignateur
sur mesure, ou plus d'abonnements que les membres ne peuvent en prendre. Le zoom liste les partitions ; le propriétaire du
groupe doit corriger l'assignation.

**Une paire est `no_commit`.** Le groupe est abonné au topic mais n'a jamais commité sur certaines partitions : un groupe
neuf encore en train de démarrer, ou une application qui ne commite pas (auto-commit désactivé).

**Une paire est à risque de perte.** Un `retention_used` proche de 1 signifie que la rétention va bientôt supprimer des
messages que le groupe n'a pas lus. Accélérer le consumer, ou allonger la rétention du topic. `commit_before_log_start`
signifie que c'est déjà arrivé : `offsets_lost` messages n'ont jamais été lus.

**La production est `stopped`.** Le problème est en amont : vérifier l'application qui produit, pas le consumer.

## 5. Interroger les données directement

Les data views `kafkabeat-lag`, `kafkabeat-metrics`, `kafkabeat-events` et `kafkabeat-health` fonctionnent dans Discover.
Quelques requêtes ES|QL :

```esql
// Pires paires en ce moment
FROM kafkabeat-lag
| WHERE @timestamp > NOW() - 1 minute
| STATS upper = MAX(kafka.lag.seconds_upper) BY kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| SORT upper DESC | LIMIT 20

// Paires pas à jour, par état
FROM kafkabeat-lag
| WHERE @timestamp > NOW() - 1 minute AND kafka.lag.state != "caught_up"
| STATS pairs = COUNT_DISTINCT(kafka.consumergroup.id) BY kafka.lag.state

// Rebalances d'un groupe sur le dernier jour
FROM kafkabeat-events
| WHERE @timestamp > NOW() - 1 day AND kafka.consumergroup.id == "my-group"
| KEEP @timestamp, kafka.consumergroup.generation, kafka.rebalance.joined.client_id, kafka.rebalance.left.client_id
| SORT @timestamp DESC
```

## 6. Règles d'alerte

Les quatre sont créées **désactivées et sans action** :

| Règle | Type | Déclenchement, par groupe de |
|---|---|---|
| kafkabeat: time lag > 5 min (upper bound) | Elasticsearch query | Borne haute > 300 s sur la dernière minute, par cluster, topic et groupe. |
| kafkabeat: silent component (no successful cycle for 2 min, per cluster) | Custom threshold | Aucun cycle réussi depuis 2 min, par cluster : agent arrêté (option « alert on missing group ») ou Kafka injoignable sur tous les cycles (`kafkabeat.cycle_failed`). |
| kafkabeat: production stopped for 15 min (consumed topic) | Elasticsearch query | `last_produce_age_s` > 15 min sur un topic consommé, par cluster et topic. |
| kafkabeat: loss risk (80% of the retained log unread, or offsets lost) | Elasticsearch query | `retention_used` ≥ 0,8 ou `commit_before_log_start`, par cluster, topic et groupe. |

**Pour les activer :**
1. Kibana a besoin d'une clé permanente `xpack.encryptedSavedObjects.encryptionKey` dans `kibana.yml`.
2. Stack Management → Rules : ouvrir une règle, ajouter un connecteur (e-mail, Slack, webhook…), ajuster le seuil,
   l'activer.
3. Vérifier sa dernière exécution dans la liste des règles. Constaté sur Kibana 8.19 : une règle activée une fois puis
   réimportée se dit active à la première activation sans jamais s'exécuter ; la désactiver puis la réactiver.

La règle « silent component » (composant muet) ne voit disparaître que les clusters qu'elle a vus vivants : l'activer
quand les agents tournent. Un cluster retiré pour de bon peut rester en alerte jusqu'à ce que la règle soit désactivée
puis réactivée (non vérifié).

Les seuils (300 s, 0,8, 15 min, et les définitions des tuiles) sont des constantes de `kibana/examples.py`. Pour les
changer, modifier les constantes, puis régénérer l'export contre un Kibana 8.19 (`make kibana` ; variables `KIBANA`,
`KIBANA_USER`/`KIBANA_PASSWORD` ou `KIBANA_API_KEY` ; `--export-only` se contente d'exporter).

## 7. Vérifier l'agent

**Document de santé** (`kafkabeat-health`, un par cycle) :

| Champ | À surveiller |
|---|---|
| `kafkabeat.cycle_duration_ms` | Doit rester bien sous l'intervalle (10 000 ms). |
| `kafkabeat.cycle_failed` | `true` : le cycle a été abandonné (Kafka injoignable) ; seul ce document est parti. |
| `kafkabeat.cycle_incomplete`, `errors`, `partial_errors`, `group_errors` | Une partie de la collecte a échoué ; `errors` dit laquelle. |
| `kafkabeat.commits_mode`, `stream.ready`, `stream.errors`, `stream.last_error`, `stream.groups_fallback` | État du flux `__consumer_offsets` ; groupes repassés en polling. |
| `kafkabeat.stream.unknown_key_versions`, `unknown_value_versions` | Versions d'enregistrement que le parseur ne connaît pas (après une mise à jour de Kafka). |
| `kafkabeat.sink.failed`, `sink.last_error`, `sink.duplicates` | Écritures refusées par Elasticsearch ; les doublons (409) sont attendus après un redémarrage dans le même créneau. |
| `kafkabeat.jolokia.brokers_failed` | Brokers dont le Jolokia n'a pas répondu. |
| `kafkabeat.last_snapshot` | Dernier état sauvegardé sur disque. |

**Journaux** : l'agent écrit sur la sortie d'erreur (avec systemd : `journalctl -u kafkabeat -f`). Au démarrage, il
journalise une ligne (version, cluster, brokers, cycle, mode des commits) et l'état qu'il a restauré ; ensuite, il ne
journalise que les problèmes.

**Problèmes courants**

| Symptôme | Cause probable |
|---|---|
| Aucun document | L'agent ne joint pas Elasticsearch : vérifier `output.elasticsearch.urls`, les identifiants et les journaux. |
| Documents de santé avec `cycle_failed: true` | Kafka injoignable : brokers, TLS, SASL, ACL. |
| `errors` mentionne une autorisation | ACL manquante : Describe sur le cluster, les topics et les groupes ; Read sur `__consumer_offsets` en mode `hybrid` ([INSTALL_fr.md](INSTALL_fr.md#7-droits-nécessaires)). |
| Paires `gone` puis de retour après la reprise d'un broker | Normal pendant quelques cycles seulement ; sinon, chercher des échecs de ListGroups dans `errors`. |
| `resumed_after_gap` à chaque démarrage | Le répertoire d'état n'est pas persisté (conteneur sans volume). |
| Deux agents sur un cluster | `sink.duplicates` ne cesse de monter : une seule instance par cluster. |
