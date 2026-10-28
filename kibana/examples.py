#!/usr/bin/env python3
"""Crée dans Kibana les exemples kafkabeat (data views, visualisations Lens et
ES|QL, dashboards, règle d'alerte) puis les exporte dans kafkabeat-examples.ndjson.

Les tuiles et les totaux sont des requêtes ES|QL en deux temps (maximum par
topic, puis comptage ou somme) : comptes exacts, sans double compte des topics
lus par deux groupes. Les séries restent en Lens quand une agrégation simple
suffit. Les seuils sont des constantes : les changer puis régénérer.

Usage : KIBANA=http://localhost:5601 python3 kibana/examples.py [--export-only]
Authentification : KIBANA_USER / KIBANA_PASSWORD ou KIBANA_API_KEY si besoin.
"""
import base64
import json
import os
import sys
import urllib.error
import urllib.request

KIBANA = os.environ.get("KIBANA", "http://localhost:5601").rstrip("/")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kafkabeat-examples.ndjson")

LAG_MAX_S = 300                 # consommation OK : retard ≤ 300 s (seuil de la règle d'alerte)
PRODUCTION_STOPPED_S = 60       # production OK : un message reçu depuis moins de 60 s
NOW_WINDOW = "30 seconds"       # tuiles de l'instant : pire état des derniers cycles (le dernier s'indexe par lots)
INVENTORY_WINDOW = "2 minutes"  # inventaire des topics : au moins un cycle des documents 60 s
RISK_RETENTION = 0.8            # paire à risque de perte : 80 % du journal retenu non lu (1 = offsets perdus)
PRODUCTION_ALERT_S = 900        # règle « production arrêtée » : aucun message depuis 15 min sur un topic consommé
MUTE_MINUTES = 2                # règle « composant muet » : aucun cycle réussi d'un cluster depuis 2 min
# règles d'exemple : identifiants fixes, l'import remplace la version précédente
RULE_IDS = {"lag": "3b0f1c9e-5a2d-4e7b-8c61-0d4f2a9b7e15", "mute": "8d2e4a61-0b7c-4f3e-9a15-6c8b2d0e4f71",
            "production": "5f1a9c3e-7d2b-4e80-b6a4-1e9c7f3d2a58", "loss": "c47e2b90-3a6d-4f1c-8e52-9b0d6a4c1f37"}

# secondes, offsets et millisecondes affichés sans décimale
INTEGER = {"id": "number", "params": {"decimals": 0}}
# débits : une décimale, pour qu'un topic lent ne s'affiche pas à 0
RATE = {"id": "number", "params": {"decimals": 1}}
# libellés anglais, posés en format d'affichage dans la data view ; les documents gardent les
# identifiants (filtres, requêtes, règles). La colonne du tableau dit le côté : consommation ou production.
STATE_LABELS = {"caught_up": "Caught up", "lagging": "Lagging", "stalled": "Stalled", "unassigned": "Unassigned",
                "no_commit": "No commit", "gone": "Gone"}
PRODUCTION_LABELS = {"active": "Active", "stopped": "Stopped"}
ASSIGNMENT_LABELS = {"members": "Members", "manual": "Manual", "none": "None"}
STATE_COLORS = {"caught_up": "#54B399", "lagging": "#D6BF57", "stalled": "#E7664C", "unassigned": "#D36086",
                "no_commit": "#9170B8", "gone": "#A5A5A5"}
GREEN, RED = "#54B399", "#E7664C"

# Filtres de chaque dashboard ; la data view couvre lag et metrics pour lister aussi les topics sans groupe.
CONTROLS = [("cluster", "kafka.cluster.name", "Cluster"), ("owner", "kafka.topic.owner", "Team"),
            ("topic", "kafka.topic.name", "Topic"), ("group", "kafka.consumergroup.id", "Group")]


def call(method, path, body=None, raw=False):
    data = None if body is None else (body if raw else json.dumps(body).encode())
    req = urllib.request.Request(KIBANA + path, data=data, method=method)
    req.add_header("kbn-xsrf", "true")
    req.add_header("Content-Type", "application/json")
    if os.environ.get("KIBANA_API_KEY"):
        req.add_header("Authorization", "ApiKey " + os.environ["KIBANA_API_KEY"])
    elif os.environ.get("KIBANA_USER"):
        cred = base64.b64encode(f"{os.environ['KIBANA_USER']}:{os.environ.get('KIBANA_PASSWORD', '')}".encode()).decode()
        req.add_header("Authorization", "Basic " + cred)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        sys.exit(f"{method} {KIBANA}{path}: {e}")


def text(body):
    """Début d'un corps de réponse, lisible dans un message d'erreur."""
    return body[:300].decode(errors="replace")


def upsert(type_, id_, attributes, references):
    call("DELETE", f"/api/saved_objects/{type_}/{id_}?force=true")
    status, body = call("POST", f"/api/saved_objects/{type_}/{id_}", {"attributes": attributes, "references": references})
    if status >= 300:
        sys.exit(f"{type_}/{id_}: HTTP {status}: {text(body)}")
    print(f"{type_} {id_} created")


def lookup(labels):
    return {"id": "static_lookup", "params": {"lookupEntries": [{"key": k, "value": v} for k, v in labels.items()]}}


def data_view(id_, title, formats=None):
    call("DELETE", f"/api/data_views/data_view/{id_}")
    spec = {"id": id_, "title": title, "name": id_, "timeFieldName": "@timestamp"}
    if formats:
        spec["fieldFormats"] = formats
    status, body = call("POST", "/api/data_views/data_view", {"data_view": spec})
    if status >= 300:
        sys.exit(f"data view {id_}: HTTP {status}: {text(body)}")
    print(f"data view {id_} created")


# --- Lens classique (formBased) ---

def date_col():
    return {"label": "@timestamp", "dataType": "date", "operationType": "date_histogram", "sourceField": "@timestamp",
            "isBucketed": True, "scale": "interval", "params": {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}}


def pair_col(order_by, size=10):
    return {"label": "Topic + group", "customLabel": True, "dataType": "string", "operationType": "terms", "sourceField": "kafka.topic.name",
            "isBucketed": True, "scale": "ordinal",
            "params": {"size": size, "orderBy": {"type": "column", "columnId": order_by}, "orderDirection": "desc",
                       "otherBucket": False, "missingBucket": False, "parentFormat": {"id": "multi_terms"},
                       "secondaryFields": ["kafka.consumergroup.id"]}}


def terms_col(field, label, order_by, size=10):
    return {"label": label, "customLabel": True, "dataType": "string", "operationType": "terms", "sourceField": field,
            "isBucketed": True, "scale": "ordinal",
            "params": {"size": size, "orderBy": {"type": "column", "columnId": order_by}, "orderDirection": "desc",
                       "otherBucket": False, "missingBucket": False}}


def metric_col(op, field, label, fmt=INTEGER):
    return {"label": label, "customLabel": True, "dataType": "number", "operationType": op, "sourceField": field, "isBucketed": False,
            "scale": "ratio", "params": {"emptyAsNull": True, "format": fmt}}


def max_col(field, label):
    return metric_col("max", field, label)


def last_col(field, label, dtype="number", fmt=INTEGER):
    params = {"sortField": "@timestamp", "showArrayValues": False}
    if dtype == "number":
        params["format"] = fmt
    return {"label": label, "customLabel": True, "dataType": dtype, "operationType": "last_value", "sourceField": field,
            "isBucketed": False, "scale": "ratio" if dtype == "number" else "ordinal", "params": params}


def lens(id_, title, vis_type, visualization, columns, order, data_view_id, query=""):
    attributes = {
        "title": title,
        "description": "",
        "visualizationType": vis_type,
        "state": {
            "visualization": visualization,
            "query": {"query": query, "language": "kuery"},
            "filters": [],
            "datasourceStates": {"formBased": {"layers": {"layer1": {"columns": columns, "columnOrder": order, "incompleteColumns": {}, "sampling": 1}}}},
            "internalReferences": [],
            "adHocDataViews": {},
        },
    }
    references = [{"type": "index-pattern", "id": data_view_id, "name": "indexpattern-datasource-layer-layer1"}]
    upsert("lens", id_, attributes, references)


def xy(accessors, x, split=None, series="line", right=None):
    layer = {"layerId": "layer1", "accessors": accessors, "position": "top", "seriesType": series, "showGridlines": False,
             "layerType": "data", "xAccessor": x}
    if split:
        layer["splitAccessor"] = split
    if right:
        layer["yConfig"] = [{"forAccessor": right, "axisMode": "right"}]
    return {"legend": {"isVisible": True, "position": "right"}, "valueLabels": "hide", "fittingFunction": "Linear",
            "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
            "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "preferredSeriesType": series, "layers": [layer]}


# --- Lens ES|QL (textBased) : le nom de colonne sert de libellé ---

def esql(id_, title, query, vis_type, visualization, columns):
    index = query.split("|", 1)[0].replace("FROM", "").replace(" ", "").strip()
    dv = f"esql-{id_}"
    attributes = {
        "title": title,
        "description": "",
        "visualizationType": vis_type,
        "state": {
            "visualization": visualization,
            "query": {"esql": query},
            "filters": [],
            "datasourceStates": {"textBased": {"layers": {"layer1": {
                "index": dv, "query": {"esql": query}, "timeField": "@timestamp",
                "columns": [{"columnId": c, "fieldName": c, "meta": {"type": t}} for c, t in columns]}}}},
            "internalReferences": [],
            "adHocDataViews": {dv: {"id": dv, "title": index, "name": index, "timeFieldName": "@timestamp", "type": "esql",
                                    "sourceFilters": [], "fieldFormats": {}, "runtimeFieldMap": {}, "allowNoIndex": False,
                                    "allowHidden": False}},
        },
    }
    upsert("lens", id_, attributes, [])


def tile(id_, query, column, alert_from=None):
    """Tuile : une colonne de la requête ; alert_from colore en rouge à partir de cette valeur."""
    vis = {"layerId": "layer1", "layerType": "data", "metricAccessor": column}
    if alert_from is not None:
        vis["palette"] = {"type": "palette", "name": "custom", "params": {
            "name": "custom", "rangeType": "number", "rangeMin": None, "rangeMax": None, "progression": "fixed",
            "reverse": False, "continuity": "all", "steps": 2, "maxSteps": 5,
            "stops": [{"color": GREEN, "stop": alert_from}, {"color": RED, "stop": None}],
            "colorStops": [{"color": GREEN, "stop": None}, {"color": RED, "stop": alert_from}]}}
    esql(id_, column, query, "lnsMetric", vis, [(column, "number")])


def xy_esql(x, accessors, split=None, series="area_stacked", colors=None, y_title=None):
    layer = {"layerId": "layer1", "layerType": "data", "seriesType": series, "xAccessor": x, "accessors": accessors}
    if split:
        layer["splitAccessor"] = split
    if colors:
        layer["yConfig"] = [{"forAccessor": a, "color": c} for a, c in colors.items()]
    vis = {"legend": {"isVisible": True, "position": "right"}, "valueLabels": "hide", "preferredSeriesType": series,
           "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True}, "layers": [layer]}
    if y_title:
        vis["yTitle"] = y_title
    return vis


# --- Requêtes de la vue d'ensemble ---

# Retard d'une paire : borne haute, ou basse quand la haute est inconnue (au-delà de l'historique) ;
# KO = bloquée, partition sans membre, sans commit, en retard au-delà du seuil ou offsets perdus (rétention
# passée sous le commit).
LAG_KO = (f'lag_s = COALESCE(kafka.lag.seconds_upper, kafka.lag.seconds_lower), '
          f'ko = CASE(kafka.lag.state IN ("stalled", "unassigned", "no_commit") OR lag_s > {LAG_MAX_S}'
          f' OR kafka.lag.flags.commit_before_log_start == true, 1, 0)')
NOW_PAIRS = f'FROM kafkabeat-lag\n| WHERE @timestamp >= NOW() - {NOW_WINDOW} AND kafka.lag.state != "gone"'

Q_INVENTORY = f'''FROM kafkabeat-lag, kafkabeat-metrics
| WHERE @timestamp >= NOW() - {INVENTORY_WINDOW} AND NOT STARTS_WITH(kafka.topic.name, "__")
| EVAL consumed = CASE(event.dataset == "kafkabeat.lag" AND kafka.lag.state != "gone" AND @timestamp >= NOW() - {NOW_WINDOW}, 1, 0)
| STATS consumed = MAX(consumed) BY kafka.cluster.name, kafka.topic.name
| STATS `Tracked topics` = COUNT(*), `Consumed topics` = SUM(consumed)
| EVAL `Topics without group` = `Tracked topics` - `Consumed topics`'''

# production : heure du dernier message (instant du document moins son âge), comparée à maintenant
Q_TOPICS_NOW = f'''{NOW_PAIRS}
| EVAL {LAG_KO}, last_message_ms = TO_LONG(@timestamp) - kafka.lag.last_produce_age_s * 1000
| STATS ko = MAX(ko), last_message_ms = MAX(last_message_ms) BY kafka.cluster.name, kafka.topic.name
| EVAL stopped = CASE(TO_LONG(NOW()) - last_message_ms >= {PRODUCTION_STOPPED_S * 1000}, 1, 0)
| STATS topics = COUNT(*), ko = SUM(ko), stopped = SUM(stopped)
| EVAL `Consumption OK` = topics - ko, `Consumption not OK` = ko, `Production active` = topics - stopped, `Production stopped` = stopped'''

Q_REBALANCE = f'''FROM kafkabeat-lag
| WHERE @timestamp >= NOW() - {NOW_WINDOW}
    AND kafka.consumergroup.state IN ("PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling")
| EVAL group = CONCAT(kafka.cluster.name, "/", kafka.consumergroup.id)
| STATS `Groups rebalancing` = COUNT_DISTINCT(group)'''

Q_RATES = f'''{NOW_PAIRS}
| STATS produce = AVG(kafka.lag.produce_rate), consume = AVG(kafka.lag.consume_rate)
    BY kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| STATS produce = MAX(produce), consume = SUM(consume) BY kafka.cluster.name, kafka.topic.name
| STATS produce = SUM(produce), consume = SUM(consume)
| EVAL `Produce rate (msg/s)` = ROUND(produce, 1), `Consume rate (msg/s)` = ROUND(consume, 1)'''

Q_WORST = f'''{NOW_PAIRS}
| EVAL lag_s = COALESCE(kafka.lag.seconds_upper, kafka.lag.seconds_lower)
| STATS worst = MAX(lag_s)
| EVAL `Worst lag (s)` = ROUND(worst)'''

# partitions d'un topic consommé sans membre assigné (groupes Stable seulement : hors rebalance), par paire
Q_UNASSIGNED = f'''{NOW_PAIRS}
| STATS n = MAX(kafka.lag.partitions_unassigned) BY kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| STATS `Unassigned partitions` = SUM(n)'''

# groupes sans membre dont les commits sont figés ; l'assignation manuelle (commits qui avancent) est à part
Q_NO_MEMBER = f'''{NOW_PAIRS} AND kafka.consumergroup.assignment == "none"
| EVAL group = CONCAT(kafka.cluster.name, "/", kafka.consumergroup.id)
| STATS `Groups without members` = COUNT_DISTINCT(group)'''

# paire à risque de perte : journal retenu presque entièrement non lu, ou offsets déjà perdus
LOSS_RISK = (f'kafka.lag.retention_used >= {RISK_RETENTION} OR kafka.lag.flags.commit_before_log_start == true')
Q_LOSS_RISK = f'''{NOW_PAIRS}
| EVAL risk = CASE({LOSS_RISK}, 1, 0)
| STATS risk = MAX(risk) BY kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| STATS `Pairs at risk of loss` = SUM(risk)'''

# groupes au protocole consumer (KIP-848) : suivis à moitié (état seul, ni membres ni rebalances)
KIP848_NOW = f'{NOW_PAIRS} AND kafka.consumergroup.type == "consumer"'
Q_KIP848 = f'''{KIP848_NOW}
| EVAL group = CONCAT(kafka.cluster.name, "/", kafka.consumergroup.id)
| STATS `KIP-848 groups` = COUNT_DISTINCT(group)'''
Q_KIP848_LIST = f'''{KIP848_NOW}
| STATS `Topics` = COUNT_DISTINCT(kafka.topic.name)
    BY `Cluster` = kafka.cluster.name, `Group` = kafka.consumergroup.id, `Group state` = kafka.consumergroup.state
| SORT `Cluster`, `Group`
| LIMIT 1000'''

# âge du dernier cycle de l'instance la plus en retard : un agent arrêté ne publie plus rien
Q_HEALTH = '''FROM kafkabeat-health
| STATS last_cycle = MAX(@timestamp) BY kafka.cluster.name
| EVAL age = DATE_DIFF("seconds", last_cycle, NOW())
| STATS `Last cycle (s)` = MAX(age)'''

def q_rate_by_owner(buckets):
    """Débit produit par équipe : moyenne par topic sur l'intervalle, puis somme par équipe."""
    return f'''FROM kafkabeat-lag
| WHERE kafka.lag.state != "gone"
| EVAL owner = COALESCE(kafka.topic.owner, "(no team)")
| STATS rate = AVG(kafka.lag.produce_rate)
    BY t = BUCKET(@timestamp, {buckets}, ?_tstart, ?_tend), kafka.cluster.name, kafka.topic.name, owner
| STATS `Produce rate (msg/s)` = SUM(rate) BY t, `Team` = owner
| LIMIT 10000'''


Q_RATE_BY_OWNER = q_rate_by_owner(60)

Q_PAIRS_BY_STATE = ('FROM kafkabeat-lag\n'
                    '| EVAL pair = CONCAT(kafka.cluster.name, "/", kafka.topic.name, "/", kafka.consumergroup.id), '
                    + ", ".join(f'{s} = CASE(kafka.lag.state == "{s}", pair)' for s in STATE_LABELS) + '\n'
                    '| STATS ' + ", ".join(f'`{STATE_LABELS[s]}` = COUNT_DISTINCT({s})' for s in STATE_LABELS)
                    + '\n    BY t = BUCKET(@timestamp, 60, ?_tstart, ?_tend)\n| LIMIT 10000')


def build_overview():
    tile("kafkabeat-tile-topics", Q_INVENTORY, "Tracked topics")
    tile("kafkabeat-tile-consumed", Q_INVENTORY, "Consumed topics")
    tile("kafkabeat-tile-no-group", Q_INVENTORY, "Topics without group")
    tile("kafkabeat-tile-rebalance", Q_REBALANCE, "Groups rebalancing", alert_from=1)
    tile("kafkabeat-tile-consumption-ok", Q_TOPICS_NOW, "Consumption OK")
    tile("kafkabeat-tile-consumption-ko", Q_TOPICS_NOW, "Consumption not OK", alert_from=1)
    tile("kafkabeat-tile-production-ok", Q_TOPICS_NOW, "Production active")
    tile("kafkabeat-tile-production-stopped", Q_TOPICS_NOW, "Production stopped", alert_from=1)
    tile("kafkabeat-tile-produce-rate", Q_RATES, "Produce rate (msg/s)")
    tile("kafkabeat-tile-consume-rate", Q_RATES, "Consume rate (msg/s)")
    tile("kafkabeat-tile-worst-lag", Q_WORST, "Worst lag (s)", alert_from=LAG_MAX_S + 1)
    tile("kafkabeat-tile-unassigned", Q_UNASSIGNED, "Unassigned partitions", alert_from=1)
    tile("kafkabeat-tile-no-member", Q_NO_MEMBER, "Groups without members")
    tile("kafkabeat-tile-loss-risk", Q_LOSS_RISK, "Pairs at risk of loss", alert_from=1)
    tile("kafkabeat-tile-kip848", Q_KIP848, "KIP-848 groups")
    tile("kafkabeat-tile-health", Q_HEALTH, "Last cycle (s)", alert_from=31)
    kip848_cols = [("Cluster", "string"), ("Group", "string"), ("Group state", "string"), ("Topics", "number")]
    esql("kafkabeat-kip848-groups", "KIP-848 groups (state only: no members, no rebalances)", Q_KIP848_LIST, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "columns": [{"columnId": c} for c, _ in kip848_cols]}, kip848_cols)

    esql("kafkabeat-rate-by-owner", "Produce rate by team (msg/s)", Q_RATE_BY_OWNER, "lnsXY",
         xy_esql("t", ["Produce rate (msg/s)"], split="Team"),
         [("t", "date"), ("Produce rate (msg/s)", "number"), ("Team", "string")])
    labels = list(STATE_LABELS.values())
    esql("kafkabeat-pairs-by-state", "Pairs by consumption state", Q_PAIRS_BY_STATE, "lnsXY",
         xy_esql("t", labels, series="bar_stacked", colors={STATE_LABELS[s]: STATE_COLORS[s] for s in STATE_LABELS},
                 y_title="Pairs"),
         [("t", "date")] + [(c, "number") for c in labels])
    lens("kafkabeat-rate-top-topics", "Produce rate, top 10 topics (msg/s)", "lnsXY",
         xy(["rate"], "date", "topic"),
         {"date": date_col(), "topic": terms_col("kafka.topic.name", "Topic", "rate"),
          "rate": metric_col("average", "kafka.lag.produce_rate", "Produce rate (msg/s)", RATE)},
         ["topic", "date", "rate"], "kafkabeat-lag", 'not kafka.lag.state : "gone"')
    lens("kafkabeat-lag-upper", "Time lag, upper bound (s), worst 10 pairs", "lnsXY",
         xy(["upper"], "date", "pair"),
         {"date": date_col(), "pair": pair_col("upper"), "upper": max_col("kafka.lag.seconds_upper", "Upper bound (s)")},
         ["pair", "date", "upper"], "kafkabeat-lag", 'event.dataset : "kafkabeat.lag"')
    lens("kafkabeat-lag-state", "State by topic and group, worst first", "lnsDatatable",
         # largeur fixée pour la paire et les deux statuts, retour à la ligne sur un écran étroit
         {"layerId": "layer1", "layerType": "data", "rowHeight": "auto", "headerRowHeight": "auto",
          "columns": [{"columnId": "pair", "width": 250}, {"columnId": "state", "width": 170}, {"columnId": "production", "width": 170}]
                     + [{"columnId": c} for c in ("produce_age", "produce", "consume", "upper", "offsets", "commit_age")]},
         {"pair": pair_col("upper", 100), "state": last_col("kafka.lag.state", "Consumption", "string"),
          "production": last_col("kafka.lag.production", "Production", "string"),
          "produce_age": last_col("kafka.lag.last_produce_age_s", "Last message (s)"),
          "produce": last_col("kafka.lag.produce_rate", "Produce rate (msg/s)", fmt=RATE),
          "consume": last_col("kafka.lag.consume_rate", "Consume rate (msg/s)", fmt=RATE),
          "upper": last_col("kafka.lag.seconds_upper", "Upper bound (s)"),
          "offsets": last_col("kafka.lag.offsets_sum", "Lag (offsets)"),
          "commit_age": last_col("kafka.lag.last_commit_age_max_s", "Last commit (s)")},
         ["pair", "state", "production", "produce_age", "produce", "consume", "upper", "offsets", "commit_age"],
         "kafkabeat-lag", 'event.dataset : "kafkabeat.lag"')
    lens("kafkabeat-health-cycle", "kafkabeat health (cycle duration, pairs)", "lnsXY",
         xy(["cycle", "pairs"], "date", right="pairs"),
         {"date": date_col(), "cycle": max_col("kafkabeat.cycle_duration_ms", "Cycle duration (ms)"),
          "pairs": max_col("kafkabeat.pairs", "Tracked pairs")},
         ["date", "cycle", "pairs"], "kafkabeat-health")

    tiles = ["topics", "consumed", "no-group", "rebalance", "consumption-ok", "consumption-ko", "production-ok", "production-stopped"]
    panels = [(f"kafkabeat-tile-{t}", 6 * i, 0, 6, 5, True) for i, t in enumerate(tiles)]
    panels += [(f"kafkabeat-tile-{t}", 6 * i, 5, 6, 5, True)
               for i, t in enumerate(["produce-rate", "consume-rate", "worst-lag", "unassigned", "no-member", "loss-risk",
                                      "kip848", "health"])]
    panels += [("kafkabeat-lag-state", 0, 10, 48, 14, False),
               ("kafkabeat-rate-by-owner", 0, 24, 24, 12, False), ("kafkabeat-pairs-by-state", 24, 24, 24, 12, False),
               ("kafkabeat-rate-top-topics", 0, 36, 24, 12, False), ("kafkabeat-lag-upper", 24, 36, 24, 12, False),
               ("kafkabeat-health-cycle", 0, 48, 32, 8, False), ("kafkabeat-kip848-groups", 32, 48, 16, 8, False)]
    # même identifiant que le dashboard v1, qu'il remplace à l'import
    dashboard("kafkabeat-dashboard", "kafkabeat: overview",
              f"Cluster monitoring: tiles for the current moment (latest cycles; consumption not OK = stalled, partition "
              f"without a member for 60 s, no commit, lag > {LAG_MAX_S} s or lost offsets; production stopped = no "
              f"message for {PRODUCTION_STOPPED_S} s; groups without members = frozen commits, manual assignment excluded; "
              f"loss risk = {RISK_RETENTION:.0%} of the retained log unread, or offsets below the log start; "
              "KIP-848 groups = consumer protocol, tracked by their state only), "
              "state by pair, recent activity. Tiles show now, charts show the selected range.",
              panels, "now-15m", 10000)


# --- Zoom topic + groupe : à lire avec les filtres Topic et Groupe ---

# débits exacts quel que soit le filtre : produit par topic, consommé par paire
Q_ZOOM_RATES = '''FROM kafkabeat-lag
| WHERE kafka.lag.state != "gone"
| STATS produce = AVG(kafka.lag.produce_rate), consume = AVG(kafka.lag.consume_rate)
    BY t = BUCKET(@timestamp, 100, ?_tstart, ?_tend), kafka.cluster.name, kafka.topic.name, kafka.consumergroup.id
| STATS produce = MAX(produce), consume = SUM(consume) BY t, kafka.cluster.name, kafka.topic.name
| STATS `Produce rate (msg/s)` = SUM(produce), `Consume rate (msg/s)` = SUM(consume) BY t
| LIMIT 10000'''

Q_GROUP_STATES = '''FROM kafkabeat-lag
| EVAL group = CONCAT(kafka.cluster.name, "/", kafka.consumergroup.id)
| STATS `Groups` = COUNT_DISTINCT(group) BY t = BUCKET(@timestamp, 100, ?_tstart, ?_tend), `Group state` = kafka.consumergroup.state
| LIMIT 10000'''

# documents consumergroup (60 s) des deux derniers cycles : une ligne par membre du groupe (client.id et hôte
# peuvent être partagés par plusieurs instances)
Q_CONSUMERS = f'''FROM kafkabeat-metrics
| WHERE event.dataset == "kafka.consumergroup" AND @timestamp >= NOW() - {INVENTORY_WINDOW}
| EVAL client = COALESCE(kafka.consumergroup.client.id, "(unassigned)"), host = COALESCE(kafka.consumergroup.client.host, ""),
       member = COALESCE(kafka.consumergroup.client.member_id, "")
| STATS lag = MAX(kafka.consumergroup.consumer_lag), lag_s = MAX(COALESCE(kafka.lag.seconds_upper, kafka.lag.seconds_lower))
    BY kafka.cluster.name, kafka.consumergroup.id, kafka.topic.name, kafka.partition.id, client, host, member
| STATS `Partitions` = COUNT(*), `Lag (offsets)` = SUM(lag), worst = MAX(lag_s)
    BY `Group` = kafka.consumergroup.id, `Consumer` = client, `Host` = host, `Member` = member
| EVAL `Worst lag (s)` = ROUND(worst)
| KEEP `Group`, `Consumer`, `Host`, `Partitions`, `Lag (offsets)`, `Worst lag (s)`, `Member`
| SORT `Worst lag (s)` DESC
| LIMIT 1000'''


# partitions sans membre des derniers cycles (groupes Stable), avec leur ancienneté
Q_ZOOM_UNASSIGNED = f'''{NOW_PAIRS} AND kafka.lag.partitions_unassigned > 0
| STATS ids = VALUES(kafka.lag.partitions_unassigned_ids), age = MAX(kafka.lag.unassigned_age_max_s)
    BY kafka.cluster.name, `Topic` = kafka.topic.name, `Group` = kafka.consumergroup.id
| EVAL `Partitions without member` = MV_CONCAT(TO_STRING(MV_SORT(ids)), ", "), `Unassigned for (s)` = ROUND(age)
| KEEP `Topic`, `Group`, `Partitions without member`, `Unassigned for (s)`
| SORT `Unassigned for (s)` DESC
| LIMIT 1000'''

Q_ZOOM_REBALANCES = '''FROM kafkabeat-events
| WHERE event.dataset == "kafkabeat.rebalance"
| STATS `Rebalances` = COUNT(*) BY t = BUCKET(@timestamp, 100, ?_tstart, ?_tend), `Group` = kafka.consumergroup.id
| LIMIT 10000'''

# journal des membres : un rebalance par ligne, membres entrés et sortis (l'identifiant de membre commence par
# le client.id) et leurs hôtes ; @timestamp gardé tel quel pour être affiché à l'heure locale
Q_ZOOM_MEMBERS = '''FROM kafkabeat-events
| WHERE event.dataset == "kafkabeat.rebalance"
| EVAL `Joined` = MV_CONCAT(kafka.rebalance.joined.member_id, ", "), `Left` = MV_CONCAT(kafka.rebalance.left.member_id, ", "),
       `Joined hosts` = MV_CONCAT(kafka.rebalance.joined.client_host, ", "), `Left hosts` = MV_CONCAT(kafka.rebalance.left.client_host, ", ")
| KEEP @timestamp, kafka.consumergroup.id, kafka.consumergroup.generation, kafka.consumergroup.members, kafka.rebalance.method,
       kafka.rebalance.duration_s, `Joined`, `Left`, `Joined hosts`, `Left hosts`
| RENAME kafka.consumergroup.id AS `Group`, kafka.consumergroup.generation AS `Generation`,
         kafka.consumergroup.members AS `Members`, kafka.rebalance.method AS `Method`, kafka.rebalance.duration_s AS `Observed duration (s)`
| SORT @timestamp DESC
| LIMIT 500'''


def build_zoom():
    lens("kafkabeat-zoom-bounds", "Time lag, lower and upper bounds (s)", "lnsXY",
         xy(["lower", "upper"], "date"),
         {"date": date_col(), "lower": max_col("kafka.lag.seconds_lower", "Lower bound (s)"),
          "upper": max_col("kafka.lag.seconds_upper", "Upper bound (s)")},
         ["date", "lower", "upper"], "kafkabeat-lag", 'not kafka.lag.state : "gone"')
    esql("kafkabeat-zoom-rates", "Produce and consume rates (msg/s)", Q_ZOOM_RATES, "lnsXY",
         xy_esql("t", ["Produce rate (msg/s)", "Consume rate (msg/s)"], series="line", y_title="msg/s"),
         [("t", "date"), ("Produce rate (msg/s)", "number"), ("Consume rate (msg/s)", "number")])
    lens("kafkabeat-zoom-partition-lag", "Lag by partition, upper bound (s)", "lnsXY",
         xy(["upper"], "date", "partition"),
         {"date": date_col(), "partition": terms_col("kafka.partition.id", "Partition", "upper", 48),
          "upper": max_col("kafka.lag.seconds_upper", "Upper bound (s)")},
         ["partition", "date", "upper"], "kafkabeat-metrics", 'event.dataset : "kafka.consumergroup"')
    lens("kafkabeat-zoom-commit-age", "Last commit age, worst partition (s)", "lnsXY",
         xy(["age"], "date", "pair"),
         {"date": date_col(), "pair": pair_col("age"), "age": max_col("kafka.lag.last_commit_age_max_s", "Last commit (s)")},
         ["pair", "date", "age"], "kafkabeat-lag", 'not kafka.lag.state : "gone"')
    esql("kafkabeat-zoom-group-states", "Group state over time", Q_GROUP_STATES, "lnsXY",
         xy_esql("t", ["Groups"], split="Group state", series="bar_stacked"),
         [("t", "date"), ("Groups", "number"), ("Group state", "string")])
    consumer_cols = [("Group", "string"), ("Consumer", "string"), ("Host", "string"), ("Partitions", "number"),
                     ("Lag (offsets)", "number"), ("Worst lag (s)", "number"), ("Member", "string")]
    esql("kafkabeat-zoom-consumers", "Consumers: partitions and lag (now)", Q_CONSUMERS, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "columns": [{"columnId": c} for c, _ in consumer_cols]},
         consumer_cols)
    pair = pair_col("upper", 20)
    lens("kafkabeat-zoom-partitions", "Partitions: offsets, lag and assigned consumer", "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "rowHeight": "single", "headerRowHeight": "auto",
          "columns": [{"columnId": "pair", "width": 220}] + [{"columnId": c} for c in
                      ("partition", "upper", "lag", "offset", "retention", "commit_age", "client", "host")]
                     + [{"columnId": "member", "width": 380}]},
         {"pair": pair, "partition": terms_col("kafka.partition.id", "Partition", "upper", 100),
          "upper": last_col("kafka.lag.seconds_upper", "Upper bound (s)"),
          "lag": last_col("kafka.consumergroup.consumer_lag", "Lag (offsets)"),
          "offset": last_col("kafka.consumergroup.offset", "Committed offset"),
          "retention": last_col("kafka.lag.retention_used", "Unread retained log", fmt={"id": "percent", "params": {"decimals": 0}}),
          "commit_age": last_col("kafka.lag.last_commit_age_s", "Last commit (s)"),
          "client": last_col("kafka.consumergroup.client.id", "Consumer", "string"),
          "host": last_col("kafka.consumergroup.client.host", "Host", "string"),
          "member": last_col("kafka.consumergroup.client.member_id", "Member", "string")},
         ["pair", "partition", "upper", "lag", "offset", "retention", "commit_age", "client", "host", "member"],
         "kafkabeat-metrics", 'event.dataset : "kafka.consumergroup"')
    unassigned_cols = [("Topic", "string"), ("Group", "string"), ("Partitions without member", "string"), ("Unassigned for (s)", "number")]
    esql("kafkabeat-zoom-unassigned", "Partitions without an assigned member (now)", Q_ZOOM_UNASSIGNED, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "columns": [{"columnId": c} for c, _ in unassigned_cols]},
         unassigned_cols)
    esql("kafkabeat-zoom-rebalances", "Rebalances by group", Q_ZOOM_REBALANCES, "lnsXY",
         xy_esql("t", ["Rebalances"], split="Group", series="bar_stacked"),
         [("t", "date"), ("Rebalances", "number"), ("Group", "string")])
    member_cols = [("@timestamp", "date"), ("Group", "string"), ("Generation", "number"), ("Members", "number"), ("Method", "string"),
                   ("Observed duration (s)", "number"), ("Joined", "string"), ("Left", "string"), ("Joined hosts", "string"),
                   ("Left hosts", "string")]
    esql("kafkabeat-zoom-members", "Member log (one rebalance per row)", Q_ZOOM_MEMBERS, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "rowHeight": "auto", "columns": [{"columnId": c} for c, _ in member_cols]},
         member_cols)
    dashboard("kafkabeat-zoom", "kafkabeat: topic + group zoom",
              "Diagnosing one pair: pick the topic and the group in the filters. Lag by partition and assigned consumers: "
              "consumergroup documents (every 60 s). Rebalances: one document per group generation (stream method), or "
              "per membership change seen every 60 s when the stream is unavailable (describe); observed duration = lower "
              "bound, seen by the 10 s cycles.",
              [("kafkabeat-zoom-bounds", 0, 0, 24, 12, False), ("kafkabeat-zoom-rates", 24, 0, 24, 12, False),
               ("kafkabeat-zoom-partition-lag", 0, 12, 24, 12, False), ("kafkabeat-zoom-commit-age", 24, 12, 24, 12, False),
               ("kafkabeat-zoom-group-states", 0, 24, 24, 12, False), ("kafkabeat-zoom-consumers", 24, 24, 24, 12, False),
               ("kafkabeat-zoom-unassigned", 0, 36, 16, 12, False), ("kafkabeat-zoom-rebalances", 16, 36, 32, 12, False),
               ("kafkabeat-zoom-members", 0, 48, 48, 18, False),
               ("kafkabeat-zoom-partitions", 0, 66, 48, 16, False)],
              "now-1h", 30000)


# --- Tendances : documents bruts sur 7 jours, puis une valeur par heure jusqu'à 90 jours (downsampling ILM) ---

Q_TREND_LAG_BY_OWNER = '''FROM kafkabeat-lag
| WHERE kafka.lag.state != "gone"
| EVAL owner = COALESCE(kafka.topic.owner, "(no team)"), lag_s = COALESCE(kafka.lag.seconds_upper, kafka.lag.seconds_lower)
| STATS `Worst lag (s)` = MAX(lag_s) BY t = BUCKET(@timestamp, 200, ?_tstart, ?_tend), `Team` = owner
| LIMIT 10000'''

# volume : écart du total des offsets de fin du topic sur la plage (même valeur pour chacun de ses groupes)
Q_TREND_TOPICS = '''FROM kafkabeat-lag
| WHERE kafka.lag.state != "gone"
| EVAL owner = COALESCE(kafka.topic.owner, "(no team)"), lag_s = COALESCE(kafka.lag.seconds_upper, kafka.lag.seconds_lower)
| STATS hi = MAX(kafka.lag.produced_total), lo = MIN(kafka.lag.produced_total), peak = MAX(kafka.lag.produce_rate), worst = MAX(lag_s)
    BY kafka.cluster.name, `Topic` = kafka.topic.name, `Team` = owner
| EVAL `Messages produced` = hi - lo, `Peak rate (msg/s)` = ROUND(peak, 1), `Worst lag (s)` = ROUND(worst)
| KEEP `Topic`, `Team`, `Messages produced`, `Peak rate (msg/s)`, `Worst lag (s)`
| SORT `Messages produced` DESC
| LIMIT 100'''


def build_trends():
    esql("kafkabeat-trend-rate-by-owner", "Produce rate by team (msg/s)", q_rate_by_owner(200), "lnsXY",
         xy_esql("t", ["Produce rate (msg/s)"], split="Team"),
         [("t", "date"), ("Produce rate (msg/s)", "number"), ("Team", "string")])
    esql("kafkabeat-trend-lag-by-owner", "Worst lag by team (s)", Q_TREND_LAG_BY_OWNER, "lnsXY",
         xy_esql("t", ["Worst lag (s)"], split="Team", series="line"),
         [("t", "date"), ("Worst lag (s)", "number"), ("Team", "string")])
    topic_cols = ["Topic", "Team", "Messages produced", "Peak rate (msg/s)", "Worst lag (s)"]
    esql("kafkabeat-trend-topics", "Topics by volume produced over the range", Q_TREND_TOPICS, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "columns": [{"columnId": c} for c in topic_cols]},
         [(c, "string" if i < 2 else "number") for i, c in enumerate(topic_cols)])
    dashboard("kafkabeat-trends", "kafkabeat: trends (up to 90 days)",
              "Rates and lag by team and by topic: raw documents for the last 7 days, then one value per hour and per "
              "series (the last of the hour) up to 90 days. Beyond 7 days, a spike shorter than an hour may not show; "
              "the volume produced stays exact to the hour.",
              [("kafkabeat-trend-rate-by-owner", 0, 0, 24, 14, False), ("kafkabeat-trend-lag-by-owner", 24, 0, 24, 14, False),
               ("kafkabeat-trend-topics", 0, 14, 48, 16, False)],
              "now-7d", 0)


# --- Santé des brokers : metricset broker (Jolokia, 60 s) et documents partition ---

BROKER_FIXED = 'FROM kafkabeat-metrics\n| WHERE event.dataset == "kafka.broker" AND kafka.broker.mbean IS NULL'
BROKER_KEY = 'broker = CONCAT(kafka.cluster.name, "/", TO_STRING(kafka.broker.id))'


def q_broker_series(fields):
    """Somme sur les brokers de la moyenne de chaque broker par intervalle : fields = [(colonne, champ)]."""
    # champ entre accents graves : « in » est un mot réservé d'ES|QL (net.in.bytes_per_sec)
    per = ", ".join(f"`{c}` = AVG(`{f}`)" for c, f in fields)
    tot = ", ".join(f"`{c}` = SUM(`{c}`)" for c, _ in fields)
    return f'''{BROKER_FIXED}
| EVAL {BROKER_KEY}
| STATS {per} BY t = BUCKET(@timestamp, 100, ?_tstart, ?_tend), broker
| STATS {tot} BY t
| LIMIT 10000'''


BYTES_COLS = [("Bytes in (B/s)", "kafka.broker.net.in.bytes_per_sec"), ("Bytes out (B/s)", "kafka.broker.net.out.bytes_per_sec")]
FAILED_COLS = [("Failed produce (req/s)", "kafka.broker.request.produce.failed"),
               ("Failed fetch (req/s)", "kafka.broker.request.fetch.failed")]
ELECTION_COLS = [("Leader elections (/s)", "kafka.broker.replication.leader_elections"),
                 ("Unclean leader elections (/s)", "kafka.broker.replication.unclean_leader_elections")]

# partition sous-répliquée : au moins un réplica hors ISR (documents partition, un par réplica)
UNDER_REPLICATED = '''FROM kafkabeat-metrics
| WHERE event.dataset == "kafka.partition"'''
OUT_OF_ISR = 'EVAL out = CASE(kafka.partition.partition.insync_replica == false, 1, 0)'

Q_UNDER_REPLICATED = f'''{UNDER_REPLICATED}
| {OUT_OF_ISR}
| STATS out = MAX(out) BY t = BUCKET(@timestamp, 100, ?_tstart, ?_tend), kafka.cluster.name, kafka.topic.name, kafka.partition.id
| STATS `Under-replicated partitions` = SUM(out) BY t
| LIMIT 10000'''

Q_UNDER_REPLICATED_NOW = f'''{UNDER_REPLICATED} AND @timestamp >= NOW() - {INVENTORY_WINDOW}
| {OUT_OF_ISR}
| STATS out = MAX(out) BY kafka.cluster.name, kafka.topic.name, kafka.partition.id
| STATS `Under-replicated partitions` = SUM(out)'''

Q_BROKERS_READ = f'''{BROKER_FIXED} AND @timestamp >= NOW() - {INVENTORY_WINDOW}
| EVAL {BROKER_KEY}
| STATS `Brokers read (Jolokia)` = COUNT_DISTINCT(broker)'''

Q_BROKERS_FAILED = f'''FROM kafkabeat-health
| WHERE @timestamp >= NOW() - {NOW_WINDOW}
| STATS failed = MAX(kafkabeat.jolokia.brokers_failed) BY kafka.cluster.name
| STATS `Unreachable brokers (Jolokia)` = SUM(failed)'''

# dernière interrogation (une par minute) : maximum sur 70 s
Q_BROKERS_TABLE = f'''{BROKER_FIXED} AND @timestamp >= NOW() - 70 seconds
| STATS bytes_in = MAX(`kafka.broker.net.in.bytes_per_sec`), bytes_out = MAX(kafka.broker.net.out.bytes_per_sec),
    msgs = MAX(kafka.broker.messages_in), produce = MAX(kafka.broker.request.produce.failed),
    fetch = MAX(kafka.broker.request.fetch.failed), queue = MAX(kafka.broker.request.channel.queue.size)
    BY `Cluster` = kafka.cluster.name, `Broker` = kafka.broker.id, `Address` = kafka.broker.address
| EVAL `Bytes in (B/s)` = ROUND(bytes_in), `Bytes out (B/s)` = ROUND(bytes_out), `Messages (msg/s)` = ROUND(msgs, 1),
       `Failed produce (req/s)` = ROUND(produce, 2), `Failed fetch (req/s)` = ROUND(fetch, 2), `Request queue` = queue
| KEEP `Cluster`, `Broker`, `Address`, `Bytes in (B/s)`, `Bytes out (B/s)`, `Messages (msg/s)`, `Failed produce (req/s)`,
       `Failed fetch (req/s)`, `Request queue`
| SORT `Cluster`, `Broker`
| LIMIT 1000'''


def build_brokers():
    tile("kafkabeat-tile-brokers-read", Q_BROKERS_READ, "Brokers read (Jolokia)")
    tile("kafkabeat-tile-brokers-failed", Q_BROKERS_FAILED, "Unreachable brokers (Jolokia)", alert_from=1)
    tile("kafkabeat-tile-under-replicated", Q_UNDER_REPLICATED_NOW, "Under-replicated partitions", alert_from=1)
    for id_, title, cols in [("kafkabeat-brokers-bytes", "Bytes per second, all brokers", BYTES_COLS),
                             ("kafkabeat-brokers-failed", "Failed requests, all brokers (req/s)", FAILED_COLS),
                             ("kafkabeat-brokers-elections", "Leader elections, all brokers (/s)", ELECTION_COLS)]:
        esql(id_, title, q_broker_series(cols), "lnsXY", xy_esql("t", [c for c, _ in cols], series="line"),
             [("t", "date")] + [(c, "number") for c, _ in cols])
    esql("kafkabeat-brokers-under-replicated", "Under-replicated partitions", Q_UNDER_REPLICATED, "lnsXY",
         xy_esql("t", ["Under-replicated partitions"], series="line"),
         [("t", "date"), ("Under-replicated partitions", "number")])
    table_cols = [("Cluster", "string"), ("Broker", "number"), ("Address", "string"), ("Bytes in (B/s)", "number"),
                  ("Bytes out (B/s)", "number"), ("Messages (msg/s)", "number"), ("Failed produce (req/s)", "number"),
                  ("Failed fetch (req/s)", "number"), ("Request queue", "number")]
    esql("kafkabeat-brokers-table", "Brokers: throughput and requests (last read)", Q_BROKERS_TABLE, "lnsDatatable",
         {"layerId": "layer1", "layerType": "data", "columns": [{"columnId": c} for c, _ in table_cols]}, table_cols)
    dashboard("kafkabeat-brokers", "kafkabeat: broker health",
              "Replaces the Metricbeat broker metricset: MBeans read through Jolokia on each broker every 60 s (one-minute "
              "rates, Metricbeat names kafka.broker.*). Under-replicated partitions: partition documents (replica out of "
              "ISR). Only the Cluster filter applies: broker documents carry neither topic nor group.",
              [("kafkabeat-tile-brokers-read", 0, 0, 16, 5, True), ("kafkabeat-tile-brokers-failed", 16, 0, 16, 5, True),
               ("kafkabeat-tile-under-replicated", 32, 0, 16, 5, True),
               ("kafkabeat-brokers-bytes", 0, 5, 24, 12, False), ("kafkabeat-brokers-failed", 24, 5, 24, 12, False),
               ("kafkabeat-brokers-elections", 0, 17, 24, 12, False), ("kafkabeat-brokers-under-replicated", 24, 17, 24, 12, False),
               ("kafkabeat-brokers-table", 0, 29, 48, 12, False)],
              "now-1h", 60000, controls_keys=["cluster"])


def controls(keys=None):
    """Groupe de contrôles (liste d'options) des dashboards, et ses références ; keys : sous-ensemble de CONTROLS."""
    panels, refs = {}, []
    for order, (key, field, title) in enumerate(c for c in CONTROLS if keys is None or c[0] in keys):
        cid = f"ctl-{key}"
        panels[cid] = {"order": order, "width": "medium", "grow": True, "type": "optionsListControl",
                       "explicitInput": {"id": cid, "fieldName": field, "title": title, "searchTechnique": "prefix",
                                         "selectedOptions": [], "sort": {"by": "_count", "direction": "desc"}}}
        refs.append({"name": f"controlGroup_{cid}:optionsListDataView", "type": "index-pattern", "id": "kafkabeat-topics"})
    return {"chainingSystem": "HIERARCHICAL", "controlStyle": "oneLine", "showApplySelections": False,
            "ignoreParentSettingsJSON": json.dumps({"ignoreFilters": False, "ignoreQuery": False, "ignoreTimerange": False,
                                                    "ignoreValidations": False}),
            "panelsJSON": json.dumps(panels)}, refs


def dashboard(id_, title, description, panels, time_from, refresh_ms, controls_keys=None):
    """panels : [(id lens, x, y, w, h, titre masqué)] ; controls_keys : contrôles retenus (tous par défaut)."""
    grid, refs = [], []
    for i, (pid, x, y, w, h, hide_title) in enumerate(panels):
        config = {"enhancements": {}}
        if hide_title:
            config["hidePanelTitles"] = True
        grid.append({"type": "lens", "gridData": {"x": x, "y": y, "w": w, "h": h, "i": f"p{i}"}, "panelIndex": f"p{i}",
                     "embeddableConfig": config, "panelRefName": f"panel_{i}"})
        refs.append({"name": f"panel_{i}", "type": "lens", "id": pid})
    control_group, control_refs = controls(controls_keys)
    upsert("dashboard", id_, {
        "title": title,
        "description": description,
        "panelsJSON": json.dumps(grid),
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": time_from, "timeTo": "now", "refreshInterval": {"pause": refresh_ms == 0, "value": refresh_ms},
        "controlGroupInput": control_group,
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})},
    }, refs + control_refs)


def es_query_params(query, agg, terms, comparator, threshold):
    """Paramètres d'une règle « Elasticsearch query » sur les documents de paire de la dernière minute, par groupe."""
    params = {"searchType": "esQuery", "index": ["kafkabeat-lag"], "timeField": "@timestamp",
              "esQuery": json.dumps({"query": {"bool": {"filter": [{"term": {"event.dataset": "kafkabeat.lag"}}] + query}}}),
              "groupBy": "top", "termField": terms, "termSize": 1000,
              "thresholdComparator": comparator, "threshold": [threshold], "timeWindowSize": 1, "timeWindowUnit": "m",
              "size": 100, "excludeHitsFromPreviousRun": False}
    params.update(agg)
    return params


def rules():
    """Règles d'exemple, créées désactivées et sans action (l'exploitant les active et leur ajoute un connecteur)."""
    pair = ["kafka.cluster.name", "kafka.topic.name", "kafka.consumergroup.id"]
    definitions = {
        "lag": (f"kafkabeat: time lag > {LAG_MAX_S // 60} min (upper bound)", ".es-query",
                es_query_params([], {"aggType": "max", "aggField": "kafka.lag.seconds_upper"}, pair, ">", LAG_MAX_S)),
        # par cluster, option « groupe disparu » : un agent arrêté ne publie plus rien, son cluster sort des groupes ;
        # un cycle en échec (Kafka injoignable) ne compte pas comme réussi
        "mute": (f"kafkabeat: silent component (no successful cycle for {MUTE_MINUTES} min, per cluster)",
                 "observability.rules.custom_threshold",
                 {"criteria": [{"comparator": "<", "threshold": [1], "timeSize": MUTE_MINUTES, "timeUnit": "m",
                                "metrics": [{"name": "A", "aggType": "count", "filter": "not kafkabeat.cycle_failed : true"}]}],
                  "groupBy": ["kafka.cluster.name"], "alertOnNoData": True, "alertOnGroupDisappear": True,
                  "searchConfiguration": {"query": {"query": "", "language": "kuery"}, "index": "kafkabeat-health"}}),
        "production": (f"kafkabeat: production stopped for {PRODUCTION_ALERT_S // 60} min (consumed topic)", ".es-query",
                       es_query_params([], {"aggType": "max", "aggField": "kafka.lag.last_produce_age_s"},
                                       ["kafka.cluster.name", "kafka.topic.name"], ">", PRODUCTION_ALERT_S)),
        "loss": (f"kafkabeat: loss risk ({RISK_RETENTION:.0%} of the retained log unread, or offsets lost)", ".es-query",
                 es_query_params([{"bool": {"minimum_should_match": 1, "should": [
                     {"range": {"kafka.lag.retention_used": {"gte": RISK_RETENTION}}},
                     {"term": {"kafka.lag.flags.commit_before_log_start": True}}]}}],
                     {"aggType": "count"}, pair, ">", 0)),
    }
    for key, (name, type_id, params) in definitions.items():
        rid = RULE_IDS[key]
        call("DELETE", f"/api/alerting/rule/{rid}")
        status, body = call("POST", f"/api/alerting/rule/{rid}", {
            "name": name, "rule_type_id": type_id, "consumer": "alerts", "schedule": {"interval": "1m"},
            "tags": ["kafkabeat"], "actions": [], "enabled": False, "params": params})
        if status >= 300:
            sys.exit(f"rule {key}: HTTP {status}: {text(body)}")
        print(f"rule {key} ({rid}) created")


def build():
    data_view("kafkabeat-lag", "kafkabeat-lag", {"kafka.lag.state": lookup(STATE_LABELS), "kafka.lag.production": lookup(PRODUCTION_LABELS),
                                                  "kafka.consumergroup.assignment": lookup(ASSIGNMENT_LABELS)})
    data_view("kafkabeat-metrics", "kafkabeat-metrics")
    data_view("kafkabeat-health", "kafkabeat-health")
    data_view("kafkabeat-topics", "kafkabeat-lag,kafkabeat-metrics")
    build_overview()
    build_zoom()
    build_trends()
    build_brokers()
    rules()


DASHBOARDS = ["kafkabeat-dashboard", "kafkabeat-zoom", "kafkabeat-trends", "kafkabeat-brokers"]


# Métadonnées de l'instance Kibana qui a produit l'export (dates et version de
# chaque objet) : retirées pour que l'export ne change que quand son contenu
# change ; Kibana les recrée à l'import. Les attributs des règles (createdAt,
# executionStatus...) restent tels quels : l'import les exige (vérifié sur 8.19 et 9.5).
DROP_OBJECT = ("created_at", "updated_at", "created_by", "updated_by", "version")


def stable(line):
    obj = json.loads(line)
    for k in DROP_OBJECT:
        obj.pop(k, None)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def export():
    objects = [{"type": "dashboard", "id": d} for d in DASHBOARDS] + [{"type": "alert", "id": r} for r in RULE_IDS.values()]
    status, body = call("POST", "/api/saved_objects/_export", {"objects": objects, "includeReferencesDeep": True, "excludeExportDetails": True})
    if status >= 300:
        sys.exit(f"export: HTTP {status}: {text(body)}")
    lines = [stable(line) for line in body.decode().splitlines() if line.strip()]
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines) + "\n")
    print(f"export: {OUT} ({len(lines)} objects)")


if __name__ == "__main__":
    if "--export-only" not in sys.argv:
        build()
    export()
