package sink

import "strings"

// templateVersion est incrémentée à chaque changement de mapping (Setup
// bascule alors les data streams existants sur le nouveau mapping).
const templateVersion = 6

// Dimensions TSDS : les identifiants de chaque document d'un flux. Une
// dimension oubliée confond deux séries, et le second document, refusé en
// 409, ne serait compté que comme doublon : d'où kafka.broker.mbean, qui
// distingue les quatre documents par topic d'un même broker.
var (
	lagDimensions     = []string{"event.dataset", "kafka.cluster.name", "kafka.topic.name", "kafka.consumergroup.id"}
	metricsDimensions = append(append([]string{}, lagDimensions...), "kafka.partition.id", "kafka.broker.id", "kafka.broker.mbean")
)

func ilmPolicy() map[string]any {
	return map[string]any{
		"policy": map[string]any{
			"_meta": map[string]any{"description": "kafkabeat: daily rollover, deleted after 30 days (adjust to your needs)"},
			"phases": map[string]any{
				"hot": map[string]any{
					"actions": map[string]any{
						"rollover": map[string]any{"max_age": "1d", "max_primary_shard_size": "50gb"},
					},
				},
				"delete": map[string]any{
					"min_age": "30d",
					"actions": map[string]any{"delete": map[string]any{}},
				},
			},
		},
	}
}

// timeSeriesPolicy : flux de lag et de métriques en TSDS. Aucun champ n'étant
// marqué métrique, le sous-échantillonnage garde la dernière valeur de chaque
// heure (ES|QL 8.19 ne lit pas aggregate_metric_double).
func timeSeriesPolicy() map[string]any {
	return map[string]any{
		"policy": map[string]any{
			"_meta": map[string]any{"description": "kafkabeat (TSDS): daily rollover, downsampled to one value per hour after 7 days, deleted after 90 days (adjust to your needs)"},
			"phases": map[string]any{
				"hot": map[string]any{
					"actions": map[string]any{
						"rollover": map[string]any{"max_age": "1d", "max_primary_shard_size": "50gb"},
					},
				},
				"warm": map[string]any{
					"min_age": "7d",
					"actions": map[string]any{"downsample": map[string]any{"fixed_interval": "1h"}},
				},
				"delete": map[string]any{
					"min_age": "90d",
					"actions": map[string]any{"delete": map[string]any{}},
				},
			},
		},
	}
}

// indexTemplate : avec des dimensions, le flux passe en TSDS au rollover qui
// suit l'installation ; seules les dimensions keyword servent au routage.
func indexTemplate(name, policy string, mapping map[string]any, dimensions []string) map[string]any {
	settings := map[string]any{
		"index.lifecycle.name":             policy,
		"index.number_of_shards":           1,
		"index.codec":                      "best_compression",
		"index.mapping.total_fields.limit": 2000,
	}
	if len(dimensions) > 0 {
		var routing []string
		for _, path := range dimensions {
			field := markDimension(mapping, path)
			if field["type"] == "keyword" {
				routing = append(routing, path)
			}
		}
		settings["index.mode"] = "time_series"
		settings["index.routing_path"] = routing
	}
	return map[string]any{
		"index_patterns": []string{name + "*"},
		"data_stream":    map[string]any{},
		"priority":       200,
		"version":        templateVersion,
		"_meta":          map[string]any{"managed_by": "kafkabeat"},
		"template": map[string]any{
			"settings": settings,
			"mappings": mapping,
		},
	}
}

// markDimension marque le champ au chemin pointé ; un chemin absent du
// mapping est une erreur de programmation.
func markDimension(mapping map[string]any, path string) map[string]any {
	props := mapping["properties"].(map[string]any)
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		props = props[p].(map[string]any)["properties"].(map[string]any)
	}
	field := props[parts[len(parts)-1]].(map[string]any)
	field["time_series_dimension"] = true
	return field
}

func keyword() map[string]any { return map[string]any{"type": "keyword", "ignore_above": 1024} }
func long() map[string]any    { return map[string]any{"type": "long"} }
func integer() map[string]any { return map[string]any{"type": "integer"} }
func double() map[string]any  { return map[string]any{"type": "double"} }
func boolean() map[string]any { return map[string]any{"type": "boolean"} }
func obj(props map[string]any) map[string]any {
	return map[string]any{"properties": props}
}

// base : champs communs aux trois flux. Les champs inconnus (enrichissement
// statique) sont indexés en keyword.
func base(extra map[string]any) map[string]any {
	props := map[string]any{
		"@timestamp": map[string]any{"type": "date"},
		"event":      obj(map[string]any{"dataset": keyword(), "module": keyword()}),
	}
	for k, v := range extra {
		props[k] = v
	}
	return map[string]any{
		"dynamic": true,
		"dynamic_templates": []map[string]any{
			{"strings_as_keyword": map[string]any{"match_mapping_type": "string", "mapping": keyword()}},
		},
		"properties": props,
	}
}

func clusterProps() map[string]any {
	return obj(map[string]any{"name": keyword(), "id": keyword()})
}

func lagMapping() map[string]any {
	flags := map[string]any{}
	for _, f := range []string{"offset_reset_suspected", "log_truncated_suspected", "topic_recreated", "group_recreated", "resumed_after_gap", "cycle_incomplete", "commit_before_log_start"} {
		flags[f] = boolean()
	}
	return base(map[string]any{
		"kafka": obj(map[string]any{
			"cluster":       clusterProps(),
			"topic":         obj(map[string]any{"name": keyword(), "id": keyword(), "owner": keyword()}),
			"consumergroup": obj(map[string]any{"id": keyword(), "state": keyword(), "type": keyword(), "members": integer(), "assignment": keyword()}),
			"lag": obj(map[string]any{
				"retention_used":            double(),
				"offsets_lost":              long(),
				"state":                     keyword(),
				"partitions_unassigned":     integer(),
				"partitions_unassigned_ids": integer(),
				"unassigned_age_max_s":      double(),
				"offsets_sum":               long(),
				"seconds_lower":             double(),
				"seconds_upper":             double(),
				"method":                    keyword(),
				"partition_count":           integer(),
				"partitions_lagging":        integer(),
				"partitions_stalled":        integer(),
				"partitions_no_commit":      integer(),
				"partitions_beyond_history": integer(),
				"partitions_unknown":        integer(),
				"worst_partition_time":      integer(),
				"worst_partition_offsets":   integer(),
				"last_commit_age_max_s":     double(),
				"production":                keyword(),
				"last_produce_age_s":        double(),
				"produced_total":            long(),
				"consumed_total":            long(),
				"produce_rate":              double(),
				"consume_rate":              double(),
				"consume_rate_smoothed":     double(),
				"smoothing_window_s":        double(),
				"interval_ms":               long(),
				"flags":                     obj(flags),
				// détail des partitions en retard : lu dans _source seulement
				// (ni ES|QL ni Lens ne lisent un nested, qui doublait la taille du flux)
				"partitions": map[string]any{"type": "object", "enabled": false},
			}),
		}),
	})
}

func healthMapping() map[string]any {
	return base(map[string]any{
		"kafka": obj(map[string]any{"cluster": clusterProps()}),
		"kafkabeat": obj(map[string]any{
			"version":           keyword(),
			"cycle":             long(),
			"cycle_duration_ms": long(),
			"cycle_incomplete":  boolean(),
			"cycle_failed":      boolean(),
			"errors":            keyword(),
			"partial_errors":    integer(),
			"group_errors":      integer(),
			"topics":            integer(),
			"partitions":        integer(),
			"groups":            integer(),
			"pairs":             integer(),
			"pairs_gone":        integer(),
			"pairs_unknown":     integer(),
			"docs":              integer(),
			"rebalance_method":  keyword(),
			"commits_mode":      keyword(),
			"jolokia": obj(map[string]any{
				"brokers": integer(), "brokers_failed": integer(), "docs": integer(), "duration_ms": long(),
				"last_poll": map[string]any{"type": "date"},
			}),
			"history":       obj(map[string]any{"series": integer(), "entries": long()}),
			"last_snapshot": map[string]any{"type": "date"},
			"sink": obj(map[string]any{
				"requests": long(), "sent": long(), "failed": long(), "duplicates": long(), "last_error": keyword(),
			}),
			"stream": obj(map[string]any{
				"ready":                   boolean(),
				"records":                 long(),
				"tombstones":              long(),
				"malformed":               long(),
				"unknown_keys":            long(),
				"unknown_key_versions":    map[string]any{"type": "flattened"},
				"unknown_values":          long(),
				"unknown_value_versions":  map[string]any{"type": "flattened"},
				"groups_fallback":         integer(),
				"group_records":           long(),
				"group_unknown_values":    long(),
				"groups_members_fallback": integer(),
				"mismatches":              long(),
				"reconciliations":         long(),
				"lag_offsets":             long(),
				"last_record_age_ms":      long(),
				"errors":                  long(),
				"last_error":              keyword(),
			}),
		}),
	})
}

func float() map[string]any { return map[string]any{"type": "float"} }

// brokerProps : champs du metricset broker de Metricbeat (types de son
// template), plus id et address communs aux documents partition.
func brokerProps() map[string]any {
	return obj(map[string]any{
		"id":      long(),
		"address": keyword(),
		"mbean":   keyword(),
		"request": obj(map[string]any{
			"channel": obj(map[string]any{"queue": obj(map[string]any{"size": long()})}),
			"produce": obj(map[string]any{"failed_per_second": float(), "failed": float()}),
			"fetch":   obj(map[string]any{"failed_per_second": float(), "failed": float()}),
		}),
		"replication": obj(map[string]any{"leader_elections": float(), "unclean_leader_elections": float()}),
		"session": obj(map[string]any{"zookeeper": obj(map[string]any{
			"disconnect": float(), "expire": float(), "readonly": float(), "sync": float(),
		})}),
		"log": obj(map[string]any{"flush_rate": float()}),
		"topic": obj(map[string]any{
			"messages_in": float(),
			"net": obj(map[string]any{
				"in":       obj(map[string]any{"bytes_per_sec": float()}),
				"out":      obj(map[string]any{"bytes_per_sec": float()}),
				"rejected": obj(map[string]any{"bytes_per_sec": float()}),
			}),
		}),
		"messages_in": float(),
		"net": obj(map[string]any{
			"in":       obj(map[string]any{"bytes_per_sec": float()}),
			"out":      obj(map[string]any{"bytes_per_sec": float()}),
			"rejected": obj(map[string]any{"bytes_per_sec": float()}),
		}),
	})
}

// eventsMapping : un document par rebalance d'un groupe classique.
func eventsMapping() map[string]any {
	member := obj(map[string]any{"member_id": keyword(), "client_id": keyword(), "client_host": keyword(), "instance_id": keyword()})
	return base(map[string]any{
		"event": obj(map[string]any{"dataset": keyword(), "module": keyword(), "kind": keyword()}),
		"kafka": obj(map[string]any{
			"cluster": clusterProps(),
			"topic":   obj(map[string]any{"name": keyword(), "owner": keyword()}),
			"consumergroup": obj(map[string]any{
				"id": keyword(), "generation": long(), "members": integer(), "protocol_type": keyword(), "protocol": keyword(),
			}),
			"rebalance": obj(map[string]any{
				"method":              keyword(),
				"previous_generation": long(),
				"members_joined":      integer(),
				"members_left":        integer(),
				"joined":              member,
				"left":                member,
				"duration_s":          double(),
			}),
		}),
	})
}

// metricsMapping : documents aux noms Metricbeat (metricsets partition,
// consumergroup et broker) complétés par kafka.lag.* par partition.
func metricsMapping() map[string]any {
	return base(map[string]any{
		"metricset": obj(map[string]any{"name": keyword(), "period": long()}),
		"service":   obj(map[string]any{"type": keyword(), "address": keyword()}),
		"kafka": obj(map[string]any{
			"cluster": clusterProps(),
			"broker":  brokerProps(),
			"topic":   obj(map[string]any{"name": keyword(), "owner": keyword(), "error": obj(map[string]any{"code": long()})}),
			"partition": obj(map[string]any{
				"id":              long(),
				"topic_id":        keyword(),
				"topic_broker_id": keyword(),
				"offset":          obj(map[string]any{"newest": long(), "oldest": long()}),
				"partition": obj(map[string]any{
					"id":             long(),
					"leader":         long(),
					"replica":        long(),
					"insync_replica": boolean(),
					"is_leader":      boolean(),
					"error":          obj(map[string]any{"code": long()}),
				}),
			}),
			"consumergroup": obj(map[string]any{
				"id":           keyword(),
				"topic":        keyword(),
				"partition":    long(),
				"offset":       long(),
				"consumer_lag": long(),
				"meta":         keyword(),
				"broker":       obj(map[string]any{"id": long(), "address": keyword()}),
				"client":       obj(map[string]any{"id": keyword(), "host": keyword(), "member_id": keyword()}),
				"error":        obj(map[string]any{"code": long()}),
			}),
			"lag": obj(map[string]any{
				"seconds_lower":           double(),
				"seconds_upper":           double(),
				"method":                  keyword(),
				"last_commit_age_s":       double(),
				"stalled":                 boolean(),
				"retention_used":          double(),
				"commit_before_log_start": boolean(),
				"offsets_lost":            long(),
			}),
		}),
	})
}
