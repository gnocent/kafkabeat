// Package recette automatise les critères d'acceptation de DESIGN.md (section
// « Validation ») sur le banc local (make bench-up). Les tests portent le tag
// de build « bench » et s'enchaînent par make recette ; chacun se lance aussi seul (go test -tags
// bench -run TestC3 ./recette/), sauf TestC15TimeSeries/volume, qui mesure le
// volume sur les documents laissés par TestC9 et se déclare ignoré sans eux.
//
// Variables : KB_KAFKA39 et KB_KAFKA44 (brokers), KB_ES, KB_ES9, KB_KIBANA et
// KB_KIBANA9 (piles Elastic 8.19 et 9.5), GO (binaire go), KB_LONG=1 (scénario
// de 2 h du critère 1 au lieu de 3 min), KB_NO_SANDBOX=1 (Chromium sans bac à
// sable pour le rendu des dashboards). Le banc reste local pour autant : les
// critères 3 et 13 mettent en pause le conteneur kafka39 ou kafka44 (docker
// pause), le critère 11 lit les journaux de Metricbeat (docker exec) et attend
// les agents Jolokia sur localhost:8778 et 8779.
//
// Écarts avec le texte des critères, pour tenir en une session : critère 2
// vérifié sur 40 s au lieu de 30 min, 2 bis avec un silence de 60 s au lieu de 10 min,
// 4 avec un arrêt de 15 s au lieu de 5 min et Elasticsearch toujours joignable
// (l'agent n'a aucun chemin de relecture depuis Elasticsearch) ; seul le
// critère 1 a un mode long (KB_LONG). Le critère 5 se déclare ignoré si le
// broker refuse l'injection ; la moitié « 30 000 partitions » du critère 6 est
// mesurée par make load, hors recette.
//
// Les topics et groupes créés sont supprimés en fin de test ; les documents
// écrits dans Elasticsearch restent jusqu'à l'expiration ILM (make bench-down
// pour repartir à neuf).
package recette
