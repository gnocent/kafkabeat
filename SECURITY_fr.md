*(English version: [SECURITY.md](SECURITY.md))*

# Sécurité

## Signaler une vulnérabilité

Ne pas ouvrir d'issue publique. Utiliser le signalement privé de GitHub (onglet **Security** → **Report a vulnerability**
du dépôt [gnocent/kafkabeat](https://github.com/gnocent/kafkabeat)) en décrivant l'impact, la version
(`kafkabeat -version`) et, si possible, une reproduction sur le banc de test (`make bench-up`).

Le projet est maintenu sur le temps libre de son auteur : accusé de réception sous une quinzaine de jours au mieux, sans
engagement de délai de correction. Seule la dernière version publiée est suivie.

## Ce que l'agent touche

- **Kafka** : métadonnées, offsets de fin et de début, consumer groups et offsets commités ; en mode `hybrid`, lecture du
  seul topic interne `__consumer_offsets`. Aucun topic applicatif n'est lu ; rien n'est écrit dans Kafka.
- **Elasticsearch** : écriture en bulk dans les data streams `<prefix>-*`, et installation de leurs templates et
  politiques ILM au démarrage (désactivable : `setup: false`).
- **Jolokia** (option) : lecture de MBeans sur chaque broker, en HTTP ou HTTPS, avec Basic Auth.
- **Disque** : son répertoire d'état (`state.path`), fichiers en `0640`.

Les identifiants ne sont lus que dans le fichier de configuration (à garder en `0600` ; l'unité systemd livrée le remet au
service par `LoadCredential`) et ne sont jamais journalisés. TLS 1.2 au minimum ; aucune option ne désactive la
vérification des certificats.
