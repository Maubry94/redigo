# Redigo

Une implémentation miniature de Redis en Go avec des fonctionnalités avancées.

## Fonctionnalités

- **Base de données clé-valeur** : Opérations de base SET/GET/DELETE
- **Support TTL** : Expiration automatique des clés
- **AOF (Append Only File)** : Journalisation des commandes pour garantir la durabilité
- **Snapshots** : Sauvegardes ponctuelles de la base de données
- **Index inversés** : Recherche rapide par valeur et par motifs de clés
- **Accès concurrent** : Opérations thread-safe grâce à des verrous (mutex)

## Commandes disponibles

### Opérations de base

- `SET {clé} {valeur} {ttl}` - Enregistre une paire clé-valeur avec un TTL optionnel
- `GET {clé}` - Récupère la valeur associée à une clé
- `DELETE {clé}` - Supprime une paire clé-valeur
- `TTL {clé}` - Affiche le temps restant avant l’expiration de la clé
- `EXPIRE {clé}` secondes - Définit un temps d’expiration pour une clé

### Opérations de recherche (Index inversés)

- `SEARCHVALUE {valeur}` - Trouve toutes les clés associées à cette valeur
- `SEARCHPREFIX {préfixe}` - Trouve toutes les clés commençant par ce préfixe
- `SEARCHSUFFIX {suffixe}` - Trouve toutes les clés finissant par ce suffixe
- `SEARCHCONTAINS {sous-chaîne}` - Trouve toutes les clés contenant cette sous-chaîne

### Persistance

- `SAVE` - Crée une sauvegarde immédiate (snapshot)
- `BGSAVE` - Crée une sauvegarde en arrière-plan

## Architecture

Le projet implémente des fonctionnalités similaires à Redis avec :

- **Stockage en mémoire** protégé contre les accès concurrents
- **Index inversés** pour des recherches efficaces par valeur ou par motif
- **Journalisation AOF** pour garantir la durabilité des commandes
- **Snapshots périodiques** pour la persistance des données
- **Expiration automatique** pour la gestion du TTL

## Utilisation

- Démarre le serveur : `go run cmd/redigo/main.go`
- Connecte-toi via TCP sur le port 6380 (configurable)
- Envoie des commandes en texte brut

Example:

```text
SET user:1 "john" 3600
GET user:1
SEARCHVALUE "john"
SEARCHPREFIX "user:"
```

## Configuration

Le projet utilise un fichier `.env` situé à la racine pour permettre la personnalisation de plusieurs paramètres.

### Paramètres configurables

Vous pouvez y modifier :

- Le **port** du serveur
Permet de définir sur quel port le serveur TCP écoute les connexions (par défaut : 6380).
- **L’intervalle de sauvegarde automatique** (snapshots)
Définit la fréquence à laquelle des snapshots de la base sont créés (ex. 2m pour 2 minutes).
- **L’intervalle de vidage du buffer AOF**
Contrôle à quelle fréquence le buffer des commandes est écrit dans le fichier AOF (ex. 1m).
- **L’intervalle de traitement des expirations**
Détermine la fréquence à laquelle les clés expirées sont vérifiées et supprimées (ex. 5s).
- **La durée de vie (TTL)** par défaut des clés
Si aucun TTL n’est précisé lors d’un SET, cette valeur est utilisée (0 = pas d’expiration).
- **Le répertoire racine** des fichiers de Redigo
Permet de spécifier le chemin parent où seront stockés les fichiers de persistance (AOF, snapshots). Par défaut : ~/.redigo.

### Configuration par défaut

```yaml
REDIGO_PORT="6380"

# Time intervals
SNAPSHOT_SAVE_INTERVAL=2m
FLUSH_BUFFER_INTERVAL=1m
DATA_EXPIRATION_INTERVAL=5s

# Default TTL for keys when not specified (0 = no expiration)
DEFAULT_TTL=0

# Optional paths (defaults to ~/.redigo if not specified)
REDIGO_ROOT_DIR_PATH=
```
