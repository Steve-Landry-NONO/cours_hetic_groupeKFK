# Spotify Data Platform — Groupe KFK

## 1. Présentation du projet

Le projet **Spotify Data Platform** est un projet pédagogique réalisé dans le cadre du Mastère Data & IA à HETIC.

L'objectif est de construire progressivement une plateforme de données inspirée de Spotify, capable de gérer :

- la génération de catalogues musicaux ;
- l'ingestion batch de catalogues depuis MinIO vers PostgreSQL ;
- la simulation d'événements d'écoute en temps réel ;
- le traitement d'événements via Redis et Airflow ;
- la gestion des événements invalides via une Dead Letter Queue ;
- le calcul d'agrégats journaliers ;
- la génération de recommandations musicales ;
- la documentation et les tests du projet ;
- puis, dans une phase suivante, l'intégration Kafka et Spark Structured Streaming.

Le groupe travaille sur une branche stable dédiée : **`groupe-kfk/main`**. Toutes les Pull Requests doivent être ouvertes vers cette branche.

## 2. Membres du groupe

| Membre | Rôle principal |
|---|---|
| Steve Landry KOUOKAM NONO | Chef de groupe — coordination, intégration, revue de code, issue #7 |
| Théophane KENGNI | DAGs Airflow : catalogue, streaming, DLQ |
| Linda MAKAMTA | Simulateur P2P, pipeline de recommandations |
| Chantal CAMARA | Modèle de données, tests et documentation |

## 3. Répartition des issues

| Issue | Intitulé | Responsable | Statut |
|---|---|---|---|
| #1 | Setup Docker Compose | Steve | ✅ Terminé |
| #2 | Documentation du modèle de données (`DATA_MODEL.md`) | Chantal | ✅ Terminé |
| #3 | Data Generator | Steve | ✅ Terminé |
| #4 | DAG `catalog_ingestion_pipeline` | Théophane | ✅ Terminé |
| #5 | Simulateur P2P | Linda | ✅ Terminé |
| #6 | DAG `streaming_events_pipeline` | Théophane | ✅ Terminé |
| #7 | DAG `aggregation_pipeline` | Steve | ✅ Terminé |
| #8 | DAG `recommendation_pipeline` | Linda | ✅ Terminé |
| #9 | DAG `dlq_reprocessing_pipeline` | Théophane | ✅ Terminé |
| #10 | Tests et documentation | Chantal | ✅ Terminé |
| #11 | Kafka KRaft | À planifier | ⏳ À faire |
| #12 | Simulateur dual Redis + Kafka | À planifier | ⏳ À faire |
| #13 | Spark Structured Streaming — premier job | À planifier | ⏳ À faire |
| #14 → #25 | Kafka / Spark / inter-groupes / chaos engineering | À planifier | ⏳ À faire |

## 4. Avancement global

À ce stade, le groupe KFK a traité **10 / 25 issues**, soit environ **40 % du projet**. Toute la **Phase 1 (batch)** est fonctionnelle, de la génération des données jusqu'aux recommandations :

```text
Data Generator
    ↓
MinIO (bucket labels-raw)
    ↓
catalog_ingestion_pipeline
    ↓
PostgreSQL (catalogue : genres, artists, albums, tracks)
    ↓
P2P Simulator
    ↓
Redis DB 1 (listening_events, p2p_network_events)
    ↓
streaming_events_pipeline
    ↓
PostgreSQL (listening_events) + MinIO (Parquet)
    ↓
aggregation_pipeline
    ↓
daily_streams + artist_stats
    ↓
recommendation_pipeline
    ↓
recommendations (PostgreSQL + Redis)
```

## 5. Architecture technique

| Composant | Rôle |
|---|---|
| Docker Compose | Orchestration locale des services |
| PostgreSQL | Base relationnelle principale (base métier `spotify`) |
| Redis | Broker Celery d'Airflow (DB 0) et file d'événements temps réel (DB 1) |
| MinIO | Stockage objet compatible S3 (Parquet, checkpoints) |
| Airflow | Orchestration des pipelines batch et streaming léger |
| Faker | Génération de données réalistes |
| Python | Développement des DAGs, simulateurs et tests |
| Kafka | Phase suivante : bus d'événements distribué |
| Spark | Phase suivante : traitement distribué batch/streaming |

## 6. Structure du dépôt

```text
cours_hetic/
│
├── dags/
│   ├── catalog_ingestion_pipeline.py
│   ├── streaming_events_pipeline.py
│   ├── aggregation_pipeline.py
│   ├── recommendation_pipeline.py
│   └── dlq_reprocessing_pipeline.py
│
├── src/
│   ├── data_generator/
│   │   └── generate_catalog.py
│   └── p2p_simulator/
│       └── simulator.py
│
├── sql/
│   └── init_spotify_db.sql
│
├── docs/
│   ├── DATA_MODEL.md
│   ├── RUNBOOK.md
│   ├── TESTING.md
│   └── daily_reports/
│       ├── JOUR_1_GROUPE_KFK.md
│       ├── JOUR_2_GROUPE_KFK.md
│       └── JOUR_3_GROUPE_KFK.md
│
├── tests/
│   └── unit/
│
├── data/
│   └── labels/
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## 7. Pipelines réalisés

### 7.1 Data Generator

Le générateur produit des catalogues musicaux réalistes pour plusieurs labels.

```bash
python -m src.data_generator.generate_catalog --artists 15
```

Sortie dans `data/labels/`, ensuite uploadée dans le bucket MinIO `labels-raw`.

### 7.2 catalog_ingestion_pipeline

Lit les catalogues JSON depuis MinIO, valide les données, insère les genres, artistes, albums et tracks dans PostgreSQL, et envoie les données invalides en DLQ.

Tables concernées : `genres`, `artists`, `albums`, `tracks`, `dead_letter_events`.

### 7.3 P2P Simulator

Fichier `src/p2p_simulator/simulator.py`. Génère des événements d'écoute et des événements réseau P2P réalistes, récupère les vrais `track_id` depuis PostgreSQL, et pousse les événements dans Redis DB 1 avec `LPUSH`.

```bash
python -m src.p2p_simulator.simulator --peers 10 --rate 5
```

Clés Redis utilisées : `listening_events`, `p2p_network_events`.

### 7.4 streaming_events_pipeline

Consomme les événements depuis Redis DB 1 (`RPOP`), les valide, les enrichit avec le catalogue PostgreSQL, écrit les événements enrichis en Parquet dans MinIO, insère les événements valides dans PostgreSQL, et envoie les invalides en DLQ.

Table principale : `listening_events`.

### 7.5 aggregation_pipeline

Calcule les agrégats journaliers par track (top 50), les statistiques journalières par artiste, et des métriques simples sur les événements P2P. Dépend de `streaming_events_pipeline` via un `ExternalTaskSensor`.

Tables alimentées : `daily_streams`, `artist_stats`.

### 7.6 recommendation_pipeline

Génère des recommandations musicales personnalisées à partir des genres et artistes écoutés, pondère par la popularité issue de `daily_streams`, exclut les tracks déjà écoutées, insère les recommandations dans PostgreSQL et les stocke dans Redis avec TTL.

Table alimentée : `recommendations`.

### 7.7 dlq_reprocessing_pipeline

Récupère les événements invalides en statut `pending`, corrige certaines erreurs simples, réinsère les événements valides dans `listening_events`, et passe les événements non retraitables en `retry` puis `abandoned`.

Table concernée : `dead_letter_events`.

## 8. Lancement du projet

### 8.1 Préparer l'environnement

```bash
cp .env.example .env

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

### 8.2 Lancer les services Docker

```bash
docker compose up -d
docker compose ps
```

| Service | URL / Port |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

## 9. Commandes de validation utiles

**Vérifier Airflow** (aucune erreur d'import attendue) :

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

**Vérifier PostgreSQL** (base métier `spotify`, user `spotify`) :

```bash
docker compose exec postgres psql -U spotify -d spotify -c "\dt"
```

**Vérifier Redis** (réponse attendue : `PONG`) :

```bash
docker compose exec redis redis-cli ping
```

**Vérifier la file d'événements (Redis DB 1)** :

```bash
docker compose exec redis redis-cli -n 1 LLEN listening_events
```

## 10. Tests unitaires

```bash
pytest -q                              # tous les tests
pytest tests/unit/ -v                  # détail des tests unitaires
```

La documentation de test est disponible dans `docs/TESTING.md`.

## 11. Rapports journaliers

| Jour | Rapport |
|---|---|
| Jour 1 | `docs/daily_reports/JOUR_1_GROUPE_KFK.md` |
| Jour 2 | `docs/daily_reports/JOUR_2_GROUPE_KFK.md` |
| Jour 3 | `docs/daily_reports/JOUR_3_GROUPE_KFK.md` |

## 12. Règles Git du groupe

- Branche stable : `groupe-kfk/main`
- Convention de branches : `groupe-kfk/feat/issue-X-description`
- Travailler sur une branche dédiée, faire des commits réguliers (toutes les 30–45 min)
- Ouvrir une Pull Request vers `groupe-kfk/main` et faire relire avant merge
- Ne jamais committer `.env`, `.venv`, `__pycache__`, `.pytest_cache` ni les fichiers générés

## 13. Prochaines étapes — Phase 2 (Kafka + Spark)

| Issue | Sujet | Objectif |
|---|---|---|
| #11 | Kafka KRaft | Cluster Kafka sans Zookeeper |
| #12 | Simulateur dual Redis + Kafka | Publier les événements vers Redis et Kafka |
| #13 | Spark Structured Streaming | Lire les événements Kafka avec Spark |
| #14 | Fenêtres temporelles | Tendances sur fenêtres de temps |
| #15 | Watermark | Gérer les événements en retard |
| #16 | Exactly-once | Fiabiliser le traitement streaming |
| #17 → #25 | Enrichissement, fraude, inter-groupes, chaos engineering | Finalisation avancée |

## 14. État actuel

Le socle principal de la plateforme est opérationnel : génération des données, ingestion catalogue, simulation d'événements, traitement Redis, stockage PostgreSQL, stockage objet MinIO, agrégations, recommandations, DLQ, tests et documentation. Le projet est prêt pour la phase suivante : **Kafka + Spark Structured Streaming**.
