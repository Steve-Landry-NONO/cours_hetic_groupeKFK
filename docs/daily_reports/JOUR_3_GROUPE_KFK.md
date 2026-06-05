# Rapport journalier — Jour 3 — Groupe KFK

## 1. Issues fermées

- #5 — Simulateur P2P
- #7 — DAG `aggregation_pipeline`
- #8 — DAG `recommendation_pipeline`
- #10 — Tests et documentation

## 2. En cours / non fermées

Aucune issue de la phase batch principale n’est bloquante à ce stade.

Les prochaines issues à préparer concernent la phase Kafka / Spark :

- #11 — Kafka KRaft
- #12 — Simulateur dual Redis + Kafka
- #13 — Premier job Spark Structured Streaming

## 3. Difficultés rencontrées

### Issue #5 — Simulateur P2P

Le simulateur P2P générait initialement des `track_id` aléatoires.

Problème :
- Les morceaux générés n’existaient pas dans PostgreSQL.
- Le DAG `streaming_events_pipeline` risquait donc d’envoyer les événements en DLQ.

Correction :
- Le simulateur récupère maintenant les vrais `track_id` depuis PostgreSQL.
- Les événements sont poussés dans Redis DB 1 avec `LPUSH`.
- Un mode de simulation finie a été ajouté avec l’option `--events`.

### Issue #8 — Recommendation pipeline

Le DAG de recommandation générait initialement 0 recommandation.

Problème :
- La première logique filtrait trop fortement les utilisateurs.
- Les utilisateurs avec moins de 3 tracks écoutées étaient exclus.

Correction :
- Le filtre a été supprimé.
- Une logique de recommandation basée sur les genres, les artistes et la popularité des tracks a été mise en place.
- Les tracks déjà écoutées par l’utilisateur sont exclues.
- Les recommandations sont insérées dans PostgreSQL et stockées dans Redis avec TTL.

### Issue #10 — Tests et documentation

L’issue #10 a permis de renforcer la stabilité du projet par :
- l’ajout de tests ;
- la documentation des commandes de validation ;
- la formalisation du RUNBOOK et des procédures de test.

## 4. Objectif demain

- Mettre à jour le `README.md` principal du projet.
- Ajouter les liens vers les rapports journaliers.
- Documenter clairement l’architecture actuelle du groupe KFK.
- Préparer la phase Kafka / Spark :
  - #11 Kafka KRaft ;
  - #12 Simulateur dual Redis + Kafka ;
  - #13 Spark Structured Streaming.
- Vérifier que toutes les branches mergées sont bien propres par rapport à `groupe-kfk/main`.

## 5. Répartition du travail

| Membre | Tâches réalisées / statut |
|---|---|
| Steve | Coordination, issue #7, audit des issues #5 et #8, validation des PR, suivi Git |
| Linda | Issue #5 simulateur P2P, issue #8 recommendation pipeline |
| Chantal | Issue #10 tests et documentation |
| Théophane | Contributions précédentes sur les DAGs catalogue, streaming et DLQ |

## 6. Validations techniques

### Issue #5 — Simulateur P2P

Validations réalisées :

- Compilation Python du simulateur : OK
- Récupération de 500 tracks depuis PostgreSQL : OK
- Génération de 20 événements : OK
- 16 événements d’écoute envoyés dans Redis DB 1 : OK
- 4 événements réseau P2P générés : OK
- Vérification Redis avec `LLEN` et `LRANGE` : OK

### Issue #7 — Aggregation pipeline

Validations réalisées :

- Import Airflow sans erreur : OK
- Exécution du DAG `aggregation_pipeline` : OK
- Insertion dans `daily_streams` : OK
- Insertion dans `artist_stats` : OK
- Calcul des métriques P2P : OK

### Issue #8 — Recommendation pipeline

Validations réalisées :

- Import Airflow sans erreur : OK
- Logique de recommandation corrigée : OK
- Recommandations basées sur :
  - genres écoutés ;
  - artistes écoutés ;
  - popularité issue de `daily_streams` ;
  - exclusion des tracks déjà écoutées.
- Insertion prévue dans `recommendations` avec upsert idempotent.
- Stockage Redis avec TTL.

### Issue #10 — Tests et documentation

Validations réalisées :

- Documentation de lancement du projet : OK
- Documentation des tests : OK
- Ajout ou structuration des tests Python : OK
- Formalisation des commandes de validation : OK

## 7. État d’avancement global

Issues traitées à ce stade :

- #1 — Setup Docker Compose
- #2 — DATA_MODEL.md
- #3 — Data Generator
- #4 — catalog_ingestion_pipeline
- #5 — p2p_simulator
- #6 — streaming_events_pipeline
- #7 — aggregation_pipeline
- #8 — recommendation_pipeline
- #9 — dlq_reprocessing_pipeline
- #10 — tests et documentation

Total :

```text
10 / 25 issues traitées

Avancement estimé :

40 %
8. Conclusion du jour

Le groupe KFK a terminé la première grande phase du projet.

Le socle batch et streaming Redis est maintenant fonctionnel :

Data Generator
    ↓
MinIO
    ↓
catalog_ingestion_pipeline
    ↓
PostgreSQL catalogue
    ↓
P2P Simulator
    ↓
Redis DB 1
    ↓
streaming_events_pipeline
    ↓
PostgreSQL listening_events + MinIO Parquet
    ↓
aggregation_pipeline
    ↓
daily_streams + artist_stats
    ↓
recommendation_pipeline
    ↓
recommendations

La prochaine étape est de consolider le README.md, puis d’attaquer la phase Kafka / Spark.
