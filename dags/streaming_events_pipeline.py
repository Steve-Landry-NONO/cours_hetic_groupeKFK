"""
DAG : streaming_events_pipeline
================================

Consomme les événements d'écoute depuis Redis en micro-batch,
les valide, les enrichit avec le catalogue PostgreSQL, puis les stocke
dans PostgreSQL et en Parquet sur MinIO.

Décision technique Groupe KFK :
    Pour Airflow, on utilise Redis comme file de messages PERSISTANTE :
        simulateur → LPUSH listening_events / p2p_network_events
        Airflow    → RPOP  listening_events / p2p_network_events

Pourquoi pas Redis Pub/Sub ici ?
    Le Pub/Sub Redis est éphémère : si le DAG Airflow n'écoute pas au moment
    exact de la publication, l'événement est perdu. Une liste Redis permet
    de stocker les messages jusqu'au passage du DAG micro-batch.

    ⚠️ Dépendance issue #5 : le simulateur doit publier avec LPUSH dans les
       clés Redis `listening_events` et `p2p_network_events` (en plus du
       redis.publish() existant si on veut garder `redis-cli subscribe`).

Architecture :
    Redis lists
        → consume_from_redis()
        → validate_events()
        → enrich_events()
        → store_to_parquet()
        → upsert_to_postgres()
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook


# ── Connexions Airflow (définies dans docker-compose / airflow-init) ──────────
POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CONN_ID = "spotify_redis"

# ── Clés Redis (= noms des canaux TOPICS du simulateur) ───────────────────────
LISTENING_EVENTS_KEY = "listening_events"
P2P_NETWORK_EVENTS_KEY = "p2p_network_events"

# ── MinIO (via boto3, variables d'env passées aux conteneurs Airflow) ─────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
PARQUET_BUCKET = "spotify-parquet"

MAX_EVENTS_PER_RUN = 1000


DAG_DOC = """
## streaming_events_pipeline

### Rôle

Ce DAG consomme les événements générés par le simulateur P2P depuis Redis,
les valide, les enrichit avec le catalogue PostgreSQL, puis les stocke dans :

- `listening_events` dans PostgreSQL ;
- bucket `spotify-parquet` dans MinIO ;
- `dead_letter_events` pour les événements invalides.

### Source Redis

Pour ce DAG, Redis est utilisé sous forme de listes persistantes :

- `listening_events`
- `p2p_network_events`

Le simulateur doit donc publier avec `LPUSH`, et le DAG consomme avec `RPOP`.

### Idempotence

Chaque événement d'écoute possède un identifiant `event_id`, inséré dans la
colonne SQL `listening_events.id`. L'insertion utilise :

```sql
ON CONFLICT (id) DO NOTHING
```

Ainsi, relancer le DAG ne crée pas de doublons.
"""


DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def get_minio_client():
    """Client boto3 pointant vers MinIO (S3-compatible)."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def insert_dlq(
    hook: PostgresHook,
    payload: dict[str, Any],
    error_type: str,
    error_message: str,
    original_topic: str,
) -> None:
    """Insère un événement invalide dans la Dead Letter Queue."""
    hook.run(
        """
        INSERT INTO dead_letter_events (
            original_topic,
            payload,
            error_type,
            error_message,
            status
        )
        VALUES (%s, %s::jsonb, %s, %s, 'pending')
        """,
        parameters=(
            original_topic,
            json.dumps(payload, ensure_ascii=False, default=str),
            error_type,
            error_message,
        ),
    )


def decode_redis_message(message: bytes | str) -> dict[str, Any]:
    """Convertit un message Redis en dictionnaire Python."""
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    return json.loads(message)


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis() -> dict[str, list[dict[str, Any]]]:
        """
        Consomme les événements accumulés dans Redis avec RPOP (FIFO :
        on traite les messages les plus anciens). Le nombre de messages par
        run est plafonné pour ne pas monopoliser le worker.
        """
        redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
        redis_conn = redis_hook.get_conn()

        events: dict[str, list[dict[str, Any]]] = {
            "listening": [],
            "p2p_network": [],
        }

        keys = {
            LISTENING_EVENTS_KEY: "listening",
            P2P_NETWORK_EVENTS_KEY: "p2p_network",
        }

        for redis_key, output_key in keys.items():
            consumed = 0
            while consumed < MAX_EVENTS_PER_RUN:
                message = redis_conn.rpop(redis_key)
                if not message:
                    break
                try:
                    events[output_key].append(decode_redis_message(message))
                except Exception as exc:
                    events[output_key].append(
                        {"raw_message": str(message), "decode_error": str(exc)}
                    )
                consumed += 1

        print(
            "📥 Redis micro-batch consommé | "
            f"listening={len(events['listening'])} "
            f"p2p={len(events['p2p_network'])}"
        )
        return events

    @task(task_id="validate_events")
    def validate_events(raw_events: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        """
        Valide les événements d'écoute. Les événements P2P réseau sont comptés
        pour information mais pas insérés (aucune table dédiée dans le schéma).
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        valid_listening: list[dict[str, Any]] = []
        invalid_count = 0

        required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        for event in raw_events.get("listening", []):
            missing = [f for f in required_fields if not event.get(f)]
            if missing:
                invalid_count += 1
                insert_dlq(
                    hook=pg_hook,
                    payload=event,
                    error_type="validation_error",
                    error_message=f"Champs manquants : {missing}",
                    original_topic=LISTENING_EVENTS_KEY,
                )
                continue

            try:
                duration_ms = int(event["duration_ms"])
            except (TypeError, ValueError):
                invalid_count += 1
                insert_dlq(
                    hook=pg_hook,
                    payload=event,
                    error_type="validation_error",
                    error_message="duration_ms invalide",
                    original_topic=LISTENING_EVENTS_KEY,
                )
                continue

            if duration_ms <= 0:
                invalid_count += 1
                insert_dlq(
                    hook=pg_hook,
                    payload=event,
                    error_type="validation_error",
                    error_message="duration_ms doit être strictement positif",
                    original_topic=LISTENING_EVENTS_KEY,
                )
                continue

            event["duration_ms"] = duration_ms
            valid_listening.append(event)

        p2p_count = len(raw_events.get("p2p_network", []))

        print(
            "✅ Validation terminée | "
            f"valides={len(valid_listening)} "
            f"DLQ={invalid_count} p2p={p2p_count}"
        )
        return {
            "valid_listening": valid_listening,
            "p2p_network_count": p2p_count,
            "errors_count": invalid_count,
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict[str, Any]) -> dict[str, Any]:
        """Enrichit les events avec le catalogue : event.track_id → tracks.id."""
        listening_events = validated["valid_listening"]

        if not listening_events:
            return {
                "events": [],
                "errors_count": validated.get("errors_count", 0),
                "missing_catalog_count": 0,
                "p2p_network_count": validated.get("p2p_network_count", 0),
            }

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        track_ids = sorted({str(e["track_id"]) for e in listening_events})
        placeholders = ",".join(["%s"] * len(track_ids))

        catalog_df = pg_hook.get_pandas_df(
            f"""
            SELECT id::text AS track_id,
                   title    AS track_title,
                   artist_id::text AS artist_id,
                   genre
            FROM tracks
            WHERE id::text IN ({placeholders})
            """,
            parameters=tuple(track_ids),
        )

        events_df = pd.DataFrame(listening_events)
        events_df["track_id"] = events_df["track_id"].astype(str)

        enriched_df = events_df.merge(catalog_df, on="track_id", how="inner")
        missing_catalog_count = len(events_df) - len(enriched_df)

        if missing_catalog_count:
            missing_events = events_df[
                ~events_df["track_id"].isin(catalog_df["track_id"])
            ].to_dict(orient="records")
            for event in missing_events:
                insert_dlq(
                    hook=pg_hook,
                    payload=event,
                    error_type="catalog_missing_track",
                    error_message="track_id absent du catalogue PostgreSQL",
                    original_topic=LISTENING_EVENTS_KEY,
                )

        enriched_events = enriched_df.to_dict(orient="records")

        print(
            "✅ Enrichissement terminé | "
            f"enrichis={len(enriched_events)} DLQ_track_inconnu={missing_catalog_count}"
        )
        return {
            "events": enriched_events,
            "errors_count": validated.get("errors_count", 0) + missing_catalog_count,
            "missing_catalog_count": missing_catalog_count,
            "p2p_network_count": validated.get("p2p_network_count", 0),
        }

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched: dict[str, Any], **context) -> dict[str, Any]:
        """Sauvegarde les events enrichis en Parquet sur MinIO (partition date/hour)."""
        enriched_events = enriched["events"]
        if not enriched_events:
            return {"parquet_path": None, "written": 0}

        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        first_ts = df["timestamp"].iloc[0]
        date_str = first_ts.strftime("%Y-%m-%d")
        hour_str = first_ts.strftime("%H")

        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        run_id = context["run_id"].replace(":", "-").replace("+", "_")
        file_key = (
            f"listening_events/date={date_str}/hour={hour_str}/events_{run_id}.parquet"
        )

        client = get_minio_client()
        client.put_object(
            Bucket=PARQUET_BUCKET,
            Key=file_key,
            Body=buffer.getvalue(),
        )

        print(f"✅ Parquet écrit : s3://{PARQUET_BUCKET}/{file_key} ({len(df)} lignes)")
        return {"parquet_path": file_key, "written": len(df)}

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched: dict[str, Any]) -> dict[str, int]:
        """Insère les events enrichis dans listening_events (event_id → colonne id)."""
        enriched_events = enriched["events"]
        if not enriched_events:
            return {"inserted": 0, "errors_count": enriched.get("errors_count", 0)}

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = [
            (
                event["event_id"],
                event["user_id"],
                event["track_id"],
                event.get("source_peer_id") or event.get("source_peer"),
                event["timestamp"],
                event["duration_ms"],
                event.get("device_type"),
                event.get("geo_country"),
                bool(event.get("completed", False)),
                event.get("event_source", "p2p"),
            )
            for event in enriched_events
        ]

        sql = """
            INSERT INTO listening_events (
                id, user_id, track_id, source_peer_id, timestamp,
                duration_ms, device_type, geo_country, completed, event_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        conn = pg_hook.get_conn()
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()

        print(f"✅ Insérés dans listening_events : {len(rows)}")
        return {"inserted": len(rows), "errors_count": enriched.get("errors_count", 0)}

    # ── Orchestration ────────────────────────────────────────────────────────
    raw = consume_from_redis()
    validated = validate_events(raw)
    enriched = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
