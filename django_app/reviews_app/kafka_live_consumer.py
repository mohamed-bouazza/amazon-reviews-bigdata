"""
Kafka Live Consumer — thread Django qui lit le topic 'predictions-live'
et forward chaque prédiction vers le Channel Layer (WebSocket).

Chemin : Spark → Kafka "predictions-live" → ce thread → group_send → WebSocket → browser
"""
from __future__ import annotations

import json
import logging
import os
import threading

logger = logging.getLogger(__name__)

_started = False
_lock    = threading.Lock()


def start_kafka_live_consumer() -> None:
    """Lance le thread consumer une seule fois (guard contre double appel)."""
    global _started
    with _lock:
        if _started:
            return
        _started = True

    t = threading.Thread(target=_run, daemon=True, name="kafka-live-consumer")
    t.start()
    logger.info("Kafka live consumer thread démarré (topic: predictions-live)")


def _run() -> None:
    """Boucle principale — tourne indéfiniment dans le thread daemon."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer
    import time

    broker = os.getenv("KAFKA_BROKER", "localhost:29092")

    # Retry loop si Kafka n'est pas encore prêt au démarrage Django
    consumer = None
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                "predictions-live",
                bootstrap_servers=broker,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",     # ignorer les anciens messages
                group_id="django-live-ws",      # group_id unique
                enable_auto_commit=True,
                consumer_timeout_ms=-1,         # bloque indéfiniment (pas de timeout)
            )
            logger.info("Kafka consumer connecté à %s (topic: predictions-live)", broker)
            break
        except NoBrokersAvailable:
            logger.warning("Kafka pas encore disponible, retry %d/10...", attempt + 1)
            time.sleep(3)
    else:
        logger.error("Impossible de se connecter à Kafka après 10 tentatives — live feed désactivé")
        return

    channel_layer = get_channel_layer()

    for msg in consumer:
        try:
            raw = msg.value  # déjà désérialisé par value_deserializer

            # Normaliser les champs (Spark écrit des noms camelCase)
            prediction = {
                "id":         str(raw.get("id", "")),
                "productId":  str(raw.get("productId", "")),
                "summary":    str(raw.get("summary", ""))[:120],
                "sentiment":  str(raw.get("sentiment", "")),
                "confidence": float(raw.get("confidence", 0.0)),
                "time":       str(raw.get("time", "")),
            }

            # Broadcaster à tous les WebSocket connectés au groupe "live_feed"
            async_to_sync(channel_layer.group_send)(
                "live_feed",
                {
                    "type":        "live.batch",
                    "predictions": [prediction],
                    "batch_id":    -1,
                },
            )
            logger.debug(
                "→ WebSocket: %s | %s | conf=%.2f",
                prediction["sentiment"],
                prediction["productId"],
                prediction["confidence"],
            )
        except Exception as exc:
            logger.warning("Erreur traitement message Kafka: %s", exc)
