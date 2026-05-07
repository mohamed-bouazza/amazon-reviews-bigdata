"""WebSocket consumers (Django Channels)."""
from __future__ import annotations

import asyncio
import json
import logging

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from . import mongo_client as mongo

logger = logging.getLogger(__name__)

LIVE_GROUP = "live_feed"


class LiveFeedConsumer(AsyncWebsocketConsumer):
    """
    Reçoit les prédictions en temps réel depuis Spark via Channel Layer.
    Spark → HTTP POST /api/internal/live-push → group_send → ici → browser.
    Aussi garde un fallback poll MongoDB toutes les 10s si Spark ne pousse pas.
    """

    FALLBACK_INTERVAL = 10.0  # secondes entre polls MongoDB de fallback

    async def connect(self) -> None:
        """Accepter la connexion et rejoindre le groupe live_feed."""
        await self.accept()

        # Rejoindre le groupe — recevra les pushs de Spark
        await self.channel_layer.group_add(LIVE_GROUP, self.channel_name)

        self._last_id: str | None = None
        self._stop = asyncio.Event()

        # Envoyer l'historique initial depuis MongoDB
        try:
            initial = await database_sync_to_async(mongo.get_recent_predictions)(20, None)
        except mongo.MongoUnavailable as exc:
            await self.send(text_data=json.dumps({"type": "error", "detail": str(exc)}))
            initial = []

        if initial:
            self._last_id = initial[0]["_id"]

        counters = await self._counters()
        await self.send(text_data=json.dumps({
            "type":        "init",
            "predictions": initial,
            "counters":    counters,
        }))

        # Fallback poll si Spark ne pousse pas (MongoDB polling)
        self._task = asyncio.create_task(self._fallback_loop())

    async def disconnect(self, code: int) -> None:
        """Quitter le groupe et arrêter le fallback."""
        await self.channel_layer.group_discard(LIVE_GROUP, self.channel_name)
        if hasattr(self, "_stop"):
            self._stop.set()
        task = getattr(self, "_task", None)
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def receive(self, text_data: str | None = None, bytes_data: bytes | None = None) -> None:
        """Ping → Pong keep-alive."""
        if not text_data:
            return
        try:
            msg = json.loads(text_data)
            if msg.get("type") == "ping":
                await self.send(text_data=json.dumps({"type": "pong"}))
        except json.JSONDecodeError:
            pass

    # ── Handler appelé par group_send depuis api_views.live_push ──────────
    async def live_batch(self, event: dict) -> None:
        """
        Reçoit un batch directement depuis Spark (via HTTP POST → group_send).
        C'est le chemin 'Visualisation en continu' du schéma prof.
        Pas de lecture MongoDB ici — données directes de Spark.
        """
        predictions = event.get("predictions", [])
        if predictions and predictions[0].get("_id"):
            self._last_id = predictions[0]["_id"]

        counters = await self._counters()
        await self.send(text_data=json.dumps({
            "type":        "batch",
            "predictions": predictions,
            "counters":    counters,
            "source":      "spark_direct",  # indique que c'est un push Spark direct
        }))

    # ── Fallback : poll MongoDB si Spark n'a pas encore pushé ─────────────
    async def _fallback_loop(self) -> None:
        """Poll MongoDB toutes les 10s — utilisé seulement si Spark ne pousse pas."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.FALLBACK_INTERVAL)
                docs = await database_sync_to_async(mongo.get_predictions_after)(
                    self._last_id, 20
                )
                if docs:
                    self._last_id = docs[0]["_id"]
                    counters = await self._counters()
                    await self.send(text_data=json.dumps({
                        "type":        "batch",
                        "predictions": docs,
                        "counters":    counters,
                        "source":      "mongo_fallback",
                    }))
            except asyncio.CancelledError:
                break
            except mongo.MongoUnavailable as exc:
                await self.send(text_data=json.dumps({"type": "error", "detail": str(exc)}))
                await asyncio.sleep(5)
            except Exception as exc:
                logger.exception("LiveFeed fallback loop error: %s", exc)
                await asyncio.sleep(5)

    async def _counters(self) -> dict:
        """Distribution globale depuis MongoDB (pour les compteurs)."""
        try:
            return await database_sync_to_async(mongo.get_global_distribution)()
        except mongo.MongoUnavailable:
            return {"positive": 0, "neutral": 0, "negative": 0}
