# Passer au Vrai Real-Time : 3 Options

> Actuellement : `processingTime="10 seconds"` → batch de ~47 reviews toutes les 10s  
> Objectif : chaque review affichée dès qu'elle est prédite (~1s de latence)

---

## Option A — Le plus simple : trigger 1 seconde (recommandé)

**1 seul changement dans `spark_streaming/consumer.py` :**

```python
# Ligne 180 — changer UNIQUEMENT ceci :
.trigger(processingTime="10 seconds")
# →
.trigger(processingTime="1 second")
```

**Résultat :**
- Spark traite les messages toutes les 1 seconde
- Avec producer delay=0.5s → chaque batch = 1-2 reviews
- Latence bout-en-bout : **1-2 secondes**
- Aucun autre composant à ajouter

**Inconvénient :** MongoDB reçoit beaucoup plus d'appels (1 insert/s au lieu de 1 insert/10s). Pas un problème en dev.

**Ce que tu dis au prof :**  
*"On peut réduire la latence à 1 seconde en changeant le trigger de 10s à 1s — chaque micro-batch contient alors 1-2 reviews au lieu de 47, ce qui donne un affichage quasi-instantané."*

---

## Option B — Vrai real-time avec `foreach` row-by-row (sans Redis)

### Architecture

```
Spark transform() → ForeachWriter.process(row) appelé pour CHAQUE ligne
                          │
                    MongoDB insert_one()   ← 1 doc à la fois
                          │
                    HTTP POST /api/internal/live-push   ← 1 review à la fois
                          │
                    WebSocket → browser   ← apparaît immédiatement
```

### Code complet — remplace la fin de `consumer.py`

```python
# ============ REAL-TIME : ForeachWriter (1 review à la fois) ============
import requests
from pymongo import MongoClient
from datetime import datetime

class RealTimeWriter:
    """Écrit et push chaque review individuellement dès qu'elle est prédite."""

    def open(self, partition_id, epoch_id):
        """Appelé une fois au démarrage de chaque partition."""
        mongo_uri = os.getenv("MONGO_URI") or "mongodb://localhost:27018"
        if "27017" in mongo_uri or "mongo:" in mongo_uri:
            mongo_uri = "mongodb://localhost:27018"
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.collection = self.client["reviews_db"]["predictions"]
        print(f"[Partition {partition_id}] MongoDB connecté")
        return True  # IMPORTANT : doit retourner True pour continuer

    def process(self, row):
        """Appelé pour CHAQUE review individuellement."""
        # Construire le document MongoDB
        doc = {
            "Id":             row["Id"],
            "ProductId":      row["ProductId"],
            "UserId":         row["UserId"],
            "Time":           row["Time"],
            "Summary":        row["Summary"],
            "Text":           row["Text"],
            "cleaned":        row["cleaned"],
            "true_sentiment": row["true_sentiment"],
            "prediction":     float(row["prediction"]),
            "sentiment_label": row["sentiment_label"],
            "confidence":     float(row["confidence"]),
            "inserted_at":    datetime.utcnow(),
        }

        # 1. Écrire dans MongoDB (1 doc à la fois)
        self.collection.insert_one(doc)

        # 2. Push immédiat vers Django WebSocket
        try:
            requests.post(
                "http://localhost:8000/api/internal/live-push",
                json={
                    "predictions": [{
                        "id":         str(row["Id"]),
                        "productId":  str(row["ProductId"]),
                        "summary":    str(row["Summary"])[:120],
                        "sentiment":  str(row["sentiment_label"]),
                        "confidence": float(row["confidence"]),
                        "time":       str(row["Time"]),
                    }],
                    "batch_id": -1,  # pas de batch_id en mode foreach
                },
                timeout=2,
            )
        except Exception as e:
            print(f"[RealTime] WebSocket push failed: {e}")

    def close(self, error):
        """Appelé à la fin — fermer les connexions."""
        if hasattr(self, 'client'):
            self.client.close()
        if error:
            print(f"[RealTime] Erreur: {error}")


# ============ LANCER LE STREAMING REAL-TIME ============
query = (output.writeStream
    .foreach(RealTimeWriter())       # ← foreach, pas foreachBatch
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 second")  # ← trigger rapide
    .start())

print("🚀 Real-Time streaming démarré (1 review à la fois)...")
query.awaitTermination()
```

**Latence :** dès qu'une review est prédite par Spark, elle apparaît dans le navigateur. Typiquement **< 2 secondes** après l'envoi dans Kafka.

**Inconvénient :** `insert_one()` au lieu de `insert_many()` → plus lent pour de gros volumes (100k+ reviews). Pour notre cas (test set), pas de problème.

---

## Option C — Architecture Redis Pub/Sub (production-grade)

### Pourquoi Redis ?

Le problème fondamental de l'Option B : chaque `process()` fait un HTTP POST vers Django. Si Spark a 10 partitions qui tournent en parallèle, ça fait 10 connexions HTTP simultanées vers Django. Redis résout ça proprement.

### Architecture avec Redis

```
Spark ForeachWriter.process(row)
        │
        └── Redis PUBLISH channel "live_predictions" JSON   ← ultra rapide (RAM)
                │
        Django RedisSubscriber (thread séparé)
                │
                └── channel_layer.group_send("live_feed", ...)
                        │
                    LiveFeedConsumer.live_batch()
                        │
                    WebSocket → Navigateur
```

### Étape 1 : Installer Redis

```bash
# Ubuntu/WSL
sudo apt install redis-server
sudo service redis-server start

# Python
pip install redis
```

### Étape 2 : Spark publie dans Redis — `spark_streaming/consumer.py`

```python
import redis
import json
from datetime import datetime

class RedisRealTimeWriter:
    """Publie chaque prédiction dans Redis Pub/Sub."""

    def open(self, partition_id, epoch_id):
        # Connexion Redis locale
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        # Connexion MongoDB pour stockage permanent
        mongo_uri = os.getenv("MONGO_URI") or "mongodb://localhost:27018"
        if "27017" in mongo_uri or "mongo:" in mongo_uri:
            mongo_uri = "mongodb://localhost:27018"
        self.mongo = MongoClient(mongo_uri)["reviews_db"]["predictions"]
        return True

    def process(self, row):
        doc = {
            "Id":             row["Id"],
            "ProductId":      row["ProductId"],
            "Summary":        row["Summary"],
            "sentiment_label": row["sentiment_label"],
            "confidence":     float(row["confidence"]),
            "Time":           row["Time"],
            "inserted_at":    datetime.utcnow().isoformat(),
        }

        # 1. MongoDB stockage permanent
        self.mongo.insert_one({**doc, "Text": row["Text"], "cleaned": row["cleaned"]})

        # 2. Redis PUBLISH → instant, < 1ms
        live_payload = {
            "id":         str(row["Id"]),
            "productId":  str(row["ProductId"]),
            "summary":    str(row["Summary"])[:120],
            "sentiment":  str(row["sentiment_label"]),
            "confidence": float(row["confidence"]),
            "time":       str(row["Time"]),
        }
        self.redis_client.publish(
            "live_predictions",          # canal Redis
            json.dumps(live_payload)     # message JSON
        )

    def close(self, error):
        if hasattr(self, 'redis_client'):
            self.redis_client.close()
        if hasattr(self, 'mongo'):
            self.mongo.database.client.close()


# Lancer
query = (output.writeStream
    .foreach(RedisRealTimeWriter())
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 second")
    .start())

query.awaitTermination()
```

### Étape 3 : Django s'abonne à Redis — nouveau fichier `django_app/reviews_app/redis_subscriber.py`

```python
"""
Thread Redis → Channel Layer bridge.
Lance un subscriber Redis qui forward chaque message
vers le Channel Layer Django (WebSocket).
"""
import json
import logging
import threading

import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)


def start_redis_subscriber():
    """
    Lance un thread daemon qui écoute Redis 'live_predictions'
    et forward vers le Channel Layer Django.
    Appelé une seule fois au démarrage de Django (dans apps.py).
    """
    def _run():
        r = redis.Redis(host='localhost', port=6379, db=0)
        pubsub = r.pubsub()
        pubsub.subscribe("live_predictions")    # s'abonner au canal Spark
        logger.info("Redis subscriber started on channel 'live_predictions'")

        channel_layer = get_channel_layer()

        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                prediction = json.loads(message["data"])
                # Forward vers tous les WebSocket connectés
                async_to_sync(channel_layer.group_send)(
                    "live_feed",
                    {
                        "type":        "live.batch",
                        "predictions": [prediction],   # 1 review à la fois
                        "batch_id":    -1,
                    },
                )
            except Exception as e:
                logger.warning("Redis subscriber error: %s", e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    logger.info("Redis subscriber thread launched")
```

### Étape 4 : Démarrer le subscriber au boot Django — `django_app/reviews_app/apps.py`

```python
from django.apps import AppConfig


class ReviewsAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reviews_app"

    def ready(self):
        """Appelé quand Django démarre — lancer le subscriber Redis."""
        import os
        # Ne pas lancer en mode manage.py migrate ou collectstatic
        if os.environ.get("RUN_MAIN") == "true" or not os.environ.get("DJANGO_MANAGE"):
            from .redis_subscriber import start_redis_subscriber
            start_redis_subscriber()
```

### Étape 5 : Vérifier que `apps.py` est référencé dans `reviews_app/__init__.py`

```python
# django_app/reviews_app/__init__.py
default_app_config = "reviews_app.apps.ReviewsAppConfig"
```

### Schéma final avec Redis

```
CSV → Kafka → Spark (transform + predict)
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
    MongoDB insert_one    Redis PUBLISH "live_predictions"
    (stockage permanent)        │
                         Django Thread (subscriber)
                                │
                         channel_layer.group_send()
                                │
                         LiveFeedConsumer.live_batch()
                                │
                         WebSocket send → Navigateur

Latence totale : ~500ms - 1s
```

---

## Comparaison des 3 options

| | Option A | Option B | Option C |
|---|---|---|---|
| **Latence** | ~1-2s | ~1-2s | ~500ms |
| **Modification** | 1 ligne | 40 lignes | 100 lignes + Redis |
| **Nouveau composant** | Aucun | Aucun | Redis |
| **Robustesse** | Bonne | Moyenne | Excellente |
| **Pour soutenance** | ✅ Parfait | ✅ Bien | ✅ Impressionnant |
| **Pour prod** | ❌ | ❌ | ✅ |

---

## Recommandation

**Pour la soutenance : Option A** (1 ligne à changer).  
Change `processingTime="10 seconds"` → `processingTime="1 second"` et tu peux montrer des reviews qui apparaissent en temps réel, 1 par 1.

**Pour impressionner le prof : Option B** (foreach row-by-row).  
Montre que tu comprends la différence entre `foreachBatch` (batch) et `foreach` (ligne par ligne).

**Pour un projet production : Option C** (Redis).  
Redis Pub/Sub est la solution standard pour ce type de pipeline.

---

## Ce que tu dis au prof

> *"Notre implémentation actuelle utilise micro-batch de 10 secondes — c'est le comportement par défaut de Spark Structured Streaming. Pour passer au vrai real-time, on a trois niveaux :*
> 
> *1. Réduire le trigger à 1 seconde — la latence passe de 10s à 1s, minimal effort.*
>
> *2. Remplacer `foreachBatch` par `foreach` avec un ForeachWriter — Spark appelle `process(row)` pour chaque review individuellement dès qu'elle est prédite, avant même que le batch soit terminé.*
>
> *3. Ajouter Redis Pub/Sub entre Spark et Django — Spark publie dans Redis en < 1ms, un thread Django subscriber relit et forward via Channel Layer. C'est la solution production-grade qui découple Spark de Django complètement."*
