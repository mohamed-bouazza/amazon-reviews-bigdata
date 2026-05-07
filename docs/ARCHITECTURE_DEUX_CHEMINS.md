# Architecture finale — Les deux chemins de données

> Documentation technique du pipeline **Pulpe / SentimentIQ** après implémentation
> du schéma prof : **Visualisation en continu** (direct Spark→WebSocket) +
> **Analyses Statistiques** (Spark→MongoDB→Dashboard).

---

## Table des matières

1. [Vue d'ensemble — pourquoi 2 chemins ?](#1-vue-densemble--pourquoi-2-chemins-)
2. [Schéma d'architecture détaillé](#2-schéma-darchitecture-détaillé)
3. [Le chemin 1 — Visualisation en continu](#3-le-chemin-1--visualisation-en-continu)
4. [Le chemin 2 — Analyses statistiques](#4-le-chemin-2--analyses-statistiques)
5. [Code : Spark consumer (`consumer.py`)](#5-code--spark-consumer-consumerpy)
6. [Code : Endpoint Django (`api_views.py`)](#6-code--endpoint-django-api_viewspy)
7. [Code : WebSocket consumer (`consumers.py`)](#7-code--websocket-consumer-consumerspy)
8. [Code : Dashboard auto-refresh (`page-dashboard.jsx`)](#8-code--dashboard-auto-refresh-page-dashboardjsx)
9. [Channel Layer expliqué](#9-channel-layer-expliqué)
10. [Flux complet d'un batch Spark](#10-flux-complet-dun-batch-spark)
11. [Mapping schéma prof ↔ interface](#11-mapping-schéma-prof--interface)
12. [Résumé des fichiers modifiés](#12-résumé-des-fichiers-modifiés)

---

## 1. Vue d'ensemble — pourquoi 2 chemins ?

Le sujet du prof demande une **architecture à deux flux distincts** :

```
                    ┌───────► Visualisation en continu
                    │         (live, sans rafraîchir)
   Spark Streaming ─┤
                    │
                    └───────► Archivage MongoDB
                              └─► Analyses Statistiques
                                  (graphes, KPIs, agrégations)
```

**Pourquoi pas un seul chemin ?**

| Critère | Si on lit MongoDB | Si on push Spark direct |
|---|---|---|
| Latence pour le live | 2-12s (polling) | ~10s (batch Spark) + 50ms |
| Charge MongoDB | Élevée (poll constant) | Normale (insert seulement) |
| Survit si Mongo tombe ? | ❌ Plus de live | ✅ Live continue |
| Données historiques | ✅ Toujours dispo | ❌ Perdues si pas de DB |
| Statistiques complexes | ✅ Aggregation pipelines | ❌ Impossible en mémoire |

**Conclusion** : on a besoin des deux chemins simultanément.
- Live Feed → **push direct** (rapide, pas de poll)
- Dashboard → **MongoDB** (stats sur tout l'historique)

---

## 2. Schéma d'architecture détaillé

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AMAZON FINE FOOD REVIEWS                          │
│                          test_set.csv (56,846 lignes)                    │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
                  ┌─────────────────────────┐
                  │  kafka_producer/        │
                  │  producer.py            │
                  │  (rejoue le CSV en JSON)│
                  └───────────┬─────────────┘
                              │ JSON message
                              ▼
                ┌─────────────────────────────┐
                │       KAFKA :29092          │
                │   topic "amazon-reviews"    │
                └─────────────┬───────────────┘
                              │ readStream
                              ▼
       ┌──────────────────────────────────────────────────┐
       │  spark_streaming/consumer.py                     │
       │  ┌────────────────────────────────────────────┐  │
       │  │ 1. from_json(value, schema)                │  │
       │  │ 2. clean_udf(Summary + Text) → cleaned     │  │
       │  │ 3. PipelineModel.transform(parsed)         │  │
       │  │    → prediction, sentiment_label, conf     │  │
       │  │ 4. foreachBatch(write_to_mongo)            │  │
       │  └────────────────────────────────────────────┘  │
       └─────────────────┬───────────────────┬────────────┘
                         │                   │
              ┌──────────▼──────────┐  ┌─────▼─────────────┐
              │ 1. insert_many()    │  │ 2. HTTP POST      │
              │    pymongo          │  │    requests.post  │
              └──────────┬──────────┘  └─────┬─────────────┘
                         │                   │
                         ▼                   ▼
            ┌────────────────────┐   ┌──────────────────────────┐
            │  MongoDB :27018    │   │ Django                    │
            │  reviews_db.       │   │ /api/internal/live-push   │
            │  predictions       │   │  ┌────────────────────┐   │
            └─────────┬──────────┘   │  │ channel_layer      │   │
                      │              │  │ .group_send(       │   │
                      │              │  │   "live_feed",     │   │
                      │              │  │   batch_data)      │   │
                      │              │  └────────┬───────────┘   │
                      │              └───────────┼───────────────┘
                      │                          │
       ┌──────────────┘                          │
       │ Aggregations pymongo                    │
       ▼                                         │
  ┌────────────────────────────┐                 │
  │  /api/kpi                  │                 │
  │  /api/distribution         │                 │
  │  /api/trend                │                 │
  │  /api/top-products         │                 │
  │  /api/recent               │                 │
  └────────────┬───────────────┘                 │
               │                                 │
               │ JSON (REST)                     │ live_batch event
               │ refresh 15s                     │ (Channel Layer)
               ▼                                 ▼
       ┌────────────────────┐           ┌────────────────────┐
       │   📊 DASHBOARD     │           │   ⚡ LIVE FEED     │
       │ (Analyses Stats)   │           │ (Visualisation     │
       │                    │           │  en continu)       │
       │  • KPI cards       │           │                    │
       │  • Trend chart     │           │  • Reviews cards   │
       │  • Distribution    │           │  • Throughput      │
       │  • Top products    │           │  • Sentiment ratio │
       │  • Confusion mat.  │           │                    │
       └────────────────────┘           └────────────────────┘
```

---

## 3. Le chemin 1 — Visualisation en continu

### Trajet exact d'une prédiction

```
Spark batch terminé (toutes les 10s)
    │
    ▼
foreachBatch(write_to_mongo):
    │
    ├─[1]─► insert_many() ──► MongoDB (archivage)
    │
    └─[2]─► requests.post(
              "http://localhost:8000/api/internal/live-push",
              json={"predictions": [...], "batch_id": N}
            )
                │
                ▼
            Django api_views.live_push()
                │
                ▼
            channel_layer.group_send("live_feed", {
                "type": "live.batch",
                "predictions": [...]
            })
                │
                ▼
            consumers.LiveFeedConsumer.live_batch(event)
                │
                ▼
            self.send(json) ──► WebSocket browser
                │
                ▼
            page-live.jsx
                │
                ▼
            setReviews([...nouvelles, ...anciennes])
            Compteurs +=1
            Graphe se met à jour
```

### Particularités

- **Pas de polling** — c'est du push événementiel.
- **Pas de lecture MongoDB** dans ce chemin (sauf pour les compteurs initiaux).
- **Latence** : ~10s (durée d'un batch Spark) + ~50ms (HTTP + WS).
- **Fallback** : si Spark ne pousse pas (ex. débuggage), le WebSocket poll MongoDB toutes les 10s.

---

## 4. Le chemin 2 — Analyses statistiques

### Trajet d'une requête Dashboard

```
Browser (Dashboard ouvert)
    │
    ▼ setInterval(15000)
fetch /api/distribution
    │
    ▼
api_views.distribution()
    │
    ▼
mongo.get_global_distribution()
    │
    ▼
db.predictions.aggregate([
    {"$group": {"_id": "$sentiment_label", "n": {"$sum": 1}}}
])
    │
    ▼
{ positive: 8412, neutral: 2103, negative: 1938 }
    │
    ▼
JsonResponse → fetch reçoit
    │
    ▼
window.MOCK.distribution = data
setTick(t => t+1)  // force re-render React
    │
    ▼
Donut chart se redessine avec nouvelles valeurs
```

### Liste des agrégations MongoDB

| Endpoint | Pipeline MongoDB | Utilisé par |
|---|---|---|
| `/api/kpi` | `estimated_document_count` + `distinct ProductId` | KPI cards |
| `/api/distribution` | `$group _id=sentiment_label` | Donut |
| `/api/trend?year=All` | `$addFields date` + `$group year+label` | Line chart |
| `/api/top-products?sentiment=neg` | `$match` + `$group ProductId` + `$sort` | Bar chart |
| `/api/recent?limit=50` | `find().sort(inserted_at DESC).limit(50)` | Table |
| `/api/confusion-matrix` | `$group (true_sentiment, prediction)` | Heatmap |
| `/api/confidence-distribution` | `$switch` sur 10 bins | Histogram |

---

## 5. Code : Spark consumer (`consumer.py`)

### Section critique : `write_to_mongo`

```python
def write_to_mongo(batch_df, batch_id):
    """
    Appelé toutes les 10s par Spark Structured Streaming
    pour chaque micro-batch de prédictions.
    """
    count = batch_df.count()
    if count == 0:
        print(f"[Batch {batch_id}] Vide, skip")
        return

    print(f"[Batch {batch_id}] → {count} prédictions reçues")

    # Spark DF → liste de dicts Python (toPandas() côté driver)
    rows = batch_df.toPandas().to_dict(orient="records")

    # Ajouter timestamp + convertir numpy → Python natif
    for row in rows:
        row["inserted_at"] = datetime.utcnow()
        for k, v in row.items():
            if hasattr(v, 'item'):  # numpy.int64, numpy.float64 → int, float
                row[k] = v.item()

    # ─── CHEMIN 2 : Archivage MongoDB ───────────────────────
    raw = os.getenv("MONGO_URI") or ""
    if not raw or "27017" in raw or "mongo:" in raw:
        raw = "mongodb://localhost:27018"

    client = MongoClient(raw, serverSelectionTimeoutMS=10000)
    db = client["reviews_db"]
    collection = db["predictions"]
    collection.insert_many(rows)
    client.close()

    print(f"[Batch {batch_id}] ✅ {count} docs → MongoDB")

    # ─── CHEMIN 1 : Push direct WebSocket ───────────────────
    try:
        import requests
        # On extrait juste les champs utiles pour le live feed
        payload = []
        for row in rows:
            payload.append({
                "id":         str(row.get("Id", "")),
                "productId":  str(row.get("ProductId", "")),
                "summary":    str(row.get("Summary", ""))[:120],  # tronqué
                "sentiment":  str(row.get("sentiment_label", "")),
                "confidence": float(row.get("confidence", 0)),
                "time":       str(row.get("Time", "")),
            })

        # POST vers Django (qui broadcastera via Channel Layer)
        resp = requests.post(
            "http://localhost:8000/api/internal/live-push",
            json={"predictions": payload, "batch_id": batch_id},
            timeout=5,
        )
        print(f"[Batch {batch_id}] ✅ Push WebSocket ({resp.status_code})")
    except Exception as e:
        print(f"[Batch {batch_id}] ⚠️ WebSocket push failed: {e}")
        # Pas grave — MongoDB a déjà reçu, le fallback prendra le relais
```

### Pourquoi `requests.post` et pas Channel Layer direct ?

Spark tourne dans un **processus séparé** (lancé via `spark-submit`). Django Channels avec `InMemoryChannelLayer` ne fonctionne qu'**à l'intérieur d'un seul processus**. La seule façon de communiquer entre Spark et Django est donc HTTP (ou Redis / un autre broker).

```
┌─────────────────┐              ┌──────────────────┐
│ Process Spark   │  HTTP POST   │  Process Django  │
│ (spark-submit)  │ ───────────► │  (daphne)        │
│                 │              │                  │
│ pymongo client  │              │  Channel Layer   │
└─────────────────┘              │  (InMemory)      │
                                 │       │          │
                                 │       ▼          │
                                 │  WebSockets      │
                                 └──────────────────┘
```

---

## 6. Code : Endpoint Django (`api_views.py`)

### Vue `live_push`

```python
@require_http_methods(["POST"])
def live_push(request: HttpRequest) -> JsonResponse:
    """
    Reçoit un batch de prédictions depuis Spark et le broadcast
    à tous les clients WebSocket connectés via Channel Layer.
    """
    try:
        data = json.loads(request.body)
        predictions = data.get("predictions", [])
        batch_id    = data.get("batch_id", -1)
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    # Importer ici pour éviter les cycles d'import au boot
    from channels.layers import get_channel_layer
    from asgiref.sync import async_to_sync

    channel_layer = get_channel_layer()

    # group_send envoie l'event à tous les WebSockets ayant rejoint "live_feed"
    async_to_sync(channel_layer.group_send)(
        "live_feed",
        {
            "type":        "live.batch",   # ← appelle live_batch() dans consumers.py
            "predictions": predictions,
            "batch_id":    batch_id,
        },
    )

    logger.info("live_push: broadcast %d predictions", len(predictions))
    return JsonResponse({"pushed": len(predictions), "batch_id": batch_id})
```

### Décomposition

| Ligne | Rôle |
|---|---|
| `@require_http_methods(["POST"])` | Refuse GET (sécurité basique) |
| `json.loads(request.body)` | Parse le JSON envoyé par Spark |
| `get_channel_layer()` | Récupère la singleton Channel Layer |
| `async_to_sync(...)` | Adapte une coroutine async à du code sync (DRF est sync) |
| `group_send("live_feed", event)` | Envoie l'event à tous les WS du groupe |
| `"type": "live.batch"` | Le `.` devient `_` → appelle la méthode `live_batch` dans le Consumer |

### Route URL

```python
# reviews_app/urls.py
path("internal/live-push", api_views.live_push),
```

---

## 7. Code : WebSocket consumer (`consumers.py`)

### Classe `LiveFeedConsumer`

```python
LIVE_GROUP = "live_feed"

class LiveFeedConsumer(AsyncWebsocketConsumer):
    """
    Reçoit les prédictions en temps réel depuis Spark via Channel Layer.
    Spark → HTTP POST → group_send → ici → browser.
    Fallback : poll MongoDB toutes les 10s si Spark ne pousse pas.
    """
    FALLBACK_INTERVAL = 10.0

    # ─── CONNECT ────────────────────────────────────────────
    async def connect(self) -> None:
        await self.accept()

        # 🔑 Rejoindre le groupe — recevra TOUS les pushs Spark
        await self.channel_layer.group_add(LIVE_GROUP, self.channel_name)

        self._last_id = None
        self._stop = asyncio.Event()

        # Envoyer l'historique initial (depuis MongoDB)
        initial = await database_sync_to_async(mongo.get_recent_predictions)(20, None)
        if initial:
            self._last_id = initial[0]["_id"]

        counters = await self._counters()
        await self.send(text_data=json.dumps({
            "type":        "init",
            "predictions": initial,
            "counters":    counters,
        }))

        # Lance le fallback polling
        self._task = asyncio.create_task(self._fallback_loop())

    # ─── DISCONNECT ─────────────────────────────────────────
    async def disconnect(self, code: int) -> None:
        await self.channel_layer.group_discard(LIVE_GROUP, self.channel_name)
        if hasattr(self, "_stop"):
            self._stop.set()
        # ... cleanup task

    # ─── HANDLER appelé par group_send ──────────────────────
    async def live_batch(self, event: dict) -> None:
        """
        Activé quand api_views.live_push fait group_send avec
        type="live.batch". Pas de lecture MongoDB ici — données
        directes de Spark. C'est LA visualisation en continu.
        """
        predictions = event.get("predictions", [])
        counters = await self._counters()

        await self.send(text_data=json.dumps({
            "type":        "batch",
            "predictions": predictions,
            "counters":    counters,
            "source":      "spark_direct",  # marqueur de débug
        }))

    # ─── FALLBACK : poll MongoDB ────────────────────────────
    async def _fallback_loop(self) -> None:
        """Si Spark ne pousse pas (debug, crash, lent), on poll MongoDB."""
        while not self._stop.is_set():
            await asyncio.sleep(self.FALLBACK_INTERVAL)
            docs = await database_sync_to_async(
                mongo.get_predictions_after
            )(self._last_id, 20)

            if docs:
                self._last_id = docs[0]["_id"]
                counters = await self._counters()
                await self.send(text_data=json.dumps({
                    "type":        "batch",
                    "predictions": docs,
                    "counters":    counters,
                    "source":      "mongo_fallback",
                }))

    async def _counters(self):
        return await database_sync_to_async(
            mongo.get_global_distribution
        )()
```

### La magie du `type` → handler

Quand `group_send` reçoit `{"type": "live.batch", ...}`, Channels :

1. Récupère tous les WebSockets dans le groupe `"live_feed"`
2. Pour chaque consumer, appelle la méthode `live_batch(event)` (le `.` devient `_`)
3. Le consumer décide quoi envoyer au navigateur via `self.send()`

C'est un **pattern observer** distribué.

---

## 8. Code : Dashboard auto-refresh (`page-dashboard.jsx`)

### Le `useEffect` d'auto-refresh

```jsx
function DashboardPage({ setPage, setSelectedProduct }) {
  const D = window.MOCK;
  // ... état local ...

  // ─── AUTO-REFRESH toutes les 15 secondes ──────────────────
  const [tick, setTick] = React.useState(0);

  React.useEffect(() => {
    const interval = setInterval(async () => {
      try {
        // Fetch parallélisé pour minimiser la latence
        const [kpiData, distData, recentData, trendData] = await Promise.all([
          window.API.getKpi(),
          window.API.getDistribution(),
          window.API.getRecent(50),
          window.API.getTrend('All'),
        ]);

        // Mettre à jour le store global window.MOCK
        if (kpiData)    Object.assign(window.MOCK.kpi, kpiData);
        if (distData)   Object.assign(window.MOCK.distribution, distData);
        if (recentData?.data) window.MOCK.recentPredictions = recentData.data;
        if (trendData?.data)  window.MOCK.yearlyTrend       = trendData.data;

        setTick(t => t + 1);  // ← FORCE le re-render React
      } catch(e) {
        // silencieux : si MongoDB tombe, on retentera dans 15s
      }
    }, 15000);

    return () => clearInterval(interval);  // cleanup à l'unmount
  }, []);  // [] = lance UNE fois au mount

  // ... reste du composant ...
}
```

### Pourquoi `[tick]` dans les useEffect des charts ?

```jsx
// Trend chart
React.useEffect(() => {
  // ... destroy old chart, create new one ...
}, [yearFilter, tick]);  // ← redessine quand tick change
```

Quand `setTick(t => t+1)` est appelé, React re-render. Les useEffect avec `[tick]` dans les dépendances détectent le changement et redessinent les charts avec les nouvelles données dans `window.MOCK`.

### Schéma du flow

```
setInterval (15000ms)
   │
   ▼
fetch /api/kpi, /api/distribution, /api/trend, /api/recent  (en parallèle)
   │
   ▼
Object.assign(window.MOCK, ...)
   │
   ▼
setTick(t => t+1)  ← React voit le changement de state
   │
   ▼
Re-render DashboardPage
   │
   ▼
useEffect([tick]) sur tous les charts → destroy + recreate
   │
   ▼
Charts affichent les nouvelles données ✨
```

---

## 9. Channel Layer expliqué

### C'est quoi exactement ?

Un **bus de messages in-memory** intégré à Django Channels. Permet à n'importe quel code Django d'envoyer un message à un groupe de WebSockets connectés.

```
┌────────────────────────────────────────────────────┐
│ Django process (daphne)                            │
│                                                    │
│  ┌──────────────┐                                  │
│  │  api_views   │ ──── group_send ──┐              │
│  │  .live_push  │                   │              │
│  └──────────────┘                   │              │
│                                     ▼              │
│  ┌──────────────────────────────────────────┐      │
│  │ InMemoryChannelLayer                     │      │
│  │   {                                      │      │
│  │     "live_feed": [                       │      │
│  │       channel_name_1,  ← user A          │      │
│  │       channel_name_2,  ← user B          │      │
│  │       channel_name_3,  ← user C          │      │
│  │     ]                                    │      │
│  │   }                                      │      │
│  └────────┬────────┬────────┬───────────────┘      │
│           │        │        │                      │
│           ▼        ▼        ▼                      │
│       ┌─────┐  ┌─────┐  ┌─────┐                    │
│       │ WS  │  │ WS  │  │ WS  │  Tous les          │
│       │  A  │  │  B  │  │  C  │  consumers reçoivent│
│       └─────┘  └─────┘  └─────┘                    │
└────────────────────────────────────────────────────┘
```

### Limitation `InMemoryChannelLayer`

- ✅ Fonctionne dans **un seul processus Django** (daphne)
- ❌ Ne fonctionne pas si tu as plusieurs workers daphne
- ❌ Spark (subprocess) ne peut pas l'utiliser directement → on passe par HTTP

Pour scaler à plusieurs workers, il faudrait passer à **Redis Channel Layer** :

```python
# settings.py
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {"hosts": [("localhost", 6379)]},
    }
}
```

Pour ton projet académique, `InMemoryChannelLayer` suffit largement.

---

## 10. Flux complet d'un batch Spark

Suivons un batch contenant **3 reviews** :

### T+0s : Producer envoie 3 messages Kafka

```python
producer.send("amazon-reviews", value={"Id":1, "ProductId":"B001", ...})
producer.send("amazon-reviews", value={"Id":2, "ProductId":"B002", ...})
producer.send("amazon-reviews", value={"Id":3, "ProductId":"B003", ...})
```

### T+10s : Spark déclenche un micro-batch

```
[Batch 42] → 3 prédictions reçues

# Modèle applique :
Id=1, cleaned="best dog food", prob=[0.02, 0.05, 0.93]
       → prediction=2, sentiment_label="positive", confidence=0.93
Id=2, cleaned="terrible product", prob=[0.85, 0.10, 0.05]
       → prediction=0, sentiment_label="negative", confidence=0.85
Id=3, cleaned="just average", prob=[0.20, 0.55, 0.25]
       → prediction=1, sentiment_label="neutral", confidence=0.55
```

### T+10s : `write_to_mongo` exécuté

```python
# 1. MongoDB insert
db.predictions.insert_many([
    {Id:1, ..., sentiment_label:"positive", confidence:0.93, inserted_at:...},
    {Id:2, ..., sentiment_label:"negative", confidence:0.85, inserted_at:...},
    {Id:3, ..., sentiment_label:"neutral",  confidence:0.55, inserted_at:...},
])

# 2. HTTP POST Django
requests.post("http://localhost:8000/api/internal/live-push",
    json={
        "predictions": [
            {"id":"1", "productId":"B001", "summary":"...",
             "sentiment":"positive", "confidence":0.93},
            {"id":"2", "productId":"B002", "summary":"...",
             "sentiment":"negative", "confidence":0.85},
            {"id":"3", "productId":"B003", "summary":"...",
             "sentiment":"neutral",  "confidence":0.55},
        ],
        "batch_id": 42
    })
```

### T+10.05s : Django reçoit, broadcast

```python
# api_views.live_push() :
channel_layer.group_send("live_feed", {
    "type": "live.batch",
    "predictions": [...3 dicts...],
    "batch_id": 42,
})
```

### T+10.05s : Tous les WebSockets connectés reçoivent

```python
# consumers.LiveFeedConsumer.live_batch() :
counters = await self._counters()  # query Mongo : {pos:8412, neu:2103, neg:1938}
await self.send(text_data=json.dumps({
    "type": "batch",
    "predictions": [...],
    "counters": counters,
    "source": "spark_direct",
}))
```

### T+10.10s : Browser React reçoit le frame WebSocket

```javascript
// page-live.jsx
ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "batch") {
        setReviews(prev => [...msg.predictions, ...prev].slice(0, 100));
        setCounters(msg.counters);
    }
};
```

### T+10.10s : User voit les 3 nouvelles cards apparaître ✨

```
┌─────────────────────────────────────┐
│ POSITIVE  B001  ✨ NEW             │
│ "best dog food..."        93%      │
├─────────────────────────────────────┤
│ NEGATIVE  B002  ✨ NEW             │
│ "terrible product..."     85%      │
├─────────────────────────────────────┤
│ NEUTRAL   B003  ✨ NEW             │
│ "just average..."         55%      │
└─────────────────────────────────────┘

Compteurs mis à jour:
  TOTAL:    5,233 (+3)
  POSITIVE: 3,433 (+1)
  NEUTRAL:    880 (+1)
  NEGATIVE:   920 (+1)
```

**Latence totale : ~10.10 secondes** (dont 10s = batch Spark, 0.1s = transport HTTP+WS+JSON parsing).

### En parallèle : T+15s — Dashboard se rafraîchit

```javascript
// page-dashboard.jsx setInterval(15000)
const [kpi, dist, trend, recent] = await Promise.all([
    fetch("/api/kpi"),
    fetch("/api/distribution"),
    fetch("/api/trend?year=All"),
    fetch("/api/recent?limit=50"),
]);
// → Object.assign(window.MOCK, ...)
// → setTick(t => t+1)
// → Re-render charts avec nouvelles valeurs MongoDB
```

---

## 11. Mapping schéma prof ↔ interface

```
┌──────────────────────────┐    ┌──────────────────────────┐
│ SCHÉMA PROF              │    │ INTERFACE PULPE          │
├──────────────────────────┤    ├──────────────────────────┤
│ Amazon Flux Temps réel   │ ←→ │ producer.py + test.csv   │
│ Kafka Collecte           │ ←→ │ kafka :29092 amazon-rev  │
│ Spark Streaming Traitement│ ←→ │ consumer.py PipelineModel│
│ Visualisation en continu │ ←→ │ ⚡ Live Feed page        │
│ MongoDB Archivage        │ ←→ │ mongo :27018 reviews_db  │
│ Analyses Statistiques    │ ←→ │ 📊 Dashboard page        │
└──────────────────────────┘    └──────────────────────────┘
```

### Détail page Live Feed

```
┌──────────────────────────────────────────┬─────────────────────┐
│ TITRE : Live Feed                        │  📊 SIDEBAR DROITE  │
│                                          │                     │
│ • LIVE indicator (rouge pulsant)         │  LAST 60S THROUGHPUT│
│ • WebSocket Connected (vert)             │  ┌────────────┐     │
│ • X.X reviews/sec                        │  │ ╲╱╲╱╲╱     │     │
│ • Total / Positive / Neutral / Negative  │  └────────────┘     │
│                                          │                     │
│ [Filter buttons : Pos / Neu / Neg]       │  SENTIMENT RATIO    │
│                                          │  ┌────────────┐     │
│ ┌──────────────────────────────────┐     │  │   donut    │     │
│ │ POSITIVE  B001E4KFG0  ✨NEW     │     │  │  65.6% pos │     │
│ │ "The perfect condiment..."  98% │     │  └────────────┘     │
│ ├──────────────────────────────────┤     │                     │
│ │ NEGATIVE  B0026ROTG  ✨NEW      │     │  TOP PRODUCTS NOW   │
│ │ "What a disappointment..." 99%   │     │  1. B007JFMH8M  9   │
│ ├──────────────────────────────────┤     │  2. B0026ROTGE  8   │
│ │ POSITIVE  B001VIY8BW             │     │  3. B003E728CE  7   │
│ │ "Works great with cat..."   57%  │     │                     │
│ └──────────────────────────────────┘     │                     │
│ ↑ alimenté DIRECTEMENT par Spark         │                     │
│   (WebSocket push, pas de Mongo poll)    │                     │
└──────────────────────────────────────────┴─────────────────────┘
```

### Détail page Dashboard

```
┌─────────────────────────────────────────────────────────────────┐
│ TITRE : Analytics Dashboard                                     │
│                                                                 │
│ ┌────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐        │
│ │ TOTAL  │ │THROUGHPUT│ │F1-SCORE  │ │ UNIQUE PRODUCTS │        │
│ │ 5,230  │ │ 47/sec   │ │ 82.7%    │ │     3,929       │        │
│ └────────┘ └──────────┘ └──────────┘ └─────────────────┘        │
│       ↑ refresh toutes les 15s depuis /api/kpi                  │
│                                                                 │
│ Sentiment Trend [year selector]                                 │
│ ┌──────────────────────────────────────────────────────────┐    │
│ │      📈 line chart 2007-2012 (pos/neu/neg)              │    │
│ │           ↑ refresh 15s depuis /api/trend                │    │
│ └──────────────────────────────────────────────────────────┘    │
│                                                                 │
│ ┌──────────────────────┐  ┌──────────────────────────────┐     │
│ │ Global Distribution  │  │ Top 10 Most Negative Reviews │     │
│ │   🍩 donut           │  │   📊 horizontal bar          │     │
│ │   ↑ refresh 15s      │  │   ↑ chargé une fois          │     │
│ └──────────────────────┘  └──────────────────────────────┘     │
│                                                                 │
│ Recent Predictions Table  (refresh 15s, paginated 8/page)       │
│ ┌──────────────────────────────────────────────────────────┐    │
│ │ ID │ PRODUCT ID │ SUMMARY │ DATE │ SENTIMENT │ CONFIDENCE│    │
│ └──────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 12. Résumé des fichiers modifiés

### Fichier 1 : `spark_streaming/consumer.py`

**Section** : `write_to_mongo()` (~ligne 120)
**Modification** : Ajout du HTTP POST après l'insert MongoDB
**But** : Notifier Django immédiatement à chaque batch

```diff
+ # Push direct vers Django WebSocket (Visualisation en continu)
+ try:
+     import requests
+     payload = [...]
+     resp = requests.post("http://localhost:8000/api/internal/live-push",
+                          json={"predictions": payload, "batch_id": batch_id},
+                          timeout=5)
+ except Exception as e:
+     print(f"⚠️ WebSocket push failed: {e}")
```

---

### Fichier 2 : `django_app/reviews_app/api_views.py`

**Section** : Nouvelle vue ajoutée
**Modification** : Endpoint `live_push` qui broadcast via Channel Layer

```python
@require_http_methods(["POST"])
def live_push(request):
    data = json.loads(request.body)
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "live_feed",
        {"type": "live.batch", "predictions": data["predictions"]}
    )
    return JsonResponse({"pushed": len(data["predictions"])})
```

---

### Fichier 3 : `django_app/reviews_app/urls.py`

**Modification** : Ajout d'une route

```python
path("internal/live-push", api_views.live_push),
```

---

### Fichier 4 : `django_app/reviews_app/consumers.py`

**Section** : `LiveFeedConsumer` réécrite
**Modifications** :
- Ajout `group_add(LIVE_GROUP)` dans `connect()`
- Ajout `group_discard` dans `disconnect()`
- Nouvelle méthode `live_batch(event)` (handler du group_send)
- Renommé l'ancien polling en `_fallback_loop` (toutes les 10s au lieu de 2s)

---

### Fichier 5 : `django_app/static/js/page-dashboard.jsx`

**Section** : Composant `DashboardPage`
**Modifications** :
- Ajout d'un `useState` pour `tick`
- Ajout d'un `useEffect` avec `setInterval` qui fetch les APIs toutes les 15s
- Ajout de `tick` dans les dépendances des `useEffect` des charts (donut, trend)

```jsx
const [tick, setTick] = React.useState(0);

React.useEffect(() => {
    const interval = setInterval(async () => {
        const [kpi, dist, recent, trend] = await Promise.all([...]);
        Object.assign(window.MOCK, ...);
        setTick(t => t+1);
    }, 15000);
    return () => clearInterval(interval);
}, []);
```

---

## 13. Pour exécuter

```bash
# Terminal 1 : Docker
cd ~/amazon-reviews-bigdata
docker compose up -d
sleep 25

# Terminal 1 : Django
source venv/bin/activate
cd django_app
daphne -b 0.0.0.0 -p 8000 reviews_project.asgi:application

# Browser : http://localhost:8000
# → Login → Pipeline Control → Start Pipeline
# → Live Feed (visualisation en continu)
# → Dashboard  (analyses statistiques)
```

### Logs Spark à surveiller

Dans `django_app/logs/spark.log` (ou directement le terminal du subprocess) :

```
[Batch 0] → 47 prédictions reçues
[Batch 0] ✅ 47 docs → MongoDB reviews_db.predictions
[Batch 0] ✅ Push WebSocket → Django (200)        ← le push direct fonctionne
[Batch 1] → 32 prédictions reçues
[Batch 1] ✅ 32 docs → MongoDB reviews_db.predictions
[Batch 1] ✅ Push WebSocket → Django (200)
```

Si tu vois `⚠️ WebSocket push failed`, c'est que Django n'écoute pas encore (vérifier daphne).

---

## 14. Checklist soutenance

> Lors de la soutenance, tu peux dire :

- ✅ **"On a Kafka pour la collecte"** → `kafka :29092` topic `amazon-reviews`
- ✅ **"Zookeeper coordonne Kafka"** → `zookeeper :2181`
- ✅ **"Spark Streaming traite en micro-batch"** → `consumer.py` `processingTime="10 seconds"`
- ✅ **"On utilise Spark MLlib"** → `PipelineModel` (Tokenizer + HashingTF + IDF + LogisticRegression)
- ✅ **"MongoDB archive les prédictions"** → `reviews_db.predictions`
- ✅ **"Docker conteneurise tout"** → `docker-compose.yml` (6 services)
- ✅ **"Django + JS pour le web"** → `daphne` ASGI + React 18 SPA
- ✅ **"Visualisation en continu via WebSocket"** → Live Feed page (push direct Spark)
- ✅ **"Analyses statistiques via MongoDB"** → Dashboard page (agrégations)

---

**Auteur** : généré le 2026-05-06
**Stack** : Spark 3.5, Kafka 7.5, MongoDB 7, Django 4.2 + Channels 4, React 18.3
**F1-score modèle** : 0.8320 (test set 56,846 reviews)
