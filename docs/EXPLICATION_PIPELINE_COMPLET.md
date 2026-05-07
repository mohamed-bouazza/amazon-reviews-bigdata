# Explication Complète du Pipeline — Du Notebook au Frontend

> **À lire avant la soutenance.** Ce document suit le chemin exact d'une review Amazon depuis le CSV jusqu'à l'affichage en temps réel dans le navigateur, avec le code réel de chaque étape et ce qu'il faut dire au professeur.

---

## Table des matières

1. [Vue d'ensemble — les deux chemins](#1-vue-densemble)
2. [Étape 0 : Les notebooks — entraînement du modèle](#2-notebooks)
3. [Étape 1 : Kafka Producer — injection des données](#3-kafka-producer)
4. [Étape 2 : Spark Consumer — lecture Kafka + prédiction](#4-spark-consumer)
5. [CHEMIN 1 — Visualisation en continu (Kafka → Django → WebSocket)](#5-chemin-1-live)
6. [CHEMIN 2 — Analyses Statistiques (MongoDB → REST API → Dashboard)](#6-chemin-2-stats)
7. [Frontend — comment le navigateur reçoit et affiche tout](#7-frontend)
8. [Ce que tu dis au professeur pour chaque partie](#8-discours-prof)

---

## 1. Vue d'ensemble — les deux chemins

```
╔══════════════════════════════════════════════════════════════════════════╗
║                          PIPELINE GLOBAL                                ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  CSV ──► Kafka Producer ──► KAFKA TOPIC "amazon-reviews"                ║
║                                           │                              ║
║                                      Spark Streaming                    ║
║                                      (prédit le sentiment)              ║
║                                           │                              ║
║                          ┌────────────────┴────────────────┐            ║
║                          ▼                                  ▼            ║
║              KAFKA TOPIC "predictions-live"            MongoDB           ║
║              (trigger 1s — ultra rapide)           (trigger 10s batch)  ║
║                          │                                  │            ║
║              Django KafkaConsumer thread            REST API (15s poll) ║
║              channel_layer.group_send()                     │            ║
║                          │                                  │            ║
║              CHEMIN 1 : Live Feed                CHEMIN 2 : Dashboard   ║
║              WebSocket → cartes 1 par 1          Chart.js → graphes     ║
╚══════════════════════════════════════════════════════════════════════════╝
```

**En une phrase :**
Spark prédit et écrit simultanément dans deux destinations — Kafka `predictions-live` (pour le live feed instantané) et MongoDB (pour les statistiques du dashboard).

**Pourquoi Kafka comme sortie et pas HTTP POST ?**
Spark tourne dans un sous-processus séparé de Django. Utiliser Kafka comme bus de sortie découple complètement les deux systèmes — Spark n'a pas besoin de savoir que Django existe. Django lit Kafka indépendamment via un thread consumer.

---

## 2. Notebooks — entraînement du modèle

### Fichiers

```
notebooks/
├── 01_eda.ipynb           ← exploration des données
├── 02_preprocessing.ipynb ← nettoyage, équilibrage
├── 03_training.ipynb      ← entraînement + CrossValidation
└── 04_evaluation.ipynb    ← métriques finales
```

### `02_preprocessing.ipynb` — Nettoyage du texte

```python
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r'<.*?>', ' ', text)        # retire les balises HTML
    text = re.sub(r'http\S+', ' ', text)       # retire les URLs
    text = re.sub(r'[^a-z\s]', ' ', text)      # garde seulement les lettres
    tokens = [lemmatizer.lemmatize(t)
              for t in text.split()
              if t not in stop_words and len(t) > 2]
    return ' '.join(tokens)
```

**Ce qu'on traite :** `"This product is NOT good!!! I hate it :("` → `"product good hate"`

Le texte traité = **Summary + Text concaténés** : `"Good Dog Food. I have bought several..."`

### `03_training.ipynb` — Pipeline ML + sauvegarde

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

pipeline = Pipeline(stages=[
    Tokenizer(inputCol="cleaned", outputCol="tokens"),
    StopWordsRemover(inputCol="tokens", outputCol="filtered"),
    HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=50000),
    IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5),
    LogisticRegression(featuresCol="features", labelCol="sentiment",
                       maxIter=100, regParam=0.01),
])

cv = CrossValidator(estimator=pipeline, numFolds=3,
                    evaluator=MulticlassClassificationEvaluator(metricName="f1"))

cv_model = cv.fit(train_df)

# ⚠️ IMPORTANT : sauvegarder bestModel (PipelineModel), pas cv_model
# cv_model est un CrossValidatorModel — PipelineModel.load() échoue dessus
cv_model.bestModel.save("models/best_model")
```

**Résultat obtenu :** F1-macro = **0.8320**

| Classe | Précision | Recall | F1 |
|--------|-----------|--------|----|
| Negative | 0.85 | 0.83 | 0.84 |
| Neutral  | 0.43 | 0.37 | 0.40 |
| Positive | 0.92 | 0.93 | 0.93 |

> **Pourquoi Neutral est plus faible ?** Classe sous-représentée (avis 3 étoiles ambigus) — attendu dans tout problème NLP de sentiment à 3 classes.

---

## 3. Kafka Producer — injection des données

### Fichier : `kafka_producer/producer.py`

```python
df = pd.read_csv("data/test_set.csv")   # 56 846 reviews pré-préparées

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

for _, row in df.iterrows():
    msg = {
        "Id":             int(row["Id"]),
        "ProductId":      str(row["ProductId"]),
        "UserId":         str(row["UserId"]),
        "Time":           int(row["Time"]),
        "Summary":        str(row.get("Summary", "")),
        "Text":           str(row.get("Text", "")),
        "cleaned":        str(row.get("cleaned", "")),
        "true_sentiment": int(row["sentiment"]),
    }
    producer.send("amazon-reviews", key=str(row["ProductId"]), value=msg)
    time.sleep(0.5)   # 2 reviews/seconde → simule un flux continu
```

**Format d'un message Kafka à ce stade :**

```json
{
  "Id": 12345,
  "ProductId": "B001E4KFG0",
  "UserId": "A3SGXH7AUHU8GW",
  "Time": 1303862400,
  "Summary": "Good Quality Dog Food",
  "Text": "I have bought several of the Vitality canned dog food products...",
  "cleaned": "bought several vitality canned dog food product good quality",
  "true_sentiment": 2
}
```

**Le producer tourne dans un sous-processus lancé par Django** via `PipelineManager.start_producer()`.

---

## 4. Spark Consumer — lecture Kafka + prédiction

### Fichier : `spark_streaming/consumer.py`

### 4.1 Chargement du modèle entraîné

```python
from pyspark.ml import PipelineModel
model = PipelineModel.load("./models/best_model")
# Contient : Tokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression
```

### 4.2 Lecture du topic Kafka en streaming

```python
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "amazon-reviews") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*")
```

### 4.3 Application du modèle ML

```python
# Nettoyage si cleaned est absent
parsed = parsed.withColumn("cleaned",
    when(col("cleaned").isNull() | (col("cleaned") == ""),
         clean_udf(concat_ws(". ", col("Summary"), col("Text"))))
    .otherwise(col("cleaned")))

# Prédiction : 0=negative, 1=neutral, 2=positive
predictions = model.transform(parsed)

predictions = predictions.withColumn("sentiment_label",
    when(col("prediction") == 0, "negative")
    .when(col("prediction") == 1, "neutral")
    .otherwise("positive"))

# Confiance = max des probabilités du vecteur
vector_to_array = udf(lambda v: float(max(v.toArray())), DoubleType())
predictions = predictions.withColumn("confidence", vector_to_array(col("probability")))
```

**Résultat après prédiction pour chaque ligne :**

```
Id=12345 | ProductId=B001E4KFG0 | Summary="Good Quality Dog Food"
true_sentiment=2 | prediction=2.0 | sentiment_label="positive" | confidence=0.94
```

### 4.4 Deux streams de sortie lancés en parallèle

```python
# CHEMIN 1 : Kafka output — live feed (trigger 1s)
q_live = kafka_live.writeStream \
    .format("kafka") \
    .option("topic", "predictions-live") \
    .trigger(processingTime="1 second") \
    .start()

# CHEMIN 2 : MongoDB — stats dashboard (trigger 10s)
q_mongo = output.writeStream \
    .foreachBatch(write_to_mongo) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()  # attend que l'un s'arrête
```

---

## 5. Chemin 1 — Visualisation en continu

### Architecture complète

```
Spark prédit → writeStream.format("kafka")
                    │
            KAFKA TOPIC "predictions-live"
            (trigger 1s — 1-2 reviews/batch)
                    │
    Django thread KafkaConsumer (démarre au boot Daphne)
    lit chaque message dès qu'il arrive
                    │
    channel_layer.group_send("live_feed", {"type":"live.batch",...})
                    │
    LiveFeedConsumer.live_batch()   ← handler WebSocket
                    │
    self.send(JSON) → WebSocket TCP → Navigateur
                    │
    setFeeds([newCard, ...old])  ← React re-render instantané
```

### 5.1 Spark écrit dans Kafka — `consumer.py`

```python
from pyspark.sql.functions import to_json, struct as spark_struct

# Sélectionner seulement les champs utiles pour le live (pas tout le doc)
kafka_live = output.select(
    col("ProductId").cast("string").alias("key"),   # clé Kafka = ProductId
    to_json(spark_struct(
        col("Id").alias("id"),
        col("ProductId").alias("productId"),
        col("Summary").alias("summary"),
        col("sentiment_label").alias("sentiment"),
        col("confidence").alias("confidence"),
        col("Time").alias("time"),
    )).alias("value")    # valeur = JSON sérialisé
)

q_live = (kafka_live.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", "predictions-live")
    .option("checkpointLocation", "/tmp/spark-checkpoint-live")
    .trigger(processingTime="1 second")
    .start())
```

**Format du message dans `predictions-live` :**

```json
{
  "id": 12345,
  "productId": "B001E4KFG0",
  "summary": "Good Quality Dog Food",
  "sentiment": "positive",
  "confidence": 0.9412,
  "time": 1303862400
}
```

### 5.2 Django démarre le thread Kafka au boot — `apps.py`

```python
class ReviewsAppConfig(AppConfig):
    name = "reviews_app"

    def ready(self) -> None:
        # Appelé automatiquement par Django au démarrage de Daphne
        # Ne pas lancer pendant migrate, collectstatic, etc.
        import sys
        excluded = {"migrate", "makemigrations", "collectstatic", "shell"}
        if len(sys.argv) > 1 and sys.argv[1] in excluded:
            return
        from .kafka_live_consumer import start_kafka_live_consumer
        start_kafka_live_consumer()
```

### 5.3 Thread Django lit Kafka et forward au WebSocket — `kafka_live_consumer.py`

```python
def _run() -> None:
    from kafka import KafkaConsumer
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer

    # S'abonner au topic de sortie Spark
    consumer = KafkaConsumer(
        "predictions-live",
        bootstrap_servers="localhost:29092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # ignorer les anciens messages
        group_id="django-live-ws",
    )

    channel_layer = get_channel_layer()

    # Boucle infinie — chaque message = 1 review prédite par Spark
    for msg in consumer:
        prediction = msg.value   # dict Python déjà désérialisé

        # Broadcaster à TOUS les WebSocket connectés au groupe "live_feed"
        async_to_sync(channel_layer.group_send)(
            "live_feed",
            {
                "type":        "live.batch",
                "predictions": [prediction],   # 1 review à la fois
                "batch_id":    -1,
            },
        )
```

**Pourquoi `async_to_sync` ?**
Le thread Kafka est synchrone. `channel_layer.group_send` est une coroutine async. `async_to_sync` crée un event loop temporaire pour exécuter la coroutine depuis le thread sync.

### 5.4 WebSocket consumer envoie au navigateur — `consumers.py`

```python
class LiveFeedConsumer(AsyncWebsocketConsumer):

    async def connect(self) -> None:
        await self.accept()
        # Rejoindre le groupe → recevra tous les group_send
        await self.channel_layer.group_add("live_feed", self.channel_name)

        # Historique initial depuis MongoDB (20 dernières reviews)
        initial = await database_sync_to_async(mongo.get_recent_predictions)(20, None)
        await self.send(text_data=json.dumps({
            "type": "init", "predictions": initial,
            "counters": await self._counters(),
        }))

    async def live_batch(self, event: dict) -> None:
        """
        Handler déclenché automatiquement par group_send.
        type="live.batch" → Django Channels appelle live_batch()
        (convention : "." remplacé par "_" dans le nom du handler)
        """
        await self.send(text_data=json.dumps({
            "type":        "batch",
            "predictions": event.get("predictions", []),
            "counters":    await self._counters(),
            "source":      "kafka_direct",
        }))
```

**Chemin complet d'une review de Spark au navigateur :**

```
Spark trigger 1s → 1-2 reviews prédites
  → to_json(struct(...)) → message JSON dans "predictions-live"
  → KafkaConsumer thread Django reçoit msg.value (dict Python)
  → async_to_sync(channel_layer.group_send("live_feed", {...}))
  → live_batch() appelé pour CHAQUE connexion WebSocket ouverte
  → self.send(JSON) via WebSocket TCP
  → browser onMessage → setFeeds([newCard, ...old]) → React re-render
```

**Latence bout-en-bout : ~1-2 secondes** (Kafka append ultra rapide + trigger 1s)

---

## 6. Chemin 2 — Analyses Statistiques (MongoDB → Dashboard)

### Architecture

```
Spark foreachBatch (trigger 10s)
    │
    insert_many(rows) → MongoDB reviews_db.predictions
    │
Django REST API (mongo_client.py → api_views.py)
    │
Frontend setInterval(15s) → fetch() → Chart.js redraw
```

### 6.1 Spark écrit dans MongoDB — `consumer.py`

```python
def write_to_mongo(batch_df, batch_id):
    rows = batch_df.toPandas().to_dict(orient="records")

    for row in rows:
        row["inserted_at"] = datetime.utcnow()
        for k, v in row.items():
            if hasattr(v, 'item'):
                row[k] = v.item()   # numpy → Python natif

    client = MongoClient("mongodb://localhost:27018")
    client["reviews_db"]["predictions"].insert_many(rows)
    client.close()
    print(f"[MongoDB] ✅ Batch {batch_id} → {len(rows)} docs insérés")

q_mongo = output.writeStream \
    .foreachBatch(write_to_mongo) \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Format d'un document MongoDB :**

```json
{
  "_id": ObjectId("6641a3b2..."),
  "Id": 12345,
  "ProductId": "B001E4KFG0",
  "Summary": "Good Quality Dog Food",
  "Text": "I have bought several...",
  "cleaned": "bought several vitality canned dog food",
  "sentiment_label": "positive",
  "confidence": 0.9412,
  "inserted_at": ISODate("2026-05-06T16:05:00Z")
}
```

### 6.2 Django lit MongoDB via aggregation — `mongo_client.py`

```python
def get_global_distribution() -> dict:
    pipeline = [{"$group": {"_id": "$sentiment_label", "count": {"$sum": 1}}}]
    result = {"positive": 0, "neutral": 0, "negative": 0}
    for doc in get_collection().aggregate(pipeline):
        result[doc["_id"]] = doc["count"]
    return result   # → {"positive": 38412, "neutral": 9234, "negative": 8123}

def get_yearly_trend() -> list:
    pipeline = [
        {"$addFields": {"year": {"$year": {"$toDate": {"$multiply": ["$Time", 1000]}}}}},
        {"$group": {
            "_id": "$year",
            "positive": {"$sum": {"$cond": [{"$eq": ["$sentiment_label","positive"]}, 1, 0]}},
            "neutral":  {"$sum": {"$cond": [{"$eq": ["$sentiment_label","neutral"]},  1, 0]}},
            "negative": {"$sum": {"$cond": [{"$eq": ["$sentiment_label","negative"]}, 1, 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]
    return list(get_collection().aggregate(pipeline))
```

### 6.3 Frontend poll toutes les 15s — `page-dashboard.jsx`

```javascript
const [tick, setTick] = React.useState(0);

React.useEffect(() => {
    const interval = setInterval(async () => {
        const [kpiData, distData, recentData, trendData] = await Promise.all([
            window.API.getKpi(),
            window.API.getDistribution(),
            window.API.getRecent(50),
            window.API.getTrend('All'),
        ]);
        if (kpiData)    Object.assign(window.MOCK.kpi, kpiData);
        if (distData)   Object.assign(window.MOCK.distribution, distData);
        if (recentData?.data) window.MOCK.recentPredictions = recentData.data;
        if (trendData?.data)  window.MOCK.yearlyTrend = trendData.data;
        setTick(t => t + 1);   // force React à redessiner les graphes
    }, 15000);
    return () => clearInterval(interval);
}, []);
```

---

## 7. Frontend — comment le navigateur gère tout

### 7.1 Bootstrap au chargement — `api.js`

Avant que React s'affiche, `api.js` charge toutes les données en parallèle :

```javascript
async function bootstrap() {
    const [kpi, dist, yearly, conf, topPos, topNeg, recent, pipeline] =
        await Promise.all([
            API.getKpi(), API.getDistribution(), API.getTrend('All'),
            API.getConfusionMatrix(), API.getTopProducts('positive', 10),
            API.getTopProducts('negative', 10), API.getRecent(50),
            API.pipelineStatus(),
        ]);

    Object.assign(window.MOCK.kpi, kpi);
    Object.assign(window.MOCK.distribution, dist);
    window.MOCK.yearlyTrend = yearly.data;
    // ...
    window.renderApp();   // signal : React peut s'afficher
}
```

### 7.2 Connexion WebSocket — `api.js`

```javascript
connectLiveFeed(handlers = {}) {
    function open() {
        ws = new WebSocket(`ws://${location.host}/ws/live/`);
        ws.onopen    = () => handlers.onOpen();
        ws.onmessage = (ev) => handlers.onMessage(JSON.parse(ev.data));
        ws.onclose   = () => {
            // Reconnexion automatique exponentielle : 2s, 4s, 8s...
            setTimeout(open, 1000 * Math.pow(2, retry));
        };
    }
    open();
}
```

### 7.3 Réception des messages WebSocket — `page-live.jsx`

```javascript
window.API.connectLiveFeed({
    onMessage: (msg) => {
        if (msg.type === 'init') {
            // Premier message : historique MongoDB des 20 dernières reviews
            setFeeds(msg.predictions);
            setCounters(msg.counters);

        } else if (msg.type === 'batch') {
            // Nouvelle review depuis Kafka (source: "kafka_direct")
            if (!pausedRef.current) {
                setFeeds(prev => [...msg.predictions, ...prev].slice(0, 50));
            }
            setCounters(msg.counters);
        }
    },
});
```

### 7.4 Message reçu par le navigateur depuis Kafka

```json
{
  "type": "batch",
  "predictions": [{
    "id": "12345",
    "productId": "B001E4KFG0",
    "summary": "Good Quality Dog Food",
    "sentiment": "positive",
    "confidence": 0.94,
    "time": "1303862400"
  }],
  "counters": {"positive": 38415, "neutral": 9235, "negative": 8125},
  "source": "kafka_direct"
}
```

**Une seule review par message** → animation `slideDown` à chaque carte → effet visuel fluide.

---

## 8. Ce que tu dis au professeur pour chaque partie

### Sur les notebooks et le modèle

> *"Nous avons 4 notebooks : EDA pour explorer les données, preprocessing pour nettoyer le texte avec lemmatisation et suppression des stop-words, training où on entraîne une régression logistique dans un Pipeline Spark ML avec CrossValidation sur 3 folds. Le modèle atteint un F1-macro de 0.83. On sauvegarde `cv_model.bestModel` — c'est important car `cv_model` est un CrossValidatorModel qu'on ne peut pas recharger directement avec `PipelineModel.load()`."*

### Sur Kafka Producer

> *"Le producer lit le CSV de test et envoie chaque review sous forme de JSON dans le topic Kafka `amazon-reviews` avec un délai de 0.5s entre chaque message — 2 reviews par seconde — ce qui simule un flux de données réel venant d'Amazon."*

### Sur Spark Streaming

> *"Spark lit le topic `amazon-reviews` en mode Structured Streaming. Il lance deux streams en parallèle : le premier écrit dans Kafka `predictions-live` avec un trigger de 1 seconde pour le live feed, le second écrit dans MongoDB avec un trigger de 10 secondes pour les statistiques. Chaque micro-batch passe dans le modèle ML chargé — Tokenizer, TF-IDF, Régression Logistique — qui produit `sentiment_label` et `confidence` pour chaque review."*

### Sur le Chemin 1 — Visualisation en continu

> *"La sortie Spark vers Kafka `predictions-live` est lue par un thread Django KafkaConsumer qui démarre automatiquement au boot de Daphne via `apps.py ready()`. Ce thread reçoit chaque prédiction dès qu'elle arrive, appelle `channel_layer.group_send()` pour broadcaster au groupe WebSocket `live_feed`. Le consumer WebSocket `live_batch()` reçoit le message et l'envoie via WebSocket au navigateur. Le navigateur affiche la carte immédiatement avec une animation slide-in. La latence bout-en-bout est de 1 à 2 secondes."*

### Sur le Chemin 2 — Analyses Statistiques

> *"Parallèlement, Spark écrit les prédictions complètes dans MongoDB via `insert_many` toutes les 10 secondes. Le dashboard interroge Django toutes les 15 secondes via `setInterval`. Django exécute des aggregation pipelines MongoDB — group, sort, count — et retourne des JSON. Chart.js redessine les graphes à chaque réponse."*

### Sur la différence des deux chemins

> *"Les deux chemins sont complètement découplés via Kafka. Le chemin live n'a jamais besoin de lire MongoDB — il reçoit les données directement depuis Spark via le bus Kafka. MongoDB sert uniquement aux analyses statistiques. Cette architecture correspond exactement au schéma demandé : visualisation en continu d'un côté, analyses statistiques de l'autre."*

### Sur pourquoi Kafka comme sortie

> *"On aurait pu utiliser un HTTP POST de Spark vers Django, mais ça créerait un couplage fort — si Django est down, Spark échoue. Avec Kafka comme bus de sortie, Spark publie indépendamment et Django consomme indépendamment. C'est l'architecture standard dans les systèmes de streaming en production."*

---

## Résumé — Le voyage complet d'une review

```
ÉTAPE 1 — CSV → Kafka
  producer.py lit test_set.csv → envoie JSON dans "amazon-reviews" (0.5s/review)
  {"Id":12345, "ProductId":"B001E4KFG0", "Summary":"Good Dog Food", ...}

ÉTAPE 2 — Spark lit et prédit
  readStream("amazon-reviews") → from_json → DataFrame
  model.transform() → prediction=2, confidence=0.94
  withColumn("sentiment_label") → "positive"

ÉTAPE 3 — Deux sorties simultanées

  3a. CHEMIN LIVE (trigger 1s) :
      to_json(struct(id, productId, summary, sentiment, confidence, time))
      → writeStream.format("kafka") → topic "predictions-live"
      → KafkaConsumer thread Django reçoit msg
      → channel_layer.group_send("live_feed", {"type":"live.batch", predictions:[...]})
      → live_batch() → self.send(JSON via WebSocket)
      → browser : setFeeds([newCard, ...old]) → carte slide in

  3b. CHEMIN STATS (trigger 10s) :
      foreachBatch → toPandas() → insert_many() → MongoDB reviews_db.predictions
      → browser setInterval(15s) → fetch("/api/distribution")
      → Django aggregate $group → JsonResponse
      → setTick(t+1) → Chart.js redraw
```

---

*Document mis à jour — architecture Kafka output (mai 2026).*
