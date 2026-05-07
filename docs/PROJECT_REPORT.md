# Pulpe — Amazon Fine Food Reviews · Sentiment Intelligence Platform

> **Rapport technique complet de l'écosystème — du dataset brut au dashboard temps réel**
> Stack : Pandas → Spark MLlib → Kafka → Spark Streaming → MongoDB → Django/Channels → React SPA

---

## Table des matières

1. [Vue d'ensemble & architecture](#1-vue-densemble--architecture)
2. [Arborescence du projet](#2-arborescence-du-projet)
3. [Dataset & preprocessing](#3-dataset--preprocessing)
4. [Notebooks (offline · ML)](#4-notebooks-offline--ml)
5. [Modèle ML & artefacts](#5-modèle-ml--artefacts)
6. [Infrastructure Docker](#6-infrastructure-docker)
7. [Kafka Producer (streaming source)](#7-kafka-producer-streaming-source)
8. [Spark Streaming Consumer](#8-spark-streaming-consumer)
9. [Backend Django](#9-backend-django)
10. [Frontend React (SPA)](#10-frontend-react-spa)
11. [Flux complet d'une review : du CSV au dashboard](#11-flux-complet-dune-review--du-csv-au-dashboard)
12. [Lancement & exploitation](#12-lancement--exploitation)
13. [Annexes : exemples de données](#13-annexes--exemples-de-données)

---

## 1. Vue d'ensemble & architecture

**Pulpe** est une plateforme bout-en-bout de classification de sentiment (3 classes : négatif / neutre / positif) sur le dataset **Amazon Fine Food Reviews** (568 454 avis). Elle simule un flux temps réel : un *producer* rejoue les avis vers Kafka, Spark Streaming les classifie via un modèle MLlib pré-entraîné, persiste les prédictions dans MongoDB, et un dashboard React lit l'état via API REST + WebSocket pour afficher KPIs, tendances, distribution, top produits, et un live-feed.

### Diagramme architecture

```
┌────────────────┐   CSV (test_set.csv)
│  Reviews.csv   │ ──► 02_preprocessing.ipynb ──► train.csv / val.csv / test_set.csv
└────────────────┘                                          │
                                                            ▼
                                              03_training.ipynb (Spark ML)
                                                            │
                                                            ▼
                                            ┌────────────────────────┐
                                            │  models/best_model/    │  PipelineModel
                                            │  + metadata.json       │  (LogReg, F1=0.83)
                                            └────────────────────────┘
                                                            ▲
              ┌─────────────────┐   JSON   ┌─────────────┐  │  load
              │ kafka_producer  │ ───────► │   Kafka     │  │
              │ producer.py     │  topic   │  port 29092 │  │
              └─────────────────┘ amazon-  └─────────────┘  │
                  reads test_set.csv     reviews            │
                                                            │
                                            ┌───────────────▼──────────┐
                                            │ spark_streaming/         │
                                            │ consumer.py              │
                                            │  · readStream(Kafka)     │
                                            │  · UDF clean_text        │
                                            │  · model.transform       │
                                            │  · foreachBatch → Mongo  │
                                            └────────────┬─────────────┘
                                                         │ insert_many
                                                         ▼
                                        ┌──────────────────────────────┐
                                        │ MongoDB :27018               │
                                        │  reviews_db.predictions      │
                                        └──────┬───────────────────────┘
                                               │ pymongo aggregations
                                               ▼
                              ┌─────────────────────────────────────┐
                              │ Django (Daphne ASGI :8000)          │
                              │  · reviews_app                       │
                              │     - api_views.py  (REST /api/*)    │
                              │     - consumers.py  (WebSocket)      │
                              │     - mongo_client.py (aggregations)│
                              │     - pipeline_manager.py (subprocs)│
                              │  · PostgreSQL :5433 (auth, audit,    │
                              │       saved dashboards, pipeline     │
                              │       state)                         │
                              └──────┬──────────────────────────────┘
                                     │ JSON / WebSocket
                                     ▼
                         ┌────────────────────────────┐
                         │ React SPA (Babel inline)   │
                         │  app.html + .jsx files     │
                         │  · Dashboard               │
                         │  · Live Feed (WS)          │
                         │  · Product Detail          │
                         │  · Pipeline Control        │
                         │  · Saved Dashboards / Admin│
                         └────────────────────────────┘
```

### Composants par rôle

| Couche | Tech | Rôle |
|---|---|---|
| **Stockage source** | CSV (data/) | Dataset brut + splits train/val/test |
| **Offline ML** | Jupyter, Pandas, NLTK, Spark MLlib | EDA, nettoyage, training, évaluation |
| **Modèle** | Spark `PipelineModel` | Tokenizer + HashingTF + IDF + LogisticRegression |
| **Broker** | Kafka 7.5 (Confluent) | Topic `amazon-reviews` |
| **Streaming** | Spark Structured Streaming 3.5 | foreachBatch micro-batches 10 s |
| **Sink temps réel** | MongoDB 7 | Collection `reviews_db.predictions` |
| **App backend** | Django 4.2 + Channels + Daphne | REST + WebSocket + pipeline control |
| **App relationnelle** | PostgreSQL 16 | Users, audit, saved dashboards, pipeline state |
| **Frontend** | React 18 (UMD via CDN, JSX in-browser via Babel Standalone) | SPA mono-template |

---

## 2. Arborescence du projet

```
amazon-reviews-bigdata/
├── data/
│   ├── Reviews.csv             ← dataset Kaggle brut (568 454 lignes)
│   ├── train.csv               ← split 80% (454 763)
│   ├── val.csv                 ← split 10% (56 845)
│   ├── test_set.csv            ← split 10% (56 846, rejoué via Kafka)
│   └── test_predictions.csv    ← prédictions hold-out (notebook 04)
├── notebooks/
│   ├── 01_eda.ipynb            ← exploration : distrib scores, longueurs, samples
│   ├── 02_preprocessing.ipynb  ← clean_text, score_to_label, splits
│   ├── 03_training.ipynb       ← Spark ML pipeline (LR + NB), choix LR
│   └── 04_evaluation.ipynb     ← métriques hold-out, confusion matrix
├── models/
│   ├── best_model/             ← Spark PipelineModel sérialisé
│   └── metadata.json           ← F1, accuracy, taille features, etc.
├── docs/
│   ├── confusion_matrix.png
│   └── preprocessing_stats.json
├── kafka_producer/
│   └── producer.py             ← rejoue test_set.csv vers Kafka
├── spark_streaming/
│   └── consumer.py             ← Structured Streaming + model + Mongo sink
├── django_app/
│   ├── manage.py
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── reviews_project/        ← settings, ASGI, root URLs
│   │   ├── settings.py
│   │   ├── asgi.py
│   │   └── urls.py
│   ├── reviews_app/            ← logique métier
│   │   ├── api_views.py        ← endpoints REST
│   │   ├── consumers.py        ← WebSocket LiveFeed
│   │   ├── mongo_client.py     ← agrégations MongoDB
│   │   ├── pipeline_manager.py ← lance/arrête producer + spark
│   │   ├── models.py           ← Postgres : SavedDashboard, AuditLog, PipelineState
│   │   ├── routing.py          ← routes WebSocket
│   │   └── urls.py             ← routes /api/
│   ├── templates/
│   │   ├── app.html            ← shell SPA unique
│   │   ├── login.html / register.html / 404.html
│   ├── static/js/
│   │   ├── api.js              ← client REST + bootstrap window.MOCK
│   │   ├── components.jsx      ← Sidebar, Topbar, Toast, KPI cards…
│   │   ├── pages-auth.jsx      ← LoginPage, RegisterPage
│   │   ├── page-dashboard.jsx  ← page principale
│   │   ├── page-live.jsx       ← live feed temps réel (WS)
│   │   ├── page-product.jsx    ← détail produit
│   │   └── pages-other.jsx     ← Pipeline, Saved, Admin
│   └── logs/
│       ├── django.log
│       ├── producer.log
│       └── spark.log
├── docker-compose.yml          ← Zookeeper, Kafka, Mongo, Postgres, UIs
└── start.sh                    ← bootstrap dev (docker up + migrate + daphne)
```

---

## 3. Dataset & preprocessing

### 3.1 Source

**Amazon Fine Food Reviews** (Kaggle, McAuley et al.) — 568 454 avis, colonnes :

| Colonne | Type | Exemple |
|---|---|---|
| `Id` | int | 1 |
| `ProductId` | str | `B001E4KFG0` |
| `UserId` | str | `A3SGXH7AUHU8GW` |
| `ProfileName` | str | `delmartian` |
| `HelpfulnessNumerator` | int | 1 |
| `HelpfulnessDenominator` | int | 1 |
| `Score` | int (1–5) | 5 |
| `Time` | int (UNIX seconds) | 1303862400 |
| `Summary` | str | `Good Quality Dog Food` |
| `Text` | str | `I have bought several of the Vitality canned dog food products and have found them all to be of good quality...` |

### 3.2 Score → label

```python
def score_to_label(s):
    if s < 3:  return 0  # negative
    if s == 3: return 1  # neutral
    return 2             # positive
```

Distribution (déséquilibrée — d'où le `balanced_size` à l'entraînement) :

```json
{ "negative": 82037, "neutral": 42640, "positive": 443777 }
```

### 3.3 `clean_text` (utilisé dans le notebook 02 *et* le consumer Spark)

```python
def clean_text(text):
    if text is None or str(text).strip() == '':
        return ""
    text = str(text).lower()
    text = re.sub(r'<.*?>', ' ', text)             # HTML
    text = re.sub(r'http\S+|www\.\S+', ' ', text)  # URLs
    text = re.sub(r'[^a-z\s]', ' ', text)          # ponctuation/chiffres
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = [lemmatizer.lemmatize(t)
              for t in text.split()
              if t not in stop_words and len(t) > 2]
    return ' '.join(tokens)
```

**Stopwords** : NLTK english **moins** les négations (`no, not, nor, never, neither, none`) — préserve la polarité.
**Lemmatisation** : `WordNetLemmatizer`.
**Filtre** : tokens de longueur > 2.

### 3.4 Splits (80/10/10 stratifiés)

| Split | Taille |
|---|---|
| train | 454 763 |
| val | 56 845 |
| test | 56 846 |

Fichier de stats : `docs/preprocessing_stats.json` (`avg_cleaned_len ≈ 275 caractères`).

---

## 4. Notebooks (offline · ML)

### 4.1 `01_eda.ipynb` — Exploration

- **Cellule 1** : imports (`pandas`, `numpy`, `matplotlib`, `seaborn`).
- **Cellule 2** : `pd.read_csv("../data/Reviews.csv")` → shape, nulls, dtypes.
- **Cellule 3** : barplot distribution `Score` 1–5.
- **Cellule 4** : création colonne `sentiment` (3 classes), affiche déséquilibre.
- **Cellule 5** : longueurs `Text`/`Summary` (moyenne, max, nulls).
- **Cellule 6** : échantillons par classe (sanity check).

### 4.2 `02_preprocessing.ipynb` — Nettoyage et splits

- Charge `Reviews.csv`, applique `score_to_label` et `clean_text`.
- Drop des `cleaned == ""` (avis vidés par le filtre).
- Split stratifié 80/10/10 → `train.csv`, `val.csv`, `test_set.csv`.
- Sauvegarde `docs/preprocessing_stats.json`.

### 4.3 `03_training.ipynb` — Entraînement Spark MLlib

Pipeline Spark ML :

```
Tokenizer → StopWordsRemover → HashingTF(numFeatures=20000) → IDF
         → [LogisticRegression | NaiveBayes]
```

**Stratégie** : sous-échantillonnage de la classe majoritaire (`balanced_size=202047`) pour réduire le biais positif. Deux modèles entraînés en parallèle, métrique de sélection = **F1 macro** sur `val`.

| Modèle | F1 val |
|---|---|
| LogisticRegression | **0.8274** ✅ retenu |
| NaiveBayes | 0.7872 |

Sauvegardé : `models/best_model/` (Spark `PipelineModel.save`).

### 4.4 `04_evaluation.ipynb` — Évaluation hold-out

- Charge `test_set.csv` + `models/best_model`.
- `model.transform(test_df)` → mappe `prediction` ∈ {0,1,2} → `sentiment_label`.
- Calcule F1, Accuracy, Precision, Recall via `MulticlassClassificationEvaluator`.
- `sklearn.metrics.confusion_matrix` + heatmap → `docs/confusion_matrix.png`.
- Exporte `data/test_predictions.csv` (utilisé éventuellement en backup d'analyse).

**Métriques hold-out** :

```json
{ "f1_test": 0.8255, "acc_test": 0.8008,
  "precision_test": 0.8703, "recall_test": 0.8008 }
```

---

## 5. Modèle ML & artefacts

### `models/metadata.json`

```json
{
  "model_type":     "LogisticRegression",
  "f1_val":         0.8274,
  "f1_lr":          0.8274,
  "f1_nb":          0.7872,
  "trained_on":     "2026-05-01 16:32:51",
  "num_features":   20000,
  "train_size":     454763,
  "balanced_size":  202047,
  "f1_test":        0.8255,
  "acc_test":       0.8008
}
```

### `models/best_model/`

Spark sérialise un `PipelineModel` en répertoire (un sous-dossier par stage : `Tokenizer_…`, `StopWordsRemover_…`, `HashingTF_…`, `IDFModel_…`, `LogisticRegressionModel_…`). Chargé en streaming via `PipelineModel.load(MODEL_PATH)`.

Sa colonne d'entrée est `cleaned` (string), sa colonne de sortie `prediction` (double) + `probability` (Vector dense de taille 3).

---

## 6. Infrastructure Docker

`docker-compose.yml` provisionne 6 conteneurs sur réseau `bigdata-net` :

| Service | Image | Ports (host:container) | Rôle |
|---|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | — | coordination Kafka |
| `kafka` | confluentinc/cp-kafka:7.5.0 | `9092:9092`, `29092:29092` | broker |
| `kafka-ui` | provectuslabs/kafka-ui | `8081:8080` | UI inspection topics |
| `mongo` | mongo:7 | **`27018:27017`** | sink des prédictions |
| `mongo-express` | mongo-express | `8082:8081` | UI Mongo |
| `postgres` | postgres:16 | `5433:5432` | DB Django (users, audit, dashboards) |

⚠ Le port host MongoDB est **27018** (et non 27017). Toutes les chaînes de connexion côté host doivent l'utiliser. Le code défensif rejette toute URI contenant `27017` ou `mongo:` (hostname Docker-internal qui ne résoudrait pas depuis l'host) :

```python
_raw_uri = os.getenv("MONGO_URI") or ""
if not _raw_uri or "27017" in _raw_uri or "mongo:" in _raw_uri:
    _raw_uri = "mongodb://localhost:27018"
```

### Listeners Kafka

```
KAFKA_ADVERTISED_LISTENERS:
  PLAINTEXT://kafka:9092            ← intra-Docker
  PLAINTEXT_HOST://localhost:29092  ← host (utilisé par producer.py et spark)
```

---

## 7. Kafka Producer (streaming source)

Fichier : `kafka_producer/producer.py` (~58 lignes).

### Rôle
Rejoue `data/test_set.csv` vers le topic Kafka `amazon-reviews`, ligne par ligne, avec un délai configurable — simule un flux temps réel.

### Variables d'env

```python
BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "amazon-reviews")
DELAY  = float(os.getenv("DELAY_SEC", "0.5"))
CSV_PATH = os.getenv("REVIEWS_CSV", "../data/test_set.csv")
```

### Format du message Kafka

Chaque ligne CSV est sérialisée JSON :

```json
{
  "Id": 12345,
  "ProductId": "B001E4KFG0",
  "UserId": "A3SGXH7AUHU8GW",
  "Time": 1303862400,
  "Summary": "Excellent product",
  "Text": "Best dog food I have ever tried...",
  "cleaned": "best dog food ever tried",
  "true_sentiment": 2
}
```

`key` = `ProductId` (pour partitionnement par produit).

### Boucle principale

```python
for _, row in df.iterrows():
    msg = { ...build dict... }
    producer.send(TOPIC, key=str(row["ProductId"]), value=msg)
    if sent % 100 == 0:
        print(f"[producer] Envoyé: {sent:,}/{len(df):,}")
    time.sleep(DELAY)
```

---

## 8. Spark Streaming Consumer

Fichier : `spark_streaming/consumer.py` (~162 lignes). Le cœur ML temps réel.

### 8.1 Initialisation

```python
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "amazon-reviews")
MONGO_URI    = "mongodb://localhost:27018"  (avec garde-fou)
MODEL_PATH   = os.getenv("MODEL_PATH", "./models/best_model")
CHECKPOINT   = "/tmp/spark-checkpoint-reviews"
```

`SparkSession` est démarré avec le package Maven `spark-sql-kafka-0-10_2.12:3.5.0` (téléchargé automatiquement par `spark-submit --packages`).

### 8.2 UDF de nettoyage (au cas où `cleaned` manque)

```python
clean_udf = udf(clean_text, StringType())

parsed = parsed.withColumn("cleaned",
    when(col("cleaned").isNull() | (col("cleaned") == ""),
         clean_udf(concat_ws(". ", col("Summary"), col("Text"))))
    .otherwise(col("cleaned")))
```

Garantit que la colonne `cleaned` est toujours non-nulle avant le modèle.

### 8.3 Lecture du flux Kafka

```python
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())
```

Le payload JSON est désérialisé via `from_json(value, schema)`.

### 8.4 Application du modèle

```python
model = PipelineModel.load(MODEL_PATH)  # chargé une fois au démarrage
predictions = model.transform(parsed)

predictions = predictions.withColumn("sentiment_label",
    when(col("prediction") == 0, "negative")
    .when(col("prediction") == 1, "neutral")
    .otherwise("positive"))

# Confidence = max de la Vector probability
vector_to_array = udf(lambda v: float(max(v.toArray())), DoubleType())
predictions = predictions.withColumn("confidence", vector_to_array(col("probability")))
```

### 8.5 Sink MongoDB via `foreachBatch`

```python
def write_to_mongo(batch_df, batch_id):
    if batch_df.count() == 0: return
    rows = batch_df.toPandas().to_dict(orient="records")
    for row in rows:
        row["inserted_at"] = datetime.utcnow()
        for k, v in row.items():
            if hasattr(v, 'item'):     # numpy → Python natif
                row[k] = v.item()
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client["reviews_db"]["predictions"].insert_many(rows)

query = (output.writeStream
    .foreachBatch(write_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start())
```

> **Note design** : on n'utilise pas `mongo-spark-connector` (lourd, fragile sur les versions). On prend la main avec `pymongo` dans `foreachBatch`, qui s'exécute côté driver.

### 8.6 Schéma final d'un document `predictions`

```json
{
  "_id": ObjectId("..."),
  "Id": 12345,
  "ProductId": "B001E4KFG0",
  "UserId": "A3SGXH7AUHU8GW",
  "Time": 1303862400,
  "Summary": "Excellent product",
  "Text": "Best dog food I have ever tried...",
  "true_sentiment": 2,
  "prediction": 2.0,
  "sentiment_label": "positive",
  "confidence": 0.9123,
  "inserted_at": ISODate("2026-05-04T...")
}
```

---

## 9. Backend Django

### 9.1 Configuration — `reviews_project/settings.py`

- `INSTALLED_APPS` : `daphne, channels, rest_framework, reviews_app`
- `MIDDLEWARE` : sécurité standard + WhiteNoise (statics)
- `ASGI_APPLICATION = "reviews_project.asgi.application"` (Daphne — supporte HTTP + WS)
- DB par défaut : **PostgreSQL** (port 5433)
- `MONGO_URI = os.getenv("MONGO_URI") or "mongodb://localhost:27018"`
  Le pattern `or` (au lieu de `os.getenv(k, default)`) gère le cas où la variable existe mais vaut `""`.
- `CHANNEL_LAYERS` : `InMemoryChannelLayer` (suffit pour single-process)
- `LOGGING` : un fichier rotatif `logs/django.log` + console

### 9.2 ASGI — `reviews_project/asgi.py`

```python
application = ProtocolTypeRouter({
    "http":      django_asgi_app,
    "websocket": AuthMiddlewareStack(URLRouter(websocket_urlpatterns)),
})
```

### 9.3 URLs racine — `reviews_project/urls.py`

```python
urlpatterns = [
    path("admin/",  admin.site.urls),
    path("",        app_views.app_view),         # SPA shell
    path("logout/", LogoutView.as_view(...)),
    path("api/",    include("reviews_app.urls")),
]
```

### 9.4 Vue SPA — `reviews_app/views.py::app_view`

Rend `app.html` en injectant `initial_page` (login si anonyme, dashboard si auth) et la liste des groupes utilisateur — consommé côté React via `window.DJANGO_CTX`.

### 9.5 Modèles PostgreSQL — `reviews_app/models.py`

| Modèle | Rôle |
|---|---|
| `ModelMetadata` | Historique de modèles entraînés (nom, F1, acc, actif) |
| `SavedDashboard` | Configurations sauvegardées par utilisateur (filtres year/sentiment/product) |
| `AuditLog` | Trace toutes les actions (`pipeline.start`, `dashboard.create`…) |
| `PipelineState` | **Singleton** (`pk=1`) avec `producer_pid`, `consumer_pid`, `status`, `started_by` |

### 9.6 Routes API — `reviews_app/urls.py`

| Endpoint | Méthode | Vue | Description |
|---|---|---|---|
| `/api/auth/login` | POST | `auth_login` | session login JSON |
| `/api/auth/register` | POST | `auth_register` | crée user + add to group `Viewer` |
| `/api/auth/status` | GET | `auth_status` | état session courant |
| `/api/kpi` | GET | `kpi` | totalPredictions, throughput, uniqueProducts, F1 |
| `/api/distribution` | GET | `distribution` | counts {pos, neu, neg} |
| `/api/trend?year=All\|2014` | GET | `trend` | yearly (granularité année) ou monthly |
| `/api/top-products?sentiment=pos&limit=10` | GET | `top_products` | top produits par sentiment |
| `/api/products?limit=2000` | GET | `all_products` | tous les produits distincts (dropdown) |
| `/api/words?limit=30` | GET | `word_frequencies` | top mots fréquents par classe |
| `/api/confusion-matrix` | GET | `confusion_matrix` | 3×3 (rows=true, cols=pred) |
| `/api/confidence-distribution` | GET | `confidence_distribution` | histo bins 0.5→0.95 |
| `/api/recent?limit=50&sentiment=neg` | GET | `recent` | dernières prédictions |
| `/api/product/<id>` | GET | `product_detail` | agrégation produit |
| `/api/product/<id>/reviews?page=1` | GET | `product_reviews` | reviews paginées |
| `/api/search?q=...` | GET | `search` | regex sur Summary/Text |
| `/api/pipeline/start\|stop` | POST | `pipeline_*` | spawn/kill subprocess (login analyst+) |
| `/api/pipeline/status` | GET | `pipeline_status` | PID + alive |
| `/api/pipeline/logs?lines=50` | GET | `pipeline_logs` | tail logs producer + spark |
| `/api/dashboards` | GET/POST | `dashboards` | CRUD saved dashboards user |
| `/api/dashboards/<id>` | DELETE | `dashboard_delete` | suppr. dashboard |
| `/api/health` | GET | `health` | mongo ping + timestamp |
| `/ws/live/` | WS | `LiveFeedConsumer` | poll Mongo toutes les 2 s, push nouveaux docs |

### 9.7 `mongo_client.py` — agrégations MongoDB

Fonctions clés (toutes décorées `@_safe` qui transforme `PyMongoError → MongoUnavailable`) :

#### `get_kpi_stats()`
```python
total = coll.estimated_document_count()
unique_products = len(coll.distinct("ProductId"))
recent = coll.count_documents({"inserted_at": {"$gte": now - 60s}})
throughput = recent / 60.0    # docs/sec sur la dernière minute
```

#### `get_global_distribution()`
```python
pipeline = [{"$group": {"_id": "$sentiment_label", "n": {"$sum": 1}}}]
```

#### `get_yearly_trend()` / `get_monthly_trend(year)`
Convertit `Time` (UNIX seconds) en date, groupe par `(year|month, sentiment_label)`, structure pour Chart.js (un point par année/mois × 3 séries).

#### `get_top_products(sentiment, limit)`
```python
[{"$match": {"sentiment_label": ...}},
 {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
 {"$sort": {"count": -1}},
 {"$limit": limit}]
```

#### `get_confusion_matrix()`
3×3 matrice depuis `(true_sentiment, prediction)`, retournée en ordre **positive, neutral, negative** pour matcher le frontend.

#### `get_confidence_distribution()`
Histogram sur 10 bins (0.5 → 0.95 step 0.05), une série par classe — alimente le Chart.js stacked bar.

#### `get_recent_predictions(limit, sentiment)` / `get_predictions_after(_id, limit)`
- Premier : tri `inserted_at DESC`
- Second : `_id > ObjectId(last)` — utilisé par le LiveFeed WebSocket pour ne renvoyer que les nouveautés.

#### `get_product_detail(product_id)`
Agrège total, distrib sentiments, breakdown annuel, et 20 dernières reviews du produit.

#### `get_word_frequencies(limit)`
```python
[{"$match": {"sentiment_label": label}},
 {"$sample": {"size": 5000}},
 {"$addFields": {"_src": {"$cond": {
     "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$cleaned", ""]}}, 2]},
     "then": "$cleaned",
     "else": {"$ifNull": ["$Text", ""]}}}}},
 {"$project": {"tokens": {"$split": [{"$toLower": "$_src"}, " "]}}},
 {"$unwind": "$tokens"},
 {"$match": {"tokens": {"$nin": _WORD_STOPWORDS}}},
 {"$match": {"tokens": {"$regex": "^[a-z]{3,15}$"}}},
 {"$group": {"_id": "$tokens", "n": {"$sum": 1}}},
 {"$sort": {"n": -1}}, {"$limit": limit}]
```

Fall-back sur `Text` quand `cleaned` est absent (anciens documents).

#### `_format_prediction(doc)`
Sérialiseur central : convertit `ObjectId → str`, `Time` UNIX → `YYYY-MM-DD`, **avec sanity check** sur la plage de dates valide (1995-2030) pour éviter les artefacts.

### 9.8 `pipeline_manager.py` — orchestration des subprocess

Classe `PipelineManager` (statique) qui :

- **`_spawn(cmd, log_path)`** : `subprocess.Popen(..., start_new_session=True)` — détache, redirige stdout/stderr vers fichier append, propage `MONGO_URI` et `KAFKA_BROKER` à l'enfant.
- **`start_producer`** / **`start_consumer`** / **`start_all`** : vérifie `_is_alive(pid)` (via `os.kill(pid, 0)`), spawn si nécessaire, persiste PID dans `PipelineState`.
- **`stop_all`** : `os.killpg(os.getpgid(pid), SIGTERM)` (kill group → tue Spark + ses workers).
- **`get_status`** : lit `PipelineState`, vérifie liveness des PIDs, met à jour status si processus mort.
- **`get_recent_logs(n)`** : tail efficace en lisant les 64 KB finaux des logs.

### 9.9 `consumers.py::LiveFeedConsumer`

WebSocket qui à la connexion :

1. Envoie un message `{type:"init", predictions:[...20], counters:{...}}`.
2. Lance une boucle qui toutes les 2 s appelle `mongo.get_predictions_after(_last_id, 20)`.
3. À chaque batch, push `{type:"batch", predictions:[...], counters:{...}}`.
4. Sur erreur Mongo, envoie `{type:"error"}` + retry.

`receive(ping)` répond `{type:"pong"}` pour keep-alive.

---

## 10. Frontend React (SPA)

### 10.1 Stratégie de rendu

Pas de bundler. React 18 UMD + Babel Standalone via CDN, JSX compilé **dans le navigateur**. Chaque fichier `.jsx` est servi en `<script type="text/babel">` dans `app.html`. C'est volontairement "dev mode permanent" — adapté à un projet académique mais pas prod-ready (perf compile JSX). Avantage : zéro toolchain.

### 10.2 `templates/app.html` — shell unique

- Charge fonts Google : **Inter** (body), **Plus Jakarta Sans** (titres), **JetBrains Mono** (code/IDs).
- Définit le thème CSS via variables (`--bg-base`, `--bg-card`, `--text-primary`...) — un thème dark par défaut + `.theme-light` override.
- Injecte le contexte Django :
  ```js
  window.DJANGO_CTX = {
    user, groups, initialPage, csrfToken, logoutUrl
  };
  ```
- Charge `api.js` puis les `.jsx` dans l'ordre, et bootstrap React via `window.renderApp()` une fois `window._apiBootstrapped = true`.
- Composant racine `App` :
  - Si `page === 'login' | 'register'` → rend `LoginPage`/`RegisterPage`.
  - Sinon → layout `Sidebar + Topbar + main(renderPage())`.
  - Routing interne par state local (`page`, `setPage`).

### 10.3 `static/js/api.js`

Module IIFE qui :

1. Définit `getCsrf()` (cookie `csrftoken` ou `DJANGO_CTX.csrfToken`).
2. Helpers `jget(path)` / `jsend(method, path, body)`.
3. **`window.API`** : surface complète des endpoints (kpi, distribution, trend, products, words, recent, search, product, dashboards, pipeline.*, logout, `connectLiveFeed(handlers)`).
4. **`window.MOCK`** : structure squelette avec valeurs vides (rend les composants safe avant que les données ne soient chargées).
5. `bootstrap()` : `Promise.all` parallélise tous les fetch initiaux (kpi, distribution, trend, confusionMatrix, confidence, top+/–, recent, pipeline status, dashboards, words). Fait du data-shaping :
   - Word freqs : `{text, value}` → `{text, size: 10 + value/max*32}` (taille font).
   - Pipeline : copie `consumer → sparkConsumer`, déclenche fetch async des logs.
6. À la fin (succès ou erreur) : `window._apiBootstrapped = true; window.renderApp()`.

### 10.4 `connectLiveFeed`

Wrapper WebSocket avec reconnection exponentielle (`1s → 2s → 4s → ...` plafonné à `1024s`). Expose `{close, send}`. Utilisé par `LiveFeedPage`.

### 10.5 Pages JSX

| Fichier | Composants exportés (globaux) |
|---|---|
| `components.jsx` | `Sidebar`, `Topbar`, `Toast`, `KpiCard`, `ChartCard`, `Pill`, `SentimentBadge` |
| `pages-auth.jsx` | `LoginPage`, `RegisterPage` |
| `page-dashboard.jsx` | `DashboardPage` — KPIs, trend chart (year selector), distribution donut, top products lists, confusion matrix, confidence histogram, recent predictions table |
| `page-live.jsx` | `LiveFeedPage` — live counters + scrolling list (via WS) |
| `page-product.jsx` | `ProductDetailPage` — switch product dropdown (lazy fetch all), yearly breakdown, paginated reviews |
| `pages-other.jsx` | `PipelinePage` (start/stop, logs), `SavedDashboardsPage`, `AdminPage` |

### 10.6 Charts

Tous les graphes utilisent **Chart.js 4.4** (chargé via CDN). Convention :

```jsx
const ref = React.useRef();
React.useEffect(() => {
  const chart = new Chart(ref.current, { type, data, options });
  return () => chart.destroy();
}, [deps]);
```

---

## 11. Flux complet d'une review : du CSV au dashboard

Suivons une ligne unique du dataset :

### Étape 1 — CSV

```
Id=42, ProductId=B001E4KFG0, Score=5,
Summary="Great quality", Text="Best dog food ever",
sentiment=2, cleaned="best dog food ever"
```
(déjà nettoyée par notebook 02)

### Étape 2 — Producer

```python
producer.send("amazon-reviews", key="B001E4KFG0", value={
   "Id": 42, "ProductId": "B001E4KFG0", "UserId": "...",
   "Time": 1303862400,
   "Summary": "Great quality", "Text": "Best dog food ever",
   "cleaned": "best dog food ever", "true_sentiment": 2
})
```

### Étape 3 — Spark Streaming readStream

```
raw.value = b'{"Id":42,...}'
parsed     = from_json(value, schema) → DataFrame[Id, ProductId, ..., cleaned, true_sentiment]
```

### Étape 4 — Modèle

```
Tokenizer:           cleaned="best dog food ever" → ["best","dog","food","ever"]
StopWordsRemover:    → ["best","dog","food","ever"]   (déjà filtrés en amont)
HashingTF(20000):    → SparseVector[20000] avec 4 indices
IDF:                 → SparseVector pondéré
LogisticRegression:  → prediction=2.0, probability=[0.04, 0.05, 0.91]
```

Ajouts post-modèle :

```
sentiment_label = "positive"
confidence      = max([0.04, 0.05, 0.91]) = 0.91
```

### Étape 5 — Sink Mongo (foreachBatch toutes les 10 s)

```
db.predictions.insert_many([{
  Id: 42, ProductId: "B001E4KFG0", Time: 1303862400,
  Summary: "Great quality", Text: "Best dog food ever",
  true_sentiment: 2, prediction: 2.0,
  sentiment_label: "positive", confidence: 0.91,
  inserted_at: ISODate("2026-05-04T15:23:14Z")
}])
```

### Étape 6 — Côté Django

- `GET /api/kpi` → `mongo.get_kpi_stats()` :
  - `total += 1`, throughput recalculé sur la dernière minute, `unique_products` recompté.
- `GET /api/distribution` → `{positive: ++, neutral, negative}`.
- `GET /api/trend?year=All` → la cellule `(2011, positive)` incrémentée.

### Étape 7 — WebSocket LiveFeed

Toutes les 2 s, `_consume_loop` :

```python
docs = mongo.get_predictions_after(self._last_id, 20)
# new doc (_id > last) trouvé
self.send({"type": "batch",
           "predictions": [_format_prediction(doc)],
           "counters": {"positive": ..., "neutral": ..., "negative": ...}})
```

### Étape 8 — React

Dans `LiveFeedPage` :

```jsx
ws.onmessage = (msg) => {
  if (msg.type === "batch") {
    setReviews(prev => [...msg.predictions, ...prev].slice(0, 100));
    setCounters(msg.counters);
  }
}
```

Une nouvelle ligne apparaît en haut de la liste avec son badge sentiment vert "positive · 91%" — l'utilisateur voit la review temps réel.

---

## 12. Lancement & exploitation

### 12.1 Bootstrap dev

```bash
./start.sh
```

Effectue :

1. `docker compose up -d postgres mongo kafka` (+ wait 25 s)
2. `source venv/bin/activate`
3. `python manage.py migrate --run-syncdb`
4. `daphne -b 0.0.0.0 -p 8000 reviews_project.asgi:application`

Puis : ouvrir `http://localhost:8000`, créer un compte, aller dans **Pipeline Control**, cliquer **Start Pipeline** (déclenche `producer.py` + `spark-submit consumer.py` en subprocess).

### 12.2 Logs

| Fichier | Contenu |
|---|---|
| `django_app/logs/django.log` | Erreurs Django + accès Mongo |
| `django_app/logs/producer.log` | Stdout/stderr de `producer.py` |
| `django_app/logs/spark.log` | Stdout/stderr de `spark-submit consumer.py` |

Lus côté UI via `GET /api/pipeline/logs?lines=50`.

### 12.3 UIs annexes

- **Kafka UI** : `http://localhost:8081` (inspecter topic `amazon-reviews`)
- **Mongo Express** : `http://localhost:8082`
- **Django Admin** : `http://localhost:8000/admin/`

### 12.4 Variables d'environnement (`.env` à la racine `django_app/`)

```ini
DJANGO_SECRET_KEY=...
DJANGO_DEBUG=1
POSTGRES_DB=reviews_app
POSTGRES_USER=django
POSTGRES_PASSWORD=django_pass
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
MONGO_URI=mongodb://localhost:27018
MONGO_DB=reviews_db
KAFKA_BROKER=localhost:29092
PROJECT_ROOT=/abs/path/to/amazon-reviews-bigdata
```

---

## 13. Annexes : exemples de données

### 13.1 Exemple ligne brute `Reviews.csv`

```csv
1,B001E4KFG0,A3SGXH7AUHU8GW,delmartian,1,1,5,1303862400,"Good Quality Dog Food","I have bought several of the Vitality canned dog food products and have found them all to be of good quality. The product looks more like a stew than a processed meat and it smells better. My Labrador is finicky and she appreciates this product better than most."
```

### 13.2 Après preprocessing (`train.csv` ligne)

```
Id=1, ProductId=B001E4KFG0, ..., sentiment=2,
cleaned="bought several vitality canned dog food product found quality product look like stew processed meat smell better labrador finicky appreciates product better"
```

### 13.3 Exemple message Kafka

```json
{"Id":1,"ProductId":"B001E4KFG0","UserId":"A3SGXH7AUHU8GW",
 "Time":1303862400,"Summary":"Good Quality Dog Food",
 "Text":"I have bought several of the Vitality...",
 "cleaned":"bought several vitality canned...",
 "true_sentiment":2}
```

### 13.4 Document MongoDB après prédiction

```json
{
  "_id":             {"$oid":"66...a4"},
  "Id":              1,
  "ProductId":       "B001E4KFG0",
  "UserId":          "A3SGXH7AUHU8GW",
  "Time":            1303862400,
  "Summary":         "Good Quality Dog Food",
  "Text":            "I have bought several of the Vitality...",
  "true_sentiment":  2,
  "prediction":      2.0,
  "sentiment_label": "positive",
  "confidence":      0.9412,
  "inserted_at":     {"$date":"2026-05-04T15:23:14.211Z"}
}
```

### 13.5 Réponse `/api/kpi`

```json
{
  "totalPredictions": 12453,
  "throughput":       8.42,
  "uniqueProducts":   1827,
  "f1Score":          82.74,
  "pipelineStatus":   "running"
}
```

### 13.6 Réponse `/api/trend?year=All`

```json
{
  "granularity": "yearly",
  "data": [
    {"year":"2007","positive":124,"neutral":18,"negative":31},
    {"year":"2008","positive":288,"neutral":42,"negative":67},
    ...
  ]
}
```

### 13.7 Réponse `/api/recent?limit=2`

```json
{
  "data": [
    {
      "id":12345, "_id":"66ab...", "productId":"B001E4KFG0",
      "summary":"Excellent product","text":"Best dog food...",
      "time":"2011-04-27","sentiment":"positive",
      "confidence":0.91,"trueSentiment":2,"prediction":2.0
    },
    { ... }
  ]
}
```

### 13.8 Frame WebSocket LiveFeed

```json
{
  "type": "batch",
  "predictions": [ {...prédiction formatée...} ],
  "counters":    {"positive":8412,"neutral":2103,"negative":1938}
}
```

---

## 14. Synthèse rapide — qui parle à qui

```
producer.py  ──KafkaProducer.send──►  Kafka(amazon-reviews)
                                        │
                                        ▼
consumer.py  ──readStream──────────►  parsed DF
            ──model.transform()────►  predictions DF
            ──foreachBatch─────────►  pymongo.insert_many ──►  Mongo

Django REST  ──pymongo aggregations──►  Mongo
Django WS    ──pymongo poll 2s──────►  Mongo
Django auth  ──ORM───────────────────►  Postgres

React        ──fetch /api/*──────────►  Django REST
React Live   ──WebSocket /ws/live/───►  Django Channels
React        ──POST /api/pipeline/start──► PipelineManager.spawn ──► subprocess(producer + spark-submit)
```

---

**Auteur** : généré à partir de l'analyse du dépôt `amazon-reviews-bigdata` le 2026-05-04.
**Version modèle** : LogisticRegression — F1 test = 0.8255.
**Stack version** : Spark 3.5, Kafka 7.5, MongoDB 7, Django 4.2, React 18.3.
