# 🛒 Amazon Reviews — Big Data Sentiment Pipeline

> **Analyse de sentiment en temps réel** sur les avis Amazon avec Apache Kafka, Spark Streaming, Django et React.

---

## 📋 Table des matières

- [Architecture](#-architecture)
- [Résultats du Modèle ML](#-résultats-du-modèle-ml)
- [Stack Technique](#️-stack-technique)
- [Structure du Projet](#-structure-du-projet)
- [Lancement Rapide](#-lancement-rapide)
- [Fonctionnement du Pipeline](#-fonctionnement-du-pipeline)
- [Monitoring avec Apache Airflow](#-monitoring-avec-apache-airflow)
- [APIs REST](#-apis-rest)
- [Variables d'Environnement](#-variables-denvironnement)
- [Données](#-données)

---

## 🏗️ Architecture

```
CSV Amazon Reviews
        │
        ▼
┌─────────────────┐     ┌───────────────────┐     ┌──────────────────────────────────┐
│  Kafka Producer │────▶│   Apache Kafka    │────▶│     Spark Structured Streaming   │
│   producer.py   │     │  amazon-reviews   │     │  Tokenizer → HashingTF → IDF → LR│
│  0.5s / review  │     │                   │     │         (Spark MLlib)            │
└─────────────────┘     └───────────────────┘     └─────────────────┬────────────────┘
                                                                     │
                                                   ┌─────────────────┴──────────────────┐
                                                   │                 FORK               │
                                                   │                                    │
                                    ⚡ CHEMIN 1 — LIVE (1s)        🗄️ CHEMIN 2 — STATS (10s)
                                                   │                                    │
                                      Kafka: predictions-live        MongoDB insert_many
                                                   │                                    │
                                        KafkaConsumer Thread          Django REST API
                                                   │                                    │
                                      WebSocket /ws/live/            fetch() toutes 15s
                                                   │                                    │
                                        ┌──────────▼──────┐              ┌─────────────▼──────┐
                                        │    Live Feed    │              │     Dashboard      │
                                        │  slide-in ~1s  │              │    Chart.js 4.4    │
                                        └─────────────────┘              └────────────────────┘
                                                   │                                    │
                                                   └──────────────┬─────────────────────┘
                                                                  │  LIT DIRECTEMENT
                                                                  ▼
                                                       ┌─────────────────┐
                                                       │ Apache Airflow  │
                                                       │  Watchdog DAG   │  ← 100% indépendant
                                                       │  toutes les 5mn │
                                                       └─────────────────┘
```

---

## 📊 Résultats du Modèle ML

| Métrique | Valeur |
|---|---|
| **F1-score (test)** | **0.8320** |
| **Accuracy (test)** | 80.77% |
| **Precision** | 87.50% |
| **Recall** | 80.77% |
| **Dataset** | 568 454 avis Amazon |
| **Train / Val / Test** | 80% / 10% / 10% |
| **Classes** | Négatif · Neutre · Positif |
| **Algorithme** | LogisticRegression (Spark MLlib) |
| **Features** | HashingTF + IDF — 20 000 features |
| **Entraîné le** | 2026-05-05 |

---

## 🛠️ Stack Technique

| Composant | Technologie |
|---|---|
| **Message Broker** | Apache Kafka 7.5 (Confluent) + Zookeeper |
| **Stream Processing** | Apache Spark 3.5 (Structured Streaming) |
| **Machine Learning** | Spark MLlib — Pipeline (Tokenizer → HashingTF → IDF → LogisticRegression) |
| **Base de données — prédictions** | MongoDB 7 |
| **Base de données — auth/meta** | PostgreSQL 16 |
| **Backend** | Django 4.2 + Django Channels 4 + Daphne ASGI |
| **Frontend** | React 18.3 (SPA) + Chart.js 4.4 |
| **Monitoring** | Apache Airflow 2.9 — Watchdog DAG |
| **Containerisation** | Docker Compose |

---

## 📁 Structure du Projet

```
amazon-reviews-bigdata/
│
├── notebooks/
│   ├── 01_eda.ipynb                  # Exploration des données
│   ├── 02_preprocessing.ipynb        # Nettoyage, lemmatisation, stop-words
│   ├── 03_training.ipynb             # Entraînement CrossValidation 3-fold
│   └── 04_evaluation.ipynb           # F1-score, matrice de confusion
│
├── kafka_producer/
│   └── producer.py                   # Envoie les reviews dans Kafka (0.5s/review)
│
├── spark_streaming/
│   └── consumer.py                   # Spark Streaming : prédiction + 2 sorties
│
├── airflow/
│   └── dags/
│       └── pipeline_watchdog_dag.py  # Watchdog DAG — 7 checks toutes les 5mn
│
├── django_app/
│   ├── reviews_app/
│   │   ├── api_views.py              # REST API (KPI, distribution, trend, recent)
│   │   ├── consumers.py              # WebSocket consumer (Django Channels)
│   │   ├── kafka_live_consumer.py    # Thread KafkaConsumer → channel_layer
│   │   ├── mongo_client.py           # Requêtes MongoDB (aggregation pipelines)
│   │   ├── pipeline_manager.py       # Start/stop producer + spark-submit
│   │   └── models.py                 # Modèles PostgreSQL (auth, state, dashboards)
│   ├── static/js/
│   │   ├── page-live.jsx             # Live Feed Page (WebSocket)
│   │   ├── page-dashboard.jsx        # Dashboard (KPI, charts, table)
│   │   └── api.js                    # Client API + WebSocket
│   └── templates/                    # HTML templates
│
├── models/
│   └── metadata.json                 # Métriques du meilleur modèle
│
├── docs/
│   └── AIRFLOW_INTEGRATION.md        # Documentation complète Airflow
├── docker-compose.yml                # 6 services : Zookeeper, Kafka, MongoDB, PostgreSQL, UIs
└── start.sh                          # Script de démarrage complet
```

---

## 🚀 Lancement Rapide

### Prérequis

- Docker & Docker Compose
- Python 3.12
- Java 11+
- Apache Spark 3.5

### Démarrage

```bash
# 1. Cloner le repo
git clone https://github.com/mohamed-bouazza/amazon-reviews-bigdata.git
cd amazon-reviews-bigdata

# 2. Configurer les variables d'environnement
cp django_app/.env.example django_app/.env
# Éditer django_app/.env avec vos valeurs

# 3. Créer l'environnement Python
python3 -m venv venv
source venv/bin/activate
pip install -r django_app/requirements.txt

# 4. Lancer le pipeline complet
./start.sh
```

Ouvrir **http://localhost:8000** → créer un compte → **Pipeline Control** → **Start Pipeline**

### Services disponibles

| Service | URL |
|---|---|
| **Application** | http://localhost:8000 |
| **Kafka UI** | http://localhost:8081 |
| **Mongo Express** | http://localhost:8082 |
| **Django Admin** | http://localhost:8000/admin |
| **Airflow UI** | http://localhost:8080 |

---

## 🔄 Fonctionnement du Pipeline

### Étape 1 — Entraînement ML (offline)

Les 4 notebooks Jupyter entraînent le modèle étape par étape :

1. **`01_eda.ipynb`** — Exploration, distribution des sentiments, longueurs de texte
2. **`02_preprocessing.ipynb`** — Nettoyage HTML, lemmatisation NLTK, suppression des stop-words (négations conservées)
3. **`03_training.ipynb`** — Pipeline Spark MLlib, CrossValidation 3-fold, sauvegarde dans `best_model/`
4. **`04_evaluation.ipynb`** — F1-macro = **0.8320**, matrice de confusion

### Étape 2 — Streaming en temps réel

```
producer.py  ──▶  Kafka(amazon-reviews)  ──▶  Spark(model.transform())
                                                         │
                                       ┌─────────────────┴─────────────────┐
                              LIVE (1s)│                        STATS (10s)│
                   Kafka: predictions-live                MongoDB.predictions
                       KafkaConsumer Thread                  Django REST API
                          WebSocket push                       fetch 15s
                        Live Feed Page ⚡                    Dashboard 📊
```

#### ⚡ Chemin 1 — Visualisation en continu (latence ~1-2s)

1. Spark écrit dans le topic Kafka `predictions-live` toutes les **1 seconde**
2. Un thread Django consomme ce topic
3. Il broadcaste via `channel_layer.group_send()` → WebSocket → navigateur

#### 🗄️ Chemin 2 — Analyses statistiques

1. Spark écrit dans MongoDB via `foreachBatch` toutes les **10 secondes**
2. Le Dashboard React interroge l'API toutes les **15 secondes**
3. Django exécute des aggregation pipelines MongoDB (`$group`, `$sort`)
4. Chart.js redessine les graphes en temps réel

---

## 🌬️ Monitoring avec Apache Airflow

Airflow joue le rôle de **watchdog externe** — il ne contrôle rien, il observe.
Il est **100% indépendant** du backend Django : il interroge Kafka et MongoDB directement,
sans passer par l'API ni dépendre d'aucun signal extérieur.

### Architecture

```
┌──────────────────────────────────────────────────────┐
│                 PIPELINE PRINCIPAL                    │
│   producer.py → Kafka → Spark → Kafka / MongoDB      │
│                           ↑           ↑              │
└───────────────────────────┼───────────┼──────────────┘
                            │  LIT      │  DIRECTEMENT
                   ┌────────┴───────────┴────────┐
                   │      Apache Airflow          │
                   │  pipeline_watchdog DAG       │
                   │  7 checks · toutes les 5mn   │
                   └─────────────────────────────┘
```

**Ce qu'Airflow ne fait PAS :**
- ❌ Ne démarre pas le producer ni Spark
- ❌ N'appelle aucune API Django
- ❌ N'est pas importé dans le backend

**Ce qu'Airflow vérifie (7 checks) :**

| Task | Ce qu'elle vérifie | Source |
|---|---|---|
| `check_kafka_topics_exist` | Topics `amazon-reviews` + `predictions-live` présents | Kafka |
| `check_producer_traffic` | Offset `amazon-reviews` croît → producer actif | Kafka |
| `check_live_path` | Offset `predictions-live` croît → Spark + chemin live actifs | Kafka |
| `check_mongodb_stats_path` | Insertions MongoDB dans les 2 dernières minutes | MongoDB |
| `check_prediction_quality` | Sentiment non null, confiance moyenne > 0.5 | MongoDB |
| `check_pipeline_latency` | Dernière insertion < 60s (Spark ne lag pas) | MongoDB |
| `check_sentiment_balance` | Distribution cohérente (pas 95%+ positif) | MongoDB |

### Flux des tasks

```
start
  └─▶ check_kafka_topics_exist
        └─▶ check_producer_traffic
              ├─▶ check_live_path ──────────────┐
              └─▶ check_mongodb_stats_path ─────┤
                                                ▼
                                  check_prediction_quality
                                        └─▶ check_pipeline_latency
                                                └─▶ check_sentiment_balance
                                                        └─▶ all_ok
```

### Comportement au démarrage

Airflow démarre avec `docker-compose up`, avant que tu cliques les boutons du frontend.
Il utilise `AirflowSkipException` pour gérer cette période proprement :

| Phase | Moment | État dans l'UI |
|---|---|---|
| Pipeline non lancé | Runs #1, #2... | 🩷 Skipped — ni succès ni échec |
| Juste après Start Producer | Run suivant | ✅ Offsets enregistrés |
| Pipeline actif | Runs suivants | ✅ SUCCESS ou ❌ FAIL si problème |

> **Pourquoi 🩷 et pas ❌ ?**
> `AirflowSkipException` signifie "non applicable" — le pipeline n'est pas encore lancé,
> ce n'est pas une erreur. Dès que tu cliques Start Producer, les checks passent
> automatiquement au vert sans aucune intervention.

### Installation

```bash
# 1. Installer Airflow dans le venv existant
source venv/bin/activate
pip install apache-airflow==2.9.0 pymongo \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"

# 2. Initialiser
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init

# 3. Créer l'admin
airflow users create \
  --username admin --password admin123 \
  --firstname Mohamed --lastname Bouazza \
  --role Admin --email admin@example.com

# 4. Lancer
airflow scheduler &
airflow webserver --port 8080
```

Interface disponible sur **http://localhost:8080** → activer `pipeline_watchdog`

> Documentation complète dans [`docs/AIRFLOW_INTEGRATION.md`](docs/AIRFLOW_INTEGRATION.md)

---

## 📈 APIs REST

| Méthode | Endpoint | Description |
|---|---|---|
| `GET` | `/api/kpi` | Total prédictions, throughput, F1, produits uniques |
| `GET` | `/api/distribution` | Répartition positive / neutre / négative |
| `GET` | `/api/trend?year=All` | Évolution annuelle des sentiments |
| `GET` | `/api/recent?limit=50` | Dernières prédictions |
| `GET` | `/api/products/top` | Top produits par sentiment |
| `POST` | `/api/pipeline/start` | Démarrer producer + spark-submit |
| `POST` | `/api/pipeline/stop` | Arrêter le pipeline |
| `WS` | `/ws/live/` | Flux temps réel des prédictions |

---

## 🔐 Variables d'Environnement

Copier `.env.example` en `.env` dans `django_app/` :

```ini
DJANGO_SECRET_KEY=your-secret-key
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

## 📦 Données

| Fichier | Description | Taille |
|---|---|---|
| `data/test_set.csv` | Ensemble de test (10%) | ~57 MB |
| `data/val.csv` | Ensemble de validation (10%) | ~57 MB |
| `data/test_predictions.csv` | Prédictions du modèle sur le test set | ~60 MB |
| `data/Reviews.csv` | Dataset brut complet | 287 MB* |
| `data/train.csv` | Ensemble d'entraînement (80%) | 347 MB* |

> ⚠️ Les fichiers > 100 MB ne sont pas versionnés sur Git.
> Télécharger le dataset original depuis [Kaggle — Amazon Fine Food Reviews](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews).

---

## 👤 Auteurs

**Mohamed Bouazza**
**Chichaoui Oussama**
**El kajdouhi Mohamed Ayman**
**El Attari Taki Eddine**
GitHub : [@mohamed-bouazza](https://github.com/mohamed-bouazza)

---

<div align="center">

*Spark 3.5 · Kafka 7.5 · MongoDB 7 · Django 4.2 · React 18.3 · Airflow 2.9 · Docker Compose*

</div>
