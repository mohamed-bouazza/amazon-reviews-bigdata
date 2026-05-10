# 🎓 Guide de Présentation — Amazon Reviews Big Data Pipeline
**BOUAZZA Mohamed · CHICHAOUI Oussama · EL KAJDOUHI Mohamed Ayman · EL ATTARI Taki eddine**

---

## ⚡ COMMANDES DE DÉMARRAGE (dans l'ordre)

### Étape 0 — Ouvrir Windows Terminal (Ubuntu)
```
Win + R → wt → Enter
```

### Étape 1 — Lancer tous les containers Docker
```bash
cd ~/amazon-reviews-bigdata
docker compose up -d
```
Attendre ~10 secondes que tout soit UP.

Vérifier que tout tourne :
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Tu dois voir : zookeeper, kafka, kafka-ui, mongo, mongo-express, postgres, airflow — tous **Up**.

---

### Étape 2 — Lancer le serveur Django (Daphne)
**Dans un nouveau terminal onglet :**
```bash
cd ~/amazon-reviews-bigdata/django_app
source ../venv/bin/activate
daphne -b 0.0.0.0 -p 8000 reviews_project.asgi:application
```
Attendre de voir :
```
INFO  Kafka live consumer thread démarré
INFO  Kafka consumer connecté → topic: predictions-live
```

---

### Étape 3 — Accéder aux interfaces
| Interface | URL | Login |
|-----------|-----|-------|
| **Dashboard principal** | http://localhost:8000 | — |
| **Airflow (monitoring)** | http://localhost:8084 | admin / sDa5tt5AGvmgWUr6 |
| **Kafka UI** | http://localhost:8081 | — |
| **Mongo Express** | http://localhost:8082 | — |

---

### Étape 4 — Lancer le pipeline depuis l'UI
1. Aller sur **http://localhost:8000** → **Pipeline Control**
2. Cliquer **Start Pipeline** (lance Producer + Spark en un clic)
3. Les logs apparaissent en temps réel dans l'UI
4. Aller sur **Live Feed** → voir les prédictions arriver une par une
5. Aller sur **Dashboard** → voir les stats se remplir

---

## 🔄 RÉINITIALISATION COMPLÈTE (si besoin repart de zéro)

```bash
# 1. Vider MongoDB
docker exec mongo mongosh --eval "db.getSiblingDB('reviews_db').predictions.deleteMany({})"

# 2. Reset Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic amazon-reviews 2>/dev/null; sleep 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic predictions-live 2>/dev/null; sleep 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic amazon-reviews --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic predictions-live --partitions 4 --replication-factor 1

# 3. Supprimer les checkpoints Spark
rm -rf /tmp/spark-checkpoint-live /tmp/spark-checkpoint-mongo

# 4. Redémarrer Daphne (Ctrl+C puis relancer)
cd ~/amazon-reviews-bigdata/django_app
source ../venv/bin/activate
daphne -b 0.0.0.0 -p 8000 reviews_project.asgi:application

# 5. Dans le navigateur : Ctrl+Shift+R (hard refresh)
# 6. Depuis l'UI → Start Pipeline
```

---

## 📁 FICHIERS À EXPLIQUER AU PROF

---

### 1. `kafka_producer/producer.py`
**Ce que c'est :** Le producteur Kafka — lit le CSV de 56,846 reviews Amazon et les envoie une par une dans le topic `amazon-reviews`.

**Ce qu'on dit :**
> "On simule un flux temps réel en rejouant le dataset historique. Chaque review est sérialisée en JSON et publiée dans Kafka avec un délai configurable. Kafka garantit la persistance et le partitionnement — ici 4 partitions pour le parallélisme."

**Code clé :**
```python
producer.send('amazon-reviews', value=row.to_dict())
```

---

### 2. `spark_streaming/consumer.py`
**Ce que c'est :** Le cœur du pipeline — Spark Structured Streaming lit Kafka, applique le modèle ML, et écrit les résultats dans 2 destinations en parallèle.

**Ce qu'on dit :**
> "Spark lit le topic Kafka en streaming, nettoie le texte (lemmatisation, stopwords), applique notre modèle LogisticRegression entraîné, et produit une prédiction (positive/neutre/négative) avec un score de confiance. On a deux streams parallèles : un vers Kafka (trigger 1s pour le live feed) et un vers MongoDB (trigger 10s pour le dashboard)."

**Code clé :**
```python
# Stream 1 : live feed (1 seconde)
q_live = kafka_live.writeStream.format("kafka").trigger(processingTime="1 second").start()

# Stream 2 : dashboard MongoDB (10 secondes)
q_mongo = output.writeStream.foreachBatch(write_to_mongo).trigger(processingTime="10 seconds").start()

spark.streams.awaitAnyTermination()
```

---

### 3. `django_app/reviews_app/kafka_live_consumer.py`
**Ce que c'est :** Thread Python qui tourne en permanence dans Django, écoute `predictions-live` et pousse chaque prédiction vers le WebSocket via Django Channels.

**Ce qu'on dit :**
> "C'est le pont entre Kafka et le navigateur. Un thread daemon lit le topic Kafka en continu. Pour chaque message, il appelle `channel_layer.group_send` qui diffuse la prédiction à tous les clients WebSocket connectés. Le résultat : les cartes apparaissent une par une dans le Live Feed en temps réel, sans polling."

**Code clé :**
```python
for msg in consumer:
    async_to_sync(channel_layer.group_send)(
        "live_feed",
        {"type": "live.batch", "predictions": [msg.value]}
    )
```

---

### 4. `django_app/reviews_app/apps.py`
**Ce que c'est :** Démarre le thread Kafka automatiquement au boot de Django.

**Ce qu'on dit :**
> "La méthode `ready()` de Django AppConfig est appelée une seule fois au démarrage du serveur. On y démarre le thread Kafka consumer, ce qui garantit que le live feed est actif dès que Daphne lance."

---

### 5. `airflow/dags/pipeline_watchdog_dag.py`
**Ce que c'est :** DAG Airflow qui surveille le pipeline toutes les 5 minutes avec 7 checks automatiques.

**Ce qu'on dit :**
> "Le watchdog vérifie automatiquement l'état de santé du pipeline : est-ce que Kafka reçoit des messages ? Est-ce que Spark prédit correctement ? Est-ce que MongoDB se remplit ? Est-ce que la latence est acceptable ? Si tout va bien → vert. Si un composant est bloqué → rouge avec message d'erreur explicite."

**Les 7 checks :**
1. `check_kafka_topics_exist` — topics présents ?
2. `check_producer_traffic` — producer actif (offset qui progresse) ?
3. `check_live_path` — Spark écrit dans predictions-live ?
4. `check_mongodb_stats_path` — insertions récentes (<2min) ?
5. `check_prediction_quality` — confiance >50%, pas de nulls ?
6. `check_pipeline_latency` — dernière insertion <60s ?
7. `check_sentiment_balance` — distribution réaliste ?

---

### 6. `notebooks/` — Les notebooks de ML
**Ce que c'est :** Tout le travail de Data Science — EDA, preprocessing, entraînement des modèles, évaluation.

**Ce qu'on dit :**
> "On a testé plusieurs algorithmes : Naive Bayes, Random Forest, Logistic Regression. La Logistic Regression avec TF-IDF donne le meilleur F1-Score de 82.7% sur le test set. Le modèle entraîné est sauvegardé en format PipelineModel Spark pour être rechargé directement dans le consumer."

---

### 7. `docker-compose.yml`
**Ce que c'est :** L'infrastructure complète définie en un seul fichier.

**Ce qu'on dit :**
> "Toute l'infrastructure Big Data tient dans un seul fichier docker-compose : Zookeeper + Kafka pour le streaming, MongoDB pour le stockage des prédictions, PostgreSQL pour Django, et Airflow pour l'orchestration. Un seul `docker compose up -d` lance tout."

**Les 7 services :**
| Service | Rôle | Port |
|---------|------|------|
| zookeeper | Coordination Kafka | interne |
| kafka | Message broker | 9092/29092 |
| kafka-ui | Interface Kafka | 8081 |
| mongo | Stockage prédictions | 27018 |
| mongo-express | Interface MongoDB | 8082 |
| postgres | DB Django | 5433 |
| airflow | Orchestration/monitoring | 8084 |

---

## 🏗️ ARCHITECTURE EN 1 PHRASE

> **CSV → Producer → Kafka → Spark ML → (Kafka live + MongoDB) → (WebSocket temps réel + Dashboard REST API)**

```
[CSV 56k reviews]
      ↓ producer.py
[Kafka: amazon-reviews]
      ↓ Spark Structured Streaming
[Modèle LogReg TF-IDF]
      ↓                    ↓
[Kafka: predictions-live]  [MongoDB: reviews_db]
      ↓                         ↓
[Django Thread]           [Django REST /api/kpi]
      ↓                         ↓
[WebSocket]               [Dashboard (polling 15s)]
      ↓
[Live Feed navigateur]
```

---

## ❓ QUESTIONS FRÉQUENTES DU PROF

**Q: Pourquoi Kafka et pas directement Spark → MongoDB ?**
> Kafka découple le producteur du consommateur. Si Spark tombe, les messages restent dans Kafka. On peut aussi avoir plusieurs consommateurs en parallèle.

**Q: C'est quoi la différence entre le Live Feed et le Dashboard ?**
> Live Feed = WebSocket, prédictions en push (1 par 1, temps réel). Dashboard = REST API polling toutes les 15s, agrégats MongoDB.

**Q: Pourquoi deux streams Spark ?**
> Le stream live (1s) est optimisé pour la latence — envoie chaque batch immédiatement dans Kafka. Le stream MongoDB (10s) est optimisé pour le débit — insère en batch pour réduire les connexions.

**Q: Airflow est nécessaire ?**
> Non, le pipeline tourne sans Airflow. Airflow est la couche de monitoring/orchestration qui surveille que tout tourne bien et alerte si quelque chose se bloque.

**Q: F1-Score 82.7%, c'est bien ?**
> Oui pour de l'analyse de sentiments sur du texte brut non structuré. Le problème est déséquilibré (beaucoup de positifs), et on conserve les négations dans le preprocessing pour améliorer la détection du négatif.
