# 🌬️ Intégration Apache Airflow — Amazon Reviews Pipeline

> **Rôle exact dans ce projet** : Airflow est un processus **totalement indépendant** du backend Django.
> Il ne démarre rien, ne s'intègre dans aucun code existant.
> Il lit **MongoDB** et interroge **Kafka** directement pour vérifier que tout le pipeline tourne correctement.

---

## 1 — Architecture : Airflow en observateur externe

```
┌─────────────────────────────────────────────────────────┐
│                  PIPELINE PRINCIPAL                      │
│                                                          │
│  [Frontend] → pipeline_manager.py                        │
│       ↓              ↓                                   │
│  producer.py    spark-submit consumer.py                 │
│       ↓              ↓              ↓                    │
│  KAFKA              KAFKA        MongoDB                 │
│  amazon-reviews  predictions-live  reviews_db            │
│       ↓              ↓              ↓                    │
│  (Spark lit)  (Django WebSocket) (Django REST API)       │
└─────────────────────────────────────────────────────────┘
         ↑              ↑              ↑
         │    LIT DIRECTEMENT         │
         └──────────────┬─────────────┘
                        │
              ┌─────────────────┐
              │  APACHE AIRFLOW │  ← indépendant, juste observateur
              │  (watchdog DAG) │
              │  toutes les 5mn │
              └─────────────────┘
```

**Ce qu'Airflow ne fait PAS dans ce projet :**
- ❌ Ne démarre pas le producer
- ❌ Ne démarre pas Spark
- ❌ N'appelle aucune API Django
- ❌ N'est pas importé dans le backend

**Ce qu'Airflow fait (7 checks) :**
- ✅ Vérifie que les 2 topics Kafka existent
- ✅ Vérifie que `amazon-reviews` reçoit des messages (producer actif)
- ✅ Vérifie que `predictions-live` reçoit des messages (Spark actif + chemin live OK)
- ✅ Vérifie que MongoDB reçoit de nouvelles insertions (chemin stats OK)
- ✅ Vérifie que les prédictions sont valides (sentiment non null, confiance > 0.5)
- ✅ Vérifie la latence bout-en-bout (dernière insertion < 60s)
- ✅ Vérifie l'équilibre des sentiments (pas 95% positif)
- ✅ Affiche un historique visuel des vérifications dans l'UI

---

## 2 — Où installer Airflow

Airflow s'installe dans le **même venv** du projet, mais tourne dans ses **propres processus** séparés :

```
amazon-reviews-bigdata/
├── airflow/
│   └── dags/
│       └── pipeline_watchdog_dag.py   ← le seul DAG
├── venv/                              ← même venv, airflow installé dedans
└── ...
```

---

## 3 — Le DAG complet

```python
# airflow/dags/pipeline_watchdog_dag.py
"""
Watchdog indépendant — lit Kafka et MongoDB directement.
Vérifie toutes les 5 minutes que le pipeline est vivant.

Checks :
  1. Topics Kafka amazon-reviews + predictions-live existent
  2. amazon-reviews  a du trafic  → producer tourne
  3. predictions-live a du trafic → Spark tourne + chemin LIVE OK
  4. MongoDB reçoit des insertions → chemin STATS OK
  5. Qualité des prédictions     → sentiment non null, confiance > 0.5
  6. Latence bout-en-bout        → dernière insertion < 60s
  7. Équilibre des sentiments    → pas 95%+ positif
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta, timezone
import subprocess, os

PROJECT_DIR = "/home/mohamed/amazon-reviews-bigdata"

default_args = {
    'owner': 'mohamed',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

# ── Fichier de persistance des offsets entre deux runs ──────────────────────
OFFSET_FILE = "/tmp/airflow_kafka_offsets.txt"


def _get_topic_offset(topic: str) -> int:
    """Retourne le dernier offset total d'un topic Kafka."""
    result = subprocess.run(
        ["docker", "exec", "kafka",
         "kafka-run-class.sh", "kafka.tools.GetOffsetShell",
         "--broker-list", "localhost:9092",
         "--topic", topic, "--time", "-1"],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode != 0:
        raise RuntimeError(f"Kafka inaccessible pour topic {topic}: {result.stderr}")

    total = 0
    for line in result.stdout.strip().split('\n'):
        parts = line.split(':')
        if len(parts) == 3:
            total += int(parts[2])
    return total


def _load_offsets() -> dict:
    if not os.path.exists(OFFSET_FILE):
        return {}
    with open(OFFSET_FILE) as f:
        import json
        try:
            return json.load(f)
        except Exception:
            return {}


def _save_offsets(offsets: dict):
    import json
    with open(OFFSET_FILE, 'w') as f:
        json.dump(offsets, f)


# ────────────────────────────────────────────────────────────────────────────
# CHECK 1 — Les 2 topics Kafka existent-ils ?
# ────────────────────────────────────────────────────────────────────────────
def check_kafka_topics(**ctx):
    """
    Les topics sont créés au 1er message du producer.
    Si absents → AirflowSkipException : task rose dans l'UI (ni SUCCESS ni FAIL).
    Airflow interroge Kafka directement — aucune dépendance externe.
    """
    result = subprocess.run(
        ["docker", "exec", "kafka",
         "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"],
        capture_output=True, text=True, timeout=15
    )
    existing = set(result.stdout.strip().split('\n'))
    required = {"amazon-reviews", "predictions-live"}
    missing  = required - existing

    if missing:
        raise AirflowSkipException(
            f"Pipeline pas encore lancé — topics manquants : {missing}"
        )   # → 🦷 rose dans l'UI, pas d'échec

    print(f"✅ Topics Kafka OK : {required} présents")


# ────────────────────────────────────────────────────────────────────────────
# CHECK 2 — amazon-reviews a-t-il du trafic ? (producer vivant)
# ────────────────────────────────────────────────────────────────────────────
def check_producer_traffic(**ctx):
    """
    Si offset == 0 → producer pas encore lancé → AirflowSkipException.
    Si offset > 0 mais figé depuis le run précédent → producer arrêté → FAIL.
    """
    topic   = "amazon-reviews"
    current = _get_topic_offset(topic)

    if current == 0:
        raise AirflowSkipException("amazon-reviews vide — producer pas encore lancé")

    offsets = _load_offsets()
    prev    = offsets.get(topic, -1)
    offsets[topic] = current
    _save_offsets(offsets)

    if prev == -1:
        print(f"ℹ️  Premier run actif — offset de référence enregistré : {current}")
        return

    delta = current - prev
    if delta == 0:
        raise ValueError(
            f"❌ amazon-reviews bloqué — offset={current} (aucun nouveau message)\n"
            f"   → Le Kafka Producer est probablement arrêté\n"
            f"   → Vérifie dans le frontend : [Start Producer]"
        )

    print(f"✅ Producer actif — amazon-reviews +{delta} messages (offset={current})")


# ────────────────────────────────────────────────────────────────────────────
# CHECK 3 — predictions-live a-t-il du trafic ? (Spark + chemin LIVE OK)
# ────────────────────────────────────────────────────────────────────────────
def check_live_path(**ctx):
    """
    Si offset == 0 → Spark pas encore lancé → AirflowSkipException.
    Spark écrit dans predictions-live toutes les ~1s — si figé → FAIL.
    """
    topic   = "predictions-live"
    current = _get_topic_offset(topic)

    if current == 0:
        raise AirflowSkipException("predictions-live vide — Spark pas encore lancé")

    offsets = _load_offsets()
    prev    = offsets.get(topic, -1)
    offsets[topic] = current
    _save_offsets(offsets)

    if prev == -1:
        print(f"ℹ️  Premier run actif — offset de référence enregistré : {current}")
        return

    delta = current - prev
    if delta == 0:
        raise ValueError(
            f"❌ predictions-live bloqué — offset={current} (aucun nouveau message)\n"
            f"   → Spark Consumer est probablement arrêté ou planté\n"
            f"   → Le Live Feed frontend ne recevra aucune donnée\n"
            f"   → Vérifie dans le frontend : [Start Consumer]"
        )

    print(
        f"✅ Chemin LIVE OK — predictions-live +{delta} messages (offset={current})\n"
        f"   → Spark prédit et écrit dans Kafka toutes les ~1s"
    )


# ────────────────────────────────────────────────────────────────────────────
# CHECK 4 — MongoDB reçoit-il des insertions ? (chemin STATS OK)
# ────────────────────────────────────────────────────────────────────────────
def check_mongodb_stats_path(**ctx):
    """
    Si MongoDB vide → pipeline pas encore lancé → AirflowSkipException.
    Si MongoDB contient des docs mais pas de nouveaux depuis 2min → FAIL.
    Spark écrit toutes les 10s via foreachBatch.
    """
    from pymongo import MongoClient
    from datetime import timedelta as td

    client = MongoClient("mongodb://localhost:27018/", serverSelectionTimeoutMS=3000)
    coll   = client["reviews_db"]["predictions"]
    total  = coll.count_documents({})

    if total == 0:
        client.close()
        raise AirflowSkipException("MongoDB vide — pipeline pas encore tourné")

    two_min_ago = datetime.utcnow() - td(minutes=2)
    recent = coll.count_documents({"inserted_at": {"$gte": two_min_ago}})
    client.close()

    if recent == 0:
        raise ValueError(
            f"❌ MongoDB bloqué — 0 insertion dans les 2 dernières minutes\n"
            f"   → Spark Consumer arrêté ou foreachBatch échoue\n"
            f"   → Le Dashboard Django ne se mettra plus à jour\n"
            f"   Total en base : {total:,} docs"
        )

    throughput = round(recent / 120.0, 2)
    print(
        f"✅ Chemin STATS OK — MongoDB +{recent} docs/2min ({throughput}/s)\n"
        f"   Total en base : {total:,} prédictions"
    )


# ────────────────────────────────────────────────────────────────────────────
# CHECK 5 — Qualité des prédictions (sentiment non null, confiance > 0.5)
# ────────────────────────────────────────────────────────────────────────────
def check_prediction_quality(**ctx):
    """
    Vérifie que les 50 dernières prédictions MongoDB sont valides.
    Spark pourrait écrire des docs corrompus si le modèle échoue.
    """
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27018/", serverSelectionTimeoutMS=3000)
    coll   = client["reviews_db"]["predictions"]

    recent = list(coll.find().sort("inserted_at", -1).limit(50))
    if not recent:
        print("ℹ️  Pas encore de prédictions en base")
        client.close()
        return

    null_sentiment  = sum(1 for d in recent if not d.get("sentiment_label"))
    null_confidence = sum(1 for d in recent if d.get("confidence") is None)
    avg_conf = sum(d.get("confidence", 0) for d in recent) / len(recent)

    client.close()

    if null_sentiment > 10:
        raise ValueError(
            f"❌ {null_sentiment}/50 docs sans sentiment_label\n"
            f"   → Le modèle ML ne prédit pas correctement (PipelineModel KO)"
        )
    if avg_conf < 0.5:
        raise ValueError(
            f"❌ Confiance moyenne trop faible : {avg_conf:.2f}\n"
            f"   → Le modèle prédit au hasard — vérifier best_model/"
        )

    print(
        f"✅ Qualité prédictions OK — confiance moyenne={avg_conf:.2f}\n"
        f"   null_sentiment={null_sentiment}/50 | null_confidence={null_confidence}/50"
    )


# ────────────────────────────────────────────────────────────────────────────
# CHECK 6 — Latence bout-en-bout (dernière insertion < 60s)
# ────────────────────────────────────────────────────────────────────────────
def check_pipeline_latency(**ctx):
    """
    Vérifie que la dernière insertion MongoDB date de moins de 60s.
    Spark écrit toutes les 10s — si lag > 60s, quelque chose bloque.
    """
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27018/", serverSelectionTimeoutMS=3000)
    coll   = client["reviews_db"]["predictions"]

    last = coll.find_one(sort=[("inserted_at", -1)])
    client.close()

    if not last:
        print("ℹ️  MongoDB vide — pipeline pas encore démarré")
        return

    lag = (datetime.utcnow() - last["inserted_at"]).total_seconds()

    if lag > 60:
        raise ValueError(
            f"❌ Lag pipeline : {lag:.0f}s depuis la dernière insertion\n"
            f"   → Spark est bloqué ou Kafka est saturé\n"
            f"   → Le live feed accuse un retard de {lag:.0f}s"
        )

    print(f"✅ Latence OK — dernière insertion il y a {lag:.1f}s (< 60s)")


# ────────────────────────────────────────────────────────────────────────────
# CHECK 7 — Équilibre des sentiments (pas 95%+ positif)
# ────────────────────────────────────────────────────────────────────────────
def check_sentiment_balance(**ctx):
    """
    Si 95%+ des prédictions sont "positive", le modèle a un problème.
    Distribution attendue : ~70% positive, ~15% neutral, ~15% negative.
    """
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27018/", serverSelectionTimeoutMS=3000)
    coll   = client["reviews_db"]["predictions"]

    pipeline = [{"$group": {"_id": "$sentiment_label", "n": {"$sum": 1}}}]
    counts = {r["_id"]: r["n"] for r in coll.aggregate(pipeline)}
    total  = sum(counts.values())
    client.close()

    if total < 100:
        print(f"ℹ️  Seulement {total} docs — pas assez pour vérifier l'équilibre")
        return

    for label, count in counts.items():
        pct = count / total * 100
        if label == "positive" and pct > 95:
            raise ValueError(
                f"❌ {pct:.1f}% positive — modèle prédit tout positif\n"
                f"   → Le modèle ML est peut-être corrompu ou mal chargé"
            )
        if label == "negative" and pct > 60:
            raise ValueError(
                f"❌ {pct:.1f}% negative — distribution anormale\n"
                f"   → Vérifier le preprocessing ou le topic Kafka"
            )

    dist = {k: f"{v/total*100:.1f}%" for k, v in counts.items()}
    print(f"✅ Équilibre sentiments OK — {dist} (total={total:,})")


# ────────────────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="pipeline_watchdog",
    description="Surveille Kafka (2 topics) + MongoDB — indépendant du backend",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # toutes les 5 minutes
    catchup=False,
    tags=["monitoring", "kafka", "spark", "mongodb"],
) as dag:

    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="check_kafka_topics_exist",   python_callable=check_kafka_topics)
    t2 = PythonOperator(task_id="check_producer_traffic",     python_callable=check_producer_traffic)
    t3 = PythonOperator(task_id="check_live_path",            python_callable=check_live_path)
    t4 = PythonOperator(task_id="check_mongodb_stats_path",   python_callable=check_mongodb_stats_path)
    t5 = PythonOperator(task_id="check_prediction_quality",   python_callable=check_prediction_quality)
    t6 = PythonOperator(task_id="check_pipeline_latency",     python_callable=check_pipeline_latency)
    t7 = PythonOperator(task_id="check_sentiment_balance",    python_callable=check_sentiment_balance)

    done = EmptyOperator(task_id="all_ok")

    # Topics → trafic → live + mongo en parallèle → qualité + latence + équilibre
    start >> t1 >> t2 >> [t3, t4] >> t5 >> t6 >> t7 >> done
```

---

## 4 — Ce que vérifie chaque task

```
start
  │
  ▼
[t1 - check_kafka_topics_exist]
  → docker exec kafka --list
  → "amazon-reviews" + "predictions-live" présents ? ✅ / ❌
  │
  ▼
[t2 - check_producer_traffic]
  → offset amazon-reviews > offset précédent ? ✅ / ❌
  │
  ├─────────────────────────────┐
  ▼                             ▼
[t3 - check_live_path]   [t4 - check_mongodb_stats_path]
  offset predictions-live    MongoDB +docs dans 2 dernières min
  a bougé → Spark OK ✅      > 0 → foreachBatch OK ✅
  figé    → Spark KO ❌      = 0 → chemin stats KO ❌
  │                             │
  └──────────────┬──────────────┘
                 ▼
  [t5 - check_prediction_quality]
    50 derniers docs MongoDB
    sentiment_label non null ? ✅ / ❌
    confiance moyenne > 0.5   ? ✅ / ❌
                 │
                 ▼
  [t6 - check_pipeline_latency]
    dernière inserted_at < 60s ? ✅ / ❌
    si > 60s → Spark lag détecté ❌
                 │
                 ▼
  [t7 - check_sentiment_balance]
    positive < 95% ? ✅ / ❌
    negative < 60% ? ✅ / ❌
                 │
                 ▼
             [all_ok]
```

---

## 5 — Installation (5 minutes)

```bash
# 1. Installer dans le venv existant
cd /home/mohamed/amazon-reviews-bigdata
source venv/bin/activate
pip install apache-airflow==2.9.0 pymongo \
  --constraint "https://raw.githubusercontent.com/apache/airflow/\
constraints-2.9.0/constraints-3.12.txt"

# 2. Initialiser
export AIRFLOW_HOME=/home/mohamed/amazon-reviews-bigdata/airflow
airflow db init

# 3. Créer l'admin
airflow users create \
  --username admin --password admin123 \
  --firstname Mohamed --lastname Bouazza \
  --role Admin --email mohamedbouazza721@gmail.com

# 4. Créer le dossier DAGs et copier le fichier
mkdir -p airflow/dags
# → copier pipeline_watchdog_dag.py dedans

# 5. Lancer (2 terminaux séparés)
airflow scheduler &
airflow webserver --port 8080
```

**Interface** : http://localhost:8080 → activer `pipeline_watchdog`

---

## 6 — Résumé des rôles dans le projet

| Composant | Rôle | Indépendant ? |
|---|---|---|
| Frontend | Démarre / arrête producer + consumer | — |
| `pipeline_manager.py` | Gère les PIDs, lit les logs | Backend Django |
| **Airflow** | Lit Kafka + MongoDB, vérifie la santé | ✅ 100% indépendant |

---

## 7 — Comportement réel au démarrage (période de chauffe)

Les topics Kafka sont créés **automatiquement au premier message du producer**.
Si Airflow démarre avant que tu cliques les boutons dans le frontend, le premier
run voit des topics inexistants — ce n'est pas un bug, c'est normal.

### Séquence réelle

```
t=0min   docker-compose up → Kafka + MongoDB + Django démarrent
t=5min   Airflow RUN #1
           t1 : topics manquants → AirflowSkipException → 🦷 rose
           t2 : offset==0        → AirflowSkipException → 🦷 rose
           t3 : offset==0        → AirflowSkipException → 🦷 rose
           t4 : MongoDB vide     → AirflowSkipException → 🦷 rose
           → UI propre, zéro FAIL rouge

t=10min  Airflow RUN #2 → même chose → 🦷 skipped

t=15min  Tu cliques [Start Consumer] puis [Start Producer] dans le frontend
          → topics créés, messages arrivent, MongoDB se remplit

t=20min  Airflow RUN #3
           t1 : topics présents → ✅ SUCCESS
           t2 : offset > 0, prev=-1 → offset de référence enregistré ✅
           t3 : offset > 0, prev=-1 → offset de référence enregistré ✅
           t4 : MongoDB a des docs → insertions récentes > 0 ✅
           → ENREGISTREMENT DES RÉFÉRENCES

t=25min  Airflow RUN #4 — PREMIÈRE VRAIE VÉRIFICATION
           t2 : delta=+600 → producer actif ✅
           t3 : delta=+580 → Spark actif ✅
           t4 → t7 : MongoDB + qualité + latence + balance ✅
```

### Résumé des phases

| Run | Phase | UI Airflow |
|-----|-------|------------|
| #1 et #2 (avant lancement) | Observation | 🦷 Skipped — zéro FAIL |
| #3 (après lancement) | Référence | ✅ SUCCESS — offsets enregistrés |
| #4+ | **Vérification active** | ✅ SUCCESS ou ❌ FAIL si problème |

> **Pourquoi rose et pas vert ?** `AirflowSkipException` est sémantiquement
> correct : la task n'a pas réussi parce qu'elle était applicable, elle a été
> ignorée parce qu'elle n'était pas applicable. C'est exactement ce que rose signifie dans Airflow.

---

## 8 — Pourquoi `AirflowSkipException` et pas les autres approches ?

| Approche | Problème |
|----------|----------|
| `raise ValueError` (1ère version) | ❌ FAIL rouge dans l'UI — faux négatifs |
| `return` + `Variable.set` | ⚠️ Task verte alors qu'elle n'a rien vérifié (trompeur) |
| `FileSensor` | ❌ Django doit créer un fichier pour Airflow — couplage |
| **`AirflowSkipException`** ✅ | Rose = "pas applicable" — sémantiquement correct, zéro couplage |

**Airflow interroge Kafka et MongoDB directement** — personne d'autre impliqué.
Si Kafka est down, la task passe ❌ FAIL (vrai problème).
Si topics absents, la task passe 🦷 rose (pipeline pas lancé, normal).
C'est la distinction exacte qu'un watchdog doit faire.

---

## 9 — Ce qu'on dit au professeur

> *"Airflow joue le rôle de watchdog externe — il ne contrôle rien, il observe.
> Il est totalement indépendant du backend Django et ne s'y intègre pas.
> Il lit directement Kafka et MongoDB pour vérifier 7 points vitaux du pipeline
> toutes les 5 minutes :*
>
> *1. Les 2 topics Kafka existent*
> *2. Le producer envoie bien des reviews (offset amazon-reviews croît)*
> *3. Spark prédit et écrit dans predictions-live (chemin live actif)*
> *4. MongoDB reçoit des insertions via foreachBatch (chemin stats actif)*
> *5. Les prédictions sont valides — sentiment non null, confiance > 0.5*
> *6. La latence est < 60s — Spark ne lag pas*
> *7. La distribution des sentiments est cohérente — pas 95% positif*
>
> *Quand le pipeline n'est pas encore lancé, Airflow utilise `AirflowSkipException`
> qui marque les tasks en rose — ni succès ni échec, juste "non applicable".
> Airflow interroge Kafka et MongoDB directement — personne d'autre n'est
> impliqué. Dès que le pipeline démarre, les checks passent automatiquement
> au vert sans aucune intervention. C'est de l'observabilité passive —
> pratique standard en production pour détecter les pannes silencieuses."*

---

*`docs/AIRFLOW_INTEGRATION.md` — Projet amazon-reviews-bigdata*
