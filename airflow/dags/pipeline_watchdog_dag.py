from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import subprocess, os

default_args = {'owner': 'mohamed', 'retries': 1, 'retry_delay': timedelta(minutes=1), 'email_on_failure': False}
OFFSET_FILE = '/tmp/airflow_kafka_offsets.json'

def _get_topic_offset(topic):
    result = subprocess.run(['docker','exec','kafka','kafka-run-class.sh','kafka.tools.GetOffsetShell','--broker-list','localhost:9092','--topic',topic,'--time','-1'], capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(f'Kafka inaccessible: {result.stderr}')
    total = 0
    for line in result.stdout.strip().split('\n'):
        parts = line.split(':')
        if len(parts) == 3:
            try: total += int(parts[2])
            except: pass
    return total

def _load_offsets():
    import json
    if not os.path.exists(OFFSET_FILE): return {}
    with open(OFFSET_FILE) as f:
        try: return json.load(f)
        except: return {}

def _save_offsets(offsets):
    import json
    with open(OFFSET_FILE, 'w') as f: json.dump(offsets, f)

def check_kafka_topics(**ctx):
    result = subprocess.run(['docker','exec','kafka','kafka-topics.sh','--bootstrap-server','localhost:9092','--list'], capture_output=True, text=True, timeout=15)
    existing = set(result.stdout.strip().split('\n'))
    missing = {'amazon-reviews','predictions-live'} - existing
    if missing:
        raise AirflowSkipException(f'Pipeline pas encore lance - topics manquants: {missing}')
    print('OK Topics Kafka presents')

def check_producer_traffic(**ctx):
    topic = 'amazon-reviews'
    current = _get_topic_offset(topic)
    if current == 0:
        raise AirflowSkipException('amazon-reviews vide - producer pas encore lance')
    offsets = _load_offsets()
    prev = offsets.get(topic, -1)
    offsets[topic] = current
    _save_offsets(offsets)
    if prev == -1:
        print(f'INFO Premier run - offset reference: {current}')
        return
    delta = current - prev
    if delta == 0:
        raise ValueError(f'ERREUR amazon-reviews bloque offset={current} - Producer arrete - Verifier frontend [Start Producer]')
    print(f'OK Producer actif +{delta} messages offset={current}')

def check_live_path(**ctx):
    topic = 'predictions-live'
    current = _get_topic_offset(topic)
    if current == 0:
        raise AirflowSkipException('predictions-live vide - Spark pas encore lance')
    offsets = _load_offsets()
    prev = offsets.get(topic, -1)
    offsets[topic] = current
    _save_offsets(offsets)
    if prev == -1:
        print(f'INFO Premier run - offset reference: {current}')
        return
    delta = current - prev
    if delta == 0:
        raise ValueError(f'ERREUR predictions-live bloque offset={current} - Spark arrete - Verifier frontend [Start Consumer]')
    print(f'OK Chemin LIVE Spark actif +{delta} messages offset={current}')

def check_mongodb_stats_path(**ctx):
    from pymongo import MongoClient
    from datetime import timedelta as td
    client = MongoClient('mongodb://localhost:27018/', serverSelectionTimeoutMS=3000)
    coll = client['reviews_db']['predictions']
    total = coll.count_documents({})
    if total == 0:
        client.close()
        raise AirflowSkipException('MongoDB vide - pipeline pas encore tourne')
    two_min_ago = datetime.utcnow() - td(minutes=2)
    recent = coll.count_documents({'inserted_at': {'': two_min_ago}})
    client.close()
    if recent == 0:
        raise ValueError(f'ERREUR MongoDB bloque 0 insertion/2min total={total} - Spark foreachBatch en echec')
    print(f'OK MongoDB +{recent} docs/2min total={total}')

def check_prediction_quality(**ctx):
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27018/', serverSelectionTimeoutMS=3000)
    coll = client['reviews_db']['predictions']
    recent = list(coll.find().sort('inserted_at', -1).limit(50))
    if not recent:
        client.close()
        raise AirflowSkipException('Pas de predictions en base')
    null_s = sum(1 for d in recent if not d.get('sentiment_label'))
    avg_conf = sum(d.get('confidence', 0) for d in recent) / len(recent)
    client.close()
    if null_s > 10:
        raise ValueError(f'ERREUR {null_s}/50 docs sans sentiment_label - Modele ML KO')
    if avg_conf < 0.5:
        raise ValueError(f'ERREUR confiance moyenne {avg_conf:.2f} < 0.5 - Modele predit au hasard')
    print(f'OK Qualite predictions confiance={avg_conf:.2f} null={null_s}/50')

def check_pipeline_latency(**ctx):
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27018/', serverSelectionTimeoutMS=3000)
    last = client['reviews_db']['predictions'].find_one(sort=[('inserted_at', -1)])
    client.close()
    if not last:
        raise AirflowSkipException('MongoDB vide')
    lag = (datetime.utcnow() - last['inserted_at']).total_seconds()
    if lag > 60:
        raise ValueError(f'ERREUR Lag pipeline {lag:.0f}s - Spark bloque ou Kafka sature')
    print(f'OK Latence {lag:.1f}s < 60s')

def check_sentiment_balance(**ctx):
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27018/', serverSelectionTimeoutMS=3000)
    counts = {r['_id']: r['n'] for r in client['reviews_db']['predictions'].aggregate([{'': {'_id': '', 'n': {'': 1}}}])}
    total = sum(counts.values())
    client.close()
    if total < 100:
        raise AirflowSkipException(f'Seulement {total} docs - pas assez pour verifier equilibre')
    for label, count in counts.items():
        pct = count / total * 100
        if label == 'positive' and pct > 95:
            raise ValueError(f'ERREUR {pct:.1f}% positive - Modele predit tout positif')
        if label == 'negative' and pct > 60:
            raise ValueError(f'ERREUR {pct:.1f}% negative - Distribution anormale')
    dist = {k: f'{v/total*100:.1f}%' for k,v in counts.items()}
    print(f'OK Equilibre sentiments {dist} total={total}')

with DAG(dag_id='pipeline_watchdog', description='Watchdog Kafka+MongoDB - independant du backend', default_args=default_args, start_date=datetime(2025,1,1), schedule='*/5 * * * *', catchup=False, tags=['monitoring','kafka','spark','mongodb']) as dag:
    start = EmptyOperator(task_id='start')
    t1 = PythonOperator(task_id='check_kafka_topics_exist',  python_callable=check_kafka_topics)
    t2 = PythonOperator(task_id='check_producer_traffic',    python_callable=check_producer_traffic)
    t3 = PythonOperator(task_id='check_live_path',           python_callable=check_live_path)
    t4 = PythonOperator(task_id='check_mongodb_stats_path',  python_callable=check_mongodb_stats_path)
    t5 = PythonOperator(task_id='check_prediction_quality',  python_callable=check_prediction_quality)
    t6 = PythonOperator(task_id='check_pipeline_latency',    python_callable=check_pipeline_latency)
    t7 = PythonOperator(task_id='check_sentiment_balance',   python_callable=check_sentiment_balance)
    done = EmptyOperator(task_id='all_ok')
    start >> t1 >> t2 >> [t3, t4] >> t5 >> t6 >> t7 >> done
