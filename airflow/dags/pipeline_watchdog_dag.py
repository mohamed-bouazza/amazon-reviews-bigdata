from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import os, json

default_args = {'owner': 'mohamed', 'retries': 1, 'retry_delay': timedelta(minutes=1), 'email_on_failure': False}
OFFSET_FILE = '/tmp/airflow_kafka_offsets.json'

# Adresses Docker internes (réseau bigdata-net)
KAFKA_BROKER  = 'kafka:9092'
MONGO_URI     = 'mongodb://mongo:27017'

def _load_offsets():
    if not os.path.exists(OFFSET_FILE): return {}
    with open(OFFSET_FILE) as f:
        try: return json.load(f)
        except: return {}

def _save_offsets(offsets):
    with open(OFFSET_FILE, 'w') as f: json.dump(offsets, f)

def _get_topic_offset(topic):
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return 0
    tps = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(tps)
    consumer.close()
    return sum(end_offsets.values())

def check_kafka_topics(**ctx):
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
    try:
        client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, request_timeout_ms=5000)
        topics = set(client.list_topics())
        client.close()
    except NoBrokersAvailable:
        raise AirflowSkipException('Kafka inaccessible - pipeline pas encore lance')
    missing = {'amazon-reviews', 'predictions-live'} - topics
    if missing:
        raise AirflowSkipException(f'Topics manquants: {missing}')
    print(f'OK Topics Kafka presents: {topics & {"amazon-reviews","predictions-live"}}')

def check_producer_traffic(**ctx):
    topic = 'amazon-reviews'
    try:
        current = _get_topic_offset(topic)
    except Exception as e:
        raise AirflowSkipException(f'Kafka inaccessible: {e}')
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
        raise ValueError(f'ERREUR amazon-reviews bloque offset={current} - Producer arrete')
    print(f'OK Producer actif +{delta} messages offset={current}')

def check_live_path(**ctx):
    topic = 'predictions-live'
    try:
        current = _get_topic_offset(topic)
    except Exception as e:
        raise AirflowSkipException(f'Kafka inaccessible: {e}')
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
        raise ValueError(f'ERREUR predictions-live bloque offset={current} - Spark arrete')
    print(f'OK Chemin LIVE Spark actif +{delta} messages offset={current}')

def check_mongodb_stats_path(**ctx):
    from pymongo import MongoClient
    from datetime import timedelta as td
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        coll = client['reviews_db']['predictions']
        total = coll.count_documents({})
    except Exception as e:
        raise AirflowSkipException(f'MongoDB inaccessible: {e}')
    if total == 0:
        client.close()
        raise AirflowSkipException('MongoDB vide - pipeline pas encore tourne')
    two_min_ago = datetime.utcnow() - td(minutes=2)
    recent = coll.count_documents({'inserted_at': {'$gte': two_min_ago}})
    client.close()
    if recent == 0:
        raise ValueError(f'ERREUR MongoDB bloque 0 insertion/2min total={total} - Spark foreachBatch en echec')
    print(f'OK MongoDB +{recent} docs/2min total={total}')

def check_prediction_quality(**ctx):
    from pymongo import MongoClient
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        coll = client['reviews_db']['predictions']
        recent = list(coll.find().sort('inserted_at', -1).limit(50))
    except Exception as e:
        raise AirflowSkipException(f'MongoDB inaccessible: {e}')
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
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        last = client['reviews_db']['predictions'].find_one(sort=[('inserted_at', -1)])
        client.close()
    except Exception as e:
        raise AirflowSkipException(f'MongoDB inaccessible: {e}')
    if not last:
        raise AirflowSkipException('MongoDB vide')
    lag = (datetime.utcnow() - last['inserted_at']).total_seconds()
    if lag > 60:
        raise ValueError(f'ERREUR Lag pipeline {lag:.0f}s - Spark bloque ou Kafka sature')
    print(f'OK Latence {lag:.1f}s < 60s')

def check_sentiment_balance(**ctx):
    from pymongo import MongoClient
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        counts = {r['_id']: r['n'] for r in client['reviews_db']['predictions'].aggregate([
            {'$group': {'_id': '$sentiment_label', 'n': {'$sum': 1}}}
        ])}
        client.close()
    except Exception as e:
        raise AirflowSkipException(f'MongoDB inaccessible: {e}')
    total = sum(counts.values())
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

with DAG(dag_id='pipeline_watchdog', description='Watchdog Kafka+MongoDB - independant du backend',
         default_args=default_args, start_date=datetime(2025,1,1),
         schedule='*/5 * * * *', catchup=False,
         tags=['monitoring','kafka','spark','mongodb']) as dag:
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
