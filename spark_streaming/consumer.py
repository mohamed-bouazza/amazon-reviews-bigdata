import os
import re
import nltk
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True)
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, concat_ws, coalesce, lit, udf,
    to_json, struct as spark_struct,
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DoubleType,
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient
from datetime import datetime

# ============ CONFIG ============
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "amazon-reviews")
LIVE_TOPIC   = "predictions-live"       # topic de sortie pour le live feed

_raw_uri = os.getenv("MONGO_URI") or ""
if not _raw_uri or "27017" in _raw_uri or "mongo:" in _raw_uri:
    _raw_uri = "mongodb://localhost:27018"
MONGO_URI  = _raw_uri
print(f"✅ MONGO_URI résolu: {MONGO_URI}")

MODEL_PATH = os.getenv("MODEL_PATH", "./models/best_model")

CHECKPOINT_LIVE  = "/tmp/spark-checkpoint-live"
CHECKPOINT_MONGO = "/tmp/spark-checkpoint-mongo"

# ============ CLEANING UDF ============
stop_words = set(stopwords.words('english'))
negations  = {'no', 'not', 'nor', 'never', 'neither', 'none'}
stop_words = stop_words - negations
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    if text is None or str(text).strip() == '':
        return ""
    text = str(text).lower()
    text = re.sub(r'<.*?>', ' ', text)
    text = re.sub(r'http\S+|www\.\S+', ' ', text)
    text = re.sub(r'[^a-z\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = [lemmatizer.lemmatize(t)
              for t in text.split()
              if t not in stop_words and len(t) > 2]
    return ' '.join(tokens)

clean_udf = udf(clean_text, StringType())

# ============ SPARK SESSION ============
spark = (SparkSession.builder
    .appName("AmazonSentimentStreaming")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark démarré")

# ============ CHARGER MODÈLE ============
print(f"Chargement modèle: {MODEL_PATH}")
model = PipelineModel.load(MODEL_PATH)
print("✅ Modèle chargé")

# ============ SCHÉMA KAFKA ============
schema = StructType([
    StructField("Id",             LongType(),    True),
    StructField("ProductId",      StringType(),  True),
    StructField("UserId",         StringType(),  True),
    StructField("Time",           LongType(),    True),
    StructField("Summary",        StringType(),  True),
    StructField("Text",           StringType(),  True),
    StructField("cleaned",        StringType(),  True),
    StructField("true_sentiment", IntegerType(), True),
])

# ============ LIRE KAFKA ============
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())

parsed = (raw
    .select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*"))

parsed = parsed.withColumn("cleaned",
    when(col("cleaned").isNull() | (col("cleaned") == ""),
         clean_udf(concat_ws(". ", col("Summary"), col("Text"))))
    .otherwise(col("cleaned")))

parsed = parsed.withColumn("cleaned", coalesce(col("cleaned"), lit("")))

# ============ PRÉDICTIONS ============
predictions = model.transform(parsed)

predictions = predictions.withColumn("sentiment_label",
    when(col("prediction") == 0, "negative")
    .when(col("prediction") == 1, "neutral")
    .otherwise("positive"))

vector_to_array = udf(
    lambda v: float(max(v.toArray())) if v is not None else 0.0,
    DoubleType()
)
predictions = predictions.withColumn("confidence", vector_to_array(col("probability")))

output = predictions.select(
    "Id", "ProductId", "UserId", "Time",
    "Summary", "Text", "cleaned", "true_sentiment",
    "prediction", "sentiment_label", "confidence"
)

# ================================================================
# CHEMIN 1 — LIVE FEED : Spark écrit dans Kafka "predictions-live"
#   → Django KafkaConsumer thread lit ce topic
#   → channel_layer.group_send → WebSocket → navigateur
#   Trigger: 1 seconde — chaque review pushée dès qu'elle est prédite
# ================================================================
kafka_live = output.select(
    col("ProductId").cast("string").alias("key"),
    to_json(spark_struct(
        col("Id").alias("id"),
        col("ProductId").alias("productId"),
        col("Summary").alias("summary"),
        col("sentiment_label").alias("sentiment"),
        col("confidence").alias("confidence"),
        col("Time").alias("time"),
    )).alias("value")
)

q_live = (kafka_live.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", LIVE_TOPIC)
    .option("checkpointLocation", CHECKPOINT_LIVE)
    .trigger(processingTime="1 second")
    .start())

print(f"✅ Stream LIVE démarré → topic '{LIVE_TOPIC}' (trigger 1s)")

# ================================================================
# CHEMIN 2 — STATS : Spark écrit dans MongoDB via pymongo (batch)
#   → Django REST API lit MongoDB
#   → Dashboard se rafraîchit toutes les 15s
#   Trigger: 10 secondes — insert_many en batch, efficace
# ================================================================
def write_to_mongo(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print(f"[MongoDB] Batch {batch_id} vide, skip")
        return

    rows = batch_df.toPandas().to_dict(orient="records")

    for row in rows:
        row["inserted_at"] = datetime.utcnow()
        for k, v in row.items():
            if hasattr(v, 'item'):
                row[k] = v.item()

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client["reviews_db"]["predictions"].insert_many(rows)
    client.close()
    print(f"[MongoDB] ✅ Batch {batch_id} → {count} docs insérés")

q_mongo = (output.writeStream
    .foreachBatch(write_to_mongo)
    .option("checkpointLocation", CHECKPOINT_MONGO)
    .trigger(processingTime="10 seconds")
    .start())

print("✅ Stream MONGO démarré → reviews_db.predictions (trigger 10s)")
print("🚀 Pipeline actif — en attente de messages Kafka...")

# Attendre que l'un des deux streams s'arrête (erreur ou stop)
spark.streams.awaitAnyTermination()
