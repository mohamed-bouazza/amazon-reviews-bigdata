import os
import sys
import json
import time
import pandas as pd
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC  = os.getenv("KAFKA_TOPIC", "amazon-reviews")
DELAY  = float(os.getenv("DELAY_SEC", "0.5"))

print(f"[producer] Connexion à Kafka: {BROKER}", flush=True)
print(f"[producer] Topic: {TOPIC}", flush=True)

try:
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000,
    )
    print("[producer] Connecté à Kafka ✓", flush=True)
except Exception as e:
    print(f"[producer] ERREUR connexion Kafka: {e}", flush=True)
    sys.exit(1)

CSV_PATH = os.getenv("REVIEWS_CSV", os.path.join(os.path.dirname(__file__), "..", "data", "test_set.csv"))
print(f"[producer] Chargement: {CSV_PATH}", flush=True)
try:
    df = pd.read_csv(CSV_PATH)
except FileNotFoundError as e:
    print(f"[producer] ERREUR fichier introuvable: {e}", flush=True)
    sys.exit(1)

print(f"[producer] → {len(df):,} reviews à envoyer (delay={DELAY}s)", flush=True)

sent = 0
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
    producer.send(TOPIC, key=str(row["ProductId"]), value=msg)
    sent += 1
    if sent % 100 == 0:
        print(f"[producer] Envoyé: {sent:,}/{len(df):,}", flush=True)
    time.sleep(DELAY)

producer.flush()
print(f"[producer] ✅ Done! {sent:,} reviews envoyées", flush=True)
