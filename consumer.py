from confluent_kafka import Consumer, KafkaError
from flask import Flask
import threading
import psycopg2
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Configura Kafka con TUS credenciales (variables de entorno)
KAFKA_CONFIG = {
    'bootstrap.servers':'d035kvrb92dfgde406p0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanism':'SCRAM-SHA-256',
    'sasl.username':'tony',
    'sasl.password':'1234',
    'group.id': 'acoustic-tracks-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Configuración de PostgreSQL en Aiven (Tus credenciales)
DB_PARAMS = {
    'dbname': 'defaultdb',
    'user': 'svnsadmin',
    'password': 'AVRS_dL4R6j8nZbXm_VWyNA',
    'host': 'pg-tony-antoniokmai-hua-af20.h.alwencloud.com',
    'port': '12917',
    'sslmode': 'require'  # SSL obligatorio en Aiven
}

TOPIC = 'acoustic_tracks'

def get_db_connection():
    """Conexión segura a PostgreSQL en Aiven con SSL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logging.info("Conexión a PostgreSQL establecida")
        return conn
    except Exception as e:
        logging.error(f"Error al conectar a PostgreSQL: {e}", exc_info=True)
        raise

def insert_track(track: dict):
    """Inserta una pista acústica en PostgreSQL. Campos ajustables según tu JSON."""
    required_fields = ['name', 'duration_ms', 'explicit', 'artists']
    
    if not all(field in track for field in required_fields):
        logging.warning(f"Campos faltantes: {track}")
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO acoustic_tracks (name, duration_ms, explicit, artists)
                    VALUES (%s, %s, %s, %s);
                """
                cur.execute(query, (
                    track.get('name'),
                    track.get('duration_ms', 0),
                    track.get('explicit', False),
                    json.dumps(track.get('artists', []))  # Convierte array a JSON
                ))
        logging.info(f"Pista insertada: {track.get('name')}")
    except Exception as e:
        logging.error(f"Error al insertar: {e}", exc_info=True)

def kafka_consumer_loop():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    logging.info(f"Escuchando tópico: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"Error en Kafka: {msg.error()}")
                break

            try:
                track = json.loads(msg.value().decode('utf-8'))
                insert_track(track)
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}", exc_info=True)
    finally:
        consumer.close()

@app.route("/health")
def health_check():
    return "ok", 200

def main():
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()
