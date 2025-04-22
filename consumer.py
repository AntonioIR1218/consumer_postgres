from confluent_kafka import Consumer, KafkaError
from flask import Flask, jsonify
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

# Configuración de Kafka (tus credenciales actuales)
KAFKA_CONFIG = {
    'bootstrap.servers': 'd03h267mtrpq60sg5cf0.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'tony',
    'sasl.password': '1234',
    'group.id': 'acoustic-tracks-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Configuración PostgreSQL (tus credenciales actuales)
DB_PARAMS = {
    'dbname': 'defaultdb',
    'user': 'svnsadmin',
    'password': 'AVRS_dL4R6j8nZbXm_VWyNA',
    'host': 'pg-tony-antoniokmai-hua-af20.h.alwencloud.com',
    'port': '12917',
    'sslmode': 'require'
}

TOPIC = 'acoustic_tracks'

def get_db_connection():
    """Conexión segura a PostgreSQL con manejo de errores mejorado"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logging.info("✅ Conexión a PostgreSQL establecida")
        return conn
    except Exception as e:
        logging.error(f"❌ Error al conectar a PostgreSQL: {e}", exc_info=True)
        raise

def create_table_if_not_exists():
    """Crea la tabla con la nueva estructura si no existe"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS acoustic_tracks (
                        id SERIAL PRIMARY KEY,
                        track_id VARCHAR(255) UNIQUE,
                        artist TEXT,
                        title TEXT,
                        genre VARCHAR(100),
                        tempo FLOAT,
                        duration_ms INTEGER DEFAULT 0,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                logging.info("✔️ Tabla verificada/creada")
    except Exception as e:
        logging.error(f"Error al crear tabla: {e}", exc_info=True)

def insert_track(track: dict):
    """Inserta pistas con la estructura real de los datos"""
    required_fields = ['track_id', 'artists', 'title']
    
    if not all(field in track for field in required_fields):
        logging.warning(f"🚨 Registro omitido (campos faltantes): {json.dumps(track)}")
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO acoustic_tracks (
                        track_id, artist, title, genre, tempo, duration_ms
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (track_id) DO NOTHING;
                """
                cur.execute(query, (
                    track.get('track_id'),
                    track.get('artists'),
                    track.get('title'),
                    track.get('track_genre', 'unknown'),
                    float(track.get('tempo', 0)),
                    int(track.get('duration_ms', 0))
                ))
        logging.info(f"🎵 Insertada: {track.get('title')} - {track.get('artists')}")
    except Exception as e:
        logging.error(f"💥 Error al insertar: {e}", exc_info=True)

def kafka_consumer_loop():
    """Consume mensajes de Kafka y los procesa"""
    create_table_if_not_exists()  # Asegura que la tabla exista
    
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    logging.info(f"🔊 Suscrito al tópico: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"❌ Error en Kafka: {msg.error()}")
                break

            try:
                track = json.loads(msg.value().decode('utf-8'))
                insert_track(track)
            except json.JSONDecodeError:
                logging.error(f"📛 Mensaje no JSON: {msg.value()}")
            except Exception as e:
                logging.error(f"⚠️ Error procesando mensaje: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("🛑 Consumer cerrado")

@app.route("/health")
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "kafka-consumer",
        "kafka_topic": TOPIC
    }), 200

def main():
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()
