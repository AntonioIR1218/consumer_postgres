from confluent_kafka import Consumer, KafkaError
from flask import Flask, jsonify
import threading
import psycopg2
import logging
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Configuración de Kafka
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

# Configuración de PostgreSQL
DB_PARAMS = {
    'dbname': 'defaultdb',
    'user': 'svnsadmin',
    'password': 'AVRS_dL4R6j8nZbXm_VWyNA',
    'host': 'hua-af20.aivencloud.com',
    'port': '12917',
    'sslmode': 'require'
}

TOPIC = 'acoustic_tracks'

def get_db_connection():
    """Conexión con reintentos"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            logging.info("✅ Conexión a PostgreSQL establecida")
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"⚠️ Intento {attempt + 1} fallido: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

def create_table_if_not_exists():
    """Crea la tabla si no existe"""
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

def safe_json_parse(message_str):
    """Intenta parsear el mensaje de diferentes formas"""
    try:
        # Intenta como JSON directo
        return json.loads(message_str)
    except json.JSONDecodeError:
        try:
            # Intenta eliminar comillas extras
            return json.loads(message_str.strip('"'))
        except json.JSONDecodeError:
            logging.error(f"No se pudo parsear el mensaje: {message_str}")
            return None

def insert_track(track_data):
    """Inserta una pista con manejo robusto de datos"""
    if not isinstance(track_data, dict):
        logging.error(f"🚨 Datos no son diccionario: {type(track_data)}")
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
                    track_data.get('track_id'),
                    track_data.get('artists'),
                    track_data.get('title'),
                    track_data.get('track_genre', 'unknown'),
                    float(track_data.get('tempo', 0)),
                    int(track_data.get('duration_ms', 0))
                ))
        logging.info(f"🎵 Insertada: {track_data.get('title')}")
    except Exception as e:
        logging.error(f"💥 Error al insertar: {e}", exc_info=True)

def kafka_consumer_loop():
    """Loop principal del consumer"""
    create_table_if_not_exists()
    
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
                # Decodificar y parsear el mensaje
                message_str = msg.value().decode('utf-8')
                track_data = safe_json_parse(message_str)
                
                if track_data:
                    insert_track(track_data)
            except Exception as e:
                logging.error(f"⚠️ Error procesando mensaje: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("🛑 Consumer cerrado")

@app.route("/health")
def health_check():
    return jsonify({"status": "healthy", "service": "kafka-consumer"}), 200

def main():
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()
