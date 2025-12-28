import json
import time
import boto3
from loguru import logger

# CONFIGURACIÓN
STREAM_NAME = "spotify-stream"
REGION = "us-east-1"
INPUT_FILE = "./data/spotify-global-music-dataset-10000.json"

kinesis = boto3.client("kinesis", region_name=REGION)

def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_producer():
    data = load_data(INPUT_FILE) # el json  es un array de objetos asi que no hace falta mas procesamiento
    records_sent = 0

    logger.info(f"Iniciando envío al stream: {STREAM_NAME}")

    for track in data:
        payload = {
            "track_id": track["track_id"],
            "track_name": track["track_name"],
            "artist": track["artist_name"],
            "album": track["album_name"],
            "album_id": track["album_id"],
            "duration_min": float(track["track_duration_min"]),
            "genres": track["artist_genres"].split(",") if track["artist_genres"] else [],
            "album_release_date": track["album_release_date"]
        }

        partition_key = track["album_release_date"][:7]

        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey=partition_key
        )

        records_sent += 1
        logger.info(
            f"Enviado track='{track['track_name']}' "
            f"partition_key='{partition_key}' "
            f"shard={response['ShardId']}"
        )

        time.sleep(0.05)  

    logger.info(f"Transmisión finalizada. Total enviados: {records_sent}")

if __name__ == "__main__":
    run_producer()
