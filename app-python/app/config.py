import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker:29092'),
    'group.id': os.getenv('KAFKA_GROUP_ID', 'default-group'),
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
}

# Nome do tópico
TOPIC = os.getenv('KAFKA_TOPIC', 'default-topic')

# Configurações do MinIO
MINIO_CONFIG = {
    'endpoint_url': os.getenv('MINIO_ENDPOINT_URL', 'http://localhost:9000'),
    'aws_access_key_id': os.getenv('MINIO_ACCESS_KEY', 'minio'),
    'aws_secret_access_key': os.getenv('MINIO_SECRET_KEY', 'minio'),
}

# Nome do bucket
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'default-bucket')
