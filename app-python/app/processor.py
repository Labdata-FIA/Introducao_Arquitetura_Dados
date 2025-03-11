import json
from urllib.parse import unquote
import boto3
from config import MINIO_CONFIG

def process_event(event):
    """
    Processa um evento Kafka e acessa o objeto correspondente no MinIO.
    """
    try:
        # Parse do evento recebido
        event_data = json.loads(event)
        bucket_name = event_data.get('Records')[0]['s3']['bucket']['name']
        object_key = unquote(event_data.get('Records')[0]['s3']['object']['key'])

        print(f"Bucket: {bucket_name}, Key: {object_key}")

        # Conexão ao MinIO usando boto3
        s3_client = boto3.client('s3', **MINIO_CONFIG)

        # Baixar o conteúdo do objeto
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')

        print(f"Conteúdo do objeto: {content}")

    except Exception as e:
        print(f"Erro ao processar evento: {e}")
