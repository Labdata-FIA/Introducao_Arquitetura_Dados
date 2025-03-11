from confluent_kafka import Consumer, KafkaError
from config import KAFKA_CONFIG, TOPIC
from processor import process_event

def consume_events():
    """
    Consome eventos do Kafka e processa.
    """
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    try:
        print(f"Consumindo eventos do tópico {TOPIC}...")
        while True:
            msg = consumer.poll(timeout=1.0)  # Aguarda por mensagens

            if msg is None:
                continue  # Sem mensagens no momento

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # Fim da partição
                else:
                    print(f"Erro no Kafka: {msg.error()}")
                    continue

            # Processar a mensagem recebida
            print(f"Mensagem recebida: {msg.value().decode('utf-8')}")
            process_event(msg.value().decode('utf-8'))

    except KeyboardInterrupt:
        print("Interrompido pelo usuário.")
    finally:
        consumer.close()
