import pika
import json
import os
import sys
import time
from pika.exceptions import AMQPConnectionError
from pika import DeliveryMode

def wait_for_rabbitmq(host, retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            connection.close()
            print(f"RabbitMQ disponível após {attempt} tentativa(s).")
            return
        except AMQPConnectionError:
            print(f"Tentativa {attempt}/{retries}: RabbitMQ ainda não disponível. Aguardando {delay}s...")
            time.sleep(delay)
    print("[X] ERRO: Não foi possível conectar ao RabbitMQ após várias tentativas.")
    sys.exit(1)

def main():
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    QUEUE_NAME = 'toy_reviews'
    FILE_PATH = os.path.join('/data', 'arquivo.json')

    print(f"Iniciando o Produtor RabbitMQ...")
    print(f"Alvo: Fila '{QUEUE_NAME}' no host '{RABBITMQ_HOST}'")
    print(f"Arquivo fonte: {FILE_PATH}")

    wait_for_rabbitmq(RABBITMQ_HOST)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        print("Conexão com RabbitMQ estabelecida com sucesso.\n")
    except AMQPConnectionError:
        print("ERRO: Não foi possível conectar ao RabbitMQ. O servidor está rodando?")
        sys.exit(1)

    if not os.path.exists(FILE_PATH):
        print(f"ERRO: Arquivo não encontrado em: {os.path.abspath(FILE_PATH)}")
        print("Monte o dataset com: -v ./dataset:/data")
        connection.close()
        sys.exit(1)

    mensagens_enviadas = 0
    linhas_com_erro = 0

    print("Lendo o arquivo e publicando mensagens. Aguarde...")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    review_raw = json.loads(line.strip())

                    document_data = {
                        "id_transferencia": line_num,
                        "id_produto": review_raw.get("asin", "Desconhecido"),
                        "usuario": review_raw.get("reviewerName", "Anônimo"),
                        "nota": review_raw.get("overall", 0.0),
                        "resumo": review_raw.get("summary", ""),
                        "texto": review_raw.get("reviewText", "")
                    }

                    message_body = json.dumps(document_data)

                    channel.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=message_body,
                        properties=pika.BasicProperties(
                            delivery_mode=DeliveryMode.Persistent
                        )
                    )

                    mensagens_enviadas += 1

                    if mensagens_enviadas % 5000 == 0:
                       print(f"{mensagens_enviadas} mensagens processadas e enviadas...")

                except json.JSONDecodeError:
                    linhas_com_erro += 1
                    continue

    except KeyboardInterrupt:
        print("\nTransferência interrompida pelo usuário (CTRL+C).")
    except Exception as e:
        print(f"\nOcorreu um erro inesperado durante o processamento do arquivo: {e}")

    finally:
        if connection and connection.is_open:
            connection.close()

        print("\n" + "="*50)
        print(" RELATÓRIO DE EXECUÇÃO DO PRODUTOR")
        print("="*50)
        print(f" Total enviado:   {mensagens_enviadas} mensagens")
        print(f" Linhas com erro: {linhas_com_erro} (ignoradas)")
        print(" Status:          Conexão encerrada com segurança.")
        print("="*50)

if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[union-attr]
    main()
