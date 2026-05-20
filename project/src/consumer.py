import pika
import json
import os
import sys
import time
from pika.exceptions import AMQPConnectionError

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
    print("ERRO: Não foi possível conectar ao RabbitMQ após várias tentativas.")
    sys.exit(1)

def main():
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    QUEUE_NAME = 'toy_reviews'

    print(f"Iniciando o Consumidor...")
    print(f"Escutando a fila '{QUEUE_NAME}' no host '{RABBITMQ_HOST}'")

    wait_for_rabbitmq(RABBITMQ_HOST)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

    except AMQPConnectionError:
        print("ERRO: Não foi possível conectar ao RabbitMQ. O servidor está rodando?")
        sys.exit(1)

    mensagens_recebidas = 0

    def callback(ch, method, properties, body):
        nonlocal mensagens_recebidas
        mensagens_recebidas += 1

        try:
            dado_recebido = json.loads(body.decode('utf-8'))

            print(f"Recebido ID: {dado_recebido.get('id_transferencia')} | Produto: {dado_recebido.get('id_produto')}")
            print(f"Usuário: {dado_recebido.get('usuario')} (Nota: {dado_recebido.get('nota')})")

            texto = dado_recebido.get('texto', '')
            texto_curto = texto[:80] + "..." if len(texto) > 80 else texto
            print(f"     Review:  {texto_curto}")
            print("-" * 50)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            print(f" Erro ao decodificar a mensagem: {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=10)

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback
    )

    print('Aguardando mensagens\n')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nConsumidor interrompido pelo usuário.")
    finally:
        if connection and connection.is_open:
            connection.close()

        print("\n" + "="*50)
        print(" RELATÓRIO DE EXECUÇÃO DO CONSUMIDOR")
        print("="*50)
        print(f" Total recebido e processado: {mensagens_recebidas} mensagens")
        print(" Status: Conexão encerrada com segurança.")
        print("="*50)

if __name__ == '__main__':

    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[union-attr]
    main()
