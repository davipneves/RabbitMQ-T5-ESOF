import pika
import json
import os
import sys
import time
from pika.exceptions import AMQPConnectionError

# RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
# QUEUE_NAME    = "toy_reviews"


def format_review(parsed: dict) -> str:
    """
    Formata os campos extraídos pelo producer.py (parse_review)
    para exibição legível no console do consumer.
    """
    stars    = "★" * int(parsed.get("rating", 0)) + "☆" * (5 - int(parsed.get("rating", 0)))
    verified = "Compra verificada" if parsed.get("verified") else "Não verificada"

    helpful_votes = parsed.get("helpful_votes", 0)
    total_votes   = parsed.get("total_votes", 0)
    helpfulness   = (
        f"{helpful_votes}/{total_votes} consideraram útil"
        if total_votes > 0
        else "sem votos de utilidade"
    )

    # trunca o texto da review para o log (máx. 200 chars)
    text_preview = parsed.get("review_text", "")
    if len(text_preview) > 200:
        text_preview = text_preview[:197] + "..."

    return (
        f"\n{'='*60}\n"
        f"  Produto  : {parsed.get('product_id', 'N/A')}\n"
        f"  Avaliador: {parsed.get('reviewer_name', 'N/A')} "
        f"({parsed.get('reviewer_id', 'N/A')})\n"
        f"  Resumo   : {parsed.get('summary', 'N/A')}\n"
        f"  Nota     : {stars} {parsed.get('rating', 'N/A')}/5.0 "
        f"- {parsed.get('sentiment', 'N/A').upper()} | {verified}\n"
        f"  Utilidade: {helpfulness}\n"
        f"  Tamanho  : {parsed.get('text_length', 0)} caracteres\n"
        f"  Data     : {parsed.get('review_date', 'N/A')}\n"
        f"  Texto    : {text_preview}\n"
        f"  Parseado : {parsed.get('parsed_at', 'N/A')}\n"
        f"{'='*60}"
    )

def callback(ch, method, properties, body):
    """
    Chamado pelo RabbitMQ para cada mensagem recebida da fila.
    Desserializa o JSON, exibe os campos e confirma o recebimento (ACK).
    """
    try:
        parsed = json.loads(body)
        print(format_review(parsed))
    except json.JSONDecodeError as exc:
        print(f"Erro ao desserializar mensagem: {exc}")
        print(f"    Corpo bruto: {body[:200]}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)



def wait_for_rabbitmq(host, tentativa=10, delay=5):
    for attempt in range(1, tentativa + 1):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            connection.close()
            print(f"RabbitMQ disponível após {attempt} tentativa(s).")
            return
        except AMQPConnectionError:
            print(f"Tentativa {attempt}/{tentativa}: RabbitMQ ainda não disponível. Aguardando {delay}s...")
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

            print(f"Produto: {dado_recebido.get('product_id')} | Avaliador: {dado_recebido.get('reviewer_name')}")
            print(f"Nota: {dado_recebido.get('rating')} | Sentimento: {dado_recebido.get('sentiment')}")

            texto = dado_recebido.get('review_text', '')
            texto_curto = texto[:80] + "..." if len(texto) > 80 else texto
            print(f"Review: {texto_curto}")
            print("-" * 50)

            ch.basic_ack(delivery_tag=method.delivery_tag)

            if mensagens_recebidas == 100:
                print(f"\nForam recebidas {mensagens_recebidas}. Recebimento finalizado")
                ch.stop_consuming()

        except json.JSONDecodeError:
            print(f"Erro ao decodificar a mensagem: {body}")
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

        print("\n")
        print("\nRELATÓRIO DE EXECUÇÃO DO CONSUMIDOR")
        print(f"\nTotal recebido e processado: {mensagens_recebidas} mensagens")
        print("\nStatus: Conexão encerrada com segurança.")

if __name__ == '__main__':

    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[union-attr]
    main()
