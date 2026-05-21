import pika
import json
import os
import sys
import time
from datetime import datetime, timezone
from pika.exceptions import AMQPConnectionError


RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME    = os.environ.get("QUEUE_NAME", "toy_reviews")
FILE_PATH     = os.environ.get("FILE_PATH", "/data/arquivo.json")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", 0))


#Parsing
def parse_review(raw: dict) -> dict | None:
    """
    Extrai e valida os campos relevantes de uma review bruta do dataset
    Amazon Toys (UCSD Amazon Review Data).

    Campos esperados no JSON de entrada:
        reviewerID      - ID único do avaliador
        asin            - ID único do produto
        reviewerName    - Nome do avaliador (opcional)
        summary         - Título / resumo da review
        reviewText      - Corpo completo da review
        overall         - Nota (1.0 - 5.0)
        verified        - Compra verificada (bool, opcional)
        helpful         - [votos_úteis, total_votos] (opcional)
        vote            - votos úteis em formato string (dataset v2, opcional)
        reviewTime      - Data legível "MM DD, YYYY" (opcional)

    Retorna um dicionário estruturado ou None se faltar campo obrigatório.
    """
    required = ("reviewerID", "asin", "summary", "reviewText", "overall")
    for field in required:
        if field not in raw or raw[field] is None:
            print(f"Review ignorada - campo ausente: '{field}'")
            return None

    try:
        rating = float(raw["overall"])
        if not (1.0 <= rating <= 5.0):
            raise ValueError
    except (ValueError, TypeError):
        print(f"Review ignorada – 'overall' inválido: {raw.get('overall')}")
        return None

    summary     = str(raw["summary"]).strip()
    review_text = str(raw["reviewText"]).strip()
    if not summary or not review_text:
        print("Review ignorada – 'summary' ou 'reviewText' vazio.")
        return None

    reviewer_name = str(raw.get("reviewerName", "Anônimo")).strip() or "Anônimo"
    verified      = bool(raw.get("verified", False))
    review_time   = raw.get("reviewTime", "")

    helpful_votes = 0
    total_votes   = 0
    if "helpful" in raw and isinstance(raw["helpful"], list) and len(raw["helpful"]) == 2:
        try:
            helpful_votes = int(raw["helpful"][0])
            total_votes   = int(raw["helpful"][1])
        except (ValueError, TypeError):
            pass
    elif "vote" in raw:
        try:
            helpful_votes = int(str(raw["vote"]).replace(",", ""))
        except (ValueError, TypeError):
            pass

    if rating >= 4.0:
        sentiment = "positivo"
    elif rating == 3.0:
        sentiment = "neutro"
    else:
        sentiment = "negativo"

    return {
        "product_id":    raw["asin"],
        "reviewer_id":   raw["reviewerID"],
        "reviewer_name": reviewer_name,
        "summary":       summary,
        "review_text":   review_text,
        "text_length":   len(review_text),
        "rating":        rating,
        "sentiment":     sentiment,
        "verified":      verified,
        "helpful_votes": helpful_votes,
        "total_votes":   total_votes,
        "review_date":   review_time,
        "parsed_at":     datetime.now(timezone.utc).isoformat(),
    }

def load_dataset(path: str) -> list[dict]:
    if not os.path.exists(path):
        print(f"ERRO: Arquivo não encontrado: {os.path.abspath(path)}")
        print("Verifique se o volume está montado corretamente no docker-compose.yml")
        sys.exit(1)

    raw_reviews = []
    errors = 0

    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()

    if content.startswith("["):
        raw_reviews = json.loads(content)
    else:
        for line_num, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                raw_reviews.append(json.loads(line))
            except json.JSONDecodeError as e:
                errors += 1
                print(f"Linha {line_num} ignorada - JSON inválido: {e}")

    print(f"Dataset carregado: {len(raw_reviews)} registros ({errors} linhas com erro)")
    return raw_reviews


def wait_for_rabbitmq(host: str, tentativa: int = 10, delay: int = 5):
    for attempt in range(1, tentativa + 1):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            conn.close()
            print(f"RabbitMQ disponível após {attempt} tentativa(s).")
            return
        except AMQPConnectionError:
            print(f"Tentativa {attempt}/{tentativa}: RabbitMQ não disponível. Aguardando {delay}s...")
            time.sleep(delay)
    print("ERRO: Não foi possível conectar ao RabbitMQ.")
    sys.exit(1)


def main():
    import io
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8")

    print("Iniciando o Produtor RabbitMQ...")
    print(f"Alvo: Fila '{QUEUE_NAME}' no host '{RABBITMQ_HOST}'")
    print(f"Arquivo fonte: {FILE_PATH}")

    raw_reviews = load_dataset(FILE_PATH)

    wait_for_rabbitmq(RABBITMQ_HOST)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        print("Conexão com RabbitMQ estabelecida com sucesso.\n")
    except AMQPConnectionError:
        print("ERRO: Não foi possível conectar ao RabbitMQ.")
        sys.exit(1)

    published: int = 0
    skipped:   int = 0
    parsed:    dict | None = None

    print("Publicando mensagens. Aguarde...")
    try:
        for raw in raw_reviews:
            parsed = parse_review(raw)

            if parsed is None:
                skipped += 1
                continue

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(parsed, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )
            published += 1

            if published % 1000 == 0:
                print(f"{published} mensagens publicadas...")
            
            # Limitador das mensagens publicadas
            if MAX_MESSAGES > 0 and published >= MAX_MESSAGES:
                print(f"\nLimite de {MAX_MESSAGES} mensagens atingido")
                break

        if published > 0 and parsed is not None:
            print(f"\nÚltima review publicada: '{parsed['summary'][:60]}'")

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário (CTRL+C).")
    except Exception as e:
        print(f"\nErro inesperado durante a publicação: {e}")
    finally:
        if connection.is_open:
            connection.close()

    print("\n" + "=" * 50)
    print(" RELATÓRIO DE EXECUÇÃO DO PRODUTOR")
    print("=" * 50)
    print(f" Total publicado: {published} mensagens")
    print(f" Ignoradas:       {skipped} reviews inválidas")
    print(f" Status:          Conexão encerrada com segurança.")
    print("=" * 50)


if __name__ == "__main__":
    main()
