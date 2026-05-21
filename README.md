# Amazon Toy Reviews — Pipeline de Mensageria com RabbitMQ & Docker

Este projeto foi desenvolvido como atividade avaliativa (T5) na disciplina de Engenharia de Software II (ESOF II) sob a orientação do Professor Allyson Costa e Silva. A aplicação implementa uma arquitetura distribuída de **Produtor/Consumidor** para processamento, validação e análise de sentimento de avaliações de brinquedos e jogos (dataset *Amazon Toys_and_Games*). A infraestrutura é totalmente containerizada e utiliza o **RabbitMQ** como broker de mensagens persistentes.

---

## Arquitetura do Sistema

O ecossistema é composto por três serviços principais coordenados via Docker Compose:

1. **`rabbitmq`**: O broker de mensageria responsável por gerenciar a fila `toy_reviews` de forma persistente (`durable=True`).
2. **`producer`**: Componente que carrega o dataset local (`.json`), valida a estrutura de cada registro, realiza uma análise inicial de sentimento e publica os dados na fila.
3. **`consumer`**: Componente assíncrono que consome as reviews da fila usando controle de fluxo (`basic_qos` com prefetch de 10) e exibe os relatórios formatados no console.

---

## Regras de Negócio e Transformações

Ao passar pelo **Producer**, cada review bruta sofre validações e enriquecimentos:
* **Validação Estrita:** São descartadas reviews com campos obrigatórios ausentes (`reviewerID`, `asin`, `summary`, `reviewText`, `overall`) ou com notas (`overall`) fora do intervalo de 1.0 a 5.0.
* **Cálculo de Sentimento:**
  * **Positivo:** Notas maiores ou iguais a `4.0`
  * **Neutro:** Notas iguais a `3.0`
  * **Negativo:** Notas menores que `3.0`
* **Métricas de Utilidade:** Converte strings de votação dinâmicas e arrays (`helpful`/`vote`) em contadores inteiros padronizados.

No **Consumer**, o processamento é controlado:
* O consumidor processa lotes de mensagens e realiza um graceful shutdown automático acompanhado de um relatório consolidado assim que atingir a marca de **100 mensagens** recebidas.

---

## Variáveis de Ambiente

As configurações do sistema podem ser customizadas diretamente no arquivo `docker-compose.yml`:

| Variável | Padrão | Escopo | Descrição |
| :--- | :--- | :--- | :--- |
| `RABBITMQ_HOST` | `rabbitmq` | Ambos | Hostname de conexão com o broker. |
| `QUEUE_NAME` | `toy_reviews` | Ambos | Nome da fila de mensageria. |
| `FILE_PATH` | `/data/arquivo.json` | Producer | Caminho interno do dataset dentro do container. |
| `MAX_MESSAGES` | `500` | Producer | Limite máximo de mensagens que o produtor irá publicar (0 para ilimitado). |

---

## Como Executar

### 1. Pré-requisitos
* [Docker](https://docs.docker.com/get-docker/) instalado.
* [Docker Compose](https://docs.docker.com/compose/install/) configurado.

### 2. Preparação do Dataset
O container do produtor espera encontrar o dataset mapeado em um volume local. Crie o diretório `dataset` na raiz do projeto e mova o seu arquivo JSON para lá com o nome exato de `arquivo.json`:

```bash
mkdir -p dataset
cp /caminho/do/seu/dataset.json dataset/arquivo.json

> **Nota:** O script aceita nativamente tanto arquivos JSON formatados como um array padrão `[...]` quanto arquivos no formato JSON Lines (um JSON por linha).

### 3. Inicialização Completa
Para construir as imagens e subir todo o ecossistema (o Docker Compose gerencia a ordem de inicialização esperando o RabbitMQ passar nos testes de *healthcheck* primeiro):

```bash
docker compose up --build
4. Gerenciamento e Logs
Para acompanhar o fluxo de dados em tempo real na tela de forma isolada, você pode filtrar os logs por serviço:

Bash
# Monitorar apenas o fluxo de processamento do Consumidor
docker compose logs -f consumer

# Monitorar apenas o envio de dados do Produtor
docker compose logs -f producer

---

Monitoramento Web (Interface do RabbitMQ)
O painel de gerenciamento oficial do RabbitMQ fica disponível durante a execução do projeto. Através dele, você pode acompanhar graficamente a taxa de publicação, mensagens prontas na fila, consumidores ativos e conexões.

URL: http://localhost:15672

Usuário: guest
Senha: guest

Finalizando a Execução
Para parar os containers e remover de maneira limpa as redes virtuais e dependências criadas pelo ecossistema, execute:

```bash
docker compose down