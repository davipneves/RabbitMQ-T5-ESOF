# T5 ESOF — RabbitMQ com Docker

Sistema produtor/consumidor de reviews de brinquedos (Dump escolhido pelo professor Allyson Costa e Silva) usando RabbitMQ, containerizado com Docker.

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (já incluído no Docker Desktop)

## Como usar

### 1. Adicionar o dataset

Crie a pasta `dataset/` e coloque o arquivo `arquivo.json` dentro:

```bash
mkdir dataset
cp /caminho/para/arquivo.json data/
```

### 2. Subir os serviços

```bash
docker compose up --build
```

Isso inicia **três containers** em ordem:
1. **rabbitmq** — broker de mensagens (aguarda ficar saudável)
2. **consumer** — começa a escutar a fila
3. **producer** — lê o JSON e publica as mensagens

### 3. Acompanhar os logs

```bash
# Todos os serviços
docker compose logs -f

# Só o producer
docker compose logs -f producer

# Só o consumer
docker compose logs -f consumer
```

### 4. Painel de administração do RabbitMQ

Acesse **http://localhost:15672** com:
- Usuário: `guest`
- Senha: `guest`

### 5. Encerrar

```bash
docker compose down
```

## Variáveis de ambiente

| Variável         | Padrão     | Descrição                        |
|------------------|------------|----------------------------------|
| `RABBITMQ_HOST`  | `rabbitmq` | Hostname do broker RabbitMQ      |

## Executar serviços separadamente

```bash
# Só o RabbitMQ e o Consumer (sem producer)
docker compose up rabbitmq consumer

# Rodar o producer depois
docker compose run --rm producer
```
