# Guia de Ambientes Docker: Desenvolvimento vs Produção

Este documento detalha a separação de responsabilidades, arquitetura de containers e estratégias de execução entre os ambientes de **Desenvolvimento** e **Produção** na plataforma Event-Driven Kafka.

---

## 1. Visão Geral da Separação

| Aspecto | Ambiente de Desenvolvimento (`dev`) | Ambiente de Produção (`prod`) |
| :--- | :--- | :--- |
| **Arquivo Dockerfile** | `docker/Dockerfile` (target: `development`) ou `docker/Dockerfile.dev` | `docker/Dockerfile` (target: `production`) ou `docker/Dockerfile.prod` |
| **Composição Compose** | `docker-compose.yml` + `docker-compose.dev.yml` | `docker-compose.yml` + `docker-compose.prod.yml` |
| **Código Fonte** | Volume montado dinamicamente (`.:/app`) com hot reload (`ts-node`) | Compilado e embutido imutavelmente em `dist/` (`node dist/index.js`) |
| **Dependências** | Todas as dependências instaladas (`npm ci` com `devDependencies`) | Apenas dependências de runtime (`npm ci --omit=dev`), cache limpo |
| **Usuário do Container** | `root` / desenvolvedor local | Usuário não-privilegiado (`USER node`) |
| **Supervisor de Processo** | `dumb-init` / `ts-node` direto | `dumb-init` (PID 1) para propagação de sinais SIGTERM/SIGINT |
| **Exposição de Portas** | API (3000), PostgreSQL (5432), Kafka (9092, 29092), Zookeeper (2181) | Apenas API HTTP/WebSocket (3000). Banco e Broker são 100% internos |
| **Políticas de Restart** | Padrão (`no`) | `restart: unless-stopped` em todos os serviços |
| **Gestão de Logs** | Saída padrão direta (stdout) com `LOG_LEVEL=debug` | Rotação automática `json-file` (limite 20M–50M, 5 arquivos), `LOG_LEVEL=info` |
| **Banco de Dados** | Auto-inicialização via `./scripts/init-db.sql` montado | Volume persistente nomeado sem injeção acidental de scripts no entrypoint |

---

## 2. Arquitetura Multi-Stage do Dockerfile

O arquivo `docker/Dockerfile` foi arquitetado em 4 estágios modulares (*multi-stage build*):

```mermaid
flowchart TD
    Base["1. Base (node:20-alpine)\n- Instalação do dumb-init e curl\n- WORKDIR /app"]
    
    Dev["2. Development (Target: development)\n- Instala devDependencies\n- Suporte a ts-node\n- Montagem de volumes para Hot-Reload"]
    
    Builder["3. Builder (Target: builder)\n- Instala todas as dependências\n- Compila TypeScript para dist/"]
    
    Prod["4. Production (Target: production)\n- Instala apenas dependências de produção\n- Copia artefatos dist/ do Builder\n- Execução como USER node\n- Healthcheck configurado\n- ENTRYPOINT com dumb-init"]
    
    Base --> Dev
    Base --> Builder
    Builder --> Prod
```

### Comandos de Build Independentes

- **Build de Desenvolvimento:**
  ```bash
  npm run docker:build:dev
  # ou
  docker build --target development -t event-driven-api:dev -f docker/Dockerfile .
  ```

- **Build de Produção:**
  ```bash
  npm run docker:build:prod
  # ou
  docker build --target production -t event-driven-api:prod -f docker/Dockerfile .
  ```

---

## 3. Estrutura dos Arquivos Docker Compose

A configuração do Docker Compose utiliza o padrão de sobreposição (*Compose Overlay*):

```
├── docker-compose.yml              # Base: topologia, redes, volumes e limites de hardware
├── docker-compose.dev.yml          # Overrides de desenvolvimento: portas expostas, volumes, hot-reload
├── docker-compose.prod.yml         # Overrides de produção: segurança, isolamento de rede, logs e reinício
└── docker-compose.override.yml.example # Template para overrides locais do desenvolvedor
```

### 3.1. Execução em Desenvolvimento

Para iniciar o ecossistema com suporte a recarga a quente e portas de depuração expostas:

```bash
# Via NPM scripts
npm run docker:dev

# Via Docker Compose diretamente
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Para encerrar o ambiente
npm run docker:dev:down
```

### 3.2. Execução em Produção

Para iniciar o ambiente endurecido, com containers imutáveis e isolamento de rede:

```bash
# Via NPM scripts
npm run docker:prod

# Via Docker Compose diretamente
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Para encerrar o ambiente
npm run docker:prod:down
```

### 3.3. Inicialização com Auto-Calibração de Memória (`scripts/start.sh`)

O script de inicialização inteligente suporta a variável `ENV` para alternar entre os ambientes:

```bash
# Modo Desenvolvimento (padrão)
scripts/start.sh

# Modo Produção
ENV=prod scripts/start.sh
```

---

## 4. Matriz de Variáveis de Ambiente

Consulte os arquivos `.env.example` e `.env.production.example` para obter as configurações completas:

| Variável | Desenvolvimento | Produção | Finalidade |
| :--- | :--- | :--- | :--- |
| `NODE_ENV` | `development` | `production` | Modo de execução do runtime Node.js |
| `LOG_LEVEL` | `debug` | `info` | Nível de detalhamento do logger Winston/Pino |
| `PORT` | `3000` | `3000` | Porta do servidor Express e WebSocket |
| `KAFKA_BROKER` | `localhost:9092` (host) ou `kafka:29092` (docker) | `kafka:29092` | Endereço do cluster Kafka |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/event_driven_test` | `postgresql://user:pass@postgres:5432/db` | String de conexão com PostgreSQL |
| `POSTGRES_DB` | `event_driven_test` | Configurado conforme ambiente | Nome da base de dados |
| `POSTGRES_USER` | `postgres` | Configurado com credenciais seguras | Usuário do banco |
| `POSTGRES_PASSWORD`| `postgres` | Senha forte | Senha de acesso |
