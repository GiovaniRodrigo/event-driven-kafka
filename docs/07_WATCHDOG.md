# 🐕 Watchdog & Resource Guard — Não Trave a Máquina

Subir a stack local (Zookeeper + Kafka + Postgres + app) num notebook comum
consome muita RAM. Sem teto, o `docker compose up` pode empurrar a máquina pra
swap e **travar tudo**. Este projeto resolve isso em duas camadas que se
auto-calibram ao hardware onde rodam — nenhum número é fixado à mão.

## As duas camadas

### 1. Tetos passivos (a trava primária)

Cada serviço no [`docker-compose.yml`](../docker-compose.yml) tem `mem_limit`,
`cpus` e (nos serviços JVM) teto de heap. Os valores **não** estão cravados no
arquivo: são interpolados de um `.env` que o `scripts/start.sh` gera medindo o
host. O Compose garante que a stack nunca ultrapasse esses limites.

### 2. Watchdog ativo (a rede de segurança)

O [`scripts/watchdog.sh`](../scripts/watchdog.sh) observa a memória disponível
do host e, se ela cair abaixo de um limiar por várias checagens seguidas,
**para a stack graciosamente** (`docker compose stop`) antes do congelamento.
É a proteção pra quando algo *fora* da stack (IDE, navegador) também pesa.

## Como rodar

```bash
# Calibra ao hardware, escreve .env, liga o watchdog e sobe a stack:
scripts/start.sh
```

> Quando o `package.json` da raiz for materializado (ver a nota sobre o `app`
> abaixo), vale apontar um script `docker:up` pra `scripts/start.sh`. Hoje esse
> `package.json` não existe, então rode o script direto.

Variações:

```bash
scripts/start.sh              # calibra + watchdog + up -d (padrão)
NO_WATCHDOG=1 scripts/start.sh   # calibra + up -d, e encerra qualquer watchdog anterior
DRY_RUN=1 scripts/start.sh    # só imprime o .env calibrado e sai (não precisa de Docker)
```

Parar o watchdog (o `start.sh` salva o PID):

```bash
kill $(cat scripts/.watchdog.pid)
```

Rodar o watchdog sozinho (ex.: numa stack que já está de pé):

```bash
scripts/watchdog.sh   # Ctrl-C pra parar
```

## Como o sistema se mede e calibra

O `start.sh` lê `MemTotal` (`/proc/meminfo`) e `nproc`, então:

1. **Headroom pro SO** = `max(2GB, 40% do total)` — fica reservado pro sistema.
2. **Orçamento da stack** = total − headroom.
3. **Repartição por peso** (memória **e** CPU, somam 100%): Kafka 45%, Postgres
   20%, App 20%, Zookeeper 15%.
4. **Heap JVM** (Kafka/ZK) ≈ 40% do teto do container; **Node old-space** ≈ 75%
   do teto do app.
5. **`shared_buffers` do Postgres** ≈ 25% do teto do Postgres (piso 64MB), pra
   nunca exceder a memória permitida ao container (senão ele é OOM-killed em
   máquinas pequenas).
6. **`cpus`** proporcional ao `nproc` com os mesmos pesos (piso de 0.5).

Exemplo numa máquina de ~8 GB / 8 CPUs:

| serviço    | mem_limit | cpus | heap / extra          |
|------------|-----------|------|-----------------------|
| kafka      | ~2094m    | 3.6  | -Xmx837m              |
| postgres   | ~931m     | 1.6  | shared_buffers ~232MB |
| app        | ~931m     | 1.6  | old-space 698         |
| zookeeper  | ~698m     | 1.2  | -Xmx279m              |

Numa máquina maior os valores sobem automaticamente; numa menor, descem (e o
script avisa se sobrar pouco).

### Limiar do watchdog

Derivado do host: `max(512MB, 9% do MemTotal)`. Dispara depois de **3**
checagens seguidas abaixo do limiar (intervalo de **5s**), evitando falso
positivo por picos momentâneos. Ajustável por env:

```bash
WATCHDOG_INTERVAL=10 WATCHDOG_BREACHES=5 WATCHDOG_THRESHOLD_MB=800 scripts/watchdog.sh
```

## Rodar sem o `start.sh`

O `docker-compose.yml` tem fallbacks conservadores (`${VAR:-default}`), então
`docker compose up` direto ainda funciona — só sem a calibração automática, com
tetos fixos pensados pra ~8 GB.

## Nota sobre o serviço `app`

O serviço `app` builda a partir de `docker/Dockerfile`, que hoje depende da
*materialization track* (o código ainda vive como snippet em `docs/` — ver os
defeitos conhecidos em [`specs/dashboard-design-system.md`](specs/dashboard-design-system.md)).
Até lá, `app` não builda. Os tetos e o watchdog **já estão corretos** e já
protegem a infra (Kafka/ZK/Postgres), que é o que come RAM; passam a cobrir o
`app` automaticamente assim que ele for materializado. Pra subir só a infra
enquanto isso:

```bash
docker compose up -d zookeeper kafka postgres   # após scripts/start.sh calibrar, ou direto com os fallbacks
```
