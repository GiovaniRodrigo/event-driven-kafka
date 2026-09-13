# GUIA PASSO-A-PASSO: GITHUB + GIT

## PASSO 1: Criar Repo no GitHub (5 minutos) - já feito!

1. Acesse: **https://github.com/new**

2. Preencha:
   - **Repository name:** `event-driven-kafka`
   - **Description:** `Production-grade event-driven microservices with Kafka. 50% latency optimization.`
   - **Public:** ✅ Sim
   - **README:** ❌ Não marcar
   - **.gitignore:** ❌ Não marcar
   - **License:** ✅ MIT License

3. Clique **"Create repository"**

4. GitHub vai mostrar commands. **COPIE a URL:**
   ```
   https://github.com/GiovaniRodrigo/event-driven-kafka.git
   ```

---

## PASSO 2: Setup Local (15 minutos)

Abra terminal/PowerShell e execute:

```bash
# Criar pasta
mkdir event-driven-kafka-architecture
cd event-driven-kafka-architecture

# Inicializar Git
git init
git config user.name "Giovani Rodrigues"
git config user.email "seu.email@gmail.com"

# Criar pastas
mkdir -p src/{producers,consumers,services,utils}
mkdir -p docker docs tests benchmarks scripts
```

**✅ Você tem 9 pastas criadas**

---

## PASSO 3: Copiar Arquivos (45 minutos)

### OPÇÃO A: VS Code (MAIS FÁCIL)

```bash
# Abrir em VS Code
code .
```

Depois:
1. Ctrl+N (novo arquivo)
2. Digite nome: `.gitignore`
3. Paste conteúdo de `03_PACKAGE_JSON.md`
4. Ctrl+S (salvar)
5. **Repita pra cada arquivo**

### OPÇÃO B: Terminal (Linux/Mac)

```bash
# Copie os conteúdos de cada arquivo .md
# E rode algo como:

cat > .gitignore << 'EOF'
[COPIE CONTEÚDO DO .gitignore AQUI]
EOF

cat > package.json << 'EOF'
[COPIE CONTEÚDO DO package.json AQUI]
EOF

# Etc...
```

### Arquivos a Copiar (ordem importante):

**ROOT (7 arquivos):**
- [ ] `.gitignore` (de 03_PACKAGE_JSON.md)
- [ ] `package.json` (de 03_PACKAGE_JSON.md)
- [ ] `tsconfig.json` (de 03_PACKAGE_JSON.md)
- [ ] `jest.config.js` (de 03_PACKAGE_JSON.md)
- [ ] `.dockerignore` (de 03_PACKAGE_JSON.md)
- [ ] `LICENSE` (de 03_PACKAGE_JSON.md)
- [ ] `.env.example` (de 03_PACKAGE_JSON.md)

**SRC (12 arquivos):**
- [ ] `src/index.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/config.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/types.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/utils/logger.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/producers/base-producer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/producers/order-producer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/consumers/base-consumer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/consumers/payment-consumer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/consumers/inventory-consumer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/consumers/notification-consumer.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/services/order-service.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/services/database.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/services/payment-service.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/services/inventory-service.ts` (de 04_ARQUIVOS_SRC.md)
- [ ] `src/services/notification-service.ts` (de 04_ARQUIVOS_SRC.md)

**DOCKER (3 arquivos):**
- [ ] `docker/Dockerfile` (de 05_DOCKER_E_DOCS.md)
- [ ] `docker-compose.yml` (de 05_DOCKER_E_DOCS.md)
- [ ] `scripts/init-db.sql` (de 05_DOCKER_E_DOCS.md)

**DOCS (3 arquivos):**
- [ ] `docs/SETUP.md` (de 05_DOCKER_E_DOCS.md)
- [ ] `docs/ARCHITECTURE.md` (de 05_DOCKER_E_DOCS.md)
- [ ] `README.md` (GRANDE, com 1500+ words - vem separado)

---

## PASSO 4: README.md (O MAIS IMPORTANTE!)

Você recebeu um arquivo separado com README completo (1500+ palavras).

**Copie e salve como:** `README.md` (na raiz)

Ele começa com:
```markdown
# Event-Driven Kafka Architecture: Production Patterns & Performance Optimization
```

---

## PASSO 5: Verificar o que você tem

```bash
git status
```

Deve mostrar (em vermelho):
```
Untracked files:
  .gitignore
  .env.example
  .dockerignore
  LICENSE
  README.md
  docker-compose.yml
  jest.config.js
  package.json
  tsconfig.json
  docker/
    Dockerfile
  docs/
    ARCHITECTURE.md
    SETUP.md
  scripts/
    init-db.sql
  src/
    index.ts
    config.ts
    types.ts
    ...
```

✅ Se aparecer mais ou menos, é porque você não copiou algum arquivo.

---

## PASSO 6: Primeiro Commit

```bash
# Adicionar tudo
git add .

# Verificar (deve estar tudo verde agora)
git status

# Fazer commit
git commit -m "Initial: Event-driven Kafka architecture

- Production-grade microservices with Kafka + Node.js + PostgreSQL
- 50% latency optimization patterns
- Producers, consumers, and services
- Docker Compose for local development
- TypeScript strict mode"
```

---

## PASSO 7: Push para GitHub

```bash
# Renomear branch para main
git branch -M main

# Adicionar remote (COPIE SUA URL DO PASSO 1)
git remote add origin https://github.com/GiovaniRodrigo/event-driven-kafka.git

# Push
git push -u origin main
```

Se pedir autenticação:
- GitHub CLI: `gh auth login` (depois `Y` e segue)
- Ou cria SSH key (mais avançado)

---

## PASSO 8: Verificar no GitHub

1. Acesse: https://github.com/GiovaniRodrigo/event-driven-kafka

2. Verifique:
   - ✅ README.md renderizado (com imagens de ASCII art)
   - ✅ Todos os arquivos na árvore de pastas
   - ✅ LICENSE MIT mostrando
   - ✅ Pasta verde (significa repo público)

3. **Edite Settings:**
   - Settings → About → Add short description
   - Add topics: kafka, event-driven, microservices, nodejs, typescript, postgresql

---

## PASSO 9: Testar Localmente (OPCIONAL)

```bash
# Instalar dependências
npm install

# Iniciar Docker
docker-compose up -d

# Esperar 30 segundos

# Rodar aplicação
npm run dev

# Em outro terminal:
curl http://localhost:3000/health

# Parar
docker-compose down
```

---

## ✅ PRONTO!

Seu repo está LIVE no GitHub! 🎉

**Próximas ações em sequência:**
1. Etapa 2: Polish README + Article
2. Etapa 3: LinkedIn post + 15 recruiter messages
3. Etapa 4: Final review e preparação

---

## 🆘 ERROS COMUNS

**Erro: "fatal: not a git repository"**
- Solução: Você não fez `git init`. Execute novamente.

**Erro: "permission denied" ao fazer push**
- Solução: Configure SSH ou GitHub CLI. Digite `gh auth login`

**Erro: Arquivo não aparece no GitHub**
- Solução: Verifique `.gitignore`. Talvez o arquivo está ignorado.

**Erro: README não renderiza**
- Solução: Verifique se é `README.md` (maiúscula M).

---

## 💡 DICAS

1. **Faça commits frequentes** - não espere pra copiar tudo de uma vez
2. **Teste localmente** - `npm install` vai avisar se algo tá errado
3. **Use VS Code** - mais fácil que terminal pra copiar-colar
4. **GitHub oferece edição online** - se errar, você pode editar direto lá

---

**Tempo total: ~1 hora**

**Próximo arquivo: 03_PACKAGE_JSON.md (para copiar arquivos root)**
