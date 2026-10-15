# Open Data Platform --- Contexto do Projeto para Cursor Agent

> Documento-base consolidado a partir das decisões e orientações
> definidas na conversa. Use-o como contexto de continuidade do
> projeto.\
> **Importante:** este arquivo registra decisões e planos; não significa
> que os componentes já foram implementados ou validados. Atualize o
> status conforme o trabalho real no repositório.

## 1. Objetivo

Construir um portfólio de Engenharia de Dados robusto, demonstrável e
reproduzível, com uma plataforma de dados que mostre competências de
ponta a ponta:

-   Ingestão de múltiplas fontes.
-   Processamento batch e streaming.
-   Data lake / lakehouse com camadas Bronze, Silver e Gold.
-   Modelagem analítica e transformação com dbt.
-   Orquestração com Airflow.
-   CDC com PostgreSQL, Debezium e Kafka.
-   Qualidade de dados, contratos, observabilidade e linhagem.
-   CI/CD, testes automatizados, infraestrutura como código e
    implantação AWS.
-   Documentação técnica com decisões, limitações, resultados medidos e
    instruções de reprodução.

O projeto deve ser útil como estudo de caso de Data Engineer em
processos seletivos, não apenas como uma coleção de tecnologias.

## 2. Perfil de trabalho e restrições

-   Disponibilidade prevista: **15+ horas por semana**.
-   Ambiente desejado: **Windows 11 como host, WSL2/Ubuntu para
    desenvolvimento Linux e Docker Desktop com integração WSL**.
-   Execução híbrida: stack local para desenvolvimento e testes; AWS
    para uma implantação controlada e demonstração.
-   Repositório: **GitHub monorepo**.
-   Experiência já existente: Airflow, dbt, Spark, AWS e Terraform.
-   Prioridades: arquitetura sólida, reprodutibilidade, clareza técnica,
    evidências de testes e custo controlado.
-   Cursor Agent: modelo **Auto**, com tarefas pequenas, contexto
    explícito e revisão humana dos diffs.

### Princípios do ambiente Windows

1.  Desenvolver dentro do filesystem Linux do WSL, por exemplo
    `~/projects/open-data-platform`, em vez de `/mnt/c/...`, para evitar
    problemas de desempenho e permissões.

2.  Usar Docker Desktop com backend WSL2 e integração habilitada para a
    distribuição Ubuntu.

3.  Verificar o ambiente antes de começar:

    ``` bash
    pwd
    uname -a
    git status
    docker --version
    docker compose version
    docker info
    ```

4.  Criar o projeto em `~/projects/open-data-platform` e abri-lo no
    Cursor pelo contexto WSL. Se o comando `cursor .` não estiver
    disponível, abrir o caminho WSL correspondente no Cursor.

5.  Começar com poucos serviços e adicionar tecnologias apenas quando
    forem necessárias para a fase atual.

6.  Diretriz prática de hardware: 16 GB de RAM podem funcionar com
    serviços limitados; 32 GB são recomendados para uma stack mais
    completa. São referências de planejamento, não requisitos oficiais.

Referências oficiais: - WSL:
https://learn.microsoft.com/pt-br/windows/wsl/install/ - Docker Desktop
com WSL: https://docs.docker.com/desktop/features/wsl/best-practices/ -
Instalação Docker Desktop no Windows:
https://docs.docker.com/desktop/setup/install/windows-install/ - Cursor
Rules: https://docs.cursor.com/context/rules - Cursor --- contexto de
trabalho: https://docs.cursor.com/en/guides/working-with-context -
Cursor Chat: https://docs.cursor.com/chat/overview

## 3. Visão de arquitetura planejada

### Fontes de dados

-   Dataset histórico de e-commerce Olist, após verificar licença e
    condições de redistribuição.
-   API pública com paginação e limites de requisição.
-   Banco transacional PostgreSQL.
-   Eventos sintéticos para cenários de streaming.

### Plataforma e processamento

-   Python para clientes de ingestão, utilitários e lógica de apoio.
-   Apache Airflow para orquestração batch.
-   PostgreSQL como fonte transacional para o fluxo CDC.
-   Debezium + Kafka para captura e transporte de alterações.
-   Spark para processamento de dados, incluindo Structured Streaming
    quando aplicável.
-   MinIO local e Amazon S3 na AWS como armazenamento de objetos.
-   Parquet e Apache Iceberg para armazenamento tabular/lakehouse, após
    validar compatibilidade das versões.
-   Catálogo de tabelas: Glue Catalog na AWS; opção local a definir
    conforme a prova de conceito.
-   Trino local e Athena na AWS para consulta analítica.
-   dbt para transformação e modelagem analítica.
-   Testes de qualidade com dbt e Python.
-   OpenLineage para linhagem, integrado aos componentes efetivamente
    suportados.
-   Prometheus e Grafana para métricas e dashboards.
-   Terraform para infraestrutura AWS.
-   GitHub Actions para CI/CD.

### Camadas de dados

-   **Bronze:** dados ingeridos próximos da origem, preservando
    metadados de ingestão e rastreabilidade.
-   **Silver:** dados normalizados, tipados, deduplicados e validados.
-   **Gold:** modelos analíticos e data marts orientados a casos de uso.

A arquitetura final deve ser escolhida com base em provas de conceito
pequenas e testáveis. Não presumir que todas as combinações de versões
de Spark, Iceberg, catálogos e motores SQL funcionarão sem validação.

### Modelos analíticos previstos

Possíveis fatos: - `fct_orders` - `fct_order_items` - `fct_payments`

Possíveis dimensões: - `dim_customers` - `dim_products` - `dim_date`

Data marts: - `mart_daily_sales` - `mart_customer_retention`

A granularidade, chaves, tratamento de atualizações e regras de negócio
devem ser documentados antes da implementação dos modelos.

## 4. Cronograma e fases

Estimativa inicial: **16 semanas**, aproximadamente **240--320 horas**,
dependendo do ritmo, dos problemas de compatibilidade e da profundidade
de cada fase. O cronograma é um plano, não uma promessa de conclusão.

### Fase 0 --- Fundação do repositório e ambiente (semana 1)

Objetivos: - Inicializar pacote Python com `uv`, Python 3.12 e
lockfile. - Configurar Ruff e pytest. - Criar Docker Compose inicial
para PostgreSQL 16 e MinIO. - Criar `.env.example`, `.gitignore`,
Makefile e README inicial. - Configurar workflow inicial de GitHub
Actions. - Documentar setup e decisões iniciais.

Critérios de aceite: - Um clone limpo consegue seguir as instruções de
setup. - PostgreSQL fica saudável e acessível. - É possível escrever e
ler um objeto no MinIO. - Lint e testes básicos executam. - Nenhum
segredo, arquivo `.env` real ou dado privado é versionado. - O CI
executa as verificações previstas.

Fora do escopo desta fase: Airflow, Kafka, Spark, Trino, Iceberg e
implantação AWS.

### Fase 1 --- Ingestão de múltiplas fontes (semanas 1--2)

Objetivos: - Ingerir dataset histórico. - Criar cliente reutilizável
para API pública paginada, com fixtures locais. - Ingerir dados do
PostgreSQL. - Gerar eventos sintéticos. - Definir contratos, metadados e
convenções de partição/caminhos. - Tratar timeouts, retries, limites de
requisição e falhas parciais.

Critérios de aceite: - Ingestões podem ser repetidas sem duplicação
indevida ou corrupção. - Testes não dependem de a API pública estar
online. - Dados brutos são rastreáveis por fonte e execução. - Contratos
e campos esperados são documentados. - Erros e registros inválidos têm
comportamento explícito.

Não introduzir streaming, Iceberg ou Airflow nesta fase sem uma
necessidade demonstrada.

### Fase 2 --- Lakehouse e analytics (semanas 3--4)

Objetivos: - Fazer uma prova de conceito mínima de Spark + Iceberg +
catálogo + motor de consulta. - Definir Bronze/Silver/Gold. -
Implementar primeiros fluxos de transformação. - Criar modelos dbt e
testes de modelo. - Documentar granularidade, chaves e reconciliações.

Critérios de aceite: - Uma tabela pode ser criada, escrita e consultada
no ambiente escolhido. - Consultas e transformações são reproduzíveis. -
Regras de qualidade e reconciliação têm testes. - A documentação
identifica versões e limitações conhecidas.

Não escalar a arquitetura antes de validar a compatibilidade da stack.

### Fase 3 --- Orquestração e qualidade de dados (semanas 5--6)

Objetivos: - Adicionar Airflow e DAGs. - Implementar retries, backfills
e execução idempotente. - Adicionar verificações de qualidade,
quarentena e reconciliação. - Registrar metadados de execução e falhas.

Critérios de aceite: - DAGs têm dependências claras e comportamento de
falha verificável. - Backfills não corrompem dados nem geram duplicação
indevida. - Registros inválidos são identificados e tratados segundo
regras documentadas. - Há testes para cenários de sucesso e falha.

Não adicionar streaming ainda.

### Fase 4 --- CDC e streaming (semanas 7--9)

Objetivos: - Capturar alterações do PostgreSQL via replicação lógica. -
Configurar Debezium e Kafka. - Processar eventos com Spark Structured
Streaming, conforme compatibilidade validada. - Documentar semântica de
entrega, snapshots, atualizações, deletes, duplicatas, eventos
atrasados, checkpoints e replay.

Critérios de aceite: - Inserções, atualizações e exclusões são tratadas
corretamente. - Duplicatas e eventos fora de ordem têm tratamento
explícito. - Reiniciar o processamento e reproduzir eventos não causa
corrupção. - A documentação não promete "exactly once" sem prova técnica
específica.

### Fase 5 --- Observabilidade, linhagem e FinOps (semanas 10--11)

Objetivos: - Adicionar logs estruturados. - Criar métricas e dashboards
Prometheus/Grafana. - Integrar OpenLineage aos componentes realmente
suportados. - Documentar custos estimados e, quando houver implantação,
custos observados na AWS. - Definir alertas úteis e runbooks.

Critérios de aceite: - Falhas e atrasos relevantes podem ser
diagnosticados. - Linhagem demonstrada corresponde a execuções reais. -
Métricas são medidas, não inventadas. - Estimativas de custo identificam
premissas; custos reais são diferenciados de estimativas.

### Fase 6 --- CI/CD, resiliência e segurança (semanas 12--13)

Objetivos: - Automatizar lint, testes Python, testes dbt e validações
Terraform. - Isolar testes de integração. - Executar testes de
resiliência para falhas, retries e recuperação. - Adicionar verificações
de segurança apropriadas.

Critérios de aceite: - CI falha quando uma verificação obrigatória
falha. - Testes de integração são reproduzíveis e isolados. - Cenários
de recuperação têm evidência. - Segredos e credenciais não são expostos
em logs ou artefatos.

Não implantar na nuvem antes de a validação local estar estável.

### Fase 7 --- AWS, documentação final e demonstração (semanas 14--16)

Objetivos: - Definir uma implantação AWS mínima e econômica. -
Provisionar infraestrutura via Terraform. - Configurar S3, Glue Catalog,
Athena, IAM e OIDC para GitHub Actions quando apropriado. - Configurar
backend remoto do Terraform com versionamento e mecanismo de lock
compatível com a versão usada. - Medir desempenho e custos reais, se
houver implantação. - Finalizar README, diagramas, runbooks e
demonstração.

Critérios de aceite: - Implantação é reproduzível e documentada. - IAM
segue o princípio do menor privilégio. - CI usa OIDC quando aplicável,
evitando chaves AWS de longa duração. - Custos, limitações e
procedimento de limpeza são claros. - Operações de nuvem são executadas
apenas após revisão e aprovação explícita.

## 5. Estrutura de diretórios proposta

Ajustar conforme a implementação real; evitar criar pastas vazias sem
propósito.

``` text
open-data-platform/
├── AGENTS.md
├── PROJECT_STATUS.md
├── ROADMAP.md
├── DECISIONS.md
├── README.md
├── pyproject.toml
├── uv.lock
├── Makefile
├── .env.example
├── .gitignore
├── .cursor/
│   └── rules/
│       ├── 00-project-principles.mdc
│       ├── 01-python-and-testing.mdc
│       ├── 02-data-engineering.mdc
│       ├── 03-infrastructure-and-security.mdc
│       └── 04-agent-workflow.mdc
├── .github/
│   └── workflows/
├── config/
│   └── contracts/
├── src/
│   └── open_data_platform/
├── orchestration/
│   └── dags/
├── streaming/
│   ├── debezium/
│   ├── kafka/
│   └── schemas/
├── dbt/
├── infrastructure/
│   └── terraform/
├── observability/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── data/
│   └── end_to_end/
├── data/
│   ├── fixtures/
│   └── samples/
└── docs/
    ├── architecture/
    ├── decisions/
    ├── runbooks/
    ├── phase-reports/
    └── acceptance-criteria.md
```

## 6. Instruções para o Cursor Agent

Criar ou adaptar o arquivo `AGENTS.md` na raiz do repositório. Conteúdo
sugerido:

``` markdown
# Instructions for AI coding agents

## Project goal
Build a reproducible, well-documented end-to-end data engineering platform.
Optimize for correctness, clarity, testability, operational safety, and portfolio value.

## Communication and documentation
- Communicate with the user in Brazilian Portuguese.
- Write code, identifiers, commit messages, and technical documentation in English unless explicitly requested otherwise.
- Treat ROADMAP.md, PROJECT_STATUS.md, DECISIONS.md, and acceptance criteria as the project source of truth.
- Do not claim planned work is implemented or tested unless evidence exists.

## Mandatory workflow
1. Read AGENTS.md and PROJECT_STATUS.md before implementation.
2. Inspect the relevant roadmap item, acceptance criteria, code, and tests.
3. For non-trivial work, propose a plan before editing files and wait for approval.
4. Keep each task small and focused on one coherent change.
5. Implement tests with the change when appropriate.
6. Run relevant checks and report their actual results.
7. Update documentation and PROJECT_STATUS.md after verified progress.
8. Report changed files, commands run, test results, limitations, and the next recommended task.

## Architecture principles
- Prefer simple, maintainable solutions over adding technologies for appearance.
- Make batch ingestion and transformations idempotent where appropriate.
- Design for retries, backfills, observability, and reproducibility.
- Define data contracts and explicit handling of invalid data.
- Document CDC semantics, duplicates, ordering, deletes, snapshots, checkpoints, and replay.
- Validate compatibility between Spark, Iceberg, catalogs, and query engines before expanding the stack.
- Keep business logic reusable across local and cloud environments where practical.

## Engineering standards
- Use type hints and clear interfaces in Python.
- Use structured logging and explicit error handling.
- Keep configuration out of source code and use environment variables or appropriate configuration files.
- Write deterministic tests; avoid requiring live public APIs in unit tests.
- Include unit, integration, data-quality, and end-to-end tests as appropriate.
- Do not silently change schemas, contracts, architecture, dependencies, or acceptance criteria.

## Environment
- Primary development environment is Ubuntu under WSL2 on Windows 11.
- Keep the repository in the WSL Linux filesystem, not under /mnt/c, when possible.
- Use Docker Compose for local services.
- Pin or lock dependency versions and document compatibility constraints.

## Security and cost
- Never commit secrets, credentials, tokens, or real .env files.
- Never run Terraform apply/destroy, delete cloud resources, or execute destructive Docker volume commands without explicit user approval.
- Ask before any operation that can incur meaningful cloud costs.
- Prefer IAM least privilege and short-lived credentials/OIDC over long-lived cloud keys.
- Do not expose secrets in logs, test output, screenshots, or documentation.

## Truthfulness
- Never report an unexecuted test as passing.
- Distinguish confirmed root causes from hypotheses.
- If blocked, explain the blocker and the smallest next step.
- Do not invent performance metrics, cost figures, lineage, or benchmark results.

## Completion report
At the end of each task, include:
- Summary of changes
- Files changed
- Commands actually run
- Tests/checks and their actual results
- Acceptance criteria met and not met
- Known risks or limitations
- Suggested next task
```

### Regra obrigatória do fluxo de trabalho do Cursor

Arquivo sugerido `.cursor/rules/04-agent-workflow.mdc`:

``` markdown
---
description: Mandatory workflow for all implementation tasks
alwaysApply: true
---
- Read AGENTS.md and PROJECT_STATUS.md before making changes.
- Check the relevant roadmap item and acceptance criteria.
- For non-trivial work, present a plan before editing files.
- Implement one coherent task at a time.
- Run relevant validation commands and report actual results.
- Update docs and project status after completing verified work.
- Ask before destructive operations, cloud deployments, resource deletion, or commands that may incur costs.
- Do not silently change architecture, dependencies, or acceptance criteria.
- Never report an unexecuted test as passing.
```

### Regras adicionais recomendadas

-   `00-project-principles.mdc`: escopo, simplicidade, qualidade e fonte
    da verdade.
-   `01-python-and-testing.mdc`: tipagem, lint, testes determinísticos e
    padrões Python.
-   `02-data-engineering.mdc`: contratos, idempotência, camadas, CDC,
    streaming, qualidade e linhagem.
-   `03-infrastructure-and-security.mdc`: Terraform, IAM, segredos,
    custos e operações destrutivas.

As regras devem ser concisas, não contradizer o `AGENTS.md` e refletir o
estado real do projeto.

## 7. Arquivos de estado e governança

### `PROJECT_STATUS.md`

Manter atualizado e factual. Modelo:

``` markdown
# Project Status

## Current phase
Phase 0 — Foundation (update as work progresses)

## Current objective
[One objective only]

## Verified complete
- [ ] Item — evidence/command/commit

## In progress
- [ ] Item — owner/next action

## Blocked
- None / explain blocker and required decision

## Known limitations
- [List current limitations]

## Decisions requiring review
- [List only decisions that need user input]

## Next task
[Smallest useful next task]

## Latest validation
- Date:
- Environment:
- Command:
- Result:
- Evidence/notes:
```

### `ROADMAP.md`

-   Listar fases, tarefas e critérios de aceite.
-   Marcar itens concluídos apenas quando houver evidência.
-   Separar "implementado" de "validado".
-   Registrar bloqueios e dependências.

### `DECISIONS.md`

Registrar decisões arquiteturais em formato leve: - Contexto/problema. -
Decisão. - Alternativas consideradas. - Consequências e trade-offs. -
Estado: proposta, aprovada, substituída. - Data e evidência relevante.

Para decisões relevantes, criar ADRs em `docs/decisions/`.

## 8. Prompts reutilizáveis para o Cursor Agent

Use tarefas pequenas e forneça o contexto necessário. Não peça para o
agente implementar todas as fases em uma única conversa.

### Prompt A --- Planejar uma issue sem editar arquivos

``` text
Follow AGENTS.md and .cursor/rules/04-agent-workflow.mdc.

I want to work on: [ISSUE ID AND TITLE].

First inspect the relevant code, documentation, roadmap, acceptance criteria, and current project status. Do not edit files yet.

Return:
1. Current state and relevant files.
2. Dependencies and prerequisites.
3. A small, ordered implementation plan.
4. Acceptance criteria and how each will be verified.
5. Risks, assumptions, and compatibility concerns.
6. Exact commands to run.
7. Expected files to create or modify.

Keep the scope limited to this issue. Do not implement future phases or silently change architecture. Wait for my approval before editing.
```

### Prompt B --- Implementar após aprovação do plano

``` text
Implement only the plan we just agreed on.

Follow AGENTS.md and the applicable Cursor rules.
- Keep the diff focused and explain any necessary deviation before proceeding.
- Add or update tests for the behavior changed.
- Do not implement tasks from future phases.
- Do not add dependencies or services unless justified by the approved plan.
- Do not run destructive commands, deploy cloud resources, or incur cloud costs without my explicit approval.
- Run the relevant checks and report the actual output/result.
- Update documentation and PROJECT_STATUS.md only for work that has actually been verified.

At the end, report changed files, checks run, results, acceptance criteria met/not met, limitations, and the next recommended task.
```

### Prompt C --- Revisão de uma fase sem alterar código

``` text
Review [PHASE NUMBER AND NAME] against ROADMAP.md and its acceptance criteria.

Do not modify files.

Classify every acceptance criterion as:
- Implemented and verified
- Implemented but not verified
- Partially implemented
- Not implemented
- Blocked

For each item, cite the relevant file/test/command or explain what evidence is missing.
Inspect whether tests are meaningful and whether failure/recovery paths are covered.
Identify architectural risks, undocumented assumptions, security/cost concerns, and unnecessary complexity.
Finish with a prioritized list of the smallest next actions.
Do not claim anything is verified without evidence.
```

### Prompt D --- Depuração

``` text
Follow AGENTS.md.

Investigate this failure: [ERROR / SYMPTOM].

First reproduce or inspect the failure using the smallest safe command. Do not make broad changes.
Separate:
1. Confirmed facts and evidence.
2. Root cause, if established.
3. Hypotheses that are not yet proven.
4. Minimal fix.
5. Regression test.
6. Exact validation commands.

Do not expose secrets. Do not run destructive commands or cloud operations without approval. After approval, implement the minimal fix and report actual test results.
```

### Prompt E --- Fechar uma tarefa

``` text
Review the current diff against the agreed plan and acceptance criteria.

Do not add features.
- Check for unrelated changes, secrets, generated files, weak tests, configuration drift, and undocumented behavior.
- Run relevant validation commands and report their actual results.
- Identify acceptance criteria that are not met or not verified.
- Update PROJECT_STATUS.md and relevant docs only when the evidence supports the update.
- Do not commit or push unless I explicitly ask.

Return a concise completion report and the next smallest useful task.
```

## 9. Prompts específicos por fase

### Fase 0 --- Fundação

``` text
Work only on Phase 0 — Foundation.

Inspect the repository first and propose a plan before editing.

Scope:
- Initialize a Python 3.12 package using uv and commit the lockfile.
- Configure Ruff and pytest.
- Add Docker Compose for PostgreSQL 16 and MinIO with health checks where supported.
- Add .env.example, .gitignore, a useful Makefile, and setup instructions in README.md.
- Add a minimal GitHub Actions workflow for lint and tests.
- Add or update a short architecture/decision note and PROJECT_STATUS.md.

Acceptance criteria:
- A clean checkout can follow the setup instructions.
- PostgreSQL becomes healthy and can be reached.
- A smoke test can write and read an object from MinIO.
- Lint and tests run with documented commands.
- No secrets, actual .env files, or private data are tracked.
- CI runs the intended checks.

Do not add Airflow, Kafka, Spark, Trino, Iceberg, Kubernetes, or AWS deployment in this phase.
Do not use unverified image tags. Pin suitable versions and document compatibility.
Do not run destructive volume cleanup without approval.
```

### Fase 1 --- Ingestão

``` text
Work only on Phase 1 — Multi-source ingestion.

Implement small, testable ingestion components for:
1. A historical e-commerce dataset, after checking its license and redistribution terms.
2. A paginated public API with timeouts, retries, rate-limit handling, and local fixtures.
3. PostgreSQL source extraction.
4. Synthetic event generation.

Define data contracts, ingestion metadata, raw storage layout, and failure behavior.
Keep unit tests independent of live public APIs.
Demonstrate repeatable execution and idempotency where appropriate.

Do not add streaming, Iceberg, or Airflow in this phase.
First propose a plan and acceptance criteria; wait for approval before editing.
```

### Fase 2 --- Lakehouse e analytics

``` text
Work only on Phase 2 — Lakehouse and analytics.

Before expanding the implementation, create a minimal proof of concept to validate compatible versions and the path to create, write, catalog, and query a table using the chosen Spark/Iceberg/catalog/query-engine combination.

Then implement the agreed Bronze/Silver/Gold transformations and initial dbt models.
Document model grain, keys, incremental behavior, and business assumptions.
Add data-quality tests and reconciliation checks.

Do not assume compatibility without executing a working proof of concept.
Do not introduce orchestration or streaming unless the roadmap is explicitly revised and approved.
```

### Fase 3 --- Airflow e qualidade

``` text
Work only on Phase 3 — Orchestration and data quality.

Add Airflow DAGs for the existing validated ingestion/transformation flows.
Define retries, timeouts, dependencies, idempotency, backfill behavior, logging, and failure handling.
Add data-quality checks, reconciliation, and a quarantine strategy for invalid records.

Test both successful and failed runs, including a rerun/backfill.
Do not add CDC or streaming in this phase.
Do not mark a DAG as production-ready unless its acceptance criteria are demonstrated.
```

### Fase 4 --- CDC e streaming

``` text
Work only on Phase 4 — CDC and streaming.

Validate PostgreSQL logical replication and the selected Debezium/Kafka versions first.
Implement the smallest end-to-end path for inserts, updates, and deletes.
Add Spark Structured Streaming only after the event path is verified.
Document event schema, keys, snapshot behavior, duplicate handling, ordering, late events, checkpoints, replay, and recovery.

Add tests or reproducible scenarios for duplicate messages, out-of-order events, restart/replay, and deletes.
Do not claim exactly-once semantics without a specific technical proof.
Do not break the already validated batch path.
```

### Fase 5 --- Observabilidade, linhagem e custos

``` text
Work only on Phase 5 — Observability, lineage, and FinOps.

Add structured logs and the minimum useful metrics/dashboards using Prometheus/Grafana.
Integrate OpenLineage only for components with verified support.
Demonstrate lineage from an actual run rather than a mocked or assumed graph.
Document alert thresholds and runbooks.
Separate estimated cloud costs from observed AWS costs and state the assumptions behind every estimate.

Do not invent metrics, benchmark results, lineage, or cost numbers.
Do not deploy resources or incur cloud costs without approval.
```

### Fase 6 --- CI/CD, segurança e resiliência

``` text
Work only on Phase 6 — CI/CD, security, and resilience.

Extend GitHub Actions for Python lint/tests, dbt checks, Terraform validation, and isolated integration tests as applicable.
Add relevant security checks without leaking credentials or creating noisy, unexplained failures.
Exercise failure/retry/recovery paths and document evidence.
Ensure CI fails when a required check fails.

Do not deploy to AWS in this phase.
Do not weaken or skip tests merely to obtain a green pipeline.
```

### Fase 7 --- AWS e demonstração

``` text
Work only on Phase 7 — AWS deployment and final case study.

First propose a minimal architecture and estimate cost assumptions for review. Do not deploy until I explicitly approve.
Use Terraform, S3, Glue Catalog, Athena, least-privilege IAM, and GitHub OIDC where appropriate.
Use a remote Terraform state with versioning and a locking mechanism supported by the selected Terraform version.
Avoid long-lived AWS credentials.
Document deployment, verification, cost monitoring, teardown, limitations, and recovery procedures.

Measure real performance and costs only when the relevant workload has actually run.
Do not run apply, destroy, delete resources, or incur cloud costs without explicit approval.
Finish with a clear README case study, architecture diagrams, setup instructions, runbooks, and a demonstration path.
```

## 10. Estratégia de Git

Fluxo recomendado por tarefa:

``` bash
git switch -c feat/short-task-name
git status
git diff --check
git diff
make lint
make test
git add <only-relevant-paths>
git commit -m "feat: describe the change"
git push -u origin feat/short-task-name
```

Observações: - Ajustar comandos aos targets reais do Makefile. - Não
assumir que `make lint` ou `make test` existem antes de criá-los. -
Revisar o diff e os caminhos adicionados antes do commit. - Não incluir
`.env`, credenciais, dados privados, caches, arquivos gerados ou
dumps. - Não fazer commit/push sem autorização explícita, se essa for a
preferência definida no repositório.

## 11. Rotina semanal sugerida

Com 15+ horas por semana: - 2--3 horas: planejamento, leitura técnica e
revisão de decisões. - 9--12 horas: implementação em tarefas pequenas. -
3--4 horas: testes, debugging, documentação, revisão de diffs e commits.

O avanço deve ser medido por critérios de aceite e evidências, não pela
quantidade de tecnologias adicionadas.

## 12. Plano de início

Checklist inicial:

-   [ ] Instalar/atualizar WSL2 e Ubuntu.
-   [ ] Instalar Docker Desktop e habilitar integração WSL para Ubuntu.
-   [ ] Verificar Docker e Compose dentro do Ubuntu.
-   [ ] Criar/abrir o repositório em `~/projects/open-data-platform`.
-   [ ] Criar `AGENTS.md`, `PROJECT_STATUS.md`, `ROADMAP.md` e
    `DECISIONS.md`.
-   [ ] Criar regras Cursor em `.cursor/rules/`.
-   [ ] Pedir ao Cursor para planejar somente a Fase 0, sem editar
    arquivos.
-   [ ] Revisar e aprovar o plano.
-   [ ] Implementar a Fase 0 em pequenas partes.
-   [ ] Executar os testes e registrar os resultados reais.
-   [ ] Revisar o diff e fazer o primeiro commit.

## 13. Como manter este documento útil

Este arquivo é um **documento de contexto consolidado**, não substitui o
estado vivo do repositório.

-   Salvar como `docs/project-context-for-cursor.md` ou
    `PROJECT_CONTEXT.md`.
-   Manter `AGENTS.md` curto e normativo; colocar detalhes extensos e
    prompts neste documento.
-   Manter `PROJECT_STATUS.md` como fonte de verdade para o que foi
    feito e validado.
-   Atualizar `ROADMAP.md` quando escopo, fases ou critérios mudarem.
-   Registrar decisões arquiteturais reais em `DECISIONS.md` ou ADRs.
-   Se este documento divergir do código ou de uma decisão aprovada mais
    recente, investigar a divergência; não assumir que o plano antigo
    prevalece.
-   Antes de cada tarefa, pedir ao Cursor que leia os documentos
    relevantes, mas evitar despejar todo o conteúdo em toda interação.
-   Não considerar uma tecnologia obrigatória se uma prova de conceito
    mostrar incompatibilidade, custo excessivo ou complexidade
    desnecessária. Propor alternativa, explicar trade-offs e pedir
    aprovação antes de mudar a arquitetura.
