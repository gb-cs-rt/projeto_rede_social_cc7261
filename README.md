# Projeto Rede Social Distribuída - CC7261

## Objetivo

Desenvolver um sistema distribuído para uma rede social que permita a interação entre usuários, incluindo:

- Publicação de textos (posts)
- Troca de mensagens privadas
- Notificações de novas postagens de usuários seguidos
- Sincronização e replicação de dados entre múltiplos servidores
- Consistência e ordenação garantidas por relógios lógicos

O sistema utiliza algoritmos de sincronização de relógios e eleição de coordenador via Bullying para garantir a consistência entre múltiplos servidores, mantendo alta disponibilidade e tolerância a falhas.

---

## Instruções de execução

### Pré-requisitos

- Docker
- Docker Compose

### Clonando e executando o projeto

```bash
git clone https://github.com/gb-cs-rt/projeto_rede_social_cc7261.git
````
```bash
cd projeto_rede_social_cc7261
````
```bash
docker compose up --build
````

Após o build e execução, o front-end web estará acessível via:

* **Front-end**: `http://localhost:8080`

Os dados e logs de cada servidor serão armazenados em:

```
projeto_rede_social_cc7261/server/data
```

---

## Funcionalidades implementadas

* Publicação de posts e visualização no feed
* Seguir usuários
* Recebimento de notificação ao ser publicado novo post de usuário seguido
* Troca de mensagens privadas entre usuários que se seguem
* Armazenamento de dados em arquivos JSON locais por servidor
* Replicação automática de dados entre os servidores via RabbitMQ (fanout exchange)
* Relógio lógico com atraso aleatório (simulação de dessincronização)
* Sincronização de relógio lógico sempre que servidor recebe mensagem com timestamp mais recente
* Coordenação inicial: thread com `id=1` inicia como coordenador
* Coordenador envia timestamp de sincronização a cada 5 segundos
* Detecção automática de falha do coordenador: se coordenador não envia timestamp em 10 segundos, inicia-se eleição via Bullying
* Eleição:

  * Thread detecta falha do coordenador
  * Envia eleição para threads de ID superior
  * Maior ID se proclama novo coordenador
  * Novo coordenador passa a publicar sua timestamp
* Atualização automática do feed e chat a cada 5 segundos
* Dockerfile em todos os módulos (API, Client, Server), facilitando execução integrada via `docker compose`

---

## Arquitetura do sistema

![Arquitetura geral](https://raw.githubusercontent.com/gb-cs-rt/projeto_rede_social_cc7261/refs/heads/main/docs/arquitetura.jpg)
![Esquemas de mensagens](https://raw.githubusercontent.com/gb-cs-rt/projeto_rede_social_cc7261/refs/heads/main/docs/mensagens.jpg)

* Cliente acessa front-end desenvolvido em HTML, CSS e JavaScript
* JavaScript realiza requisições:

  * POST: criação de post, seguir usuário, enviar mensagem
  * GET: recuperar feed, followers, histórico de mensagens
* API REST desenvolvida com FastAPI (Python) recebe requisições do cliente
* API encaminha mensagens para o RabbitMQ via `msg_queue`
* RabbitMQ distribui as mensagens aos servidores Java utilizando round-robin
* Servidor Java:

  * Processa a mensagem
  * Atualiza seu JSON local
  * Publica a atualização no `update_exchange` (fanout)
  * Responde a requisição
* Todos os servidores recebem atualizações e mantêm seus dados sincronizados
* Coordenador envia a cada 5 segundos seu relógio lógico via `clock_sync_exchange`
* Servidores ajustam seus relógios conforme mensagem do coordenador
* Em falha de coordenador (sem mensagem em 10s), threads iniciam eleição via `election_queue`
* Maior ID se proclama novo coordenador e passa a enviar sincronizações

---

## Fluxo de mensagens

### Requisições do Cliente via API:

```
/post
/posts
/follow
/follows
/mutual-follows
/enviar-mensagem
/get-historico
/shutdown-coordinator
```

Cada requisição é convertida em uma mensagem JSON, enviada ao RabbitMQ e processada pelos servidores.

---

## Tecnologias utilizadas

* Java — Servidores de processamento e replicação de dados
* Python — FastAPI para API REST
* RabbitMQ — Sistema de mensagens para comunicação entre componentes
* Docker & Docker Compose — Containerização e orquestração
* HTML/CSS/JS — Front-end interativo

---

## Estrutura do repositório

```
├── api/               # Código da API FastAPI
├── client/            # Código do front-end (HTML/CSS/JS)
├── server/            # Código do servidor Java + arquivos JSON e logs
├── docker-compose.yml # Orquestração de containers
└── README.md          # Este arquivo
```

---

## Observações

* A troca de mensagens entre servidores garante a consistência eventual dos dados.
* O uso de relógios lógicos e algoritmo de eleição garante a ordenação e sincronização em ambiente distribuído.
* Sistema resiliente: ao derrubar o coordenador, nova eleição ocorre automaticamente.
