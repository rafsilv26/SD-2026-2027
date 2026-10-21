# Guia do Estudante

Sprints/E1-COMINDIR02/rep02-produtor/src/rep02/Partitioner.java — calcula a partição a partir do ID do sensor.

Sprints/E1-COMINDIR02/rep02-produtor/src/rep02/Producer.java — escolhe o tópico da partição e publica a leitura.

Sprints/E1-COMINDIR02/rep02-lider/src/rep02/PartitionLeader.java — recebe as leituras, atribui sequências, grava os registos e responde aos pedidos de recuperação TCP.

Sprints/E1-COMINDIR02/rep02-replica/src/rep02/PartitionReplica.java — recebe e ordena os registos, deteta lacunas e pede os dados em falta ao líder.

Sprints/E1-COMINDIR02/rep02-lider/src/rep02/RecordFile.java — grava e lê intervalos de registos usados na recuperação.
Este repositório contém os exercícios e tarefas da disciplina. Segue este guia para configurar a tua área de trabalho local, resolver as tarefas e submetê-las para avaliação.

> **Modelo de trabalho:** *fork* + *branch* por tarefa + *Pull Request*. Cada estudante trabalha na sua cópia pessoal do repositório e submete o trabalho através de um Pull Request para o repositório do docente.

## Índice

- [0. Pré-requisitos](#0-pré-requisitos)
- [1. Configuração inicial (setup)](#1-configuração-inicial-setup)
- [2. Fluxo de trabalho passo a passo](#2-fluxo-de-trabalho-passo-a-passo)
- [3. Avaliação e integração de novas tarefas](#3-avaliação-e-integração-de-novas-tarefas)
- [4. Exemplo prático completo (Tarefa 2)](#4-exemplo-prático-completo-tarefa-2)
- [5. Referência rápida de comandos](#5-referência-rápida-de-comandos)
- [6. Problemas frequentes](#6-problemas-frequentes)
- [7. Regras de ouro](#7-regras-de-ouro)

---

## 0. Pré-requisitos

- **Conta no GitHub** ([criar aqui](https://github.com/signup)).

Se NÃO for utilizado o IntelliJ podem ser seguidos os passos seguintes para configurar o repositório Git em linha de comando:
- **Identidade configurada** (só é preciso fazer uma vez por computador):
- **Git instalado** ([download](https://git-scm.com/downloads)). Verifica com `git --version`.

  ```bash
  git config --global user.name "O Teu Nome"
  git config --global user.email "o_teu_email@exemplo.pt"
  ```

- **Autenticação configurada.** O GitHub já não aceita palavra-passe na linha de comandos. Usa uma das opções:
   - [GitHub CLI](https://cli.github.com/): `gh auth login` (mais simples);
   - ou uma [chave SSH](https://docs.github.com/pt/authentication/connecting-to-github-with-ssh).

---

## 1. Configuração inicial (*setup*)

Esta secção faz-se **uma única vez**, no início do semestre.

### 1.1. Fazer *fork* do repositório

Na página do repositório do docente ([`cunhaestgv/SD-2026-2027`](https://github.com/cunhaestgv/SD-2026-2027)), clica em **Fork** (canto superior direito) para criar uma cópia pessoal na tua conta:

```text
cunhaestgv/SD-2026-2027   →   O-TEU-UTILIZADOR/SD-2026-2027
```

### 1.2. Clonar o *teu fork* para a máquina local

> ⚠️ Atenção: o clone é feito a partir do **teu fork**, não do repositório do docente. Substitui `O-TEU-UTILIZADOR` pelo teu nome de utilizador do GitHub.

```bash
git clone https://github.com/O-TEU-UTILIZADOR/SD-2026-2027.git
cd SD-2026-2027
```

### 1.3. Adicionar o repositório do docente como `upstream`

O `upstream` é a ligação ao repositório original, que permite receber as novas tarefas publicadas pelo docente:

```bash
git remote add upstream https://github.com/cunhaestgv/SD-2026-2027.git
```

### 1.4. Confirmar que está tudo correto

```bash
git remote -v
```

O resultado esperado é:

```text
origin    https://github.com/O-TEU-UTILIZADOR/SD-2026-2027.git (fetch)
origin    https://github.com/O-TEU-UTILIZADOR/SD-2026-2027.git (push)
upstream  https://github.com/cunhaestgv/SD-2026-2027.git (fetch)
upstream  https://github.com/cunhaestgv/SD-2026-2027.git (push)
```

| *Remote* | Aponta para | Serve para |
| --- | --- | --- |
| `origin` | O **teu** fork | Enviar (*push*) o teu trabalho |
| `upstream` | Repositório do **docente** | Receber (*fetch*) novas tarefas |

---

## 2. Fluxo de trabalho para uma nova tarefa

Repete esta sequência **sempre que iniciares uma nova tarefa**.

<img width="1472" height="1312" alt="image" src="https://github.com/user-attachments/assets/6f4712f2-400a-49ff-ba28-c9f93e3dfeff" />

### Passo A — Atualizar a branch principal (`master`)

Antes de começar, garante que tens a versão mais recente publicada pelo docente:

```bash
git checkout master
git fetch upstream
git merge upstream/master
git push origin master
```

### Passo B — Criar uma branch para a tarefa

Nunca trabalhes diretamente na `master`. Cria uma branch nova por cada exercício:

```bash
git checkout -b tarefa-1
```

**Convenção de nomes:** letras minúsculas, sem acentos nem espaços — `tarefa-1`, `tarefa-2`, `tarefa-3-extra`.

### Passo C — Trabalhar, fazer *commit* e *push*

Resolve o exercício, guarda o progresso com *commits* e envia a branch para o teu fork:

```bash
git status                 # ver o que foi alterado
git add .                  # preparar as alterações
git commit -m "Resolução da Tarefa 1: cliente/servidor TCP"
git push origin tarefa-1
```

**Boas práticas de *commit*:**

- Escreve mensagens no commit que descrevam **o que** foi feito: `"Implementação da Tarefa UDP001"` em vez de `"update"` ou `"cenas"`.
- Não envies ficheiros gerados pela compilação nem pastas do IDE (`bin/`, `target/`, `.idea/`, `node_modules/`) — usa um ficheiro `.gitignore`.

### Passo D — Criar o *Pull Request* (PR) para avaliação

1. Acede à página do **teu fork** no GitHub.
2. Clica no botão **Compare & pull request** que aparece relativo à branch enviada (`tarefa-1`).
3. Confirma a direção do PR:
   - **base repository:** `cunhaestgv/SD-2026-2027` · **base:** `master`
   - **head repository:** `O-TEU-UTILIZADOR/SD-2026-2027` · **compare:** `tarefa-1`
4. Dá um título claro ao PR (ex.: `Tarefa UDP001`) e descreve brevemente o que foi feito.
5. Clica em **Create pull request** e aguarda a revisão do docente.

> 💡 Podes continuar a fazer *commits* e *push* para a branch `tarefa-1` depois de abrires o PR — o Pull Request atualiza-se automaticamente. É assim que se aplicam as correções pedidas na revisão.

---

## 3. Avaliação e integração de novas tarefas

1. **Revisão:** o docente avalia o Pull Request na aula, durante a apresentação.
2. **Merge:** quando o trabalho é aprovado, o docente aceita (*merge*) as alterações e/ou publica novas tarefas no repositório central (`upstream`).
3. **Sincronização:** depois do *merge*, volta à `master`, sincroniza com as novidades e elimina a branch já concluída:

```bash
# Voltar à branch master
git checkout master

# Obter e aplicar as novas tarefas/atualizações do docente
git fetch upstream
git merge upstream/master

# Atualizar o teu fork no GitHub
git push origin master

# (Opcional) Eliminar a branch da tarefa já concluída
git branch -d tarefa-1              # local
git push origin --delete tarefa-1   # no teu fork do GitHub
```

---

## 4. Exemplo prático completo (Tarefa 2)

```bash
# 1. Garantir que a master está atualizada
git checkout master
git fetch upstream
git merge upstream/master
git push origin master

# 2. Criar a branch da Tarefa 2
git checkout -b tarefa-2

# 3. Trabalhar nos ficheiros, adicionar e fazer commit
git add .
git commit -m "Resolução da Tarefa 2: sincronização entre threads"

# 4. Enviar a branch para o teu fork
git push origin tarefa-2

# 5. Abrir o Pull Request no GitHub para o docente avaliar
```

---

## 5. Referência rápida de comandos

| Objetivo                                        | Comando |
|-------------------------------------------------| --- |
| Ver o estado do repositório                     | `git status` |
| Ver em que branch estou                         | `git branch` |
| Ver o histórico resumido                        | `git log --oneline --graph --all` |
| Mudar de branch                                 | `git checkout nome-da-branch` |
| Criar e mudar para uma branch                   | `git checkout -b nome-da-branch` |
| Ver alterações ainda não preparadas             | `git diff` |
| Descartar alterações num ficheiro               | `git restore ficheiro.txt` |
| Obter novos ficheiros do respositório do docente | `git fetch upstream && git merge upstream/master` |

---

## 6. Problemas frequentes

<details>
<summary><strong>Fiz commits na branch <code>master</code> por engano</strong></summary>

Move o trabalho para uma branch nova (ainda não foi feito *push*):

```bash
git checkout -b tarefa-1     # leva os commits para a nova branch
git checkout master
git reset --hard upstream/master   # limpa a master (perde alterações não commitadas!)
```
</details>

<details>
<summary><strong>Aparecem conflitos no <code>git merge upstream/master</code></strong></summary>

O Git indica os ficheiros em conflito. Abre cada um, procura os marcadores `<<<<<<<`, `=======` e `>>>>>>>`, escolhe a versão correta, apaga os marcadores e depois:

```bash
git add ficheiro-resolvido
git commit
```

Em caso de dúvida, `git merge --abort` cancela o merge e devolve tudo ao estado anterior.
</details>

<details>
<summary><strong><code>git branch -d</code> recusa apagar a branch</strong></summary>

Acontece quando o Git ainda não vê a branch como integrada localmente (o *merge* foi feito no GitHub). Depois de confirmares que o PR foi aceite, força a eliminação:

```bash
git branch -D tarefa-1
```
</details>

<details>
<summary><strong>O botão "Compare & pull request" não aparece</strong></summary>

Vai ao separador **Pull requests** do repositório do docente → **New pull request** → **compare across forks** e seleciona manualmente a tua branch.
</details>

<details>
<summary><strong>Enviei o PR para o sítio errado</strong></summary>

Fecha o Pull Request e abre um novo, confirmando que **base repository** é `cunhaestgv/SD-2026-2027` e **base** é `master`.
</details>

<details>
<summary><strong>O meu fork está muito desatualizado</strong></summary>

Na página do teu fork no GitHub, clica em **Sync fork** → **Update branch**. Depois, localmente: `git checkout master && git pull origin master`.
</details>

---

## 7. Regras de ouro

1. ✅ Uma **branch por tarefa** — nunca trabalhar diretamente na `master`.
2. ✅ **Atualizar a `master`** (`fetch` + `merge upstream/master`) antes de criar cada branch nova.
3. ✅ **Commits** com mensagens descritivas.
4. ✅ Fazer **push antes da aula** — o PR tem de estar aberto no momento da apresentação.
5. ❌ Nunca fazer `git push --force` para a `master`.
6. ❌ Não submeter binários, executáveis nem pastas de configuração do IDE.
