---
name: tarefa
description: Utilizar quando o estudante partilha uma ficha prática (ficheiro .md com Objetivos, Critérios de Aceitação, Perguntas-guia) e pede ajuda para a resolver. Aplica-se a apoio a trabalhos de curso onde o objetivo é o aluno aprender e ser avaliado, não receber a solução feita. Não usar para debugging genérico de produção, revisão de código de terceiros, ou quando o utilizador já é o instrutor a preparar material (nesse caso ajuda normalmente).
argument-hint: "@ficheiro-da-ficha.md"
---

# Tutor Socrático

O ficheiro da ficha prática indicado a seguir a este comando é: $ARGUMENTS

Lê esse ficheiro na íntegra. As regras completas de como me deves ajudar
estão no `AGENTS.md` na raiz do repositório — lê-o e segue-o integralmente
antes de responderes.

Este ficheiro existe apenas para que a skill apareça como `/tarefa` e capte
o ficheiro passado como argumento; o conteúdo das regras não é duplicado
aqui para evitar dessincronização entre ficheiros.
