# E1-UDP01 — Comunicação baseada em Datagramas

## CA1 — Modelo de falhas e desordenação

Os datagramas UDP são independentes. Duas mensagens enviadas em sequência
podem sofrer atrasos diferentes ou seguir caminhos distintos na rede, pelo
que a mensagem 2 pode chegar antes da mensagem 1. O UDP não fornece ao
recetor uma ordem de entrega nem informação sobre a posição de cada
datagrama. Por isso, a aplicação inclui o número de sequência `N` em cada
mensagem: ao comparar `N` com o próximo número esperado, o servidor consegue
detetar uma mensagem fora de ordem.

## CA2 — Decisões da API de datagramas

O servidor cria um `DatagramSocket` no porto fixo `9876`, porque esse é o
ponto conhecido onde fica à espera de pedidos. O cliente cria o socket sem
indicar porto; o sistema atribui-lhe um porto disponível, que identifica
temporariamente aquele cliente para poder receber a resposta.

Se o servidor estiver desligado, o envio UDP não confirma que o datagrama foi
entregue. Assim, o cliente pode enviar sem erro imediato e ficar à espera da
resposta até expirar o seu timeout.

O buffer de receção tem capacidade para 1024 bytes, mas o comprimento válido
depois de uma receção é dado por `getLength()`. Ao converter ou reenviar a
mensagem, deve ser usado esse comprimento, e não o tamanho total do buffer,
para evitar bytes residuais de mensagens anteriores.

O servidor responde para `request.getAddress()` e `request.getPort()`, pois
estes valores identificam o endereço e o porto de origem do datagrama que
acabou de receber.

## CA3 — Protocolo, estado e regra de decisão

Cada pedido usa o formato `<N>,<mensagem>`, pelo que o número de sequência
viaja dentro dos dados do datagrama. O servidor guarda apenas `L`, o número da
última mensagem aceite em ordem. Inicialmente, `L = 0`, o que significa que a
primeira mensagem esperada é a número 1.

Regra de decisão: o servidor aceita e faz echo apenas se `N == L + 1`; nesse
caso atualiza `L` para `N`. Para qualquer outro número responde
`waitingfor,<L+1>` e mantém `L` inalterado. Uma mensagem adiantada não é
guardada: é descartada, sendo responsabilidade do cliente reenviá-la após
enviar a mensagem pedida.

O cliente reconhece um pedido de retransmissão pelo prefixo `waitingfor,` e
apresenta-o de forma diferente de um echo. Uma mensagem sem vírgula ou com um
número de sequência inválido é ignorada pelo servidor, sem alterar `L`; o
servidor continua disponível para receber os datagramas seguintes.

## Teste normal

| Mensagem introduzida | Mensagem enviada | Resposta | L após processamento |
| --- | --- | --- | --- |
| `olá` | `1,olá` | `Echo: 1,olá` | 1 |
| `mundo` | `2,mundo` | `Echo: 2,mundo` | 2 |
| `teste` | `3,teste` | `Echo: 3,teste` | 3 |

Output do cliente:

```text
> olá
Echo: 1,olá
> mundo
Echo: 2,mundo
> teste
Echo: 3,teste
```

Output do servidor:

```text
Recebido de 127.0.0.1:60883 -> 1,olá
Aceite. L = 1
Recebido de 127.0.0.1:60883 -> 2,mundo
Aceite. L = 2
Recebido de 127.0.0.1:60883 -> 3,teste
Aceite. L = 3
```

## Teste de desordenação

| Mensagem enviada | Resposta recebida | L após processamento | Justificação |
| --- | --- | --- | --- |
| `1,olá` | echo `1,olá` | 1 | A mensagem 1 é a esperada quando `L = 0`. |
| `3,mundo` | `waitingfor,2` | 1 | A mensagem 3 está adiantada; a mensagem 2 é a esperada. |
| `2,cruel` | echo `2,cruel` | 2 | A mensagem 2 passa a ser aceite e atualiza `L`. |
| `3,mundo` | echo `3,mundo` | 3 | Depois de aceitar a 2, a mensagem 3 volta a ser a esperada. |

Output do cliente:

```text
> 1,olá
Echo: 1,olá
> 3,mundo
O servidor espera a mensagem 2. O contador foi ajustado.
> 2,cruel
Echo: 2,cruel
> 3,mundo
Echo: 3,mundo
```

Output do servidor:

```text
Recebido de 127.0.0.1:53253 -> 1,olá
Aceite. L = 1
Recebido de 127.0.0.1:53253 -> 3,mundo
Fora de ordem. Esperava 2. L mantém-se em 1
Recebido de 127.0.0.1:53253 -> 2,cruel
Aceite. L = 2
Recebido de 127.0.0.1:53253 -> 3,mundo
Aceite. L = 3
```

O último envio de `3,mundo` demonstra a recuperação: a mensagem 3 tinha sido
rejeitada quando faltava a 2, mas é aceite após a mensagem 2 atualizar `L`.
Em `localhost` a desordenação tem de ser provocada manualmente porque os
datagramas tendem a ter atrasos e trajetos muito semelhantes.

## Reflexão crítica (4.4)

### Duplicados

No teste, a segunda mensagem `1,ola` chegou quando `L = 1`. Como o servidor
esperava a mensagem 2, respondeu `waitingfor,2` e manteve `L = 1`. O
mecanismo identifica que o datagrama não é o próximo esperado, mas apenas com
`L` não consegue provar que é um duplicado: também poderia ser uma mensagem
antiga recebida fora de ordem. Guardar informação adicional sobre sequências
já aceites permitiria classificar mensagens com `N <= L` como mensagens
antigas/duplicadas.

```text
Cliente
> 1, ola
Echo: 1, ola
> 1, ola
O servidor espera a mensagem 2. O contador foi ajustado.

Servidor
Recebido de 127.0.0.1:55228 -> 1, ola
Aceite. L = 1
Recebido de 127.0.0.1:55228 -> 1, ola
Fora de ordem. Esperava 2. L mantém-se em 1
```

### Perdas

Quando chegou `3,mundo` depois de `1,ola`, o servidor concluiu que faltava a
mensagem 2 e respondeu `waitingfor,2`. Contudo, se a mensagem 2 se perder e
nenhum datagrama posterior chegar, o servidor fica à espera e não sabe que
houve perda. O timeout no cliente indica apenas a ausência de resposta; não
permite saber se se perdeu o pedido ou a resposta. Uma solução aplicacional
precisaria de temporizadores, confirmações e retransmissões.

```text
Cliente
> 1, ola
Echo: 1, ola
> 3, mundo
O servidor espera a mensagem 2. O contador foi ajustado.

Servidor
Recebido de 127.0.0.1:49874 -> 1, ola
Aceite. L = 1
Recebido de 127.0.0.1:49874 -> 3, mundo
Fora de ordem. Esperava 2. L mantém-se em 1
```

### Vários clientes

O servidor usa um único valor global de `L`. No teste, o cliente A enviou a
mensagem 1 e fez `L = 1`; quando o cliente B enviou a sua própria primeira
mensagem, também com sequência 1 mas a partir de outro porto, recebeu
`waitingfor,2`. As sequências de ambos os clientes ficaram misturadas. A
solução seria manter um valor de `L` por cliente, usando o par endereço IP e
porto de origem como chave.

```text
Recebido de 127.0.0.1:59223 -> 1,olá
Aceite. L = 1
Recebido de 127.0.0.1:49844 -> 1,teste
Fora de ordem. Esperava 2. L mantém-se em 1
```

### Limite do mecanismo

O mecanismo implementado deteta que uma mensagem recebida não é a próxima
esperada e pede a sequência em falta; não reordena nem guarda mensagens
adiantadas. Corrupção é detetada pelo checksum do UDP, mas a aplicação não
faz recuperação de um datagrama descartado. Duplicados, perdas silenciosas e
vários clientes continuam por resolver.
