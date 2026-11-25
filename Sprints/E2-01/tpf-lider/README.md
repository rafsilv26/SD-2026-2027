# tpf-lider

Base do nó líder para o sistema de arquivo distribuído sobre Discord (Java 17 + JDA).

## Pré-requisitos
- Java 17 ou superior
- Maven 3.8+
- Aplicação criada no [Discord Developer Portal](https://discord.com/developers/applications) com o **Message Content Intent** ativado.

## Configuração
Define as seguintes variáveis de ambiente antes de arrancar o bot (nunca faças commit destes valores):

**Windows (PowerShell):**
```powershell
$env:DISCORD_TOKEN="o_teu_token_aqui"
$env:DISCORD_GUILD_ID="id_do_servidor_discord"
```

**Linux / macOS:**
```bash
export DISCORD_TOKEN="o_teu_token_aqui"
export DISCORD_GUILD_ID="id_do_servidor_discord"
```

## Execução
Dentro da pasta `tpf-lider`, compila e arranca o bot:

```bash
mvn clean compile exec:java -Dexec.mainClass="tpf.LeaderBot"
```

## Sprint E2-02

O líder escreve em `registos.log`, guarda os eventos em `eventos-lider.txt`,
gera `master.key` na primeira execução e cria chunks em `selados/`.

Os diagramas UML do fluxo e das classes estão em:

- `E2-02-Fluxo-Leader.puml`
- `E2-02-Classes.puml`
- `Explicacao-E2-02-Registo-Duravel.pdf` — explicação do fluxo e das decisões de implementação.

### Configuração

Os parâmetros podem ser alterados no arranque:

```bash
mvn test
mvn exec:java -Dtpf.chunkMaxBytes=65536 -Dtpf.padBytes=4096 -Dtpf.selagemMs=5000
```

### Formato de `registos.log`

Cada linha usa `lsn;instante;autor;canal;texto`, onde os três últimos campos
estão em Base64. O `lsn` começa em 1 após a primeira execução e continua a
partir do último valor encontrado no ficheiro.

### Formato binário do chunk

O chunk começa com um inteiro de 32 bits com a quantidade de registos. Cada
registo contém:

1. `lsn`: inteiro longo de 64 bits;
2. `instante`: inteiro longo de 64 bits;
3. `autor`: inteiro de comprimento de 32 bits seguido dos bytes UTF-8;
4. `canal`: inteiro de comprimento de 32 bits seguido dos bytes UTF-8;
5. `texto`: inteiro de comprimento de 32 bits seguido dos bytes UTF-8.

Todos os inteiros são escritos em ordem de bytes *big-endian*. Os bytes finais
são zeros de padding até o tamanho total ser múltiplo de `PAD_BYTES`.

### Formato cifrado

Cada chunk gera um salt de 32 bytes e um nonce de 12 bytes. A DEK é obtida por
HKDF-SHA256 da `master.key`, com salt aleatório e info `tpf-chunk-v1`. O texto
com padding é cifrado por AES-256-GCM, com tag de 128 bits.

O AAD é: ASCII `tpf-v1`, seguido da `keyVersion` como inteiro de 32 bits
*big-endian*, depois do salt e do nonce.

`selados/<chunkId>.bin` contém texto cifrado e tag. `chunkId` é o SHA-256
hexadecimal desses bytes. `selados/<chunkId>.hdr` contém
`keyVersion;saltBase64;nonceBase64`.
