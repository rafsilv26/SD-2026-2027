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
