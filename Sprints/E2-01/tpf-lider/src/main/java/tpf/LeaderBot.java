package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.util.List;

public class LeaderBot extends ListenerAdapter {

    private static final String ADMIN_ROLE_NAME = "admin-arquivo";
    private static final String ARCHIVE_CHANNEL_NAME = "arquivo";
    private final LeaderStore store;

    public LeaderBot(LeaderStore store) {
        this.store = store;
    }

    public static void main(String[] args) throws InterruptedException {
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("DISCORD_GUILD_ID");

        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: Define as variáveis de ambiente DISCORD_TOKEN e DISCORD_GUILD_ID.");
            System.exit(1);
        }

        LeaderStore store;
        try {
            Wire.validar();
            store = new LeaderStore();
        } catch (Exception e) {
            System.err.println("ERRO ao iniciar armazenamento do líder: " + e.getMessage());
            System.exit(1);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                store.close();
            } catch (Exception ignored) {
                // O processo já está a terminar.
            }
        }));

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot(store))
                .build();

        jda.awaitReady();

        Guild guild = jda.getGuildById(guildId);
        if (guild == null) {
            System.err.println("ERRO: Servidor com ID " + guildId + " não encontrado.");
            System.exit(1);
        }

        registarComandos(guild);
        System.out.println("LeaderBot online e comandos registados no servidor: " + guild.getName());
    }

    private static void registarComandos(Guild guild) {
        guild.updateCommands().addCommands(
                Commands.slash("registar", "Regista uma entrada no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Mostra o estado atual do sistema"),

                Commands.slash("config", "Configurações do sistema")
                        .addSubcommands(
                                new SubcommandData("r", "Define o fator de replicação")
                                        .addOption(OptionType.STRING, "n", "Número de réplicas", true)
                        ),

                Commands.slash("recuperar", "Recupera registos num intervalo")
                        .addOption(OptionType.INTEGER, "de", "Índice inicial", true)
                        .addOption(OptionType.INTEGER, "ate", "Índice final", true),

                Commands.slash("replica-token", "Gera um token para registar uma nova réplica"),

                Commands.slash("replica-remover", "Remove uma réplica existente")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),

                Commands.slash("rajada", "Envia uma rajada de mensagens de teste")
                        .addOption(OptionType.INTEGER, "n", "Número de mensagens", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo entre mensagens em ms", true)
        ).queue();
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String comando = event.getName();

        if (List.of("config", "replica-token", "replica-remover", "rajada").contains(comando)) {
            if (!temPapelAdmin(event.getMember())) {
                event.reply("Acesso negado: precisas do papel `" + ADMIN_ROLE_NAME + "` para executar `/" + comando + "`.")
                        .setEphemeral(true)
                        .queue();
                return;
            }
        }

        switch (comando) {
            case "registar" -> {
                String texto = event.getOption("texto", "", OptionMapping::getAsString).trim();
                if (texto.isEmpty()) {
                    event.reply("Erro: o parâmetro `texto` não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                try {
                    long lsn = store.registar(
                            event.getUser().getName(), "arquivo", texto);
                    event.reply("Registo criado com LSN " + lsn).queue();
                } catch (Exception e) {
                    event.reply("Erro ao guardar o registo: " + e.getMessage())
                            .setEphemeral(true).queue();
                }
            }

            case "estado" -> {
                event.reply("/estado recebido — ainda não implementado").queue();
            }

            case "config" -> {
                if ("r".equals(event.getSubcommandName())) {
                    String nRaw = event.getOption("n", "", OptionMapping::getAsString);
                    try {
                        int n = Integer.parseInt(nRaw);
                        if (n < 1) {
                            event.reply("Erro: `n` deve ser um número inteiro maior ou igual a 1.").setEphemeral(true).queue();
                            return;
                        }
                        event.reply("/config r recebido: n=" + n + " — ainda não implementado").queue();
                    } catch (NumberFormatException e) {
                        event.reply("Erro: o argumento `n` (\"" + nRaw + "\") deve ser um número inteiro válido.").setEphemeral(true).queue();
                    }
                }
            }

            case "recuperar" -> {
                long de = event.getOption("de", 0L, OptionMapping::getAsLong);
                long ate = event.getOption("ate", 0L, OptionMapping::getAsLong);

                if (de < 0 || ate < 0) {
                    event.reply("Erro: os índices `de` e `ate` não podem ser negativos.").setEphemeral(true).queue();
                    return;
                }
                if (de > ate) {
                    event.reply("Erro: `de` (" + de + ") não pode ser maior do que `ate` (" + ate + ").").setEphemeral(true).queue();
                    return;
                }
                event.reply("/recuperar recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
            }

            case "replica-token" -> {
                event.reply("/replica-token recebido — ainda não implementado").queue();
            }

            case "replica-remover" -> {
                String id = event.getOption("id", "", OptionMapping::getAsString).trim();
                if (id.isEmpty()) {
                    event.reply("Erro: o `id` da réplica não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                event.reply("/replica-remover recebido: id=" + id + " — ainda não implementado").queue();
            }

            case "rajada" -> {
                long n = event.getOption("n", 0L, OptionMapping::getAsLong);
                long intervaloMs = event.getOption("intervaloms", -1L, OptionMapping::getAsLong);

                if (n <= 0) {
                    event.reply("Erro: `n` deve ser maior que 0.").setEphemeral(true).queue();
                    return;
                }
                if (intervaloMs < 0) {
                    event.reply("Erro: `intervaloMs` deve ser maior ou igual a 0.").setEphemeral(true).queue();
                    return;
                }
                try {
                    for (int i = 0; i < n; i++) {
                        store.registar("rajada", "arquivo",
                                "rajada-" + i);
                        if (intervaloMs > 0 && i + 1 < n) {
                            Thread.sleep(intervaloMs);
                        }
                    }
                    event.reply("Rajada concluída: " + n + " registos").queue();
                } catch (Exception e) {
                    event.reply("Erro ao executar a rajada: " + e.getMessage())
                            .setEphemeral(true).queue();
                }
            }

            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) {
            return;
        }

        if (event.isFromGuild() && ARCHIVE_CHANNEL_NAME.equals(event.getChannel().getName())) {
            String conteudo = event.getMessage().getContentDisplay();
            try {
                long lsn = store.registar(
                        event.getAuthor().getName(), "arquivo", conteudo);
                event.getChannel()
                        .sendMessage("Registo criado com LSN " + lsn)
                        .queue();
            } catch (Exception e) {
                event.getChannel()
                        .sendMessage("Erro ao guardar o registo: " + e.getMessage())
                        .queue();
            }
        }
    }

    private boolean temPapelAdmin(Member member) {
        if (member == null) return false;
        for (Role role : member.getRoles()) {
            if (ADMIN_ROLE_NAME.equals(role.getName())) {
                return true;
            }
        }
        return false;
    }
}
