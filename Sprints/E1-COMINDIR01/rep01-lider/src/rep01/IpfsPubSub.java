package rep01;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;
import java.util.function.Consumer;

/**
 * Cliente PubSub mínimo para Kubo, implementado com a biblioteca-padrão Java.
 * Esta classe comunica por HTTP com o daemon local, não diretamente com os
 * outros processos Java.
 */
public class IpfsPubSub {
    private final String api;
    private final HttpClient http = HttpClient.newHttpClient();

    public IpfsPubSub(String api) {
        this.api = api;
    }

    /** Codifica UTF-8 na forma multibase base64url exigida pelo Kubo. */
    static String toMultibase(String value) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Descodifica a forma multibase base64url devolvida pelo Kubo. */
    static String fromMultibase(String value) {
        if (!value.startsWith("u")) {
            throw new IllegalArgumentException("Codificação multibase não suportada: " + value);
        }
        byte[] decoded = Base64.getUrlDecoder().decode(value.substring(1));
        return new String(decoded, StandardCharsets.UTF_8);
    }

    /** Publica texto; HTTP 200 confirma aceitação pelo daemon local, não pelas réplicas. */
    public void publish(String topic, String message) throws IOException, InterruptedException {
        String boundary = "rep01-" + System.nanoTime();
        String body = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"msg\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n"
                + message + "\r\n"
                + "--" + boundary + "--\r\n";
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(api + "/pubsub/pub?arg=" + toMultibase(topic)))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("pub falhou (" + response.statusCode() + "): "
                    + response.body());
        }
    }

    /**
     * Subscreve um tópico. A resposta é um fluxo contínuo de linhas JSON;
     * consumir o corpo bloqueia enquanto aguarda a publicação seguinte.
     */
    public void subscribe(String topic, Consumer<String> onMessage)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(api + "/pubsub/sub?arg=" + toMultibase(topic)))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Stream<String>> response =
                http.send(request, HttpResponse.BodyHandlers.ofLines());
        if (response.statusCode() != 200) {
            response.body().close();
            throw new IOException("sub falhou (" + response.statusCode() + ")");
        }

        try (Stream<String> lines = response.body()) {
            lines.filter(json -> json.contains("\"data\""))
                    .forEach(json -> onMessage.accept(
                            fromMultibase(extractField(json, "data"))));
        }
    }

    /**
     * Extrai um campo textual JSON sem biblioteca externa. Trata espaços e
     * escapes JSON comuns; não pretende ser um analisador JSON genérico.
     */
    static String extractField(String json, String field) {
        String key = "\"" + field + "\"";
        int position = json.indexOf(key);
        if (position < 0) {
            throw new IllegalArgumentException("Campo '" + field + "' ausente: " + json);
        }
        position += key.length();

        while (position < json.length() && Character.isWhitespace(json.charAt(position))) {
            position++;
        }
        if (position >= json.length() || json.charAt(position++) != ':') {
            throw new IllegalArgumentException("Campo '" + field + "' inválido: " + json);
        }
        while (position < json.length() && Character.isWhitespace(json.charAt(position))) {
            position++;
        }
        if (position >= json.length() || json.charAt(position++) != '"') {
            throw new IllegalArgumentException("Campo '" + field + "' não é texto: " + json);
        }

        StringBuilder value = new StringBuilder();
        while (position < json.length()) {
            char current = json.charAt(position++);
            if (current == '"') {
                return value.toString();
            }
            if (current != '\\') {
                value.append(current);
                continue;
            }
            if (position >= json.length()) {
                break;
            }

            char escaped = json.charAt(position++);
            switch (escaped) {
                case '"':
                case '\\':
                case '/':
                    value.append(escaped);
                    break;
                case 'b':
                    value.append('\b');
                    break;
                case 'f':
                    value.append('\f');
                    break;
                case 'n':
                    value.append('\n');
                    break;
                case 'r':
                    value.append('\r');
                    break;
                case 't':
                    value.append('\t');
                    break;
                case 'u':
                    if (position + 4 > json.length()) {
                        throw new IllegalArgumentException("Escape Unicode inválido: " + json);
                    }
                    try {
                        value.append((char) Integer.parseInt(
                                json.substring(position, position + 4), 16));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Escape Unicode inválido: " + json, e);
                    }
                    position += 4;
                    break;
                default:
                    throw new IllegalArgumentException("Escape JSON inválido: \\" + escaped);
            }
        }
        throw new IllegalArgumentException("String JSON não terminada: " + json);
    }
}
