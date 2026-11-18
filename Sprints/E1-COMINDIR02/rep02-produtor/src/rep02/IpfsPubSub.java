package rep02;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class IpfsPubSub {
    private static final Pattern DATA = Pattern.compile("\"data\"\\s*:\\s*\"([^\"]+)\"");
    private final String api;
    private final HttpClient http = HttpClient.newHttpClient();

    public IpfsPubSub(String api) {
        this.api = api;
    }

    private static String toMultibase(String value) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String fromMultibase(String value) {
        if (!value.startsWith("u")) {
            throw new IllegalArgumentException("Multibase não suportada: " + value);
        }
        return new String(Base64.getUrlDecoder().decode(value.substring(1)),
                StandardCharsets.UTF_8);
    }

    public void publish(String topic, String message) throws IOException, InterruptedException {
        String boundary = "rep02-" + System.nanoTime();
        String body = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"msg\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n"
                + message + "\r\n--" + boundary + "--\r\n";
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(api + "/pubsub/pub?arg=" + toMultibase(topic)))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response =
                http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("pub falhou (" + response.statusCode() + "): "
                    + response.body());
        }
    }

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
            lines.forEach(json -> {
                Matcher matcher = DATA.matcher(json);
                if (matcher.find()) {
                    onMessage.accept(fromMultibase(matcher.group(1)));
                }
            });
        }
    }
}
