import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class JavaBootstrap {
    private static final String RUNTIME_API = envOr("RUNTIME_API", "http://localhost:9000");
    private static final String HANDLER = envOr("FUNCTION_HANDLER", "Hello::handleRequest");
    private static final String FUNCTION_DIR = envOr("FUNCTION_DIR", "/function");

    public static void main(String[] args) throws Exception {
        HandlerMethod handler = loadHandler();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                handler.loader.close();
            } catch (IOException ignored) {
            }
        }));
        while (true) {
            Invocation invocation = nextInvocation();
            if (invocation == null) {
                continue;
            }

            try {
                String result = (String) handler.method.invoke(null, invocation.body);
                requireJson(result);
                post("/runtime/invocation/" + invocation.requestId + "/response", result);
            } catch (InvocationTargetException err) {
                post("/runtime/invocation/" + invocation.requestId + "/error", errorJson(err.getTargetException()));
            } catch (Throwable err) {
                post("/runtime/invocation/" + invocation.requestId + "/error", errorJson(err));
            }
        }
    }

    private static HandlerMethod loadHandler() throws Exception {
        String[] parts = HANDLER.split("::", 2);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException("invalid FUNCTION_HANDLER " + HANDLER);
        }

        URLClassLoader loader = new URLClassLoader(new URL[]{Path.of(FUNCTION_DIR).toUri().toURL()});
        Class<?> cls = Class.forName(parts[0], true, loader);
        Method method = cls.getMethod(parts[1], String.class);
        if (!java.lang.reflect.Modifier.isStatic(method.getModifiers()) || method.getReturnType() != String.class) {
            throw new IllegalArgumentException("handler must be public static String " + parts[1] + "(String eventJson)");
        }
        return new HandlerMethod(method, loader);
    }

    private static Invocation nextInvocation() throws IOException {
        HttpURLConnection conn = open("GET", "/runtime/invocation/next");
        int status = conn.getResponseCode();
        if (status == 204) {
            return null;
        }
        if (status != 200) {
            throw new IOException("next invocation failed with " + status);
        }
        String requestId = conn.getHeaderField("Lambda-Runtime-Aws-Request-Id");
        String body = readAll(conn.getInputStream());
        return new Invocation(requestId, body);
    }

    private static void post(String path, String body) throws IOException {
        HttpURLConnection conn = open("POST", path);
        try {
            byte[] data = body.getBytes(StandardCharsets.UTF_8);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setFixedLengthStreamingMode(data.length);
            conn.setDoOutput(true);
            try (OutputStream out = conn.getOutputStream()) {
                out.write(data);
            }
            int status = conn.getResponseCode();
            InputStream stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
            if (stream != null) {
                readAll(stream);
            }
            if (status < 200 || status >= 300) {
                throw new IOException("post " + path + " failed with " + status);
            }
        } finally {
            conn.disconnect();
        }
    }

    private static HttpURLConnection open(String method, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(RUNTIME_API + path).toURL().openConnection();
        conn.setRequestMethod(method);
        return conn;
    }

    private static String readAll(InputStream in) throws IOException {
        try (InputStream input = in) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static void validateJsonForTest(String value) throws IOException {
        requireJson(value);
    }

    private static void requireJson(String value) throws IOException {
        if (value == null || value.isBlank()) {
            throw new IOException("handler returned empty JSON");
        }
        JsonScanner scanner = new JsonScanner(value);
        scanner.readValue();
        scanner.skipWhitespace();
        if (!scanner.isDone()) {
            throw new IOException("handler returned invalid JSON");
        }
    }

    private static String errorJson(Throwable err) {
        String type = err == null ? "Error" : err.getClass().getSimpleName();
        String message = err == null || err.getMessage() == null ? "" : err.getMessage();
        return "{\"errorType\":" + quote(type) + ",\"errorMessage\":" + quote(message) + "}";
    }

    private static String quote(String value) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"': sb.append("\\\""); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }

    private static String envOr(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static final class JsonScanner {
        private final String value;
        private int pos;

        JsonScanner(String value) {
            this.value = value;
        }

        boolean isDone() {
            return pos == value.length();
        }

        void skipWhitespace() {
            while (!isDone() && Character.isWhitespace(value.charAt(pos))) {
                pos++;
            }
        }

        void readValue() throws IOException {
            skipWhitespace();
            if (isDone()) {
                throw invalidJson();
            }
            char c = value.charAt(pos++);
            switch (c) {
                case '{': readComposite('{', '}'); return;
                case '[': readComposite('[', ']'); return;
                case '"': readString(); return;
                case 't': readLiteral("rue"); return;
                case 'f': readLiteral("alse"); return;
                case 'n': readLiteral("ull"); return;
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        readNumber(c);
                        return;
                    }
                    throw invalidJson();
            }
        }

        private void readComposite(char open, char close) throws IOException {
            skipWhitespace();
            if (!isDone() && value.charAt(pos) == close) {
                pos++;
                return;
            }
            while (true) {
                if (open == '{') {
                    skipWhitespace();
                    if (isDone() || value.charAt(pos++) != '"') {
                        throw invalidJson();
                    }
                    readString();
                    skipWhitespace();
                    if (isDone() || value.charAt(pos++) != ':') {
                        throw invalidJson();
                    }
                }
                readValue();
                skipWhitespace();
                if (!isDone() && value.charAt(pos) == ',') {
                    pos++;
                    continue;
                }
                if (!isDone() && value.charAt(pos) == close) {
                    pos++;
                    return;
                }
                throw invalidJson();
            }
        }

        private void readString() throws IOException {
            while (!isDone()) {
                char c = value.charAt(pos++);
                if (c == '"') {
                    return;
                }
                if (c == '\\') {
                    if (isDone()) {
                        throw invalidJson();
                    }
                    char escaped = value.charAt(pos++);
                    if (escaped == 'u') {
                        readHexDigits();
                    } else if ("\\\"/bfnrt".indexOf(escaped) < 0) {
                        throw invalidJson();
                    }
                } else if (c < 0x20) {
                    throw invalidJson();
                }
            }
            throw invalidJson();
        }

        private void readHexDigits() throws IOException {
            for (int i = 0; i < 4; i++) {
                if (isDone()) {
                    throw invalidJson();
                }
                char c = value.charAt(pos++);
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                    throw invalidJson();
                }
            }
        }

        private void readLiteral(String suffix) throws IOException {
            for (int i = 0; i < suffix.length(); i++) {
                if (isDone() || value.charAt(pos++) != suffix.charAt(i)) {
                    throw invalidJson();
                }
            }
        }

        private void readNumber(int first) throws IOException {
            if (first == '-') {
                if (isDone()) {
                    throw invalidJson();
                }
                first = value.charAt(pos++);
            }

            if (first == '0') {
                if (!isDone() && value.charAt(pos) >= '0' && value.charAt(pos) <= '9') {
                    throw invalidJson();
                }
            } else if (first >= '1' && first <= '9') {
                readDigits();
            } else {
                throw invalidJson();
            }

            if (!isDone() && value.charAt(pos) == '.') {
                pos++;
                if (isDone() || value.charAt(pos) < '0' || value.charAt(pos) > '9') {
                    throw invalidJson();
                }
                readDigits();
            }

            if (!isDone() && (value.charAt(pos) == 'e' || value.charAt(pos) == 'E')) {
                pos++;
                if (!isDone() && (value.charAt(pos) == '+' || value.charAt(pos) == '-')) {
                    pos++;
                }
                if (isDone() || value.charAt(pos) < '0' || value.charAt(pos) > '9') {
                    throw invalidJson();
                }
                readDigits();
            }
        }

        private void readDigits() {
            while (!isDone() && value.charAt(pos) >= '0' && value.charAt(pos) <= '9') {
                pos++;
            }
        }

        private IOException invalidJson() {
            return new IOException("handler returned invalid JSON");
        }
    }

    private record HandlerMethod(Method method, URLClassLoader loader) {}

    private record Invocation(String requestId, String body) {}
}
