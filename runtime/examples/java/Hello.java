public class Hello {
    public static String handleRequest(String eventJson) {
        String name = "world";
        String marker = "\"name\"";
        int key = eventJson.indexOf(marker);
        if (key >= 0) {
            int colon = eventJson.indexOf(':', key + marker.length());
            int start = eventJson.indexOf('"', colon + 1);
            int end = eventJson.indexOf('"', start + 1);
            if (colon >= 0 && start >= 0 && end > start) {
                name = eventJson.substring(start + 1, end);
            }
        }
        return "{\"statusCode\":200,\"message\":\"Hello, " + escape(name) + "!\"}";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
