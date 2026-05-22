public class JavaBootstrapJsonTest {
    public static void main(String[] args) throws Exception {
        assertValid("{}");
        assertValid("[]");
        assertValid("{\"ok\":true,\"n\":-12.34e+5,\"s\":\"a\\\\b\\\"c\",\"unicode\":\"你好\"}");
        assertValid("0");
        assertValid("-0.1");
        assertValid("null");

        assertInvalid("");
        assertInvalid("01");
        assertInvalid("-");
        assertInvalid("1e");
        assertInvalid("1+");
        assertInvalid("{\"ok\":true} trailing");
        assertInvalid("[1,]");
        assertInvalid("{bad:true}");
    }

    private static void assertValid(String value) throws Exception {
        JavaBootstrap.validateJsonForTest(value);
    }

    private static void assertInvalid(String value) throws Exception {
        try {
            JavaBootstrap.validateJsonForTest(value);
        } catch (Exception expected) {
            return;
        }
        throw new AssertionError("expected invalid JSON: " + value);
    }
}
