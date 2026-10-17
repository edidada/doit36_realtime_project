package cn.doitedu.rtmk.utils;

/** Connection settings shared by local examples and deployed jobs. */
public final class RuntimeConfig {
    private RuntimeConfig() {}

    // IDEA direct launches also use the local private profile; explicit properties/env win.
    private static final java.util.Map<String, String> LOCAL_SETTINGS = loadLocalSettings();

    private static java.util.Map<String, String> loadLocalSettings() {
        String configured = System.getProperty("MAIN_ENV_JSON", System.getenv("MAIN_ENV_JSON"));
        java.nio.file.Path path = java.nio.file.Paths.get(configured == null || configured.isBlank()
                ? ".artifacts/main-audit/k3s/runtime-env.json" : configured);
        if (!java.nio.file.Files.exists(path) && (configured == null || configured.isBlank())) {
            return java.util.Collections.emptyMap();
        }
        try {
            com.alibaba.fastjson.JSONObject json = com.alibaba.fastjson.JSON.parseObject(
                    java.nio.file.Files.readString(path));
            java.util.Map<String, String> settings = new java.util.HashMap<>();
            if (json == null) throw new IllegalArgumentException("Expected an object");
            for (String key : json.keySet()) {
                Object value = json.get(key);
                if (!(value instanceof String)) throw new IllegalArgumentException("Expected string values");
                settings.put(key, (String) value);
            }
            return settings;
        } catch (java.io.IOException | RuntimeException e) {
            throw new IllegalStateException("Cannot load private MAIN_ENV_JSON profile: " + path);
        }
    }

    public static String value(String name, String fallback) {
        String value = System.getProperty(name, System.getenv(name));
        if (value == null || value.trim().isEmpty()) value = LOCAL_SETTINGS.get(name);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    public static String sql(String name, String fallback) {
        return value(name, fallback).replace("'", "''");
    }

    public static int port(String name, int fallback) {
        return Integer.parseInt(value(name, Integer.toString(fallback)));
    }

    public static String jdbcUrl(String database) {
        return "jdbc:mysql://" + value("MYSQL_HOST", "doitedu") + ":"
                + port("MYSQL_PORT", 3306) + "/" + database;
    }
    public static redis.clients.jedis.Jedis redis() {
        redis.clients.jedis.Jedis client = new redis.clients.jedis.Jedis(
                value("REDIS_HOST", "doitedu"), port("REDIS_PORT", 6379));
        String password = value("REDIS_PASSWORD", "");
        if (!password.isEmpty()) {
            client.auth(password);
        }
        client.select(port("REDIS_DATABASE", 0));
        return client;
    }
}
