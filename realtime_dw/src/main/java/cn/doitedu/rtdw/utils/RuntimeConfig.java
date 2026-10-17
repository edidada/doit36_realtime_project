package cn.doitedu.rtdw.utils;

/** Connection settings shared by local examples and deployed jobs. */
public final class RuntimeConfig {
    private RuntimeConfig() {}

    public static String value(String name, String fallback) {
        String value = System.getProperty(name, System.getenv(name));
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
