package cn.doitedu.fin.risk.fraud;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Embedded JDBC transactions. A DB row serialises commands across service instances sharing this DB. */
public final class Database {
    public static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private final String url;

    public Database(String url) {
        this.url = url;
        try (Connection connection = open(); var statement = connection.createStatement()) {
            try (var schema = Database.class.getResourceAsStream("/fraud-schema.sql")) {
                if (schema == null) { throw new IllegalStateException("Missing schema"); }
                for (String sql : new String(schema.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                    if (!sql.isBlank()) { statement.execute(sql); }
                }
            }
        } catch (Exception failure) { throw new IllegalStateException("Cannot initialise risk database", failure); }
    }

    private Connection open() throws SQLException { return DriverManager.getConnection(url, "sa", ""); }

    @FunctionalInterface public interface Work<T> { T run(Connection connection) throws Exception; }

    public <T> T transaction(Work<T> work) {
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            try {
                try (var lock = connection.prepareStatement("SELECT id FROM command_lock WHERE id=1 FOR UPDATE")) {
                    try (var ignored = lock.executeQuery()) { if (!ignored.next()) { throw new SQLException("Missing command lock"); } }
                }
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (Exception failure) {
                connection.rollback();
                if (failure instanceof RuntimeException runtime) { throw runtime; }
                throw new IllegalStateException("Database transaction failed", failure);
            }
        } catch (SQLException failure) { throw new IllegalStateException("Risk store unavailable; no approval issued", failure); }
    }

    public static int update(Connection c, String sql, Object... values) throws SQLException {
        try (var statement = prepare(c, sql, values)) { return statement.executeUpdate(); }
    }

    public static List<Map<String, Object>> rows(Connection c, String sql, Object... values) throws SQLException {
        try (var statement = prepare(c, sql, values); var result = statement.executeQuery()) {
            List<Map<String, Object>> rows = new ArrayList<>();
            while (result.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int index = 1; index <= result.getMetaData().getColumnCount(); index++) {
                    Object value = result.getObject(index);
                    if (value instanceof Clob clob) { value = clob.getSubString(1, Math.toIntExact(clob.length())); }
                    row.put(result.getMetaData().getColumnLabel(index).toLowerCase(Locale.ROOT), value);
                }
                rows.add(row);
            }
            return rows;
        }
    }

    public static Map<String, Object> one(Connection c, String sql, Object... values) throws SQLException {
        var results = rows(c, sql, values);
        if (results.isEmpty()) { throw new Contracts.ApiException(404, "NOT_FOUND", "Object not found"); }
        return results.get(0);
    }

    private static PreparedStatement prepare(Connection c, String sql, Object... values) throws SQLException {
        var statement = c.prepareStatement(sql);
        for (int index = 0; index < values.length; index++) { statement.setObject(index + 1, values[index]); }
        return statement;
    }

    public static String text(Map<String,Object> row, String key) { return (String) row.get(key); }
    public static long number(Map<String,Object> row, String key) { return ((Number) row.get(key)).longValue(); }
    public static boolean flag(Map<String,Object> row, String key) { return Boolean.TRUE.equals(row.get(key)); }
    public static String json(Object value) {
        try { return JSON.writeValueAsString(value); }
        catch (Exception failure) { throw new IllegalStateException("Cannot encode result", failure); }
    }
    public static <T> T decode(String value, Class<T> type) {
        try { return JSON.readValue(value, type); }
        catch (Exception failure) { throw new IllegalStateException("Invalid stored result", failure); }
    }
    public static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (Exception impossible) { throw new IllegalStateException(impossible); }
    }
}
