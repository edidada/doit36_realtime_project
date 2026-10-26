package cn.doitedu.rtdw.data_etl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EtlJobConfigurationTest {
    @Test void buildsBothSinksOfflineAndPreservesOverriddenSqlValues() throws Exception {
        Map<String, String> settings = Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "test-kafka:9092",
                "KAFKA_INPUT_TOPIC", "isolated-input",
                "KAFKA_OUTPUT_TOPIC", "isolated-output",
                "KAFKA_GROUP_ID", "test-group",
                "HBASE_ZOOKEEPER_QUORUM", "test-hbase:2181",
                "DORIS_FENODES", "test-doris:8030",
                "DORIS_USERNAME", "test-user",
                "DORIS_PASSWORD", "quoted'password");
        Map<String, String> previous = new HashMap<>();
        settings.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));
        try {
            var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
            assertNotNull(EtlJob01_UserEventsLogCommonDim.createStatements(tables));
            var source = options(tables, "mall_events_kafka_source");
            assertEquals("isolated-input", source.get("topic"));
            assertEquals("test-kafka:9092", source.get("properties.bootstrap.servers"));
            assertEquals("test-group", source.get("properties.group.id"));
            var kafka = options(tables, "mall_events_wide_kafkasink");
            assertEquals("isolated-output", kafka.get("topic"));
            assertEquals("test-kafka:9092", kafka.get("properties.bootstrap.servers"));
            var doris = options(tables, "mall_events_wide_doris_sink");
            assertEquals("test-doris:8030", doris.get("fenodes"));
            assertEquals("test-user", doris.get("username"));
            assertEquals("quoted'password", doris.get("password"));
            for (String dimension : new String[]{"ums_member_hbase_source", "dim_geo_area_hbase_source",
                    "dim_page_info_hbase_source"}) {
                assertEquals("test-hbase:2181", options(tables, dimension).get("zookeeper.quorum"));
            }
        } finally {
            previous.forEach((key, value) -> {
                if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); }
            });
        }
    }

    private static Map<String, String> options(StreamTableEnvironment tables, String table) throws Exception {
        return tables.getCatalog(tables.getCurrentCatalog()).orElseThrow()
                .getTable(new ObjectPath(tables.getCurrentDatabase(), table)).getOptions();
    }
}
