package cn.doitedu.rtdw.deployment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import cn.doitedu.rtdw.utils.RuntimeConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Infrastructure validation stream; does not replace the business ETL. */
public final class StandaloneEventsJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        tables.getConfig().getConfiguration().setString("pipeline.name", "standalone-events-validation");
        tables.executeSql("CREATE TABLE events_source (event_id BIGINT) WITH ("
                + "'connector'='datagen', 'rows-per-second'='2',"
                + "'fields.event_id.min'='1', 'fields.event_id.max'='1000000')");
        tables.executeSql("CREATE TABLE events_output (event_id BIGINT, event_type STRING,"
                + " channel STRING, processed_at TIMESTAMP_LTZ(3)) WITH ("
                + "'connector'='filesystem', 'path'='" + RuntimeConfig.sql("STANDALONE_OUTPUT_URI", "file:///flink-data/standalone-events") + "',"
                + "'format'='json', 'sink.rolling-policy.rollover-interval'='10 s',"
                + "'sink.rolling-policy.check-interval'='5 s')");
        tables.executeSql("INSERT INTO events_output SELECT event_id, 'page_view',"
                + " UPPER('web'), CURRENT_TIMESTAMP FROM events_source");
    }
}
