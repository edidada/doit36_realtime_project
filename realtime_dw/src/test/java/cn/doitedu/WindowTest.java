package cn.doitedu;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/ckpt");

        //
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql(
                " CREATE TABLE tmp (               "
                        // 物理字段
                        + "     score        int,                            "
                        + "     eventTime    bigint,                            "
                        + "     rt    AS  to_timestamp_ltz(eventTime,3) ,   "
                        + "     watermark for rt as rt - interval '0' second "
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'window-test',                           "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'goo4',                   "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )                                                   ");

        tenv.executeSql(
                " CREATE TABLE res (               "
                        // 物理字段
                        + "     window_start        TIMESTAMP(3),                        "
                        + "     window_end          TIMESTAMP(3),                        "
                        + "     score               int                         "
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'window-res',                            "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'goo5',                    "
                        + "  'scan.startup.mode' = 'latest-offset',             "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )");

        tenv.executeSql("insert into res\n" +
                "select\n" +
                "window_start,\n" +
                "window_end,\n" +
                "sum(score) as score_amt\n" +
                "from TABLE(\n" +
                " TUMBLE(TABLE tmp,DESCRIPTOR(rt),INTERVAL '5' SECOND)\n" +
                ")\n" +
                "GROUP BY window_start,window_end");
    }
}
