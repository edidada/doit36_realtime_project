package cn.doitedu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/10
 * @Desc: 学大数据，到多易教育
 *   停止job时drain掉所有数据（触发最后窗口）
 *   {"score":10,"eventTime":1678620730000}
 *   {"score":10,"eventTime":1678620731000}
 *   {"score":10,"eventTime":1678620732000}
 *   {"score":10,"eventTime":1678620733000}
 *   {"score":10,"eventTime":1678620734000}
 *   {"score":10,"eventTime":1678620735000}
 *   {"score":10,"eventTime":1678620736000}
 *   {"score":10,"eventTime":1678620737000}
 *   {"score":10,"eventTime":1678620738000}
 *   {"score":10,"eventTime":1678620739000}
 *   {"score":10,"eventTime":1678620740000}
 *
 *
 *   bin/flink run-application -t yarn-application \
 *   -yjm 1024 -ynm sea -yqu default -ys 2 \
 *   -ytm 1024 -p 1 \
 *   -c cn.doitedu.rtdw.WindowTest  /root/rtdw.jar
 *
 *   bin/flink stop -d -p hdfs://doitedu:8020/ckpt 9f8cc8c3f67ffc783b6a79897a86ad26
 **/
public class WindowTest {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(2000);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu:8020/ckpt");

        //
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql(
                " CREATE TABLE tmp (                                     "
                        // 物理字段
                        + "     score        int,                            "
                        + "     eventTime    bigint,                         "
                        + "     rt    AS  to_timestamp_ltz(eventTime,3) ,    "
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
