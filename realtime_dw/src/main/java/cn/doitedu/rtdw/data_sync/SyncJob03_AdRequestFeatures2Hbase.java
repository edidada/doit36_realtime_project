package cn.doitedu.rtdw.data_sync;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SyncJob03_AdRequestFeatures2Hbase {

    public static void main(String[] args) {
        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建kafka映射表，读取特征流数据
        tenv.executeSql(
                " CREATE TABLE ad_request_source (               "
                        // 物理字段
                        + "     ad_tracking_id     string,                      "
                        + "     ad_id              STRING,                         "
                        + "     features     string,                            "
                        + "     requestTime  bigint                             "
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'ad-request-log',                        "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'goo4',                    "
                        + "  'scan.startup.mode' = 'latest-offset',             "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )                                                   ");


        // 创建hbase映射表，写入特征流数据
        tenv.executeSql("CREATE TABLE ad_features_hbase( " +
                " ad_tracking_id STRING, " +
                " f ROW<ad_id STRING,features STRING, requestTime BIGINT>, " +
                " PRIMARY KEY (ad_tracking_id) NOT ENFORCED " +
                ") WITH (                             " +
                " 'connector' = 'hbase-2.2',          " +
                " 'table-name' = 'ad_features',       " +
                " 'zookeeper.quorum' = 'doitedu:2181' " +
                ")");

        // insert语句
        tenv.executeSql("insert into ad_features_hbase  " +
                "select ad_tracking_id,row(ad_id,features,requestTime) as f from ad_request_source");

    }
}
