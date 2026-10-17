package cn.doitedu.rtdw.data_etl;

import cn.doitedu.rtdw.utils.RuntimeConfig;
import cn.doitedu.rtdw.utils.CommonSql;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 广告请求实时特征工程
 **/
public class EtlJob07_AdRequestSessionProcess {
    public static void main(String[] args) {
        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建kafka行为日志明细宽表
        tenv.executeSql(CommonSql.KAFKA_WIDE_LOG_DDL);

        // 对行为数据进行过滤：只留下广告事件
        tenv.executeSql("create temporary view events AS " +
                "select user_id,event_id,properties['creative_id'] as creative_id," +
                "event_time,properties['ad_tracking_id'] as ad_tracking_id,rt,pt " +
                " from mall_events_wide_source where  event_id in ('ad_show','ad_click')");

        //tenv.executeSql("select * from events").print();


        // 利用 CEP 来对曝光和点击进行关联
        tenv.executeSql(
                " CREATE TEMPORARY VIEW show_click AS         "
                        + " SELECT                                     "
                        + "    T.user_id,                          "
                        + "    T.tracking_id,                     "
                        + "    T.creative_id,                      "
                        + "    T.show_time,                        "
                        + "    T.click_time,                       "
                        + "    T.proctime                        "
                        + " FROM events                          "
                        + "   MATCH_RECOGNIZE(                   "
                        + "     PARTITION BY ad_tracking_id       "
                        + " 	   ORDER BY rt                         "
                        + " 	   MEASURES                            "
                        + " 	      A.user_id  as  user_id ,         "
                        + " 	      A.pt  as  proctime ,         "
                        + " 	      A.creative_id    as  creative_id,     "
                        + " 	      A.event_time as show_time,       "
                        + " 	      A.ad_tracking_id as tracking_id ,       "
                        + " 	      B.event_time as click_time       "
                        + "        ONE ROW PER MATCH                "
                        + "        AFTER MATCH SKIP TO NEXT ROW	    "
                        + " 	   PATTERN(A B)                         "
                        + " 	   DEFINE                              "
                        + " 	      A AS A.event_id = 'ad_show',     "
                        + " 	      B AS B.event_id = 'ad_click'    ) AS T"
        );

        // 将流内关联好的数据，再次去关联hbase中的特征流数据  look  up  join
        // 创建hbase映射表，读取特征流数据
        tenv.executeSql("CREATE TABLE ad_features_hbase( " +
                " ad_tracking_id STRING, " +
                " f ROW<ad_id STRING,features STRING, requestTime BIGINT>, " +
                " PRIMARY KEY (ad_tracking_id) NOT ENFORCED " +
                ") WITH (                             " +
                " 'connector' = 'hbase-2.2',          " +
                " 'table-name' = 'ad_features',       " +
                " 'zookeeper.quorum' = '" + RuntimeConfig.sql("HBASE_ZOOKEEPER_QUORUM", "doitedu:2181") + "' " +
                ")");

        // 创建kafka映射表，用于写入最终结果数据
        tenv.executeSql(
                "create table show_click_features ( " +
                        "user_id BIGINT," +
                        "tracking_id STRING," +
                        "creative_id STRING," +
                        "show_time BIGINT," +
                        "click_time BIGINT," +
                        "ad_id STRING," +
                        "features STRING," +
                        "request_time BIGINT"
                        + " ) WITH (                                             "
                        + "  'connector' = 'kafka',                              "
                        + "  'topic' = 'show-click-features',                     "
                        + "  'properties.bootstrap.servers' = '" + RuntimeConfig.sql("KAFKA_BOOTSTRAP_SERVERS", "doitedu:9092") + "',    "
                        + "  'properties.group.id' = 'testGroup',                "
                        + "  'scan.startup.mode' = 'latest-offset',            "
                        + "  'value.format'='json',                              "
                        + "  'value.json.fail-on-missing-field'='false',         "
                        + "  'value.fields-include' = 'EXCEPT_KEY')              "
        );


        tenv.executeSql(
                " INSERT INTO show_click_features                                       "
                +" SELECT                                                                   "
                        + "   sc.user_id,                                                  "
                        + "   sc.tracking_id,                                              "
                        + "   sc.creative_id,                                              "
                        + "   sc.show_time,                                                "
                        + "   sc.click_time,                                               "
                        + "   ft.f.ad_id as ad_id,                                         "
                        + "   ft.f.features as features,                                   "
                        + "   ft.f.requestTime as request_time                             "
                        + " FROM                                                           "
                        + " show_click  sc                                                 "
                        + " LEFT JOIN ad_features_hbase FOR SYSTEM_TIME AS OF sc.proctime  AS ft "
                        + " ON sc.tracking_id = ft.ad_tracking_id                          "

        ).print();


    }

}
