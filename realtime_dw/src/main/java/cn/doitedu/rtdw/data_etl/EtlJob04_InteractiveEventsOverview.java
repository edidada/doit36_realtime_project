package cn.doitedu.rtdw.data_etl;

import cn.doitedu.rtdw.utils.RuntimeConfig;
import cn.doitedu.rtdw.utils.CommonSql;
import cn.doitedu.rtdw.utils.TimeRound;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>
 * 交互事件 多维概况统计
 **/
public class EtlJob04_InteractiveEventsOverview {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setString("state.backend.rocksdb.localdir", System.getProperty("java.io.tmpdir") + "/doit36-rocksdb");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));
        env.getCheckpointConfig().setCheckpointTimeout(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置backend类型为rockDbStateBackend，并开启增量checkpoint
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建kafka连接器表，映射 kafka中的dwd层的事件明细topic
        tenv.executeSql(CommonSql.KAFKA_WIDE_LOG_DDL);

        // 创建doris连接器表，映射doris中的dws层 互动事件  概况分析表 dws.mall_evts_overview_agg
        tenv.executeSql(
                " create table mall_evts_overview_agg(    "
                        + "    dt  DATE,                           "
                        + "    time_60m          STRING,           "
                        + "    time_30m          STRING,           "
                        + "    time_10m          STRING,           "
                        + "    page_service      STRING,           "
                        + "    page_type         STRING,           "
                        + "    release_channel   STRING,           "
                        + "    device_type       STRING,           "
                        + "    event_id          STRING,           "
                        + "    user_id           INT,              "
                        + "    act_count         BIGINT            "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = '" + RuntimeConfig.sql("DORIS_FENODES", "doitedu:8030") + "',         "
                        + "    'table.identifier' = 'dws.mall_evts_overview_agg',  "
                        + "    'username' = '" + RuntimeConfig.sql("DORIS_USERNAME", "root") + "' ,                "
                        + "    'password' = '" + RuntimeConfig.sql("DORIS_PASSWORD", "") + "' ,                    "
                        + "    'sink.label-prefix' = 'doris_tl" + System.currentTimeMillis() + "')"
        );

        // 加工数据
        tenv.createTemporaryFunction("time_round", TimeRound.class);
        tenv.executeSql(
                " insert into   mall_evts_overview_agg                                                              "
                        + " with tmp as (                                                                  "
                        + " select                                                                         "
                        + "    page_service,                                                               "
                        + "    page_type,                                                                  "
                        + "    release_channel,                                                            "
                        + "    device_type,                                                                "
                        + "    event_id,                                                                   "
                        + "    user_id,                                                                    "
                        + "    rt                                                                          "
                        + " from mall_events_wide_source                                                   "
                        + " where event_id not in ('page_load','launch','app_close','put_back','wake_up')  "
                        + " )                                                                              "
                        + "                                                                                "
                        + " SELECT                                                                         "
                        + "   to_date(date_format(window_start,'yyyy-MM-dd')) as dt,                       "
                        + "   time_round(date_format(window_start,'yyyy-MM-dd HH:mm:ss'),60) as time_60m,  "
                        + "   time_round(date_format(window_start,'yyyy-MM-dd HH:mm:ss'),30) as time_30m,  "
                        + "   time_round(date_format(window_start,'yyyy-MM-dd HH:mm:ss'),10) as time_10m,  "
                        + "   page_service,                                                                "
                        + "   page_type,                                                                   "
                        + "   release_channel,                                                             "
                        + "   device_type,                                                                 "
                        + "   event_id,                                                                    "
                        + "   user_id,                                                                     "
                        + "   count(1) as act_count                                                        "
                        + " FROM TABLE(                                                                    "
                        + "   TUMBLE(TABLE tmp,DESCRIPTOR(rt),INTERVAL '10' MINUTE )                       "
                        + " )                                                                              "
                        + " GROUP BY                                                                       "
                        + "   window_start,                                                                "
                        + "   window_end,                                                                  "
                        + "   page_service,                                                                "
                        + "   page_type,                                                                   "
                        + "   release_channel,                                                             "
                        + "   device_type,                                                                 "
                        + "   event_id,                                                                    "
                        + "   user_id                                                                      "
        );
    }
}
