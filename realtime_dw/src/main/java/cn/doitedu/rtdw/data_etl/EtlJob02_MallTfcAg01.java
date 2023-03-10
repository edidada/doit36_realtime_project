package cn.doitedu.rtdw.data_etl;

import cn.doitedu.rtdw.utils.CommonSql;
import cn.doitedu.rtdw.utils.TimeRound;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/9
 * @Desc: 学大数据，到多易教育
 *  实时流量概况多维模型统计任务（轻度聚合）
 **/
public class EtlJob02_MallTfcAg01 {

    public static void main(String[] args) {
        // 创建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE); // 启用checkpoint，模式为精确一次
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");  // 设置checkpoint的持久化存储路径
        env.setParallelism(1);  // 设置默认并行度
        // TODO 可以增加一个设置： rocksdbBackend可以开启增量checkpoint
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.setStateBackend(new HashMapStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建 kafka 连接器表： 映射维度打宽后的  行为明细宽表 topic
        tEnv.executeSql(CommonSql.KAFKA_WIDE_LOG_DDL);

        // 创建doris连接器表： 轻度聚合目标sink表
        tEnv.executeSql(
                " CREATE TABLE mall_tfc_ag01(     "
                        +" dt  DATE,                 "
                        +" time_60m STRING,          "
                        +" time_30m STRING,          "
                        +" time_10m STRING,          "
                        +" time_m STRING,            "
                        +" user_id INT,              "
                        +" is_newuser INT,           "
                        +" session_id STRING,        "
                        +" release_channel STRING,   "
                        +" device_type STRING,       "
                        +" gps_province STRING,      "
                        +" gps_city STRING,          "
                        +" gps_region STRING,        "
                        +" page_type STRING,         "
                        +" page_service STRING,      "
                        +" page_url STRING,          "
                        +" pv_amt BIGINT             "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'doitedu:8030',         "
                        + "    'table.identifier' = 'dws.mall_tfc_ag01',  "
                        + "    'username' = 'root',                "
                        + "    'password' = '',                    "
                        + "    'sink.label-prefix' = 'doris_label_dws"+System.currentTimeMillis()+"'"
                        + " )                                         ");


        // 选择所需字段，扩展：小时、30分，10分，分 维度字段， 按照最小粒度，聚合pv数
        // 这里麻烦的点在：各种时间的处理，尤其是按30分钟、10分钟取整的操作，用内置函数极为不便，所以干脆做了自定义函数
        tEnv.createTemporaryFunction("time_round", TimeRound.class);
        tEnv.executeSql(
                "INSERT INTO mall_tfc_ag01                                                       "
                + " WITH tmp AS (                                                                   "
                +" SELECT                                                                          "
                +"   user_id,                                                                      "
                +"   if(DATE_FORMAT(register_time,'yyyy-MM-dd')<CURRENT_DATE,0,1) AS is_newuser,   "
                +"   session_id,                                                                   "
                +"   release_channel,                                                              "
                +"   device_type,                                                                  "
                +"   gps_province,                                                                 "
                +"   gps_city,                                                                     "
                +"   gps_region,                                                                   "
                +"   page_type,                                                                    "
                +"   page_service,                                                                 "
                +"   properties['url'] as page_url,                                                                     "
                +"   rt                                                                            "
                +" FROM mall_events_wide_source                                                    "
                +" WHERE event_id = 'page_load' )                                                  "
                +" SELECT                                                                          "
                +"   TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd'))  as dt,                       "
                +"   time_round(cast(window_start as string),60) as time_60m,                      "
                +"   time_round(cast(window_start as string),30) as time_30m,                      "
                +"   time_round(cast(window_start as string),10) as time_10m,                      "
                +"   time_round(cast(window_start as string),1) as time_m,                         "
                +"   user_id,                                                                      "
                +"   is_newuser,                                                                   "
                +"   session_id,                                                                   "
                +"   release_channel,                                                              "
                +"   device_type,                                                                  "
                +"   gps_province,                                                                 "
                +"   gps_city,                                                                     "
                +"   gps_region,                                                                   "
                +"   page_type,                                                                    "
                +"   page_service,                                                                 "
                +"   page_url,                                                                     "
                +"   count(1) as pv_amt                                                            "
                +" FROM TABLE(                                                                     "
                +"   TUMBLE(TABLE tmp,DESCRIPTOR(rt), INTERVAL '1' MINUTE)                         "
                +" )                                                                               "
                +" GROUP BY                                                                        "
                +"   window_start,window_end,                                                      "
                +"   user_id,                                                                      "
                +"   is_newuser,                                                                   "
                +"   session_id,                                                                   "
                +"   release_channel,                                                              "
                +"   device_type,                                                                  "
                +"   gps_province,                                                                 "
                +"   gps_city,                                                                     "
                +"   gps_region,                                                                   "
                +"   page_type,                                                                    "
                +"   page_service,                                                                 "
                +"   page_url                                                                      "
        );
    }

}
