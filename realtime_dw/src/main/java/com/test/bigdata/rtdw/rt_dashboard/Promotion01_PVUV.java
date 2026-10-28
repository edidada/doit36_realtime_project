package com.test.bigdata.rtdw.rt_dashboard;

import com.test.bigdata.rtdw.utils.CommonSql;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 *
 *  营销活动A/B/C 的活动页（page006,page007,page008）流量实时监控指标计算
 *    计算指标： 每个营销活动页当天累计到当前的pv、uv量，每分钟更新一次
 *
 **/
public class Promotion01_PVUV {
    public static void main(String[] args) throws Exception {

        // 创建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));
        env.setParallelism(1);

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建连接器表，映射kafka中的dwd层的明细数据topic
        tenv.executeSql(CommonSql.KAFKA_WIDE_LOG_DDL);

        // 创建连接器表，映射mysql中的该实时看板指标对应的表
        tenv.executeSql(
                " CREATE TABLE dashboard_promotion_abc_pvuv (            "
                        +"   page_url STRING,                                "
                        +"   update_time  timestamp(3),                      "
                        +"   pv   BIGINT,                                    "
                        +"   uv   BIGINT,                                    "
                        +"   PRIMARY KEY (page_url) NOT ENFORCED             "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://doitedu:3306/rtrpt',     "
                        +"    'table-name' = 'dashboard_promotion_abc_pvuv', "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'root'                            "
                        +" )                                                 "
        );


        // 写一个统计sql，计算结果并插入目标sink表
        tenv.executeSql(
                /*" INSERT INTO dashboard_promotion_abc_pvuv                                       "*/
                " create temporary view res_view  AS                                 "
                        +" WITH tmp AS (                                                                  "
                        +"   SELECT                                                                       "
                        +"     properties['url'] as page_url,                                             "
                        +"     user_id,                                                                   "
                        +"     rt                                                                         "
                        +"   FROM  mall_events_wide_source                                                "
                        +"   WHERE event_id='page_load'                                                   "
                        +"     and properties['url'] in ('/page008','/page007','/page009')                "
                        +" )                                                                              "
                        +"                                                                                "
                        +" SELECT                                                                         "
                        +"   page_url,                                                                    "
                        +"   window_end as update_time,                                                                  "
                        +"   count(1) as pv,                                                              "
                        +"   count(distinct user_id) as uv                                                "
                        +" FROM TABLE(                                                                    "
                        +"   CUMULATE(TABLE tmp,DESCRIPTOR(rt),INTERVAL '1' MINUTE , INTERVAL '24' HOUR)  "
                        +" )                                                                              "
                        +" GROUP BY                                                                       "
                        +"   window_start,                                                                "
                        +"   window_end,                                                                  "
                        +"   page_url                                                                     "
        );

        //tenv.executeSql("select * from res_view").print();


        // 把表转成流
        DataStream<Row> res_view = tenv.toDataStream(tenv.from("res_view"));

        res_view.addSink(new RedisSink("doitedu",6379));

        env.execute();


    }

    public static class RedisSink extends RichSinkFunction<Row>{
        String host;
        int port;
        Jedis jedis;


        public RedisSink(String host,int port){
            this.host = host;
            this.port = port;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis(this.host, this.port);
        }


        @Override
        public void invoke(Row row, Context context) throws Exception {

            // key-value  ,key-List  ,key-HashMap  , key-Set , key-SortedSet , key-bitmap ,key-GpS
            String pageUrl = row.getFieldAs("page_url");
            LocalDateTime update_time = row.getFieldAs("update_time");

            Long pv = row.getFieldAs("pv");
            Long uv = row.getFieldAs("uv");

            JSONObject jsonObj = new JSONObject();
            jsonObj.put("update_time",update_time);
            jsonObj.put("pv",pv);
            jsonObj.put("uv",uv);

            jedis.set(pageUrl, jsonObj.toJSONString());

        }
    }


}
