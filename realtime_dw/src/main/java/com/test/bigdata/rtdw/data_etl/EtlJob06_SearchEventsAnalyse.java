package com.test.bigdata.rtdw.data_etl;

import com.test.bigdata.rtdw.beans.EventBean;
import com.test.bigdata.rtdw.beans.SearchBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;

/**
 * 搜索主题分析，轻度聚合（把一次搜索生命周期的信息聚合成1行）
 **/
public class EtlJob06_SearchEventsAnalyse {

    public static void main(String[] args) throws Exception {

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

        /* *
         * 一、 读 kafka中的日志明细数据
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("gpac01" + System.currentTimeMillis())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("dwd-events-log")
                .build();
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");
        SingleOutputStreamOperator<EventBean> beans = ds.map(json -> JSON.parseObject(json, EventBean.class));


        /* *
         * 二、 数据加工核心逻辑
         */
        SingleOutputStreamOperator<SearchBean> wideStream =
                /* *
                 * 1.  过滤行为事件，只留下搜索相关行为
                 */
                beans.filter(new FilterFunction<EventBean>() {
                            @Override
                            public boolean filter(EventBean eventBean) throws Exception {
                                String eventId = eventBean.getEvent_id();
                                return eventId.equals("search") || "search_return".equals(eventId) || "search_click".equals(eventId);
                            }
                        })
                        /* *
                         * 2. 按 搜索id号  keyBy
                         */
                        .keyBy(new KeySelector<EventBean, String>() {
                            @Override
                            public String getKey(EventBean eventBean) throws Exception {
                                return eventBean.getProperties().get("search_id");
                            }
                        })
                        /* *
                         * 3. 请求接口获取近义词和分词结果
                         * 为每条数据添加搜索起始时间
                         */
                        .process(new KeyedProcessFunction<String, EventBean, SearchBean>() {
                            HttpClient client;
                            HttpPost post;
                            HashMap<String, String> body = new HashMap<>();
                            SearchBean searchBean;
                            ValueState<Long> searchStartTimeState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                client = HttpClientBuilder.create().build();

                                post = new HttpPost("http://doitedu:8081/api/post/simwords");
                                post.addHeader("Content-type", "application/json; charset=utf-8");
                                post.addHeader("Accept", "application/json");

                                searchBean = new SearchBean();

                                searchStartTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("searchStartTime", Long.class));
                            }

                            @Override
                            public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, SearchBean>.Context ctx, Collector<SearchBean> out) throws Exception {

                                if (searchStartTimeState.value() == null || eventBean.getEvent_id().equals("search")) {
                                    searchStartTimeState.update(eventBean.getEvent_time());
                                }

                                // 取出搜索词
                                String keyword = eventBean.getProperties().get("keyword");
                                body.put("origin", keyword);

                                // 将body设置到post的请求体中
                                StringEntity stringEntity = new StringEntity(JSON.toJSONString(body), "utf-8");
                                post.setEntity(stringEntity);

                                // 发送请求
                                HttpResponse response = client.execute(post);
                                // 取出返回结果中的json
                                String resJson = EntityUtils.toString(response.getEntity(), "utf-8");
                                // 解析结果json，取出所要的数据
                                JSONObject jsonObject = JSON.parseObject(resJson);
                                String similarWord = jsonObject.getString("similarWord");
                                String splitWords = jsonObject.getString("words");

                                searchBean.setUser_id(eventBean.getUser_id());
                                searchBean.setEvent_time(eventBean.getEvent_time());
                                // 从状态中获取本次搜索生命周期的搜索发起时间
                                searchBean.setSearch_time(searchStartTimeState.value());

                                searchBean.setSearch_id(eventBean.getProperties().get("search_id"));
                                searchBean.setKeyword(keyword);
                                searchBean.setSplit_word(splitWords);
                                searchBean.setSimilar_word(similarWord);
                                searchBean.setSearch_click_cnt(0);
                                searchBean.setSearch_return_cnt(0);

                                // 如果本事件是一个搜索结果返回事件，则填充返回结果条数字段
                                if (eventBean.getEvent_id().equals("search_return")) {
                                    searchBean.setSearch_return_cnt(Integer.parseInt(eventBean.getProperties().get("res_cnt")));
                                }
                                // 如果本事件是一个搜索结果点击事件，则填充点击次数字段
                                if (eventBean.getEvent_id().equals("search_click")) {
                                    searchBean.setSearch_click_cnt(1);
                                }
                                out.collect(searchBean);
                            }
                        });


        /* *
         * 三、 流 转 表
         * 并自定义schema （添加事件时间属性字段及watermark，用于后续的时间窗口聚合）
         */
        tenv.createTemporaryView("wide", wideStream, Schema.newBuilder()
                .column("user_id", DataTypes.BIGINT())
                .column("event_time", DataTypes.BIGINT())
                .column("search_time", DataTypes.BIGINT())
                .column("search_id", DataTypes.STRING())
                .column("keyword", DataTypes.STRING())
                .column("split_word", DataTypes.STRING())
                .column("similar_word", DataTypes.STRING())
                .column("search_return_cnt", DataTypes.INT())
                .column("search_click_cnt", DataTypes.INT())
                .columnByExpression("rt", "to_timestamp_ltz(event_time,3)")
                .watermark("rt", "rt - interval '0' second ")
                .build());

        /* *
         * 四、 窗口聚合，并输出结果到doris
         */
        tenv.executeSql(
                " create table search_ana_agg_doris(    "
                        + "     dt               DATE,           "
                        + "     user_id          BIGINT,          "
                        + "     search_time      timestamp(3), "
                        + "     search_id        VARCHAR(20),  "
                        + "     keyword          VARCHAR(60),  "
                        + "     split_words      VARCHAR(60),  "
                        + "     similar_word     VARCHAR(60),  "
                        + "     return_item_count     INT,     "
                        + "     click_item_count      INT      "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'doitedu:8030',         "
                        + "    'table.identifier' = 'dws.search_ana_agg',  "
                        + "    'username' = 'root',                "
                        + "    'password' = '',                    "
                        + "    'sink.label-prefix' = 'doris_tl" + System.currentTimeMillis()+"')");

        tenv.executeSql(
                " INSERT INTO search_ana_agg_doris                                       "
                        +" SELECT                                                                 "
                        +"   to_date(date_format(to_timestamp_ltz(search_time,3),'yyyy-MM-dd')) as dt, "
                        +"   user_id,                                                             "
                        +"   to_timestamp_ltz(search_time,3) as search_time,                      "
                        +"   search_id,                                                           "
                        +"   keyword,                                                             "
                        +"   split_word,                                                          "
                        +"   similar_word,                                                        "
                        +"   sum(search_return_cnt) as search_return_cnt,                         "
                        +"   sum(search_click_cnt) as search_click_cnt                            "
                        +" FROM TABLE(                                                            "
                        +"   TUMBLE(TABLE wide,DESCRIPTOR(rt),INTERVAL '10' MINUTE)               "
                        +" )                                                                      "
                        +" GROUP BY                                                               "
                        +"   window_start,                                                        "
                        +"   window_end,                                                          "
                        +"   user_id,                                                             "
                        +"   search_time,                                                         "
                        +"   search_id,                                                           "
                        +"   keyword,                                                             "
                        +"   split_word,                                                          "
                        +"   similar_word                                                         "
        );


        env.execute();
    }
}
