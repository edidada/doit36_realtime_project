package com.test.bigdata.rtmk.demo1;

import com.test.bigdata.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 *   实现目标：
 *      监控在线用户的行为
 *      如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种
 *
 *   此版本不需要Kafka，使用内置数据源进行测试
 **/
public class TargetProductCartMonitorLocal {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/tmp/ckpt");
        env.setParallelism(1);

        HashMap<String, String> props1 = new HashMap<>();
        props1.put("item_id", "p01");
        props1.put("item_name", "商品1");

        HashMap<String, String> props2 = new HashMap<>();
        props2.put("item_id", "p02");
        props2.put("item_name", "商品2");

        HashMap<String, String> props3 = new HashMap<>();
        props3.put("item_id", "p03");
        props3.put("item_name", "商品3");

        HashMap<String, String> props4 = new HashMap<>();
        props4.put("item_id", "p04");
        props4.put("item_name", "商品4");

        HashMap<String, String> props5 = new HashMap<>();
        props5.put("item_id", "p05");
        props5.put("item_name", "商品5");

        EventBean event1 = new EventBean(1001, "add_cart", System.currentTimeMillis(), props1);
        EventBean event2 = new EventBean(1001, "page_view", System.currentTimeMillis(), props2);
        EventBean event3 = new EventBean(1002, "add_cart", System.currentTimeMillis(), props3);
        EventBean event4 = new EventBean(1003, "add_cart", System.currentTimeMillis(), props4);
        EventBean event5 = new EventBean(1004, "add_cart", System.currentTimeMillis(), props5);
        EventBean event6 = new EventBean(1001, "add_cart", System.currentTimeMillis(), props2);

        DataStreamSource<EventBean> ds = env.fromElements(
                event1, event2, event3, event4, event5, event6
        );

        ds.print("输入事件:");

        KeyedStream<EventBean, Integer> keyed = ds.keyBy(EventBean::getUser_id);

        keyed.process(new KeyedProcessFunction<Integer, EventBean, String>() {
            HashSet<String> targetItems;
            JSONObject messageJsonObj;

            @Override
            public void open(Configuration parameters) throws Exception {
                targetItems = new HashSet<>();
                targetItems.addAll(Arrays.asList("p01", "p03", "p05"));

                messageJsonObj = new JSONObject();
                messageJsonObj.put("ruleId", "r001");
            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<Integer, EventBean, String>.Context ctx, Collector<String> out) throws Exception {
                if (eventBean.getEvent_id().equals("add_cart")
                        && targetItems.contains(eventBean.getProperties().get("item_id"))) {

                    messageJsonObj.put("userId", eventBean.getUser_id());
                    messageJsonObj.put("matchTime", eventBean.getEvent_time());

                    out.collect(messageJsonObj.toJSONString());
                }
            }
        }).print("匹配结果:");

        env.execute("目标商品购物车监控-本地测试版");
    }
}
