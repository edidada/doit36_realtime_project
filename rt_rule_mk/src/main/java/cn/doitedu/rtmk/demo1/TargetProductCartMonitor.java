package cn.doitedu.rtmk.demo1;

import cn.doitedu.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/14
 * @Desc: 学大数据，到多易教育
 *
 *   实现目标：
 *      监控在线用户的行为
 *      如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种
 **/
public class TargetProductCartMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        // 读 kafka 数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("dwd-events-log")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("g01")
                .build();


        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "log-source");

        SingleOutputStreamOperator<EventBean> beans = ds.map(json -> JSON.parseObject(json, EventBean.class));
        beans.print();

        KeyedStream<EventBean, Integer> keyed = beans.keyBy(EventBean::getUser_id);

        keyed.process(new KeyedProcessFunction<Integer, EventBean, String>() {
            HashSet<String> targetItems;
            JSONObject messageJsonObj;
            @Override
            public void open(Configuration parameters) throws Exception {

                targetItems = new HashSet<>();
                targetItems.addAll(Arrays.asList("p01","p03","p05"));

                messageJsonObj = new JSONObject();
                messageJsonObj.put("ruleId","r001");
            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<Integer, EventBean, String>.Context ctx, Collector<String> out) throws Exception {

                // ruleId='r001'
                // 如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种
                if(eventBean.getEvent_id().equals("add_cart")
                        && targetItems.contains(eventBean.getProperties().get("item_id"))){

                    messageJsonObj.put("userId",eventBean.getUser_id());
                    messageJsonObj.put("matchTime",eventBean.getEvent_time());

                    out.collect(messageJsonObj.toJSONString());

                }
            }
        }).print();


        env.execute();

    }
}
