package cn.doitedu.rtmk.demo2;

import cn.doitedu.rtmk.beans.EventBean;
import cn.doitedu.rtmk.demo3.Rule001Calculator;
import cn.doitedu.rtmk.demo3.Rule002Calculator;
import cn.doitedu.rtmk.demo3.RuleCalculator;
import com.alibaba.fastjson.JSON;
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

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * 实现目标，两个规则同时运行
 * 规则1： id=r001
 * 如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种
 * 规则2：id = r002
 * 如果某个用户在规则运行之后，发生了如下行为 >=4 次 ：collect事件且收藏的物品id属于  [p02,p04,p08]中的一种
 * 则，当该用户再发生分享行为(share)，就给他推送消息
 * <p>
 * <p>
 * 测试环境，先创建一个topic
 * kafka-topics.sh --create --topic dwd-events-log --partitions 1 --replication-factor 1 --zookeeper doitedu:2181
 * <p>
 * 然后可以用如下测试数据
 * {"user_id":5,"event_id":"event1","event_time":1678777681000,"properties":{}}
 * {"user_id":5,"event_id":"event2","event_time":1678777682000,"properties":{}}
 * {"user_id":5,"event_id":"event3","event_time":1678777683000,"properties":{}}
 * {"user_id":6,"event_id":"add_cart","event_time":1678777684000,"properties":{"item_id":"p04"}}
 * {"user_id":7,"event_id":"add_cart","event_time":1678777685000,"properties":{"item_id":"p05"}}
 * {"user_id":7,"event_id":"add_cart","event_time":1678777686000,"properties":{"item_id":"p01"}}
 **/
public class MultiRuleEngine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));
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

        KeyedStream<EventBean, Integer> keyed = beans.keyBy(EventBean::getUser_id);

        keyed.process(new KeyedProcessFunction<Integer, EventBean, String>() {

            Rule001Calculator rule001Calculator;
            Rule002Calculator rule002Calculator;

            HashMap<String, RuleCalculator> ruleCalculators =  new HashMap<String, RuleCalculator>();

            @Override
            public void open(Configuration parameters) throws Exception {
                rule001Calculator = new Rule001Calculator();
                rule001Calculator.init(getRuntimeContext());

                rule002Calculator = new Rule002Calculator();
                rule002Calculator.init(getRuntimeContext());

                ruleCalculators.put("r001",rule001Calculator);
                ruleCalculators.put("r002",rule002Calculator);

            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<Integer, EventBean, String>.Context ctx, Collector<String> out) throws Exception {

                for (Map.Entry<String, RuleCalculator> entry : ruleCalculators.entrySet()) {
                    RuleCalculator calculator = entry.getValue();
                    calculator.calc(eventBean,out);
                }
            }
        }).print();


        env.execute();

    }
}
