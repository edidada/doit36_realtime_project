package cn.doitedu.rtmk.demo3;

import cn.doitedu.rtmk.beans.EventBean;
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

public class RuleCalculatorPattern {
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
