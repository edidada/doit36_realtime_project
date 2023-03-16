package cn.doitedu.rtmk.demo4;

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

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/16
 * @Desc: 学大数据，到多易教育
 *
 * 规则运算逻辑，封装到  规则运算机
 * 规则运算机所需要的规则参数从硬编码中剥离出来，可由外部传入；
 * 此时的规则运算机，严格上可称之为模型运算机
 * 每个模型，有一个运算机类；有一套固定格式的参数模板；
 * 每个具体规则，则有一套自己的具体的确定的参数；
 *
 *
 *
 **/
public class Demo4 {
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

            HashMap<String, RuleCalculator> ruleCalculators =  new HashMap<String, RuleCalculator>();

            @Override
            public void open(Configuration parameters) throws Exception {

                RuleModel001Calculator rule001Calculator = new RuleModel001Calculator();
                String rule001Param =
                        "{\n" +
                        "   \"ruleId\":\"r001\",\n" +
                        "   \"triggerEventId\":\"ad_click\",\n" +
                        "   \"triggerEventPropName\":\"ad_id\",\n" +
                        "   \"triggerEventPropValue\":[\"ad_01\",\"ad_02\",\"ad_03\"]\n" +
                        "}";
                rule001Calculator.init(getRuntimeContext(),rule001Param);


                RuleModel001Calculator rule002Calculator = new RuleModel001Calculator();
                String rule002Param =
                        "{\n" +
                                "   \"ruleId\":\"r002\",\n" +
                                "   \"triggerEventId\":\"item_share\",\n" +
                                "   \"triggerEventPropName\":\"item_id\",\n" +
                                "   \"triggerEventPropValue\":[\"item_01\",\"item_02\",\"item_03\"]\n" +
                        "}";
                rule002Calculator.init(getRuntimeContext(),rule002Param);


                RuleModel002Calculator rule003Calculator = new RuleModel002Calculator();
                String rule003Param =
                                "{\n" +
                                "  \"ruleId\":\"r003\",\n" +
                                "  \"triggerEventId\":\"item_share\",\n" +
                                "  \"dynamicProfile\":[\n" +
                                "    {\n" +
                                "      \"tagEvent\":{\n" +
                                "        \"tagEventId\":\"item_collect\",\n" +
                                "        \"tagEventProp\":\"item_id\",\n" +
                                "        \"tagEventValues\":[\"p02\",\"p04\",\"p08\"]\n" +
                                "      },\n" +
                                "      \"tagCompareOp\":\">\",\n" +
                                "      \"tagCompareValue\":4\n" +
                                "    }\n" +
                                "  ]\n" +
                                "}";
                rule003Calculator.init(getRuntimeContext(),rule003Param);



                RuleModel003Calculator rule004Calculator = new RuleModel003Calculator();
                String rule004Param =
                        "{\n" +
                                "  \"ruleId\":\"r004\",\n" +
                                "  \"ruleModelId\":\"m003\",\n" +
                                "  \"triggerEvent\":{\n" +
                                "    \"eventId\":\"ad_click\",\n" +
                                "    \"eventPropName\":\"url\",\n" +
                                "    \"eventPropCompareOp\":\"in\",\n" +
                                "    \"eventPropValue\":[\"/page008\",\"/page009\"]\n" +
                                "  },\n" +
                                "  \"dynamicProfile\":[\n" +
                                "    {\n" +
                                "      \"tagEvent\":{\n" +
                                "        \"eventId\":\"add_cart\",\n" +
                                "        \"eventPropName\":\"item_price\",\n" +
                                "        \"eventPropValueMaxMin\":2\n" +
                                "      }\n" +
                                "    }\n" +
                                "  ]\n" +
                                "}";
                rule004Calculator.init(getRuntimeContext(),rule004Param);



                RuleModel004Calculator rule005Calculator = new RuleModel004Calculator();
                String rule005Param =
                        "{\n" +
                                "  \"ruleId\":\"r005\",\n" +
                                "  \"ruleModelId\":\"m004\",\n" +
                                "  \"triggerEvent\":{\n" +
                                "    \"eventId\":\"a\",\n" +
                                "    \"propName\":\"p1\",\n" +
                                "    \"compareOp\":\"=\",\n" +
                                "    \"compareValue\":\"v1\"\n" +
                                "  },\n" +
                                "  \"dynamicProfile\":{\n" +
                                "    \"eventSeq\":[\n" +
                                "      {\n" +
                                "        \"eventId\":\"x\",\n" +
                                "        \"propName\":\"p2\",\n" +
                                "        \"compareOp\":\"=\",\n" +
                                "        \"compareValue\":\"v2\"\n" +
                                "      },\n" +
                                "      {\n" +
                                "        \"eventId\":\"y\",\n" +
                                "        \"propName\":\"p3\",\n" +
                                "        \"compareOp\":\"=\",\n" +
                                "        \"compareValue\":\"v3\"\n" +
                                "      },\n" +
                                "      {\n" +
                                "        \"eventId\":\"z\",\n" +
                                "        \"propName\":\"p4\",\n" +
                                "        \"compareOp\":\"=\",\n" +
                                "        \"compareValue\":\"v4\"\n" +
                                "      }\n" +
                                "\n" +
                                "    ],\n" +
                                "    \"seqActMinCount\": 2\n" +
                                "  },\n" +
                                "  \"staticProfile\":[\n" +
                                "    {\n" +
                                "      \"tagName\":\"tg01\",\n" +
                                "      \"compareOp\":\"=\",\n" +
                                "      \"compareValue\":5\n" +
                                "    },\n" +
                                "    {\n" +
                                "      \"tagName\":\"tg08\",\n" +
                                "      \"compareOp\":\"=\",\n" +
                                "      \"compareValue\":28\n" +
                                "    }\n" +
                                "  ]\n" +
                                "}";
                rule005Calculator.init(getRuntimeContext(),rule005Param);


                ruleCalculators.put("r001", rule001Calculator);
                ruleCalculators.put("r002", rule002Calculator);
                ruleCalculators.put("r003", rule003Calculator);
                ruleCalculators.put("r004", rule004Calculator);
                ruleCalculators.put("r005", rule005Calculator);

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
