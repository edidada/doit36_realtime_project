package cn.doitedu.rtmk.demo5;

import cn.doitedu.rtmk.beans.EventBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/16
 * @Desc: 学大数据，多易教育
 *
 * 规则运算逻辑，封装到  规则运算机
 * 规则运算机所需要的规则参数从硬编码中剥离出来，可由外部传入；
 * 此时的规则运算机，严格上可称之为模型运算机
 * 每个模型，有一个运算机类；有一套固定格式的参数模板；
 * 每个具体规则，则有一套自己的具体的确定的参数；
 *
 * 使用RoaringBitmap优化静态画像人群匹配
 *
 * 此版本不需要Kafka，使用内置数据源进行测试
 **/
public class BitmapOptimizedRuleEngineLocal {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/tmp/ckpt");
        env.setParallelism(1);

        HashMap<String, String> props1 = new HashMap<>();
        props1.put("ad_id", "ad_01");

        HashMap<String, String> props2 = new HashMap<>();
        props2.put("item_id", "item_01");

        HashMap<String, String> props3 = new HashMap<>();
        props3.put("item_id", "p02");

        HashMap<String, String> props4 = new HashMap<>();
        props4.put("item_price", "100");

        HashMap<String, String> props5 = new HashMap<>();
        props5.put("p1", "v1");

        EventBean event1 = new EventBean(1001, "ad_click", System.currentTimeMillis(), props1);
        EventBean event2 = new EventBean(1002, "ad_click", System.currentTimeMillis(), new HashMap<>());
        EventBean event3 = new EventBean(1003, "item_share", System.currentTimeMillis(), props2);
        EventBean event4 = new EventBean(1004, "item_collect", System.currentTimeMillis(), props3);
        EventBean event5 = new EventBean(1005, "item_share", System.currentTimeMillis(), new HashMap<>());
        EventBean event6 = new EventBean(1006, "add_cart", System.currentTimeMillis(), props4);
        EventBean event7 = new EventBean(1007, "ad_click", System.currentTimeMillis(), new HashMap<>());
        EventBean event8 = new EventBean(1008, "x", System.currentTimeMillis(), new HashMap<>());
        EventBean event9 = new EventBean(1009, "y", System.currentTimeMillis(), new HashMap<>());
        EventBean event10 = new EventBean(1010, "a", System.currentTimeMillis(), props5);
        EventBean event11 = new EventBean(20, "a", System.currentTimeMillis(), props5);
        EventBean event12 = new EventBean(21, "a", System.currentTimeMillis(), props5);

        DataStreamSource<EventBean> ds = env.fromElements(
                event1, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12
        );

        ds.print("输入事件:");

        KeyedStream<EventBean, Integer> keyed = ds.keyBy(EventBean::getUser_id);

        keyed.process(new KeyedProcessFunction<Integer, EventBean, String>() {

            HashMap<String, RuleCalculator> ruleCalculators = new HashMap<String, RuleCalculator>();

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
                rule001Calculator.init(getRuntimeContext(), rule001Param, RoaringBitmap.bitmapOf());

                RuleModel001Calculator rule002Calculator = new RuleModel001Calculator();
                String rule002Param =
                        "{\n" +
                        "   \"ruleId\":\"r002\",\n" +
                        "   \"triggerEventId\":\"item_share\",\n" +
                        "   \"triggerEventPropName\":\"item_id\",\n" +
                        "   \"triggerEventPropValue\":[\"item_01\",\"item_02\",\"item_03\"]\n" +
                        "}";
                rule002Calculator.init(getRuntimeContext(), rule002Param, null);

                ruleCalculators.put("r001", rule001Calculator);
                ruleCalculators.put("r002", rule002Calculator);
            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<Integer, EventBean, String>.Context ctx, Collector<String> out) throws Exception {
                for (Map.Entry<String, RuleCalculator> entry : ruleCalculators.entrySet()) {
                    RuleCalculator calculator = entry.getValue();
                    calculator.calc(eventBean, out);
                }
            }
        }).print("匹配结果:");

        env.execute("Bitmap优化规则引擎-本地测试版");
    }
}