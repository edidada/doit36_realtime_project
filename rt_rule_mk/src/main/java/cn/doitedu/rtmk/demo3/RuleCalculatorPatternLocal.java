package cn.doitedu.rtmk.demo3;

import cn.doitedu.rtmk.beans.EventBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 规则计算器模式演示
 * 规则1： id=r001
 * 如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种
 * 规则2：id = r002
 * 如果某个用户在规则运行之后，发生了如下行为 >=4 次 ：collect事件且收藏的物品id属于  [p02,p04,p08]中的一种
 * 则，当该用户再发生分享行为(share)，就给他推送消息
 *
 * 此版本不需要Kafka，使用内置数据源进行测试
 **/
public class RuleCalculatorPatternLocal {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/tmp/ckpt");
        env.setParallelism(1);

        HashMap<String, String> props1 = new HashMap<>();
        props1.put("item_id", "p01");

        HashMap<String, String> props2 = new HashMap<>();
        props2.put("item_id", "p02");

        HashMap<String, String> props3 = new HashMap<>();
        props3.put("item_id", "p03");

        HashMap<String, String> props4 = new HashMap<>();
        props4.put("item_id", "p04");

        HashMap<String, String> props5 = new HashMap<>();
        props5.put("item_id", "p05");

        HashMap<String, String> props6 = new HashMap<>();
        props6.put("item_id", "p08");

        EventBean event1 = new EventBean(5, "add_cart", System.currentTimeMillis(), props1);
        EventBean event2 = new EventBean(5, "page_view", System.currentTimeMillis(), new HashMap<>());
        EventBean event3 = new EventBean(5, "event3", System.currentTimeMillis(), new HashMap<>());
        EventBean event4 = new EventBean(6, "add_cart", System.currentTimeMillis(), props4);
        EventBean event5 = new EventBean(7, "add_cart", System.currentTimeMillis(), props5);
        EventBean event6 = new EventBean(7, "add_cart", System.currentTimeMillis(), props1);
        EventBean event7 = new EventBean(8, "collect", System.currentTimeMillis(), props2);
        EventBean event8 = new EventBean(8, "collect", System.currentTimeMillis(), props2);
        EventBean event9 = new EventBean(8, "collect", System.currentTimeMillis(), props2);
        EventBean event10 = new EventBean(8, "collect", System.currentTimeMillis(), props2);
        EventBean event11 = new EventBean(8, "collect", System.currentTimeMillis(), props2);
        EventBean event12 = new EventBean(8, "share", System.currentTimeMillis(), new HashMap<>());

        DataStreamSource<EventBean> ds = env.fromElements(
                event1, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12
        );

        ds.print("输入事件:");

        KeyedStream<EventBean, Integer> keyed = ds.keyBy(EventBean::getUser_id);

        keyed.process(new KeyedProcessFunction<Integer, EventBean, String>() {

            Rule001Calculator rule001Calculator;
            Rule002Calculator rule002Calculator;

            HashMap<String, RuleCalculator> ruleCalculators = new HashMap<String, RuleCalculator>();

            @Override
            public void open(Configuration parameters) throws Exception {
                rule001Calculator = new Rule001Calculator();
                rule001Calculator.init(getRuntimeContext());

                rule002Calculator = new Rule002Calculator();
                rule002Calculator.init(getRuntimeContext());

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

        env.execute("规则计算器模式-本地测试版");
    }
}
