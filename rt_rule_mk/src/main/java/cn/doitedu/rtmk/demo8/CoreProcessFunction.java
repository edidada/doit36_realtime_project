package cn.doitedu.rtmk.demo8;

import cn.doitedu.rtmk.beans.EventBean;
import cn.doitedu.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CoreProcessFunction extends KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String> {

    // 定义一个运算机池
    //HashMap<Integer, RuleCalculator> ruleCalculators = new HashMap<>();

    // 定义一个运算机池
    //MapState<Integer, RuleCalculator> calculatorMapState;

    @Override
    public void open(Configuration parameters) throws Exception {

        //calculatorMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, RuleCalculator>("caculatorMapState", Integer.class, RuleCalculator.class));

    }

    @Override
    public void processElement(EventBean eventBean, KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 遍历运算机池(HashMap)，将本次接收到的行为事件，交给每个运算机去处理
        /* for (RuleCalculator calculator : ruleCalculators.values()) {
            calculator.calc(eventBean,out);
        }*/

        // 遍历运算机池(MapState)，将本次接收到的行为事件，交给每个运算机去处理
        // 此法不可行，MapState在processBroadcastElement方法中不可用
       /* for (Map.Entry<Integer, RuleCalculator> entry : calculatorMapState.entries()) {
            RuleCalculator ruleCalculator = entry.getValue();
            ruleCalculator.calc(eventBean,out);
        }*/


        // 遍历运算机池（广播状态），将本次接收到的行为事件，交给每个运算机去处理
        MapStateDescriptor<Integer, RuleMeta> mapStateDescriptor = new MapStateDescriptor<>("ruleMetaState", Integer.class, RuleMeta.class);
        ReadOnlyBroadcastState<Integer, RuleMeta> calculators = ctx.getBroadcastState(mapStateDescriptor);
        for (Map.Entry<Integer, RuleMeta> entry : calculators.immutableEntries()) {
            RuleMeta ruleMeta = entry.getValue();
            RuleCalculator ruleCalculator = ruleMeta.getRuleCalculator();
            ruleCalculator.calc(eventBean,out);
        }


    }

    @Override
    public void processBroadcastElement(RuleMeta ruleMeta, KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String>.Context ctx, Collector<String> out) throws Exception {

        log.info("收到一条新注入的规则定义信息： {}",ruleMeta);

        int ruleId = ruleMeta.getId();
        int modelId = ruleMeta.getModel_id();

        // 将元数据中获取到的规则id和模型id，填充到规则参数json中
        String param_json = ruleMeta.getParam_json();
        JSONObject jsonObject = JSON.parseObject(param_json);
        jsonObject.put("ruleId",ruleId);
        jsonObject.put("modelId",modelId);
        param_json = jsonObject.toJSONString();


        byte[] static_crowd_bytes = ruleMeta.getStatic_crowd();

        // 将静态人群bitmap的序列化结果，反序列化到一个bitmap对象中
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(static_crowd_bytes));

        // 根据规则元数据中的规则模型ID，构造对应模型的具体运算机
        RuleCalculator ruleCalculator = null;
        switch (modelId) {
            case 5:
                ruleCalculator = new RuleModel005Calculator();
                break;
            default:
                break;
        }

        // 初始化运算机
        ruleCalculator.init(getRuntimeContext(),param_json,bitmap);

        // 把运算机放入运算机池(HASHMAP)
        //ruleCalculators.put(ruleId,ruleCalculator);


        // ！！！ 在processBroadCast方法中，压根不能使用  keyedState
        //calculatorMapState.put(ruleId,ruleCalculator);

        // 既然此路不通，我们可以选择换用  broadcastState
        ruleMeta.setRuleCalculator(ruleCalculator); // 让ruleMeta对象携带上构造好的运算
        MapStateDescriptor<Integer, RuleMeta> mapStateDescriptor = new MapStateDescriptor<>("ruleMetaState", Integer.class, RuleMeta.class);
        BroadcastState<Integer, RuleMeta> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 然后将携带了运算机的ruleMeta对象放入广播状态
        broadcastState.put(ruleId,ruleMeta);

    }
}
