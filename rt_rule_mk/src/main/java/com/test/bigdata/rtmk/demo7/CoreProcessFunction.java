package com.test.bigdata.rtmk.demo7;

import com.test.bigdata.rtmk.beans.EventBean;
import com.test.bigdata.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.HashMap;

@Slf4j
public class CoreProcessFunction extends KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String> {

    // 定义一个运算机池
    HashMap<Integer, RuleCalculator> ruleCalculators = new HashMap<>();

    @Override
    public void processElement(EventBean eventBean, KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 遍历运算机池，将本次接收到的行为事件，交给每个运算机去处理
        for (RuleCalculator calculator : ruleCalculators.values()) {
            calculator.calc(eventBean,out);
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

        // 把运算机放入运算机池
        ruleCalculators.put(ruleId,ruleCalculator);
    }
}
