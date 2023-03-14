package cn.doitedu.rtmk.demo3;

import cn.doitedu.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;

public class Rule001Calculator implements RuleCalculator{
    HashSet<String> r001TargetItems;
    JSONObject messageJsonObj = new JSONObject();

    @Override
    public void init(RuntimeContext runtimeContext) {
        this.r001TargetItems = new HashSet<>();
        r001TargetItems.addAll(Arrays.asList("p01", "p03", "p05"));
        messageJsonObj.put("ruleId", "r001");
    }


    @Override
    public void calc(EventBean eventBean, Collector<String> out) {
        if (eventBean.getEvent_id().equals("add_cart")
                && r001TargetItems.contains(eventBean.getProperties().get("item_id"))) {
            trigRule(eventBean, out);
        }

    }


    private void trigRule(EventBean eventBean, Collector<String> out) {
        // 如果某用户实施了添加购物车行为，且添加到购物车的商品属于 [p01,p03,p05]中的一种

        messageJsonObj.put("userId", eventBean.getUser_id());
        messageJsonObj.put("matchTime", eventBean.getEvent_time());

        out.collect(messageJsonObj.toJSONString());
    }
}



