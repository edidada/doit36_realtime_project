package com.test.bigdata.rtmk.demo6;

import com.test.bigdata.rtmk.beans.EventBean;
import com.test.bigdata.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashSet;
import java.util.List;

/**
 * 本模型的，参数模板
 *  {
 *    "ruleId":"r001",
 *    "triggerEventId":"ad_click",
 *    "triggerEventPropName":"ad_id",
 *    "triggerEventPropValue":["ad_01","ad_02","ad_03"]
 * }
 */
public class RuleModel001Calculator implements RuleCalculator {
    HashSet<String> r001TargetItems;
    JSONObject messageJsonObj = new JSONObject();
    JSONObject paramJsonObj;

    @Override
    public void init(RuntimeContext runtimeContext, String paramJson, RoaringBitmap staticCrowd) {

        paramJsonObj = JSON.parseObject(paramJson);

        this.r001TargetItems = new HashSet<>();
        // 从规则参数中，获取触发事件的属性值要求
        List<String> triggerEventPropValue = paramJsonObj.getJSONArray("triggerEventPropValue").toJavaList(String.class);
        r001TargetItems.addAll(triggerEventPropValue);

        messageJsonObj.put("ruleId", paramJsonObj.getString("ruleId"));
    }


    @Override
    public void calc(EventBean eventBean, Collector<String> out) {
        if (eventBean.getEvent_id().equals(paramJsonObj.getString("triggerEventId"))
                && r001TargetItems.contains(eventBean.getProperties().get(paramJsonObj.getString("triggerEventPropName")))) {
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



