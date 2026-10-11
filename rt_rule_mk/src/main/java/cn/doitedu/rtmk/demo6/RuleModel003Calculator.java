package cn.doitedu.rtmk.demo6;

import cn.doitedu.rtmk.beans.EventBean;
import cn.doitedu.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.List;

/**
 * 规则模型3的参数模板：
{
"ruleId":"r004",
"ruleModelId":"m003",
"triggerEvent":{
"eventId":"ad_click",
"eventPropName":"url",
"eventPropCompareOp":"in",
"eventPropValue":["/page008","/page009"]
},
"dynamicProfile":[
{
"tagEvent":{
"eventId":"add_cart",
"eventPropName":"item_price",
"eventPropValueMaxMin":2
}
}
]
}
 *
 **/
public class RuleModel003Calculator implements RuleCalculator {

    ValueState<Double> maxPriceState;
    ValueState<Double> minPriceState;
    JSONObject paramJsonObj;
    JSONObject resultJsonObj;


    @Override
    public void init(RuntimeContext runtimeContext, String paramJson, RoaringBitmap staticCrowd) throws IOException {

        maxPriceState = runtimeContext.getState(new ValueStateDescriptor<Double>("max_price", Double.class));
        minPriceState = runtimeContext.getState(new ValueStateDescriptor<Double>("min_price", Double.class));

        paramJsonObj = JSON.parseObject(paramJson);

        resultJsonObj = new JSONObject();
        resultJsonObj.put("ruleId", paramJsonObj.getString("ruleId"));
        resultJsonObj.put("ruleModelId", paramJsonObj.getString("ruleModelId"));

    }

    /**
     * 规则运算机的核心逻辑处理入口方法
     *
     * @param eventBean
     * @param out
     * @throws IOException
     */
    @Override
    public void calc(EventBean eventBean, Collector<String> out) throws IOException {


        JSONObject triggerEvent = paramJsonObj.getJSONObject("triggerEvent");
        String triggerEventId = triggerEvent.getString("eventId");
        String triggerEventPropName = triggerEvent.getString("eventPropName");
        String eventPropCompareOp = triggerEvent.getString("eventPropCompareOp");
        List<String> eventPropValues = triggerEvent.getJSONArray("eventPropValue").toJavaList(String.class);

        // 判断当前事件是否是  触发条件 事件
        if(triggerEventId.equals(eventBean.getEvent_id())){
            if(eventPropCompareOp.equals("in")  && eventPropValues.contains(eventBean.getProperties().get(triggerEventPropName))){
                trigRule(eventBean,out);
            } else if (eventPropCompareOp.equals("=") && eventBean.getProperties().get(triggerEventPropName).equals(eventPropValues.get(0))) {
                trigRule(eventBean,out);
            }
        }

        // 判断当前事件是否是  画像条件 事件
        JSONObject dynamicProfile = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);
        JSONObject tagEvent = dynamicProfile.getJSONObject("tagEvent");
        String tagEventId = tagEvent.getString("eventId");

        if(eventBean.getEvent_id().equals(tagEventId)){
            dynamicProfileProcess(eventBean);
        }


    }

    // 触发事件处理逻辑
    private void trigRule(EventBean eventBean, Collector<String> out) throws IOException {
        // 取出参数中的  最大值 比  最小值的  要求倍数
        JSONObject dynamicProfile = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);
        JSONObject tagEvent = dynamicProfile.getJSONObject("tagEvent");
        int eventPropValueMaxMin  = tagEvent.getInteger("eventPropValueMaxMin");

        // 去状态中查询是否满足参数中要求的倍数
        Double stateMax = maxPriceState.value();
        Double stateMin = minPriceState.value();
        if(stateMax !=null && stateMin !=null && stateMax/stateMin>eventPropValueMaxMin){

            resultJsonObj.put("userId",eventBean.getUser_id());
            resultJsonObj.put("hitTime",eventBean.getEvent_time());

            out.collect(resultJsonObj.toJSONString());
        }
    }

    // 画像事件处理逻辑
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {
        JSONObject dynamicProfile = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);
        JSONObject tagEvent = dynamicProfile.getJSONObject("tagEvent");
        String tagEventPropName = tagEvent.getString("eventPropName");

        Double oldMax = maxPriceState.value();
        Double oldMin = minPriceState.value();

        double currentPrice = Double.parseDouble(eventBean.getProperties().get(tagEventPropName));

        if(oldMax == null || currentPrice > oldMax){
           maxPriceState.update(currentPrice);
        }

        if(oldMin == null || currentPrice < oldMin){
            minPriceState.update(currentPrice);
        }


    }


}














