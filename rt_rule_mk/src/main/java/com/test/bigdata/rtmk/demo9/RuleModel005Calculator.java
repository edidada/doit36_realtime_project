package com.test.bigdata.rtmk.demo9;

import com.test.bigdata.rtmk.beans.EventBean;
import com.test.bigdata.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;


public class RuleModel005Calculator implements RuleCalculator {
    ValueState<Integer> valueState;
    RoaringBitmap staticCrowd;
    JSONObject paramJsonObj;
    Jedis jedis;
    Integer ruleId;
    JSONObject resMessage;

    @Override
    public void init(RuntimeContext runtimeContext, String paramJson, RoaringBitmap staticCrowd) throws IOException {

        this.staticCrowd = staticCrowd;

        paramJsonObj = JSON.parseObject(paramJson);
        ruleId = paramJsonObj.getInteger("ruleId");

        valueState = runtimeContext.getState(new ValueStateDescriptor<>("value", Integer.class));

        jedis = new Jedis("doitedu", 6379);

        resMessage = new JSONObject();
        resMessage.put("ruleId",ruleId);


    }

    /**
     * 规则运算机的核心逻辑处理方法
     *
     * @param eventBean
     * @param out
     * @throws IOException
     */
    @Override
    public void calc(EventBean eventBean, Collector<String> out) throws IOException {
        JSONObject dynamicProfileObj = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);

        /**
         * 获取触发事件  条件 参数
         */
        String triggerEventId = paramJsonObj.getString("triggerEventId");


        /**
         * 获取动态画像条件 事件参数
         */
        JSONObject tagEvent = dynamicProfileObj.getJSONObject("tagEvent");

        // 获取画像事件ID
        String tagEventId = tagEvent.getString("tagEventId");
        // 获取画像事件属性名
        String tagEventPropName = tagEvent.getString("tagEventProp");
        // 获取画像事件属性值
        List<String> tagEventValues = tagEvent.getJSONArray("tagEventValues").toJavaList(String.class);


        // 判断当前事件是： 触发事件
        if(eventBean.getEvent_id().equals(triggerEventId)){
            trigRule(eventBean,out);
        }

        // 判断当前事件是：画像事件
        if(eventBean.getEvent_id().equals(tagEventId)){
            if(tagEventValues.contains( eventBean.getProperties().get(tagEventPropName)   )){
                dynamicProfileProcess(eventBean);
            }
        }

    }

    private void trigRule(EventBean eventBean, Collector<String> out) throws IOException {

        JSONObject dynamicProfileObj = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);
        String tagCompareOp = dynamicProfileObj.getString("tagCompareOp");
        int tagCompareValue = dynamicProfileObj.getInteger("tagCompareValue");
        String conditionId = dynamicProfileObj.getString("conditionId");

        if(valueState.value() == null) {
            // redis中的规则条件历史值的数据结构：  KEY[规则ID]   => { FIELD[条件ID:用户ID] ,  VALUE[历史值] }
            String historyValueStr = jedis.hget(ruleId + "", conditionId + ":" + eventBean.getUser_id());
            // 万一从redis中没有取到数据，则将历史值赋值为 0
            int historyValue = historyValueStr == null ? 0 : Integer.parseInt(historyValueStr);

            valueState.update(historyValue);
        }


        // 判断该用户是否满足了规则中的所有条件
        if(valueState.value() > tagCompareValue ){

            // 输出 命中结果
            resMessage.put("userId",eventBean.getUser_id());
            resMessage.put("hitTime",eventBean.getEvent_time());

            out.collect(resMessage.toJSONString());
        }


    }

    // 动态画像条件统计运算
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {

        /**
         * 从参数json中获取各类参数值
         */
        // 获取动态画像条件 参数对象
        JSONObject dynamicProfileObj = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);

        // 获取动态画像条件的 条件ID
        String conditionId = dynamicProfileObj.getString("conditionId");

        // 获取画像事件的统计时间窗口
        String startTime = dynamicProfileObj.getString("startTime");
        String endTime = dynamicProfileObj.getString("endTime");


        /* *
         * 动态画像统计逻辑
         */
        // 如果是第一次对本条件进行统计，则要去redis中查询历史值
        if(valueState.value() == null) {
            // redis中的规则条件历史值的数据结构：  KEY[规则ID]   => { FIELD[条件ID:用户ID] ,  VALUE[历史值] }
            String historyValueStr = jedis.hget(ruleId + "", conditionId + ":" + eventBean.getUser_id());
            // 万一从redis中没有取到数据，则将历史值赋值为 0
            int historyValue = historyValueStr == null ? 0 : Integer.parseInt(historyValueStr);

            valueState.update(historyValue + 1);
        }else{
            valueState.update(valueState.value()+1);
        }


    }


}














