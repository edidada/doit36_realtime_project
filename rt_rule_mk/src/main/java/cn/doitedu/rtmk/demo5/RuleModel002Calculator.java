package cn.doitedu.rtmk.demo5;

import cn.doitedu.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.List;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/16
 * @Desc: 学大数据，到多易教育
 * 规则模型2的参数模板：
 * {
 * "ruleId":"r002",
 * "triggerEventId":"item_share",
 * "dynamicProfile":[
 * {
 * "tagEvent":{
 * "tagEventId":"item_collect"
 * "tagEventProp":"item_id",
 * "tagEventValues":["p02","p04","p08"],
 * }
 * "tagCompareOp":">",
 * "tagCompareValue":4
 * }
 * ]
 * }
 **/
public class RuleModel002Calculator implements RuleCalculator {
    JSONObject messageJsonObj = new JSONObject();
    ValueState<Integer> r002_c001_state;
    JSONObject paramJsonObj;

    @Override
    public void init(RuntimeContext runtimeContext, String paramJson, RoaringBitmap staticCrowd) throws IOException {

        // 解析规则参数为一个json对象
        paramJsonObj = JSONObject.parseObject(paramJson);

        // 开辟一个状态，用于动态画像统计
        r002_c001_state = runtimeContext.getState(new ValueStateDescriptor<Integer>("r002-c001-state", Integer.class));
        r002_c001_state.update(0);

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

        String eventId = eventBean.getEvent_id();
        String triggerEventId = paramJsonObj.getString("triggerEventId");

        /**
         * 一、判断本次行为事件是否是本规则的触发事件
         */
        if (eventId.equals(triggerEventId)) {
            // 调用触发逻辑
            trigRule(eventBean, out);
        }

        /**
         * 二、否则，判断本次行为事件，是否是规则中要做动态画像统计的事件
         */
        JSONArray dynamicProfileArray = paramJsonObj.getJSONArray("dynamicProfile");
        JSONObject dynamicProfileObject = dynamicProfileArray.getJSONObject(0);

        JSONObject tagEventObject = dynamicProfileObject.getJSONObject("tagEvent");
        String tagEventId = tagEventObject.getString("tagEventId");
        String tagEventProp = tagEventObject.getString("tagEventProp");
        List<String> tagEventPropValues = tagEventObject.getJSONArray("tagEventValues").toJavaList(String.class);

        if (eventId.equals(tagEventId)
                && tagEventPropValues.contains(eventBean.getProperties().get(tagEventProp))
        ) {
            // 如果是，则去做画像统计
            dynamicProfileProcess(eventBean);
        }
    }

    private void trigRule(EventBean eventBean, Collector<String> out) throws IOException {
        // 查询该用户的动态画像的标签统计值
        Integer currentTagValue = r002_c001_state.value();
        if(currentTagValue == null) currentTagValue=0;

        // 判断统计值是否已满足规则参数中的要求
        JSONObject dynamicProfile = paramJsonObj.getJSONArray("dynamicProfile").getJSONObject(0);
        String tagCompareOp = dynamicProfile.getString("tagCompareOp");
        Integer tagCompareValue = dynamicProfile.getInteger("tagCompareValue");

        boolean isHit = false;
        if(">".equals(tagCompareOp)){
            isHit = currentTagValue > tagCompareValue;
        }else if ("<".equals(tagCompareOp)){
            isHit = currentTagValue < tagCompareValue;
        }else if ("=".equals(tagCompareOp)){
            isHit = currentTagValue == tagCompareValue;
        }

        /**
         * 如果命中，则输出消息
         */
        if(isHit){

            messageJsonObj.put("userId",eventBean.getUser_id());
            messageJsonObj.put("hitTime",eventBean.getEvent_time());
            messageJsonObj.put("ruleId",paramJsonObj.getString("ruleId"));

            out.collect(messageJsonObj.toJSONString());
        }
    }

    // 动态画像条件统计运算
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {
        // 这里是在为规则 r002中的画像条件进行实时动态统计
        Integer oldValue = r002_c001_state.value();
        if(oldValue == null ) oldValue = 0;
        r002_c001_state.update(oldValue+1);
    }


}














