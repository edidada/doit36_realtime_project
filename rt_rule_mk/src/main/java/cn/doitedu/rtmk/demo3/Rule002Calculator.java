package cn.doitedu.rtmk.demo3;

import cn.doitedu.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class Rule002Calculator implements RuleCalculator{
    HashSet<String> r002TargetItems;
    JSONObject messageJsonObj = new JSONObject();
    ValueState<Integer> r002_c001_state;

    @Override
    public void init(RuntimeContext runtimeContext) {
        r002TargetItems = new HashSet<>();
        r002TargetItems.addAll(Arrays.asList("p02", "p04", "p08"));

        messageJsonObj.put("ruleId","r002");

        r002_c001_state = runtimeContext.getState(new ValueStateDescriptor<Integer>("r002-c001-state", Integer.class));

    }

    @Override
    public void calc(EventBean eventBean, Collector<String> out) throws IOException {
        if(eventBean.getEvent_id().equals("share")){
            trigRule(eventBean,out);
        }else{
            dynamicProfileProcess(eventBean);
        }
    }

    private void trigRule(EventBean eventBean, Collector<String> out) throws IOException {
        // 如果是触发事件，则去判断该用户是否已经满足了r002中的各种 受众条件
        if((r002_c001_state.value()==null? 0:r002_c001_state.value()) >=4){
            messageJsonObj.put("userId", eventBean.getUser_id());
            messageJsonObj.put("matchTime", eventBean.getEvent_time());

            out.collect(messageJsonObj.toJSONString());
        }

    }

    // 动态画像条件统计运算
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {
        // 这里是在为规则 r002中的画像条件进行实时动态统计
        if(eventBean.getEvent_id().equals("collect") && r002TargetItems.contains(eventBean.getProperties().get("item_id"))){
            // 给r002规则中定义的画像统计条件，进行统计
            r002_c001_state.update((r002_c001_state.value()==null? 0:r002_c001_state.value()) + 1);
        }
    }



}
