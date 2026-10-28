package com.test.bigdata.rtmk.demo6;

import com.test.bigdata.rtmk.utils.RuntimeConfig;
import com.test.bigdata.rtmk.beans.EventBean;
import com.test.bigdata.rtmk.demo5.RuleCalculator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

@Slf4j
public class RuleModel004Calculator implements RuleCalculator {
    ValueState<Integer> seq_cnt_state;
    JSONObject jsonParamObj;
    Table tagTable;
    JSONObject resultJsonObj;
    RoaringBitmap staticCrowd;

    @Override
    public void init(RuntimeContext runtimeContext, String paramJson, RoaringBitmap staticCrowd) throws IOException {

        this.staticCrowd = staticCrowd;

        seq_cnt_state = runtimeContext.getState(new ValueStateDescriptor<Integer>("seq_cnt_state", Integer.class));
        jsonParamObj = JSON.parseObject(paramJson);

        // 用于输出消息的jsonObject
        resultJsonObj = new JSONObject();
        resultJsonObj.put("ruleId", jsonParamObj.getString("ruleId"));
        resultJsonObj.put("ruleModelId", jsonParamObj.getString("ruleModelId"));

        // 构造hbase的查询客户端
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", RuntimeConfig.value("HBASE_ZOOKEEPER_QUORUM", "doitedu:2181"));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        tagTable = hbaseConn.getTable(TableName.valueOf("user_profile_tags"));


    }

    @Override
    public void calc(EventBean eventBean, Collector<String> out) throws IOException {

        if(staticCrowd.contains(eventBean.getUser_id())) {

            // 取触发事件的参数
            JSONObject triggerEvent = jsonParamObj.getJSONObject("triggerEvent");
            String triggerEventId = triggerEvent.getString("eventId");
            String triggerPropName = triggerEvent.getString("propName");
            String triggerPropCompareValue = triggerEvent.getString("compareValue");

            if (eventBean.getEvent_id().equals(triggerEventId) &&
                    eventBean.getProperties().get(triggerPropName).equals(triggerPropCompareValue)
            ) {
                triggerProcess(eventBean, out);
            } else {
                dynamicProfileProcess(eventBean);
            }
        }
    }


    /**
     * 触发事件处理，只要判断该用户的动态画像条件和静态画像条件是否已经满足
     *
     * @param eventBean
     * @param out
     */
    private void triggerProcess(EventBean eventBean, Collector<String> out) throws IOException {

        log.info("触发处理被调用,触发人:{},触发事件id:{}",eventBean.getUser_id(),eventBean.getEvent_id());

        JSONObject dynamicProfile = jsonParamObj.getJSONObject("dynamicProfile");
        int eventSeqSize = dynamicProfile.getJSONArray("eventSeq").size();
        Integer minCount = dynamicProfile.getInteger("seqActMinCount");


        // 查询动态画像的统计值
        int seqRealCount = seq_cnt_state.value() == null?0:seq_cnt_state.value();

        if ( seqRealCount/eventSeqSize >= minCount ) {
            resultJsonObj.put("userId", eventBean.getUser_id());
            resultJsonObj.put("hitTime", eventBean.getEvent_time());

            out.collect(resultJsonObj.toJSONString());
        }
    }

    /**
     * 动态画像，需要实时统计计算
     *
     * @param eventBean
     * @throws IOException
     */
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {

        Integer stateOldValue = seq_cnt_state.value();
        if (stateOldValue == null) stateOldValue = 0;

        JSONObject dynamicProfile = jsonParamObj.getJSONObject("dynamicProfile");
        JSONArray eventSeq = dynamicProfile.getJSONArray("eventSeq");
        int seqSize = eventSeq.size();

        // 判断本次行为，是否是第N个序列条件事件
        JSONObject nSeqEvent = eventSeq.getJSONObject(stateOldValue % seqSize);
        if (eventBean.getEvent_id().equals(nSeqEvent.getString("eventId"))) {

            String compareOp = nSeqEvent.getString("compareOp");
            String compareValue = nSeqEvent.getString("compareValue");
            boolean match = false;
            switch (compareOp) {
                case ">":
                    if (eventBean.getProperties().get(nSeqEvent.getString("propName")).compareTo(compareValue) > 0)
                        match = true;
                    break;
                case "<":
                    if (eventBean.getProperties().get(nSeqEvent.getString("propName")).compareTo(compareValue) < 0)
                        match = true;
                    break;
                case "=":
                    if (eventBean.getProperties().get(nSeqEvent.getString("propName")).compareTo(compareValue) == 0)
                        match = true;
                    break;
                default:
                    break;
            }

            if (match) seq_cnt_state.update(stateOldValue + 1);

        }


    }



}
