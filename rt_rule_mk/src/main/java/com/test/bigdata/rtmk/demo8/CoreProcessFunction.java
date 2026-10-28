package com.test.bigdata.rtmk.demo8;

import com.test.bigdata.rtmk.beans.EventBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** Checkpoint rule definitions; recreate live clients and keyed state handles on demand. */
@Slf4j
public class CoreProcessFunction extends KeyedBroadcastProcessFunction<Integer, EventBean, RuleMeta, String> {
    private static final MapStateDescriptor<Integer, RuleMeta> RULES =
            new MapStateDescriptor<>("ruleMetaState", Integer.class, RuleMeta.class);
    private transient Map<Integer, RuleModel005Calculator> calculators;

    @Override
    public void open(Configuration parameters) {
        calculators = new HashMap<>();
    }

    @Override
    public void processElement(EventBean event, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        for (Map.Entry<Integer, RuleMeta> entry : ctx.getBroadcastState(RULES).immutableEntries()) {
            RuleMeta rule = entry.getValue();
            RuleModel005Calculator calculator = calculators.get(entry.getKey());
            if (calculator == null) {
                if (rule.getModel_id() != 5) {
                    throw new IllegalArgumentException("Unsupported recovery rule model: " + rule.getModel_id());
                }
                RoaringBitmap crowd = new RoaringBitmap();
                if (rule.getStatic_crowd() != null && rule.getStatic_crowd().length > 0) {
                    crowd.deserialize(ByteBuffer.wrap(rule.getStatic_crowd()));
                }
                calculator = new RuleModel005Calculator();
                // Keyed state must be obtained in the keyed event path, not the broadcast path.
                calculator.init(getRuntimeContext(), rule.getParam_json(), crowd);
                calculators.put(entry.getKey(), calculator);
            }
            calculator.calc(event, out);
        }
    }

    @Override
    public void processBroadcastElement(RuleMeta rule, Context ctx, Collector<String> out) throws Exception {
        log.info("收到一条新注入的规则定义信息： {}", rule);
        RuleModel005Calculator previous = calculators.remove(rule.getId());
        if (previous != null && previous.jedis != null) previous.jedis.close();
        // Ignore UPDATE_BEFORE; the following UPDATE_AFTER contains the new definition.
        if (rule.getOpType() == 1) return;
        if (rule.getOpType() == 3 || rule.getStatus() != 1) {
            ctx.getBroadcastState(RULES).remove(rule.getId());
        } else {
            rule.setRuleCalculator(null);
            ctx.getBroadcastState(RULES).put(rule.getId(), rule);
        }
    }

    @Override
    public void close() {
        if (calculators != null) {
            for (RuleModel005Calculator calculator : calculators.values()) {
                if (calculator.jedis != null) calculator.jedis.close();
            }
        }
    }
}
