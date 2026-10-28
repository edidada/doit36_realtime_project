package com.test.bigdata.rtmk.demo9;

import com.test.bigdata.rtmk.beans.EventBean;
import com.test.bigdata.rtmk.demo5.RuleCalculator;
import groovy.lang.GroovyClassLoader;
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
    private transient Map<Integer, RuleCalculator> calculators;
    private transient Map<Integer, GroovyClassLoader> loaders;

    @Override
    public void open(Configuration parameters) {
        calculators = new HashMap<>();
        loaders = new HashMap<>();
    }

    @Override
    public void processElement(EventBean event, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        for (Map.Entry<Integer, RuleMeta> entry : ctx.getBroadcastState(RULES).immutableEntries()) {
            RuleMeta rule = entry.getValue();
            RuleCalculator calculator = calculators.get(entry.getKey());
            if (calculator == null) {
                RoaringBitmap crowd = new RoaringBitmap();
                if (rule.getStatic_crowd() != null && rule.getStatic_crowd().length > 0) {
                    crowd.deserialize(ByteBuffer.wrap(rule.getStatic_crowd()));
                }
                if (rule.getModel_calculator_code() == null || rule.getModel_calculator_code().trim().isEmpty()) {
                    throw new IllegalArgumentException("Missing Groovy calculator code for rule " + rule.getId());
                }
                GroovyClassLoader loader = new GroovyClassLoader(getClass().getClassLoader());
                try {
                    Class<?> type = loader.parseClass(rule.getModel_calculator_code());
                    calculator = (RuleCalculator) type.getDeclaredConstructor().newInstance();
                    loaders.put(entry.getKey(), loader);
                } catch (Exception failure) {
                    loader.close();
                    throw failure;
                }
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
        RuleCalculator previous = calculators.remove(rule.getId());
        if (previous instanceof AutoCloseable) ((AutoCloseable) previous).close();
        GroovyClassLoader loader = loaders.remove(rule.getId());
        if (loader != null) loader.close();
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
    public void close() throws Exception {
        if (calculators != null) {
            for (RuleCalculator calculator : calculators.values()) {
                if (calculator instanceof AutoCloseable) ((AutoCloseable) calculator).close();
            }
        }
        if (loaders != null) {
            for (GroovyClassLoader loader : loaders.values()) loader.close();
        }
    }
}
