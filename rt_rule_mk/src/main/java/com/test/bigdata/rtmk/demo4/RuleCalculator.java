package com.test.bigdata.rtmk.demo4;

import com.test.bigdata.rtmk.beans.EventBean;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.io.IOException;

public interface RuleCalculator {

    void init(RuntimeContext runtimeContext,String paramJson) throws IOException;

    void calc(EventBean eventBean, Collector<String> out) throws IOException;
}
