package com.test.bigdata.rtmk.demo5;

import com.test.bigdata.rtmk.beans.EventBean;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Set;

public interface RuleCalculator {

    void init(RuntimeContext runtimeContext,String paramJson, RoaringBitmap staticCrowd) throws IOException;

    void calc(EventBean eventBean, Collector<String> out) throws IOException;
}
