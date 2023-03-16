package cn.doitedu.rtmk.demo4;

import cn.doitedu.rtmk.beans.EventBean;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2023/3/16
 * @Desc: 学大数据，到多易教育
 * 规则模型3的参数模板：
 *
 **/
public class RuleModel003Calculator implements RuleCalculator {


    @Override
    public void init(RuntimeContext runtimeContext, String paramJson) throws IOException {



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


    }

    // 触发事件处理逻辑
    private void trigRule(EventBean eventBean, Collector<String> out) throws IOException {


    }

    // 画像事件处理逻辑
    private void dynamicProfileProcess(EventBean eventBean) throws IOException {



    }


}














