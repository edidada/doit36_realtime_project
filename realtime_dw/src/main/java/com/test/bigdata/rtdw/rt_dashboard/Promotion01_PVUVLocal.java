package com.test.bigdata.rtdw.rt_dashboard;

import com.test.bigdata.rtdw.beans.EventBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.util.HashMap;

/**
 *
 * 营销活动A/B/C 的活动页（page006,page007,page008）流量实时监控指标计算
 *    计算指标： 每个营销活动页当天累计到当前的pv、uv量，每分钟更新一次
 *
 * 此版本不需要Kafka和MySQL，使用内置数据源进行测试
 **/
public class Promotion01_PVUVLocal {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/tmp/ckpt");
        env.setParallelism(1);

        HashMap<String, String> props1 = new HashMap<>();
        props1.put("url", "/page006");

        HashMap<String, String> props2 = new HashMap<>();
        props2.put("url", "/page007");

        HashMap<String, String> props3 = new HashMap<>();
        props3.put("url", "/page008");

        EventBean event1 = new EventBean(1001, "android", "app", "session001", "page", "/page006", "page_load", System.currentTimeMillis(), props1, "session001", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event2 = new EventBean(1002, "ios", "app", "session002", "page", "/page006", "page_load", System.currentTimeMillis(), props1, "session002", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event3 = new EventBean(1003, "android", "app", "session003", "page", "/page007", "page_load", System.currentTimeMillis(), props2, "session003", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event4 = new EventBean(1004, "ios", "app", "session004", "page", "/page007", "page_load", System.currentTimeMillis(), props2, "session004", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event5 = new EventBean(1005, "android", "app", "session005", "page", "/page008", "page_load", System.currentTimeMillis(), props3, "session005", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event6 = new EventBean(1006, "ios", "app", "session006", "page", "/page008", "page_load", System.currentTimeMillis(), props3, "session006", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event7 = new EventBean(1007, "android", "app", "session007", "page", "/page006", "page_load", System.currentTimeMillis(), props1, "session007", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event8 = new EventBean(1008, "ios", "app", "session008", "page", "/page007", "page_load", System.currentTimeMillis(), props2, "session008", System.currentTimeMillis(), System.currentTimeMillis());
        EventBean event9 = new EventBean(1009, "android", "app", "session009", "page", "/page008", "page_load", System.currentTimeMillis(), props3, "session009", System.currentTimeMillis(), System.currentTimeMillis());

        DataStreamSource<EventBean> source = env.fromElements(
                event1, event2, event3, event4, event5, event6, event7, event8, event9
        );

        source.print("输入事件:");

        source.keyBy(event -> event.getProperties().get("url"))
                .process(new PVUVProcessFunction())
                .print("PVUV统计结果:");

        env.execute("营销活动PVUV实时监控-本地测试版");
    }

    public static class PVUVProcessFunction extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, EventBean, PVUVResult> {
        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void processElement(EventBean event, Context ctx, Collector<PVUVResult> out) throws Exception {
            String pageUrl = event.getProperties().get("url");
            Long userId = (long) event.getUser_id();
            Long timestamp = event.getEvent_time();

            PVUVResult result = new PVUVResult();
            result.setPageUrl(pageUrl);
            result.setUpdateTime(LocalDateTime.now());
            result.setPv(1L);
            result.setUv(userId);

            out.collect(result);
        }
    }

    public static class PVUVResult {
        private String pageUrl;
        private LocalDateTime updateTime;
        private Long pv;
        private Long uv;

        public String getPageUrl() {
            return pageUrl;
        }

        public void setPageUrl(String pageUrl) {
            this.pageUrl = pageUrl;
        }

        public LocalDateTime getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(LocalDateTime updateTime) {
            this.updateTime = updateTime;
        }

        public Long getPv() {
            return pv;
        }

        public void setPv(Long pv) {
            this.pv = pv;
        }

        public Long getUv() {
            return uv;
        }

        public void setUv(Long uv) {
            this.uv = uv;
        }

        @Override
        public String toString() {
            return "PVUVResult{" +
                    "pageUrl='" + pageUrl + '\'' +
                    ", updateTime=" + updateTime +
                    ", pv=" + pv +
                    ", uv=" + uv +
                    '}';
        }
    }
}
