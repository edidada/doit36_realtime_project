package cn.doitedu.rtdw.data_etl;

import cn.doitedu.rtdw.utils.RuntimeConfig;
import cn.doitedu.rtdw.beans.EventBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 *  页面访问时长分析etl任务
 **/
public class EtlJob03_PageAccessTimeLong {
    public static void main(String[] args) throws Exception {
        // 构建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 读 kafka中的日志明细数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(RuntimeConfig.value("KAFKA_BOOTSTRAP_SERVERS", "doitedu:9092"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("gpac01"+System.currentTimeMillis())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("dwd-events-log")
                .build();
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s");

        // 将从kafka读取的json日志，转成java bean
        SingleOutputStreamOperator<EventBean> beans = ds.map(s -> JSON.parseObject(s, EventBean.class));

        // 将数据按照相同用户、相同会话进行keyBy
        KeyedStream<EventBean, Tuple2<Integer, String>> keyedStream = beans.keyBy(new KeySelector<EventBean, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> getKey(EventBean eventBean) throws Exception {
                return Tuple2.of(eventBean.getUser_id(), eventBean.getSession_id());
            }
        });
        
        // 核心逻辑
        SingleOutputStreamOperator<EventBean> resultStream = keyedStream.process(new KeyedProcessFunction<Tuple2<Integer, String>, EventBean, EventBean>() {
            ValueState<EventBean> beanState;
            ValueState<Long> timerState;

            @Override
            public void open(Configuration parameters) throws Exception {
                beanState = getRuntimeContext().getState(new ValueStateDescriptor<EventBean>("beanState", EventBean.class));
                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));
            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<Tuple2<Integer, String>, EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {

                // 删掉之前定时器
                context.timerService().deleteProcessingTimeTimer(timerState.value());

                // 并注册新的定时器
                long thisTimerTime = context.timerService().currentProcessingTime()+2*60*60*1000L;
                context.timerService().registerProcessingTimeTimer(thisTimerTime);
                timerState.update(thisTimerTime);


                if (beanState.value() == null || eventBean.getEvent_id().equals("wake_up")) {
                    // 填充结果所需要的各个字段
                    eventBean.setPage_url(eventBean.getProperties().get("url"));
                    eventBean.setSession_cut_id(eventBean.getSession_id() + "-" + eventBean.getEvent_time());
                    eventBean.setPage_start_time(eventBean.getEvent_time());
                    eventBean.setPage_end_time(eventBean.getEvent_time());

                    // 更新到状态中
                    beanState.update(eventBean);
                } else if (eventBean.getEvent_id().equals("page_load")) {
                    // 先更新状态中数据的最新行为时间,对上一个页面做闭合
                    beanState.value().setEvent_time(eventBean.getEvent_time());
                    beanState.value().setPage_end_time(eventBean.getEvent_time());
                    // 并马上输出
                    collector.collect(beanState.value());

                    // 更换掉状态数据中的  url，page_start_time,作为一个新页面的起始
                    beanState.value().setPage_url(eventBean.getProperties().get("url"));
                    beanState.value().setPage_start_time(eventBean.getEvent_time());
                } else {
                    // 其他事件，则只要更新页面结束时间即可
                    beanState.value().setPage_end_time(eventBean.getEvent_time());
                }

                // 输出
                collector.collect(beanState.value());

                // 如果本次事件是一个appclose事件，则在最后清理掉状态
                if(eventBean.getEvent_id().equals("app_close")) {
                    beanState.clear();
                }


            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<Integer, String>, EventBean, EventBean>.OnTimerContext ctx, Collector<EventBean> out) throws Exception {
                beanState.clear();
            }
        });



        // 将流转成表
        tableEnv.createTemporaryView("tmp",resultStream);

        // 创建doris连接器表，来映射doris中接收结果的表
        tableEnv.executeSql(
                " CREATE TABLE tfc_page_timelong(         "
                        +"     dt  DATE                                  "
                        +"     ,user_id            INT                   "
                        +"     ,device_type        VARCHAR(20)           "
                        +"     ,release_channel    VARCHAR(20)           "
                        +"     ,session_id         VARCHAR(20)           "
                        +"     ,session_cut_id     VARCHAR(20)           "
                        +"     ,page_type          VARCHAR(20)           "
                        +"     ,page_url           VARCHAR(100)          "
                        +"     ,page_start_time    BIGINT                "
                        +"     ,page_end_time      BIGINT                "
                        +" ) WITH (                                      "
                        +"    'connector' = 'doris',                     "
                        +"    'fenodes' = '" + RuntimeConfig.sql("DORIS_FENODES", "doitedu:8030") + "',                "
                        +"    'table.identifier' = 'dws.tfc_page_timelong',"
                        +"    'username' = '" + RuntimeConfig.sql("DORIS_USERNAME", "root") + "' ,                       "
                        +"    'password' = '" + RuntimeConfig.sql("DORIS_PASSWORD", "") + "' ,                           "
                        +"    'sink.label-prefix' = 'doris_tl"+System.currentTimeMillis()+"')"
        );


        tableEnv.executeSql(
                "insert into tfc_page_timelong " +
                   "select to_date(date_format(to_timestamp_ltz(page_start_time,3),'yyyy-MM-dd')) as dt," +
                        "user_id,device_type,release_channel,session_id,session_cut_id,page_type,page_url,page_start_time,page_end_time " +
                        "from tmp"
        ).await();

    }
}
