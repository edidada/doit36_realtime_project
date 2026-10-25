package cn.doitedu.rtdw.data_etl;

import cn.doitedu.rtdw.beans.VideoAggBean;
import cn.doitedu.rtdw.beans.VideoWideBean;
import cn.doitedu.rtdw.utils.CommonSql;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 视频播放事件主题分析，多维轻度聚合计算
 **/
public class EtlJob05_VideoEventsAgg {
    public static void main(String[] args) throws Exception {

        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建kafka连接器表，映射kafka中的行为明细宽表topic
        tenv.executeSql(CommonSql.KAFKA_WIDE_LOG_DDL);

        // 创建hbase连接器表，映射hbase中的视频信息维表
        tenv.executeSql("CREATE TABLE hbase_cms_video( " +
                " id BIGINT, " +
                " f ROW<video_name STRING ,video_type STRING, video_album STRING, video_author STRING,video_timelong BIGINT, create_time TIMESTAMP(3)>, " +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH (                             " +
                " 'connector' = 'hbase-2.2',          " +
                " 'table-name' = 'dim_video_info',     " +
                " 'zookeeper.quorum' = 'doitedu:2181' " +
                ")");

        tenv.executeSql(
                " create temporary view video_events AS  "
                        + " with video_events AS (                                       "
                        + " SELECT                                                       "
                        + "    user_id,                                                  "
                        + "    event_id,                                                 "
                        + "    event_time,                                               "
                        + "    release_channel,                                          "
                        + "    device_type,                                              "
                        + "    cast(properties['video_id'] as bigint) as video_id,       "
                        + "    properties['play_id'] as video_play_id,             "
                        + "    pt                                                        "
                        + " FROM  mall_events_wide_source                                "
                        + " WHERE event_id in ('video_play','video_hb','video_pause'     "
                        + "      ,'video_resume','video_stop')                           "
                        + " )                                                            "
                        + "                                                              "
                        + " SELECT                                                       "
                        + "    e.user_id,                                                "
                        + "    e.event_id,                                               "
                        + "    e.event_time,                                             "
                        + "    e.release_channel,                                        "
                        + "    e.device_type,                                            "
                        + "    e.video_id,                                               "
                        + "    e.video_play_id,                                          "
                        + "    dim.video_type,                                           "
                        + "    dim.video_name,                                           "
                        + "    dim.video_album,                                          "
                        + "    dim.video_author,                                         "
                        + "    dim.video_timelong,                                       "
                        + "    dim.create_time                                           "
                        + " FROM video_events  e                                         "
                        + " LEFT JOIN hbase_cms_video  FOR SYSTEM_TIME AS OF e.pt AS dim "
                        + " ON e.video_id = dim.id                                       "
        );


        // 表转流
        Table table = tenv.from("video_events");
        DataStream<VideoWideBean> dataStream = tenv.toDataStream(table, VideoWideBean.class);


        // 按 (用户id,播放id) 进行 keyBy
        KeyedStream<VideoWideBean, Tuple2<Long, String>> keyedStream = dataStream.keyBy(new KeySelector<VideoWideBean, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> getKey(VideoWideBean bean) throws Exception {
                return Tuple2.of(bean.getUser_id(), bean.getVideo_play_id());
            }
        });

        // 核心逻辑 ： 将视频行为序列，整理成统一的字段格式,尤其是加入了视频播放起始时间字段
        SingleOutputStreamOperator<VideoAggBean> resultStream = keyedStream.process(new KeyedProcessFunction<Tuple2<Long, String>, VideoWideBean, VideoAggBean>() {
            ValueState<VideoAggBean> beanState;

            @Override
            public void open(Configuration parameters) throws Exception {
                beanState = getRuntimeContext().getState(new ValueStateDescriptor<VideoAggBean>("beanState", VideoAggBean.class));
            }

            @Override
            public void processElement(VideoWideBean bean, KeyedProcessFunction<Tuple2<Long, String>, VideoWideBean, VideoAggBean>.Context context, Collector<VideoAggBean> collector) throws Exception {

                // 如果是第一条数据或者是一个播放事件，则将这条数据设置播放时间字段和播放结束时间字段，并放入state
                // 如果是一个resume事件，则更新掉起始时间和结束时间，放入state
                if (beanState.value() == null || bean.getEvent_id().equals("video_play") || bean.getEvent_id().equals("video_resume")) {

                    VideoAggBean aggBean = new VideoAggBean(bean.getUser_id(), bean.getEvent_id(),
                            bean.getEvent_time(), bean.getRelease_channel(),
                            bean.getDevice_type(), bean.getVideo_id(), bean.getVideo_play_id(), bean.getVideo_type(), bean.getVideo_name(),
                            bean.getVideo_album(), bean.getVideo_author(), bean.getVideo_timelong(), bean.getCreate_time(), bean.getEvent_time(), bean.getEvent_time()
                    );
                    beanState.update(aggBean);

                } else {
                    // 如果是其他事件，则更新结束时间
                    beanState.value().setEvent_time(bean.getEvent_time());
                    beanState.value().setPlay_end_time(bean.getEvent_time());
                }

                // 输出state中的数据
                collector.collect(beanState.value());

            }
        });


        //
        tenv.createTemporaryView("res", resultStream,
                Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("event_id", DataTypes.STRING())
                        .column("event_time", DataTypes.BIGINT())
                        .column("release_channel", DataTypes.STRING())
                        .column("device_type", DataTypes.STRING())
                        .column("video_id", DataTypes.BIGINT())
                        .column("video_play_id", DataTypes.STRING())
                        .column("video_type", DataTypes.STRING())
                        .column("video_name", DataTypes.STRING())
                        .column("video_album", DataTypes.STRING())
                        .column("video_author", DataTypes.STRING())
                        .column("video_timelong", DataTypes.BIGINT())
                        .column("create_time", DataTypes.TIMESTAMP(3))
                        .column("play_start_time", DataTypes.BIGINT())
                        .column("play_end_time", DataTypes.BIGINT())
                        // 声明事件时间语义字段
                        .columnByExpression("rt", "to_timestamp_ltz(event_time,3)")
                        // 声明watermark
                        .watermark("rt", "rt - interval '0' second ")
                        .build());

        /*tenv.executeSql("select * from res").print();
        System.exit(1);*/

//       // 创建一个连接器表，映射doris中的聚合结果表
        tenv.executeSql(
                " create table video_events_agg01(    "
                        + "     start_dt         DATE,         "
                        + "     user_id          BIGINT,          "
                        + "     release_channel  VARCHAR(20),  "
                        + "     device_type      VARCHAR(20),  "
                        + "     video_id         BIGINT ,         "
                        + "     video_play_id    VARCHAR(20),  "
                        + "     video_name       VARCHAR(20),  "
                        + "     video_type       VARCHAR(20),  "
                        + "     video_album      VARCHAR(40),  "
                        + "     video_author     VARCHAR(40),  "
                        + "     video_timelong   BIGINT,       "
                        + "     create_time      TIMESTAMP,    "
                        + "     play_start_time  BIGINT ,      "
                        + "     play_end_time    BIGINT        "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'doitedu:8030',         "
                        + "    'table.identifier' = 'dws.video_events_agg01',  "
                        + "    'username' = 'root',                "
                        + "    'password' = '',                    "
                        + "    'sink.label-prefix' = 'doris_tl" + System.currentTimeMillis() + "')"
        );


        // 执行一个insert语句
        tenv.executeSql(
                " insert into video_events_agg01                                                              "
                        + " select                                                                                "
                        + "   to_date(date_format(to_timestamp_ltz(play_start_time,3),'yyyy-MM-dd'))  as start_dt "
                        + "   ,user_id                                                                            "
                        + "   ,release_channel                                                                    "
                        + "   ,device_type                                                                        "
                        + "   ,video_id                                                                           "
                        + "   ,video_play_id                                                                      "
                        + "   ,video_name                                                                         "
                        + "   ,video_type                                                                         "
                        + "   ,video_album                                                                        "
                        + "   ,video_author                                                                       "
                        + "   ,video_timelong                                                                     "
                        + "   ,create_time                                                                        "
                        + "   ,play_start_time                                                                    "
                        + "   ,MAX(play_end_time) AS play_end_time                                                "
                        // 通过滚动时间窗口聚合计算，来减轻写入doris的压力！！！
                        + " from TABLE(                                                                           "
                        + "    TUMBLE(TABLE res, DESCRIPTOR(rt),INTERVAL '10' MINUTE)                             "
                        + " )                                                                                     "
                        + " GROUP BY                                                                              "
                        + "     window_start                                                                      "
                        + " 	,window_end                                                                          "
                        + "     ,user_id                                                                          "
                        + "     ,release_channel                                                                  "
                        + "     ,device_type                                                                      "
                        + "     ,video_id                                                                         "
                        + "     ,video_play_id                                                                    "
                        + "     ,video_name                                                                       "
                        + "     ,video_type                                                                       "
                        + "     ,video_album                                                                      "
                        + "     ,video_author                                                                     "
                        + "     ,video_timelong                                                                   "
                        + "     ,create_time                                                                      "
                        + "     ,play_start_time                                                                  "

        );

       // TODO  补充状态清理的逻辑


        env.execute();
    }
}
