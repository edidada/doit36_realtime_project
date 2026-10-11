package cn.doitedu.rtdw.data_sync;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  业务库的视频信息表，同步到hbase
 **/
public class SyncJob02_VideoInfo2Hbase {
    public static void main(String[] args) {
        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建连接器表，映射  mysql中的视频信息表
        // 挑选一些关心的维度字段：  id,username,phone,status,create_time,gender,province,city,job
        tenv.executeSql("CREATE TABLE cms_video (    " +
                "      id BIGINT,                              " +
                "      video_name STRING,                      " +
                "      video_type STRING,                      " +
                "      video_album STRING,                     " +
                "      video_author STRING,                    " +
                "      video_timelong bigint,                  " +
                "      create_time timestamp(3),               " +
                "      update_time timestamp(3),               " +
                "     PRIMARY KEY (id) NOT ENFORCED            " +
                "     ) WITH (                                 " +
                "     'connector' = 'mysql-cdc',               " +
                "     'hostname' = 'doitedu'   ,               " +
                "     'port' = '3306'          ,               " +
                "     'username' = 'root'      ,               " +
                "     'password' = 'root'      ,               " +
                "     'database-name' = 'realtimedw',          " +
                "     'table-name' = 'cms_video'               " +
                ")");


        // 建连接器表，映射  hbase中的视频维表
        tenv.executeSql("CREATE TABLE hbase_cms_video( " +
                " id BIGINT, " +
                " f ROW<video_name STRING,video_type STRING, video_album STRING, video_author STRING,video_timelong BIGINT, create_time TIMESTAMP(3)>, " +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH (                             " +
                " 'connector' = 'hbase-2.2',          " +
                " 'table-name' = 'dim_video_info',     " +
                " 'zookeeper.quorum' = 'doitedu:2181' " +
                ")");


        // 写一个sql，插入数据
        tenv.executeSql("insert into hbase_cms_video select id,row(video_name,video_type,video_album,video_author,video_timelong,create_time) from cms_video");
    }
}
