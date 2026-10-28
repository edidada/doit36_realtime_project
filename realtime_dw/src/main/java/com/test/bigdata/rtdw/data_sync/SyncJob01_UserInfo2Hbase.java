package com.test.bigdata.rtdw.data_sync;

import com.test.bigdata.rtdw.utils.RuntimeConfig;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  用户注册信息表 -> 同步 -> hbase
 **/
public class SyncJob01_UserInfo2Hbase {

    public static void main(String[] args) {

        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        /**
         * 监听MySQL的binlog，得到 用户注册信息表数据，然后写入  hbase
         */

        // 建连接器表，映射  mysql中的 用户注册信息表<ums_member>
        // 挑选一些关心的维度字段：  id,username,phone,status,create_time,gender,province,city,job
        tenv.executeSql("CREATE TABLE ums_member_source (    " +
                "      id BIGINT,                                   " +
                "      username STRING,                             " +
                "      phone STRING,                                " +
                "      status int,                                  " +
                "      create_time timestamp(3),                    " +
                "      gender int,                                  " +
                "      birthday date,                               " +
                "      province STRING,                             " +
                "      city STRING,                                 " +
                "      job STRING ,                                 " +
                "      source_type INT ,                            " +
                "     PRIMARY KEY (id) NOT ENFORCED            " +
                "     ) WITH (                                 " +
                "     'connector' = 'mysql-cdc',               " +
                "     'hostname' = '" + RuntimeConfig.sql("MYSQL_HOST", "doitedu") + "'    ,               " +
                "     'port' = '" + RuntimeConfig.port("MYSQL_PORT", 3306) + "'           ,               " +
                "     'username' = '" + RuntimeConfig.sql("MYSQL_USERNAME", "root") + "'       ,               " +
                "     'password' = '" + RuntimeConfig.sql("MYSQL_PASSWORD", "root") + "'       ,               " +
                "     'database-name' = '" + RuntimeConfig.sql("MYSQL_DATABASE", "realtimedw") + "',          " +
                "     'table-name' = 'ums_member'              " +
                ")");


        // 建连接器表，映射  hbase中的 用户信息维表<dim_user_info>
        tenv.executeSql("CREATE TABLE ums_member_hbasesink( " +
                " username STRING, " +
                " f ROW<id BIGINT,phone STRING, status INT, create_time TIMESTAMP(3), gender INT, birthday DATE, province STRING, city STRING, job STRING, source_type INT>, " +
                " PRIMARY KEY (username) NOT ENFORCED " +
                ") WITH (                             " +
                " 'connector' = 'hbase-2.2',          " +
                " 'table-name' = 'dim_user_info',     " +
                " 'zookeeper.quorum' = '" + RuntimeConfig.sql("HBASE_ZOOKEEPER_QUORUM", "doitedu:2181") + "' " +
                ")");

        // 写一个insert ... select  from ..  的 sql
        tenv.executeSql("insert into ums_member_hbasesink select username,row(id,phone,status,create_time,gender,birthday,province,city,job,source_type) as f from ums_member_source");
    }
}
