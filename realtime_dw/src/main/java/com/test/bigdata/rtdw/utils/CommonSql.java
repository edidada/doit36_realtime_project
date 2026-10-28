package com.test.bigdata.rtdw.utils;
/**
 *   用来封装一些需要被多次重复使用的sql语句
 **/
public class CommonSql {

    public static final String KAFKA_WIDE_LOG_DDL =
            " CREATE TABLE mall_events_wide_source(                        "
            + "     user_id           INT,                            "
            + "     username          string,                         "
            + "     session_id        string,                         "
            + "     event_id          string,                         "
            + "     event_time        bigint,                         "
            + "     lat               double,                         "
            + "     lng               double,                         "
            + "     release_channel   string,                         "
            + "     device_type       string,                         "
            + "     properties        map<string,string>,             "
            + "     register_phone    STRING,                         "
            + "     user_status       INT,                            "
            + "     register_time     TIMESTAMP(3),                   "
            + "     register_gender   INT,                            "
            + "     register_birthday DATE, register_province STRING, "
            + "     register_city STRING, register_job STRING, register_source_type INT,   "
            + "     gps_province   STRING, gps_city STRING, gps_region STRING,             "
            + "     page_type   STRING, page_service STRING,         "
            + "     pt   as proctime(),                              "
            + "     rt as to_timestamp_ltz(event_time,3) ,           " // 表达式字段（逻辑字段）
            + "     watermark for  rt as rt - interval '0' seconds   " // 声明watermark（也就意味着这个rt字段具备了事件时间语义）
            + " ) WITH (                                             "
            + "  'connector' = 'kafka',                              "
            + "  'topic' = 'dwd-events-log',                     "
            + "  'properties.bootstrap.servers' = '" + RuntimeConfig.sql("KAFKA_BOOTSTRAP_SERVERS", "doitedu:9092") + "',    "
            + "  'properties.group.id' = 'testGroup',                "
            + "  'scan.startup.mode' = 'latest-offset',            "
            + "  'value.format'='json',                              "
            + "  'value.json.fail-on-missing-field'='false',         "
            + "  'value.fields-include' = 'EXCEPT_KEY')              ";


}
