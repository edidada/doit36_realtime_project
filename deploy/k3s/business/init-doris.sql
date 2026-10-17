CREATE DATABASE IF NOT EXISTS dwd;
CREATE TABLE IF NOT EXISTS dwd.mall_events_wide (
  gps_province VARCHAR(16), gps_city VARCHAR(16), gps_region VARCHAR(16),
  dt DATE, user_id BIGINT, username VARCHAR(20), session_id VARCHAR(20),
  event_id VARCHAR(10), event_time BIGINT, lat DOUBLE, lng DOUBLE,
  release_channel VARCHAR(20), device_type VARCHAR(20), properties VARCHAR(40),
  register_phone VARCHAR(20), user_status INT, register_time DATETIME,
  register_gender INT, register_birthday DATE, register_province VARCHAR(20),
  register_city VARCHAR(20), register_job VARCHAR(20), register_source_type INT,
  page_type VARCHAR(20), page_service VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(gps_province, gps_city, gps_region, dt, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES ("replication_num"="1");
