-- Isolated fixtures for the CDC and Spark mains; does not create production databases.
CREATE TABLE IF NOT EXISTS doit36_main_audit.ums_member (
 id BIGINT PRIMARY KEY, username VARCHAR(255), phone VARCHAR(32), status INT,
 create_time DATETIME(3), gender INT, birthday DATE, province VARCHAR(255),
 city VARCHAR(255), job VARCHAR(255), source_type INT
);
INSERT INTO doit36_main_audit.ums_member VALUES (990017,'main-audit-user','13800090017',1,NOW(3),1,'2000-01-01','AuditProvince','AuditCity','tester',1)
ON DUPLICATE KEY UPDATE username=VALUES(username);
CREATE TABLE IF NOT EXISTS doit36_main_audit.cms_video (
 id BIGINT PRIMARY KEY, video_name VARCHAR(255), video_type VARCHAR(255), video_album VARCHAR(255),
 video_author VARCHAR(255), video_timelong BIGINT, create_time DATETIME(3), update_time DATETIME(3)
);
INSERT INTO doit36_main_audit.cms_video VALUES (990017,'main-audit-video','test','test','audit',30,NOW(3),NOW(3))
ON DUPLICATE KEY UPDATE update_time=NOW(3);
CREATE TABLE IF NOT EXISTS doit36_main_audit.t_md_areas (
 ID INT PRIMARY KEY, AREANAME VARCHAR(255), `LEVEL` INT, PARENTID INT, BD09_LAT DOUBLE, BD09_LNG DOUBLE
);
INSERT IGNORE INTO doit36_main_audit.t_md_areas VALUES
 (990001,'AuditProvince',1,0,NULL,NULL),(990002,'AuditCity',2,990001,NULL,NULL),
 (990003,'AuditRegion',3,990002,NULL,NULL),(990004,'AuditStreet',4,990003,31.23,121.47);
CREATE TABLE IF NOT EXISTS doit36_main_audit.dashboard_promotion_abc_pvuv (
 page_url VARCHAR(255) PRIMARY KEY, update_time DATETIME(3), pv BIGINT, uv BIGINT
);
