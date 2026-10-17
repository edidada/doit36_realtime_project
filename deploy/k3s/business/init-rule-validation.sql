CREATE DATABASE IF NOT EXISTS doit36_main_audit;
CREATE TABLE IF NOT EXISTS doit36_main_audit.rule_meta_doit36 (
 id INT PRIMARY KEY, model_id INT, param_json TEXT, static_crowd MEDIUMBLOB,
 status INT, publisher VARCHAR(255), create_time DATETIME(3), update_time DATETIME(3)
);
INSERT INTO doit36_main_audit.rule_meta_doit36 VALUES
(900017,5,'{"ruleId":900017,"modelId":5,"triggerEventId":"item_share","dynamicProfile":[{"tagEvent":{"tagEventId":"item_collect","tagEventProp":"item_id","tagEventValues":["p02"]},"tagCompareOp":">","tagCompareValue":0,"startTime":"2026-01-01 00:00:00","endTime":"2027-01-01 00:00:00","conditionId":"mainAudit"}]}',UNHEX('3a30000000000000'),1,'main-audit',NOW(3),NOW(3))
ON DUPLICATE KEY UPDATE param_json=VALUES(param_json),status=VALUES(status),update_time=NOW(3);
