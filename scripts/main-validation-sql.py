#!/usr/bin/env python3
"""Print isolated MySQL fixtures using the repository's actual Groovy rule."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
for name in ('init-rule-validation.sql', 'init-main-validation.sql'):
    print((root / 'deploy/k3s/business' / name).read_text())
code = (root / 'rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/groovy/RuleMode005Calculator.groovy').read_bytes().hex()
print("UPDATE doit36_main_audit.rule_meta_doit36 SET model_calculator_code="
      "CONVERT(UNHEX('" + code + "') USING utf8mb4) WHERE id=900017;")
print("""CREATE DATABASE IF NOT EXISTS doit36_main_audit_demo6;
CREATE TABLE IF NOT EXISTS doit36_main_audit_demo6.rule_meta_doit36
LIKE doit36_main_audit.rule_meta_doit36;
INSERT INTO doit36_main_audit_demo6.rule_meta_doit36
(id,model_id,param_json,static_crowd,status,publisher,create_time,update_time) VALUES
(900016,1,'{"ruleId":900016,"modelId":1,"triggerEventId":"ad_click","triggerEventPropName":"ad_id","triggerEventPropValue":["ad_01"]}',UNHEX('3a30000000000000'),1,'main-audit',NOW(3),NOW(3))
ON DUPLICATE KEY UPDATE param_json=VALUES(param_json),model_id=1,status=1;
""")
