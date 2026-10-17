#!/usr/bin/env python3
"""Initialize dedicated acceptance services. Run on master from uploaded script directory."""
import base64
import json
import secrets
import subprocess
import time
from pathlib import Path

K = ['kubectl', '-n', 'doit']
def run(args, data=None):
    return subprocess.run(K + args, input=data, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True, timeout=180).stdout

def mysql(sql):
    return run(['exec', '-i', 'deployment/business-doris-fe', '--', 'mysql',
                '--protocol=TCP', '-h127.0.0.1', '-P9030', '-uroot', '-N', '-B'], sql)

for name in ['business-kafka', 'business-hbase', 'business-doris-fe', 'business-doris-be']:
    run(['rollout', 'status', 'deployment/' + name, '--timeout=150s'])
for topic in ['mall-events-log', 'dwd-events-log']:
    run(['exec', 'deployment/business-kafka', '--', '/opt/kafka/bin/kafka-topics.sh',
         '--bootstrap-server', '192.168.31.211:9092', '--create', '--if-not-exists',
         '--topic', topic, '--partitions', '1', '--replication-factor', '1'])
run(['exec', '-i', 'deployment/business-hbase', '--', '/opt/hbase/bin/hbase', '--config',
     '/etc/hbase', 'shell', '-n'], Path('init-hbase.rb').read_text())
backends = mysql('SHOW BACKENDS;')
if not any(line.split('\t')[1:2] == ['192.168.31.251'] for line in backends.splitlines()):
    mysql('ALTER SYSTEM ADD BACKEND "192.168.31.251:9050";')
for _ in range(30):
    rows = [line.split('\t') for line in mysql('SHOW BACKENDS;').splitlines()]
    if any(len(r) > 9 and r[1] == '192.168.31.251' and r[9] == 'true' for r in rows):
        break
    time.sleep(3)
else:
    raise RuntimeError('Doris BE did not become Alive')
mysql(Path('init-doris.sql').read_text())
raw = run(['get', 'secret', 'business-doris-auth', '--ignore-not-found', '-o', 'json']).strip()
if raw:
    password = base64.b64decode(json.loads(raw)['data']['password']).decode()
else:
    password = secrets.token_hex(20)
    secret = dict(apiVersion='v1', kind='Secret', metadata=dict(name='business-doris-auth',namespace='doit'),
                  stringData=dict(password=password))
    run(['apply', '-f', '-'], json.dumps(secret))
escaped = password.replace('\\', '\\\\').replace("'", "''")
mysql("CREATE USER IF NOT EXISTS 'doit_etl' IDENTIFIED BY '" + escaped + "';\n"
      "GRANT SELECT_PRIV, LOAD_PRIV ON dwd.* TO 'doit_etl';\n")
print('Kafka topics, typed HBase dimensions, Doris schema and dedicated credentials initialized.')
