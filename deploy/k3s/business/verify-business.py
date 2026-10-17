#!/usr/bin/env python3
"""Run on the k3s master; verify real enrichment and both sinks with unique events."""
import json
import subprocess
import time
import urllib.request
from pathlib import Path

KUBECTL = ['kubectl', '-n', 'doit']
URL = 'http://192.168.31.88:30816'
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def command(args, data=None, timeout=45, allow_timeout_exit=False):
    result = subprocess.run(KUBECTL + args, input=data, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode and not (allow_timeout_exit and result.returncode == 1):
        raise RuntimeError(result.stderr[-4000:])
    return result.stdout

def api(path):
    with HTTP.open(URL + path, timeout=8) as response:
        return json.load(response)

spec = json.loads(command(['get', 'flinkdeployment', 'realtime-dw-etl-01', '-o', 'json']))
assert spec['spec']['job']['entryClass'].endswith('.EtlJob01_UserEventsLogCommonDim'), 'Wrong entry point'
ready = None
for _ in range(60):
    try:
        jobs = api('/jobs/overview')['jobs']
        ready = next((j for j in jobs if j['state'] == 'RUNNING' and j['tasks']['running'] == j['tasks']['total'] and j['tasks']['total'] > 0), None)
        if ready:
            break
    except (OSError, ValueError):
        pass
    time.sleep(5)
assert ready, 'Business job did not become fully RUNNING'
job_id = ready['jid']
before = api('/jobs/' + job_id + '/checkpoints')['counts']
session = 'a' + str(int(time.time() * 1000))
now_ms = int(time.time() * 1000)
events = [dict(username='accept-user', session_id=session, eventId='pv', eventTime=now_ms + i,
               lat=39.9, lng=116.4, release_channel='acceptance', device_type='web',
               properties={'url': '/mall/item/42'}) for i in range(5)]
command(['exec', '-i', 'deployment/business-kafka', '--', '/opt/kafka/bin/kafka-console-producer.sh',
         '--bootstrap-server', '192.168.31.211:9092', '--topic', 'mall-events-log'],
        ''.join(json.dumps(e) + '\n' for e in events))
raw = command(['exec', 'deployment/business-kafka', '--', '/opt/kafka/bin/kafka-console-consumer.sh',
               '--bootstrap-server', '192.168.31.211:9092', '--topic', 'dwd-events-log',
               '--from-beginning', '--timeout-ms', '20000'], timeout=35, allow_timeout_exit=True)
rows = []
for line in raw.splitlines():
    try:
        row = json.loads(line)
    except ValueError:
        continue
    if row.get('session_id') == session:
        rows.append(row)
assert len(rows) >= 5, ('Kafka output missing', session, rows)
for row in rows:
    assert row['user_id'] == 10001 and row['register_phone'] == '13800000000', row
    assert row['gps_region'] == 'Haidian' and row['page_type'] == 'product' and row['page_service'] == 'mall', row
assert {r['event_time'] for r in rows} == {e['eventTime'] for e in events}, rows
sql = ("SELECT COUNT(*), MIN(user_id), MIN(register_phone), MIN(gps_region), MIN(page_type), "
       "MIN(page_service) FROM dwd.mall_events_wide WHERE session_id='" + session + "'")
doris = None
for _ in range(30):
    doris = command(['exec', 'deployment/business-doris-fe', '--', 'mysql', '--protocol=TCP',
                     '-h127.0.0.1', '-P9030', '-uroot', '-N', '-B', '-e', sql]).strip().split('\t')
    if doris == ['5', '10001', '13800000000', 'Haidian', 'product', 'mall']:
        break
    time.sleep(5)
assert doris == ['5', '10001', '13800000000', 'Haidian', 'product', 'mall'], ('Doris output mismatch', session, doris)
after = api('/jobs/' + job_id + '/checkpoints')['counts']
assert after['completed'] > before['completed'], (before, after)
assert after['failed'] == before['failed'], (before, after)
final_job = next(j for j in api('/jobs/overview')['jobs'] if j['jid'] == job_id)
assert final_job['state'] == 'RUNNING', final_job
result = dict(session_id=session, job_id=job_id, state=final_job['state'], input_events=5,
              kafka_output_events=len(rows), kafka_output_sample=rows[0], doris_verified=doris,
              checkpoints_before=before, checkpoints_after=after)
Path('business-acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
print(json.dumps(result, ensure_ascii=False, indent=2))
