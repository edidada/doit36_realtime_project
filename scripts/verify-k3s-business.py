#!/usr/bin/env python3
"""Bounded live Kafka -> HBase enrichment -> Kafka + Doris acceptance, with isolated session IDs."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
BROKER = 'kafka.middleware.svc.cluster.local:9092'


def command(namespace, args, data=None, timeout=45, accepted=(0,)):
    process = subprocess.run(['kubectl', '-n', namespace] + args, input=data, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if process.returncode not in accepted:
        # Do not print arbitrary middleware output or credentials into evidence logs.
        raise RuntimeError('Kubernetes test command failed: ' + ' '.join(args[:3]))
    return process.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--require-version')
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Refusing to overwrite an earlier acceptance report')
    deployment = json.loads(command('realtime-dw', ['get', 'flinkdeployment', 'realtime-dw-etl-01', '-o', 'json']))
    service = json.loads(command('realtime-dw', ['get', 'service', 'realtime-dw-etl-01-rest', '-o', 'json']))
    base = 'http://' + service['spec']['clusterIP'] + ':8081'

    def api(path):
        with HTTP.open(base + path, timeout=10) as response:
            return json.load(response)

    overview = api('/overview')
    if args.require_version:
        assert overview['flink-version'] == args.require_version, 'Unexpected Flink runtime version'
    jobs = api('/jobs/overview')['jobs']
    job = next(j for j in jobs if j['state'] == 'RUNNING')
    assert job['tasks']['running'] == job['tasks']['total'] > 0, 'Not all operators are running'
    job_id = job['jid']
    before = api('/jobs/' + job_id + '/checkpoints')
    kafka_exec = ['exec', 'deployment/kafka', '--']
    offsets = command('middleware', kafka_exec + ['/opt/kafka/bin/kafka-get-offsets.sh',
                      '--bootstrap-server', BROKER, '--topic', 'dwd-events-log', '--time', '-1'])
    starts = [line.split(':') for line in offsets.splitlines() if line.startswith('dwd-events-log:')]
    assert starts, 'Output topic has no partitions'
    # The existing Doris session_id column is VARCHAR(20); longer probe IDs get truncated.
    session = 'u' + uuid.uuid4().hex[:18]
    now = int(time.time() * 1000)
    events = [dict(username='accept-user', session_id=session, eventId='pv', eventTime=now + index,
                   lat=39.9, lng=116.4, release_channel='upgrade-acceptance', device_type='web',
                   properties={'url': '/mall/item/42'}) for index in range(5)]
    command('middleware', ['exec', '-i', 'deployment/kafka', '--', '/opt/kafka/bin/kafka-console-producer.sh',
                           '--bootstrap-server', BROKER, '--topic', 'mall-events-log'],
            ''.join(json.dumps(event) + '\n' for event in events))
    rows = []
    for _, partition, offset in starts:
        raw = command('middleware', kafka_exec + ['/opt/kafka/bin/kafka-console-consumer.sh',
                      '--bootstrap-server', BROKER, '--topic', 'dwd-events-log', '--partition', partition,
                      '--offset', offset, '--max-messages', '5', '--timeout-ms', '15000'],
                      timeout=30, accepted=(0, 1))
        for line in raw.splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get('session_id') == session:
                rows.append(row)
    assert len(rows) == 5, 'Missing or duplicated Kafka output for acceptance session ' + session
    assert {row['event_time'] for row in rows} == {event['eventTime'] for event in events}
    for row in rows:
        assert (row['user_id'], row['register_phone'], row['gps_region'], row['page_type'], row['page_service']) == (
            10001, '13800000000', 'Haidian', 'product', 'mall'), 'HBase enrichment mismatch'
    sql = ("SELECT COUNT(*), MIN(user_id), MIN(gps_region), MIN(page_type), MIN(page_service) "
           "FROM dwd.mall_events_wide WHERE session_id='" + session + "'")
    doris = []
    for _ in range(30):
        doris = command('middleware', ['exec', 'deployment/business-doris-fe', '--', 'mysql', '--protocol=TCP',
                        '-h127.0.0.1', '-P9030', '-uroot', '-N', '-B', '-e', sql]).strip().split('\t')
        if doris == ['5', '10001', 'Haidian', 'product', 'mall']:
            break
        time.sleep(2)
    assert doris == ['5', '10001', 'Haidian', 'product', 'mall'], ('Doris output mismatch', session, doris)
    after = api('/jobs/' + job_id + '/checkpoints')
    assert after['counts']['completed'] > before['counts']['completed'], 'No new completed checkpoint'
    assert after['counts']['failed'] == before['counts']['failed'], 'New failed checkpoint during acceptance'
    result = dict(session_id=session, job_id=job_id, flink_version=overview['flink-version'],
                  image=deployment['spec']['image'], entry_class=deployment['spec']['job']['entryClass'],
                  input_events=5, kafka_output_events=len(rows), doris_output_events=int(doris[0]),
                  hbase_dimensions_verified=True, checkpoints_before=before['counts'],
                  checkpoints_after=after['counts'], restored=before.get('latest', {}).get('restored'))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    main()
