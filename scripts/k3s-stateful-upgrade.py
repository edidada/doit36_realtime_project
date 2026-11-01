#!/usr/bin/env python3
"""Run on k3s-master: back up and upgrade only realtime-dw's existing Flink job."""
import argparse
import copy
import hashlib
import json
import os
import shlex
from pathlib import Path
import subprocess
import time
import urllib.request

NAMESPACE = 'realtime-dw'
APPLICATION = 'realtime-dw-etl-01'
KUBECTL = ['kubectl', '-n', NAMESPACE]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def command(arguments, data=None):
    result = subprocess.run(KUBECTL + arguments, input=data, text=True,
                            capture_output=True, timeout=45, check=True)
    return result.stdout


def resource():
    return json.loads(command(['get', 'flinkdeployment', APPLICATION, '-o', 'json']))


def api(base, path, data=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(base + path, data=body, headers={'Content-Type': 'application/json'})
    with HTTP.open(request, timeout=20) as response:
        return json.load(response)


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    path.chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'preflight', 'deploy', 'rollback', 'status'])
    parser.add_argument('directory', type=Path)
    parser.add_argument('--image')
    parser.add_argument('--log-xml', type=Path)
    parser.add_argument('--job-name', default='realtime-dw-upgrade-preflight')
    parser.add_argument('--restore-path')
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.action == 'prepare':
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        before = resource()
        assert before['status']['jobStatus']['state'] == 'RUNNING', 'Existing job is not healthy'
        assert before['status']['lifecycleState'] == 'STABLE', 'Existing deployment is not stable'
        write(directory / 'before.json', before)
        service = json.loads(command(['get', 'service', APPLICATION + '-rest', '-o', 'json']))
        base = 'http://' + service['spec']['clusterIP'] + ':8081'
        job_id = before['status']['jobStatus']['jobId']
        write(directory / 'overview-before.json', api(base, '/overview'))
        write(directory / 'checkpoints-before.json', api(base, '/jobs/' + job_id + '/checkpoints'))
        trigger = api(base, '/jobs/' + job_id + '/savepoints', {
            'target-directory': 'file:///flink-data/savepoints', 'cancel-job': False, 'formatType': 'CANONICAL'})
        request_id = trigger['request-id']
        for _ in range(90):
            result = api(base, '/jobs/' + job_id + '/savepoints/' + request_id)
            if result['status']['id'] == 'COMPLETED':
                location = result.get('operation', {}).get('location')
                if not location:
                    raise RuntimeError('Savepoint failed; old job was not stopped')
                write(directory / 'savepoint.json', {'jobId': job_id, 'location': location, 'rest': base})
                print(json.dumps({'prepared': True, 'jobId': job_id, 'savepoint': location}), flush=True)
                return
            time.sleep(2)
        raise TimeoutError('Savepoint timed out; no deployment changes applied')
    before = json.loads((directory / 'before.json').read_text())
    snapshot = json.loads((directory / 'savepoint.json').read_text())
    current = resource()
    if args.action == 'rollback':
        if not args.restore_path:
            parser.error('rollback requires an explicit verified old-version --restore-path')
        from urllib.parse import urlparse
        restore = urlparse(args.restore_path)
        assert restore.scheme == 'file' and not restore.netloc and '..' not in restore.path
        assert restore.path.startswith('/flink-data/savepoints/savepoint-' + snapshot['jobId'][:6] + '-')
        assert current['metadata']['uid'] == before['metadata']['uid'], 'Resource was recreated; re-audit first'
        spec = copy.deepcopy(before['spec'])
        # Force restore from the named OLD-version savepoint, never from empty state or a new-version checkpoint.
        spec['job'].update(upgradeMode='stateless', initialSavepointPath=args.restore_path,
                           savepointRedeployNonce=int(time.time()), allowNonRestoredState=False, state='running')
        write(directory / 'rollback-request.json', {'spec': spec})
        print(command(['patch', 'flinkdeployment', APPLICATION, '--type=json', '-p',
                       json.dumps([{'op': 'replace', 'path': '/spec', 'value': spec}])]), flush=True)
        print('Rollback requested from explicit old-version savepoint: ' + args.restore_path, flush=True)
        return
    if args.action == 'preflight':
        if not args.image:
            parser.error('preflight requires --image')
        pod = before['spec']['podTemplate']['spec']
        original = next(c for c in pod['containers'] if c['name'] == 'flink-main-container')
        environment = copy.deepcopy(original.get('env', []))
        for entry in environment:
            if entry['name'] == 'FLINK_CHECKPOINT_URI':
                entry['value'] = 'file:///tmp/realtime-preview-checkpoints'
                entry.pop('valueFrom', None)
        job = {'apiVersion': 'batch/v1', 'kind': 'Job',
               'metadata': {'name': args.job_name, 'namespace': NAMESPACE,
                            'labels': {'app.kubernetes.io/name': 'realtime-dw-preflight'}},
               'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 180, 'ttlSecondsAfterFinished': 3600,
                        'template': {'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                            'nodeSelector': copy.deepcopy(pod['nodeSelector']),
                            'containers': [{'name': 'preflight', 'image': args.image, 'imagePullPolicy': 'Never',
                                'env': environment,
                                'resources': {'requests': {'cpu': '250m', 'memory': '512Mi'},
                                              'limits': {'cpu': '1', 'memory': '1536Mi'}},
                                'command': ['/bin/bash', '-ec',
                                    'java -Xmx256m -jar /opt/realtime-tools/fin-risk-engine-app.jar pricing; '
                                    'extra=(); for dependency in /opt/flink/usrlib/realtime_dw/*.jar; do '
                                    'extra+=(-C "file://$dependency"); done; '
                                    '/opt/flink/bin/flink info "${extra[@]}" -c com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim '
                                    '/opt/flink/usrlib/realtime_dw/realtime_dw-job.jar > /tmp/execution-plan.txt; '
                                    'test -s /tmp/execution-plan.txt; echo FLINK_PLAN_OK']}]}}}}
        write(directory / 'preflight-job.json', job)
        print(command(['create', '-f', '-'], json.dumps(job)), flush=True)
        return
    if args.action == 'status':
        reconciled = current.get('status', {}).get('reconciliationStatus', {}).get('lastReconciledSpec')
        observed = json.loads(reconciled).get('spec', {}) if reconciled else {}
        raw_error = current.get('status', {}).get('error')
        error = None
        if raw_error:
            try:
                error = json.loads(raw_error).get('type')
            except (ValueError, AttributeError):
                error = raw_error.splitlines()[0].split(':')[0][:160]
        print(json.dumps({'generation': current['metadata']['generation'],
                          'image': current['spec']['image'], 'version': current['spec']['flinkVersion'],
                          'reconciledImage': observed.get('image'), 'reconciledVersion': observed.get('flinkVersion'),
                          'entryClass': current['spec']['job']['entryClass'],
                          'state': current.get('status', {}).get('jobStatus', {}).get('state'),
                          'lifecycle': current.get('status', {}).get('lifecycleState'),
                          'errorType': error,
                          'savepointInfo': current.get('status', {}).get('jobStatus', {}).get('savepointInfo')},
                         ensure_ascii=False), flush=True)
        return
    if not args.image or not args.log_xml:
        parser.error('deploy requires --image and --log-xml')
    assert current['metadata']['uid'] == before['metadata']['uid'], 'Resource was recreated; re-audit first'
    if current['metadata']['generation'] != before['metadata']['generation']:
        previous_request = json.loads((directory / 'desired-spec.json').read_text())['spec']
        assert current['spec'] == previous_request, 'Deployment changed outside this release; re-audit first'
    assert current['status']['jobStatus']['jobId'] == snapshot['jobId'], 'Job restarted since backup'
    # Do not change middleware, namespaces, secrets, PVCs, service accounts, placement or CPU/memory.
    spec = copy.deepcopy(before['spec'])
    spec['image'] = args.image
    spec['flinkVersion'] = 'v1_19'
    spec['job'].update(entryClass='com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim',
                       upgradeMode='savepoint', allowNonRestoredState=False)
    spec['flinkConfiguration']['kubernetes.operator.job.upgrade.last-state-fallback.enabled'] = 'false'
    spec['flinkConfiguration']['execution.checkpointing.externalized-checkpoint-retention'] = 'RETAIN_ON_CANCELLATION'
    options = shlex.split(spec['flinkConfiguration'].get('env.java.opts', ''))
    options = [option for option in options if not option.startswith(
        ('-Dlog4j.configurationFile=', '-Dlog4j2.configurationFile='))]
    # Flink appends the legacy property itself. The canonical Log4j2 property wins over it.
    options.append('-Dlog4j2.configurationFile=file:/opt/realtime-conf/log4j2.xml')
    spec['flinkConfiguration']['env.java.opts'] = shlex.join(options)
    # Operator logConfiguration only accepts its legacy properties/logback keys. Mount XML separately.
    xml = args.log_xml.read_text(encoding='utf-8')
    log_name = 'realtime-dw-log4j2-' + hashlib.sha256(xml.encode()).hexdigest()[:12]
    config_map = {'apiVersion': 'v1', 'kind': 'ConfigMap',
                  'metadata': {'name': log_name, 'namespace': NAMESPACE}, 'immutable': True,
                  'data': {'log4j2.xml': xml}}
    existing = command(['get', 'configmap', log_name, '--ignore-not-found', '-o', 'json']).strip()
    if existing:
        assert json.loads(existing)['data'] == config_map['data'], 'Unexpected XML ConfigMap content'
    else:
        command(['create', '-f', '-'], json.dumps(config_map))
    pod = spec['podTemplate']['spec']
    pod['volumes'] = [v for v in pod['volumes'] if v['name'] not in ('job-libs', 'business-job', 'realtime-log4j2')]
    pod['volumes'].append({'name': 'realtime-log4j2', 'configMap': {'name': log_name}})
    for container in pod['containers']:
        container['volumeMounts'] = [v for v in container.get('volumeMounts', [])
                                   if v['name'] not in ('job-libs', 'realtime-log4j2')]
        if container['name'] == 'flink-main-container':
            container['volumeMounts'].append({'name': 'realtime-log4j2', 'mountPath': '/opt/realtime-conf', 'readOnly': True})
    pod['initContainers'] = [c for c in pod.get('initContainers', []) if c['name'] != 'prepare-job-libs']
    for container in pod['initContainers']:
        if container['name'] == 'prepare-flink-state':
            container['image'] = args.image
            container['command'] = ['/bin/sh', '-ec',
                'mkdir -p /flink-data/checkpoints /flink-data/savepoints /flink-data/logs; '
                'chown 9999:9999 /flink-data /flink-data/checkpoints /flink-data/savepoints /flink-data/logs']
    patch = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
             {'op': 'replace', 'path': '/spec', 'value': spec}]
    write(directory / 'desired-spec.json', {'spec': spec})
    print(command(['patch', 'flinkdeployment', APPLICATION, '--type=json', '-p', json.dumps(patch)]), flush=True)
    print('Requested stateful upgrade; no stateless fallback or non-restored state is allowed.', flush=True)


if __name__ == '__main__':
    main()
