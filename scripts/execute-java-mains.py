#!/usr/bin/env python3
"""逐个执行全部 Java main，保留脱敏日志和退出/主动停止结果。"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--seconds', type=int, default=90, help='每个 main 最长观察秒数；超时主动停止不算正常退出')
parser.add_argument('--output', type=Path, default=Path('logs/validation'))
parser.add_argument('--only', help='只运行完整类名匹配该正则的入口，用于单项复验')
args = parser.parse_args()
if args.seconds < 1:
    parser.error('--seconds must be positive')
args.output.mkdir(parents=True, exist_ok=True)
spec = importlib.util.spec_from_file_location('audit', ROOT/'scripts/audit-mains.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
profile = ROOT/'.artifacts/main-audit/k3s/runtime-env.json'
profile = Path(os.environ.get('MAIN_ENV_JSON', str(profile)))
if 'MAIN_ENV_JSON' in os.environ and not profile.is_file():
    parser.error('MAIN_ENV_JSON 指定的文件不存在')
settings = json.loads(profile.read_text()) if profile.is_file() else {}
if not isinstance(settings, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in settings.items()):
    parser.error('MAIN_ENV_JSON 必须是环境变量名到字符串值的 JSON 对象')
env = dict(os.environ)
env.update(settings)
env.pop('MAIN_ENV_JSON', None)
env.update(MAIN_ENV_LOADED='true', SKIP_MAIN_COMPILE='true', EVENT_BATCHES='1',
           DORIS_TABLE_IDENTIFIER='dwd.mall_events_wide_main_validation',
           KAFKA_INPUT_TOPIC='main-audit-rules', KAFKA_OUTPUT_TOPIC='main-audit-output',
           GROOVY_SCRIPT=str(ROOT/'examples/main-inputs/hello.groovy'),
           DIM_REC_INPUT=str(ROOT/'examples/main-inputs/dim_rec.csv'),
           DIM_REC_OUTPUT=str(ROOT/'.artifacts/main-audit/k3s/dim-rec-commands.sh'),
           SIMWORDS_URL='http://192.168.31.211:8081/api/post/simwords', SOCKET_HOST='192.168.31.211',
           SOCKET_PORT='9090',
           FLINK_CHECKPOINT_URI=(ROOT/'.artifacts/main-audit/log4j2-checkpoints').as_uri(),
           STANDALONE_OUTPUT_URI=(ROOT/'.artifacts/main-audit/log4j2-files').as_uri())
secrets = [v for k, v in settings.items() if any(s in k.upper() for s in ('PASSWORD', 'SECRET', 'TOKEN')) and v]
def redact(text):
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, '[REDACTED]')
    text = re.sub(r'(?im)(password\s*[=:]\s*)([^\s,]+)', r'\1[REDACTED]', text)
    return '\n'.join(line.rstrip() for line in text.splitlines()) + '\n'

results = []
for entry in audit.discover():
    if not entry['source'].endswith('.java'):
        continue
    name = entry['class']
    if args.only and not re.search(args.only, name):
        continue
    current = dict(env, KAFKA_GROUP_ID='log4j2-audit-'+str(time.time_ns()))
    if entry['module'] == 'realtime_dw':
        current['KAFKA_INPUT_TOPIC'] = 'main-audit-input'
    if name == 'com.test.bigdata.rtmk.demo6.DynamicRuleManagement':
        current['MYSQL_RULE_DATABASE'] = 'doit36_main_audit_demo6'
    raw = ROOT/f'.artifacts/main-audit/log4j2-{os.getpid()}.log'
    raw.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    stopped = False
    print('START '+name, flush=True)
    with raw.open('w') as log:
        p = subprocess.Popen(['bash', 'scripts/java-mains.sh', name], env=current,
                             stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + args.seconds
            while p.poll() is None:
                if time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(p.args, args.seconds)
                current_log = raw.read_text(errors='replace')
                finite = name.endswith('Local') or name.endswith(('DimRecProcess', 'TestDiaoyong', 'EventsMoni', 'HbaseProfileTagsMoni', 'HistoryConditionValueMoni', 'RedisDemo'))
                if not finite and len(re.findall(r'Completed checkpoint \d+', current_log)) >= 2:
                    raise subprocess.TimeoutExpired(p.args, args.seconds)
                time.sleep(.5)

        except subprocess.TimeoutExpired:
            stopped = True
            os.killpg(p.pid, signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
    content = redact(raw.read_text(errors='replace'))
    path = args.output/(name+'.log')
    path.write_text(content)
    result = dict(entry, exit_code=p.returncode, actively_stopped=stopped,
                  seconds=round(time.monotonic()-started, 2), log=str(path),
                  running_observed='to RUNNING' in content,
                  completed_checkpoints=len(re.findall(r'Completed checkpoint \d+', content)),
                  errors=len(re.findall(r'\[ERROR\]', content)),
                  status='stopped_after_observation' if stopped else ('exited_0' if p.returncode==0 else 'failed'))
    results.append(result)
    (args.output/'results.json').write_text(json.dumps({'results': results}, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(result, ensure_ascii=False), flush=True)
print('Finished '+str(len(results))+' Java mains', flush=True)
