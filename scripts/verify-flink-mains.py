#!/usr/bin/env python3
"""Check Flink main lifecycle: RUNNING, two checkpoints, savepoint, exit 0.

This checks runtime health; business output requires separate validation. Settings
come from the environment or an optional private JSON file; credentials are never
printed. See docs/k3s-main-execution.md.
"""
import argparse,json,os,subprocess,time,re,urllib.request,signal
from pathlib import Path
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--env-file', type=Path, help='Private JSON object containing environment variables')
parser.add_argument('--main', action='append', required=True, help='Fully qualified main; repeat for multiple mains')
parser.add_argument('--topic', default='main-audit-rules')
parser.add_argument('--checkpoints', type=int, default=2, help='Minimum successful checkpoints (at least two)')
parser.add_argument('--output', type=Path, default=Path('.artifacts/main-audit/lifecycle'))
args = parser.parse_args()
if args.checkpoints < 2:parser.error('--checkpoints must be at least 2')
os.chdir(Path(__file__).resolve().parents[1])
base = args.output.resolve()
base.mkdir(parents=True, exist_ok=True)
environment = dict(os.environ)
if args.env_file:
    environment.update(json.loads(args.env_file.read_text()))
environment.update(SKIP_MAIN_COMPILE='true', KAFKA_INPUT_TOPIC=args.topic,
                   KAFKA_GROUP_ID='main-audit-recovery-' + str(int(time.time())), KAFKA_OUTPUT_TOPIC='main-audit-rule-results')
http=urllib.request.build_opener(urllib.request.ProxyHandler({}));processes=[]
def request(port,path,data=None):
 req=urllib.request.Request(f'http://localhost:{port}'+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
 with http.open(req,timeout=10) as r:return json.load(r)
def start(name,savepoint=None):
 env=dict(environment)
 if savepoint:env['FLINK_SAVEPOINT_PATH']=savepoint
 log=base/(name+'.log');f=log.open('w');p=subprocess.Popen(['bash','scripts/run-main.sh',module,main_class],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);processes.append(p)
 deadline=time.time()+120
 while time.time()<deadline:
  if p.poll() is not None:raise RuntimeError('Main exited: '+str(p.returncode))
  m=re.search(r'Rest endpoint listening at localhost:(\d+)',log.read_text(errors='replace'))
  if m:
   port=int(m.group(1))
   try:
    job=next((j for j in request(port,'/jobs/overview')['jobs'] if j['state']=='RUNNING' and j['tasks']['running']==j['tasks']['total'] and j['tasks']['total']>0),None)
    text=log.read_text(errors='replace')
    if job:return p,log,port,job['jid']
   except (OSError,ValueError):pass
  time.sleep(.5)
 raise RuntimeError('No fully running job: '+str(log))
def checkpoints(port,jid):
 for _ in range(40):
  snapshot=request(port,f'/jobs/{jid}/checkpoints')
  counts=snapshot['counts']
  if counts['completed']>=args.checkpoints:
   if counts['failed']:
    failure=snapshot.get('latest',{}).get('failed') or {}
    message=failure.get('failure_message','')
    if not message:
     startup=re.findall(r'Failed to trigger checkpoint[^\n]+Failure reason: Not all required tasks are currently running',log.read_text(errors='replace'))
     if len(startup)>=counts['failed']:
      message='Not all required tasks are currently running (all failed triggers accounted for in main log)'
    if 'Not all required tasks are currently running' not in message:
     raise RuntimeError('Checkpoint failure: '+json.dumps(failure))
    # Flink may schedule its first checkpoint while local tasks are deploying.
    # Retain this failure explicitly; require two further successful checkpoints.
    counts['latest_startup_failure']=message
   return counts
  time.sleep(1)
 raise RuntimeError('Checkpoint did not succeed: '+str(counts))
def stop(p,port,jid):
 result=request(port,f'/jobs/{jid}/stop',{'drain':False,'targetDirectory':(base/'savepoints').as_uri()});trigger=result['request-id']
 for _ in range(80):
  result=request(port,f'/jobs/{jid}/savepoints/{trigger}')
  if result['status']['id']=='COMPLETED':break
  time.sleep(.5)
 assert 'location' in result.get('operation',{}),result
 code=p.wait(timeout=30);assert code==0,code
 return result['operation']['location']
import importlib.util,sys
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('audit','scripts/audit-mains.py');audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
entries=[e for e in audit.discover() if e['class'] in args.main]
unknown=set(args.main)-{e['class'] for e in entries}
if unknown:raise SystemExit('Unknown mains: '+', '.join(sorted(unknown)))
results=[]
for entry in entries:
 module=entry['module'];main_class=entry['class'];environment['KAFKA_GROUP_ID']='lifecycle-'+str(time.time_ns())
 environment['FLINK_CHECKPOINT_URI']=(base/'checkpoints').as_uri()
 environment['STANDALONE_OUTPUT_URI']=(base/'standalone-output').as_uri()
 try:
  p,log,port,jid=start(main_class)
  counts=checkpoints(port,jid)
  sp=stop(p,port,jid)
  result=dict(entry,status='passed',exit_code=0,checkpoints=counts,savepoint=sp,log=str(log))
 except Exception as e:
  result=dict(entry,status='failed',error=str(e))
 finally:
  for proc in processes:
   if proc.poll() is None:
    os.killpg(proc.pid,signal.SIGTERM)
    try:proc.wait(10)
    except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 results.append(result);(base/'results.json').write_text(json.dumps({'results':results},indent=2)+'\n');print(json.dumps(result),flush=True)
if any(r['status'] != 'passed' for r in results):raise SystemExit(1)
