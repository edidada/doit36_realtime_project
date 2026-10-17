#!/usr/bin/env python3
"""Verify the original Kafka/CDC/Redis recovery main, then restore its savepoint.

Initialize deploy/k3s/business/init-rule-validation.sql first. Connection settings
come from the environment or an optional private JSON file; credentials are never
printed. See docs/k3s-main-execution.md.
"""
import argparse,json,os,subprocess,time,re,urllib.request,signal
from pathlib import Path
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--env-file', type=Path, help='Private JSON object containing environment variables')
parser.add_argument('--master', default='root@192.168.31.77')
parser.add_argument('--topic', default='main-audit-rules')
parser.add_argument('--output', type=Path, default=Path('.artifacts/main-audit/recovery-validation'))
args = parser.parse_args()
os.chdir(Path(__file__).resolve().parents[1])
base = args.output.resolve()
base.mkdir(parents=True, exist_ok=True)
environment = dict(os.environ)
if args.env_file:
    environment.update(json.loads(args.env_file.read_text()))
environment.update(SKIP_MAIN_COMPILE='true', KAFKA_INPUT_TOPIC=args.topic,
                   KAFKA_GROUP_ID='main-audit-recovery-' + str(int(time.time())))
http=urllib.request.build_opener(urllib.request.ProxyHandler({}));processes=[]
def request(port,path,data=None):
 req=urllib.request.Request(f'http://localhost:{port}'+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
 with http.open(req,timeout=10) as r:return json.load(r)
def start(name,savepoint=None):
 env=dict(environment)
 if savepoint:env['FLINK_SAVEPOINT_PATH']=savepoint
 log=base/(name+'.log');f=log.open('w');p=subprocess.Popen(['bash','scripts/run-main.sh','rt_rule_mk','cn.doitedu.rtmk.demo8.RuleEngineRecovery'],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);processes.append(p)
 deadline=time.time()+120
 while time.time()<deadline:
  if p.poll() is not None:raise RuntimeError('Main exited: '+str(p.returncode))
  m=re.search(r'Rest endpoint listening at localhost:(\d+)',log.read_text(errors='replace'))
  if m:
   port=int(m.group(1))
   try:
    job=next((j for j in request(port,'/jobs/overview')['jobs'] if j['state']=='RUNNING' and j['tasks']['running']==j['tasks']['total'] and j['tasks']['total']>0),None)
    text=log.read_text(errors='replace')
    if job and (savepoint or '收到一条新注入' in text):return p,log,port,job['jid']
   except (OSError,ValueError):pass
  time.sleep(.5)
 raise RuntimeError('No fully running job: '+str(log))
def produce(user,collect):
 now=int(time.time()*1000);events=[]
 if collect:events.append(dict(user_id=user,event_id='item_collect',event_time=now,properties={'item_id':'p02'}))
 events.append(dict(user_id=user,event_id='item_share',event_time=now+1,properties={'item_id':'p02'}))
 p=subprocess.run(['ssh','-o','BatchMode=yes',args.master,'kubectl -n doit exec -i deployment/business-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 192.168.31.211:9092 --topic ' + __import__('shlex').quote(args.topic)],input=''.join(json.dumps(e)+'\n' for e in events),capture_output=True,text=True,timeout=30)
 assert p.returncode==0,p.stderr
 return len(events)
def hit(log,user,port,jid):
 for _ in range(100):
  t=log.read_text(errors='replace')
  if any('"ruleId":900017' in l and f'"userId":{user}' in l for l in t.splitlines()):return
  job=request(port,'/jobs/'+jid)
  assert job['state']=='RUNNING',job['state']
  time.sleep(.5)
 raise RuntimeError('No rule match in '+str(log))
def checkpoints(port,jid):
 for _ in range(40):
  counts=request(port,f'/jobs/{jid}/checkpoints')['counts']
  if counts['completed']>=2:return counts
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
try:
 user=int(time.time())%10000000+100000000
 p,log,port,jid=start('recovery-fixed');produce(user,True);hit(log,user,port,jid);before=checkpoints(port,jid);assert before['failed']==0,before
 savepoint=stop(p,port,jid)
 print('Initial rule match, two checkpoints and savepoint succeeded.',flush=True)
 p,log,port,jid2=start('recovery-restored',savepoint);produce(user,False);hit(log,user,port,jid2);after=checkpoints(port,jid2)
 assert after['restored']==1 and after['failed']==0,after
 restored_savepoint=stop(p,port,jid2)
 result=dict(main='cn.doitedu.rtmk.demo8.RuleEngineRecovery',flink='1.19.3',user_id=user,initial_job_id=jid,restored_job_id=jid2,checkpoint_before_restore=before,checkpoint_after_restore=after,savepoint=savepoint,restored_savepoint=restored_savepoint,initial_input_events=2,post_restore_input_events=1,both_main_exit_codes=[0,0],verified='Restored broadcast rule metadata and per-user keyed count; trigger alone matched after restore')
 (base/'recovery-acceptance.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2),flush=True)
finally:
 for p in processes:
  if p.poll() is None:
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(10)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
