"""Finite six-phase real-log comparison. Runs labctl; Kinesis Agent sends the records."""
import argparse,datetime,email.utils,json,pathlib,subprocess,sys,time
import boto3
p=argparse.ArgumentParser();p.add_argument('--a-profile',default='provider');p.add_argument('--b-profile',default='consumer');p.add_argument('--completion-stack');args=p.parse_args()
root=pathlib.Path(__file__).resolve().parent;e=root/'private-evidence';e.mkdir(exist_ok=True)
plan=e/'comparison-plan.json'
if plan.exists():raise SystemExit('Comparison plan already exists. Inspect recorded phases; do not replay automatically.')
s=boto3.Session(profile_name=args.a_profile,region_name='ap-southeast-2')
before=datetime.datetime.now(datetime.timezone.utc);response=s.client('sts').get_caller_identity();after=datetime.datetime.now(datetime.timezone.utc)
server=email.utils.parsedate_to_datetime(response['ResponseMetadata']['HTTPHeaders']['date'])
offset=(server-after).total_seconds();start_mono=time.monotonic()
def utc():return server+datetime.timedelta(seconds=time.monotonic()-start_mono)
def save(path,obj):path.write_text(json.dumps(obj,indent=2,default=str)+'\n',encoding='utf-8')
save(e/'clock-calibration.json',{'local_before':before,'local_after':after,'aws_date':server,'estimated_offset_seconds':offset,'note':'HTTP Date has 1 second precision; use minute boundaries with guard intervals.'})
base=[sys.executable,str(root/'labctl.py'),'--a-profile',args.a_profile,'--b-profile',args.b_profile]
if args.completion_stack:base+=['--completion-stack',args.completion_stack]
def call(label,*params):
    r=subprocess.run(base+list(params),capture_output=True,text=True,encoding='utf-8')
    (e/(label+'-output.txt')).write_text(r.stdout+'\n'+r.stderr,encoding='utf-8')
    if r.returncode:raise RuntimeError(label+' failed; no retry; see saved output')
    return r.stdout
def pause(seconds,message):
    print(utc().isoformat(),message,flush=True)
    until=time.monotonic()+seconds
    while time.monotonic()<until:time.sleep(min(1,max(0,until-time.monotonic())))
def boundary():
    t=utc();target=t.replace(second=0,microsecond=0)+datetime.timedelta(minutes=1,seconds=3)
    pause(max(0,(target-t).total_seconds()),'Waiting for an isolated metric window')
    return target.replace(second=0)
for name,commands in [('stop-agent.sh','set -eu\nsystemctl stop aws-kinesis-agent\n'),('start-agent.sh','set -eu\nsystemctl start aws-kinesis-agent\nsleep 3\nsystemctl is-active aws-kinesis-agent\n')]:
    (e/name).write_text(commands,encoding='utf-8')
report={'max_requested':720,'requests_per_node_per_phase':60,'interval_seconds':0.1,'phases':[],'start':utc()};save(plan,report)
for kind in ['live','backlog']:
    for size in [0,5120,51200]:
        phase_id=kind[0]+str(size).zfill(5)
        print(utc().isoformat(),'BEGIN',phase_id,flush=True)
        call(phase_id+'-mode','mode','--size',str(size),'--poll','100','--run',phase_id)
        if kind=='backlog':call(phase_id+'-stop','exec','--script',str(e/'stop-agent.sh'),'--run',phase_id+'stop')
        start=boundary();phase={'id':phase_id,'kind':kind,'aggregate_bytes':size,'start':start,'request_start':utc()};report['phases'].append(phase);save(plan,report)
        for route in ['az1','az2']:
            print(utc().isoformat(),'GENERATE',phase_id,route,60,flush=True)
            call(phase_id+'-'+route,'generate','--route',route,'--count','60','--interval','0.1','--run',phase_id+'-'+route)
        phase['request_end']=utc();save(plan,report)
        if kind=='backlog':call(phase_id+'-start','exec','--script',str(e/'start-agent.sh'),'--run',phase_id+'start')
        pause(90,'Waiting for Agent acknowledgements and Firehose ingestion; no additional requests')
        phase['end']=boundary();phase['completed']=True;save(plan,report)
        print(utc().isoformat(),'COMPLETE',phase_id,flush=True)
report['end']=utc();save(plan,report)
for phase in report['phases']:
    call(phase['id']+'-metrics','metrics','--start',phase['start'].isoformat(),'--end',phase['end'].isoformat(),'--run',phase['id'])
print(utc().isoformat(),'COMPARISON COMPLETE; environment and logs kept',flush=True)
