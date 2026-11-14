"""Finite rotation, normal restart, and unreachable-endpoint recovery checks.

Only request generation is orchestrated here; Kinesis Agent on the EC2 sends logs.
"""
import argparse,datetime,json,pathlib,subprocess,sys,time
p=argparse.ArgumentParser();p.add_argument('--a-profile',default='provider');p.add_argument('--b-profile',default='consumer');p.add_argument('--completion-stack');args=p.parse_args()
root=pathlib.Path(__file__).parent;e=root/'private-evidence';plan=e/'continuity-plan.json'
if plan.exists():raise SystemExit('Continuity run already recorded. Inspect it; do not replay automatically.')
comparison=json.loads((e/'comparison-plan.json').read_text())
if not comparison.get('end') or len(comparison['phases'])!=6 or not all(x.get('completed')for x in comparison['phases']):raise SystemExit('Comparison must finish first')
base=[sys.executable,str(root/'labctl.py'),'--a-profile',args.a_profile,'--b-profile',args.b_profile]
if args.completion_stack:base+=['--completion-stack',args.completion_stack]
report={'max_http_requests':60,'stages':[]}
def save():plan.write_text(json.dumps(report,indent=2,default=str)+'\n',encoding='utf-8')
def call(label,*params):
 r=subprocess.run(base+list(params),capture_output=True,text=True,encoding='utf-8');(e/(label+'-output.txt')).write_text(r.stdout+'\n'+r.stderr,encoding='utf-8')
 if r.returncode:raise RuntimeError(label+' failed; do not resend automatically')
 return r.stdout
def script(name,text):p=e/name;p.write_text(text,encoding='utf-8');return str(p)
def requests(label):
 for route in ['az1','az2']:call(label+'-'+route,'generate','--route',route,'--count','5','--run',label+'-'+route)
def wait(seconds):
 print('Waiting',seconds,'seconds; sending no requests',flush=True)
 for _ in range(seconds):time.sleep(1)
save()
call('continuity-mode','mode','--size','5120','--run','continuity')
for stage,operation in [('rotation','logrotate -f /etc/logrotate.d/squid\nls -li /var/log/squid/access.log*\n'),('restart','systemctl restart aws-kinesis-agent\nsleep 5\nsystemctl is-active aws-kinesis-agent\n')]:
 print('BEGIN',stage,flush=True);row={'stage':stage,'start':datetime.datetime.now(datetime.timezone.utc)};report['stages'].append(row);save()
 requests(stage+'before')
 call(stage,'exec','--script',script(stage+'.sh','set -eu\n'+operation),'--run',stage)
 requests(stage+'after');row['end']=datetime.datetime.now(datetime.timezone.utc);row['requests']=20;save()
 # Complete the current send buffer before the next independent scenario.
 wait(90)
print('BEGIN unreachable endpoint',flush=True);row={'stage':'unreachable-endpoint','start':datetime.datetime.now(datetime.timezone.utc),'expected':'connection refused, then recovery using persistent checkpoints'};report['stages'].append(row);save()
fail=script('set-unreachable.sh','''set -eu
cp -a /etc/aws-kinesis/agent.json /etc/aws-kinesis/agent.json.before-failure
systemctl stop aws-kinesis-agent
jq '."firehose.endpoint"="https://127.0.0.1:9"' /etc/aws-kinesis/agent.json >/etc/aws-kinesis/agent.json.new
chmod 0644 /etc/aws-kinesis/agent.json.new
mv /etc/aws-kinesis/agent.json.new /etc/aws-kinesis/agent.json
systemctl start aws-kinesis-agent
''')
restore=script('restore-endpoint.sh','''set -eu
test -f /etc/aws-kinesis/agent.json.before-failure
systemctl stop aws-kinesis-agent
cp -a /etc/aws-kinesis/agent.json.before-failure /etc/aws-kinesis/agent.json
systemctl start aws-kinesis-agent
sleep 5
systemctl is-active aws-kinesis-agent
''')
try:
 call('set-unreachable','exec','--script',fail,'--run','unreachable')
 requests('failduring');wait(75)
 call('failure-observed','exec','--script',script('observe-failure.sh','set -eu\ngrep -E "Connection refused|Unable to execute HTTP request|Retriable send error|127.0.0.1" /var/log/aws-kinesis-agent/aws-kinesis-agent.log | tail -n 15\n'),'--run','failureobserved')
finally:
 # Restore the exact previous endpoint even if the finite test aborts.
 call('restore-endpoint','exec','--script',restore,'--run','restored')
requests('failafter');row['end']=datetime.datetime.now(datetime.timezone.utc);row['requests']=20;row['restored']=True;save()
wait(150)
report['end']=datetime.datetime.now(datetime.timezone.utc);save()
print('CONTINUITY COMPLETE. Environment and all logs kept.',flush=True)
