"""Infrastructure/test orchestration and evidence collection, never a Firehose log sender.

Only Kinesis Agent on EC2 may submit log records. All evidence is private by default.
"""
import argparse,collections,datetime,hashlib,json,os,pathlib,re,time
import boto3
from botocore.config import Config

ROOT=pathlib.Path(__file__).resolve().parent
E=ROOT/'private-evidence'; E.mkdir(exist_ok=True)
UTC=datetime.timezone.utc
def now(): return datetime.datetime.now(UTC)
def dump(name,value):
    p=E/name; p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(value,indent=2,default=str,ensure_ascii=False)+'\n',encoding='utf-8')
    return value
def read(name): return json.loads((E/name).read_text(encoding='utf-8'))
def outputs(client,name):
    stack=client.describe_stacks(StackName=name)['Stacks'][0]
    return stack,{x['OutputKey']:x['OutputValue'] for x in stack.get('Outputs',[])}
def provider_outputs():
    st,o=outputs(cf,'squidlog-a')
    if not args.completion_stack:return st,o
    rs=cf.list_stack_resources(StackName='squidlog-a')['StackResourceSummaries']
    ids={x['LogicalResourceId']:x['PhysicalResourceId'] for x in rs if x.get('PhysicalResourceId')}
    _,extra=outputs(cf,args.completion_stack);o.update(extra)
    for n in (1,2):
        o['SquidInstanceIdAz'+str(n)]=ids['SquidInstanceAz'+str(n)]
        subnet=ec2.describe_subnets(SubnetIds=[ids['PublicSubnetAz'+str(n)]])['Subnets'][0]
        o['ProviderAzId'+str(n)]=subnet['AvailabilityZoneId']
    st=dict(st);st['StackStatus']='RECOVERED_WITH_COMPLETION_STACK' if extra.get('ServiceName') else 'COMPLETION_IN_PROGRESS'
    return st,o
def create(client,name,filename,parameters):
    # Never auto-delete failed resources. Existing stacks are not updated/rebuilt implicitly.
    try:
        existing=client.describe_stacks(StackName=name)['Stacks'][0]
    except client.exceptions.ClientError as exc:
        if 'does not exist' not in str(exc): raise
    else:
        print(json.dumps({'stack':name,'status':existing['StackStatus'],'action':'kept_existing'}));return
    result=client.create_stack(StackName=name,TemplateBody=(ROOT/filename).read_text(encoding='utf-8'),Parameters=[{'ParameterKey':k,'ParameterValue':str(v)}for k,v in parameters.items()],Capabilities=['CAPABILITY_NAMED_IAM'],DisableRollback=True,EnableTerminationProtection=True,Tags=[{'Key':'Lab','Value':'SquidAgentLab'}])
    dump(name+'-created.json',result);print(json.dumps({'stack':name,'created':True}))
def wait_ssm(ssm,instances,commands,label):
    cmd=ssm.send_command(InstanceIds=instances,DocumentName='AWS-RunShellScript',Parameters={'commands':commands,'executionTimeout':['120']},TimeoutSeconds=150)['Command']['CommandId']
    dump('commands/'+label+'-request.json',{'command_id':cmd,'instances':instances,'commands':commands,'utc':now()})
    results=[]
    for instance in instances:
        for _ in range(80):
            try: result=ssm.get_command_invocation(CommandId=cmd,InstanceId=instance)
            except ssm.exceptions.InvocationDoesNotExist: time.sleep(1);continue
            if result['Status'] not in ('Pending','InProgress','Delayed'): break
            time.sleep(2)
        else: raise TimeoutError('SSM command still running; inspect it, never re-send blindly')
        dump('commands/'+label+'-'+instance+'.json',result);results.append(result)
    return results

parser=argparse.ArgumentParser()
parser.add_argument('--a-profile',default='provider');parser.add_argument('--b-profile',default='consumer')
parser.add_argument('--region',default='ap-southeast-2')
parser.add_argument('--completion-stack',help='Explicit recovery stack using the same EC2/NLB; do not set for a fresh deployment.')
parser.add_argument('action',choices=['preflight','storage','provider','consumer','accept','status','inspect','mode','generate','collect','metrics','exec'])
parser.add_argument('--size',type=int,choices=[0,5120,51200],default=0)
parser.add_argument('--poll',type=int,choices=[100,1000,5000],default=100)
parser.add_argument('--count',type=int,default=1);parser.add_argument('--interval',type=float,default=0)
parser.add_argument('--route',choices=['regional','az1','az2'],default='regional')
parser.add_argument('--run',default='smoke');parser.add_argument('--script');parser.add_argument('--node',choices=['az1','az2','both'],default='both')
parser.add_argument('--start');parser.add_argument('--end')
args=parser.parse_args()
a=boto3.Session(profile_name=args.a_profile,region_name=args.region)
b=boto3.Session(profile_name=args.b_profile,region_name=args.region)
aid=a.client('sts').get_caller_identity()['Account'];bid=b.client('sts').get_caller_identity()['Account']
if aid==bid: raise RuntimeError('A and B must be different accounts; nothing was created or sent.')
cf=a.client('cloudformation');bcf=b.client('cloudformation');ssm=a.client('ssm');ec2=a.client('ec2')
if args.action=='preflight':
    report={'utc':now(),'region':args.region,'accounts_different':aid!=bid,'provider_account':aid,'consumer_account':bid,'max_http_attempts':1000,'max_squid_bytes':10*1024**2}
    for label,s in [('a',a),('b',b)]:
        report[label+'_stacks']=[{'name':x['StackName'],'status':x['StackStatus']} for page in s.client('cloudformation').get_paginator('list_stacks').paginate() for x in page['StackSummaries'] if x['StackStatus']!='DELETE_COMPLETE']
    dump('preflight.json',report);print(json.dumps(report,default=str))
elif args.action=='storage':create(cf,'squidlog-store','00-log-storage.json',{})
elif args.action=='provider':
    st,o=outputs(cf,'squidlog-store');assert st['StackStatus']=='CREATE_COMPLETE'
    create(cf,'squidlog-a','01-provider-logging.json',{'ConsumerAccountId':bid,'FirehoseStreamName':o['StreamName']})
elif args.action=='consumer':
    st,o=provider_outputs();assert st['StackStatus'] in ('CREATE_COMPLETE','UPDATE_COMPLETE','RECOVERED_WITH_COMPLETION_STACK')
    create(bcf,'squidlog-b','02-consumer-logging.json',{'ServiceName':o['ServiceName'],'ProviderAzId1':o['ProviderAzId1'],'ProviderAzId2':o['ProviderAzId2']})
elif args.action=='status':
    for cl,name in [(cf,'squidlog-store'),(cf,'squidlog-a'),(bcf,'squidlog-b')]+([(cf,args.completion_stack)] if args.completion_stack else []):
        try:st,o=outputs(cl,name)
        except cl.exceptions.ClientError:continue
        dump(name+'-state.json',st)
        ev=cl.describe_stack_events(StackName=name)['StackEvents'];dump(name+'-events.json',ev)
        print(json.dumps({'stack':name,'status':st['StackStatus'],'outputs':o,'recent':[{'logical':x['LogicalResourceId'],'status':x['ResourceStatus'],'reason':x.get('ResourceStatusReason')}for x in ev[:5]]}))
elif args.action=='accept':
    _,po=provider_outputs();_,bo=outputs(bcf,'squidlog-b')
    endpoint=bo.get('ConsumerEndpointId')
    if not endpoint:
        rs=bcf.list_stack_resources(StackName='squidlog-b')['StackResourceSummaries']
        endpoint=next(x['PhysicalResourceId'] for x in rs if x['LogicalResourceId']=='ConsumerEndpoint' and x.get('PhysicalResourceId'))
    endpoint_state=b.client('ec2').describe_vpc_endpoints(VpcEndpointIds=[endpoint])['VpcEndpoints'][0]
    if endpoint_state['ServiceName']!=po['ServiceName']: raise RuntimeError('Service mismatch')
    if endpoint_state['State']=='pendingAcceptance':
        result=ec2.accept_vpc_endpoint_connections(ServiceId=po['ServiceId'],VpcEndpointIds=[endpoint])
    else:result={'existing_state':endpoint_state['State']}
    dump('endpoint-accept.json',result);print(json.dumps(result,default=str))
elif args.action in ('inspect','mode','exec'):
    _,o=provider_outputs()
    ids=[o['SquidInstanceIdAz'+str(n)] for n in (1,2) if args.node=='both' or args.node=='az'+str(n)]
    label=args.run+'-'+args.action
    if args.action=='inspect':
        commands=['set -eu','cat /etc/os-release','rpm -q squid aws-kinesis-agent','java -version 2>&1','cat /var/log/squid-agent-versions.txt','cat /etc/aws-kinesis/agent.json','systemctl status aws-kinesis-agent --no-pager','systemctl status squid --no-pager','ls -l /var/log/squid','tail -n 12 /var/log/aws-kinesis-agent/aws-kinesis-agent.log','cat /etc/logrotate.d/squid']
    elif args.action=='mode':
        commands=['set -eu','systemctl stop aws-kinesis-agent',f"jq '.flows[0].aggregatedRecordSizeBytes={args.size} | .flows[0].minTimeBetweenFilePollsMillis={args.poll}' /etc/aws-kinesis/agent.json >/etc/aws-kinesis/agent.json.new",'chmod 0644 /etc/aws-kinesis/agent.json.new','mv /etc/aws-kinesis/agent.json.new /etc/aws-kinesis/agent.json','systemctl start aws-kinesis-agent','cat /etc/aws-kinesis/agent.json']
    else:
        if not args.script: raise ValueError('--script required')
        commands=[pathlib.Path(args.script).read_text(encoding='utf-8')]
    results=wait_ssm(ssm,ids,commands,label)
    for r in results: print(json.dumps({'instance':r['InstanceId'],'status':r['Status'],'stdout':r.get('StandardOutputContent'),'stderr':r.get('StandardErrorContent')}))
    if any(r['Status']!='Success' for r in results): raise RuntimeError('SSM failed; preserve evidence and inspect')
elif args.action=='generate':
    if not 1<=args.count<=120 or not 0<=args.interval<=1 or not re.fullmatch(r'[a-zA-Z0-9_-]{1,32}',args.run): raise ValueError('Safe finite run: count 1..120, interval 0..1')
    # Reserve attempts BEFORE invoking. Retrying a failed invocation consumes the budget too.
    budget=E/'request-budget.json'; ledger=read('request-budget.json') if budget.exists() else {'reserved':0,'runs':[]}
    if any(x['run']==args.run for x in ledger['runs']): raise ValueError('Run ID already reserved; inspect evidence instead of replaying')
    if ledger['reserved']+args.count>1000: raise ValueError('1000-request hard cap')
    # Stop if the existing real Squid log exceeds 10 MiB on either host.
    _,po=provider_outputs()
    checks=wait_ssm(ssm,[po['SquidInstanceIdAz1'],po['SquidInstanceIdAz2']],['set -eu','test "$(find /var/log/squid -maxdepth 1 -name \'access.log*\' -type f -printf \'%s\\n\' | awk \'{s+=$1} END {print s+0}\')" -lt 5242880'],'budget-'+args.run)
    if any(x['Status']!='Success' for x in checks): raise RuntimeError('Log size cap check failed')
    ledger['reserved']+=args.count;ledger['runs'].append({'run':args.run,'count':args.count,'route':args.route,'interval':args.interval,'utc':str(now())});dump('request-budget.json',ledger)
    _,bo=outputs(bcf,'squidlog-b')
    lc=b.client('lambda',config=Config(retries={'max_attempts':0},read_timeout=70))
    response_rows=[]
    for offset in range(0,args.count,10):
        payload={'run':args.run+'-'+str(offset),'route':args.route,'count':min(10,args.count-offset),'interval':args.interval}
        started=now();result=lc.invoke(FunctionName=bo['TestLambdaName'],InvocationType='RequestResponse',Payload=json.dumps(payload).encode())
        body=json.loads(result['Payload'].read());record={'utc_start':started,'utc_end':now(),'payload':payload,'response':body,'function_error':result.get('FunctionError')}
        dump('requests/'+args.run+'-'+str(offset)+'.json',record);response_rows.append(record)
        if result.get('FunctionError') or len(body.get('requests',[]))!=payload['count'] or any('error'in x or x.get('status')!=200 for x in body.get('requests',[])):
            raise RuntimeError('Request failed; no automatic resend. See evidence.')
    print(json.dumps({'run':args.run,'actual_requests':sum(len(x['response']['requests'])for x in response_rows),'utc_end':str(now())}))
elif args.action=='collect':
    _,so=outputs(cf,'squidlog-store');s3=a.client('s3',endpoint_url='https://s3.'+args.region+'.amazonaws.com',config=Config(signature_version='s3v4'));manifest=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=so['BucketName']):
        for obj in page.get('Contents',[]):
            body=s3.get_object(Bucket=so['BucketName'],Key=obj['Key'])['Body'].read();p=E/'s3-originals'/obj['Key'];p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
            manifest.append({'key':obj['Key'],'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'last_modified':obj['LastModified']})
    dump('s3-manifest.json',manifest)
    _,po=provider_outputs()
    results=wait_ssm(ssm,[po['SquidInstanceIdAz1'],po['SquidInstanceIdAz2']],['set -eu',"tar -czf /tmp/squid-agent-evidence.tgz /var/log/squid/access.log* /etc/squid/squid.conf /etc/aws-kinesis/agent.json /etc/aws-kinesis/log4j.xml /etc/systemd/system/aws-kinesis-agent.service /etc/systemd/system/aws-kinesis-agent.service.d /etc/cron.d/aws-kinesis-agent /etc/logrotate.d/squid /var/log/aws-kinesis-agent /var/log/squid-agent-versions.txt /var/log/squid-agent-bootstrap.log 2>/tmp/squid-agent-tar-stderr",'base64 -w0 /tmp/squid-agent-evidence.tgz'],'collect-'+args.run)
    # SSM inline output is limited. Fetch full tar through the already-private evidence bucket.
    for instance in [po['SquidInstanceIdAz1'],po['SquidInstanceIdAz2']]:
        # Presigned URL grants only one small PUT for this exact evidence key. It is never printed.
        key='host-evidence/'+args.run+'/'+instance+'.tgz'
        url=s3.generate_presigned_url('put_object',Params={'Bucket':so['BucketName'],'Key':key},ExpiresIn=600)
        cmd=ssm.send_command(InstanceIds=[instance],DocumentName='AWS-RunShellScript',Parameters={'commands':['set -eu',"code=$(curl -fsS --retry 2 -X PUT --upload-file /tmp/squid-agent-evidence.tgz --output /tmp/evidence-upload-response --write-out '%{http_code}' '"+url+"'); test $code = 200; echo evidence-uploaded"],'executionTimeout':['60']})['Command']['CommandId']
        for _ in range(40):
            try:r=ssm.get_command_invocation(CommandId=cmd,InstanceId=instance)
            except ssm.exceptions.InvocationDoesNotExist:time.sleep(1);continue
            if r['Status'] not in ('Pending','InProgress','Delayed'):break
            time.sleep(1)
        dump('host-originals/'+args.run+'-'+instance+'-upload.json',{k:r.get(k) for k in ['Status','StandardOutputContent','StandardErrorContent']})
        if r['Status']!='Success':raise RuntimeError('Host evidence upload failed; preserve host')
        body=s3.get_object(Bucket=so['BucketName'],Key=key)['Body'].read();path=E/'host-originals'/(args.run+'-'+instance+'.tgz');path.parent.mkdir(exist_ok=True);path.write_bytes(body)
        dump('host-originals/'+args.run+'-'+instance+'-manifest.json',{'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'s3_key':key})
    print(json.dumps({'objects':len(manifest),'bytes':sum(x['bytes']for x in manifest),'squid_objects':sum(x['key'].startswith('squid/')for x in manifest),'squid_bytes':sum(x['bytes']for x in manifest if x['key'].startswith('squid/')),'host_originals_saved':2}))
elif args.action=='metrics':
    if not args.start or not args.end: raise ValueError('Explicit UTC --start and --end required')
    start=datetime.datetime.fromisoformat(args.start);end=datetime.datetime.fromisoformat(args.end)
    _,so=outputs(cf,'squidlog-store');cw=a.client('cloudwatch');report={'start':start,'end':end,'metrics':{}}
    for name in ['IncomingBytes','IncomingRecords','IncomingPutRequests','DeliveryToS3.Bytes','DeliveryToS3.Records','DeliveryToS3.Success']:
        result=cw.get_metric_statistics(Namespace='AWS/Firehose',MetricName=name,Dimensions=[{'Name':'DeliveryStreamName','Value':so['StreamName']}],StartTime=start,EndTime=end,Period=60,Statistics=['Sum'])
        report['metrics'][name]=result
    dump('metrics/'+args.run+'.json',report)
    print(json.dumps({k:sum(x['Sum']for x in v['Datapoints'])for k,v in report['metrics'].items()}))
