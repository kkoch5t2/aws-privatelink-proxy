"""Read-only audit of the retained lab. Saves raw AWS responses privately."""
import argparse,datetime,json,pathlib
import boto3
from botocore.exceptions import ClientError
p=argparse.ArgumentParser();p.add_argument('--a-profile',default='provider');p.add_argument('--b-profile',default='consumer');args=p.parse_args()
root=pathlib.Path(__file__).parent/'private-evidence';root.mkdir(exist_ok=True)
a=boto3.Session(profile_name=args.a_profile,region_name='ap-southeast-2');b=boto3.Session(profile_name=args.b_profile,region_name='ap-southeast-2')
report={'checked_utc':datetime.datetime.now(datetime.timezone.utc),'accounts_different':a.client('sts').get_caller_identity()['Account']!=b.client('sts').get_caller_identity()['Account'],'stacks':{}}
def save(name,value):
 path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')
for label,session,names in [('a',a,['squidlog-store','squidlog-a','squidlog-a-link']),('b',b,['squidlog-b'])]:
 cf=session.client('cloudformation')
 for name in names:
  try:st=cf.describe_stacks(StackName=name)['Stacks'][0]
  except ClientError as e:
   if name=='squidlog-a-link' and 'does not exist'in str(e):continue
   raise
  rs=cf.list_stack_resources(StackName=name)['StackResourceSummaries'];template=cf.get_template(StackName=name)
  save('infra/'+name+'-stack.json',st);save('infra/'+name+'-resources.json',rs);save('infra/'+name+'-deployed-template.json',template)
  report['stacks'][name]={'status':st['StackStatus'],'termination_protection':st.get('EnableTerminationProtection'),'disable_rollback':st.get('DisableRollback'),'resources':len(rs)}
  if name=='squidlog-a':provider={x['LogicalResourceId']:x['PhysicalResourceId']for x in rs if x.get('PhysicalResourceId')}
  if name=='squidlog-b':consumer={x['LogicalResourceId']:x['PhysicalResourceId']for x in rs if x.get('PhysicalResourceId')}
  if name=='squidlog-store':storage={x['OutputKey']:x['OutputValue']for x in st['Outputs']}
ec2=a.client('ec2');bec2=b.client('ec2')
ids=[provider['SquidInstanceAz1'],provider['SquidInstanceAz2']]
instances=ec2.describe_instances(InstanceIds=ids);save('infra/provider-ec2.json',instances)
sg=ec2.describe_security_groups(GroupIds=[provider['ProxySecurityGroup'],provider['NlbSecurityGroup']]);save('infra/provider-sg.json',sg)
report['provider_ec2_count']=sum(len(x['Instances'])for x in instances['Reservations'])
report['instance_ids']=ids
report['imds_v2_required']=all(x['MetadataOptions']['HttpTokens']=='required'for r in instances['Reservations']for x in r['Instances'])
report['no_ssh_ingress']=all(not (x.get('IpProtocol')=='-1' or x.get('IpProtocol')=='tcp' and x.get('FromPort',0)<=22<=x.get('ToPort',0))for g in sg['SecurityGroups']for x in g.get('IpPermissions',[]))
routes=bec2.describe_route_tables(Filters=[{'Name':'vpc-id','Values':[consumer['ConsumerVpc']]}]);save('infra/consumer-route-tables.json',routes)
report['consumer_has_no_internet_default_route']=all(not (x.get('DestinationCidrBlock')=='0.0.0.0/0' or x.get('DestinationIpv6CidrBlock')=='::/0')for rt in routes['RouteTables']for x in rt['Routes'])
endpoints=bec2.describe_vpc_endpoints(VpcEndpointIds=[consumer['ConsumerEndpoint']]);save('infra/consumer-endpoint.json',endpoints)
report['endpoint_available']=all(x['State']=='available'for x in endpoints['VpcEndpoints'])
report['endpoint_az_count']=len(endpoints['VpcEndpoints'][0]['SubnetIds'])
fh=a.client('firehose').describe_delivery_stream(DeliveryStreamName=storage['StreamName']);save('infra/firehose.json',fh)
s3=a.client('s3');bucket=storage['BucketName']
for method,name in [('get_bucket_versioning','s3-versioning.json'),('get_public_access_block','s3-public-access-block.json'),('get_bucket_encryption','s3-encryption.json')]:save('infra/'+name,getattr(s3,method)(Bucket=bucket))
try:lifecycle=s3.get_bucket_lifecycle_configuration(Bucket=bucket)
except ClientError as e:
 if e.response['Error']['Code']!='NoSuchLifecycleConfiguration':raise
 lifecycle={'status':'NoSuchLifecycleConfiguration'}
save('infra/s3-lifecycle.json',lifecycle)
report['s3_has_no_expiration_rules']=lifecycle.get('status')=='NoSuchLifecycleConfiguration'
report['no_automatic_rollback']=all(report['stacks'][name]['disable_rollback']for name in report['stacks'])
save('environment-audit.json',report)
print(json.dumps(report,default=str))
assert report['accounts_different'] and report['provider_ec2_count']==2 and report['imds_v2_required'] and report['no_ssh_ingress'] and report['consumer_has_no_internet_default_route'] and report['endpoint_available'] and report['endpoint_az_count']==2 and report['s3_has_no_expiration_rules']
