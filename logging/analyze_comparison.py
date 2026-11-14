"""Use saved originals and Agent send acknowledgements to reconcile each measured phase."""
import argparse,collections,datetime,json,math,pathlib,re,tarfile
p=argparse.ArgumentParser();p.add_argument('--snapshot',default='final');args=p.parse_args()
root=pathlib.Path(__file__).parent/'private-evidence'
plan=json.loads((root/'comparison-plan.json').read_text())
rows=[];sender=[]
for archive in (root/'host-originals').glob(args.snapshot+'-*.tgz'):
    with tarfile.open(archive) as tar:
        for m in tar.getmembers():
            if m.isfile() and re.fullmatch('var/log/squid/access.log[^/]*',m.name):rows+=tar.extractfile(m).read().splitlines(keepends=True)
        text=tar.extractfile('var/log/aws-kinesis-agent/aws-kinesis-agent.log').read().decode()
        for line in text.splitlines():
            match=re.search(r'RecordBuffer\(id=(\d+),records=(\d+),bytes=(\d+)\) Records sent firehose .+: (\d+)\. Failed records: (\d+)',line)
            if match:
                timestamp=datetime.datetime.strptime(line[:28],'%Y-%m-%d %H:%M:%S.%f%z')
                sender.append({'host':archive.name,'utc':timestamp,'buffer_id':int(match[1]),'buffer_records':int(match[2]),'bytes':int(match[3]),'successful':int(match[4]),'failed':int(match[5])})
results=[]
for phase in plan['phases']:
    start=datetime.datetime.fromisoformat(phase['start']);end=datetime.datetime.fromisoformat(phase['end'])
    lines=[x for x in rows if (' request='+phase['id']+'-').encode() in x]
    sends=[x for x in sender if start<=x['utc']<end]
    metrics=json.loads((root/'metrics'/(phase['id']+'.json')).read_text())['metrics']
    sums={k:sum(x['Sum']for x in v['Datapoints']) for k,v in metrics.items()}
    incoming_records=int(sums.get('IncomingRecords',0));incoming_bytes=int(sums.get('IncomingBytes',0))
    per_record_sizes=[]
    if phase['aggregate_bytes']==0 or incoming_records==len(lines):
        per_record_sizes=list(map(len,lines));basis='Actual original line lengths; one line per record.'
    elif phase['aggregate_bytes']==5120:
        # All original lines must be below the size hint. Each resulting record is <=5120.
        assert max(map(len,lines),default=0)<=5120
        basis='Each accepted record <=5120B, confirmed with configured splitter and original line sizes.'
    elif all(x['buffer_records']==1 and x['successful']==1 and x['failed']==0 for x in sends):
        per_record_sizes=[x['bytes'] for x in sends];basis='One record per acknowledged Agent buffer; its byte size is in the actual Agent log.'
    else:basis='Per-record boundaries unavailable; do not infer exact rounded billing from the average.'
    billed=sum(math.ceil(n/5120)*5120 for n in per_record_sizes) if per_record_sizes else incoming_records*5120 if phase['aggregate_bytes']==5120 else None
    reconcile={'original_line_count':len(lines),'original_bytes':sum(map(len,lines)),'incoming_records':incoming_records,'incoming_bytes':incoming_bytes,'agent_ack_records':sum(x['successful']for x in sends),'agent_ack_bytes':sum(x['bytes']for x in sends),'original_bytes_match_metric':sum(map(len,lines))==incoming_bytes,'agent_records_match_metric':sum(x['successful']for x in sends)==incoming_records,'agent_bytes_match_metric':sum(x['bytes']for x in sends)==incoming_bytes}
    if per_record_sizes:reconcile['sizing_record_count_match']=len(per_record_sizes)==incoming_records
    results.append({**phase,**reconcile,'record_size_min':min(per_record_sizes)if per_record_sizes else None,'record_size_max':max(per_record_sizes)if per_record_sizes else None,'record_size_list':per_record_sizes or None,'record_size_average':incoming_bytes/incoming_records if incoming_records else None,'calculated_billed_bytes':billed,'calculated_direct_put_usd':billed/1024**3*0.037 if billed is not None else None,'billing_basis':basis,'agent_acknowledgements':sends,'cloudwatch_sums':sums})
report={'phases':results,'note':'Calculated 5KiB-rounded ingestion volume, not Cost Explorer measured charges.'}
(root/'comparison-results.json').write_text(json.dumps(report,default=str,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps([{k:r[k]for k in ['id','kind','original_line_count','original_bytes','incoming_records','incoming_bytes','agent_ack_records','agent_ack_bytes','calculated_billed_bytes','record_size_min','record_size_max','original_bytes_match_metric','agent_records_match_metric','agent_bytes_match_metric']}for r in results],indent=2))
if not all(r['original_bytes_match_metric'] and r['agent_records_match_metric'] and r['agent_bytes_match_metric']for r in results):raise SystemExit(1)
