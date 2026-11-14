"""Compare actual EC2 Squid log bytes against S3 originals; no AWS calls or log sending."""
import argparse,collections,hashlib,json,pathlib,re,tarfile
p=argparse.ArgumentParser();p.add_argument('--evidence',default=str(pathlib.Path(__file__).parent/'private-evidence'));p.add_argument('--snapshot',required=True);a=p.parse_args()
e=pathlib.Path(a.evidence)
source=[];files=[]
for archive in sorted((e/'host-originals').glob(a.snapshot+'-*.tgz')):
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            if member.isfile() and re.fullmatch(r'var/log/squid/access\.log[^/]*',member.name):
                body=tar.extractfile(member).read();source.extend(body.splitlines(keepends=True));files.append({'archive':archive.name,'file':member.name,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()})
if not files: raise SystemExit('No host originals found: do not claim successful verification')
dest=[];objects=[]
for path in sorted((e/'s3-originals'/'squid').rglob('*')):
    if path.is_file():
        body=path.read_bytes();dest.extend(body.splitlines(keepends=True));objects.append({'key':str(path.relative_to(e/'s3-originals')),'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()})
sc=collections.Counter(source);dc=collections.Counter(dest)
missing=sc-dc;extra=dc-sc
rid=lambda line:re.search(rb' request=([^\s]+)',line)
src_ids=collections.Counter(rid(x).group(1).decode() for x in source if rid(x))
dst_ids=collections.Counter(rid(x).group(1).decode() for x in dest if rid(x))
expected=[]
for path in (e/'requests').glob('*.json'):
    req=json.loads(path.read_text(encoding='utf-8'))
    expected.extend(x['request_id'] for x in req.get('response',{}).get('requests',[]) if x.get('status')==200)
id_missing=[x for x in expected if dst_ids[x]!=1]
by_source=collections.Counter()
for line in dest:
    m=re.search(rb' source=(\S+)',line)
    by_source[m.group(1).decode() if m else 'UNKNOWN']+=1
lf_ok=all(x.endswith(b'\n') and not x.endswith(b'\r\n') for x in dest)
report={'snapshot':a.snapshot,'source_lines':len(source),'s3_lines':len(dest),'source_bytes':sum(map(len,source)),'s3_bytes':sum(map(len,dest)),'missing_lines':sum(missing.values()),'extra_lines':sum(extra.values()),'all_lines_end_lf':lf_ok,'request_ids_expected':len(expected),'request_ids_missing_or_not_once':id_missing,'duplicate_request_ids':{k:v for k,v in dst_ids.items() if v>1 and k!='-'},'source_counts':dict(by_source),'source_files':files,'s3_objects':objects,'source_line_size_min':min(map(len,source)),'source_line_size_max':max(map(len,source)),'verified':not missing and not extra and not id_missing and lf_ok and set(by_source)=={'az1','az2'}}
(e/('verification-'+a.snapshot+'.json')).write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
print(json.dumps(report,ensure_ascii=False))
raise SystemExit(0 if report['verified'] else 1)
