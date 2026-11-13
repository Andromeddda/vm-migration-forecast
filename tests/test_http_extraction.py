"""Exercise real HTTP Range, Parquet decoding, transactions, packing, and backtest handoff."""
import http.server
import importlib.util
import json
from pathlib import Path
import threading

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

pytest.importorskip('duckdb')
spec = importlib.util.spec_from_file_location('http_extract',
    Path(__file__).parents[1]/'src/vmforecast/google2019_http.py')
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)


@pytest.fixture
def trace(tmp_path):
    source, work, output = [tmp_path/x for x in ('source','work','output')]
    for x in (source,work,output):
        x.mkdir()
    t0, t1 = 600_000_000,600_000_000+3*3600_000_000
    usage = []
    for cid in (1,2,3):
        for k in range(36):
            # Object 2 has a future gap; must not disappear silently from raw data.
            if cid==2 and k==20:
                continue
            usage.append(dict(start_time=t0+k*h.STEP,end_time=t0+(k+1)*h.STEP,
                collection_id=cid,instance_index=0,machine_id=100,collection_type=0,
                alloc_collection_id=0,average_usage={'cpus':.1,'memory':.2},
                maximum_usage={'cpus':.3,'memory':.4},sample_rate=1.,
                cpu_usage_distribution=[.01,.1,.3]))
    events = [dict(time=0,type=3,collection_id=c,instance_index=0,machine_id=100,
                   missing_type=0,resource_request={'cpus':.3,'memory':.4}) for c in (1,2,3)]
    # Instance 3 restarts after the prefix: not strict-backtest valid.
    events.append(dict(events[-1],time=t0+2*3600_000_000,type=5))
    tables = {'instance_usage':usage,'instance_events':events,
        'machine_events':[dict(time=0,type=0,machine_id=100,capacity={'cpus':1.,'memory':1.})],
        'machine_attributes':[dict(time=0,machine_id=100,name='platform',value='x',deleted=False)],
        'collection_events':[dict(time=0,type=0,collection_id=c) for c in (1,2,3)]}
    files = []
    for table, rows in tables.items():
        chunks = [rows[::2],rows[1::2]] if table=='instance_usage' else [rows]
        for i, chunk in enumerate(chunks):
            name = f'{table}-{i:012d}.parquet.gz'
            pq.write_table(pa.Table.from_pylist(chunk),source/name,row_group_size=11,compression='snappy')
            files.append(dict(name=name,table=table,size=(source/name).stat().st_size))
    requests = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            assert 'Authorization' not in self.headers
            path = source/self.path.split('?')[0].lstrip('/')
            data = path.read_bytes()
            start,end = map(int,self.headers['Range'].split('=')[1].split('-'))
            requests.append((path.name,start,end))
            self.send_response(206)
            self.send_header('Content-Range',f'bytes {start}-{end}/{len(data)}')
            self.send_header('Content-Length',str(end-start+1))
            self.end_headers()
            self.wfile.write(data[start:end+1])
        def log_message(self,*args):
            pass
    server = http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    for obj in files:
        obj['url']=f'http://127.0.0.1:{server.server_port}/{obj["name"]}'
    cfg = dict(t0=t0,t1=t1,prefix_hours=1,days=3/24,cell='a',seed=7,max_objects=3,
        max_shard_mib=128,batch_rows=7,retry_hours=0,columns='research',memory_mib=128,
        rss_mib=1024,threads=1,spill_gb=1,reserve_gb=1,stage_gb=2,disk_gb=100,
        target_gb=.1,output=str(output),work=str(work))
    yield work,output,cfg,files,requests
    server.shutdown()
    thread.join()
    server.server_close()


def test_http_pipeline_resume_and_backtest(trace,tmp_path):
    work,out,cfg,files,requests = trace
    h.worker(work,out,cfg,files)
    manifest = json.loads((out/'manifest.json').read_text())
    assert manifest['instances']==3
    assert manifest['full_window_backtest_valid_instances']==1
    assert manifest['source_row_counts']['instance_usage']==107
    assert h.tree_bytes(out)<cfg['target_gb']*h.GB
    q = pq.read_table(out/'quality.parquet').to_pandas().set_index('collection_id')
    assert q.loc[1,'valid']
    assert not q.loc[2,'continuous']
    assert not q.loc[3,'lifecycle_ok']
    usage = pq.read_table(out/'usage.parquet'/'1_0.parquet')
    assert usage.num_rows==36
    assert 'maximum_usage' in usage.column_names
    assert usage['cpu'].to_pylist()==[.1]*36
    hashes = {p.relative_to(out):h.checksum(p) for p in out.rglob('*.parquet') if p.is_file()}
    count = len(requests)
    h.worker(work,out,cfg,files)
    assert len(requests)==count  # Successful shards are never downloaded twice.
    assert hashes=={p.relative_to(out):h.checksum(p) for p in out.rglob('*.parquet') if p.is_file()}
    h.prepare(out,tmp_path/'prepared',objects=3,hours=3)
    from vmforecast.data import load_workload
    from vmforecast import WindowSignal,SimulationConfig
    from vmforecast.experiments import run_saved
    workload = load_workload(tmp_path/'prepared')
    assert workload.demand.shape==(36,1,2)
    summary,_ = run_saved(workload,WindowSignal(window=4,mode='max'),
                         SimulationConfig(warmup=12),tmp_path/'runs')
    assert summary


def test_sparse_projection_and_changed_outputs(trace):
    work,out,cfg,files,requests = trace
    obj = next(x for x in files if x['table']=='instance_usage')
    path = work/'sparse.parquet'
    rows,transferred = h.fetch_sparse(obj,path,h.PREFIX_COLUMNS,cfg)
    r = pq.ParquetFile(path)
    batches = list(r.iter_batches(columns=h.PREFIX_COLUMNS,use_threads=False))
    assert sum(b.num_rows for b in batches)==rows
    assert 'maximum_usage' not in batches[0].schema.names
    r.close()
    path.unlink()
    h.worker(work,out,cfg,files)
    (out/'usage.parquet'/'1_0.parquet').write_bytes(b'corrupt')
    with pytest.raises(h.Paused,match='damaged'):
        h.export(work,out,cfg,files)


def test_core_projection_is_readable(trace):
    work,out,cfg,files,_ = trace
    cfg['columns']='core'
    h.worker(work,out,cfg,files)
    usage = pq.read_table(out/'usage.parquet'/'1_0.parquet')
    assert usage.num_rows==36
    assert 'maximum_usage' not in usage.column_names
    assert 'cpu' in usage.column_names
    events = pq.read_table(out/'events.parquet'/'1_0.parquet')
    assert events['cpu_request'].to_pylist()==[.3]


def test_standalone_cli_supervised_end_to_end(trace):
    import subprocess
    import sys
    work,out,cfg,files,_ = trace
    h.atomic_json(work/'sources.json',files)
    script = Path(__file__).parents[1]/'tools/download_google2019.py'
    cmd = [sys.executable,str(script),'download','--work',str(work),'--output',str(out),
           '--days','0.125','--max-objects','3','--target-gb','0.1',
           '--reserve-gb','1','--memory-mib','128','--rss-mib','1024',
           '--threads','1','--batch-rows','7']
    result = subprocess.run(cmd,capture_output=True,text=True,timeout=60)
    assert result.returncode==0,result.stdout+result.stderr
    assert json.loads((out/'manifest.json').read_text())['instances']==3
    assert not (work/'stage.duckdb').exists()  # Space released only after checksum verification.
    # Resume through the CLI too, not just helper functions.
    result = subprocess.run(cmd,capture_output=True,text=True,timeout=60)
    assert result.returncode==0,result.stdout+result.stderr
    assert not (work/'stage.duckdb').exists()


def test_interruption_rolls_back_whole_shard(trace,monkeypatch):
    work,out,cfg,files,requests = trace
    original = h.fetch_sparse
    calls = 0
    def fail(*args,**kwargs):
        nonlocal calls
        calls += 1
        if calls==2:
            raise h.Paused('simulated interruption')
        return original(*args,**kwargs)
    monkeypatch.setattr(h,'fetch_sparse',fail)
    with pytest.raises(h.Paused):
        h.ingest(work,out,cfg,files,'prefix')
    # Close/recover the persistent DB after failure; only the first shard is committed.
    monkeypatch.setattr(h,'fetch_sparse',original)
    h.worker(work,out,cfg,files)
    assert json.loads((out/'manifest.json').read_text())['instances']==3


def test_exact_pair_budget_and_low_disk(trace,monkeypatch):
    work,out,cfg,files,requests = trace
    h.ingest(work,out,cfg,files,'prefix')
    h.ingest(work,out,cfg,files,'data')
    h.export(work,out,cfg,files)
    # A second independent export, fitted to one complete instance pair + metadata.
    import shutil
    import duckdb
    rows = pq.read_table(out/'checksums.parquet').to_pylist()
    base = sum((out/(t+'.parquet')).stat().st_size for t in
               ('machine_events','machine_attributes','collection_events'))
    cap = 16*h.MIB+base+max(x['bytes'] for x in rows)
    shutil.rmtree(out)
    out.mkdir()
    db = h.connect_db(work,cfg)
    db.execute('DELETE FROM exported')
    db.close()
    cfg['target_gb']=cap/h.GB
    h.export(work,out,cfg,files)
    manifest = json.loads((out/'manifest.json').read_text())
    assert manifest['instances']==1
    assert h.tree_bytes(out)<=cap
    assert len(list((out/'usage.parquet').glob('*.parquet')))==1
    assert len(list((out/'events.parquet').glob('*.parquet')))==1
    usage = shutil.disk_usage(work)
    monkeypatch.setattr(h.shutil,'disk_usage',lambda _:usage._replace(free=0))
    with pytest.raises(h.Paused,match='Low filesystem'):
        h.guard(work,out,cfg)


def test_http_does_not_accept_ignored_range(monkeypatch):
    import io
    class Response(io.BytesIO):
        status=200
        headers={}
    monkeypatch.setattr(h.urllib.request,'urlopen',lambda *a,**k:Response(b'full file'))
    with pytest.raises(h.Paused,match='ignored'):
        h.get_bytes('http://example.invalid/file',expected=8,start=0,retry_hours=0)


def test_retry_incomplete_http_response(monkeypatch):
    import io
    import http.client
    calls=[]
    class Response(io.BytesIO):
        status=206
        headers={'Content-Range':'bytes 0-7/10'}
    def urlopen(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:
            raise http.client.IncompleteRead(b'ab',8)
        return Response(b'abcdefgh')
    monkeypatch.setattr(h.urllib.request,'urlopen',urlopen)
    monkeypatch.setattr(h.time,'sleep',lambda _:None)
    assert h.get_bytes('http://example.invalid/file',expected=8,start=0,retry_hours=.01)==b'abcdefgh'
    assert len(calls)==2


def test_publish_across_filesystems(tmp_path,monkeypatch):
    import errno
    src,dst=tmp_path/'source',tmp_path/'destination'
    src.write_bytes(b'complete data')
    replace=Path.replace
    def cross_device(self,target):
        if self==src:
            raise OSError(errno.EXDEV,'cross-device')
        return replace(self,target)
    monkeypatch.setattr(Path,'replace',cross_device)
    h.publish(src,dst)
    assert dst.read_bytes()==b'complete data'
    assert not src.exists()
    assert not (tmp_path/'destination.partial').exists()


def test_interrupted_pair_publication_recovers(trace,monkeypatch):
    work,out,cfg,files,_=trace
    h.ingest(work,out,cfg,files,'prefix')
    h.ingest(work,out,cfg,files,'data')
    original=h.publish
    def fail_between_pair(source,destination):
        if destination.parent.name=='events.parquet':
            raise h.Paused('interrupted between usage and events publication')
        return original(source,destination)
    monkeypatch.setattr(h,'publish',fail_between_pair)
    with pytest.raises(h.Paused):
        h.export(work,out,cfg,files)
    assert not (out/'manifest.json').exists()
    monkeypatch.setattr(h,'publish',original)
    h.export(work,out,cfg,files)
    assert h.verify_dataset(out)['instances']==3


def test_worker_ram_ceiling_terminates_child(tmp_path,monkeypatch):
    import psutil
    class FakeChild:
        def memory_info(self):
            return type('Mem',(),{'rss':2*h.MIB})()
        def children(self,recursive=False):
            return []
    class Process:
        pid=123456
        done=False
        def poll(self):
            return -15 if self.done else None
        def wait(self,timeout=None):
            self.done=True
            return -15
    killed=[]
    monkeypatch.setattr(h.subprocess,'Popen',lambda *a,**k:Process())
    monkeypatch.setattr(psutil,'Process',lambda _:FakeChild())
    monkeypatch.setattr(h.os,'killpg',lambda *a:killed.append(a))
    monkeypatch.setattr(h.time,'sleep',lambda _:None)
    with pytest.raises(h.Paused,match='RSS'):
        h.supervise(tmp_path,tmp_path,{'rss_mib':1})
    assert killed==[(123456,h.signal.SIGTERM)]
