"""Peer reverse download and per-shard merge on real Parquet and HTTP Range fixtures."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

import pytest
import pyarrow.parquet as pq
from test_http_extraction import trace, h

spec=importlib.util.spec_from_file_location('http_peer',
    Path(__file__).parents[1]/'src/vmforecast/google2019_peer.py')
p=importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def seed_fixture(trace):
    work,out,cfg,files,requests=trace
    h.atomic_json(work/'config.json',cfg)
    h.atomic_json(work/'sources.json',files)
    h.ingest(work,out,cfg,files,'prefix')
    # Seed is a snapshot of a real session already past prefix and partway into data.
    h.ingest(work,out,cfg,files[:1],'data')
    seed=work.parent/'seed'
    p.make_seed(work,seed)
    return seed


def download(trace,seed,root,max_new=0,order='reverse',cli=False):
    args=['download','--seed',str(seed),'--root',str(root),'--order',order,
          '--memory-mib','128','--rss-mib','1024','--batch-rows','7','--threads','1',
          '--max-new-shards',str(max_new),'--retry-hours','0']
    if cli:
        script=Path(__file__).parents[1]/'tools/google2019_peer.py'
        result=subprocess.run([sys.executable,str(script),*args],capture_output=True,text=True,timeout=60)
        assert result.returncode==0,result.stdout+result.stderr
    else:
        # Set up the same root/settings but run the worker in-process for fault injection.
        root.mkdir()
        for name in p.CONTROLS:
            shutil.copyfile(seed/name,root/name)
        settings=vars(p.parser().parse_args(args))
        for k in ('command','seed','root'):
            settings.pop(k)
        settings['stage_gb']=65
        h.atomic_json(root/'peer.json',settings)
        p.tail_worker(root)


def counts(work,cfg):
    db=h.connect_db(work,cfg)
    result={table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            for table in h.TABLES if db.execute('SELECT 1 FROM information_schema.tables '
                'WHERE table_name=?',[table]).fetchone()}
    done=db.execute("SELECT * FROM done WHERE phase='data' ORDER BY name").fetchall()
    db.close()
    return result,done


def test_reverse_overlap_idempotence_and_main_resume(trace,monkeypatch):
    work,out,cfg,files,requests=trace
    seed=seed_fixture(trace)
    # Main advances from its old snapshot while the laptop works independently.
    h.ingest(work,out,cfg,files[:3],'data')
    root=work.parent/'peer'
    download(trace,seed,root,max_new=4,cli=True)
    _,_,_,metas=p.bundle_index(root)
    assert {m['source']['name'] for m in metas.values()}=={s['name'] for s in files[2:]}
    merged=p.merge(work,root,reserve_gb=1)
    assert merged=={'imported':3,'skipped':1}
    before=counts(work,cfg)
    assert len(before[1])==len(files)
    assert p.merge(work,root,reserve_gb=1)=={'imported':0,'skipped':4}
    assert counts(work,cfg)==before
    def forbidden(*a,**kw):
        pytest.fail('Merged sources must not be downloaded again')
    monkeypatch.setattr(h,'fetch_sparse',forbidden)
    h.ingest(work,out,cfg,files,'data')
    assert counts(work,cfg)==before
    h.export(work,out,cfg,files)
    assert h.verify_dataset(out)['instances']==3


@pytest.mark.parametrize('mode',['research','core'])
def test_merged_raw_rows_equal_single_computer(trace,mode):
    work,out,cfg,files,_=trace
    cfg['columns']=mode
    seed=seed_fixture(trace)
    root=work.parent/'peer'
    download(trace,seed,root)
    p.merge(work,root,reserve_gb=1)
    baseline_work,baseline_out=work.parent/'single-work',out.parent/'single-out'
    baseline_work.mkdir();baseline_out.mkdir()
    h.ingest(baseline_work,baseline_out,cfg,files,'prefix')
    h.ingest(baseline_work,baseline_out,cfg,files,'data')
    a=h.connect_db(work,cfg);b=h.connect_db(baseline_work,cfg)
    for table in h.TABLES:
        # EXCEPT ALL preserves true repeated rows; no DISTINCT deduplication is used.
        a.register('reference',b.execute(f'SELECT * FROM {table}').fetch_arrow_table())
        assert a.execute(f'SELECT count(*) FROM ((SELECT * FROM {table} EXCEPT ALL '
                         'SELECT * FROM reference) UNION ALL (SELECT * FROM reference '
                         f'EXCEPT ALL SELECT * FROM {table}))').fetchone()[0]==0
        a.unregister('reference')
    a.close();b.close()


@pytest.mark.parametrize('kind',['tar','zip'])
def test_archive_import_streams_without_unpacking_bundle(trace,kind):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root)
    archive=work.parent/('bundle.tar.gz' if kind=='tar' else 'bundle.zip')
    if kind=='tar':
        with tarfile.open(archive,'w:gz') as stream:
            stream.add(root,arcname='peer')
    else:
        with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as stream:
            for path in root.rglob('*'):
                if path.is_file():stream.write(path,'peer/'+path.relative_to(root).as_posix())
    assert p.merge(work,archive,reserve_gb=1)['imported']==5
    assert not list(work.glob('peer-import-*'))
    assert p.merge(work,archive,reserve_gb=1)['skipped']==5


def test_corrupted_piece_is_not_committed(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    payload=next((root/'pieces').glob('*.parquet'))
    payload.write_bytes(b'corrupted')
    before=counts(work,cfg)
    with pytest.raises(p.h.Paused):p.merge(work,root,reserve_gb=1)
    assert counts(work,cfg)==before


def test_incompatible_cohort_is_rejected_before_import(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=2)
    db=h.connect_db(work,cfg)
    db.execute('UPDATE candidates SET collection_id=collection_id+100')
    db.close()
    before=counts(work,cfg)
    with pytest.raises(p.h.Paused,match='mismatch'):p.merge(work,root,reserve_gb=1)
    assert counts(work,cfg)==before


def test_transaction_failure_then_retry(trace,monkeypatch):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    before=counts(work,cfg)
    connect=p.h.connect_db
    class FailedCommit:
        def __init__(self,db):self.db=db
        def __getattr__(self,name):return getattr(self.db,name)
        def execute(self,sql,*args):
            if sql.startswith('INSERT INTO done '):raise RuntimeError('interrupted before checkpoint')
            return self.db.execute(sql,*args)
    monkeypatch.setattr(p.h,'connect_db',lambda *a,**kw:FailedCommit(connect(*a,**kw)))
    with pytest.raises(RuntimeError):p.merge(work,root,reserve_gb=1)
    assert counts(work,cfg)==before
    monkeypatch.setattr(p.h,'connect_db',connect)
    assert p.merge(work,root,reserve_gb=1)['imported']==1


def test_interruption_after_commit_is_not_reinserted(trace,monkeypatch):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    before=counts(work,cfg)
    insert=p.import_piece
    def after_commit(*a,**kw):
        insert(*a,**kw)
        raise RuntimeError('interrupted after commit')
    monkeypatch.setattr(p,'import_piece',after_commit)
    with pytest.raises(RuntimeError):p.merge(work,root,reserve_gb=1)
    committed=counts(work,cfg)
    assert len(committed[1])==len(before[1])+1
    monkeypatch.setattr(p,'import_piece',insert)
    assert p.merge(work,root,reserve_gb=1)=={'imported':0,'skipped':1}
    assert counts(work,cfg)==committed


def test_process_exit_after_commit_recovers_wal(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    module=Path(__file__).parents[1]/'src/vmforecast/google2019_peer.py'
    program='''import importlib.util,json,os,sys
from pathlib import Path
s=importlib.util.spec_from_file_location('peer_child',sys.argv[1]);p=importlib.util.module_from_spec(s);s.loader.exec_module(p)
work=Path(sys.argv[2]);root=Path(sys.argv[3]);meta=next(iter(p.bundle_index(root)[3].values()))
db=p.h.connect_db(work,json.loads((work/'config.json').read_text()))
p.import_piece(db,root/'pieces'/(meta['id']+'.parquet'),meta)
os._exit(73)
'''
    result=subprocess.run([sys.executable,'-c',program,str(module),str(work),str(root)],
                           capture_output=True,text=True,timeout=60)
    assert result.returncode==73,result.stderr
    assert (work/'stage.duckdb.wal').exists()
    assert p.merge(work,root,reserve_gb=1)=={'imported':0,'skipped':1}
    assert len(counts(work,cfg)[1])==2


def test_hard_payload_ceiling_and_importable_partial_result(trace,monkeypatch):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    # First piece completes, the second is cut off before any over-budget write.
    original=p.BoundedFile
    opened=0
    class Limited(original):
        def __init__(self,path,limit):
            nonlocal opened
            opened+=1
            super().__init__(path,limit if opened==1 else 10)
    monkeypatch.setattr(p,'BoundedFile',Limited)
    with pytest.raises((p.SpaceFull,OSError)):download(trace,seed,root)
    assert len(list((root/'pieces').glob('*.json')))==1
    assert not list((root/'pieces').glob('*.partial'))
    assert p.merge(work,root,reserve_gb=1)['imported']==1
    # Exact byte ceiling is enforced before writing, independent of supervisor polling.
    path=work.parent/'limited'
    with original(path,5) as stream:
        stream.write(b'12345')
        with pytest.raises(p.SpaceFull):stream.write(b'6')
    assert path.stat().st_size==5


def test_seed_from_work_archive_and_preflight_budget(trace):
    work,out,cfg,files,_=trace
    seed_fixture(trace)
    archive=work.parent/'old-work.tar.gz'
    with tarfile.open(archive,'w:gz') as stream:stream.add(work,arcname='old-work')
    seed2=work.parent/'archive-seed'
    p.seed_from_archive(archive,seed2,disk_gb=30)
    assert p.load_controls(p.seed_data(seed2))[0]['fingerprint']==p.load_controls(p.seed_data(work.parent/'seed'))[0]['fingerprint']
    with pytest.raises(p.SpaceFull):p.seed_from_archive(archive,work.parent/'too-small',disk_gb=.01)
    assert not list(work.parent.glob('peer-seed-work-*'))


def test_active_main_is_locked(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    with p.lock(work/'run.lock'):
        with pytest.raises(p.h.Paused,match='holds'):p.merge(work,root,reserve_gb=1)


def test_uncommitted_orphan_is_ignored(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    (root/'pieces'/'orphan.parquet').write_bytes(b'not committed')
    assert p.merge(work,root,reserve_gb=1)['imported']==1


@pytest.mark.parametrize('change',['window','generation','columns'])
def test_incompatible_dataset_parameters_rejected(trace,change):
    work,out,cfg,files,_=trace
    seed_fixture(trace)
    original_cfg=(work/'config.json').read_bytes()
    original_sources=(work/'sources.json').read_bytes()
    changed=dict(cfg)
    if change=='window':
        changed['t1']+=3600_000_000
        h.atomic_json(work/'config.json',changed)
    elif change=='columns':
        changed['columns']='core'
        h.atomic_json(work/'config.json',changed)
    else:
        altered=json.loads(original_sources)
        altered[-1]['generation']='different-generation'
        h.atomic_json(work/'sources.json',altered)
    seed=work.parent/'other-seed'
    p.make_seed(work,seed)
    (work/'config.json').write_bytes(original_cfg)
    (work/'sources.json').write_bytes(original_sources)
    root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    before=counts(work,cfg)
    with pytest.raises(p.h.Paused,match='mismatch'):p.merge(work,root,reserve_gb=1)
    assert counts(work,cfg)==before


def test_merge_cli_and_updated_seed_prunes_acknowledged_pieces(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1,cli=True)
    script=Path(__file__).parents[1]/'tools/google2019_peer.py'
    result=subprocess.run([sys.executable,str(script),'merge','--work',str(work),
                           '--bundle',str(root),'--reserve-gb','1'],capture_output=True,text=True,timeout=60)
    assert result.returncode==0,result.stdout+result.stderr
    assert json.loads(result.stdout)['imported']==1
    old_piece=next((root/'pieces').glob('*.json'))
    old_payload=old_piece.with_suffix('.parquet')
    updated=work.parent/'updated-seed'
    p.make_seed(work,updated)
    download(trace,updated,root,max_new=1,cli=True)
    assert not old_piece.exists() and not old_payload.exists()
    assert len(list((root/'pieces').glob('*.json')))==1


def test_zero_filtered_rows_is_valid_progress(trace):
    import pyarrow as pa
    work,out,cfg,files,_=trace
    name='instance_events-000000009999.parquet.gz'
    path=work.parent/'source'/name
    table=pq.read_table(work.parent/'source'/files[2]['name']).slice(0,1)
    index=table.schema.get_field_index('collection_id')
    table=table.set_column(index,table.schema.field(index),pa.array([999],type=pa.int64()))
    pq.write_table(table,path)
    obj=dict(table='instance_events',name=name,size=path.stat().st_size,
             url=files[2]['url'].rsplit('/',1)[0]+'/'+name)
    files.append(obj)
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    meta=p.bundle_index(root)[3].popitem()[1]
    assert meta['filtered_rows']==0 and meta['source_rows']==1
    assert p.merge(work,root,reserve_gb=1)['imported']==1
    db=h.connect_db(work,cfg)
    assert db.execute('SELECT count(*) FROM instance_events').fetchone()[0]==0
    assert db.execute("SELECT rows FROM done WHERE phase='data' AND name=?",[name]).fetchone()[0]==1
    db.close()


def test_missing_payload_and_unsafe_archive_rejected(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    next((root/'pieces').glob('*.parquet')).unlink()
    with pytest.raises(p.h.Paused,match='missing'):p.merge(work,root,reserve_gb=1)
    archive=work.parent/'unsafe.zip'
    with zipfile.ZipFile(archive,'w') as stream:
        stream.writestr('../escape',b'bad')
    with pytest.raises(p.h.Paused,match='Unsafe'):p.merge(work,archive,reserve_gb=1)
    assert not (work.parent/'escape').exists()


def test_true_duplicate_source_events_are_preserved(trace):
    import pyarrow as pa
    work,out,cfg,files,_=trace
    event_path=work.parent/'source'/files[2]['name']
    events=pq.read_table(event_path)
    pq.write_table(pa.concat_tables([events,events.slice(0,1)]),event_path)
    files[2]['size']=event_path.stat().st_size
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root)
    p.merge(work,root,reserve_gb=1)
    p.merge(work,root,reserve_gb=1)
    db=h.connect_db(work,cfg)
    assert db.execute('SELECT count(*) FROM instance_events WHERE '
                      'collection_id=1 AND type=3 AND time=0').fetchone()[0]==2
    db.close()


def test_missing_stage_is_not_replaced_with_empty_database(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'peer'
    download(trace,seed,root,max_new=1)
    (work/'stage.duckdb').unlink()
    with pytest.raises(p.h.Paused,match='absent'):p.merge(work,root,reserve_gb=1)
    with pytest.raises(p.h.Paused,match='absent'):p.make_seed(work,work.parent/'bad-seed')
    assert not (work/'stage.duckdb').exists()


def test_peer_refuses_limit_above_30gb_before_creating_root(trace):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work.parent/'over-budget'
    with pytest.raises(p.h.Paused,match='30 GB'):
        p.main(['download','--seed',str(seed),'--root',str(root),'--disk-gb','31'])
    assert not root.exists()


def test_bundle_inside_work_is_not_double_counted_against_disk_budget(trace,monkeypatch):
    work,out,cfg,files,_=trace
    seed=seed_fixture(trace);root=work/'received-peer'
    download(trace,seed,root,max_new=1)
    monkeypatch.setattr(p.h,'tree_bytes',lambda path:2*p.GB if Path(path).resolve()==work else 20*p.GB)
    assert p.merge(work,root,disk_gb=5,reserve_gb=1)['imported']==1
