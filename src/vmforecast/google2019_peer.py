"""Offline seed, bounded reverse data acquisition, and idempotent per-shard import.

No prefix is downloaded by a peer. No raw-table databases are unioned or deduplicated.
The main downloader's (phase,name) checkpoint is the import transaction boundary.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import io
import inspect
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import sys
import tarfile
import tempfile
import zipfile

_spec=importlib.util.spec_from_file_location('vmforecast_http_peer_core',
    Path(__file__).with_name('google2019_http.py'))
h=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(h)
GB,MIB=h.GB,h.MIB
CONTRACT='google2019-http-data-v1'
MAX_CONTROL=16*MIB
MAX_META=8192
CONTROLS=('seed.json','sources.json','cohort.parquet')


class SpaceFull(h.Paused):
    pass


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def source_id(obj):
    return digest(obj)


def identity(cfg,sources,cohort):
    import duckdb
    import pyarrow
    keys=('cell','t0','t1','prefix_hours','seed','max_objects','columns','target_gb')
    return {'contract':CONTRACT,'cfg':{k:cfg[k] for k in keys},
            'sources':digest(sources),'cohort':digest(cohort),
            'transform':digest({'sql':inspect.getsource(h.filtered_data_sql),
                'where':inspect.getsource(h.usage_where),'projection':inspect.getsource(h.source_columns),
                'core_usage':h.CORE_USAGE,'core_events':h.CORE_EVENTS}),
            'duckdb':duckdb.__version__,'pyarrow':pyarrow.__version__}


@contextlib.contextmanager
def lock(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as stream:
        try:
            fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise h.Paused(f'Another process holds {path}; stop it first') from exc
        yield stream


def read_json(path,limit=MAX_CONTROL):
    if path.stat().st_size>limit:
        raise h.Paused(f'Oversized control file: {path}')
    return json.loads(path.read_bytes())


def cohort_rows(db):
    if not db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='candidates'").fetchone():
        raise h.Paused('No committed candidates; finish prefix selection before creating a peer seed')
    rows=db.execute('SELECT collection_id,instance_index,rank FROM candidates ORDER BY rank').fetchall()
    if (not rows or any(None in r for r in rows) or
        [r[2] for r in rows]!=list(range(1,len(rows)+1)) or
        len({r[:2] for r in rows})!=len(rows)):
        raise h.Paused('A committed, unique nonempty cohort is required')
    return [list(r) for r in rows]


def make_seed(work,destination):
    """Read a stopped main work directory; publish only small immutable control files."""
    work,destination=Path(work).resolve(),Path(destination).resolve()
    if destination==work or work in destination.parents or destination in work.parents:
        raise h.Paused('Seed destination must be separate from source work')
    if destination.exists():
        raise h.Paused('Seed destination already exists; use a new empty destination')
    if not (work/'stage.duckdb').is_file():
        raise h.Paused('Main staging DB is absent; it may already have been finalized and cleaned')
    cfg=read_json(work/'config.json')
    sources=read_json(work/'sources.json')
    with lock(work/'run.lock'):
        db=h.connect_db(work,cfg)
        try:
            required={s['name'] for s in sources if s['table']=='instance_usage'}
            completed={r[0] for r in db.execute("SELECT name FROM done WHERE phase='prefix'").fetchall()}
            if not required or not required<=completed:
                raise h.Paused('Prefix is incomplete; seed cannot be prepared')
            cohort=cohort_rows(db)
            baseline=sorted(r[0] for r in db.execute("SELECT name FROM done WHERE phase='data'").fetchall())
            ident=identity(cfg,sources,cohort)
            destination.parent.mkdir(parents=True,exist_ok=True)
            tmp=Path(tempfile.mkdtemp(prefix='peer-seed-',dir=destination.parent))
            try:
                h.copy_parquet(db,'SELECT collection_id,instance_index,rank FROM candidates ORDER BY rank',
                               tmp/'cohort.parquet')
                h.atomic_json(tmp/'sources.json',sources)
                seed={'version':1,'identity':ident,'fingerprint':digest(ident),'done_data':baseline,
                      'cohort_file_sha256':h.checksum(tmp/'cohort.parquet')}
                h.atomic_json(tmp/'seed.json',seed)
                tmp.replace(destination)
            finally:
                if tmp.exists():
                    shutil.rmtree(tmp)
        finally:
            db.close()
    return seed


def safe_name(name):
    p=PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts or '\\' in name:
        raise h.Paused(f'Unsafe archive path: {name}')
    return '/'.join(part for part in p.parts if part!='.')


@contextlib.contextmanager
def members(bundle):
    """Yield streams, never extract arbitrary archive paths. Tar is read sequentially."""
    bundle=Path(bundle)
    if bundle.is_dir():
        def directory():
            for path in sorted(bundle.rglob('*')):
                if path.is_symlink():
                    raise h.Paused(f'Symlink in bundle: {path}')
                if path.is_file():
                    with path.open('rb') as stream:
                        yield path.relative_to(bundle).as_posix(),path.stat().st_size,stream
        yield directory()
    elif zipfile.is_zipfile(bundle):
        with zipfile.ZipFile(bundle) as archive:
            def zipped():
                for info in archive.infolist():
                    name=safe_name(info.filename)
                    if not info.is_dir():
                        with archive.open(info) as stream:
                            yield name,info.file_size,stream
            yield zipped()
    else:
        with tarfile.open(bundle,mode='r|*') as archive:
            def tarred():
                for info in archive:
                    name=safe_name(info.name)
                    if info.issym() or info.islnk():
                        raise h.Paused(f'Link in archive: {name}')
                    if info.isfile():
                        with archive.extractfile(info) as stream:
                            yield name,info.size,stream
            yield tarred()


def logical_name(name,controls=CONTROLS):
    parts=PurePosixPath(safe_name(name)).parts
    if not parts:
        return None
    if parts[-1] in controls:
        return parts[-1]
    if len(parts)>=2 and parts[-2]=='pieces':
        return 'pieces/'+parts[-1]
    return None


def load_controls(data):
    import pyarrow.parquet as pq
    if set(data)!=set(CONTROLS):
        raise h.Paused('Seed needs seed.json, sources.json and cohort.parquet')
    seed=json.loads(data['seed.json'])
    sources=json.loads(data['sources.json'])
    cohort=[list(row.values()) for row in pq.read_table(io.BytesIO(data['cohort.parquet']),
             columns=['collection_id','instance_index','rank']).to_pylist()]
    if seed.get('version')!=1 or seed['identity'].get('contract')!=CONTRACT:
        raise h.Paused('Unsupported peer data contract')
    if hashlib.sha256(data['cohort.parquet']).hexdigest()!=seed['cohort_file_sha256']:
        raise h.Paused('Seed cohort checksum mismatch')
    if identity(seed['identity']['cfg'],sources,cohort)!=seed['identity'] or digest(seed['identity'])!=seed['fingerprint']:
        raise h.Paused('Seed identity mismatch')
    names=[s['name'] for s in sources]
    if len(names)!=len(set(names)) or not set(seed['done_data'])<=set(names):
        raise h.Paused('Invalid source list or baseline checkpoint')
    return seed,sources,cohort


def seed_data(root):
    return {name:(Path(root)/name).read_bytes() for name in CONTROLS
            if (Path(root)/name).stat().st_size<=MAX_CONTROL}


def seed_from_archive(archive,destination,disk_gb=30,reserve_gb=1):
    """Extract only the DB/WAL to a temporary area, with preflight against archive+DB size."""
    archive,destination=Path(archive).resolve(),Path(destination).resolve()
    if destination.exists():
        raise h.Paused('Seed destination already exists; use a new destination')
    controls={}; sizes={}
    allowed=('config.json','sources.json','stage.duckdb','stage.duckdb.wal')
    with members(archive) as stream:
        for name,size,file in stream:
            key=PurePosixPath(name).name
            if key in allowed:
                if key in sizes:
                    raise h.Paused('Archive contains multiple work directories')
                sizes[key]=size
                if key in ('config.json','sources.json'):
                    if size>MAX_CONTROL:
                        raise h.Paused('Oversized archive control file')
                    controls[key]=file.read()
    if not {'config.json','sources.json','stage.duckdb'}<=set(sizes):
        raise h.Paused('Work archive lacks config.json, sources.json or stage.duckdb')
    required=sum(sizes.values())+64*MIB
    if archive.stat().st_size+required+reserve_gb*GB>disk_gb*GB:
        raise SpaceFull('Archive plus temporary DB exceeds budget; prepare the small seed on the main PC')
    destination.parent.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(destination.parent).free<required+reserve_gb*GB:
        raise SpaceFull('Not enough space to read work archive safely')
    with tempfile.TemporaryDirectory(prefix='peer-seed-work-',dir=destination.parent) as temp:
        temp=Path(temp)
        with members(archive) as stream:
            for name,size,file in stream:
                key=PurePosixPath(name).name
                if key in sizes:
                    with (temp/key).open('wb') as out:
                        shutil.copyfileobj(file,out,length=MIB)
        return make_seed(temp,destination)


def peer_guard(root,settings):
    used=h.tree_bytes(root)
    if used>(settings['disk_gb']-settings['reserve_gb'])*GB:
        raise SpaceFull('Peer disk budget reached; completed pieces remain importable')
    if shutil.disk_usage(root).free<settings['reserve_gb']*GB:
        raise SpaceFull('Peer filesystem reserve reached; completed pieces remain importable')


class BoundedFile(io.BufferedIOBase):
    """Enforce a per-piece byte ceiling BEFORE writing; leave room for caches/control files."""
    def __init__(self,path,limit):
        super().__init__()
        self.stream=Path(path).open('wb')
        self.limit=limit
    def writable(self):
        return True
    def seekable(self):
        return True
    def tell(self):
        return self.stream.tell()
    def write(self,data):
        if self.tell()+len(data)>self.limit:
            raise SpaceFull('Peer payload budget reached before next piece could be committed')
        return self.stream.write(data)
    def flush(self):
        if not self.stream.closed:
            self.stream.flush()
    def close(self):
        if not self.closed:
            try:
                self.flush()
                os.fsync(self.stream.fileno())
            finally:
                super().close()
                self.stream.close()


def validate_meta(meta,seed,source,path=None):
    if (meta.get('version')!=1 or meta.get('seed')!=seed['fingerprint'] or
        meta.get('source')!=source or meta.get('id')!=source_id(source)):
        raise h.Paused('Piece source/cohort identity mismatch')
    for key in ('source_rows','filtered_rows','bytes','network_bytes'):
        if not isinstance(meta.get(key),int) or meta[key]<0:
            raise h.Paused('Invalid piece counts')
    if meta['filtered_rows']>meta['source_rows']:
        raise h.Paused('Piece has more filtered rows than source rows')
    if path is not None and (path.stat().st_size!=meta['bytes'] or h.checksum(path)!=meta['sha256']):
        raise h.Paused(f'Piece checksum mismatch: {path.name}')


def tail_worker(root):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    root=Path(root)
    settings=read_json(root/'peer.json')
    seed,sources,cohort=load_controls(seed_data(root))
    cfg=dict(seed['identity']['cfg'],**settings)
    work,pieces=root/'work',root/'pieces'
    work.mkdir(exist_ok=True);pieces.mkdir(exist_ok=True)
    # Input joins and output writes are batch bounded; the peer does not sort/store a raw DB.
    db=duckdb.connect(config={'memory_limit':f'{cfg["memory_mib"]}MiB','threads':str(cfg['threads']),
                             'preserve_insertion_order':'false','max_temp_directory_size':'0B',
                             'temp_directory':str(work/'spill')})
    db.register('cohort_seed',pa.table({'collection_id':[r[0] for r in cohort],
        'instance_index':[r[1] for r in cohort],'rank':[r[2] for r in cohort]}))
    db.execute('CREATE TABLE candidates AS SELECT * FROM cohort_seed')
    db.unregister('cohort_seed')
    done=set(seed['done_data'])
    used=0
    known={source_id(s):s for s in sources}
    for meta_path in pieces.glob('*.json'):
        meta=read_json(meta_path,MAX_META)
        if meta_path.stem not in known:
            raise h.Paused('Unknown piece in peer directory')
        path=pieces/(meta_path.stem+'.parquet')
        if meta.get('source',{}).get('name') in done:
            # A newer seed from the main PC acknowledges an earlier successful import.
            validate_meta(meta,seed,known[meta_path.stem])
            path.unlink(missing_ok=True);meta_path.unlink()
            continue
        validate_meta(meta,seed,known[meta_path.stem],path)
        done.add(meta['source']['name']);used+=path.stat().st_size
    for path in pieces.glob('*.parquet*'):
        if path.name.endswith('.partial') or not (pieces/(path.stem+'.json')).exists():
            path.unlink()  # Uncommitted cache/output, never a checkpoint.
    ordered=sources if settings['order']=='forward' else list(reversed(sources))
    jobs=[(i,s,h.source_columns('data',s['table'],cfg)) for i,s in enumerate(ordered,1)
          if s['name'] not in done]
    overhead=len(sources)*MAX_META+3*MAX_CONTROL+64*MIB
    cache=(cfg['prefetch_shards']+1)*cfg['max_shard_mib']*MIB
    payload_limit=int((cfg['disk_gb']-cfg['reserve_gb'])*GB)-overhead-cache
    committed=0
    h.LOG.info('Peer data %s: known=%d/%d; payload budget %.3fGB',settings['order'],
               len(done),len(sources),payload_limit/GB)
    try:
        with h.prefetch_downloads(work,pieces,cfg,jobs) as downloads:
            for _,source,columns,cache_path,source_rows,network_bytes,seconds in downloads:
                peer_guard(root,settings)
                pid=source_id(source)
                temporary=pieces/(pid+'.parquet.partial')
                final=pieces/(pid+'.parquet')
                sink=BoundedFile(temporary,max(0,payload_limit-used))
                writer=None
                filtered=decoded=0
                reader=pq.ParquetFile(cache_path)
                try:
                    for batch in reader.iter_batches(batch_size=cfg['batch_rows'],columns=columns,
                                                     use_threads=False):
                        db.register('incoming',batch)
                        result=db.execute(h.filtered_data_sql(source['table'],cfg)).fetch_arrow_table()
                        db.unregister('incoming')
                        if writer is None:
                            writer=pq.ParquetWriter(sink,result.schema,compression='zstd')
                        writer.write_table(result,row_group_size=16384)
                        decoded+=batch.num_rows;filtered+=result.num_rows
                    if decoded!=source_rows:
                        raise h.Paused('Source decoded row count mismatch')
                    if writer is None:
                        db.register('incoming',reader.read(columns=columns,use_threads=False))
                        result=db.execute(h.filtered_data_sql(source['table'],cfg)).fetch_arrow_table()
                        db.unregister('incoming')
                        writer=pq.ParquetWriter(sink,result.schema,compression='zstd')
                    writer.close();writer=None;sink.close()
                    meta={'version':1,'seed':seed['fingerprint'],'id':pid,'source':source,
                          'source_rows':source_rows,'filtered_rows':filtered,
                          'network_bytes':network_bytes,'bytes':temporary.stat().st_size,
                          'sha256':h.checksum(temporary)}
                    if len(encoded(meta))>MAX_META//2:
                        raise h.Paused('Piece metadata exceeds reserved budget')
                    temporary.replace(final)
                    h.atomic_json(pieces/(pid+'.json'),meta)
                    used+=meta['bytes'];committed+=1
                    h.LOG.info('Peer committed %s; source_rows=%d filtered_rows=%d bytes=%d download=%.2fs',
                               source['name'],source_rows,filtered,meta['bytes'],seconds)
                finally:
                    reader.close()
                    if writer is not None:
                        with contextlib.suppress(Exception):
                            writer.close()
                    sink.close()
                    temporary.unlink(missing_ok=True)
                if settings['max_new_shards'] and committed>=settings['max_new_shards']:
                    break
        h.atomic_json(root/'status.json',{'status':'peer_complete' if committed==len(jobs) else 'batch_complete',
                      'new_pieces':committed,
                      'note':'Importable shard pieces, not a finalized dataset'})
    except BaseException as exc:
        h.atomic_json(root/'status.json',{'status':'space_limit' if isinstance(exc,SpaceFull) else 'paused',
                      'new_pieces':committed,'reason':str(exc)})
        raise
    finally:
        db.close()


def bundle_index(bundle):
    controls={};metas={};payloads={};seen=set()
    with members(bundle) as stream:
        for name,size,file in stream:
            logical=logical_name(name)
            if logical is None:
                continue
            if logical in seen:
                raise h.Paused('Duplicate member in bundle')
            seen.add(logical)
            if logical in CONTROLS:
                if size>MAX_CONTROL:
                    raise h.Paused('Oversized bundle control')
                controls[logical]=file.read()
            elif logical.endswith('.json'):
                if size>MAX_META:
                    raise h.Paused('Oversized piece metadata')
                metas[PurePosixPath(logical).stem]=json.loads(file.read())
            elif logical.endswith('.parquet'):
                payloads[PurePosixPath(logical).stem]=size
    seed,sources,cohort=load_controls(controls)
    source_map={source_id(s):s for s in sources}
    if not set(metas)<=set(payloads):
        raise h.Paused('Bundle is missing a committed payload')
    for pid,meta in metas.items():
        if pid not in source_map:
            raise h.Paused('Unknown piece source')
        validate_meta(meta,seed,source_map[pid])
        if payloads[pid]!=meta['bytes']:
            raise h.Paused('Piece header size mismatch')
    return seed,sources,cohort,metas


def import_piece(db,path,meta):
    """One atomic source import, including the existing main checkpoint key."""
    import pyarrow.parquet as pq
    table=meta['source']['table']
    schema=pq.ParquetFile(path).metadata
    if schema.num_rows!=meta['filtered_rows']:
        raise h.Paused('Filtered row count mismatch')
    # Both schemas are resolved by DuckDB, avoiding Arrow list-field naming differences.
    incoming=db.execute(f'DESCRIBE SELECT * FROM read_parquet({h.literal(path)})').fetchall()
    exists=db.execute('SELECT 1 FROM information_schema.tables WHERE table_name=?',[table]).fetchone()
    if exists:
        actual=db.execute(f'DESCRIBE {table}').fetchall()
        if [r[:2] for r in actual]!=[r[:2] for r in incoming]:
            raise h.Paused('Main/piece schema mismatch')
    db.execute('BEGIN')
    try:
        if not exists:
            db.execute(f'CREATE TABLE {table} AS SELECT * FROM read_parquet({h.literal(path)}) LIMIT 0')
        db.execute(f'INSERT INTO {table} SELECT * FROM read_parquet({h.literal(path)})')
        db.execute('INSERT INTO done VALUES (?,?,?,?)',
                   ['data',meta['source']['name'],meta['source_rows'],meta['network_bytes']])
        db.execute('COMMIT')
    except BaseException:
        with contextlib.suppress(Exception):
            db.execute('ROLLBACK')
        raise


def merge(work,bundle,disk_gb=100,reserve_gb=5,_locked=False):
    work,bundle=Path(work).resolve(),Path(bundle).resolve()
    if not (work/'stage.duckdb').is_file():
        raise h.Paused('Main staging DB is absent; no empty replacement will be created')
    seed,sources,cohort,metas=bundle_index(bundle)
    bundle_bytes=(0 if work==bundle or work in bundle.parents else
                  h.tree_bytes(bundle) if bundle.is_dir() else bundle.stat().st_size)
    cfg=read_json(work/'config.json')
    own_sources=read_json(work/'sources.json')
    imported=skipped=0
    with (contextlib.nullcontext() if _locked else lock(work/'run.lock')):
        db=h.connect_db(work,cfg)
        try:
            if identity(cfg,own_sources,cohort_rows(db))!=seed['identity']:
                raise h.Paused('Main/peer cohort, source generation, filter or window mismatch')
            done={r[0] for r in db.execute("SELECT name FROM done WHERE phase='data'").fetchall()}
            pending={pid for pid,m in metas.items() if m['source']['name'] not in done}
            if pending and db.execute("SELECT 1 FROM information_schema.tables WHERE "
                                       "table_name IN ('quality','exported')").fetchone():
                raise h.Paused('Main already entered finalization; refusing to invalidate results')
            with tempfile.TemporaryDirectory(prefix='peer-import-',dir=work) as temp:
                path=Path(temp)/'piece.parquet'
                with members(bundle) as stream:
                    for name,size,file in stream:
                        logical=logical_name(name)
                        if not logical or not logical.startswith('pieces/') or not logical.endswith('.parquet'):
                            continue
                        pid=PurePosixPath(logical).stem
                        if pid not in metas:
                            continue  # An interrupted, unpublished payload is not progress.
                        meta=metas[pid]
                        if pid not in pending:
                            skipped+=1
                            continue
                        # Include the supplied bundle in the job's conservative import budget.
                        if (h.tree_bytes(work)+bundle_bytes+2*size+reserve_gb*GB>disk_gb*GB or
                            shutil.disk_usage(work).free<2*size+reserve_gb*GB):
                            raise SpaceFull('Import space reserve reached; earlier commits remain safe')
                        with path.open('wb') as out:
                            remaining=size
                            while remaining:
                                chunk=file.read(min(MIB,remaining))
                                if not chunk:
                                    raise h.Paused('Truncated piece stream')
                                out.write(chunk);remaining-=len(chunk)
                        validate_meta(meta,seed,meta['source'],path)
                        import_piece(db,path,meta)
                        path.unlink()
                        imported+=1
                        if imported%64==0:
                            db.execute('CHECKPOINT')
                        h.LOG.info('Imported %s; filtered_rows=%d',meta['source']['name'],meta['filtered_rows'])
        finally:
            db.close()
    return {'imported':imported,'skipped':skipped}


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    seed=sub.add_parser('seed',help='Read a stopped work directory or archive; export a small peer seed')
    source=seed.add_mutually_exclusive_group(required=True)
    source.add_argument('--work',type=Path);source.add_argument('--archive',type=Path)
    seed.add_argument('--output',type=Path,required=True)
    seed.add_argument('--disk-gb',type=float,default=30)
    seed.add_argument('--reserve-gb',type=float,default=1)
    run=sub.add_parser('download',help='DATA only, with the frozen cohort; no prefix rescan')
    run.add_argument('--seed',type=Path,required=True);run.add_argument('--root',type=Path,required=True)
    run.add_argument('--order',choices=['reverse','forward'],default='reverse')
    run.add_argument('--disk-gb',type=float,default=30)
    run.add_argument('--reserve-gb',type=float,default=1)
    run.add_argument('--memory-mib',type=int,default=512);run.add_argument('--rss-mib',type=int,default=1536)
    run.add_argument('--threads',type=int,default=2);run.add_argument('--batch-rows',type=int,default=16384)
    run.add_argument('--network-workers',type=int,default=2);run.add_argument('--prefetch-shards',type=int,default=2)
    run.add_argument('--max-shard-mib',type=int,default=128);run.add_argument('--retry-hours',type=float,default=12)
    run.add_argument('--max-new-shards',type=int,default=0)
    imp=sub.add_parser('merge',help='Import directory, tar(.gz) or zip bundle into a stopped main work')
    imp.add_argument('--work',type=Path,required=True);imp.add_argument('--bundle',type=Path,required=True)
    imp.add_argument('--disk-gb',type=float,default=100);imp.add_argument('--reserve-gb',type=float,default=5)
    child=sub.add_parser('_worker',help=argparse.SUPPRESS);child.add_argument('--root',type=Path,required=True)
    child_merge=sub.add_parser('_merge-worker',help=argparse.SUPPRESS)
    child_merge.add_argument('--work',type=Path,required=True)
    return p


def main(argv=None):
    args=parser().parse_args(argv)
    if args.command=='seed':
        if args.archive:
            result=seed_from_archive(args.archive,args.output,args.disk_gb,args.reserve_gb)
        else:
            result=make_seed(args.work,args.output)
        print('Seed ready:',args.output,'identity:',result['fingerprint'])
    elif args.command=='merge':
        work=args.work.resolve();bundle=args.bundle.resolve()
        if args.reserve_gb<1 or args.disk_gb<=args.reserve_gb:
            raise h.Paused('Invalid import disk budget')
        h.setup_log(work)
        with lock(work/'run.lock') as owned:
            settings=read_json(work/'config.json')
            result_path=work/'peer-merge-result.json'
            result_path.unlink(missing_ok=True)
            h.atomic_json(work/'peer-merge-request.json',{'bundle':str(bundle),
                'disk_gb':args.disk_gb,'reserve_gb':args.reserve_gb})
            bundle_bytes=(0 if work==bundle or work in bundle.parents else
                          h.tree_bytes(bundle) if bundle.is_dir() else bundle.stat().st_size)
            def import_guard():
                if (h.tree_bytes(work)+bundle_bytes+args.reserve_gb*GB>args.disk_gb*GB or
                    shutil.disk_usage(work).free<args.reserve_gb*GB):
                    raise SpaceFull('Import disk reserve reached; earlier pieces remain committed')
                stage=work/'stage.duckdb'
                if stage.exists() and stage.stat().st_size>settings['stage_gb']*GB:
                    raise SpaceFull('Main staging limit reached')
            import_guard()
            cmd=[sys.executable,str(Path(__file__).resolve()),'_merge-worker','--work',str(work)]
            h.supervise(work,work,settings,(owned.fileno(),),command=cmd,resource_guard=import_guard)
            print(json.dumps(read_json(result_path)))
    elif args.command in ('_worker','_merge-worker'):
        if 'VMFORECAST_SUPERVISOR_PID' in os.environ:
            import ctypes
            if ctypes.CDLL(None).prctl(1,signal.SIGTERM,0,0,0)!=0 or os.getppid()!=int(os.environ['VMFORECAST_SUPERVISOR_PID']):
                raise h.Paused('Supervisor unavailable')
        os.nice(10)
        if args.command=='_worker':
            h.setup_log(args.root)
            tail_worker(args.root)
        else:
            h.setup_log(args.work)
            request=read_json(args.work/'peer-merge-request.json')
            result=merge(args.work,request['bundle'],request['disk_gb'],request['reserve_gb'],_locked=True)
            h.atomic_json(args.work/'peer-merge-result.json',result)
    else:
        if (not 0<args.disk_gb<=30 or args.reserve_gb<1 or args.disk_gb<=args.reserve_gb+1
            or args.memory_mib<128 or args.rss_mib<args.memory_mib+256
            or not 1<=args.threads<=4 or not 1<=args.network_workers<=4
            or not 0<=args.prefetch_shards<=4 or not 1<=args.batch_rows<=65536
            or not 1<=args.max_shard_mib<=512 or args.retry_hours<0 or args.max_new_shards<0):
            raise h.Paused('Invalid peer resource limits (disk ceiling is 30 GB)')
        root=args.root.expanduser().resolve()
        root.mkdir(parents=True,exist_ok=True)
        with lock(root/'peer.lock') as owned:
            seed,sources,_=load_controls(seed_data(args.seed))
            if (root/'peer.json').exists():
                current=load_controls(seed_data(root))[0]
                if current['fingerprint']!=seed['fingerprint']:
                    raise h.Paused('Peer root belongs to another seed')
                current['done_data']=sorted(set(current['done_data'])|set(seed['done_data']))
                h.atomic_json(root/'seed.json',current)
            else:
                if any(p.name!='peer.lock' for p in root.iterdir()):
                    raise h.Paused('Peer root must initially be empty')
                for name in CONTROLS:
                    shutil.copyfile(args.seed/name,root/name)
            settings=vars(args).copy()
            for key in ('command','seed','root'):
                settings.pop(key)
            settings.update(stage_gb=65)
            h.atomic_json(root/'peer.json',settings)
            h.setup_log(root)
            peer_guard(root,settings)
            cmd=[sys.executable,str(Path(__file__).resolve()),'_worker','--root',str(root)]
            h.supervise(root/'work',root/'pieces',settings,(owned.fileno(),),command=cmd,
                        resource_guard=lambda:peer_guard(root,settings))


if __name__=='__main__':
    try:
        main()
    except (h.Paused,KeyboardInterrupt) as exc:
        h.LOG.error('PAUSED: %s',exc)
        sys.exit(75)
