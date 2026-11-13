"""Resumable public Google 2019 Parquet acquisition, with a supervised local worker.

The output is a collection of COMPLETE per-instance files, not a truncated stream.
No source archive is saved. Temporary sparse Parquet contains only fetched columns.
SQL below runs in local DuckDB; it never invokes BigQuery or a cloud SQL service.
"""
from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import http.client
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import shutil
import signal
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

VERSION = 1
GB = 1_000_000_000
MIB = 1024**2
STEP = 300_000_000
TABLES = ('instance_usage', 'instance_events', 'machine_events', 'machine_attributes',
          'collection_events')
PREFIX_COLUMNS = ['start_time', 'end_time', 'collection_id', 'instance_index',
                  'machine_id', 'collection_type', 'alloc_collection_id',
                  'average_usage.cpus', 'average_usage.memory']
CORE_USAGE = PREFIX_COLUMNS + ['sample_rate']
CORE_EVENTS = ['time', 'type', 'collection_id', 'instance_index', 'machine_id',
               'missing_type', 'resource_request.cpus', 'resource_request.memory']
LOG = logging.getLogger('google2019-http')


class Paused(RuntimeError):
    """A resource/source problem that must not silently produce a complete dataset."""


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as out:
        json.dump(value, out, ensure_ascii=False, indent=2, allow_nan=False)
        out.flush()
        os.fsync(out.fileno())
    tmp.replace(path)


def tree_bytes(root):
    # Apparent size is deliberately conservative for the sparse source cache.
    return sum(p.stat().st_size for p in Path(root).rglob('*') if p.is_file())


def publish(source, destination):
    """Atomic destination even when scratch and output are on different filesystems."""
    try:
        source.replace(destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        partial = destination.with_name(destination.name+'.partial')
        with source.open('rb') as inp, partial.open('wb') as out:
            shutil.copyfileobj(inp,out,length=8*MIB)
            out.flush()
            os.fsync(out.fileno())
        partial.replace(destination)
        source.unlink()


def setup_log(work):
    LOG.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    for handler in (logging.StreamHandler(), RotatingFileHandler(
            work / 'download.log', maxBytes=4*MIB, backupCount=3)):
        handler.setFormatter(fmt)
        LOG.addHandler(handler)


def get_bytes(url, expected=None, start=None, generation=None, retry_hours=12):
    """Bounded response, exact Range validation; only transient failures are retried."""
    if generation:
        url += ('&' if '?' in url else '?') + urllib.parse.urlencode({'generation': generation})
    headers = {'User-Agent': 'vmforecast-public-trace/1', 'Accept-Encoding': 'identity'}
    if start is not None:
        headers['Range'] = f'bytes={start}-{start+expected-1}'
    deadline = time.monotonic() + retry_hours*3600
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
                if start is not None:
                    cr = r.headers.get('Content-Range', '')
                    if r.status != 206 or not cr.startswith(f'bytes {start}-{start+expected-1}/'):
                        raise Paused(f'Server ignored/changed Range: {url} ({r.status}, {cr})')
                limit = expected if expected is not None else 8*MIB
                data = r.read(limit+1)
                if len(data) > limit:
                    raise Paused(f'Oversized HTTP response: {url}')
                if expected is not None and len(data) != expected:
                    raise OSError(f'Short HTTP response: {len(data)} != {expected}')
                return data
        except urllib.error.HTTPError as exc:
            if exc.code not in (408, 429, 500, 502, 503, 504):
                raise Paused(f'HTTP {exc.code}; source was NOT skipped: {url}') from exc
            error = exc
        except (OSError, TimeoutError, urllib.error.URLError,http.client.HTTPException) as exc:
            error = exc
        attempt += 1
        if time.monotonic() >= deadline:
            raise Paused(f'Network retry deadline reached: {url}: {error}') from error
        delay = min(60, 2**min(attempt, 6)) + random.random()
        if attempt == 1 or attempt % 20 == 0:
            LOG.warning('HTTP retry %s in %.1fs: %s', attempt, delay, error)
        time.sleep(delay)


def list_sources(cell, tables=TABLES):
    objects = []
    for table in tables:
        token = None
        while True:
            args = {'prefix': table+'-', 'maxResults': 1000,
                    'fields': 'items(name,size,generation),nextPageToken'}
            if token:
                args['pageToken'] = token
            url = f'https://storage.googleapis.com/storage/v1/b/clusterdata_2019_{cell}/o?'
            page = json.loads(get_bytes(url + urllib.parse.urlencode(args)))
            for obj in page.get('items', []):
                if obj['name'].endswith('.parquet.gz'):
                    objects.append(dict(table=table, name=obj['name'], size=int(obj['size']),
                                        generation=obj['generation'],
                                        url=f'https://storage.googleapis.com/clusterdata_2019_{cell}/'
                                            + urllib.parse.quote(obj['name'])))
            token = page.get('nextPageToken')
            if not token:
                break
        if not any(x['table'] == table for x in objects):
            raise Paused(f'No Parquet sources found for {table}')
    return sorted(objects, key=lambda x: (x['table'], x['name']))


def fetch_sparse(obj, path, columns, cfg):
    """Fetch footer + projected column chunks, with a 128 MiB source-file guard."""
    import pyarrow.parquet as pq
    size = obj['size']
    if size < 12 or size > cfg['max_shard_mib']*MIB:
        raise Paused(f'Unexpected source size {size}: {obj["name"]}')
    def fetch(a, n):
        return get_bytes(obj['url'], expected=n, start=a, generation=obj.get('generation'),
                         retry_hours=cfg['retry_hours'])
    tail = fetch(size-8, 8)
    if tail[4:] != b'PAR1':
        raise Paused(f'Not plain Parquet: {obj["name"]}')
    footer_size = struct.unpack('<I', tail[:4])[0]
    if not 0 < footer_size < min(size-8, 4*MIB):
        raise Paused('Invalid/oversized Parquet footer')
    footer = fetch(size-footer_size-8, footer_size+8)
    path.unlink(missing_ok=True)
    with path.open('w+b') as out:
        out.truncate(size)
        out.write(b'PAR1')
        out.seek(size-len(footer))
        out.write(footer)
    reader = pq.ParquetFile(path)
    metadata = reader.metadata
    ranges = []
    for g in range(metadata.num_row_groups):
        group = metadata.row_group(g)
        for j in range(group.num_columns):
            col = group.column(j)
            if columns is not None and col.path_in_schema not in columns:
                continue
            offsets = [x for x in (col.dictionary_page_offset, col.data_page_offset)
                       if x is not None and x >= 4]
            if not offsets:
                raise Paused('Invalid Parquet column offset')
            a, n = min(offsets), col.total_compressed_size
            if n <= 0 or a+n > size-len(footer):
                raise Paused('Invalid Parquet column bounds')
            ranges.append((a, a+n))
    reader.close()
    # Coalesce adjacent selected chunks, but never fetch intervening unselected columns.
    merged = []
    for a, b in sorted(ranges):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
        else:
            merged.append((a, b))
    transferred = len(tail) + len(footer)
    with path.open('r+b') as out:
        for a, b in merged:
            for start in range(a, b, 8*MIB):
                chunk = fetch(start, min(8*MIB, b-start))
                out.seek(start)
                out.write(chunk)
                transferred += len(chunk)
    return metadata.num_rows, transferred


def connect_db(work, cfg, readonly=False):
    import duckdb
    db = duckdb.connect(str(work/'stage.duckdb'), read_only=readonly)
    db.execute(f"SET memory_limit='{cfg['memory_mib']}MiB'")
    db.execute(f"SET threads={cfg['threads']}")
    db.execute('SET preserve_insertion_order=false')
    db.execute(f"SET temp_directory={literal(work/'spill')}")
    db.execute(f"SET max_temp_directory_size='{cfg['spill_gb']}GB'")
    db.execute("SET checkpoint_threshold='64MB'")
    db.execute('SET enable_progress_bar=false')
    return db


def guard(work, output, cfg):
    if min(shutil.disk_usage(work).free,shutil.disk_usage(output).free) < cfg['reserve_gb']*GB:
        raise Paused('Low filesystem space; checkpoint preserved, free space and rerun')
    stage = work/'stage.duckdb'
    if stage.exists() and stage.stat().st_size > cfg['stage_gb']*GB:
        raise Paused('Staging limit reached; rerun a NEW job with fewer --max-objects')
    # Workers check between units; supervisor checks while DuckDB/HTTP are running.
    if tree_bytes(work) + tree_bytes(output) > cfg['disk_gb']*GB - 2*GB:
        raise Paused('Combined working-space guard reached')


def usage_where(t0, t1, prefix=False):
    base = (f'start_time < {t1} AND end_time > {t0} AND collection_type=0 '
            'AND coalesce(alloc_collection_id,0)=0')
    if prefix:
        base += (' AND machine_id>0 AND average_usage.cpus IS NOT NULL '
                 'AND average_usage.memory IS NOT NULL '
                 'AND isfinite(average_usage.cpus) AND isfinite(average_usage.memory) '
                 'AND average_usage.cpus>=0 AND average_usage.memory>=0')
    return base


def ingest(work, output, cfg, sources, phase):
    import pyarrow.parquet as pq
    db = connect_db(work, cfg)
    db.execute('CREATE TABLE IF NOT EXISTS done(phase VARCHAR, name VARCHAR, rows BIGINT, '
               'network_bytes BIGINT, PRIMARY KEY(phase,name))')
    if phase == 'prefix':
        db.execute('CREATE TABLE IF NOT EXISTS prefix(collection_id BIGINT, instance_index BIGINT, '
                   'a BIGINT,b BIGINT)')
    for number, obj in enumerate(sources):
        table = obj['table']
        if phase == 'prefix' and table != 'instance_usage':
            continue
        if db.execute('SELECT 1 FROM done WHERE phase=? AND name=?', [phase, obj['name']]).fetchone():
            continue
        guard(work, output, cfg)
        cache = work/'source.partial.parquet'
        columns = PREFIX_COLUMNS if phase == 'prefix' else None
        if phase == 'data' and cfg['columns'] == 'core':
            columns = CORE_USAGE if table == 'instance_usage' else (
                CORE_EVENTS if table == 'instance_events' else None)
        rows, transferred = fetch_sparse(obj, cache, columns, cfg)
        reader = pq.ParquetFile(cache)
        db.execute('BEGIN')
        try:
            count = 0
            for batch in reader.iter_batches(batch_size=cfg['batch_rows'], columns=columns,
                                             use_threads=False):
                db.register('incoming', batch)
                if phase == 'prefix':
                    end = cfg['t0']+cfg['prefix_hours']*3600_000_000
                    sql = (f'SELECT collection_id,instance_index,greatest(start_time,{cfg["t0"]}) a,'
                           f'least(end_time,{end}) b FROM incoming WHERE '
                           + usage_where(cfg['t0'], end, prefix=True))
                    db.execute('INSERT INTO prefix '+sql)
                else:
                    if table == 'instance_usage':
                        sql = ('SELECT i.*, average_usage.cpus AS cpu, average_usage.memory AS mem '
                               'FROM incoming i SEMI JOIN candidates c USING(collection_id,instance_index) '
                               'WHERE '+usage_where(cfg['t0'], cfg['t1']))
                    elif table == 'instance_events':
                        sql = ('SELECT i.*,resource_request.cpus AS cpu_request,'
                               'resource_request.memory AS mem_request FROM incoming i '
                               'SEMI JOIN candidates c USING(collection_id,instance_index) '
                               f'WHERE time < {cfg["t1"]}')
                    elif table == 'collection_events':
                        sql = ('SELECT i.* FROM incoming i SEMI JOIN '
                               '(SELECT DISTINCT collection_id FROM candidates) c USING(collection_id) '
                               f'WHERE time < {cfg["t1"]}')
                    else:
                        # Host lifecycle/capacity history for the entire cell is small and useful.
                        sql = f'SELECT * FROM incoming WHERE time < {cfg["t1"]}'
                    db.execute(f'CREATE TABLE IF NOT EXISTS {table} AS {sql} LIMIT 0')
                    db.execute(f'INSERT INTO {table} {sql}')
                count += batch.num_rows
                db.unregister('incoming')
            if count != rows:
                raise Paused(f'Footer/decoded row count mismatch for {obj["name"]}')
            db.execute('INSERT INTO done VALUES (?,?,?,?)', [phase,obj['name'],rows,transferred])
            db.execute('COMMIT')
            db.execute('CHECKPOINT')
        except BaseException:
            with contextlib.suppress(Exception):
                db.execute('ROLLBACK')
            raise
        finally:
            reader.close()
            cache.unlink(missing_ok=True)
        LOG.info('%s %d/%d %s; source_rows=%d HTTP=%.2fMB', phase, number+1,
                 len(sources), obj['name'], rows, transferred/1e6)
    if phase == 'prefix':
        # Reject gaps/overlaps in the initial observable prefix. Selection uses no future usage.
        end = cfg['t0']+cfg['prefix_hours']*3600_000_000
        db.execute(f'''CREATE TABLE IF NOT EXISTS candidates AS
            WITH ordered AS (SELECT *,max(b) OVER(PARTITION BY collection_id,instance_index
                ORDER BY a,b ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) prev FROM prefix),
            eligible AS (SELECT collection_id,instance_index FROM ordered GROUP BY ALL
                HAVING sum(b-a)={end-cfg['t0']} AND min(a)={cfg['t0']} AND max(b)={end}
                AND count(*) FILTER(WHERE a<prev)=0)
            SELECT *,row_number() OVER(ORDER BY md5(collection_id::VARCHAR||':'||
                instance_index::VARCHAR||':{cfg['seed']}'),collection_id,instance_index) rank
            FROM eligible ORDER BY rank LIMIT {cfg['max_objects']}''')
        n = db.execute('SELECT count(*) FROM candidates').fetchone()[0]
        if not n:
            raise Paused('No prefix-complete instances: inspect source/window configuration')
        LOG.info('Selected %d prefix-complete instances (ceiling %d)', n, cfg['max_objects'])
    db.close()


def quality_sql(usage, events, t0, t1):
    """Same conservative fixed-cohort lifecycle/interval contract as data.prepare_usage."""
    return f'''WITH clipped AS (
        SELECT collection_id,instance_index,greatest(start_time,{t0}) a,
               least(end_time,{t1}) b, cpu,mem,machine_id
        FROM {usage} WHERE start_time<{t1} AND end_time>{t0}),
    ordered AS (SELECT *,max(b) OVER(PARTITION BY collection_id,instance_index
        ORDER BY a,b ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) prev FROM clipped),
    u AS (SELECT collection_id,instance_index,count(*) usage_rows,
        min(a)={t0} AND max(b)={t1} AND sum(b-a)={t1-t0}
        AND count(*) FILTER(WHERE a<prev OR b<=a OR machine_id IS NULL OR machine_id<=0
            OR cpu IS NULL OR mem IS NULL OR NOT isfinite(cpu) OR NOT isfinite(mem)
            OR cpu<0 OR mem<0)=0 continuous
        FROM ordered GROUP BY ALL),
    last_time AS (SELECT collection_id,instance_index,max(time) ts FROM {events}
        WHERE time<={t0} GROUP BY ALL),
    last_state AS (SELECT collection_id,instance_index,
        bool_or(type IN (3,10)) AND bool_and(type IN (0,2,3,10)) initial_ok
        FROM {events} e JOIN last_time l USING(collection_id,instance_index)
        WHERE e.time=l.ts GROUP BY ALL),
    e AS (SELECT collection_id,instance_index,count(*) event_rows,
        count(*) FILTER(WHERE coalesce(missing_type,0)<>0 OR time IS NULL OR type IS NULL
            OR (time>{t0} AND time<{t1} AND type<>10)
            OR (time={t0} AND type IN (0,1,2,4,5,6,7,8,9)))=0 lifecycle_ok
        FROM {events} WHERE time<{t1} GROUP BY ALL)
    SELECT u.*,coalesce(e.event_rows,0) event_rows,
        coalesce(e.lifecycle_ok AND l.initial_ok,false) lifecycle_ok,
        coalesce(u.continuous AND e.lifecycle_ok AND l.initial_ok,false) AS "valid"
    FROM u LEFT JOIN e USING(collection_id,instance_index)
           LEFT JOIN last_state l USING(collection_id,instance_index)'''


def copy_parquet(db, sql, path):
    db.execute(f'COPY ({sql}) TO {literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD, '
               'ROW_GROUP_SIZE 16384)')
    with Path(path).open('rb') as stream:
        os.fsync(stream.fileno())


def checksum(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(MIB), b''):
            h.update(chunk)
    return h.hexdigest()


def split_export(db, sql, directory, batch_rows):
    """Sort only a bounded cohort group; stream each instance to its own file."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    directory.mkdir(exist_ok=True)
    writer, previous = None, None
    try:
        reader = db.execute(sql).fetch_record_batch(batch_rows)
        for batch in reader:
            cids = batch.column(batch.schema.get_field_index('collection_id')).to_pylist()
            indices = batch.column(batch.schema.get_field_index('instance_index')).to_pylist()
            keys = list(zip(cids, indices))
            start = 0
            while start < len(keys):
                key = keys[start]
                end = start+1
                while end < len(keys) and keys[end] == key:
                    end += 1
                if key != previous:
                    if writer:
                        writer.close()
                    path = directory/f'{key[0]}_{key[1]}.parquet'
                    writer = pq.ParquetWriter(path,batch.schema,compression='zstd')
                    previous = key
                writer.write_table(pa.Table.from_batches([batch.slice(start,end-start)]),
                                   row_group_size=16384)
                start = end
    finally:
        if writer:
            writer.close()


def build_quality(db,work,output,cfg):
    """Bound the interval-sort working set; never sort every object's month together."""
    db.execute('CREATE TEMP VIEW quality_u AS SELECT * FROM instance_usage LIMIT 0')
    db.execute('CREATE TEMP VIEW quality_e AS SELECT * FROM instance_events LIMIT 0')
    db.execute('CREATE TABLE IF NOT EXISTS quality AS '+quality_sql(
        'quality_u','quality_e',cfg['t0'],cfg['t1']))
    db.execute('CREATE TABLE IF NOT EXISTS quality_groups(first_rank BIGINT PRIMARY KEY)')
    maximum = db.execute('SELECT max(rank) FROM candidates').fetchone()[0]
    for lo in range(1,maximum+1,256):
        if db.execute('SELECT 1 FROM quality_groups WHERE first_rank=?',[lo]).fetchone():
            continue
        guard(work,output,cfg)
        for view,table in [('quality_u','instance_usage'),('quality_e','instance_events')]:
            db.execute(f'CREATE OR REPLACE TEMP VIEW {view} AS SELECT i.* FROM {table} i '
                       f'SEMI JOIN (SELECT * FROM candidates WHERE rank BETWEEN {lo} AND {lo+255}) c '
                       'USING(collection_id,instance_index)')
        db.execute('BEGIN')
        try:
            db.execute('INSERT INTO quality '+quality_sql('quality_u','quality_e',cfg['t0'],cfg['t1']))
            db.execute('INSERT INTO quality_groups VALUES (?)',[lo])
            db.execute('COMMIT')
            db.execute('CHECKPOINT')
        except BaseException:
            with contextlib.suppress(Exception):
                db.execute('ROLLBACK')
            raise
        LOG.info('Quality: instances %d..%d / %d',lo,min(maximum,lo+255),maximum)


def export(work, output, cfg, sources):
    db = connect_db(work, cfg)
    db.execute('CREATE TABLE IF NOT EXISTS exported(collection_id BIGINT,instance_index BIGINT,'
               'bytes BIGINT,usage_hash VARCHAR,events_hash VARCHAR,PRIMARY KEY(collection_id,instance_index))')
    build_quality(db,work,output,cfg)
    # Reserve actual space for manifest, source inventory, cohort and quality tables.
    metadata_reserve = 16*MIB+max(0,cfg['max_objects']-25000)*200
    payload_limit = int(cfg['target_gb']*GB)-metadata_reserve
    if payload_limit <= 0:
        raise Paused('Target must exceed metadata reserve (16 MiB)')
    for directory in (output,output/'usage.parquet',output/'events.parquet'):
        for partial in directory.glob('*.parquet.partial'):
            partial.unlink()
    for table in ('machine_events','machine_attributes','collection_events'):
        path = output/(table+'.parquet')
        if not path.exists():
            tmp = work/(table+'.partial.parquet')
            copy_parquet(db, f'SELECT * FROM {table}', tmp)
            if tree_bytes(output)+tmp.stat().st_size > payload_limit:
                raise Paused('Metadata/host tables exceed output budget')
            publish(tmp,path)
    for folder in ('usage.parquet','events.parquet'):
        (output/folder).mkdir(exist_ok=True)
    rows = db.execute('SELECT collection_id,instance_index,rank FROM candidates ORDER BY rank').fetchall()
    total = tree_bytes(output)
    group_dir = work/'export-group'
    # Grouping avoids re-scanning the staging warehouse for every single instance.
    # At most 128 objects are externally sorted at once, not the entire month.
    for group_start in range(0,len(rows),128):
        group = rows[group_start:group_start+128]
        todo = [x for x in group if not db.execute('SELECT 1 FROM exported WHERE '
                'collection_id=? AND instance_index=?',list(x[:2])).fetchone()]
        if todo:
          shutil.rmtree(group_dir,ignore_errors=True)
          group_dir.mkdir()
          guard(work,output,cfg)
          lo, hi = group[0][2],group[-1][2]
          for table, folder, order in [('instance_usage','usage','start_time,end_time'),
                                       ('instance_events','events','time,type')]:
              split_export(db,f'SELECT i.* FROM {table} i SEMI JOIN '
                           f'(SELECT * FROM candidates WHERE rank BETWEEN {lo} AND {hi}) c '
                           f'USING(collection_id,instance_index) ORDER BY collection_id,instance_index,{order}',
                           group_dir/folder,cfg['batch_rows'])
        for cid, idx, rank in group:
            name = f'{cid}_{idx}.parquet'
            final_u, final_e = output/'usage.parquet'/name, output/'events.parquet'/name
            done = db.execute('SELECT bytes,usage_hash,events_hash FROM exported WHERE '
                              'collection_id=? AND instance_index=?',[cid,idx]).fetchone()
            if done:
                if (not final_u.exists() or not final_e.exists() or checksum(final_u)!=done[1]
                        or checksum(final_e)!=done[2]):
                    raise Paused(f'Output damaged/changed: {name}')
                continue
            # Recover an interruption between publishing the pair and committing its checkpoint.
            for path in (final_u, final_e):
                if path.exists():
                    total -= path.stat().st_size
                    path.unlink()
            guard(work, output, cfg)
            up, ep = group_dir/'usage'/name, group_dir/'events'/name
            # Empty events still need a valid schema-bearing Parquet file.
            for table,path in [('instance_usage',up),('instance_events',ep)]:
                if not path.exists():
                    path.parent.mkdir(exist_ok=True)
                    copy_parquet(db,f'SELECT * FROM {table} LIMIT 0',path)
            size = up.stat().st_size + ep.stat().st_size
            if total+size <= payload_limit:
                uh, eh = checksum(up), checksum(ep)
                publish(up,final_u)
                publish(ep,final_e)
                db.execute('INSERT INTO exported VALUES (?,?,?,?,?)',[cid,idx,size,uh,eh])
                db.execute('CHECKPOINT')
                total += size
            else:
                up.unlink()
                ep.unlink()
            if rank % 100 == 0:
                LOG.info('Export %d/%d; output %.3f / %.3f GB',rank,len(rows),total/GB,cfg['target_gb'])
        shutil.rmtree(group_dir,ignore_errors=True)
    for table, sql in [
        ('candidate_cohort','SELECT c.*,e.collection_id IS NOT NULL AS retained FROM '
         'candidates c LEFT JOIN exported e USING(collection_id,instance_index) ORDER BY rank'),
        ('cohort','SELECT c.* FROM candidates c JOIN exported USING(collection_id,instance_index) ORDER BY rank'),
        ('quality','SELECT q.* FROM quality q JOIN exported USING(collection_id,instance_index)'),
        ('checksums','SELECT * FROM exported')]:
        tmp = work/(table+'.partial.parquet')
        copy_parquet(db, sql, tmp)
        publish(tmp,output/(table+'.parquet'))
    n, valid = db.execute('SELECT count(*),count(*) FILTER(WHERE "valid") FROM quality '
                         'JOIN exported USING(collection_id,instance_index)').fetchone()
    source_counts = dict(db.execute("SELECT split_part(name,'-',1),sum(rows) FROM done "
                                    "WHERE phase='data' GROUP BY 1").fetchall())
    if not n:
        raise Paused('No complete instance pair fits the byte target')
    root_hashes = {p.name:checksum(p) for p in output.glob('*.parquet') if p.is_file()}
    manifest = {'format_version':VERSION, 'status':'complete', 'config':cfg,
                'source_files':sources, 'source_row_counts':source_counts,
                'instances':n, 'full_window_backtest_valid_instances':valid,
                'selection':'prefix-complete; seeded MD5 order; whole-instance byte-budget packing',
                'time_window':[cfg['t0'],cfg['t1']], 'output_bytes':0,'metadata_sha256':root_hashes,
                'notes':['No gap imputation; full-window validity is a diagnostic, not cohort selection.',
                         'Whole-window byte-size packing can bias the retained subset; original candidates are retained.',
                         'A backtest subset selected for future completeness has survivorship bias.',
                         'Byte target is best effort within max_objects; no padding or duplicate data.']}
    for _ in range(4):
        atomic_json(output/'manifest.json',manifest)
        size = tree_bytes(output)
        if manifest['output_bytes']==size:
            break
        manifest['output_bytes']=size
    if tree_bytes(output) > int(cfg['target_gb']*GB):
        (output/'manifest.json').unlink()
        raise Paused('Metadata exceeded final hard cap; output NOT marked complete')
    db.close()
    LOG.info('COMPLETE: %d instances, %d strict full-window usable; %.3f GB',n,valid,tree_bytes(output)/GB)
    if size < cfg['target_gb']*GB*.9:
        LOG.warning('Output below 90%% of target: object ceiling/time window and real data '
                    'are binding; no padding was added')


def verify_dataset(output):
    import pyarrow.parquet as pq
    manifest = json.loads((output/'manifest.json').read_text())
    if manifest.get('status')!='complete':
        raise Paused('Dataset is not marked complete')
    for name,digest in manifest['metadata_sha256'].items():
        if checksum(output/name)!=digest:
            raise Paused(f'Metadata checksum mismatch: {name}')
    rows = pq.read_table(output/'checksums.parquet').to_pylist()
    for row in rows:
        name = f'{row["collection_id"]}_{row["instance_index"]}.parquet'
        for folder,key in [('usage.parquet','usage_hash'),('events.parquet','events_hash')]:
            if checksum(output/folder/name)!=row[key]:
                raise Paused(f'Checksum mismatch: {folder}/{name}')
    if tree_bytes(output)>manifest['config']['target_gb']*GB:
        raise Paused('Output byte cap exceeded (external files added?)')
    return manifest


def worker(work, output, cfg, sources):
    if hasattr(os,'nice'):
        os.nice(10)
    ingest(work,output,cfg,sources,'prefix')
    ingest(work,output,cfg,sources,'data')
    export(work,output,cfg,sources)


def supervise(work, output, cfg, lock_fds=()):
    import psutil
    env = dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1',
               MKL_NUM_THREADS='1', ARROW_NUM_THREADS='1',
               VMFORECAST_SUPERVISOR_PID=str(os.getpid()))
    cmd = [sys.executable,str(Path(__file__).resolve()),'_worker','--work',str(work)]
    process = subprocess.Popen(cmd,env=env,start_new_session=True,pass_fds=lock_fds)
    child = psutil.Process(process.pid)
    reason = None
    try:
        while process.poll() is None:
            time.sleep(.5)
            try:
                rss = child.memory_info().rss + sum(p.memory_info().rss for p in child.children(recursive=True))
                if rss > cfg['rss_mib']*MIB:
                    raise Paused('Worker RSS limit reached; checkpoint preserved; lower --batch-rows/threads')
                if psutil.virtual_memory().available < 256*MIB:
                    raise Paused('System available RAM below 256 MiB; checkpoint preserved')
                guard(work,output,cfg)
            except psutil.NoSuchProcess:
                break
            except Paused as exc:
                reason = str(exc)
                os.killpg(process.pid,signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                break
        code = process.wait()
        if reason:
            raise Paused(reason)
        if code:
            raise Paused(f'Worker stopped (exit {code}); rerun the identical command to resume')
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid,signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL)
                process.wait()
        raise


def prepare(dataset, destination, objects=64, hours=168, offset_hours=0, memory_mib=512):
    """Produce the existing project's prepared/workload.{npz,json}, one instance at a time."""
    import duckdb
    import numpy as np
    from vmforecast.data import prepare_usage, workload_from_tidy, save_workload
    import pandas as pd
    import pyarrow.parquet as pq
    manifest = json.loads((dataset/'manifest.json').read_text())
    if manifest['status'] != 'complete':
        raise Paused('Dataset is not complete')
    cfg = manifest['config']
    t0 = cfg['t0']+int(offset_hours*3600_000_000)
    t1 = t0+int(hours*3600_000_000)
    if t0 < cfg['t0'] or t1 > cfg['t1'] or (t1-t0)%STEP:
        raise Paused('Requested backtest window outside downloaded data / five-minute grid')
    if not 1 <= objects <= 512 or hours <= 1:
        raise Paused('prepare: use 1..512 instances and more than one hour; run several smaller backtests')
    ticks = (t1-t0)//STEP
    if ticks*objects*256 > memory_mib*MIB:
        raise Paused('Backtest preparation estimate exceeds RAM budget; reduce objects/hours')
    db = duckdb.connect(config={'memory_limit':f'{memory_mib}MiB','threads':'1'})
    frames, ids = [], []
    cohort = pq.read_table(dataset/'cohort.parquet').sort_by([('rank','ascending')]).to_pylist()
    for candidate in cohort:
        cid,idx = candidate['collection_id'],candidate['instance_index']
        name = f'{cid}_{idx}.parquet'
        # Check one instance at a time; no global month-wide sort or all-data pandas load.
        for view,folder,fields,condition in [
            ('u','usage.parquet','start_time,end_time,collection_id,instance_index,machine_id,cpu,mem',
             f'start_time<{t1} AND end_time>{t0}'),
            ('e','events.parquet','time,type,collection_id,instance_index,missing_type',f'time<{t1}')]:
            db.execute(f'CREATE OR REPLACE VIEW {view} AS SELECT {fields} FROM read_parquet('
                       f'{literal(dataset/folder/name)}) WHERE {condition}')
        ok = db.execute('SELECT "valid" FROM ('+quality_sql('u','e',t0,t1)+')').fetchone()
        if not ok or not ok[0]:
            continue
        records = db.execute('SELECT (SELECT count(*) FROM u)+(SELECT count(*) FROM e)').fetchone()[0]
        if records*256 > memory_mib*MIB//4:
            continue  # Exploratory subset also obeys the per-instance RAM guard.
        raw = db.execute('SELECT * FROM u').fetchdf()
        ev = db.execute('SELECT * FROM e').fetchdf()
        frame, _ = prepare_usage(raw,ev,t0,t1)
        frames.append(frame)
        ids.append((cid,idx))
        if len(ids)>=objects:
            break
    if not ids:
        raise Paused('No strict continuous instances fitting RAM in this window; '
                     'try a shorter/shifted window')
    tidy = pd.concat(frames,ignore_index=True)
    warmup = 12
    peaks = tidy[tidy.tick<warmup].groupby('service_id')[['cpu','mem']].max().to_numpy()
    # Synthetic hosts; never claim replay of physical Google machines.
    capacity = np.maximum(peaks.max(axis=0)*2,1e-9)
    capacities = np.tile(capacity,(max(2,len(ids)),1))
    workload = workload_from_tidy(tidy,capacities,warmup=warmup,origin_us=t0,
        provenance={'source':str(dataset.resolve()),'window':[t0,t1],
                    'selection':'EXPLORATORY future-complete-case subset; survivorship bias',
                    'hosts':'synthetic capacities from initial observable prefix'})
    save_workload(workload,destination)
    atomic_json(destination/'quality.json',{'valid':True,'instances':len(ids),'ticks':ticks,
        'selection':'EXPLORATORY future-complete-case subset; survivorship bias',
        'hosts':'synthetic; machine events retained separately'})
    db.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command',required=True)
    run = sub.add_parser('download',help='Run/resume unauthenticated HTTP extraction')
    run.add_argument('--output',type=Path,required=True)
    run.add_argument('--work',type=Path,required=True,help='Local SSD scratch, separate from output')
    run.add_argument('--cell',choices=list('abcdefgh'),default='a')
    run.add_argument('--days',type=float,default=30)
    run.add_argument('--t0',type=int,default=600_000_000)
    run.add_argument('--prefix-hours',type=int,default=1)
    run.add_argument('--max-objects',type=int,default=25000)
    run.add_argument('--target-gb',type=float,default=15)
    run.add_argument('--disk-gb',type=float,default=100)
    run.add_argument('--reserve-gb',type=float,default=5)
    run.add_argument('--stage-gb',type=float,default=65)
    run.add_argument('--spill-gb',type=float,default=8)
    run.add_argument('--memory-mib',type=int,default=512,help='DuckDB buffer memory, not total RSS')
    run.add_argument('--rss-mib',type=int,default=1536,help='Supervisor ceiling for total worker RSS')
    run.add_argument('--threads',type=int,default=2)
    run.add_argument('--batch-rows',type=int,default=16384)
    run.add_argument('--max-shard-mib',type=int,default=128)
    run.add_argument('--retry-hours',type=float,default=12)
    run.add_argument('--seed',type=int,default=7)
    run.add_argument('--columns',choices=['research','core'],default='research',
                     help='research retains all source columns; core downloads only backtest fields')
    run.add_argument('--keep-stage',action='store_true',help='Keep local staging DB after verified completion')
    verify = sub.add_parser('verify',help='Verify finished output checksums without the staging DB')
    verify.add_argument('--dataset',type=Path,required=True)
    prep = sub.add_parser('prepare',help='Create a bounded backtest subset; requires project installed')
    prep.add_argument('--dataset',type=Path,required=True)
    prep.add_argument('--destination',type=Path,required=True)
    prep.add_argument('--objects',type=int,default=64)
    prep.add_argument('--hours',type=float,default=168)
    prep.add_argument('--offset-hours',type=float,default=0)
    hidden = sub.add_parser('_worker',help=argparse.SUPPRESS)
    hidden.add_argument('--work',type=Path,required=True)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == 'verify':
        m = verify_dataset(args.dataset)
        print(json.dumps({k:m[k] for k in ('status','instances','output_bytes')},indent=2))
        return
    if args.command == 'prepare':
        # The standalone downloader can run without project installation. Preparation needs it.
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
        prepare(args.dataset,args.destination,args.objects,args.hours,args.offset_hours)
        return
    if args.command == '_worker':
        if sys.platform.startswith('linux') and 'VMFORECAST_SUPERVISOR_PID' in os.environ:
            import ctypes
            # Prevent an unmonitored orphan if the supervisor is killed by the OS/user.
            if ctypes.CDLL(None).prctl(1,signal.SIGTERM,0,0,0)!=0:
                raise Paused('Unable to install Linux parent-death signal')
            if os.getppid()!=int(os.environ['VMFORECAST_SUPERVISOR_PID']):
                raise Paused('Supervisor disappeared during worker startup')
        work = args.work.resolve()
        cfg = json.loads((work/'config.json').read_text())
        setup_log(work)
        worker(work,Path(cfg['output']),cfg,json.loads((work/'sources.json').read_text()))
        return
    work, output = args.work.expanduser().resolve(), args.output.expanduser().resolve()
    if work == output or work in output.parents or output in work.parents:
        raise Paused('--work and --output must be separate non-nested directories')
    cfg = vars(args).copy()
    cfg.pop('command')
    cfg.update(work=str(work),output=str(output),format_version=VERSION,
               t1=args.t0+int(args.days*24*3600_000_000))
    if (args.days<=0 or args.prefix_hours<=0 or args.days*24<args.prefix_hours
        or cfg['t1']<=args.t0 or (cfg['t1']-args.t0)%STEP or args.max_objects<=0
        or args.target_gb<=.02 or args.stage_gb<=0 or args.spill_gb<=0
        or args.disk_gb < args.target_gb+args.stage_gb+args.spill_gb+4
        or args.reserve_gb<1 or args.memory_mib<128 or args.rss_mib<args.memory_mib+256
        or not 1<=args.threads<=4 or not 1<=args.batch_rows<=65536
        or not 1<=args.max_shard_mib<=512 or args.retry_hours<0):
        raise Paused('Invalid configuration / insufficient disk or RAM budget separation')
    work.mkdir(parents=True,exist_ok=True)
    output.mkdir(parents=True,exist_ok=True)
    with (work/'run.lock').open('w') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Paused('Another downloader owns this work directory') from exc
        # Prevent a second job with a different work directory from writing the same output.
        with (output/'download.lock').open('a') as output_lock:
            try:
                fcntl.flock(output_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise Paused('Another downloader owns this output directory') from exc
            setup_log(work)
            config_path = work/'config.json'
            if config_path.exists():
                previous = json.loads(config_path.read_text())
                operational = {'disk_gb','reserve_gb','stage_gb','spill_gb','memory_mib',
                               'rss_mib','threads','batch_rows','max_shard_mib','retry_hours','keep_stage'}
                if {k:v for k,v in previous.items() if k not in operational} != {
                        k:v for k,v in cfg.items() if k not in operational}:
                    raise Paused('Dataset configuration changed: use original selection/window/columns '
                                 'or new directories; resource limits alone may be adjusted')
                atomic_json(config_path,cfg)
            else:
                if list(output.glob('*.parquet')) or (output/'manifest.json').exists():
                    raise Paused('Output already contains data; use its original work directory')
                atomic_json(config_path,cfg)
            if (output/'manifest.json').exists():
                verify_dataset(output)
                LOG.info('Output already complete and verified; nothing to download')
                return
            guard(work,output,cfg)
            sources_path = work/'sources.json'
            if not sources_path.exists():
                LOG.info('Listing public sources (no credentials)')
                atomic_json(sources_path,list_sources(args.cell))
            LOG.info('Resuming %s; window %.1f days; output cap %.3f GB; max instances %d',
                     args.cell,args.days,args.target_gb,args.max_objects)
            supervise(work,output,cfg,(lock.fileno(),output_lock.fileno()))
            verify_dataset(output)
            if not args.keep_stage:
                for name in ('stage.duckdb','stage.duckdb.wal','source.partial.parquet'):
                    (work/name).unlink(missing_ok=True)
                for name in ('spill','export-group'):
                    shutil.rmtree(work/name,ignore_errors=True)
                LOG.info('Verified output; freed staging DB and temporary files')
    LOG.info('Verified output complete: %s',output)


if __name__ == '__main__':
    try:
        main()
    except (Paused,KeyboardInterrupt) as exc:
        LOG.error('PAUSED: %s',exc)
        sys.exit(75)
