"""Strict preparation of a fixed continuous task cohort; missing usage is never zero."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from .core import Workload
from .allocators import initial_placement
from .experiments import json_write, file_hash


def prepare_usage(raw, events, t0, t1, step_us=300_000_000, report_path=None, complete_case=False):
    """Split measured interval means into bins, weighted by overlap duration.

    Strict v0.1: fixed cohort, full coverage, no lifecycle interruptions in window.
    This avoids silently stitching restarts or treating missing observations as demand.
    """
    if not isinstance(t0, int) or not isinstance(t1, int) or step_us <= 0 or (t1-t0) % step_us or t1 <= t0:
        raise ValueError('Use an exact integer-microsecond grid')
    required = {'start_time','end_time','collection_id','instance_index','cpu','mem','machine_id'}
    if not required <= set(raw.columns) or raw.empty:
        raise ValueError('Raw usage columns missing or empty')
    raw = raw.copy()
    for col in ['start_time','end_time','collection_id','instance_index','machine_id']:
        if raw[col].isna().any() or pd.api.types.is_float_dtype(raw[col].dtype):
            raise ValueError(f'{col}: integer/string IDs required, float IDs are unsafe')
        raw[col] = raw[col].map(int)
    raw['service_id'] = raw.collection_id.astype(str)+':'+raw.instance_index.astype(str)
    raw = raw[(raw.start_time < t1) & (raw.end_time > t0)]
    if raw.empty:
        raise ValueError('No samples intersect requested interval')
    if events is None or not {'time','type','collection_id','instance_index','missing_type'} <= set(events.columns):
        raise ValueError('Instance events with missing_type are required to validate lifecycle')
    events = events.copy()
    for col in ['time','type','collection_id','instance_index']:
        if events[col].isna().any() or pd.api.types.is_float_dtype(events[col].dtype):
            raise ValueError(f'Event {col} must be integer/string')
        events[col] = events[col].map(int)
    events['service_id'] = events.collection_id.astype(str)+':'+events.instance_index.astype(str)
    bad = []
    overlapping_ids = set()
    ids = sorted(raw.service_id.unique())
    T = (t1-t0)//step_us
    values = np.zeros((T,len(ids),2))
    coverage = np.zeros((T,len(ids)), dtype=np.int64)
    availability = np.zeros((T,len(ids)), dtype=np.int64)
    overlap_count = 0
    for s, sid in enumerate(ids):
        ev = events[events.service_id == sid]
        inside = ev[(ev.time > t0) & (ev.time < t1)]
        before = ev[ev.time <= t0]
        # At/after t0 a lifecycle transition is rejected conservatively; UPDATE_RUNNING allowed.
        at_start = ev[(ev.time == t0) & (ev.type.isin([0,1,2,4,5,6,7,8,9]))]
        before_types = before[before.time == before.time.max()].type.tolist() if len(before) else []
        if (not len(before) or not (set(before_types) & {3,10})
            or not set(before_types) <= {0,2,3,10}
            or not at_start.empty or (~inside.type.isin([10])).any()
            or ev.missing_type.fillna(0).ne(0).any()):
            bad.append(sid)
        previous_end = None
        for row in raw[raw.service_id == sid].sort_values(['start_time','end_time']).itertuples(index=False):
            a, b = max(t0, row.start_time), min(t1, row.end_time)
            if b <= a or row.machine_id <= 0 or not np.isfinite([row.cpu,row.mem]).all() or min(row.cpu,row.mem)<0:
                raise ValueError(f'Invalid observation for {sid}')
            if previous_end is not None and a < previous_end:
                overlap_count += 1
                overlapping_ids.add(sid)
                continue
            previous_end = b
            lo, hi = (a-t0)//step_us, (b-1-t0)//step_us
            for k in range(lo, hi+1):
                duration = min(b,t0+(k+1)*step_us)-max(a,t0+k*step_us)
                values[k,s] += np.array([row.cpu,row.mem])*duration
                coverage[k,s] += duration
                availability[k,s] = max(availability[k,s], row.end_time)
    keep = [i for i,sid in enumerate(ids) if sid not in bad and sid not in overlapping_ids
            and bool((coverage[:,i] == step_us).all())]
    dropped = [sid for i,sid in enumerate(ids) if i not in keep]
    report = {'services': len(ids), 'ticks': T, 'incomplete_service_ticks': int((coverage!=step_us).sum()),
              'overlapping_intervals': overlap_count, 'lifecycle_ambiguous_services': bad,
              'partial_interval_mean_assumption': True, 'cohort_selection': 'prefix-selected, no future filtering',
              'valid': not bad and not overlap_count and bool((coverage==step_us).all()),
              'complete_case_opt_in': bool(complete_case), 'dropped_services': dropped,
              'usable_services': len(keep)}
    if complete_case:
        report['cohort_selection'] = 'EXPLORATORY future-complete-case subset; survivorship bias'
        report['valid'] = bool(keep)
    if report_path:
        json_write(report_path, report)
    if not report['valid']:
        raise ValueError('Strict fixed-cohort validation failed; inspect quality.json. '
                         'Do not impute gaps or silently select future survivors.')
    if complete_case:
        values = values[:,keep]
        availability = availability[:,keep]
        ids = [ids[i] for i in keep]
    values /= step_us
    rows = []
    for k in range(T):
        for s,sid in enumerate(ids):
            rows.append({'tick':k, 'service_id':sid, 'cpu':values[k,s,0], 'mem':values[k,s,1],
                         'available_at_us':int(availability[k,s])})
    return pd.DataFrame(rows), report


def workload_from_tidy(frame, capacities, warmup=12, tick_seconds=300., provenance=None, origin_us=None):
    required = {'tick','service_id','cpu','mem'}
    if frame.empty or not required <= set(frame.columns) or frame.duplicated(['tick','service_id']).any():
        raise ValueError('Need unique complete tidy rows')
    ticks, ids = sorted(frame.tick.unique()), sorted(frame.service_id.unique())
    if ticks != list(range(len(ticks))) or not 1 <= warmup < len(ticks):
        raise ValueError('Ticks must be 0..T-1 with a valid warmup')
    full = pd.MultiIndex.from_product([ticks,ids], names=['tick','service_id'])
    x = frame.set_index(['tick','service_id']).reindex(full)[['cpu','mem']].to_numpy()
    if not np.isfinite(x).all() or (x<0).any():
        raise ValueError('Incomplete/negative demand; no automatic imputation')
    demand = x.reshape(len(ticks),len(ids),2)
    available = None
    if 'available_at_us' in frame.columns:
        if origin_us is None:
            raise ValueError('origin_us required for timestamped observation availability')
        step = int(tick_seconds*1_000_000)
        a = frame.set_index(['tick','service_id']).reindex(full).available_at_us.to_numpy(dtype=np.int64)
        available = ((a-int(origin_us)+step-1)//step).reshape(len(ticks),len(ids))
        if (available[:warmup] > warmup).any():
            raise ValueError('Initial placement prefix is not yet observable: extend warmup')
    p = initial_placement(demand[:warmup].max(axis=0), np.asarray(capacities))
    return Workload(demand, capacities, p, ids, tick_seconds, provenance=provenance, available_at_tick=available)


def save_workload(workload, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(directory/'workload.npz', demand=workload.demand,
                        capacities=workload.capacities, placement=workload.placement,
                        migration_weights=workload.migration_weights, available_at_tick=workload.available_at_tick)
    json_write(directory/'workload.json', {'service_ids': workload.service_ids,
        'tick_seconds': workload.tick_seconds, 'provenance': workload.provenance,
        'sha256': file_hash(directory/'workload.npz')})


def load_workload(directory):
    directory = Path(directory)
    meta = json.loads((directory/'workload.json').read_text())
    if file_hash(directory/'workload.npz') != meta['sha256']:
        raise ValueError('Workload file checksum mismatch')
    with np.load(directory/'workload.npz', allow_pickle=False) as x:
        return Workload(x['demand'], x['capacities'], x['placement'], meta['service_ids'],
                        meta['tick_seconds'], x['migration_weights'], meta['provenance'],x['available_at_tick'])
