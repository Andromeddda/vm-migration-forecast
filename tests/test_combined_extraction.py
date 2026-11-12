"""Execute production SQL locally on SQLite window functions, without Google access.

Only source/struct names and the BigQuery hash primitive are adapted. The hash
stub exercises signed order and collisions; real FARM_FINGERPRINT is not validated.
This is a semantic fixture test, not a BigQuery syntax/scan benchmark.
"""
from dataclasses import asdict
import json
import random
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from vmforecast.google2019 import (
    BoundedExtractor, ExtractionConfig, EXTRACTION_STRATEGY, build_queries,
)

LEGACY_COHORT = '''
SELECT collection_id, instance_index FROM source_rows
WHERE collection_type = 0
  AND (alloc_collection_id IS NULL OR alloc_collection_id = 0)
  AND machine_id > 0
  AND start_time < @prefix_end AND end_time > @t0
  AND end_time > start_time AND cpu IS NOT NULL AND mem IS NOT NULL
GROUP BY collection_id, instance_index
HAVING SUM(LEAST(end_time, @prefix_end) - GREATEST(start_time, @t0)) = @prefix_end - @t0
ORDER BY FARM_FINGERPRINT(CONCAT(CAST(collection_id AS STRING), ':',
         CAST(instance_index AS STRING))), collection_id, instance_index
LIMIT @n
'''
LEGACY_USAGE = '''
SELECT start_time, end_time, collection_id, instance_index, machine_id,
       cpu, mem, sample_rate FROM source_rows
WHERE collection_type = 0
  AND (alloc_collection_id IS NULL OR alloc_collection_id = 0)
  AND machine_id > 0
  AND start_time < @t1 AND end_time > @t0 AND end_time > start_time
ORDER BY collection_id, instance_index, start_time, end_time
'''


def fixture_db(rows):
    db = sqlite3.connect(':memory:')
    db.create_function('LEAST', 2, min)
    db.create_function('GREATEST', 2, max)
    db.create_function('CONCAT', -1, lambda *values: ''.join(map(str, values)))
    db.create_function('FARM_FINGERPRINT', 1, lambda key: sum(key.encode()) % 5 - 2)
    db.execute('''CREATE TABLE source_rows (
        start_time INTEGER, end_time INTEGER, collection_id INTEGER,
        instance_index INTEGER, machine_id INTEGER, cpu REAL, mem REAL,
        sample_rate REAL, collection_type INTEGER, alloc_collection_id INTEGER)''')
    db.executemany('INSERT INTO source_rows VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    return db


def adapted_sql(cfg):
    return (build_queries(cfg)['usage']
            .replace(f'`{cfg.table_prefix}.instance_usage`', 'source_rows')
            .replace('average_usage.cpus', 'cpu')
            .replace('average_usage.memory', 'mem'))


@pytest.mark.parametrize('seed', range(12))
@pytest.mark.parametrize('n', [1, 3, 100])
def test_combined_sql_equals_legacy_selection_and_usage(seed, n):
    cfg = ExtractionConfig(source_project='public-source', hours=3, n_services=n)
    start, hour = cfg.t0, 3_600_000_000
    rows = []
    rng = random.Random(seed)
    for service in range(1, 21):
        # Different row counts per service; several valid services share hash ranks.
        cuts = sorted({0, hour, 2*hour, 3*hour, rng.randrange(1, hour)})
        for a, b in zip(cuts, cuts[1:]):
            cpu, mem, machine, kind, alloc = .1, .2, 1, 0, None
            if service == 2 and a == 0: cpu = None  # invalid prefix
            if service == 3 and a >= hour: cpu = None  # preserve future nulls
            if service == 4: machine = 0
            if service == 5: kind = 1
            if service == 6: alloc = 12
            if service == 7 and a == 0: continue  # prefix gap
            rows.append((start+a, start+b, service, 0, machine,
                         cpu, mem, 1., kind, alloc))
    # Prefix-only service (future coverage NOT used for selecting cohort).
    rows.append((start, start+hour, 101, 1, 1, .1, .2, 1., 0, 0))
    # Interval spanning both window boundaries and prefix end.
    rows.append((start-hour, cfg.t1+hour, 102, 1, 1, .1, .2, 1., 0, None))
    # IDs must remain exact above float's integer precision.
    rows.append((start, cfg.t1, 9007199254740993, 2, 1, .1, .2, 1., 0, None))
    # Invalid interval and rows entirely outside the requested window.
    rows.extend([(start, start, 103, 0, 1, .1, .2, 1., 0, None),
                 (0, start, 104, 0, 1, .1, .2, 1., 0, None),
                 (cfg.t1, cfg.t1+hour, 105, 0, 1, .1, .2, 1., 0, None)])
    params = {'t0': start, 't1': cfg.t1, 'prefix_end': start+hour, 'n': n}
    with fixture_db(rows) as db:
        cohort = set(db.execute(LEGACY_COHORT, params).fetchall())
        expected = [row for row in db.execute(LEGACY_USAGE, params)
                    if (row[2], row[3]) in cohort]
        actual = db.execute(adapted_sql(cfg), params).fetchall()
    assert actual == expected
    assert set((r[2], r[3]) for r in actual) == cohort


def test_no_future_coverage_filter_and_empty_cohort():
    cfg = ExtractionConfig(source_project='public-source', hours=3)
    hour = 3_600_000_000
    row = (cfg.t0, cfg.t0+hour, 1, 0, 1, .1, .2, 1., 0, None)
    params = {'t0': cfg.t0, 't1': cfg.t1, 'prefix_end': cfg.t0+hour, 'n': 10}
    with fixture_db([row]) as db:
        assert len(db.execute(adapted_sql(cfg), params).fetchall()) == 1
    with fixture_db([(*row[:5], None, *row[6:])]) as db:
        assert not db.execute(adapted_sql(cfg), params).fetchall()


class FakeResult:
    def __init__(self, frame): self.frame, self.total_rows = frame, len(frame)
    def to_dataframe(self, create_bqstorage_client):
        assert create_bqstorage_client is False
        return self.frame.copy()


class FakeClient:
    def __init__(self, estimates=(50, 40), usage=None):
        self.calls = []
        self.estimates = estimates
        self.usage = (pd.DataFrame({'collection_id': [2, 2, 1],
                                   'instance_index': [0, 0, 0]})
                      if usage is None else usage)
    def query(self, sql, location, job_config):
        self.calls.append((sql, job_config))
        is_usage = 'instance_usage' in sql
        if job_config.dry_run:
            return SimpleNamespace(total_bytes_processed=self.estimates[0 if is_usage else 1])
        assert job_config.maximum_bytes_billed > 0
        frame = self.usage if is_usage else pd.DataFrame({'time': [1]})
        return SimpleNamespace(job_id='usage' if is_usage else 'events',
                               total_bytes_billed=30,
                               result=lambda: FakeResult(frame))


def extractor(tmp_path, client=None, **kwargs):
    ex = BoundedExtractor(client or FakeClient(), ExtractionConfig(
        source_project='public-source', max_query_bytes=100, total_budget_bytes=200,
        **kwargs), tmp_path)
    ex.location = 'US'
    return ex


def test_plan_reports_both_estimates_above_budget_without_reservation(tmp_path):
    ex = extractor(tmp_path, FakeClient(estimates=(500, 40)))
    plan = ex.plan()
    assert plan['usage']['reasons'] == ['per_query_limit', 'remaining_budget']
    assert plan['total_scan_upper_estimate_bytes'] == 540
    assert not plan['fits_configured_budget_conservatively']
    assert len(ex.client.calls) == 2
    assert all(c.dry_run for _, c in ex.client.calls)
    assert not ex.ledger['jobs'] and not ex.ledger_path.exists()
    ex.extract(execute=False)
    assert all(c.dry_run for _, c in ex.client.calls)


def test_plan_unknown_estimate_and_existing_reservations(tmp_path):
    ex = extractor(tmp_path, FakeClient(estimates=(None, 40)))
    ex.ledger['jobs'] = [{'budget_charge_bytes': 180}]
    plan = ex.plan()
    assert plan['usage']['reasons'] == ['unknown_scan']
    assert plan['total_scan_upper_estimate_bytes'] is None
    assert plan['usage']['remaining_budget_bytes'] == 20
    assert plan['events_scan_upper_estimate']['reasons'] == ['remaining_budget']


def test_execute_uses_two_jobs_derives_cohort_and_accounts_each_once(tmp_path):
    ex = extractor(tmp_path)
    usage, events = ex.extract(execute=True)
    live = [(sql, c) for sql, c in ex.client.calls if not c.dry_run]
    assert len(live) == 2
    assert live[0][0].count('`public-source.clusterdata_2019_a.instance_usage`') == 1
    assert [r['name'] for r in ex.ledger['jobs']] == ['usage', 'events']
    assert sum(r['budget_charge_bytes'] for r in ex.ledger['jobs']) == 60
    assert pd.read_parquet(tmp_path/'cohort.parquet').values.tolist() == [[1, 0], [2, 0]]
    manifest = json.loads((tmp_path/'extraction_manifest.json').read_text())
    assert manifest['strategy'] == EXTRACTION_STRATEGY
    assert set(manifest['queries']) == {'usage', 'events'}
    assert usage.equals(ex.client.usage)
    assert len(events) == 1
    # Every artifact is present and linked to its manifest checksum.
    from vmforecast.experiments import file_hash
    for name, digest in manifest['files'].items(): assert file_hash(tmp_path/name) == digest


def test_execute_budget_failure_before_real_job(tmp_path):
    ex = extractor(tmp_path, FakeClient(estimates=(500, 40)))
    with pytest.raises(RuntimeError, match='per_query_limit'):
        ex.extract(execute=True)
    assert all(c.dry_run for _, c in ex.client.calls)
    assert not ex.ledger['jobs']


def test_row_cap_and_empty_usage_stop_before_events(tmp_path):
    ex = extractor(tmp_path, max_result_rows=2)
    with pytest.raises(RuntimeError, match='local row cap'):
        ex.extract(execute=True)
    assert len([1 for _, c in ex.client.calls if not c.dry_run]) == 1
    assert not (tmp_path/'extraction_manifest.json').exists()
    empty = extractor(tmp_path/'empty', FakeClient(usage=pd.DataFrame()))
    with pytest.raises(RuntimeError, match='Empty prefix cohort'):
        empty.extract(execute=True)
    assert len([1 for _, c in empty.client.calls if not c.dry_run]) == 1


def test_nonempty_legacy_ledger_not_silently_reused_or_reset(tmp_path):
    cfg = ExtractionConfig(source_project='public-source')
    path = tmp_path/'query_ledger.json'
    original = {'source_config': asdict(cfg), 'jobs': [{'budget_charge_bytes': 80}]}
    path.write_text(json.dumps(original))
    with pytest.raises(ValueError, match='Legacy extraction ledger'):
        BoundedExtractor(FakeClient(), cfg, tmp_path)
    assert json.loads(path.read_text()) == original
