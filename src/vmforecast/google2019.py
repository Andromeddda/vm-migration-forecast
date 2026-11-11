"""Bounded BigQuery extraction. Authentication is supplied by the runtime, never serialized."""
from dataclasses import dataclass, asdict
from pathlib import Path
import hashlib
import json
import uuid
import re
from .experiments import json_write, file_hash

@dataclass(frozen=True)
class ExtractionConfig:
    source_project: str = ''
    cell: str = 'a'
    t0: int = 600_000_000
    hours: int = 48
    prefix_hours: int = 1
    n_services: int = 100
    max_query_bytes: int = 5 * 1024**3
    total_budget_bytes: int = 15 * 1024**3
    max_result_rows: int = 2_000_000

    def __post_init__(self):
        if self.source_project and not re.fullmatch(r'[a-zA-Z0-9:.\-]+', self.source_project):
            raise ValueError('Invalid source project ID')
        if self.cell not in list('abcdefgh') or len(self.cell) != 1:
            raise ValueError('cell must be a..h')
        for key in ['t0','hours','prefix_hours','n_services','max_query_bytes','total_budget_bytes','max_result_rows']:
            if not isinstance(getattr(self,key), int) or getattr(self,key) <= 0:
                raise ValueError(f'{key} must be a positive integer')
        if self.prefix_hours > self.hours:
            raise ValueError('prefix must be inside extraction period')

    @property
    def table_prefix(self):
        source = self.source_project
        if not source:
            from .settings import Settings
            source = Settings.load().source_project
        if not source:
            raise ValueError('Set VMFORECAST_SOURCE_PROJECT with sh lab configure')
        return f'{source}.clusterdata_2019_{self.cell}'

    @property
    def t1(self):
        return self.t0 + self.hours*3600*1_000_000


def build_queries(config):
    p = config.table_prefix
    cohort = f'''
    SELECT collection_id, instance_index
    FROM `{p}.instance_usage`
    WHERE collection_type = 0
      AND (alloc_collection_id IS NULL OR alloc_collection_id = 0)
      AND machine_id > 0
      AND start_time < @prefix_end AND end_time > @t0
      AND end_time > start_time
      AND average_usage.cpus IS NOT NULL AND average_usage.memory IS NOT NULL
    GROUP BY collection_id, instance_index
    HAVING SUM(LEAST(end_time, @prefix_end) - GREATEST(start_time, @t0)) = @prefix_end - @t0
    ORDER BY FARM_FINGERPRINT(CONCAT(CAST(collection_id AS STRING), ':',
             CAST(instance_index AS STRING))), collection_id, instance_index
    LIMIT @n
    '''
    usage = f'''
    SELECT start_time, end_time, collection_id, instance_index, machine_id,
           average_usage.cpus AS cpu, average_usage.memory AS mem, sample_rate
    FROM `{p}.instance_usage`
    WHERE collection_type = 0
      AND (alloc_collection_id IS NULL OR alloc_collection_id = 0)
      AND machine_id > 0
      AND start_time < @t1 AND end_time > @t0 AND end_time > start_time
      AND CONCAT(CAST(collection_id AS STRING), ':',
                 CAST(instance_index AS STRING)) IN UNNEST(@keys)
    ORDER BY collection_id, instance_index, start_time, end_time
    '''
    events = f'''
    SELECT time, type, collection_id, instance_index, machine_id, missing_type,
           resource_request.cpus AS cpu_request, resource_request.memory AS mem_request
    FROM `{p}.instance_events`
    WHERE time < @t1
      AND CONCAT(CAST(collection_id AS STRING), ':',
                 CAST(instance_index AS STRING)) IN UNNEST(@keys)
    ORDER BY collection_id, instance_index, time, type
    '''
    return {'cohort':cohort, 'usage':usage, 'events':events}


class BoundedExtractor:
    def __init__(self, client, config, directory):
        self.client, self.config = client, config
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.ledger_path = self.directory/'query_ledger.json'
        if self.ledger_path.exists():
            self.ledger = json.loads(self.ledger_path.read_text())
            if self.ledger['source_config'] != asdict(config):
                raise ValueError('Different extraction config: use a separate output directory')
        else:
            self.ledger = {'source_config':asdict(config), 'jobs':[]}
        self.location = None

    def metadata(self):
        from google.cloud import bigquery
        ds = bigquery.DatasetReference(self.config.table_prefix.rsplit('.', 1)[0], f'clusterdata_2019_{self.config.cell}')
        self.location = self.client.get_dataset(ds).location
        tables = {}
        for name in ['collection_events','instance_events','machine_events','machine_attributes','instance_usage']:
            table = self.client.get_table(bigquery.TableReference(ds,name))
            tables[name] = {'schema':[f.to_api_repr() for f in table.schema],
                            'num_rows':table.num_rows,'num_bytes':table.num_bytes,
                            'partitioning':table.time_partitioning.to_api_repr() if table.time_partitioning else None,
                            'clustering_fields':table.clustering_fields}
        json_write(self.directory/'source_metadata.json', {'location':self.location,'tables':tables})
        return tables

    def query(self, name, sql, params, execute=False):
        from google.cloud import bigquery
        if self.location is None:
            self.metadata()
        dry = self.client.query(sql, location=self.location,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, query_parameters=params))
        estimated = dry.total_bytes_processed
        spent = sum(j.get('budget_charge_bytes',0) for j in self.ledger['jobs'])
        remaining = self.config.total_budget_bytes-spent
        cap = min(self.config.max_query_bytes,remaining)
        plan = {'name':name,'estimated_bytes':estimated,'cap_bytes':cap,
                'remaining_budget_bytes':remaining,'execute':execute}
        print(plan)
        if estimated is None or estimated > cap or cap <= 0:
            raise RuntimeError('Unknown/too-large scan: query NOT executed; revise extraction or explicit budget')
        if not execute:
            return None
        # Reserve the full cap BEFORE execution. Unknown outcomes keep their reservation.
        row = {'name':name,'reservation_id':uuid.uuid4().hex,'status':'reserved',
               'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),
               'estimated_bytes':estimated,'budget_charge_bytes':cap}
        self.ledger['jobs'].append(row)
        json_write(self.ledger_path,self.ledger)
        job = self.client.query(sql, location=self.location, job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=cap, query_parameters=params, use_query_cache=True))
        row['job_id'] = job.job_id
        json_write(self.ledger_path,self.ledger)
        result = job.result()
        row['status'] = 'completed'
        # Cache hits may have 0 billed bytes; unknown billed usage conservatively retains cap.
        row['budget_charge_bytes'] = cap if job.total_bytes_billed is None else job.total_bytes_billed
        row['rows'] = result.total_rows
        json_write(self.ledger_path,self.ledger)
        if result.total_rows > self.config.max_result_rows:
            raise RuntimeError('Query completed, but result exceeds local row cap; no download attempted')
        frame = result.to_dataframe(create_bqstorage_client=False)
        path = self.directory/f'{name}.parquet'
        frame.to_parquet(path,index=False)
        row['file_sha256'] = file_hash(path)
        json_write(self.ledger_path,self.ledger)
        return frame

    def extract(self, execute=False):
        """Dry-run only by default. Execution uses prefix cohort then targeted usage/events."""
        from google.cloud import bigquery
        cfg = self.config
        q = build_queries(cfg)
        cohort = self.query('cohort',q['cohort'],[
            bigquery.ScalarQueryParameter('t0','INT64',cfg.t0),
            bigquery.ScalarQueryParameter('prefix_end','INT64',cfg.t0+cfg.prefix_hours*3600*1_000_000),
            bigquery.ScalarQueryParameter('n','INT64',cfg.n_services)],execute)
        if cohort is None:
            print('Only cohort dry-run is available before its IDs are selected. No data query executed.')
            return None
        if cohort.empty:
            raise RuntimeError('Empty prefix cohort')
        keys = [f'{int(r.collection_id)}:{int(r.instance_index)}' for r in cohort.itertuples(index=False)]
        params = [bigquery.ScalarQueryParameter('t0','INT64',cfg.t0),
                  bigquery.ScalarQueryParameter('t1','INT64',cfg.t1),
                  bigquery.ArrayQueryParameter('keys','STRING',keys)]
        usage = self.query('usage',q['usage'],params,execute)
        events = self.query('events',q['events'],params[1:],execute)
        json_write(self.directory/'extraction_manifest.json', {
            'source':'https://github.com/google/cluster-data/blob/master/ClusterData2019.md',
            'license':'CC-BY', 'config':asdict(cfg),'cohort_keys':keys,
            'queries':{k:hashlib.sha256(v.encode()).hexdigest() for k,v in q.items()},
            'files':{name:file_hash(self.directory/name) for name in
                     ['cohort.parquet','usage.parquet','events.parquet']}})
        return usage, events


def main():
    import argparse
    from google.cloud import bigquery
    parser = argparse.ArgumentParser(description='Google 2019 bounded extraction via runtime ADC; dry-run by default')
    from .settings import Settings
    settings = Settings.load()
    parser.add_argument('--project', default=settings.project, help='Sandbox query project ID; default from participant configuration')
    parser.add_argument('--source-project', default=settings.source_project)
    parser.add_argument('--output', default=str(settings.data/'google2019_a_6h_20'))
    parser.add_argument('--cell', default='a', choices=list('abcdefgh'))
    parser.add_argument('--hours', type=int, default=6)
    parser.add_argument('--services', type=int, default=20)
    parser.add_argument('--max-query-gib', type=int, default=5)
    parser.add_argument('--total-budget-gib', type=int, default=15)
    parser.add_argument('--execute', action='store_true', help='Actually run bounded data queries')
    args = parser.parse_args()
    if not args.project:
        parser.error('Run sh lab configure --project YOUR_PROJECT_ID')
    cfg = ExtractionConfig(source_project=args.source_project, cell=args.cell, hours=args.hours,n_services=args.services,
        max_query_bytes=args.max_query_gib*1024**3,total_budget_bytes=args.total_budget_gib*1024**3)
    ex = BoundedExtractor(bigquery.Client(project=args.project),cfg,args.output)
    ex.extract(execute=args.execute)


if __name__ == '__main__':
    main()
