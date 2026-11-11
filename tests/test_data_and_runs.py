import json
import numpy as np
import pandas as pd
import pytest
from vmforecast.data import prepare_usage,save_workload,load_workload
from vmforecast.synthetic import make_synthetic
from vmforecast.experiments import run_saved
from vmforecast import WindowSignal,SimulationConfig
from vmforecast.google2019 import ExtractionConfig,build_queries,BoundedExtractor
from vmforecast.reporting import paired_difference


def fixture_rows():
    raw=pd.DataFrame({'start_time':[0,5,10],'end_time':[5,10,20],
        'collection_id':['9007199254740993']*3,'instance_index':['0']*3,
        'machine_id':[1]*3,'cpu':[1.,3.,4.],'mem':[.1,.3,.4]})
    events=pd.DataFrame({'time':[0],'type':[3], 'collection_id':['9007199254740993'],
                         'instance_index':['0'],'missing_type':[0]})
    return raw,events


def test_interval_weighting_and_large_integer_ids():
    raw,ev=fixture_rows()
    tidy,qa=prepare_usage(raw,ev,0,20,10)
    assert tidy.cpu.tolist()==[2.,4.]
    assert tidy.available_at_us.tolist()==[10,20]
    assert tidy.service_id.iloc[0]=='9007199254740993:0'
    assert qa['valid']


def test_gaps_overlaps_and_restart_rejected(tmp_path):
    raw,ev=fixture_rows()
    for altered in [raw.iloc[1:],pd.concat([raw,raw.iloc[:1]])]:
        with pytest.raises(ValueError):prepare_usage(altered,ev,0,20,10,tmp_path/'qa.json')
        assert not json.loads((tmp_path/'qa.json').read_text())['valid']
    ev=pd.concat([ev,ev.assign(time=15,type=4)])
    with pytest.raises(ValueError):prepare_usage(raw,ev,0,20,10)


def test_float_ids_rejected():
    raw,ev=fixture_rows();raw['collection_id']=1.0
    with pytest.raises(ValueError):prepare_usage(raw,ev,0,20,10)


def test_workload_roundtrip(tmp_path):
    w=make_synthetic(ticks=30,services=4,hosts=2,warmup=3)
    save_workload(w,tmp_path)
    assert load_workload(tmp_path).fingerprint()==w.fingerprint()


def test_run_resume_and_corruption_recovery(tmp_path):
    w=make_synthetic(ticks=30,services=4,hosts=2,warmup=3)
    s,p=run_saved(w,WindowSignal(),SimulationConfig(warmup=3),tmp_path)
    s2,p2=run_saved(w,WindowSignal(),SimulationConfig(warmup=3),tmp_path)
    assert s==s2 and p==p2
    (p/'ticks.parquet').write_bytes(b'corrupted')
    s3,_=run_saved(w,WindowSignal(),SimulationConfig(warmup=3),tmp_path)
    assert s3['migrations_completed']==s['migrations_completed']
    pd.read_parquet(p/'ticks.parquet')


def test_query_scope_and_config_validation():
    q=build_queries(ExtractionConfig(source_project='public-source'))
    assert 'LIMIT @n' in q['cohort'] and '@keys' in q['usage']
    assert 'time < @t1' in q['events'] and 'time >= @t0' not in q['events']
    with pytest.raises(ValueError):ExtractionConfig(cell='a` WHERE true')


def test_budget_stops_before_live_query(tmp_path):
    class Dataset:location='US'
    class Dry:total_bytes_processed=1000
    class Client:
        calls=[]
        def query(self,sql,location,job_config):
            assert job_config.dry_run  # any paid execution is a failure in this fake
            self.calls.append(sql);return Dry()
    ex=BoundedExtractor(Client(),ExtractionConfig(max_query_bytes=100),tmp_path)
    ex.location='US'
    with pytest.raises(RuntimeError):ex.query('test','SELECT 1',[],execute=True)
    assert not ex.ledger['jobs']


def test_ci_uses_paired_cases_and_requires_replicates():
    f=pd.DataFrame([{'case':'seed0','method':'a','price':0.,'migrations_completed':2},
                    {'case':'seed0','method':'b','price':0.,'migrations_completed':3}])
    ci=paired_difference(f,'a','b')
    assert ci.iloc[0].mean_difference==-1 and np.isnan(ci.iloc[0].ci_low)
