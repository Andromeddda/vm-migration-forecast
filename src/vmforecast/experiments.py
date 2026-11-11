"""Content-addressed run directories; restart a grid by skipping verified completed runs."""
from dataclasses import asdict, replace
from pathlib import Path
import hashlib
import importlib.metadata
import json
import platform
import tempfile
import os
import pandas as pd
from .core import Backtester
from .allocators import GreedyAllocator, NoMoveAllocator


def code_hash():
    h = hashlib.sha256()
    for p in sorted(Path(__file__).parent.glob('*.py')):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()


def json_write(path, obj):
    """Replace one complete file; a run is committed only after summary.json exists."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, allow_nan=False)
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def file_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1024*1024), b''):
            h.update(b)
    return h.hexdigest()


def run_saved(workload, signal, config, output_dir, allocator=None):
    allocator = allocator or GreedyAllocator()
    manifest = {"workload_hash": workload.fingerprint(), "provenance": workload.provenance,
                "signal": {"type": type(signal).__name__, "params": asdict(signal)},
                "allocator": {"type": type(allocator).__name__, "params": asdict(allocator)},
                "simulation": asdict(config), "code_hash": code_hash()}
    identity = json.dumps(manifest, sort_keys=True, ensure_ascii=False, allow_nan=False)
    run_id = hashlib.sha256(identity.encode()).hexdigest()[:20]
    dest = Path(output_dir) / run_id
    complete = dest / 'summary.json'
    if complete.exists():
        old = json.loads(complete.read_text())
        if all((dest / name).is_file() and file_hash(dest/name) == digest
               for name, digest in old.get('artifact_hashes', {}).items()) and old.get('artifact_hashes'):
            return old, dest
    result = Backtester(signal, allocator, config).run(workload)
    dest.mkdir(parents=True, exist_ok=True)
    json_write(dest/'manifest.json', manifest)
    versions = {}
    for name in ['vmforecast-lab', 'numpy', 'pandas', 'pyarrow', 'matplotlib', 'plotly']:
        try:
            if name == 'vmforecast-lab':
                from . import __version__
                versions[name] = __version__
            else:
                versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = 'not-installed'
    json_write(dest/'environment.json', {"python": platform.python_version(), "packages": versions})
    for name in ['ticks', 'hosts', 'migrations']:
        getattr(result, name).to_parquet(dest/f'{name}.parquet', index=False)
    summary = {**result.summary, "run_id": run_id, "price": config.price}
    summary['artifact_hashes'] = {p.name: file_hash(p) for p in dest.iterdir()
                                  if p.name in {'manifest.json', 'environment.json', 'ticks.parquet',
                                                'hosts.parquet', 'migrations.parquet'}}
    json_write(complete, summary)
    return summary, dest


def run_grid(workloads, signals, prices, config, output_dir):
    """workloads: [(independent_case_id, Workload)]; signals: {label: WindowSignal}.

    Case IDs should represent independent seeds/blocks. Repeated ticks are not replicates.
    """
    workloads, prices = list(workloads), list(prices)
    if len({name for name, _ in workloads}) != len(workloads):
        raise ValueError('Case IDs must be unique')
    if not workloads or not signals or not prices:
        raise ValueError('Nonempty cases/signals/prices required')
    rows = []
    for case, workload in workloads:
        for label, signal in signals.items():
            for price in prices:
                summary, dest = run_saved(workload, signal, replace(config, price=float(price)),
                                          Path(output_dir)/'runs')
                rows.append({**{k:v for k,v in summary.items() if k != 'artifact_hashes'},
                             "case": str(case), "method": label, "run_path": str(dest)})
        # One no-migration control per case; its price is irrelevant.
        label, signal = next(iter(signals.items()))
        summary, dest = run_saved(workload, signal, replace(config, price=0.),
                                  Path(output_dir)/'runs', NoMoveAllocator())
        rows.append({**{k:v for k,v in summary.items() if k != 'artifact_hashes'},
                     "case": str(case), "method": 'no_move', "run_path": str(dest)})
    frame = pd.DataFrame(rows)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    frame.to_csv(Path(output_dir)/'grid.csv', index=False)
    return frame
