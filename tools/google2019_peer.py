#!/usr/bin/env python3
"""Prepare a peer seed, download data backwards, and import verified shard progress."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[1]/'src/vmforecast/google2019_peer.py'),
               run_name='__main__')
