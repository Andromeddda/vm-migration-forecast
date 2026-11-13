#!/usr/bin/env python3
"""Standalone entry point: no Google credentials, notebook, or installed project needed."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[1] / 'src/vmforecast/google2019_http.py'),
               run_name='__main__')
