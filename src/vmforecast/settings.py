"""
Configuration boundary: literal dotenv + environment -> immutable settings.

No shell evaluation. No authentication, network access or directory creation on import.
"""

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
import re

# Supported keys for .env-like config file 
KEYS = {
    'VMFORECAST_GOOGLE_CLOUD_PROJECT', 
    'VMFORECAST_SOURCE_PROJECT',
    'VMFORECAST_STATE_DIR', 
    'VMFORECAST_DATA_DIR', 
    'VMFORECAST_RUNS_DIR',
    'VMFORECAST_MIN_FREE_GIB',
    'VMFORECAST_GCLOUD_BIN',
}

# Hash as name for ~/.config/vmforecast subfolder
def checkout_id(root):
    return hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:12]

# Get path for 
def config_file(root, env=None) -> Path:
    env = os.environ if env is None else env
    base = Path(env.get('XDG_CONFIG_HOME') or Path.home()/'.config')
    return Path(env.get('VMFORECAST_ENV_FILE') or base/'vmforecast'/checkout_id(root)/'lab.env').expanduser().resolve()

# Read key-value pairs from .env-like config file
def read_env(path) -> dict:
    values = {}

    # Non-existent path
    if not Path(path).exists():
        return values

    # Read line by line
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        line = line.strip()

        # Skip comments
        if not line or line.startswith('#'):
            continue

        key, separator, value = line.partition('=')
        key, value = key.strip(), value.strip()

        if not separator or key not in KEYS or key in values:
            raise ValueError(f'Invalid/duplicate configuration key on line {number}')

        # Parse JSON
        if value.startswith('"'):
            value = json.loads(value)
        # Parse string
        elif value.startswith("'"):
            if not value.endswith("'") or len(value) < 2:
                raise ValueError(f'Unclosed quote on line {number}')
            value = value[1:-1]

        if not isinstance(value, str) or '\n' in value or '\x00' in value:
            raise ValueError(f'Invalid configuration value on line {number}')
        values[key] = value
    return values

# Return (value or default) if value is not a subdirectory of root
# Raise ValueError otherwise
def external_path(value, default, root) -> Path:
    path = Path(value).expanduser() if value else default
    
    if not path.is_absolute():
        raise ValueError('Runtime paths must be absolute (or use ~); relative paths are ambiguous')
    
    path = path.resolve()

    if path == root or root in path.parents:
        raise ValueError('Runtime/configuration paths must be outside the source directory')
    return path


@dataclass(frozen=True)
class Settings:
    root: Path
    env_file: Path
    state: Path
    data: Path
    runs: Path
    project: str
    source_project: str
    min_free_gib: int
    gcloud: str

    @classmethod
    def load(cls, root=None, env=None):
        # Resolve root project folder
        root = Path(root or Path(__file__).resolve().parents[2]).resolve()
        
        # Get dictionary of environment variables
        env = dict(os.environ if env is None else env)

        # Get .env-like config file
        path = config_file(root, env)
        if path == root or root in path.parents:
            raise ValueError('Participant configuration must be outside the source directory')
        
        values = read_env(path)
        values.update({k: env[k] for k in KEYS if k in env})

        base = Path(env.get('XDG_DATA_HOME') or Path.home()/'.local/share')/'vmforecast'/checkout_id(root)
        state_base = Path(env.get('XDG_STATE_HOME') or Path.home()/'.local/state')/'vmforecast'/checkout_id(root)

        # Parse directories and check that they don't intersect
        state = external_path(values.get('VMFORECAST_STATE_DIR'), state_base, root)
        data = external_path(values.get('VMFORECAST_DATA_DIR'), base/'data', root)
        runs = external_path(values.get('VMFORECAST_RUNS_DIR'), base/'runs', root)
        for a, b in [(state, data), (state, runs), (data, runs)]:
            if a == b or a in b.parents or b in a.parents:
                raise ValueError('State, data and runs directories must be disjoint')

        # Parse Google Cloud project ID
        project = values.get('VMFORECAST_GOOGLE_CLOUD_PROJECT', '')
        if project and not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project):
            raise ValueError('Use a Google Cloud project ID, not its display name or number')

        # Parse source project (public google cloud dataset)
        source = values.get('VMFORECAST_SOURCE_PROJECT', '')
        if source and not re.fullmatch(r'[a-zA-Z0-9:.\-]+', source):
            raise ValueError('Invalid dataset owner')

        # Parse mininmal free disk space
        minimum = int(values.get('VMFORECAST_MIN_FREE_GIB') or '3')
        if minimum < 1:
            raise ValueError('Minimum free disk space must be at least 1 GiB')
        
        return cls(root, path, state, data, runs, project, source, minimum,
                   values.get('VMFORECAST_GCLOUD_BIN') or 'gcloud')

    @property
    def python(self):
        return self.state/'venv/bin/python'

    def process_env(self):
        env = dict(os.environ)
        env.update({
            'VMFORECAST_ENV_FILE': str(self.env_file),
            'VMFORECAST_PROJECT': str(self.root),
            'VMFORECAST_DATA_DIR': str(self.data),
            'VMFORECAST_RUNS_DIR': str(self.runs),
            'UV_PROJECT_ENVIRONMENT': str(self.state/'venv'),
            'PYTHONPATH': str(self.root/'src'),
            'PYTHONPYCACHEPREFIX': str(self.state/'pycache'),
            'JUPYTER_CONFIG_DIR': str(self.state/'jupyter/config'),
            'JUPYTER_DATA_DIR': str(self.state/'jupyter/data'),
            'JUPYTER_RUNTIME_DIR': str(self.state/'jupyter/runtime'),
            'IPYTHONDIR': str(self.state/'ipython'),
            'MPLCONFIGDIR': str(self.state/'matplotlib'),
            'MPLBACKEND': 'Agg',
        })
        env.pop('VIRTUAL_ENV', None)
        env.pop('CONDA_PREFIX', None)

        # Do not inherit a public server's authentication or remote routing config.
        for key in ('JUPYTER_TOKEN', 'JUPYTER_PASSWORD', 'JUPYTER_CONFIG_PATH', 'JUPYTER_PATH'):
            env.pop(key, None)
        return env
