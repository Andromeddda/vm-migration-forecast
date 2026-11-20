"""Regression coverage of configuration and the new deployment boundary."""
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
from unittest.mock import Mock
import pytest
from vmforecast.settings import Settings, read_env, RETIRED_KEYS

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'deployment'))
spec = importlib.util.spec_from_file_location('deployment_cli', ROOT/'deployment/cli.py')
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


def cfg(tmp_path, **extra):
    return Settings.load(ROOT, {'VMFORECAST_ENV_FILE': str(tmp_path/'lab.env'),
                               'XDG_DATA_HOME': str(tmp_path/'data-home'),
                               'XDG_STATE_HOME': str(tmp_path/'state-home'), **extra})


def test_settings_import_without_scientific_dependencies():
    # -S ignores site packages: configure must work before any project installation.
    result = subprocess.run([sys.executable, '-B', '-S', '-c',
        'import sys;sys.path.insert(0,sys.argv[1]);from vmforecast.settings import Settings;'
        'assert "numpy" not in sys.modules', str(ROOT/'src')], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_config_literal_and_environment_precedence(tmp_path):
    p = tmp_path/'lab.env'
    literal = str(tmp_path/'$(touch SENTINEL)')
    p.write_text('VMFORECAST_DATA_DIR='+json.dumps(literal)+'\nVMFORECAST_MIN_FREE_GIB=4\n')
    s = cfg(tmp_path, VMFORECAST_MIN_FREE_GIB='5')
    assert s.min_free_gib == 5
    assert s.data == Path(literal)
    assert not (ROOT/'SENTINEL').exists()
    assert read_env(p)['VMFORECAST_MIN_FREE_GIB'] == '4'


@pytest.mark.parametrize('body', ['BAD_KEY=x', 'VMFORECAST_DATA_DIR=x\nVMFORECAST_DATA_DIR=y',
                                  'VMFORECAST_DATA_DIR="unterminated'])
def test_bad_config_rejected(tmp_path, body):
    (tmp_path/'lab.env').write_text(body)
    with pytest.raises(ValueError):cfg(tmp_path)


def test_runtime_and_config_must_be_outside_source_and_disjoint(tmp_path):
    for env in ({'VMFORECAST_DATA_DIR': str(ROOT/'data')},
                {'VMFORECAST_DATA_DIR': 'relative'},
                {'VMFORECAST_DATA_DIR': str(tmp_path/'same'), 'VMFORECAST_RUNS_DIR': str(tmp_path/'same/child')},
                {'VMFORECAST_ENV_FILE': str(ROOT/'.env')}):
        with pytest.raises(ValueError):cfg(tmp_path, **env)
    link = tmp_path/'linked-source';link.symlink_to(ROOT, target_is_directory=True)
    with pytest.raises(ValueError):cfg(tmp_path, VMFORECAST_STATE_DIR=str(link/'state'))


def test_private_write_is_atomic_and_restricts_permissions(tmp_path):
    path = tmp_path/'config/env'
    cli.private_write(path, 'first')
    cli.private_write(path, 'second')
    assert path.read_text() == 'second'
    assert path.stat().st_mode & 0o777 == 0o600


def test_deployment_does_not_install_project_or_touch_system_python(tmp_path, monkeypatch):
    settings = cfg(tmp_path)
    calls = []
    monkeypatch.setattr(cli, 'run', lambda argv, **kwargs: calls.append((list(map(str, argv)), kwargs)))
    monkeypatch.setattr(cli, 'executable', lambda value: 'uv')
    cli.provision(settings)
    sync = calls[0][0]
    assert '--locked' in sync and '--no-install-project' in sync and '--no-build' in sync
    assert '--all-extras' in sync
    assert calls[0][1]['env']['UV_PROJECT_ENVIRONMENT'] == str(settings.state/'venv')
    spec = json.loads((settings.state/'jupyter/data/kernels/python3/kernel.json').read_text())
    assert spec['argv'][0] == str(settings.python)


def test_disk_failure_before_download(tmp_path, monkeypatch):
    monkeypatch.setattr(cli.shutil, 'disk_usage', lambda p: type('Disk', (), {'free': 1})())
    run = Mock();monkeypatch.setattr(cli, 'run', run)
    with pytest.raises(cli.LabError, match='disk'):cli.provision(cfg(tmp_path))
    run.assert_not_called()


def test_server_port_busy_preflight(monkeypatch):
    # Real kernel-assigned port; intercept only the chosen port to avoid conflicts in CI.
    real_socket = socket.socket
    with real_socket() as listener:
        listener.bind(('127.0.0.1', 0));listener.listen()
        busy_port = listener.getsockname()[1]
        class Socket:
            def __enter__(self):self.sock=real_socket();return self
            def __exit__(self, *args):self.sock.close()
            def bind(self, address):self.sock.bind(('127.0.0.1', busy_port))
        monkeypatch.setattr(cli.socket, 'socket', Socket)
        with pytest.raises(cli.LabError, match='8888'):cli.port_check()


def test_failed_configure_preserves_previous_file(tmp_path, monkeypatch):
    p = tmp_path/'lab.env';p.write_text('VMFORECAST_MIN_FREE_GIB=3\n')
    monkeypatch.setenv('VMFORECAST_ENV_FILE', str(p))
    args = cli.parser().parse_args(['configure', '--data-dir', 'relative'])
    with pytest.raises(ValueError):cli.configure(args)
    assert p.read_text() == 'VMFORECAST_MIN_FREE_GIB=3\n'


def test_source_audit_checks_itself_and_detects_private_runtime_paths(tmp_path):
    import source_hygiene
    import shutil
    shutil.copy(ROOT/'deployment/source_hygiene.py', tmp_path/'source_hygiene.py')
    source_hygiene.audit(tmp_path)
    (tmp_path/'private.py').write_text('path='+repr('/'+'home'+'/someone/data'))
    with pytest.raises(ValueError, match='private.py'):source_hygiene.audit(tmp_path)


def test_legacy_config_preserves_paths_and_configure_removes_retired_keys(tmp_path, monkeypatch):
    paths = {f'VMFORECAST_{name}_DIR': str(tmp_path/name.lower())
             for name in ('STATE', 'DATA', 'RUNS')}
    retired = dict(zip(sorted(RETIRED_KEYS), ['unused-cli', 'unused-project', 'unused-source']))
    original = paths | retired
    p = tmp_path/'lab.env'
    p.write_text(''.join(k+'='+json.dumps(v)+'\n' for k, v in original.items()))
    monkeypatch.setenv('VMFORECAST_ENV_FILE', str(p))
    for key in paths:
        monkeypatch.delenv(key, raising=False)
    for key, value in retired.items():
        monkeypatch.setenv(key, value)
    # Even an interactive configure must not prompt for any account/project.
    monkeypatch.setattr('builtins.input', lambda *a: pytest.fail('configure must not prompt'))
    settings = Settings.load(ROOT)
    assert (settings.state, settings.data, settings.runs) == tuple(Path(v) for v in paths.values())
    assert read_env(p) == paths
    assert not any(key in settings.process_env() for key in RETIRED_KEYS)
    cli.configure(cli.parser().parse_args(['configure']))
    assert read_env(p) == paths
    assert not any(key in p.read_text() for key in RETIRED_KEYS)
    assert p.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('argv', [
    ['auth'], ['install-gcloud'], ['configure', '--project', 'old-project'],
    ['configure', '--source-project', 'old-source'], ['configure', '--gcloud', 'old-cli'],
])
def test_removed_cloud_entry_points_are_rejected(argv):
    with pytest.raises(SystemExit) as error:
        cli.parser().parse_args(argv)
    assert error.value.code == 2


def test_configure_works_without_existing_config(tmp_path, monkeypatch):
    path = tmp_path/'new-config/lab.env'
    monkeypatch.setenv('VMFORECAST_ENV_FILE', str(path))
    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path/'data-home'))
    monkeypatch.setenv('XDG_STATE_HOME', str(tmp_path/'state-home'))
    for key in ('VMFORECAST_STATE_DIR', 'VMFORECAST_DATA_DIR', 'VMFORECAST_RUNS_DIR'):
        monkeypatch.delenv(key, raising=False)
    cli.configure(cli.parser().parse_args(['configure']))
    assert path.is_file()
    assert read_env(path) == {}
    settings = Settings.load(ROOT)
    assert all(p.is_dir() for p in (settings.state, settings.data, settings.runs))


def test_local_notebooks_default_to_synthetic_and_have_no_saved_output():
    paths = sorted((ROOT/'notebooks').glob('*.ipynb'))
    assert [p.name for p in paths] == ['02_single_experiment.ipynb', '03_benchmark_and_reports.ipynb']
    for path in paths:
        book = json.loads(path.read_text())
        assert all(not c.get('outputs') and c.get('execution_count') is None
                   for c in book['cells'] if c['cell_type'] == 'code')
    sources = '\n'.join(''.join(c['source']) for c in json.loads(paths[0].read_text())['cells'])
    assert 'USE_PREPARED = False' in sources


def test_notebook_verification_uses_a_synthetic_copy():
    import nbformat
    import verify_notebooks
    book = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(
        'USE_PREPARED = True\nresult = "prepared" if USE_PREPARED else "synthetic"')])
    verified = verify_notebooks.synthetic_copy(book)
    namespace = {}
    exec(verified.cells[0].source, namespace)
    assert namespace['result'] == 'synthetic'
    assert 'USE_PREPARED = True' in book.cells[0].source


def test_source_audit_ignores_git_metadata_but_rejects_generated_files(tmp_path):
    import source_hygiene
    git = tmp_path/'.git'
    git.mkdir()
    (git/'index').write_bytes(b'\x00not source')
    venv = tmp_path/'.venv-http'
    venv.mkdir()
    (venv/'python').write_bytes(b'\x00not source')
    (tmp_path/'requirements-http.txt').write_text('duckdb==1.4.2\n')
    source_hygiene.audit(tmp_path)
    cache = tmp_path/'__pycache__'
    cache.mkdir()
    (cache/'module.pyc').write_bytes(b'\x00compiled')
    with pytest.raises(ValueError, match='generated directory'):
        source_hygiene.audit(tmp_path)
