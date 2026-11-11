"""Regression coverage of configuration and the new deployment boundary."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from unittest.mock import Mock
import pytest
from vmforecast.settings import Settings, read_env, config_file

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
    p.write_text('VMFORECAST_GOOGLE_CLOUD_PROJECT="test-project"\nVMFORECAST_GCLOUD_BIN="$(touch SENTINEL)"\n')
    s = cfg(tmp_path, VMFORECAST_GOOGLE_CLOUD_PROJECT='another-project')
    assert s.project == 'another-project'
    assert s.gcloud == '$(touch SENTINEL)'
    assert not (ROOT/'SENTINEL').exists()
    assert read_env(p)['VMFORECAST_GOOGLE_CLOUD_PROJECT'] == 'test-project'


@pytest.mark.parametrize('body', ['BAD_KEY=x', 'VMFORECAST_GCLOUD_BIN=x\nVMFORECAST_GCLOUD_BIN=y',
                                  'VMFORECAST_GCLOUD_BIN="unterminated'])
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
    p = tmp_path/'lab.env';p.write_text('VMFORECAST_GCLOUD_BIN=gcloud\n')
    monkeypatch.setenv('VMFORECAST_ENV_FILE', str(p))
    args = cli.parser().parse_args(['configure', '--project', 'BAD PROJECT'])
    with pytest.raises(ValueError):cli.configure(args)
    assert p.read_text() == 'VMFORECAST_GCLOUD_BIN=gcloud\n'


def test_auth_no_token_output_and_no_global_project_change(tmp_path, monkeypatch):
    settings = cfg(tmp_path, VMFORECAST_GOOGLE_CLOUD_PROJECT='test-project', VMFORECAST_SOURCE_PROJECT='public-source')
    monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
    monkeypatch.setattr(cli, 'executable', lambda value: 'gcloud')
    calls=[]
    def run(argv, **kwargs):
        calls.append(argv)
        assert 'billing' not in argv
        assert 'set-quota-project' not in argv
        return ''
    monkeypatch.setattr(cli, 'run', run)
    cli.authenticate(settings)
    assert ['gcloud', 'auth', 'application-default', 'login', '--disable-quota-project'] in calls
    assert not any('print-access-token' in call or 'config' in call for call in calls)
    assert any('bigquery.googleapis.com' in call for call in calls)


def test_no_network_or_paid_execution_defaults():
    book = json.loads((ROOT/'notebooks/01_extract_google2019.ipynb').read_text())
    sources = '\n'.join(''.join(c['source']) for c in book['cells'])
    assert 'CONNECT_GOOGLE = False' in sources
    assert 'EXECUTE_QUERIES = False' in sources
    assert all(not c.get('outputs') for c in book['cells'])


def test_gcloud_installer_hash_and_archive_validation(tmp_path, monkeypatch):
    import hashlib
    import io
    import tarfile
    import install_gcloud
    monkeypatch.setattr(install_gcloud.platform, 'system', lambda: 'Linux')
    monkeypatch.setattr(install_gcloud.platform, 'machine', lambda: 'x86_64')
    def archive(name):
        out=io.BytesIO()
        with tarfile.open(fileobj=out, mode='w:gz') as stream:
            body=b'#!/bin/sh\nexit 0\n';info=tarfile.TarInfo(name)
            info.size=len(body);info.mode=0o755
            stream.addfile(info,io.BytesIO(body))
        return out.getvalue()
    good=archive('google-cloud-sdk/bin/gcloud')
    monkeypatch.setattr(install_gcloud.urllib.request, 'urlopen', lambda *a, **k: io.BytesIO(good))
    monkeypatch.setattr(install_gcloud, 'ARCHIVES', {'x86_64': ('x86_64', 'incorrect')})
    with pytest.raises(ValueError, match='SHA-256'):install_gcloud.install(cfg(tmp_path))
    assert not install_gcloud.target(cfg(tmp_path)).exists()
    bad=archive('google-cloud-sdk/../../escape')
    monkeypatch.setattr(install_gcloud.urllib.request, 'urlopen', lambda *a, **k: io.BytesIO(bad))
    monkeypatch.setattr(install_gcloud, 'ARCHIVES', {'x86_64': ('x86_64', hashlib.sha256(bad).hexdigest())})
    with pytest.raises((ValueError, tarfile.FilterError)):install_gcloud.install(cfg(tmp_path))
    monkeypatch.setattr(install_gcloud.urllib.request, 'urlopen', lambda *a, **k: io.BytesIO(good))
    monkeypatch.setattr(install_gcloud, 'ARCHIVES', {'x86_64': ('x86_64', hashlib.sha256(good).hexdigest())})
    binary=install_gcloud.install(cfg(tmp_path))
    assert binary.is_file() and binary.stat().st_mode & 0o111


def test_install_gcloud_records_path_without_manual_copy(tmp_path, monkeypatch):
    import install_gcloud
    settings=cfg(tmp_path)
    binary=settings.state/'tools/gcloud/bin/gcloud'
    monkeypatch.setattr(cli.Settings, 'load', lambda *a, **k: settings)
    monkeypatch.setattr(install_gcloud, 'install', lambda _: binary)
    monkeypatch.setattr(cli, 'run', lambda *a, **k: '')
    assert cli.main(['install-gcloud']) == 0
    assert read_env(settings.env_file)['VMFORECAST_GCLOUD_BIN'] == str(binary)


def test_source_audit_checks_itself_and_detects_private_runtime_paths(tmp_path):
    import source_hygiene
    import shutil
    shutil.copy(ROOT/'deployment/source_hygiene.py', tmp_path/'source_hygiene.py')
    source_hygiene.audit(tmp_path)
    (tmp_path/'private.py').write_text('path='+repr('/'+'home'+'/someone/data'))
    with pytest.raises(ValueError, match='private.py'):source_hygiene.audit(tmp_path)
