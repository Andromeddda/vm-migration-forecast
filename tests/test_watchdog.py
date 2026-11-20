"""Real Bash/runtime tests with a simulated systemd manager (no user bus in CI)."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'tools/watchdog.sh'


@pytest.fixture
def host(tmp_path):
    # Exercise spaces, percent specifiers, dollars and quotes in unit arguments.
    home = tmp_path / 'home % " $cash'
    home.mkdir()
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    store = tmp_path / 'manager.json'
    store.write_text(json.dumps({'calls': [], 'states': {}}))
    mock = binaries / 'systemctl'
    mock.write_text(f'#!{sys.executable}\n' + '''
import fcntl, json, os, sys
from pathlib import Path
p = Path(os.environ['MOCK_STORE'])
lock = open(str(p) + '.lock', 'w'); fcntl.flock(lock, fcntl.LOCK_EX)
s = json.loads(p.read_text()); a = sys.argv[1:]
s['calls'].append(a.copy())
p.write_text(json.dumps(s))
if os.environ.get('MOCK_BUS_FAIL'):
    sys.exit(1)
if a == ['--version']:
    print('systemd 252'); sys.exit(0)
assert a.pop(0) == '--user'
cmd = a.pop(0)
if cmd == 'show':
    if a[0].endswith('.service'):
        print(s['states'].get(a[0], 'inactive'))
    else:
        print('/user.slice/fake')
elif cmd in ('start', 'stop', 'enable', 'disable'):
    for unit in a:
        if unit.endswith(('.service', '.timer')):
            s['states'][unit] = 'inactive' if cmd in ('stop', 'disable') else 'active'
    p.write_text(json.dumps(s))
elif cmd in ('show-environment', 'daemon-reload', 'list-timers'):
    pass
else:
    raise RuntimeError((cmd, a))
''')
    mock.chmod(0o755)
    for name in ('screen', 'systemd-inhibit'):
        tool = binaries / name
        tool.write_text('#!/bin/bash\nexit 0\n')
        tool.chmod(0o755)
    login = binaries / 'loginctl'
    login.write_text('#!/bin/bash\necho yes\n')
    login.chmod(0o755)
    env = os.environ | {
        'HOME': str(home), 'XDG_STATE_HOME': str(home / 'state'),
        'XDG_CONFIG_HOME': str(home / 'config'), 'MOCK_STORE': str(store),
        'PATH': str(binaries) + ':' + os.environ['PATH'],
        'TMPDIR': str(tmp_path),
    }
    runtime = home / 'runtime'
    runtime.mkdir(mode=0o700)
    env['XDG_RUNTIME_DIR'] = str(runtime)

    class Host:
        def launch(self, *args, check=True):
            return subprocess.run(['bash', str(SCRIPT), *map(str, args)], env=env,
                                  cwd=tmp_path, capture_output=True, text=True, check=check)

        def data(self):
            return json.loads(store.read_text())

        def state(self, name, value):
            data = self.data()
            data['states'][f'vmwatch-{name}.service'] = value
            store.write_text(json.dumps(data))

        def job(self, name='test'):
            return Path(env['XDG_STATE_HOME']) / 'vmforecast-watchdog' / name

        def internal(self, action, name='test', check=True):
            return subprocess.run(['bash', str(self.job(name) / 'runtime.sh'),
                                   action, str(self.job(name))], env=env,
                                  capture_output=True, text=True, check=check)

        def queue_count(self):
            return sum(c[:3] == ['--user', 'start', '--no-block']
                       and 'vmwatch-test.service' in c for c in self.data()['calls'])

    h = Host()
    h.env = env
    h.directory = tmp_path
    return h


def install(host, *command, extra=()):
    return host.launch('--name', 'test', *extra, '--', *(command or ('true',)))


def test_same_launch_is_idempotent_and_private(host):
    install(host)
    first = host.data()
    install(host)
    assert host.data()['states'] == first['states']
    assert host.queue_count() == 1
    assert host.job().stat().st_mode & 0o777 == 0o700
    assert (host.job() / 'environment').stat().st_mode & 0o777 == 0o600
    changed = host.launch('--name', 'test', 'false', check=False)
    assert changed.returncode != 0
    assert host.queue_count() == 1


def test_auto_names_depend_on_exact_argv_and_cwd(host):
    host.launch('printf', '%s', 'a b')
    host.launch('printf', '%s', 'a b')
    host.launch('printf', '%s', 'a', 'b')
    jobs = list((Path(host.env['XDG_STATE_HOME']) / 'vmforecast-watchdog').glob('*/argv'))
    assert len(jobs) == 2


def test_preserves_argv_environment_cwd_without_eval(host):
    values = ['a b', '', 'line\nbreak', '$(touch OWNED)', '; touch OWNED', "'\"%$\\"]
    host.env['CUSTOM_VALUE'] = 'hello $world\nnext'
    # Environment keys must never overwrite the wrapper's internal variables.
    host.env['job'] = '/wrong'
    output = host.directory / 'arguments.json'
    program = ('import json,os,sys;json.dump([sys.argv[2:],os.getcwd(),'
               'os.environ["CUSTOM_VALUE"],os.environ["job"]],open(sys.argv[1],"w"))')
    install(host, sys.executable, '-c', program, output, *values)
    host.internal('_run')
    data = json.loads(output.read_text())
    assert data == [values, str(host.directory), host.env['CUSTOM_VALUE'], '/wrong']
    assert not (host.directory / 'OWNED').exists()
    assert (host.job() / 'exit-code').read_text().strip() == '0'


@pytest.mark.parametrize('code', [1, 75, 137])
def test_failure_restarts_once_on_next_tick(host, code):
    install(host, sys.executable, '-c', f'raise SystemExit({code})')
    result = host.internal('_run', check=False)
    assert result.returncode == code
    host.state('test', 'failed')
    host.internal('_check')
    assert host.queue_count() == 2
    host.internal('_check')  # manager already accepted start; no duplicate
    assert host.queue_count() == 2
    assert not (host.job() / 'exit-code').exists()


def test_unexpected_sigkill_with_no_marker_restarts(host):
    install(host)
    host.state('test', 'inactive')
    host.internal('_check')
    assert host.queue_count() == 2


def test_success_stops_monitor_and_inhibitor(host):
    install(host)
    host.internal('_run')
    host.internal('_check')
    assert (host.job() / 'completed').exists()
    assert host.queue_count() == 1
    assert host.data()['states']['vmwatch-test.timer'] == 'inactive'
    assert host.data()['states']['vmwatch-test-inhibit.service'] == 'inactive'
    host.internal('_check')
    assert host.queue_count() == 1


def test_always_restarts_success(host):
    install(host, extra=('--always',))
    host.internal('_run')
    host.state('test', 'inactive')
    host.internal('_check')
    assert host.queue_count() == 2
    assert not (host.job() / 'completed').exists()


def test_manual_stop_persists_and_explicit_restart_clears_markers(host):
    install(host)
    host.launch('stop', 'test')
    host.internal('_check')
    assert host.queue_count() == 1
    host.launch('restart', 'test')
    assert not (host.job() / 'stopped').exists()
    assert host.queue_count() == 2
    # stop old cgroup precedes starting it again
    calls = host.data()['calls']
    assert calls.index(['--user', 'stop', 'vmwatch-test.service', 'vmwatch-test-inhibit.service']) < len(calls) - 1


@pytest.mark.parametrize('state', ['active', 'activating', 'deactivating', 'reloading'])
def test_live_or_transitional_unit_not_duplicated(host, state):
    install(host)
    host.state('test', state)
    host.internal('_check')
    assert host.queue_count() == 1


def test_manager_failure_is_not_a_dead_workload(host):
    install(host)
    host.env['MOCK_BUS_FAIL'] = '1'
    assert host.internal('_check', check=False).returncode != 0
    assert host.queue_count() == 1


def test_missing_working_directory_is_a_recorded_failure(host):
    install(host)
    (host.job() / 'cwd').write_bytes(b'/nonexistent/watchdog-directory\0')
    assert host.internal('_run', check=False).returncode != 0
    assert (host.job() / 'exit-code').read_text().strip() != '0'


def test_concurrent_launches_only_register_once(host):
    processes = [subprocess.Popen(['bash', str(SCRIPT), '--name', 'test', 'true'],
                                 cwd=host.directory, env=host.env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(5)]
    for process in processes:
        out, err = process.communicate(timeout=15)
        assert process.returncode == 0, (out, err)
    assert host.queue_count() == 1


def test_registered_units_validate_with_real_systemd_parser(host):
    install(host)
    units = Path(host.env['XDG_CONFIG_HOME']) / 'systemd/user'
    files = list(units.glob('vmwatch-test.*')) + list(units.glob('vmwatch-test-*.service'))
    assert len(files) == 4
    # Verify syntax/escaping with the actual systemd parser; no running bus needed.
    if not shutil.which('systemd-analyze'):
        pytest.skip('systemd-analyze is not installed')
    result = subprocess.run(['systemd-analyze', '--user', 'verify', *map(str, files)],
                            env=host.env, capture_output=True, text=True)
    if 'Failed to setup working directory' in result.stderr and not Path('/tmp').exists():
        pytest.skip('Sandbox has no /tmp; systemd-analyze hardcodes its temporary directory')
    assert result.returncode == 0, result.stderr
    assert 'Unknown' not in result.stderr
    service = (units / 'vmwatch-test.service').read_text()
    assert 'KillMode=control-group' in service
    assert 'OOMPolicy=kill' in service
    assert 'OOMScoreAdjust=500' in service
    assert '-D -m -S vmwatch-test' in service
    assert '--what=sleep:idle:handle-lid-switch' in service
    assert 'Restart=no' in service
    timer = (units / 'vmwatch-test.timer').read_text()
    assert 'OnUnitInactiveSec=300s' in timer


def test_name_and_resource_value_injection_rejected(host):
    assert host.launch('--name', '../bad', 'true', check=False).returncode != 0
    assert host.launch('--memory-max', '2G\nExecStart=bad', 'true', check=False).returncode != 0


def test_command_named_status_requires_separator(host):
    result = host.launch('--', 'status', check=False)
    assert result.returncode == 0


def test_runtime_is_independent_of_repository_copy(host):
    install(host)
    assert (host.job() / 'runtime.sh').read_bytes() == SCRIPT.read_bytes()
    host.internal('_prepare')
    host.internal('_run')


def test_real_sigkill_has_no_false_success(host):
    install(host, 'bash', '-c', 'kill -KILL $$')
    assert host.internal('_run', check=False).returncode == 137
    assert (host.job() / 'exit-code').read_text().strip() == '137'
    host.state('test', 'failed')
    host.internal('_check')
    assert host.queue_count() == 2


def test_busy_checker_skips_tick(host):
    import fcntl
    install(host)
    host.state('test', 'failed')
    with (host.job() / 'lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        host.internal('_check')
    assert host.queue_count() == 1
    host.internal('_check')
    assert host.queue_count() == 2


def test_missing_memory_delegation_fails_before_start(host):
    result = host.launch('--name', 'test', '--memory-max', '2G', 'true', check=False)
    assert result.returncode != 0
    assert host.queue_count() == 0
    assert not (host.job() / 'signature').exists()


def test_completed_job_after_manager_restart_does_not_restart_payload(host):
    install(host)
    host.internal('_run')
    host.internal('_check')
    host.state('test', 'inactive')
    host.internal('_check')
    assert host.queue_count() == 1


def test_generated_units_separate_payload_check_and_guard(host):
    install(host)
    units = Path(host.env['XDG_CONFIG_HOME']) / 'systemd/user'
    service = (units / 'vmwatch-test.service').read_text()
    assert 'KillMode=control-group' in service
    assert 'OOMPolicy=kill' in service
    assert 'OOMScoreAdjust=500' in service
    assert '-D -m -S vmwatch-test' in service
    assert 'Restart=no' in service
    checker = (units / 'vmwatch-test-check.service').read_text()
    assert 'MemoryMax=64M' in checker
    assert '_check' in checker and 'screen' not in checker
    timer = (units / 'vmwatch-test.timer').read_text()
    assert 'OnUnitInactiveSec=300s' in timer
    guard = (units / 'vmwatch-test-inhibit.service').read_text()
    assert 'sleep' in guard and 'infinity' in guard
