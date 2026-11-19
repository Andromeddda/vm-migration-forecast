#!/usr/bin/env bash
# Linux/systemd user timer supervisor. No eval and no resident polling shell.
set -euo pipefail
umask 077
# Snapshot before shell variables such as `job` can alter inherited exported
# variables with the same names. Internal timer calls do not need a snapshot.
case ${1:-} in
    _run|_check|_prepare) ;;
    *) mapfile -d '' -t WATCHDOG_LAUNCH_ENV < <(env -0) ;;
esac

die() { printf 'watchdog: %s\n' "$*" >&2; exit 1; }
usage() {
    cat <<'EOF'
Usage: tools/watchdog.sh [--name NAME] [--memory-max 2G] [--always] -- COMMAND [ARG...]
       tools/watchdog.sh COMMAND [ARG...]
       tools/watchdog.sh {status|attach|stop|restart} NAME
       tools/watchdog.sh list

Check every 5 minutes; restart failures, stop after exit 0 (unless --always).
Run in the foreground inside one detached screen, supervised by systemd --user.
No sudo, Python, virtualenv activation, or permanent watchdog process required.
Use -- before a command named like a management action. Shell syntax requires
an explicit shell, e.g. -- bash -lc 'first && second'.
EOF
}

# Quote a literal systemd ExecStart argument (not shell quoting).
unit_arg() {
    local x=$1
    x=${x//\\/\\\\}; x=${x//\"/\\\"}
    x=${x//%/%%}; x=${x//\$/\$\$}
    x=${x//$'\n'/\\n}; x=${x//$'\r'/\\r}; x=${x//$'\t'/\\t}
    printf '"%s"' "$x"
}
atomic() { printf '%s\n' "$2" > "$1.tmp"; mv -f -- "$1.tmp" "$1"; }
ctl() { systemctl --user "$@"; }
load_job() {
    job=$1
    [[ -f $job/argv && -f $job/cwd ]] || die "Not a watchdog job: $job"
    name=${job##*/}; unit="vmwatch-$name"
}
lock_job() { exec 9>"$job/lock"; flock -w 10 9 || die 'Job is busy; try again later'; }

run_payload() {
    load_job "$1"
    local entry key cwd rc=0
    local -a argv saved_env=()
    mapfile -d '' -t argv < "$job/argv"
    (( ${#argv[@]} )) || die 'Empty command file'
    IFS= read -r -d '' cwd < "$job/cwd" || true
    # Only the payload inherits the invoking shell's saved environment. Exclude
    # stale session/manager variables; never interpret values as shell code.
    while IFS= read -r -d '' entry; do
        key=${entry%%=*}
        [[ $key =~ ^[a-zA-Z_][a-zA-Z_0-9]*$ ]] || continue
        case $key in
            BASH*|SHELLOPTS|UID|EUID|PPID|PWD|OLDPWD|SHLVL|_|STY|WINDOW|TERM|TERMCAP|SCREENDIR|INVOCATION_ID|JOURNAL_STREAM|SYSTEMD_EXEC_PID|NOTIFY_SOCKET|LISTEN_*|WATCHDOG_*|DBUS_SESSION_BUS_ADDRESS|XDG_RUNTIME_DIR) continue ;;
        esac
        saved_env+=("$entry")
    done < "$job/environment"
    cd -- "$cwd" || rc=$?
    if (( rc == 0 )); then
        # Foreground workloads only; backgrounding/daemonizing defeats exit
        # status supervision. screen supplies the terminal and signal handling.
        env -- "${saved_env[@]}" "${argv[@]}" || rc=$?
    fi
    atomic "$job/exit-code" "$rc"
    printf '\nwatchdog: command exited with code %s\n' "$rc"
    exit "$rc"
}

check_job() {
    load_job "$1"
    # If another check/management operation holds the lock, skip this tick.
    exec 9>"$job/lock"; flock -n 9 || exit 0
    if [[ -e $job/stopped || -e $job/completed ]]; then
        ctl stop --no-block "$unit.timer" "$unit-inhibit.service"
        exit 0
    fi
    local state code=''
    # A manager/DBus failure must NOT be mistaken for a dead program.
    state=$(ctl show "$unit.service" -p ActiveState --value) || exit 1
    [[ ! -f $job/exit-code ]] || read -r code < "$job/exit-code"
    if [[ $code == 0 && ! -e $job/always ]]; then
        atomic "$job/completed" 'exit 0'
        ctl stop --no-block "$unit.timer" "$unit-inhibit.service" "$unit.service"
        printf 'watchdog: %s completed successfully; monitoring stopped\n' "$name"
        exit 0
    fi
    ctl start --no-block "$unit-inhibit.service"
    case $state in
        active|activating|deactivating|reloading) exit 0 ;;
        inactive|failed) ;;
        *) die "Unexpected ActiveState=$state; no restart attempted" ;;
    esac
    rm -f -- "$job/exit-code"
    # systemd does the forking/cgroup work. No expensive workload runs in this
    # tiny checker's cgroup. One queued attempt; the timer retries in 5 minutes.
    ctl start --no-block "$unit-inhibit.service" "$unit.service"
    printf 'watchdog: queued %s\n' "$name"
}

# Internal entry points are used by the copied, per-job runtime script.
case ${1:-} in
    _run) [[ $# == 2 ]] || exit 2; run_payload "$2" ;;
    _check) [[ $# == 2 ]] || exit 2; check_job "$2"; exit ;;
    _prepare) [[ $# == 2 ]] || exit 2; rm -f -- "$2/exit-code"; exit ;;
esac

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit; }
(( $# )) || { usage; exit 2; }
for tool in systemctl systemd-inhibit screen flock sha256sum env readlink; do
    command -v "$tool" >/dev/null || die "Missing dependency: $tool (Debian/Ubuntu: sudo apt install screen systemd util-linux coreutils)"
done
[[ ${BASH_VERSINFO[0]} -gt 4 || (${BASH_VERSINFO[0]} == 4 && ${BASH_VERSINFO[1]} -ge 4) ]] || die 'Bash >= 4.4 required'
ctl show-environment >/dev/null || die 'No systemd user manager. Run from your normal Linux user session, without sudo.'
version=$(systemctl --version); version=${version#systemd }; version=${version%% *}; version=${version%%$'\n'*}
[[ $version =~ ^[0-9]+$ && $version -ge 243 ]] || die 'systemd >= 243 required (Type=exec, OOMPolicy)'

root=${XDG_STATE_HOME:-$HOME/.local/state}/vmforecast-watchdog
units=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
[[ $root == /* && $units == /* ]] || die 'XDG_STATE_HOME and XDG_CONFIG_HOME must be absolute paths'
mkdir -p -- "$root" "$units"
action=launch
case $1 in
    status|attach|stop|restart) action=$1; shift; [[ $# == 1 ]] || die "$action requires NAME"; name=$1 ;;
    list) action=list; shift; (( $# == 0 )) || die 'list takes no arguments' ;;
esac
if [[ $action == list ]]; then
    for file in "$root"/*/argv; do
        [[ ! -f $file ]] || printf '%s\n' "${file%/argv}"
    done
    exit
fi
if [[ $action != launch ]]; then
    [[ $name =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,47}$ ]] || die 'Invalid NAME'
    load_job "$root/$name"
    case $action in
        attach) exec screen -r "$unit" ;;
        status)
            printf 'Name: %s\nState directory: %s\nScreen: %s\n' "$name" "$job" "$unit"
            for marker in stopped completed exit-code; do
                [[ ! -f $job/$marker ]] || printf '%s: %s\n' "$marker" "$(cat "$job/$marker")"
            done
            ctl show "$unit.service" -p ActiveState -p SubState -p Result -p MemoryCurrent -p MemoryMax -p ControlGroup
            ctl list-timers --all "$unit.timer" --no-pager
            exit ;;
        stop)
            lock_job
            atomic "$job/stopped" 'manual stop'
            ctl disable --now "$unit.timer"
            ctl stop "$unit-inhibit.service" "$unit.service"
            printf 'Stopped: %s\n' "$name"
            exit ;;
        restart)
            lock_job
            # First stop the old cgroup, including all screen/payload children.
            # Do not remove success markers until it can no longer write them.
            atomic "$job/stopped" 'restart in progress'
            ctl stop "$unit.service" "$unit-inhibit.service"
            rm -f -- "$job/stopped" "$job/completed" "$job/exit-code"
            ctl enable --now "$unit.timer"
            ctl start --no-block "$unit-inhibit.service" "$unit.service"
            printf 'Restart queued: %s\n' "$name"
            exit ;;
    esac
fi

name=''; memory=infinity; always=0
while (( $# )); do
    case $1 in
        --name) (( $# >= 2 )) || die '--name requires a value'; name=$2; shift 2 ;;
        --memory-max) (( $# >= 2 )) || die '--memory-max requires a value'; memory=$2; shift 2 ;;
        --always) always=1; shift ;;
        --) shift; break ;;
        -*) die "Unknown option: $1 (use -- before your command)" ;;
        *) break ;;
    esac
done
(( $# )) || die 'No command specified'
[[ $memory == infinity || $memory =~ ^[1-9][0-9]*[KMGT]?$ ]] || die '--memory-max: use e.g. 2048M or 2G'
cwd=$(pwd -P)
digest=$(printf '%s\0' "$cwd" "$@" | sha256sum); digest=${digest%% *}
[[ -n $name ]] || name="job-${digest:0:20}"
[[ $name =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,47}$ ]] || die 'NAME: 1-48 letters/digits/underscores/hyphens, starting with a letter or digit'
job=$root/$name; unit=vmwatch-$name
mkdir -p -- "$job"; chmod 700 "$root" "$job"
lock_job
if [[ -f $job/signature ]]; then
    read -r saved < "$job/signature"
    [[ $saved == "$digest:$memory:$always" ]] || die 'NAME already belongs to another command/configuration. Choose another --name.'
    printf 'Already registered: %s (use status, attach, restart or stop)\n' "$name"
    exit
fi
# Resolve executables now; generated units never depend on the manager's PATH.
bash_bin=$(command -v bash); screen_bin=$(command -v screen)
inhibit_bin=$(command -v systemd-inhibit); sleep_bin=$(command -v sleep)
[[ -f /sys/fs/cgroup/cgroup.controllers || $memory == infinity ]] || die '--memory-max requires cgroup v2 with a delegated memory controller; see docs/WATCHDOG.md'
if [[ $memory != infinity ]]; then
    control=$(ctl show -p ControlGroup --value)
    [[ -n $control && -f /sys/fs/cgroup$control/memory.max ]] || die 'User manager has no delegated memory controller; cannot promise --memory-max'
    [[ " $(cat "/sys/fs/cgroup$control/cgroup.controllers") " == *" memory "* ]] || die 'Memory controller is not delegated to the user manager'
fi
printf '%s\0' "$@" > "$job/argv"
printf '%s\0' "$cwd" > "$job/cwd"
printf '%s\0' "${WATCHDOG_LAUNCH_ENV[@]}" > "$job/environment"
cp -- "$(readlink -f -- "${BASH_SOURCE[0]}")" "$job/runtime.sh"
chmod 700 "$job/runtime.sh"
(( always == 0 )) || : > "$job/always"
cat > "$job/screenrc" <<'EOF'
startup_message off
defscrollback 1000
log off
EOF
runtime="$(unit_arg "$bash_bin") $(unit_arg "$job/runtime.sh")"
where=$(unit_arg "$job")
cat > "$units/$unit.service" <<EOF
[Unit]
Description=Watchdog payload $name
StartLimitIntervalSec=0

[Service]
Type=exec
ExecStartPre=$runtime _prepare $where
ExecStart=$(unit_arg "$inhibit_bin") --what=sleep:idle:handle-lid-switch --mode=block --no-ask-password --who=vmwatch-$name --why=Watchdog-payload $(unit_arg "$screen_bin") -D -m -S $unit -c $(unit_arg "$job/screenrc") $runtime _run $where
KillMode=control-group
TimeoutStopSec=30s
Restart=no
OOMPolicy=kill
OOMScoreAdjust=500
MemoryAccounting=yes
MemoryMax=$memory
UMask=0077
EOF
cat > "$units/$unit-inhibit.service" <<EOF
[Unit]
Description=Keep system awake while watching $name
StartLimitIntervalSec=0

[Service]
Type=exec
ExecStart=$(unit_arg "$inhibit_bin") --what=sleep:idle:handle-lid-switch --mode=block --no-ask-password --who=vmwatch-$name --why=Watchdog-waiting-or-running $(unit_arg "$sleep_bin") infinity
KillMode=control-group
Restart=no
OOMPolicy=kill
EOF
cat > "$units/$unit-check.service" <<EOF
[Unit]
Description=Check watchdog $name
StartLimitIntervalSec=0

[Service]
Type=oneshot
ExecStart=$runtime _check $where
TimeoutStartSec=20s
MemoryMax=64M
OOMPolicy=kill
EOF
cat > "$units/$unit.timer" <<EOF
[Unit]
Description=Check watchdog $name every five minutes

[Timer]
OnActiveSec=300s
OnUnitInactiveSec=300s
AccuracySec=1s
Unit=$unit-check.service

[Install]
WantedBy=timers.target
EOF
ctl daemon-reload
# Commit before enabling the timer: even an interruption here must not let a
# retry overwrite a runtime which a timer may already have started executing.
atomic "$job/signature" "$digest:$memory:$always"
ctl enable --now "$unit.timer"
ctl start --no-block "$unit-inhibit.service" "$unit.service"
printf 'Registered: %s\nScreen: %s\nState: %s\n' "$name" "$unit" "$job"
printf 'Attach: %q attach %q\nStatus: %q status %q\nStop: %q stop %q\n' "$0" "$name" "$0" "$name" "$0" "$name"
if command -v loginctl >/dev/null && [[ $(loginctl show-user "$(id -u)" -p Linger --value 2>/dev/null || true) != yes ]]; then
    printf 'Note: linger is off/unknown. To survive logout: loginctl enable-linger "%s"\n' "$(id -un)" >&2
fi
printf 'Startup is asynchronous. Check status and journalctl --user -u %s.service for errors.\n' "$unit"
