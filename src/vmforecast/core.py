"""Fixed-cohort causal replay. Current observations are revealed AFTER decisions."""
from dataclasses import dataclass, asdict
import hashlib
import numpy as np
import pandas as pd
from .allocators import loads


@dataclass
class Workload:
    demand: np.ndarray
    capacities: np.ndarray
    placement: np.ndarray
    service_ids: list[str] | None = None
    tick_seconds: float = 300.0
    migration_weights: np.ndarray | None = None
    provenance: dict | None = None
    available_at_tick: np.ndarray | None = None

    def __post_init__(self):
        self.demand = np.array(self.demand, dtype=float, copy=True)
        self.capacities = np.array(self.capacities, dtype=float, copy=True)
        raw_p = np.asarray(self.placement)
        if not np.issubdtype(raw_p.dtype, np.integer):
            raise ValueError("Placement must contain integer host indices")
        self.placement = raw_p.astype(int, copy=True)
        if self.demand.ndim != 3 or self.demand.shape[-1] != 2:
            raise ValueError("Demand shape must be [ticks, services, CPU/RAM]")
        t, n, _ = self.demand.shape
        if t < 2 or n == 0 or self.capacities.ndim != 2 or self.capacities.shape[1] != 2:
            raise ValueError("Nonempty demand and capacities required")
        if (not np.isfinite(self.demand).all() or (self.demand < 0).any()
            or not np.isfinite(self.capacities).all() or (self.capacities <= 0).any()):
            raise ValueError("Active demand must be finite/nonnegative; capacity finite/positive")
        if self.placement.shape != (n,) or (self.placement < 0).any() or (self.placement >= len(self.capacities)).any():
            raise ValueError("Exactly one valid host per service required")
        if not np.isfinite(self.tick_seconds) or self.tick_seconds <= 0:
            raise ValueError("Invalid tick duration")
        self.service_ids = list(self.service_ids) if self.service_ids is not None else [str(i) for i in range(n)]
        if len(self.service_ids) != n or len(set(self.service_ids)) != n:
            raise ValueError("Service IDs must be unique and match demand")
        self.migration_weights = (np.ones(n) if self.migration_weights is None
                                  else np.array(self.migration_weights, dtype=float, copy=True))
        if (self.migration_weights.shape != (n,) or not np.isfinite(self.migration_weights).all()
            or (self.migration_weights < 0).any()):
            raise ValueError("Nonnegative finite migration weights required")
        self.provenance = dict(self.provenance or {})
        self.available_at_tick = (np.repeat(np.arange(1,t+1)[:,None], n, axis=1)
            if self.available_at_tick is None else np.array(self.available_at_tick, copy=True))
        if (self.available_at_tick.shape != (t,n)
            or not np.issubdtype(self.available_at_tick.dtype,np.integer)
            or (self.available_at_tick < np.arange(1,t+1)[:,None]).any()):
            raise ValueError('Observation availability must be integer [T,N] and no earlier than bin end')
        for x in (self.demand, self.capacities, self.placement, self.migration_weights,self.available_at_tick):
            x.flags.writeable = False

    def fingerprint(self):
        h = hashlib.sha256()
        for x in (self.demand, self.capacities, self.placement, self.migration_weights,self.available_at_tick):
            h.update(str(x.shape).encode())
            h.update(x.tobytes())
        h.update(str(self.tick_seconds).encode())
        h.update(repr(self.service_ids).encode())
        return h.hexdigest()


@dataclass(frozen=True)
class SimulationConfig:
    warmup: int = 12
    price: float = 0.0
    horizon: int = 1
    migration_duration_ticks: int = 0
    max_concurrent: int = 3
    overhead_cpu: float = 0.0
    overhead_mem: float = 0.0

    def __post_init__(self):
        ints = (self.warmup, self.horizon, self.migration_duration_ticks, self.max_concurrent)
        if any(not isinstance(v, (int, np.integer)) for v in ints):
            raise ValueError("Tick counts must be integers")
        if self.warmup < 1 or self.horizon < 1 or self.migration_duration_ticks < 0 or self.max_concurrent < 1:
            raise ValueError("Invalid time/concurrency parameters")
        if any(not np.isfinite(v) or v < 0 for v in (self.price, self.overhead_cpu, self.overhead_mem)):
            raise ValueError("Nonnegative finite cost/overhead required")


@dataclass
class RunResult:
    summary: dict
    ticks: pd.DataFrame
    hosts: pd.DataFrame
    migrations: pd.DataFrame
    config: dict


class Backtester:
    def __init__(self, signal, allocator, config=None):
        self.signal, self.allocator = signal, allocator
        self.config = config or SimulationConfig()

    def run(self, workload):
        from time import perf_counter
        w, c = workload, self.config
        T, N, _ = w.demand.shape
        if c.warmup >= T:
            raise ValueError("warmup must leave at least one evaluated tick")
        if (w.available_at_tick[:c.warmup] > c.warmup).any():
            raise ValueError('Warm-up contains unavailable observations: extend warmup or align slice')
        p = w.placement.copy()
        pending, actions, tick_rows, host_rows = [], [], [], []
        longest = np.zeros(len(w.capacities), dtype=int)
        streak = longest.copy()
        started_cost, completed_cost, completed = 0.0, 0.0, 0
        for t in range(c.warmup, T):
            # Complete moves at the boundary, before evaluating this tick.
            newly_done = 0
            for m in pending[:]:
                if m["complete_tick"] == t:
                    if p[m["service"]] != m["source"]:
                        raise RuntimeError("Migration source changed in transit")
                    p[m["service"]] = m["target"]
                    m["status"] = "completed"
                    completed += 1
                    newly_done += 1
                    completed_cost += m["cost"]
                    pending.remove(m)
            actual = loads(w.demand[t], p, len(w.capacities))
            # Add capacity-fraction overhead on BOTH endpoints during transit only.
            overhead = np.array([c.overhead_cpu, c.overhead_mem])
            for m in pending:
                for h in (m["source"], m["target"]):
                    actual[h] += overhead * w.capacities[h]
            ratio = actual / w.capacities
            excess = np.maximum(actual - w.capacities, 0)
            violated = (ratio > 1 + 1e-12).any(axis=1)
            streak = np.where(violated, streak + 1, 0)
            longest = np.maximum(longest, streak)
            globally_short = bool((w.demand[t].sum(axis=0) > w.capacities.sum(axis=0) + 1e-12).any())
            for h in range(len(w.capacities)):
                host_rows.append({"tick": t, "host": h, "cpu": actual[h, 0], "mem": actual[h, 1],
                                  "cpu_ratio": ratio[h, 0], "mem_ratio": ratio[h, 1],
                                  "violated": bool(violated[h])})
            count_started, latency = 0, 0.0
            # Do not start moves that cannot complete inside the evaluation horizon.
            effective = t + 1 + c.migration_duration_ticks
            if effective < T and len(pending) < c.max_concurrent:
                observed = 0
                while observed < t and np.all(w.available_at_tick[observed] <= t):
                    observed += 1
                history = w.demand[:observed].copy()
                history.flags.writeable = False
                start = perf_counter()
                # Last observation is t-1. First affected tick is t+1+duration.
                offset = effective - observed + 1
                path = np.asarray(self.signal.predict(history, offset + c.horizon - 1), dtype=float)
                expected_shape = (offset + c.horizon - 1, N, 2)
                if path.shape != expected_shape or not np.isfinite(path).all() or (path < 0).any():
                    raise ValueError("Signal returned invalid forecast")
                forecast = path[offset-1:]
                planning = p.copy()
                locked = {m["service"] for m in pending}
                for m in pending:
                    planning[m["service"]] = m["target"]
                plan = self.allocator.decide(planning.copy(), forecast.copy(), w.capacities.copy(),
                                             c.price, w.migration_weights.copy(), locked)
                latency = (perf_counter() - start) * 1000
                free = c.max_concurrent - len(pending)
                seen = set(locked)
                for move in plan[:free]:
                    s, source, target = move["service"], move["source"], move["target"]
                    if (s in seen or not 0 <= s < N or source != p[s] or target == source
                        or not 0 <= target < len(w.capacities)):
                        raise ValueError("Invalid migration plan")
                    seen.add(s)
                    cost = c.price * w.migration_weights[s]
                    row = {**move, "service_id": w.service_ids[s], "start_tick": t+1,
                           "decision_tick": t, "complete_tick": effective,
                           "cost": float(cost), "status": "pending"}
                    pending.append(row)
                    actions.append(row)
                    started_cost += cost
                    count_started += 1
            tick_rows.append({"tick": t, "violated_hosts": int(violated.sum()),
                              "any_overload": bool(violated.any()),
                              "cpu_excess": excess[:, 0].sum(), "mem_excess": excess[:, 1].sum(),
                              "risk": np.square(np.maximum(ratio-1, 0)).sum(),
                              "worst_ratio": ratio.max(), "global_shortage": globally_short,
                              "migrations_started": count_started, "migrations_completed": newly_done,
                              "pending": len(pending), "decision_ms": latency})
        ticks, hosts = pd.DataFrame(tick_rows), pd.DataFrame(host_rows)
        cols = ["service", "source", "target", "service_id", "start_tick", "decision_tick",
                "complete_tick", "cost", "status", "predicted_risk_before", "predicted_risk_after", "expected_gain"]
        migrations = pd.DataFrame(actions, columns=cols)
        summary = {"migrations_started": len(actions), "migrations_completed": completed,
                   "migration_cost_started": started_cost, "migration_cost_completed": completed_cost,
                   "pending_at_end": len(pending), "violated_host_ticks": int(ticks.violated_hosts.sum()),
                   "eligible_host_ticks": len(hosts), "violation_rate": float(hosts.violated.mean()),
                   "time_any_overload": float(ticks.any_overload.mean()),
                   "cpu_excess_integral": float(ticks.cpu_excess.sum() * w.tick_seconds),
                   "mem_excess_integral": float(ticks.mem_excess.sum() * w.tick_seconds),
                   "risk_sum": float(ticks.risk.sum()), "worst_ratio": float(ticks.worst_ratio.max()),
                   "longest_overload_ticks": int(longest.max()),
                   "global_shortage_ticks": int(ticks.global_shortage.sum()),
                   "decision_ms_total": float(ticks.decision_ms.sum()),
                   "workload_hash": w.fingerprint()}
        return RunResult(summary, ticks, hosts, migrations, asdict(c))
