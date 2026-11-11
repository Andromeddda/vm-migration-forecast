"""Controlled repeatable scenarios, independent of policy and placement."""
import numpy as np
from .core import Workload
from .allocators import initial_placement


def make_synthetic(seed=0, ticks=288, services=12, hosts=4, capacity=1.0,
                   scenario="seasonal", warmup=12, noise=0.015):
    if ticks <= warmup or warmup < 1 or min(services, hosts) < 1 or capacity <= 0 or noise < 0:
        raise ValueError("Invalid synthetic parameters")
    if scenario not in {"seasonal", "trend", "jump", "returning_burst"}:
        raise ValueError("Unknown scenario")
    rng = np.random.default_rng(seed)
    t = np.arange(ticks)[:, None, None]
    base = rng.uniform(.10, .17, (1, services, 2))
    phase = rng.uniform(0, 2*np.pi, (1, services, 2))
    period = max(24, ticks//3)
    demand = base + .06 * (1+np.sin(2*np.pi*t/period+phase))
    if scenario == "trend":
        demand += .16*t/ticks
    elif scenario == "jump":
        demand[ticks//2:, :max(2, services//3)] += .30
    elif scenario == "returning_burst":
        subset = max(2, services//3)
        demand[ticks//3:ticks//2, :subset] += .24
        demand[2*ticks//3:2*ticks//3+max(3,ticks//12), :subset] += .35
    demand = np.maximum(demand + rng.normal(0, noise, (ticks, services, 2)), 0)
    cap = np.full((hosts, 2), capacity)
    placement = initial_placement(demand[:warmup].max(axis=0), cap)
    return Workload(demand, cap, placement, provenance={"kind": "synthetic", "seed": seed,
                    "scenario": scenario, "noise": noise, "period_ticks": period})
