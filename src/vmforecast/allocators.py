"""Deterministic local search with explicit no-op and migration penalty."""
from dataclasses import dataclass
import numpy as np


def loads(demand, placement, n_hosts):
    out = np.zeros((n_hosts, 2), dtype=float)
    np.add.at(out, placement, demand)
    return out


def risk(forecast, placement, capacities, discount=1.0):
    """Sum of squared positive CPU/RAM overload ratios, discounted over horizon."""
    return sum(discount**k * np.square(np.maximum(
        loads(d, placement, len(capacities)) / capacities - 1, 0)).sum()
        for k, d in enumerate(forecast))


def initial_placement(demand, capacities):
    """CPU/RAM best-fit greedy using only the supplied prefix estimate; fails explicitly."""
    demand, capacities = np.asarray(demand), np.asarray(capacities)
    assigned = np.full(len(demand), -1, dtype=int)
    used = np.zeros_like(capacities, dtype=float)
    scale = capacities.max(axis=0)
    order = sorted(range(len(demand)), key=lambda s: (-max(demand[s] / scale), s))
    for s in order:
        choices = [h for h in range(len(capacities))
                   if np.all(used[h] + demand[s] <= capacities[h] + 1e-12)]
        if not choices:
            raise ValueError("Initial packing failed: enlarge fleet or provide explicit placement")
        h = min(choices, key=lambda h: (np.sum((capacities[h]-used[h]-demand[s]) / capacities[h]), h))
        assigned[s] = h
        used[h] += demand[s]
    return assigned


@dataclass(frozen=True)
class NoMoveAllocator:
    def decide(self, placement, forecast, capacities, price, costs, locked=()):
        return []


@dataclass(frozen=True)
class GreedyAllocator:
    max_moves: int = 3
    discount: float = 1.0
    tolerance: float = 1e-12

    def __post_init__(self):
        if self.max_moves < 0 or not 0 < self.discount <= 1 or self.tolerance < 0:
            raise ValueError("Invalid greedy search parameters")

    def decide(self, placement, forecast, capacities, price, costs, locked=()):
        p = np.asarray(placement, dtype=int).copy()
        locked = set(locked)
        moves = []
        for _ in range(self.max_moves):
            baseline = risk(forecast, p, capacities, self.discount)
            best = None
            improvement = self.tolerance
            for s in range(len(p)):
                if s in locked:
                    continue
                source = int(p[s])
                for target in range(len(capacities)):
                    if target == source:
                        continue
                    # A single service that does not fit cannot be rescued by moving it.
                    if np.any(forecast[:, s] > capacities[target] + self.tolerance):
                        continue
                    candidate = p.copy()
                    candidate[s] = target
                    after = risk(forecast, candidate, capacities, self.discount)
                    gain = baseline - after - price * costs[s]
                    if gain > improvement:
                        improvement, best = gain, (s, source, target, baseline, after)
            if best is None:
                break
            s, source, target, before, after = best
            moves.append({"service": s, "source": source, "target": target,
                          "predicted_risk_before": before, "predicted_risk_after": after,
                          "expected_gain": improvement})
            p[s] = target
            locked.add(s)
        return moves
