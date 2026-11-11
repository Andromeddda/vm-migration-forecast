"""Signals receive a read-only copy of past observations, never the whole workload."""
from dataclasses import dataclass
import numpy as np


@dataclass(frozen=True)
class WindowSignal:
    window: int = 12
    mode: str = "max"
    quantile: float = 0.95
    safety_margin: float = 0.0

    def __post_init__(self):
        if self.window < 1 or self.mode not in {"max", "quantile", "last"}:
            raise ValueError("window >= 1; mode: max, quantile or last")
        if not 0 <= self.quantile <= 1 or self.safety_margin < 0:
            raise ValueError("quantile in [0,1], nonnegative margin")

    def predict(self, history, horizon=1):
        """history shape [observations, services, 2]; repeats the window estimate."""
        x = np.asarray(history, dtype=float)
        if x.ndim != 3 or x.shape[-1] != 2 or len(x) == 0 or horizon < 1:
            raise ValueError("Nonempty [time, service, 2] history and horizon >= 1 required")
        if not np.isfinite(x).all() or (x < 0).any():
            raise ValueError("Missing/negative history is not silently imputed")
        recent = x[-self.window:]
        estimate = (recent.max(axis=0) if self.mode == "max" else
                    np.quantile(recent, self.quantile, axis=0) if self.mode == "quantile"
                    else recent[-1])
        return np.repeat((estimate * (1 + self.safety_margin))[None], horizon, axis=0)
