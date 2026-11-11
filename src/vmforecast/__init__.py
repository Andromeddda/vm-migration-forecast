"""VM migration experiments; settings are usable before scientific dependencies exist."""
from importlib import import_module

__version__ = '0.1.0'
_EXPORTS = {
    'Workload': 'core', 'SimulationConfig': 'core', 'Backtester': 'core',
    'WindowSignal': 'signals', 'GreedyAllocator': 'allocators',
    'NoMoveAllocator': 'allocators', 'initial_placement': 'allocators',
    'make_synthetic': 'synthetic',
}
__all__ = list(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(name)
    value = getattr(import_module('.'+_EXPORTS[name], __name__), name)
    globals()[name] = value
    return value
