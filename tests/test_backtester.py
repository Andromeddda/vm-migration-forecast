import numpy as np
import pytest
from vmforecast import Workload, Backtester, WindowSignal, SimulationConfig
from vmforecast.allocators import GreedyAllocator, NoMoveAllocator


def overloaded(ticks=7, cpu=.7, mem=.7):
    return Workload(np.tile([[cpu,mem],[cpu,mem]],(ticks,1,1)),
                    np.ones((2,2)), np.array([0,0]))


def test_manual_no_move_and_resource_denominator():
    r = Backtester(WindowSignal(),NoMoveAllocator(),SimulationConfig(warmup=2)).run(overloaded())
    assert r.summary['migrations_completed'] == 0
    assert r.summary['violated_host_ticks'] == 5
    assert r.summary['eligible_host_ticks'] == 10
    assert r.summary['violation_rate'] == .5  # not double-counted for two resources
    assert r.summary['cpu_excess_integral'] == pytest.approx(.4*5*300)


def test_manual_move_only_affects_next_tick():
    r = Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=2,price=.1)).run(overloaded())
    assert r.summary['migrations_completed'] == 1
    assert r.summary['violated_host_ticks'] == 1
    assert r.summary['longest_overload_ticks'] == 1
    m = r.migrations.iloc[0]
    assert (m.decision_tick,m.start_tick,m.complete_tick) == (2,3,3)
    assert m.source != m.target and m.cost == .1


def test_price_can_make_noop_optimal():
    r = Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=2,price=1)).run(overloaded())
    assert r.summary['migrations_completed'] == 0
    assert r.summary['violated_host_ticks'] == 5


def test_memory_overload_not_hidden_by_low_cpu():
    r = Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=2)).run(overloaded(cpu=.1))
    assert r.summary['migrations_completed'] == 1
    assert r.summary['cpu_excess_integral'] == 0
    assert r.summary['mem_excess_integral'] > 0


def test_delay_locks_service_and_accounts_overhead():
    r = Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(
        warmup=2,migration_duration_ticks=2,overhead_cpu=.05)).run(overloaded(ticks=9))
    assert len(r.migrations) == 1 and r.migrations.iloc[0].complete_tick == 5
    assert r.summary['violated_host_ticks'] == 3
    assert r.hosts.query('tick == 3 and host == 0').cpu.iloc[0] == pytest.approx(1.45)
    assert r.hosts.query('tick == 3 and host == 1').cpu.iloc[0] == pytest.approx(.05)
    assert r.summary['pending_at_end'] == 0


def test_equal_prefix_gives_equal_decision_despite_changed_future():
    a = overloaded()
    future = a.demand.copy()
    future[2:] = .01
    b = Workload(future,a.capacities,a.placement)
    config = SimulationConfig(warmup=2)
    ra = Backtester(WindowSignal(),GreedyAllocator(),config).run(a)
    rb = Backtester(WindowSignal(),GreedyAllocator(),config).run(b)
    cols = ['service','source','target','decision_tick']
    assert ra.migrations.query('decision_tick == 2')[cols].equals(rb.migrations.query('decision_tick == 2')[cols])


def test_history_is_readonly_and_horizon_includes_observation_delay():
    class Spy:
        def predict(self,history,horizon):
            assert len(history) == 2
            assert horizon == 2  # last observed tick=1; target tick=3
            with pytest.raises(ValueError):
                history[0] = 0
            return np.repeat(history[-1:,:],horizon,axis=0)
    w = overloaded(4)
    Backtester(Spy(),NoMoveAllocator(),SimulationConfig(warmup=2)).run(w)


def test_no_migration_beyond_end():
    r = Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=6)).run(overloaded())
    assert r.summary['migrations_started'] == 0


def test_missing_and_invalid_values_rejected():
    w = overloaded()
    x = w.demand.copy();x[0,0,0]=np.nan
    with pytest.raises(ValueError):Workload(x,w.capacities,w.placement)
    with pytest.raises(ValueError):Workload(w.demand,w.capacities,np.array([0.,1.]))
    with pytest.raises(ValueError):SimulationConfig(price=-1)
    with pytest.raises(ValueError):SimulationConfig(warmup=1.5)


def test_window_uses_only_last_w_values():
    h = np.array([10.,1.,3.])[:,None,None]*np.ones((1,2,2))
    assert np.all(WindowSignal(2).predict(h,3) == 3)
    assert np.all(WindowSignal(2,'quantile',.5).predict(h) == 2)
    with pytest.raises(ValueError):WindowSignal(0)


def test_repeatability_and_input_immutability():
    w=overloaded();digest=w.fingerprint()
    a=Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=2)).run(w)
    b=Backtester(WindowSignal(),GreedyAllocator(),SimulationConfig(warmup=2)).run(w)
    assert a.migrations.equals(b.migrations)
    assert a.ticks.drop(columns='decision_ms').equals(b.ticks.drop(columns='decision_ms'))
    assert w.fingerprint()==digest


def test_delayed_source_observation_is_hidden():
    w=overloaded(6)
    a=w.available_at_tick.copy()
    a[2]=5
    w=Workload(w.demand,w.capacities,w.placement,available_at_tick=a)
    class Spy:
        def predict(self,history,horizon):
            # Called at t2,t3,t4: t2 sees two bins; bin2 cannot be seen until t5.
            assert len(history)==2
            return np.repeat(history[-1:],horizon,axis=0)
    Backtester(Spy(),NoMoveAllocator(),SimulationConfig(warmup=2)).run(w)
