"""Exportable figures and self-contained HTML; no hosted tracking service."""
from pathlib import Path
from html import escape
import numpy as np
import pandas as pd


def paired_difference(frame, a, b, metric='migrations_completed', seed=0, samples=2000):
    """Bootstrap independent paired cases, not ticks. Delta = a - b."""
    out = []
    rng = np.random.default_rng(seed)
    for price, group in frame[frame.method.isin([a,b])].groupby('price'):
        if group.duplicated(['case','method']).any():
            raise ValueError('Multiple observations per independent case/method')
        pivot = group.pivot(index='case', columns='method', values=metric)
        if a not in pivot or b not in pivot:
            continue
        pivot = pivot[[a,b]].dropna()
        d = (pivot[a]-pivot[b]).to_numpy()
        n = len(d)
        if n == 0:
            continue
        ci = (np.quantile(rng.choice(d, (samples,n), replace=True).mean(axis=1), [.025,.975])
              if n >= 2 else [np.nan,np.nan])
        out.append({'price': price, 'metric': metric, 'a': a, 'b': b, 'n_pairs': n,
                    'mean_difference': d.mean(), 'ci_low': ci[0], 'ci_high': ci[1]})
    return pd.DataFrame(out)


def single_report(run_dir):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import plotly.express as px
    run_dir = Path(run_dir)
    hosts = pd.read_parquet(run_dir/'hosts.parquet')
    ticks = pd.read_parquet(run_dir/'ticks.parquet')
    moves = pd.read_parquet(run_dir/'migrations.parquet')
    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    for host, group in hosts.groupby('host'):
        axes[0].plot(group.tick, group.cpu_ratio, label=f'host {host}')
        axes[1].plot(group.tick, group.mem_ratio)
    for ax, label in zip(axes[:2], ['CPU / capacity','RAM / capacity']):
        ax.axhline(1, color='red', linestyle='--')
        ax.set_ylabel(label)
    axes[0].legend(ncol=4, fontsize=8)
    axes[2].step(ticks.tick, ticks.migrations_completed, label='completed moves')
    axes[2].step(ticks.tick, ticks.violated_hosts, label='overloaded hosts')
    axes[2].set_xlabel('Evaluation tick')
    axes[2].legend()
    fig.tight_layout()
    fig.savefig(run_dir/'timeseries.png', dpi=150)
    plt.close(fig)
    graph = px.line(hosts, x='tick', y='cpu_ratio', color=hosts.host.astype(str),
                    title='CPU load ratios (RAM and moves in exported tables)')
    graph.add_hline(y=1, line_dash='dash')
    body = graph.to_html(full_html=False, include_plotlyjs=True)
    html = '<!doctype html><meta charset="utf-8"><title>Single replay</title>'
    html += '<h1>Replay diagnostics</h1>'+body+'<h2>Migrations</h2>'+moves.to_html(index=False)
    (run_dir/'single_report.html').write_text(html, encoding='utf-8')
    return run_dir/'single_report.html'


def grid_report(frame, output_dir):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import plotly.express as px
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics = ['migrations_completed','violation_rate','risk_sum','migration_cost_completed']
    aggregate = frame.groupby(['method','price'])[metrics].agg(['mean','std','count'])
    aggregate.to_csv(output_dir/'aggregate.csv')
    means = frame.groupby(['method','price'], as_index=False)[metrics].mean()
    figures = []
    for metric in ['migrations_completed','violation_rate']:
        fig, ax = plt.subplots(figsize=(7,4))
        for name, g in means.groupby('method'):
            ax.plot(g.price, g[metric], marker='o', label=name)
        ax.set_xscale('symlog', linthresh=.01)
        ax.set_xlabel('Migration price (dimensionless)')
        ax.set_ylabel(metric)
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_dir/f'{metric}.png', dpi=150)
        plt.close(fig)
        figures.append(px.line(means, x='price', y=metric, color='method', markers=True,
                               title=metric))
    figures.append(px.scatter(means, x='migrations_completed', y='violation_rate',
                              color='method', hover_data=['price'], title='Count / risk trade-off'))
    fragments = [f.to_html(full_html=False, include_plotlyjs=(True if i == 0 else False))
                 for i,f in enumerate(figures)]
    note = ('This is a pilot comparison of window policies. Means across cases are descriptive; '
            'no forecast superiority or equal-risk claim follows from these plots. '
            'Violation rate = overloaded host-ticks / fixed-fleet evaluated host-ticks.')
    html = '<!doctype html><meta charset="utf-8"><title>Migration benchmark</title>'
    html += '<h1>Migration benchmark</h1><p>'+escape(note)+'</p>'+''.join(fragments)
    html += '<h2>Aggregates</h2>'+aggregate.to_html()
    (output_dir/'report.html').write_text(html, encoding='utf-8')
    (output_dir/'report.md').write_text('# Pilot benchmark\n\n'+note+'\n\n'
        'See grid.csv, aggregate.csv, report.html and per-run manifests/logs.\n', encoding='utf-8')
    return output_dir/'report.html'
