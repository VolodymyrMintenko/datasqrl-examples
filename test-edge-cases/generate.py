#!/usr/bin/env python3
"""Generates the SQRL scripts in this folder.

Every script has the same shape and differs only in how many data domains it
contains and how many metric columns each domain carries. Line count comes from
the width of the tables and the bucketing expressions, not from the number of
tables, so the scripts stay small in table count while being very long.
Re-run after changing the VARIANTS table below.
"""

from pathlib import Path

HERE = Path(__file__).parent

# name -> (number of domains, metric columns per domain)
VARIANTS = {
    'edge-large': (18, 160),
    'edge-medium': (4, 40),
    'edge-baseline': (1, 12),
}

DIMS = 8
CASE_BRANCHES = 10
ROLLING_METRICS = 12


def metric(index):
    return f'metric_{index:03d}'


def create_table(domain, metrics):
    columns = [
        '  eventId STRING NOT NULL',
        '  deviceId STRING NOT NULL',
        '  region STRING NOT NULL',
        '  category STRING NOT NULL',
    ]
    columns += [f'  dim_{i:02d} STRING NOT NULL' for i in range(1, DIMS + 1)]
    columns += [f'  {metric(i)} DOUBLE NOT NULL' for i in range(1, metrics + 1)]
    columns.append('  event_time AS NOW()')
    columns.append("  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND")

    options = [
        "  'connector' = 'datagen'",
        "  'rows-per-second' = '2'",
    ]
    for name, length in (
        ('eventId', 12), ('deviceId', 6), ('region', 2), ('category', 3),
    ):
        options.append(f"  'fields.{name}.kind' = 'random'")
        options.append(f"  'fields.{name}.length' = '{length}'")
    for i in range(1, DIMS + 1):
        options.append(f"  'fields.dim_{i:02d}.kind' = 'random'")
        options.append(f"  'fields.dim_{i:02d}.length' = '4'")
    for i in range(1, metrics + 1):
        options.append(f"  'fields.{metric(i)}.kind' = 'random'")
        options.append(f"  'fields.{metric(i)}.min' = '0.0'")
        options.append(f"  'fields.{metric(i)}.max' = '1000.0'")

    return (
        f'CREATE TABLE _Raw{domain} (\n'
        + ',\n'.join(columns)
        + '\n) WITH (\n'
        + ',\n'.join(options)
        + '\n);\n'
    )


def bucketed_sum(index):
    """A weighted SUM where the weight depends on which band the value lands in."""
    name = metric(index)
    lines = [f'  SUM(CASE']
    for branch in range(CASE_BRANCHES, 0, -1):
        threshold = branch * 100.0 - 100.0
        weight = 1.0 + branch / 10.0
        lines.append(f'    WHEN {name} > {threshold:.1f} THEN {name} * {weight:.2f}')
    lines.append(f'    ELSE {name}')
    lines.append(f'  END) AS sum_{name},')
    return '\n'.join(lines)


def metrics_table(domain, metrics):
    aggregates = [bucketed_sum(i) for i in range(1, metrics + 1)]
    return (
        f'/** Weighted 30 second totals for domain {domain}, keyed by region. */\n'
        f'/*+primary_key(region, window_end) */\n'
        f'Metrics{domain} := SELECT\n'
        '  region,\n'
        '  window_start,\n'
        '  window_end,\n'
        '  window_time,\n'
        + '\n'.join(aggregates)
        + '\n  COUNT(*) AS record_count\n'
        'FROM TABLE(\n'
        f"  TUMBLE(TABLE _Raw{domain}, DESCRIPTOR(event_time), INTERVAL '30' SECOND)\n"
        ')\n'
        'GROUP BY region, window_start, window_end, window_time;\n'
    )


def rolling_tables(domain, metrics):
    """Only defined for the first domain, so the table count stays low."""
    count = min(ROLLING_METRICS, metrics)
    aggregates = [
        f'  MAX({metric(i)}) AS max_{metric(i)},' for i in range(1, count + 1)
    ]
    aggregates[-1] = aggregates[-1].rstrip(',')
    return (
        f'_Rolling{domain} := SELECT\n'
        '  deviceId,\n'
        '  window_time,\n'
        + '\n'.join(aggregates)
        + '\nFROM TABLE(\n'
        f'  HOP(TABLE _Raw{domain}, DESCRIPTOR(event_time), '
        "INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)\n"
        ')\n'
        'GROUP BY deviceId, window_start, window_end, window_time;\n'
        '\n'
        f'/** Latest rolling maxima per device for domain {domain}. */\n'
        f'/*+query_by_all(deviceId) */\n'
        f'Latest{domain} := DISTINCT _Rolling{domain} ON deviceId '
        'ORDER BY window_time DESC;\n'
        '\n'
        f'_Alerts{domain} := SELECT region, window_end, sum_metric_001, record_count\n'
        f'FROM Metrics{domain} WHERE sum_metric_001 > 100000.0;\n'
        '\n'
        f'EXPORT _Alerts{domain} TO logger.alerts{domain};\n'
    )


def domain_block(index, metrics):
    domain = f'{index:02d}'
    parts = [
        '/* ============================================================\n'
        f'   Domain {domain}\n'
        '   ============================================================ */\n',
        create_table(domain, metrics),
        metrics_table(domain, metrics),
    ]
    if index == 1:
        parts.append(rolling_tables(domain, metrics))
    return '\n'.join(parts)


def queries(domains):
    union_domains = [f'{i:02d}' for i in range(1, min(4, domains) + 1)]
    union = '\n  UNION ALL\n'.join(
        f"  SELECT '{d}' AS domainId, region, window_end, window_time,\n"
        f'         sum_metric_001, record_count\n'
        f'  FROM Metrics{d}'
        for d in union_domains
    )
    return (
        '/* ============================================================\n'
        '   Query functions and subscription\n'
        '   ============================================================ */\n\n'
        '/** Returns the 30 second totals of domain 01 for a region and time range. */\n'
        'MetricsByRegion(region STRING NOT NULL, fromTime TIMESTAMP NOT NULL, '
        'toTime TIMESTAMP NOT NULL) :=\n'
        'SELECT * FROM Metrics01\n'
        'WHERE region = :region AND :fromTime <= window_start AND :toTime > window_end\n'
        'ORDER BY window_end DESC;\n\n'
        '/** Returns the latest rolling maxima for one device of domain 01. */\n'
        'LatestByDevice(deviceId STRING NOT NULL) :=\n'
        'SELECT * FROM Latest01 WHERE deviceId = :deviceId;\n\n'
        '/** The first domains side by side, one row per domain, region and window. */\n'
        '/*+primary_key(domainId, region, window_end) */\n'
        'RegionTotals :=\n'
        + union
        + ';\n\n'
        '/** Pushes alerts of domain 01 to subscribers. */\n'
        'HighMetricAlert := SUBSCRIBE SELECT * FROM _Alerts01;\n'
    )


def header(name, domains, metrics):
    return (
        '/*\n'
        f'  {name}.sqrl - generated by generate.py, do not edit by hand.\n'
        '\n'
        f'  {domains} data domains, {metrics} metric columns each, all fed by the\n'
        '  datagen connector so the project runs without any external system.\n'
        '  Used to test how the platform behaves on a large SQRL project.\n'
        '*/\n\n'
    )


for name, (domains, metrics) in VARIANTS.items():
    body = header(name, domains, metrics)
    body += '\n'.join(domain_block(i, metrics) for i in range(1, domains + 1))
    body += '\n' + queries(domains)
    target = HERE / f'{name}.sqrl'
    target.write_text(body)
    tables = domains * 2 + 3 + 1
    print(f'{target.name}: {len(body.splitlines())} lines, {tables} tables')
