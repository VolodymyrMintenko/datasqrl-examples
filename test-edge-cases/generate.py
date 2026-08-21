#!/usr/bin/env python3
"""Generates the SQRL scripts in this folder.

Every script has the same shape and differs only in how many data domains it
contains and how many metric columns each domain carries. Re-run after changing
the VARIANTS table below.
"""

from pathlib import Path

HERE = Path(__file__).parent

# name -> (number of domains, metric columns per domain)
VARIANTS = {
    'edge-large': (36, 156),
    'edge-medium': (6, 60),
    'edge-baseline': (1, 12),
}

DIMS = 8
ROLLING_METRICS = 12
EXPORTED_ALERT_DOMAINS = 4


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


def enriched(domain, metrics):
    lines = [
        '  eventId,',
        '  deviceId,',
        '  region,',
        '  category,',
    ]
    lines += [f'  dim_{i:02d},' for i in range(1, DIMS + 1)]
    lines += [
        f'  {metric(i)} * 1.0 + {i % 17}.0 AS {metric(i)},'
        for i in range(1, metrics + 1)
    ]
    lines.append('  event_time')
    return (
        f'_Enriched{domain} := SELECT\n'
        + '\n'.join(lines)
        + f'\nFROM _Raw{domain};\n'
    )


def metrics_table(domain, metrics):
    aggregates = [
        f'  SUM({metric(i)}) AS sum_{metric(i)},' for i in range(1, metrics + 1)
    ]
    aggregates.append('  COUNT(*) AS record_count')
    return (
        f'/** Tumbling 30 second totals for domain {domain}, keyed by region. */\n'
        f'/*+primary_key(region, window_end) */\n'
        f'Metrics{domain} := SELECT\n'
        '  region,\n'
        '  window_start,\n'
        '  window_end,\n'
        '  window_time,\n'
        + '\n'.join(aggregates)
        + f'\nFROM TABLE(\n'
        f'  TUMBLE(TABLE _Enriched{domain}, DESCRIPTOR(event_time), INTERVAL \'30\' SECOND)\n'
        ')\n'
        'GROUP BY region, window_start, window_end, window_time;\n'
    )


def detail_table(domain, metrics):
    aggregates = []
    for i in range(1, metrics + 1):
        aggregates.append(f'  AVG({metric(i)}) AS avg_{metric(i)},')
        aggregates.append(f'  MAX({metric(i)}) AS max_{metric(i)},')
    aggregates[-1] = aggregates[-1].rstrip(',')
    return (
        f'/*+primary_key(region, window_end) */\n'
        f'_Detail{domain} := SELECT\n'
        '  region,\n'
        '  window_start,\n'
        '  window_end,\n'
        '  window_time,\n'
        + '\n'.join(aggregates)
        + f'\nFROM TABLE(\n'
        f'  TUMBLE(TABLE _Enriched{domain}, DESCRIPTOR(event_time), INTERVAL \'30\' SECOND)\n'
        ')\n'
        'GROUP BY region, window_start, window_end, window_time;\n'
    )


def summary_table(domain):
    return (
        f'/** Narrow projection of the wide detail aggregate for domain {domain}. */\n'
        f'/*+primary_key(region, window_end) */\n'
        f'Summary{domain} := SELECT\n'
        '  region,\n'
        '  window_end,\n'
        '  window_time,\n'
        '  avg_metric_001,\n'
        '  max_metric_001,\n'
        '  avg_metric_002,\n'
        '  max_metric_002\n'
        f'FROM _Detail{domain};\n'
    )


def rolling_table(domain, metrics):
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
        + f'\nFROM TABLE(\n'
        f'  HOP(TABLE _Enriched{domain}, DESCRIPTOR(event_time), '
        "INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)\n"
        ')\n'
        'GROUP BY deviceId, window_start, window_end, window_time;\n'
    )


def latest_table(domain):
    return (
        f'/** Latest rolling maxima per device for domain {domain}. */\n'
        f'/*+query_by_all(deviceId) */\n'
        f'Latest{domain} := DISTINCT _Rolling{domain} ON deviceId '
        'ORDER BY window_time DESC;\n'
    )


def alerts(domain, index):
    block = (
        f'_Alerts{domain} := SELECT region, window_end, sum_metric_001, record_count\n'
        f'FROM Metrics{domain} WHERE sum_metric_001 > 100000.0;\n'
    )
    if index <= EXPORTED_ALERT_DOMAINS:
        block += f'\nEXPORT _Alerts{domain} TO logger.alerts{domain};\n'
    return block


def domain_block(index, metrics):
    domain = f'{index:02d}'
    parts = [
        f'/* ============================================================\n'
        f'   Domain {domain}\n'
        f'   ============================================================ */\n',
        create_table(domain, metrics),
        enriched(domain, metrics),
        metrics_table(domain, metrics),
        detail_table(domain, metrics),
        summary_table(domain),
        rolling_table(domain, metrics),
        latest_table(domain),
        alerts(domain, index),
    ]
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
        f'/*\n'
        f'  {name}.sqrl - generated by generate.py, do not edit by hand.\n'
        f'\n'
        f'  {domains} data domains, {metrics} metric columns each, all fed by the\n'
        f'  datagen connector so the project runs without any external system.\n'
        f'  Used to test how the platform behaves on a large SQRL project.\n'
        f'*/\n\n'
    )


for name, (domains, metrics) in VARIANTS.items():
    body = header(name, domains, metrics)
    body += '\n'.join(domain_block(i, metrics) for i in range(1, domains + 1))
    body += '\n' + queries(domains)
    target = HERE / f'{name}.sqrl'
    target.write_text(body)
    print(f'{target.name}: {len(body.splitlines())} lines')
