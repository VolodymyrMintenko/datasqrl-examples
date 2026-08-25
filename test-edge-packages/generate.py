#!/usr/bin/env python3
"""Generates the package-file merge fixture in this folder.

One project, one test package, and many non-test overlay packages. Used to test
how many package files the platform accepts on a single deployment and to show
what a merged config looks like. Re-run after editing the OVERLAYS list.
"""
import json
from pathlib import Path

HERE = Path(__file__).parent

# Each overlay sets ONE distinct Flink config key, so the compiled config visibly
# contains all of them at once — proof the platform merged every file, not just
# the last. Keys are real Flink 2.x options that compile cleanly together.
OVERLAYS = [
    ('table.exec.source.idle-timeout', '1 s'),
    ('table.exec.mini-batch.enabled', 'true'),
    ('table.exec.mini-batch.allow-latency', '2 s'),
    ('table.exec.mini-batch.size', '1000'),
    ('table.optimizer.join-reorder-enabled', 'true'),
    ('pipeline.object-reuse', 'true'),
    ('table.local-time-zone', 'UTC'),
    ('table.exec.sink.not-null-enforcer', 'DROP'),
    ('table.exec.sink.upsert-materialize', 'NONE'),
    ('table.exec.async-lookup.buffer-capacity', '100'),
    ('table.exec.async-lookup.timeout', '3 min'),
    ('table.exec.state.ttl', '0'),
]

BASE_SQRL = '''/*
  base.sqrl - fixture for the "max amount of package files" edge case.
  A trivial datagen pipeline; the point of this folder is the package files,
  not the SQRL.
*/

CREATE TABLE Events (
  id STRING NOT NULL,
  amount DOUBLE NOT NULL,
  event_time AS NOW(),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.id.kind' = 'random',
  'fields.id.length' = '4'
);

/** Totals per id. */
/*+query_by_all(id) */
Totals := SELECT id, SUM(amount) AS total, COUNT(*) AS n FROM Events GROUP BY id;

/*+test */
TotalsTest := SELECT id, total, n FROM Totals ORDER BY id;
'''

def write(name, obj):
    (HERE / name).write_text(json.dumps(obj, indent=2) + '\n')

(HERE / 'base.sqrl').write_text(BASE_SQRL)
(HERE / 'tests-api' / 'totals.graphql').write_text('query Totals { Totals { id total n } }\n')

# Base: the deployable project.
write('base-package.json', {
    'version': '1',
    'enabled-engines': ['flink', 'postgres', 'vertx'],
    'script': {'main': 'base.sqrl'},
})

# The single test package. Its marker is the `test-runner` block, NOT the name.
write('base-test-package.json', {
    'version': '1',
    'test-runner': {'snapshot-folder': 'snapshots-api', 'test-folder': 'tests-api'},
})

# Non-test overlays, each a distinct Flink config key.
for i, (key, val) in enumerate(OVERLAYS, start=1):
    write(f'pkg-{i:02d}-package.json', {
        'version': '1',
        'engines': {'flink': {'config': {key: val}}},
    })

print(f'base + 1 test + {len(OVERLAYS)} overlays = {len(OVERLAYS) + 2} package files')
