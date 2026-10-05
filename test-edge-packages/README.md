# Test Edge Packages

Fixture for the "max amount of package files" edge case. One deployable project
plus many package files that merge onto it, so you can test how many the platform
accepts on a single deployment.

Everything runs on the `datagen` connector, no external systems. The SQRL is
trivial on purpose — the subject here is the package files, not the script.

## Files

| File | Role |
| :--- | :--- |
| `base.sqrl` | trivial datagen pipeline (one source, one aggregate, one `/*+test */` table) |
| `base-package.json` | the deployable project: engines + `script.main` |
| `base-test-package.json` | the single **test** package (see below) |
| `pkg-01..pkg-12-package.json` | non-test overlays, each setting one distinct Flink config key |
| `tests-api/totals.graphql` | one API test used by the test package |

Regenerate with `python3 generate.py` after editing the `OVERLAYS` list.

## What makes a file a "test" file

The marker is the **`test-runner`** block inside the file, not the filename. Any
package file that carries `test-runner` (with `snapshot-folder`, `test-folder`,
`delay-sec`, `create-topics`, `headers`, `required-checkpoints`, ...) is a test
config. `base-test-package.json` is the only file here that has it.

## Why only one test file

Package files are merged last-wins into one configuration. `test-runner` holds a
single `snapshot-folder` and a single `test-folder`, so a second test file does
not add a second test run — it overwrites the first. Verified locally with
`datasqrl/cmd:latest`: compiling `base + test-a + test-b` succeeds but the run
uses only the last test file's `snapshot-folder` (`snapshots-b`); the first is
silently dropped. That is the practical reason the backend allows only one test
file: extra ones are not additive, they replace.

Non-test overlays are additive across distinct keys. `base + 12 overlays + 1 test`
(14 files) compiles cleanly and the merged Flink config contains the keys from
every overlay. The compiler imposes no small limit; the cloud-frontend UI caps a
project's package-file list at 10.

## How to test the max on the platform

1. Create a project from `base-package.json`.
2. Add overlays `pkg-01` .. up through the UI's limit, deploy, confirm the
   merged config on the Config tab.
3. Push past the UI limit via the deployment's package-file list to find where
   the platform (not the form) stops accepting more.
