#!/bin/bash
set -e
/opt/agent/cmd.sh test -r . orders-shared-package.json orders-test-package.json -b orders
