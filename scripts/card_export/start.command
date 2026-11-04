#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
[ -x .venv/bin/python ] || python3 -m venv .venv
.venv/bin/python -c 'import dissect.extfs, dissect.volume' 2>/dev/null || .venv/bin/python -m pip install --no-index --find-links wheels -r requirements.txt
exec sudo "$PWD/.venv/bin/python" "$PWD/export.py" "$@"
