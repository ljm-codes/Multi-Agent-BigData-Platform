#!/bin/sh
set -e

ENV=/export/server/miniconda3/envs/spark_study
REQ=/opt/sandbox/requirements.txt

if [ ! -x "$ENV/bin/python" ]; then
    micromamba create -y -p "$ENV" -c conda-forge python=3.8 pip
fi

if ! "$ENV/bin/python" -c "import pyspark, pandas, numpy, pytest" 2>/dev/null; then
    "$ENV/bin/python" -m pip install --no-cache-dir -r "$REQ"
fi

"$ENV/bin/python" --version
"$ENV/bin/python" -c "import pyspark, pandas, numpy, pytest; print('sandbox packages ok')"
