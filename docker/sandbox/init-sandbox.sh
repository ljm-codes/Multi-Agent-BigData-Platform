#!/bin/sh
set -e

ENV=/export/server/miniconda3/envs/spark_study

if [ ! -x "$ENV/bin/python3.8" ]; then
    mkdir -p "$ENV"
    cp -a /usr/local/. "$ENV/"
    # /usr/local/bin/python3 原本指向 python3.8，直接写会覆盖二进制
    rm -f "$ENV/bin/python3"
    cat > "$ENV/bin/python3" <<'EOF'
#!/bin/sh
export PYTHONHOME=/export/server/miniconda3/envs/spark_study
export LD_LIBRARY_PATH=/export/server/miniconda3/envs/spark_study/lib:${LD_LIBRARY_PATH:-}
exec /export/server/miniconda3/envs/spark_study/bin/python3.8 "$@"
EOF
    chmod +x "$ENV/bin/python3"
fi

if ! "$ENV/bin/python3" -c "import pyspark, pandas, numpy, pytest" 2>/dev/null; then
    "$ENV/bin/python3" -m pip install --no-cache-dir -r /opt/sandbox/requirements.txt
fi

"$ENV/bin/python3" --version
"$ENV/bin/python3" -c "import pyspark, pandas, numpy, pytest; print('sandbox packages ok')"
