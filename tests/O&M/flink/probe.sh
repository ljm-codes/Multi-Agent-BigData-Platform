#!/usr/bin/env bash
# 在 Windows(Git Bash) 上跑 —— 和 checker 同机。
# Flink REST 走网络到 node1:7788, Redis 读本机 localhost:6379。
#
# 用法:
#   bash tests/flink/probe.sh                     # 自动从 YARN 找 RUNNING 的 FLINK 应用
#   bash tests/flink/probe.sh application_xxx_001 # 指定 YARN app id
#
# 待验证问题:
#   Q1 /applications/{YARN app_id} 到底认不认 YARN 的 id? 返回结构是什么?
#   Q2 /taskmanagers/{id} 的 metrics 键名是 heapUsed/heapMax 还是
#      Status.JVM.Memory.Heap.Used ? 值是字符串还是数字?
#   Q3 子任务的 duration 字段, 对 RUNNING 状态是恒定值还是递增?
#      (若恒定, check_flink_job_hang 会对健康作业误报)

set -uo pipefail

FLINK_REST="${FLINK_REST:-http://node1:7788}"
YARN_REST="${YARN_REST:-http://node1:8088}"

# Windows 上 python3 可能只是个 Microsoft Store 的占位符, 要真能跑才行
PY=""
for cand in python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import json,sys" >/dev/null 2>&1; then
    PY="$cand"; break
  fi
done
if [ -z "$PY" ]; then
  echo "找不到可用的 python, 请设置 PATH" >&2
  exit 1
fi

# Redis 相关段落在没装 redis-cli 时会静默跳过
REDIS_CLI="${REDIS_CLI:-redis-cli -h localhost -p 6379 -a 123456 --no-auth-warning}"

hr() { printf '\n%s\n' "────────────────────────────────────────────────────────"; }

echo "Flink REST: $FLINK_REST"

# ---------- 概览 ----------
hr; echo "【1】/jobs/overview  (clean_finished_job_records 用它判 RUNNING)"
curl -s --max-time 10 "$FLINK_REST/jobs/overview" | $PY -m json.tool 2>/dev/null | head -60

APP_ID="${1:-}"
if [ -z "$APP_ID" ]; then
  hr; echo "【提示】未传 app_id, 跳过 /applications 探测"
  echo "  从 YARN 取正在运行的 FLINK 应用:"
  curl -s --max-time 10 "$YARN_REST/ws/v1/cluster/apps?states=RUNNING" \
    | $PY -c "
import json,sys
apps = json.load(sys.stdin).get('apps',{}).get('app',[])
for a in apps:
    print('   ', a.get('id'), a.get('applicationType'), a.get('name'))
" 2>/dev/null
  echo "  然后重跑: bash probe.sh <application_id>"
  echo "  (Q2/Q3 不依赖 app_id, 继续往下探测)"
fi

# ---------- Q1: /applications/{id} ----------
hr; echo "【Q1】/applications/$APP_ID   —— 认不认 YARN 的 app id?"
CODE=$(curl -s -o /tmp/_app.json -w '%{http_code}' --max-time 10 "$FLINK_REST/applications/$APP_ID")
echo "HTTP $CODE"
if [ "$CODE" = "200" ]; then
  echo "  ✅ 认! 返回结构:"
  $PY -c "
import json
d = json.load(open('/tmp/_app.json'))
print('    顶层键:', list(d.keys()))
jobs = d.get('jobs')
print('    jobs 类型:', type(jobs).__name__)
if isinstance(jobs, list) and jobs:
    print('    jobs[0] 键:', list(jobs[0].keys()))
    print('    jobs[0][jid] =', jobs[0].get('jid'))
elif isinstance(jobs, dict):
    print('    jobs 的键:', list(jobs.keys())[:5])
" 2>/dev/null || head -c 600 /tmp/_app.json
else
  echo "  ❌ 不认 YARN 的 app id (checker 的 7 个 Flink 入口都会 404)"
  echo "  响应片段: $(head -c 300 /tmp/_app.json)"
  echo
  echo "  试试不带前缀的 Flink 自身 application id — 从 /jobs/overview 里的 jid 反查:"
  curl -s --max-time 10 "$FLINK_REST/jobs/overview" | head -c 400
fi

# ---------- Q2: /taskmanagers ----------
hr; echo "【Q2】/taskmanagers/{id}  —— metrics 的键名与值类型"
curl -s --max-time 10 "$FLINK_REST/taskmanagers" | $PY -c "
import json,sys
d = json.load(sys.stdin)
tms = d.get('taskmanagers', [])
print('  TaskManager 数量:', len(tms))
for tm in tms[:2]:
    print('   ', tm.get('id'))
    print('      path =', tm.get('path'))
" 2>/dev/null

TM_ID=$(curl -s --max-time 10 "$FLINK_REST/taskmanagers" \
        | $PY -c "import json,sys; t=json.load(sys.stdin).get('taskmanagers',[]); print(t[0]['id'] if t else '')" 2>/dev/null)
if [ -n "$TM_ID" ]; then
  echo "  取 $TM_ID 详情:"
  curl -s --max-time 10 "$FLINK_REST/taskmanagers/$TM_ID" -o /tmp/_tm.json
  $PY -c "
import json
d = json.load(open('/tmp/_tm.json'))
print('    顶层键:', list(d.keys()))
m = d.get('metrics', {})
print('    metrics 条数:', len(m))
keys = list(m.keys())
print('    含 heap 的键:', [k for k in keys if 'heap' in k.lower() or 'Heap' in k])
print('    含 Memory 的键:', [k for k in keys if 'Memory' in k][:12])
# checker 用的两个键是否存在, 以及值类型
for k in ('heapUsed', 'heapMax'):
    if k in m:
        v = m[k]
        print(f'    ✅ {k} 存在, 值={v!r} 类型={type(v).__name__}')
    else:
        print(f'    ❌ {k} 不存在  <-- checker 读的就是这个键名')
# 随便取几个值看类型(字符串还是数字)
for k in keys[:5]:
    print(f'    样例 {k} = {m[k]!r} ({type(m[k]).__name__})')
" 2>/dev/null
else
  echo "  没有 TaskManager (作业没跑起来?)"
fi

# ---------- Q3: 子任务 duration ----------
hr; echo "【Q3】子任务 duration 对 RUNNING 是恒定还是递增 (挂起检测的假阳性风险)"
JID=$(curl -s --max-time 10 "$FLINK_REST/jobs/overview" \
      | $PY -c "
import json,sys
js = json.load(sys.stdin).get('jobs', [])
run = [j for j in js if j.get('state')=='RUNNING']
print(run[0]['jid'] if run else (js[0]['jid'] if js else ''))
" 2>/dev/null)
if [ -z "$JID" ]; then
  echo "  没有作业, 跳过"
else
  echo "  作业 $JID, 取两次(间隔 6 秒)对比:"
  for round in 1 2; do
    curl -s --max-time 10 "$FLINK_REST/jobs/$JID" -o /tmp/_job.json
    VID=$($PY -c "
import json
d = json.load(open('/tmp/_job.json'))
vs = d.get('vertices', [])
print(vs[0]['id'] if vs else '')
" 2>/dev/null)
    if [ -n "$VID" ]; then
      curl -s --max-time 10 "$FLINK_REST/jobs/$JID/vertices/$VID" -o /tmp/_v.json
      echo "  --- 第 $round 次 ---"
      $PY -c "
import json
d = json.load(open('/tmp/_v.json'))
subs = d.get('subtasks', [])
print('    子任务数:', len(subs))
for s in subs[:3]:
    print('      subtask={} status={!r} duration={!r} start-time={!r} end-time={!r}'.format(
        s.get('subtask'), s.get('status'), s.get('duration'),
        s.get('start-time'), s.get('end-time')))
"
    fi
    [ "$round" = "1" ] && sleep 6
  done
  echo
  echo "  >>> 判读: 若两次 duration 完全相同且 status 也是 RUNNING 不变,"
  echo "      则 check_flink_job_hang 会在 3 次轮询后对所有健康作业误报挂起。"
fi

# ---------- 附: Redis 里的 checker 状态 ----------
hr; echo "【附】Redis 中 checker 留下的键"
$REDIS_CLI --scan --pattern 'job_hang:*' 2>/dev/null | head -5
$REDIS_CLI --scan --pattern 'watermarks:*' 2>/dev/null | head -5
echo "(空则说明 checker 还没跑过, 或 job_id 已变更)"
