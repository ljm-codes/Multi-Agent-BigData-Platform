#!/bin/bash
# 采 executor JVM 的 RSS **和 NM 自己算出来的内存** —— 验证容器超限杀到底卡在哪一环
#
# **在 Windows 侧跑**, 结果**只写本地**文件; 集群上不落任何脚本、不写任何日志
# (远端只读 /proc 和 NM REST)。采集脚本经 ssh 的 stdin 送过去, 用完即走。
#
# 为什么要这个探针: oom_limit 的地基是「容器 RSS 越过它自己的额度 -> NM 杀掉」。
# 撑不爆有**两种**可能 —— 注入没生效, 或者 NM 的监控没算出这个数 ——
# 只看 checker 报不报**分不开**(这正是 README 里「让检测不了变得可见」要解决的问题)。
# 所以要同时看两侧:
#   * /proc/<pid>/status 的 VmRSS —— **内核视角**, 容器真实占了多少内存;
#   * NM REST 的 resourceUtilization.memory —— **NM 视角**, 它自己算出来多少。
#
# 判据(本轮容器额度 = 1024MB, 见 submit.sh 的 oom_limit):
#   * 两边都低于额度        -> 注入没生效, 回头改注入;
#   * 内核视角超了、NM 视角没超 -> **卡在 NM 的取数上**(进程树/算法), 不是注入问题;
#   * 两边都超了但容器没死   -> 卡在 NM 的判定/杀进程上。
#
# 用法: bash tests/probe_executor_jvm.sh [轮数] [间隔秒]
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
ROUNDS=${1:-60}
GAP=${2:-10}
OUT="$ROOT/tmp/jvm-probe-$(date +%H%M%S).log"
mkdir -p "$ROOT/tmp"

READER=$(mktemp)
cat > "$READER" <<'EOS'
for h in node1 node2 node3; do
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$h" bash -s -- "$h" <<'EOR'
h="$1"
found=""
for p in $(pgrep -f "[C]oarseGrainedExecutorBackend" 2>/dev/null); do
  # **必须按 comm 过滤成 java**: YARN 起的容器是「bash -c 'java ...'」两层, 两层的
  # 命令行里都含 YarnCoarseGrainedExecutorBackend, pgrep 会**先**匹配到 bash 包装进程。
  # 实测踩过: 取到包装进程, RSS 报成 1MB, 看着像"JVM 根本没吃内存"。
  [ "$(cat /proc/$p/comm 2>/dev/null)" = "java" ] || continue
  rss=$(awk '/^VmRSS:/{print $2}' /proc/$p/status 2>/dev/null)
  found="$found pid=$p(内核视角 RSS=$(( ${rss:-0} / 1024 ))MB)"
done
[ -z "$found" ] && exit 0
echo "$h$found"
# NM 视角: 它自己按进程树算出来的内存 (resourceUtilization.memory, 单位 MB)
curl -s --max-time 5 "http://$h:8042/ws/v1/node/containers" 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in d.get('containers', {}).get('container', []):
    ru = (c.get('resourceUtilization') or {}).get('memory')
    print('    $h NM视角: %s 额度=%sMB NM实测=%sMB' % (
        (c.get('id') or '')[-14:], c.get('totalMemoryNeededMB'), ru))
" 2>/dev/null
EOR
done
EOS

echo "=== jvm probe start $(date '+%F %T') rounds=$ROUNDS gap=${GAP}s ===" | tee "$OUT"
for _ in $(seq 1 "$ROUNDS"); do
  line=$(bash -c "/tmp/n1.sh 'bash -s'" < "$READER" 2>/dev/null)
  [ -n "$line" ] && echo "$(date '+%H:%M:%S') $line" | tee -a "$OUT"
  sleep "$GAP"
done
rm -f "$READER"
echo "=== jvm probe end $(date '+%F %T') ===" | tee -a "$OUT"
