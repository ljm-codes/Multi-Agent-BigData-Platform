#!/bin/bash
# 抓 NM 给容器记录的那个 PID, 和 /proc 里的真实情况对照
#
# **在 Windows 侧跑**, 结果**只写本地**文件; 集群上不落任何脚本、不写任何日志。
#
# 为什么需要它: 已经确认 `checkLimit()`(内存超限判定)在本集群**从来执行不到**,
# 卡在它上游的 CPU 门槛上 —— `pTree.getCpuUsagePercent()` 恒为 -1。而 CPU 用量取自
# **进程树**, 树建不起来时 `ProcfsBasedProcessTree.updateProcessTree()` 会静默早退:
#     if (me == null) { return; }        // 根进程没在 /proc 里找到
# 这个探针就是去区分「树为什么建不起来」的三种可能:
#   * PID 不在 /proc            -> NM 记的是个已经退出的包装进程(getProcessId 取错)
#   * PID 在 /proc 但 pid!=pgrp -> 容器不是进程组组长, 而树是按 pgrp 筛的
#   * PID 在 /proc 且 pid==pgrp -> 前两种都不是, 得回头再查
#
# 顺带把 launch_container.sh 里的 setsid 那行抠出来 —— 这是「容器到底有没有被
# setsid」的直接证据(源码里 DefaultContainerExecutor 写死的是
# `String exec = Shell.isSetsidAvailable ? "exec setsid" : "exec";`)。
#
# 用法: bash tests/probe_container_pid.sh [轮数] [间隔秒]
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
ROUNDS=${1:-45}
GAP=${2:-12}
OUT="$ROOT/tmp/container-pid-$(date +%H%M%S).log"
mkdir -p "$ROOT/tmp"

READER=$(mktemp)
cat > "$READER" <<'EOS'
for h in node1 node2 node3; do
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$h" bash -s -- "$h" <<'EOR'
h="$1"
d=$(ls -dt /data/nm-local/usercache/hadoop/appcache/application_*/ 2>/dev/null | head -1)
[ -z "$d" ] && exit 0
for c in "$d"container_*/; do
  [ -d "$c" ] || continue
  cid=$(basename "$c")

  # NM 记录的容器 PID: 优先找 pid 文件(名字随版本变, 直接列目录看有什么)
  pidfile=$(ls "$c" 2>/dev/null | grep -i '^.*pid$' | head -1)
  nm_pid=""
  [ -n "$pidfile" ] && nm_pid=$(tr -d ' \n' < "$c$pidfile" 2>/dev/null)

  # 容器里真实的 java 进程
  jpid=""
  for p in $(pgrep -f "[C]oarseGrainedExecutorBackend" 2>/dev/null); do
    [ "$(cat /proc/$p/comm 2>/dev/null)" = "java" ] || continue
    jpid=$p
  done

  line="$h $cid"
  [ -n "$pidfile" ] && line="$line NM_PID=$nm_pid($pidfile)"
  # /proc/<nm_pid> 是否存在, 以及它的 pgrp(stat 第 5 个字段)
  if [ -n "$nm_pid" ] && [ -d "/proc/$nm_pid" ]; then
    pgrp=$(awk '{print $5}' /proc/$nm_pid/stat 2>/dev/null)
    cmd=$(tr '\0' ' ' < /proc/$nm_pid/cmdline 2>/dev/null | cut -c1-60)
    line="$line [存活 pgrp=$pgrp ${nm_pid}==${pgrp}? $([ "$nm_pid" = "$pgrp" ] && echo 是 || echo 否)] cmd=$cmd"
  elif [ -n "$nm_pid" ]; then
    line="$line [**NM_PID 已不在 /proc**]"
  fi
  [ -n "$jpid" ] && line="$line java=$jpid(java_pgrp=$(awk '{print $5}' /proc/$jpid/stat 2>/dev/null))"
  echo "$line"

  # setsid 的直接证据
  sed -n '/setsid/p' "$c"launch_container.sh 2>/dev/null | tail -1 | sed "s|^|    $h launch: |"
done
EOR
done
EOS

echo "=== container-pid probe start $(date '+%F %T') rounds=$ROUNDS gap=${GAP}s ===" | tee "$OUT"
for _ in $(seq 1 "$ROUNDS"); do
  line=$(bash -c "/tmp/n1.sh 'bash -s'" < "$READER" 2>/dev/null)
  [ -n "$line" ] && echo "$(date '+%H:%M:%S') $line" | tee -a "$OUT"
  sleep "$GAP"
done
rm -f "$READER"
echo "=== container-pid probe end $(date '+%F %T') ===" | tee -a "$OUT"
