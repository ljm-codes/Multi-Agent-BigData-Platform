#!/usr/bin/env bash
# 批量跑 Spark 故障验证 —— 从 Windows 侧执行(要 ssh 到 node1, 且 checker 在这边跑)
#
#   bash tests/batch_spark_live.sh                    # 跑默认这一批
#   bash tests/batch_spark_live.sh normal skew fmt    # 只跑指定故障
#
# 每个故障: 杀掉上一个应用 -> submit.sh 提交 -> 拿 app_id -> check_spark_live.py 验证。
# 应用之间必须串行: 上一次的 hold 还没结束就提交下一个, 集群上会有两个 Spark 应用
# 抢 3 台机器的 4G 内存, 结果不可信。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
N1=/tmp/n1.sh                     # ssh 到 node1 的助手(见 /tmp/n1.sh)
REMOTE=/home/hadoop/spark-fault

# 故障 -> check_spark_live.py 的额外参数
extra_args() {
  case "$1" in
    # check_spark_job_timeout 写死 30 分钟才报, 验证时调小;
    # 它还要隔一会儿调第二次(第一次只写 redis 快照)
    stuck) echo "--job-timeout-sec 60 --timeout-gap 25" ;;
    # spill 的大聚合作业要 ~9 分钟才真正开始调度, 等「job 列表非空」会跑太早
    # (--wait 重复时 argparse 取最后一个)
    spill) echo "--wait 900 --wait-job-done" ;;
    # netfail 的证据是「shuffle 数据丢了之后失败过一次」, 不是「跑完了」——
    # 只等 job 列表非空会跑太早, 几个 checker 全返回 None, 和「真的没异常」分不开
    netfail) echo "--wait-failed-stage --wait 420" ;;
    # oom_limit 的证据是「容器被 YARN 按内存超限杀掉」, 而容器越界要等 JVM 堆真的涨上去。
    # 只等「job 列表非空」会跑太早, 几个 checker 全返回 None, 和「真的没异常」分不开。
    oom_limit) echo "--wait-removed-executor --wait 600" ;;
    *)     echo "" ;;
  esac
}

FAULTS=("$@")
[ ${#FAULTS[@]} -eq 0 ] && FAULTS=(normal skew fmt spill oom_kill9 fetchwait shrink stuck netfail oom_limit)

PREV=""
for fault in "${FAULTS[@]}"; do
  if [ -n "$PREV" ]; then
    timeout 60 $N1 "yarn application -kill $PREV" >/dev/null 2>&1
    sleep 8
  fi

  echo
  echo "##################### $fault #####################"
  OUT=$(timeout 300 $N1 "cd $REMOTE && bash submit.sh $fault" 2>&1)
  APP=$(echo "$OUT" | grep -oE 'application_[0-9]+_[0-9]+' | tail -1)
  if [ -z "$APP" ]; then
    echo "########## $fault -> 提交失败 ##########"
    echo "$OUT" | tail -15
    PREV=""
    continue
  fi
  echo "########## $fault -> $APP ##########"

  # --wait: 等 Spark UI 经代理可达且 job 列表非空。故障作业跑起来之前 checker 会
  # 全部返回 None, 看起来像「没检出」, 其实只是跑早了。
  # shellcheck disable=SC2046
  # python 必须加 -u: stdout 不是终端时是块缓冲的, 被 timeout 杀掉时缓冲里的输出
  # 会整个丢掉 —— 表现是「跑了几百秒, 一行结果都没有」。超时也要给足(某些故障要
  # 等到 --wait 用完), 这里只当安全网。
  # shellcheck disable=SC2086
  timeout 1500 python -u "$HERE/check_spark_live.py" --app "$APP" --wait 200 $(extra_args "$fault") --dump 2>&1 \
    | grep --line-buffered -vE '^\s*阶段 .* 数据倾斜比为'   # 每轮的比值日志太吵, 只留 checker 结论
    # --line-buffered: grep 默认按块缓冲(4KB), 进度要攒够才可见 —— 和 python 不加 -u 是同一类坑
  # --dump 一直开着: 抓到的原始响应是事后唯一能复盘的证据(哪些 checker 返回了 None、
  # 是真没异常还是测量失败, 只能拿这些 JSON 离线复放)。体积很小, 不值得省。
  PREV="$APP"
done

if [ -n "$PREV" ]; then
  timeout 60 $N1 "yarn application -kill $PREV" >/dev/null 2>&1
fi
echo
echo "##################### 批量结束 #####################"
