#!/usr/bin/env bash
# 在 node1 上提交 FaultJob 到 YARN (application 模式)
#
# 注意必须用 `flink run-application`, 不能用 `flink run -t yarn-application` ——
# 后者会在客户端报 "No ExecutorFactory found to execute the application."
#
# 用法:
#   bash submit.sh normal                      # 健康基线 —— 先跑这个
#   bash submit.sh skew                        # 数据倾斜
#   bash submit.sh hang                        # 子任务挂起
#   bash submit.sh backpressure --slow-ms 50   # 反压
#   bash submit.sh checkpoint                  # checkpoint 失败
#   bash submit.sh oom --heap-fill-mb 600      # TaskManager 高水位
#   bash submit.sh watermark_stalled           # 水位线停滞
#
# 提交后作业会先发数据、再进入 hold 阶段保持 RUNNING, 期间在 Windows 侧跑 checker。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 本机在 build/ 下, 传到 node1 后通常和脚本同目录
if [ -z "${JAR:-}" ]; then
  if [ -f "$HERE/build/fault-flink.jar" ]; then
    JAR="$HERE/build/fault-flink.jar"
  else
    JAR="$HERE/fault-flink.jar"
  fi
fi
FLINK="${FLINK:-/export/server/flink/bin/flink}"
FAULT="${1:-normal}"
shift || true

if [ ! -f "$JAR" ]; then
  echo "找不到 $JAR" >&2
  echo "先在 Windows 上执行: bash tests/flink/build.sh, 再把 build/fault-flink.jar 传过来" >&2
  exit 1
fi

# 各故障的默认参数
HOLD=300
EXTRA=()
case "$FAULT" in
  normal)            EXTRA=(--records 200000 --rate 400) ;;
  skew)              EXTRA=(--records 60000  --rate 1000 --parallelism 3) ;;
  hang)              EXTRA=(--records 2000   --rate 500  --parallelism 3) ;;
  # --record-size 必须给大值: 记录太小则下游网络缓冲填不满, backpressureLevel 会一直是 ok
  backpressure)      EXTRA=(--records 500000 --rate 2000 --parallelism 3 --slow-ms 200 --record-size 4096) ;;
  # 10ms 是 Flink 允许的最小 checkpointTimeout (小于它作业启动即报非法参数)
  checkpoint)        EXTRA=(--records 100000 --rate 400 --checkpoint-interval-ms 8000 --checkpoint-timeout-ms 10) ;;
  # 填充量是实测标定的: 该集群 TM 的 heapMax=519MB(frameworkHeap 128 + taskHeap 384),
  # 空载 heapUsed 约 68MB。注意堆使用率会随 GC 浮动, 余量必须留够:
  #   填 420MB -> 实测在 89%~96% 之间抖, 会骑在 0.9 阈值上, 时检时不检
  #   填 450MB -> 稳定在 97.6%~97.9%, 可靠触发
  # 堆上限(519MB)远低于 YARN 容器上限(2048MB), 所以压到 90%+ 不会触发 exit 137。
  # 换集群需重新标定: 先跑 --heap-fill-mb 100 读一次 heapUsed/heapMax 再按比例算, 并留 5% 余量。
  oom)               EXTRA=(--records 2000 --rate 100 --parallelism 1 --heap-fill-mb 450) ;;
  watermark_stalled) EXTRA=(--records 50000  --rate 300) ;;
  watermark_uninit)  EXTRA=(--records 50000  --rate 300) ;;
  *) echo "未知故障: $FAULT" >&2; exit 1 ;;
esac

echo "=== 提交 FaultJob-$FAULT (hold ${HOLD}s) ==="
echo "jar: $JAR"
echo

$FLINK run-application -t yarn-application \
  -c com.ops.fault.FaultJob "$JAR" \
  --fault "$FAULT" \
  --hold-seconds "$HOLD" \
  "${EXTRA[@]}" "$@"

echo
echo "=== 提交完成, 作业会保持 RUNNING ${HOLD}s ==="
echo "提交日志里的这行给出了 JobManager 实际落在哪台机器:"
echo "    Found Web Interface <host>:7788 of application '<app_id>'"
echo "但 checker 走 YARN 代理(node1:8089/proxy/{app_id}), 所以地址动态也无所谓。"
echo
echo "在 Windows 侧验证:"
echo "  python tests/check_flink_live.py"
echo
echo "提前结束作业(application 模式要走 yarn 命令, 不是 flink cancel):"
echo "  yarn application -list"
echo "  yarn application -kill <application_id>"
