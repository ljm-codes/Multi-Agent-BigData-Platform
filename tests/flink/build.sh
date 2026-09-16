#!/usr/bin/env bash
# 用 javac 直接编译 FaultJob, 不依赖 Maven (这台机器 mvn 不在 PATH, .mvn 也是空的)
# classpath 取自本机 .m2 里已有的 Flink 1.18.1 依赖
#
# 用法: bash tests/flink/build.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

JAVA8_HOME="${JAVA8_HOME:-/d/Java/java8}"
JAVAC="$JAVA8_HOME/bin/javac.exe"
JARBIN="$JAVA8_HOME/bin/jar.exe"

if [ ! -x "$JAVAC" ]; then
  echo "找不到 javac: $JAVAC" >&2
  echo "设置 JAVA8_HOME 指向你的 JDK8 根目录" >&2
  exit 1
fi

M2_FLINK="$HOME/.m2/repository/org/apache/flink"
if [ ! -d "$M2_FLINK" ]; then
  echo "找不到 Flink 依赖: $M2_FLINK" >&2
  exit 1
fi

# Java 的 classpath 用 ; 分隔, 且必须是 Windows 路径
CP=""
JAR_COUNT=0
while IFS= read -r j; do
  CP="${CP}$(cygpath -w "$j");"
  JAR_COUNT=$((JAR_COUNT + 1))
done < <(find "$M2_FLINK" -path "*1.18.1*" -name "*.jar" \
           | grep -v -- '-sources' | grep -v -- '-javadoc' | sort)

if [ "$JAR_COUNT" -eq 0 ]; then
  echo "没找到任何 Flink 1.18.1 jar" >&2
  exit 1
fi
echo "classpath: $JAR_COUNT 个 jar"

OUT_DIR="build/classes"
JAR_OUT="build/fault-flink.jar"
rm -rf build
mkdir -p "$OUT_DIR"

# 收集源文件 (相对路径, javac 认正斜杠)
SRCS=$(find src/main/java -name '*.java')
echo "源文件: $(echo "$SRCS" | wc -l) 个"

# MSYS2_ARG_CONV_EXCL 防止 Git Bash 把 Windows 路径参数再转换一次
# -J-Duser.language=en : javac 默认按系统 locale 输出 GBK 中文, 在 bash 里是乱码
MSYS2_ARG_CONV_EXCL="*" "$JAVAC" -encoding UTF-8 -nowarn \
  -J-Duser.language=en -J-Duser.country=US \
  -cp "$CP" \
  -d "$OUT_DIR" \
  $SRCS

echo "编译完成 -> $OUT_DIR"

# 只打包自己的类; Flink 自身的 jar 由集群提供, 不能塞进来
MSYS2_ARG_CONV_EXCL="*" "$JARBIN" cf "$JAR_OUT" -C "$OUT_DIR" .
echo "打包完成 -> $JAR_OUT"
ls -lh "$JAR_OUT"
