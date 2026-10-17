#!/usr/bin/env bash
# 所有 Java main 的命令行执行说明与入口（包括 src/test/java，不包含 Scala）。
# bash scripts/java-mains.sh --help
# bash scripts/java-mains.sh --commands   # 打印每个 main 的完整命令，不启动任务
# bash scripts/java-mains.sh cn.doitedu.rtmk.demo8.RuleEngineRecovery
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'HELP'
用法（在项目根目录执行）：
  bash scripts/java-mains.sh --list
  bash scripts/java-mains.sh --commands
  bash scripts/java-mains.sh --all [--seconds 90]
  bash scripts/java-mains.sh --prepare
  bash scripts/java-mains.sh <完整类名> [传给 main 的参数...]

第一次使用：安装 Maven、Python 3、JDK 17，设置 JAVA_HOME：
  export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS
  export PATH="$JAVA_HOME/bin:$PATH"
  bash scripts/java-mains.sh --prepare

默认自动读取已有 .artifacts/main-audit/k3s/runtime-env.json；也可显式选择配置：
  export MAIN_ENV_JSON="$PWD/.artifacts/main-audit/k3s/runtime-env.json"
  KAFKA_INPUT_TOPIC=main-audit-rules bash scripts/java-mains.sh cn.doitedu.rtmk.demo8.RuleEngineRecovery
该 JSON 仅存在于本地，不随 Git 分发；没有此文件时，直接 export 所需环境变量。
JSON 格式为环境变量名到字符串值的对象；文件内同名变量优先于终端变量。

连接配置按程序需要设置：
  KAFKA_BOOTSTRAP_SERVERS；HBASE_ZOOKEEPER_QUORUM
  MYSQL_HOST / MYSQL_PORT / MYSQL_USERNAME / MYSQL_PASSWORD
  MYSQL_DATABASE（业务库） / MYSQL_RULE_DATABASE（规则库）
  REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DATABASE
  DORIS_FENODES / DORIS_USERNAME / DORIS_PASSWORD
未设置时部分程序仍使用课程默认 doitedu 地址，Mac 通常无法解析。
Kafka broker 返回的地址也必须从本机可达。库、表、规则及画像数据需事先准备。
示例：现有 k3s MySQL、Redis 使用本地 13306、16379 隧道时，在另一终端保持：
  ssh -N -o ExitOnForwardFailure=yes -L 13306:10.43.251.207:3306 -L 16379:10.43.215.114:6379 root@192.168.31.77
以上 ClusterIP 对应当前环境，其他环境需替换；端口已占用时先检查已有隧道。

本地文件示例：
  mkdir -p .artifacts
  bash scripts/java-mains.sh cn.doitedu.rtdw.utils.DimRecProcess examples/main-inputs/dim_rec.csv .artifacts/dim-rec-commands.sh
  bash scripts/java-mains.sh cn.doitedu.rtmk.demo9.groovy.TestDiaoyong examples/main-inputs/hello.groovy
  EVENT_BATCHES=1 KAFKA_OUTPUT_TOPIC=main-audit-rules bash scripts/java-mains.sh cn.doitedu.test.EventsMoni
EventsMoni 会真实写 Kafka；HbaseProfileTagsMoni、HistoryConditionValueMoni、RedisDemo
等会访问或写入外部系统，执行前应选择自己的测试库、topic 和 Redis DB。

持续运行的 Flink/Kafka/CDC 程序不会自动退出：观察所有算子 RUNNING、checkpoint 成功，
且没有持续 RESTARTING；无新数据时可能没有业务输出。Ctrl+C 停止（通常退出码 130）。
需要正常退出或恢复验证时，请参考 docs/k3s-main-execution.md 的 savepoint 验收说明。
Local 命名不代表全部无需中间件，仍以具体代码的连接调用为准。

额外前置条件：
  HttpRequestDemo、cn.doitedu.Http、搜索 ETL06：SIMWORDS_URL 指向真实语义 HTTP 服务。
  cn.doitedu.EtlAsync：还需要 SOCKET_HOST / SOCKET_PORT 对应文本 Socket 服务。
  当前验收环境缺少语义服务和该 Socket 服务，不能仅靠启动命令保证这些业务成功。
  StandaloneEventsJob：STANDALONE_OUTPUT_URI 设置为可写目录的 file URI。
  WindowTest：FLINK_CHECKPOINT_URI 设置为本机可写 file URI，避免课程默认 HDFS 地址。

脚本复用 run-main.sh，自动应用 scripts/java17.options，并默认编译所选模块。
日志同时输出控制台及 logs/runtime/<完整类名>.log。
全量复跑并保存脱敏快照：python3 scripts/execute-java-mains.py --seconds 90
快照和结果见 logs/validation/；完整说明见 logs/README.md。
若私有 JSON 设置 SKIP_MAIN_COMPILE=true，修改代码后须先重新执行 --prepare。
完整连接、数据准备和各 main 已验证范围见 docs/k3s-main-execution.md。
HELP
}

# 模块 | 完整类名 | 源文件；--commands 为以下每一项生成可复制命令。
catalog() {
  cat <<'MAINS'
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob02_MallTfcAg01|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob02_MallTfcAg01.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob03_PageAccessTimeLong|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob03_PageAccessTimeLong.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob04_InteractiveEventsOverview|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob04_InteractiveEventsOverview.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob05_VideoEventsAgg|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob05_VideoEventsAgg.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob06_SearchEventsAnalyse.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse_Async|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob06_SearchEventsAnalyse_Async.java
realtime_dw|cn.doitedu.rtdw.data_etl.EtlJob07_AdRequestSessionProcess|realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob07_AdRequestSessionProcess.java
realtime_dw|cn.doitedu.rtdw.data_sync.SyncJob01_UserInfo2Hbase|realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob01_UserInfo2Hbase.java
realtime_dw|cn.doitedu.rtdw.data_sync.SyncJob02_VideoInfo2Hbase|realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob02_VideoInfo2Hbase.java
realtime_dw|cn.doitedu.rtdw.data_sync.SyncJob03_AdRequestFeatures2Hbase|realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob03_AdRequestFeatures2Hbase.java
realtime_dw|cn.doitedu.rtdw.deployment.StandaloneEventsJob|realtime_dw/src/main/java/cn/doitedu/rtdw/deployment/StandaloneEventsJob.java
realtime_dw|cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUV|realtime_dw/src/main/java/cn/doitedu/rtdw/rt_dashboard/Promotion01_PVUV.java
realtime_dw|cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUVLocal|realtime_dw/src/main/java/cn/doitedu/rtdw/rt_dashboard/Promotion01_PVUVLocal.java
realtime_dw|cn.doitedu.rtdw.utils.DimRecProcess|realtime_dw/src/main/java/cn/doitedu/rtdw/utils/DimRecProcess.java
realtime_dw|cn.doitedu.rtdw.utils.HttpRequestDemo|realtime_dw/src/main/java/cn/doitedu/rtdw/utils/HttpRequestDemo.java
realtime_dw|cn.doitedu.EtlAsync|realtime_dw/src/test/java/cn/doitedu/EtlAsync.java
realtime_dw|cn.doitedu.Http|realtime_dw/src/test/java/cn/doitedu/Http.java
realtime_dw|cn.doitedu.WindowTest|realtime_dw/src/test/java/cn/doitedu/WindowTest.java
rt_rule_mk|cn.doitedu.rtmk.demo1.TargetProductCartMonitor|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo1/TargetProductCartMonitor.java
rt_rule_mk|cn.doitedu.rtmk.demo1.TargetProductCartMonitorLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo1/TargetProductCartMonitorLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo2.MultiRuleEngine|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo2/MultiRuleEngine.java
rt_rule_mk|cn.doitedu.rtmk.demo2.MultiRuleEngineLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo2/MultiRuleEngineLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo3.RuleCalculatorPattern|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo3/RuleCalculatorPattern.java
rt_rule_mk|cn.doitedu.rtmk.demo3.RuleCalculatorPatternLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo3/RuleCalculatorPatternLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo4.ModelBasedRuleEngine|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo4/ModelBasedRuleEngine.java
rt_rule_mk|cn.doitedu.rtmk.demo4.ModelBasedRuleEngineLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo4/ModelBasedRuleEngineLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngine|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo5/BitmapOptimizedRuleEngine.java
rt_rule_mk|cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngineLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo5/BitmapOptimizedRuleEngineLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo6.DynamicRuleManagement|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo6/DynamicRuleManagement.java
rt_rule_mk|cn.doitedu.rtmk.demo6.DynamicRuleManagementLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo6/DynamicRuleManagementLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo7.DynamicRuleManagementV2|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo7/DynamicRuleManagementV2.java
rt_rule_mk|cn.doitedu.rtmk.demo7.DynamicRuleManagementV2Local|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo7/DynamicRuleManagementV2Local.java
rt_rule_mk|cn.doitedu.rtmk.demo8.RuleEngineRecovery|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo8/RuleEngineRecovery.java
rt_rule_mk|cn.doitedu.rtmk.demo8.RuleEngineRecoveryLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo8/RuleEngineRecoveryLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo9.DynamicRuleWithGroovy|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/DynamicRuleWithGroovy.java
rt_rule_mk|cn.doitedu.rtmk.demo9.DynamicRuleWithGroovyLocal|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/DynamicRuleWithGroovyLocal.java
rt_rule_mk|cn.doitedu.rtmk.demo9.groovy.TestDiaoyong|rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/groovy/TestDiaoyong.java
rt_rule_mk|cn.doitedu.test.EventsMoni|rt_rule_mk/src/test/java/cn/doitedu/test/EventsMoni.java
rt_rule_mk|cn.doitedu.test.HbaseProfileTagsMoni|rt_rule_mk/src/test/java/cn/doitedu/test/HbaseProfileTagsMoni.java
rt_rule_mk|cn.doitedu.test.HistoryConditionValueMoni|rt_rule_mk/src/test/java/cn/doitedu/test/HistoryConditionValueMoni.java
redis_demo|cn.doitedu.jedis.RedisDemo|redis_demo/src/main/java/cn/doitedu/jedis/RedisDemo.java
MAINS
}

case "${1:---help}" in
  --help|-h) usage ;;
  --list)
    catalog
    ;;
  --commands)
    while IFS='|' read -r module main source; do
      printf '# %s\nbash scripts/java-mains.sh %s\n\n' "$source" "$main"
    done < <(catalog)
    ;;
  --all)
    shift
    exec python3 "$ROOT_DIR/scripts/execute-java-mains.py" "$@"
    ;;
  --prepare|prepare|perpare)
    mvn -B -ntp test-compile
    mvn -B -ntp -q dependency:build-classpath \
      -Dmdep.includeScope=test -Dmdep.outputFile=target/main-audit-classpath.txt
    ;;
  *)
    selected_main="$1"
    shift
    while IFS='|' read -r module main source; do
      if [[ "$main" == "$selected_main" ]]; then
        exec bash "$ROOT_DIR/scripts/run-main.sh" "$module" "$main" "$@"
      fi
    done < <(catalog)
    printf '未找到 Java main：%s；使用 --list 查看完整类名。\n' "$selected_main" >&2
    exit 2
    ;;
esac
