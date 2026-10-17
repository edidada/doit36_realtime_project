# master 分支全部 main 执行记录（JDK 17）

执行日期：2026-10-17（Asia/Shanghai）。分支：`master`，修改基线：`94fccc3`。
执行机器：macOS 12.7.6 / Intel，Microsoft OpenJDK 17.0.18，Maven 3.6.1。

扫描四个模块的 `src/main` 和 `src/test`，包含 Java 和 Scala，共 **42 个 main，全部实际启动执行**。最终 **10 个正常退出，32 个因外部服务或输入文件不具备条件而未通过**。不能据此宣称 Kafka/HBase/Doris 全链路、故障恢复或生产环境验收通过。

## 修复内容

| 项目 | 原版本 | 本次版本 |
| --- | --- | --- |
| Flink | 1.16.1 | 1.19.3 |
| Kafka / JDBC connector | 跟随旧版本 | 3.2.0-1.19 |
| HBase connector | 旧版 | 4.0.0-1.19 |
| MySQL CDC | com.ververica 2.3.0 | org.apache.flink 3.1.1 |
| Doris connector | flink-doris-connector-1.14_2.12:1.1.0 | flink-doris-connector-1.19:1.6.1 |
| Spark | 3.1.2 | 3.5.8 |
| Scala / scala-maven-plugin | 2.12.12 / 旧插件 | 2.12.21 / 4.9.2 |

Flink 1.19 文档说明：从 1.18 引入对 Java 17 的实验性支持；运行时仍需要相应 Java 模块开放参数。本次选择 1.19.3，并配套升级连接器。参考 [Flink Java 兼容性](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/java_compatibility/)、[CDC 3.1 兼容范围](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/overview/)、[Spark 3.5.8 运行条件](https://spark.apache.org/docs/3.5.8/)。

其他实际修复：

- 显式补齐新 Kafka connector 运行时需要的 `flink-connector-base`。
- 补齐 `hadoop-hdfs-client:3.3.2`，修复 WindowTest 的 `HdfsConfiguration` 缺失；重跑后进入 HDFS 连接阶段，因 `doitedu` 无法解析而停止。
- 对齐 Spark 的 Log4j API/Core、SLF4J 和 commons-lang3，解决 `ServiceLoaderUtil` 缺失及 `JAVA_11` 字段冲突。最终已启动 SparkContext、Executor 和 BlockManager，随后受 MySQL 地址限制。
- 将 22 处 Windows 专用 checkpoint 路径改为系统临时目录，并提供 `flink.checkpoints.dir` 系统属性覆盖；RocksDB 本地目录也改为系统临时目录。临时目录仅用于本地执行，生产环境需要持久存储。
- ETL01 的 LOOKUP hint 改为 SQL 中实际使用的别名 `u`，修复新版 Flink 的校验错误。
- 新增统一启动脚本和逐项执行脚本，保留完整日志与退出码。

## RuleEngineRecovery 专项结果

升级前在 JDK 17 上复现：`InaccessibleObjectException`，无法开放 `java.lang.String.value`，调用栈经过 ClosureCleaner，到 `RuleEngineRecovery.main`。

升级后使用本仓库 Java 17 参数启动，原反射错误消失。原 `RuleEngineRecovery` 在 Kafka 初始化时报告 `No resolvable bootstrap urls given in bootstrap.servers`；其配置仍为 `doitedu:9092`。因此原程序未通过业务验收。后续还需要 MySQL CDC 规则表以及规则使用的 Redis/HBase 数据。

`RuleEngineRecoveryLocal` 使用内置数据，输出 **2 条规则匹配结果**，退出码 **0**。这仅验证本地示例执行，不代表已经验证 checkpoint 恢复、savepoint 迁移或故障重启。

## 逐项执行结果

下表每个类名均链接到实际执行的源码，包含测试目录中的 main。

- **通过 / 0**：有限输入处理完成，进程自行正常退出。
- **条件不足 / 1**：程序自行异常退出，原因见说明。
- **条件不足 / 143**：日志已出现明确外部依赖错误，由执行脚本发送 SIGTERM 停止重试，不能算通过。
- **超时 / 143**：达到观察上限后终止，不能算通过。TargetProductCartMonitor 首轮观察约 40 秒，其他最终记录以表格为准。

配置条件代码：K=Kafka；M=MySQL（CDC 程序还需 binlog）；H=HBase/ZooKeeper；R=Redis；D=Doris；A=HTTP 语义服务；S=Socket 输入；F=HDFS。具体条件见下文。

### realtime_dw

| main（源码） | 结果 / 退出码 | 耗时 | 实际证据及执行条件 |
| --- | --- | --- | --- |
| [cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim.java) | 条件不足 / 143 | 23.27s | Kafka bootstrap 地址不可解析；需 K、H、M、D。 |
| [cn.doitedu.rtdw.data_etl.EtlJob02_MallTfcAg01](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob02_MallTfcAg01.java) | 条件不足 / 1 | 42.82s | Doris FE schema HTTP 请求失败；需 D、K。 |
| [cn.doitedu.rtdw.data_etl.EtlJob03_PageAccessTimeLong](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob03_PageAccessTimeLong.java) | 条件不足 / 143 | 19.12s | Doris FE schema HTTP 请求失败；需 D、K。 |
| [cn.doitedu.rtdw.data_etl.EtlJob04_InteractiveEventsOverview](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob04_InteractiveEventsOverview.java) | 条件不足 / 143 | 19.89s | Doris FE schema HTTP 请求失败；需 D、K。 |
| [cn.doitedu.rtdw.data_etl.EtlJob05_VideoEventsAgg](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob05_VideoEventsAgg.java) | 条件不足 / 143 | 24.33s | Doris FE schema HTTP 请求失败；需 D、K。 |
| [cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob06_SearchEventsAnalyse.java) | 条件不足 / 143 | 22.74s | Doris FE schema HTTP 请求失败；需 D、K。另需 A（搜索词服务）。 |
| [cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse_Async](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob06_SearchEventsAnalyse_Async.java) | 条件不足 / 143 | 18.88s | Doris FE schema HTTP 请求失败；需 D、K。另需 A（搜索词服务）。 |
| [cn.doitedu.rtdw.data_etl.EtlJob07_AdRequestSessionProcess](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_etl/EtlJob07_AdRequestSessionProcess.java) | 条件不足 / 143 | 15.91s | Kafka bootstrap 地址不可解析；需 K、H。 |
| [cn.doitedu.rtdw.data_sync.SyncJob01_UserInfo2Hbase](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob01_UserInfo2Hbase.java) | 条件不足 / 143 | 9.81s | MySQL CDC 初始化 UnknownHostException: doitedu；需 M、H。 |
| [cn.doitedu.rtdw.data_sync.SyncJob02_VideoInfo2Hbase](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob02_VideoInfo2Hbase.java) | 条件不足 / 143 | 9.79s | MySQL CDC 初始化 UnknownHostException: doitedu；需 M、H。 |
| [cn.doitedu.rtdw.data_sync.SyncJob03_AdRequestFeatures2Hbase](../realtime_dw/src/main/java/cn/doitedu/rtdw/data_sync/SyncJob03_AdRequestFeatures2Hbase.java) | 条件不足 / 143 | 9.04s | Kafka bootstrap 地址不可解析；需 K、H。 |
| [cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUV](../realtime_dw/src/main/java/cn/doitedu/rtdw/rt_dashboard/Promotion01_PVUV.java) | 条件不足 / 143 | 11.59s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUVLocal](../realtime_dw/src/main/java/cn/doitedu/rtdw/rt_dashboard/Promotion01_PVUVLocal.java) | 通过 / 0 | 6.83s | 内置输入处理完成，输出 PVUV统计结果；此 Local 实现是逐事件示例，未验证生产窗口聚合。 |
| [cn.doitedu.rtdw.utils.DimRecProcess](../realtime_dw/src/main/java/cn/doitedu/rtdw/utils/DimRecProcess.java) | 条件不足 / 1 | 0.26s | FileNotFoundException: d:/dim_red.csv；需输入 CSV，并配置可写输出路径（原为 d:/ok.sh）。 |
| [cn.doitedu.rtdw.utils.HttpRequestDemo](../realtime_dw/src/main/java/cn/doitedu/rtdw/utils/HttpRequestDemo.java) | 条件不足 / 1 | 1.01s | HTTP 请求 UnknownHostException: doitedu；需 A。 |
| [cn.doitedu.EtlAsync](../realtime_dw/src/test/java/cn/doitedu/EtlAsync.java) | 条件不足 / 143 | 5.0s | Socket 输入主机 doitedu 无法解析；需 S、A。 |
| [cn.doitedu.Http](../realtime_dw/src/test/java/cn/doitedu/Http.java) | 条件不足 / 143 | 1.4s | HTTP 请求 UnknownHostException: doitedu；需 A。 |
| [cn.doitedu.WindowTest](../realtime_dw/src/test/java/cn/doitedu/WindowTest.java) | 条件不足 / 143 | 14.84s | 补齐 HDFS 客户端后 UnknownHostException: doitedu；需 F、K，尚未验证窗口 drain/stop。 |

### rt_rule_mk

| main（源码） | 结果 / 退出码 | 耗时 | 实际证据及执行条件 |
| --- | --- | --- | --- |
| [cn.doitedu.rtmk.demo1.TargetProductCartMonitor](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo1/TargetProductCartMonitor.java) | 超时 / 143 | 40.45s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.rtmk.demo1.TargetProductCartMonitorLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo1/TargetProductCartMonitorLocal.java) | 通过 / 0 | 7.06s | 内置输入处理完成；输出 3 条匹配结果。 |
| [cn.doitedu.rtmk.demo2.MultiRuleEngine](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo2/MultiRuleEngine.java) | 条件不足 / 143 | 10.32s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.rtmk.demo2.MultiRuleEngineLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo2/MultiRuleEngineLocal.java) | 通过 / 0 | 10.13s | 内置输入处理完成；输出 1 条匹配结果。 |
| [cn.doitedu.rtmk.demo3.RuleCalculatorPattern](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo3/RuleCalculatorPattern.java) | 条件不足 / 143 | 5.49s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.rtmk.demo3.RuleCalculatorPatternLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo3/RuleCalculatorPatternLocal.java) | 通过 / 0 | 10.44s | 内置输入处理完成；输出 1 条匹配结果。 |
| [cn.doitedu.rtmk.demo4.ModelBasedRuleEngine](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo4/ModelBasedRuleEngine.java) | 条件不足 / 143 | 7.86s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.rtmk.demo4.ModelBasedRuleEngineLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo4/ModelBasedRuleEngineLocal.java) | 通过 / 0 | 10.89s | 内置输入处理完成；输出 3 条匹配结果。 |
| [cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngine](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo5/BitmapOptimizedRuleEngine.java) | 条件不足 / 143 | 5.26s | Kafka bootstrap 地址不可解析；需 K；启用画像/历史规则时另需 H/R。 |
| [cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngineLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo5/BitmapOptimizedRuleEngineLocal.java) | 通过 / 0 | 6.58s | 内置输入处理完成；输出 2 条匹配结果。 |
| [cn.doitedu.rtmk.demo6.DynamicRuleManagement](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo6/DynamicRuleManagement.java) | 条件不足 / 143 | 11.58s | Kafka bootstrap 地址不可解析；需 K；动态规则类还需 M，相关画像/历史规则需 H/R。 |
| [cn.doitedu.rtmk.demo6.DynamicRuleManagementLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo6/DynamicRuleManagementLocal.java) | 通过 / 0 | 7.09s | 内置输入处理完成；输出 2 条匹配结果。 |
| [cn.doitedu.rtmk.demo7.DynamicRuleManagementV2](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo7/DynamicRuleManagementV2.java) | 条件不足 / 143 | 10.59s | Kafka bootstrap 地址不可解析；需 K；动态规则类还需 M，相关画像/历史规则需 H/R。 |
| [cn.doitedu.rtmk.demo7.DynamicRuleManagementV2Local](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo7/DynamicRuleManagementV2Local.java) | 通过 / 0 | 7.6s | 内置输入处理完成；输出 2 条匹配结果。 |
| [cn.doitedu.rtmk.demo8.RuleEngineRecovery](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo8/RuleEngineRecovery.java) | 条件不足 / 143 | 14.11s | Kafka bootstrap 地址不可解析；需 K；动态规则类还需 M，相关画像/历史规则需 H/R。 |
| [cn.doitedu.rtmk.demo8.RuleEngineRecoveryLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo8/RuleEngineRecoveryLocal.java) | 通过 / 0 | 6.85s | 内置输入处理完成；输出 2 条匹配结果。 |
| [cn.doitedu.rtmk.demo9.DynamicRuleWithGroovy](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/DynamicRuleWithGroovy.java) | 条件不足 / 143 | 10.33s | Kafka bootstrap 地址不可解析；需 K；动态规则类还需 M，相关画像/历史规则需 H/R。 |
| [cn.doitedu.rtmk.demo9.DynamicRuleWithGroovyLocal](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/DynamicRuleWithGroovyLocal.java) | 通过 / 0 | 6.85s | 内置输入处理完成；输出 2 条匹配结果。 |
| [cn.doitedu.rtmk.demo9.groovy.TestDiaoyong](../rt_rule_mk/src/main/java/cn/doitedu/rtmk/demo9/groovy/TestDiaoyong.java) | 条件不足 / 1 | 0.77s | FileNotFoundException: d:/a.groovy；需提供实现 HelloInterface 的真实 Groovy 脚本，并将源码路径改为可读位置。 |
| [cn.doitedu.test.EventsMoni](../rt_rule_mk/src/test/java/cn/doitedu/test/EventsMoni.java) | 条件不足 / 1 | 1.78s | Kafka bootstrap 地址不可解析；需 K。 |
| [cn.doitedu.test.HbaseProfileTagsMoni](../rt_rule_mk/src/test/java/cn/doitedu/test/HbaseProfileTagsMoni.java) | 条件不足 / 143 | 2.3s | ZooKeeper/HBase 连接 UnknownHostException: doitedu；需 H。 |
| [cn.doitedu.test.HistoryConditionValueMoni](../rt_rule_mk/src/test/java/cn/doitedu/test/HistoryConditionValueMoni.java) | 条件不足 / 1 | 0.76s | JedisConnectionException，根因为 UnknownHostException: doitedu；需 R。 |

### redis_demo

| main（源码） | 结果 / 退出码 | 耗时 | 实际证据及执行条件 |
| --- | --- | --- | --- |
| [cn.doitedu.jedis.RedisDemo](../redis_demo/src/main/java/cn/doitedu/jedis/RedisDemo.java) | 条件不足 / 1 | 5.58s | JedisConnectionException，根因为 UnknownHostException: doitedu；需 R。 |

### batch_jobs

| main（源码） | 结果 / 退出码 | 耗时 | 实际证据及执行条件 |
| --- | --- | --- | --- |
| [cn.doitedu.batch.jobs.DataLoadJob01_GeoAreaInfo2Hbase](../batch_jobs/src/main/java/cn/doitedu/batch/jobs/DataLoadJob01_GeoAreaInfo2Hbase.scala) | 条件不足 / 1 | 10.73s | Spark 已启动；读取 MySQL 时 CommunicationsException / UnknownHostException: doitedu；需 M、H。 |

## 缺失条件与恢复执行方式

本机当前无法解析 `doitedu`。它是源码中的部署主机名，不是需要特殊支持的产品。应按实际部署拓扑，给各连接配置可达域名或 IP；只有服务确实同机时才适合统一 hosts 映射。Kafka advertised.listeners、HBase RegionServer 通告地址和 Doris 重定向到的 BE 地址，也必须能从运行机器访问。仅替换一个域名不能证明业务链路可用。

| 条件 | 需要准备的内容 |
| --- | --- |
| K | 可达 Kafka（源码常用 doitedu:9092），对应输入/输出 topic、正确 JSON/事件结构和真实测试数据；例如 WindowTest 使用 window-test / window-res。 |
| M | 可达 MySQL（常用 doitedu:3306）、源码指定库表及账号权限；CDC 还需启用 ROW binlog、合适的 server-id 和复制权限。 |
| H | 可达 ZooKeeper（常用 2181）和 HBase RegionServer RPC，源码指定 namespace、table、column family 与画像/维度数据。 |
| R | 可达 Redis（常用 doitedu:6379）及规则所需 key/历史值。 |
| D | 可达 Doris FE（源码 HTTP 8030）和 BE Stream Load 地址，dws 等库中的目标表、schema、账号权限；本次 schema 请求失败，未发生成功写入验收。 |
| A | 可达 http://doitedu:8081/api/post/simwords，响应格式符合调用代码。 |
| S | doitedu:9090 上提供 Socket 文本事件输入。 |
| F | 可达 hdfs://doitedu:8020/ckpt，Hadoop 配置、目录权限及可用 NameNode/DataNode；WindowTest 还需要 Kafka 输入输出与 stop/drain 操作。 |

CSV 和 Groovy 文件未随本次执行补造，源码中的 Windows 输入路径也未替换为虚构数据。Groovy Local 使用硬编码规则，并没有动态编译 Groovy 脚本；不能将它的成功当作 TestDiaoyong 或原动态规则链路通过。

## 如何重跑

在仓库根目录执行，先完成编译和 classpath 生成，再跑 main；不要在执行审计的同时重编译同一模块，以免 target/classes 变化造成临时类加载失败。

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
mvn -B -ntp test-compile
mvn -B -ntp -q dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/main-audit-classpath.txt

# 执行所有 42 个 main，每个独立 JVM；默认结果目录 .artifacts/main-audit/after
python3 scripts/audit-mains.py --timeout 45

# 单独执行（默认会先编译对应模块）
bash scripts/run-main.sh rt_rule_mk cn.doitedu.rtmk.demo8.RuleEngineRecoveryLocal
bash scripts/run-main.sh rt_rule_mk cn.doitedu.rtmk.demo8.RuleEngineRecovery
```

Linux 上将 JAVA_HOME 设为实际 JDK 17 路径，其余命令相同。IDE 运行时选择 JDK 17，将 [scripts/java17.options](../scripts/java17.options) 中非注释行复制到 VM options；只升级 Maven 依赖而不配置模块参数，不能保证消除 JDK 17 反射访问异常。这些参数取自 [Flink 1.19.3 官方发行配置的 env.java.opts.all](https://github.com/apache/flink/blob/release-1.19.3/flink-dist/src/main/resources/config.yaml)。

checkpoint 默认使用 `${java.io.tmpdir}/doit36-checkpoints`；可通过 IDE VM options 或 `JAVA_TOOL_OPTIONS` 传入 `-Dflink.checkpoints.dir=file:///实际持久目录`。WindowTest 保留 HDFS 条件，各 Local 示例原有 `/tmp/ckpt` 不变。

执行脚本的批次完成不等于所有 main 通过，应读取 results.json 中每项退出码、timed_out 和 environment_error。外部依赖首次出现明确错误后脚本会停止对应 JVM；未触达的后续步骤仍需环境就绪后继续验证。

## 日志、验证与范围

本次每个程序的最终日志路径与耗时保存在本机 `.artifacts/main-audit/results.json`。分批日志在 final-rules、final-realtime、final-other；Spark 最终结果使用 final-batch；Groovy Local 重跑使用 final-groovy-retry；WindowTest 补齐客户端后的结果使用 final-window-retry。早期失败日志保留，不作为最终状态。Groovy Local 首轮曾出现 Calculator 类加载失败并超时，完整 classpath 下重跑正常退出，未为此改写示例业务代码。

升级前的 RuleEngineRecovery 复现日志位于 `.artifacts/main-audit/before/RuleEngineRecovery.log`。本地日志目录已加入 .gitignore，不会将依赖缓存与大量运行日志加入 Git；本 Markdown 保存全部 42 项结论和条件。

编译验证：四模块 `mvn -B -ntp test-compile` 通过。另检查脚本语法和 Git diff 空白。没有调用自动代码审查，没有恢复 GitHub Actions 跟踪，也没有修改或重新部署 k3s 集群。现有 Flink 1.16 镜像不能直接视为本次 1.19.3 运行环境；部署前需要同步运行时版本，状态兼容与恢复另行验证。
