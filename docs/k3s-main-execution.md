# k3s 分支合并与原程序执行验收

执行日期：2026-10-17，macOS 12.7.6 / Intel / JDK 17.0.18。

本次共核对 **43 个 main：40 个执行成功，3 个缺少外部服务**。40 个包括 17 个有限输入程序和 23 个无界流程序；无界流通过 checkpoint、savepoint 和正常退出验证。完整业务数据验收范围与限制逐项列在下文。全模块 `mvn -B -ntp test-compile` 已通过。

## 合并与 Markdown 状态

先将 master 的 `cbcc4c0` 合入 k3s，merge commit 为 **487bf85**，两个父提交分别为 `1f84587`、`cbcc4c0`。没有遗留 Git 冲突。

四处冲突的处理：

- `.gitignore` 保留 `.artifacts/`、`deploy/k3s/.image-build/`、`deploy/k3s/output/`，本地 JAR 和镜像 tar 不纳入 Git。
- 根 POM 同时保留 k3s 的 Lombok/annotation processor 配置与 master 的 Flink 1.19.3 版本配置。
- batch_jobs 使用 Scala 2.12.21、Spark 3.5.8、scala-maven-plugin 4.9.2。
- 原 ETL01 保留 `FLINK_CHECKPOINT_URI` 部署参数，同时兼容 `flink.checkpoints.dir` 和系统临时目录。

`main-execution-status.md` 随 merge 纳入 k3s。`rule-engine-recovery-dns-troubleshooting.md`、`k3s-three-improvements-deployment-test.md` 在合并前已属于 k3s 的已跟踪文件，没有冲突。前者描述最初 DNS 故障；master 执行报告中的“42 个 main、10 个通过”是当时环境的历史结果，本次进展见本文。k3s 另有 StandaloneEventsJob，共 43 个 main。

## 已完成的真实验收

### 集群原有 ETL（Flink 1.16.1）

复跑已有 `business/verify-business.py`：输入 5 条，Kafka 输出 5 条，Doris 查询 5 行，HBase 用户、地域和页面维度正确。成功 checkpoint 从 606 增至 612，失败数保持 6，没有新增失败。

这是已有集群镜像的复验，不代表集群已升级至 Flink 1.19。

### 升级后的原 ETL01 main（Flink 1.19.3）

在 Mac 上用 JDK 17 启动原 `com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim`，连接集群真实 Kafka/HBase/Doris。

为避免与现有作业竞争 consumer group 或重复写入，使用独立输入 `main-audit-input`、输出 `main-audit-output` 和目标表 `dwd.mall_events_wide_main_validation`。没有用 Local 示例替代业务 main。

- job id：`a367f01e1370907240b5758032d9bd2b`。
- 唯一批次：`m1792226477267`。
- 输入 5 条、Kafka 输出 5 条、Doris 入库 5 行。
- `user_id=10001`、`gps_region=Haidian`、`page_type=product` 等 HBase 维度一致。
- 完成 6 个 checkpoint，失败 0。
- 首次验收在验证结束后主动停止；最终又完成了一次 savepoint 正常退出验收，见下段。

最终复验 job `b3e92f900298cdf69f9f7c4fbce51d03`、批次 `m1792229774769`：输入 5 / Kafka 5 / Doris 5，维度一致，完成 6 个 checkpoint。启动阶段 task 未就绪中止 2 次，数据验收前后失败数均为 2，没有新增失败；保存 savepoint 后 main **退出码 0**。证据为 `etl-main-acceptance.json`、`verify-local-etl-final.log`。

### 原 RuleEngineRecovery 的运行与状态恢复

连接真实 Kafka、MySQL CDC、Redis。MySQL 来自已有 middleware 命名空间，版本 8.0.46，已开启 `log_bin=ON`、`binlog_format=ROW`。MySQL、Redis 通过 SSH 隧道访问，不修改全局 hosts，也没有新建中间件 Deployment。

隔离数据使用 `doit36_main_audit.rule_meta_doit36`、Kafka `main-audit-rules`，Redis 使用 DB 15。凭据仅保存在本地忽略目录和进程环境，不写进 Git。

连通后发现了 DNS 之外的实际代码错误：广播状态中保存了包含 Jedis 连接和 Flink ValueState 句柄的计算器，checkpoint 尝试序列化运行时对象，报 `KryoException` / `InaccessibleObjectException`，作业反复重启。增加 `--add-opens jdk.internal.ref` 不能解决状态模型的问题。

修复方式：广播状态只存规则元数据；计算器使用进程内缓存，在 keyed 事件路径按元数据重建客户端和 State 句柄；为不同规则使用不同的 keyed state 名称；规则下线/删除时移除状态并关闭缓存客户端。新增 `FLINK_SAVEPOINT_PATH` 支持显式恢复。

恢复验收包含：先发送一条画像事件和一条触发事件，观察规则命中；完成 checkpoint，stop-with-savepoint 后正常退出；从 savepoint 启动新的 main，只发送触发事件，验证之前的用户计数与广播规则均被恢复；再次生成 savepoint 并正常退出。详细最终数据记录在本地 `recovery-acceptance.json`。

本修复改变了此前错误的计算器序列化方式和 keyed state 命名。旧版本产生的状态不能直接视为兼容；本次验证的是修复后程序生成并恢复的 savepoint。

## 连接配置

realtime_dw 和 rt_rule_mk 的入口、规则计算器和测试 main 支持环境变量（也可用同名 JVM 系统属性）：

| 配置 | 作用 |
| --- | --- |
| KAFKA_BOOTSTRAP_SERVERS | 实际 broker 地址；此环境为 192.168.31.211:9092 |
| HBASE_ZOOKEEPER_QUORUM | 此环境为 192.168.31.211:2181 |
| MYSQL_HOST / MYSQL_PORT / MYSQL_USERNAME / MYSQL_PASSWORD | CDC/JDBC 连接；Mac 隧道为 127.0.0.1:13306 |
| MYSQL_RULE_DATABASE | 规则元数据库，默认 rtmk；验收为 doit36_main_audit |
| REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DATABASE | Mac 隧道为 127.0.0.1:16379，验收选择 DB 15 |
| DORIS_FENODES / DORIS_USERNAME / DORIS_PASSWORD | 此环境 FE 为 192.168.31.251:8030 |
| SIMWORDS_URL | 搜索词语义服务 URL |
| SOCKET_HOST / SOCKET_PORT | EtlAsync 的文本输入 |
| FLINK_CHECKPOINT_URI | 部分示例的 checkpoint 地址；WindowTest 可覆盖默认 HDFS 条件 |
| STANDALONE_OUTPUT_URI | StandaloneEventsJob 的输出目录；Mac 需设置可写 file URI |
| FLINK_SAVEPOINT_PATH | RuleEngineRecovery 的恢复路径 |

RuleEngineRecovery 支持 `KAFKA_INPUT_TOPIC`、`KAFKA_GROUP_ID`；ETL01 支持 `KAFKA_INPUT_TOPIC`、`KAFKA_OUTPUT_TOPIC`、`KAFKA_GROUP_ID`、`DORIS_TABLE_IDENTIFIER`，用于隔离验收与已有业务。

RedisDemo、Scala 批作业使用相应环境变量；Scala 与业务 CDC/JDBC 可通过 `MYSQL_DATABASE` 指定验收库。Java 程序未设置参数时保留课程默认值，不能期望本机自动解析 doitedu。

## 复现

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
mvn -B -ntp test-compile
mvn -B -ntp -q dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/main-audit-classpath.txt

# 保持该 SSH 进程运行；目标 ClusterIP 以当前集群 Service 为准。
ssh -N -o ExitOnForwardFailure=yes \
  -L 13306:10.43.251.207:3306 \
  -L 16379:10.43.215.114:6379 root@192.168.31.77
```

另一个终端设置上表参数与实际凭据后：

```bash
bash scripts/run-main.sh rt_rule_mk com.test.bigdata.rtmk.demo8.RuleEngineRecovery

# 或用私有 JSON 连接配置（值须为字符串）；不会执行其中的 shell 内容。
MAIN_ENV_JSON=.artifacts/main-audit/k3s/runtime-env.json \
  bash scripts/run-main.sh rt_rule_mk com.test.bigdata.rtmk.demo8.RuleEngineRecovery
```

集成验收脚本为 `scripts/verify-rule-recovery.py`，初始化数据使用 `deploy/k3s/business/init-rule-validation.sql`。需先创建 Kafka `main-audit-rules`，在 MySQL 中执行初始化 SQL，并设置连接环境变量；也可使用权限为 0600 的私有 JSON 环境文件，文件应放在 `.artifacts/` 下。

```bash
python3 scripts/verify-rule-recovery.py \
  --env-file .artifacts/main-audit/k3s/runtime-env.json
```

该脚本运行原 main 两次，检查命中、checkpoint、savepoint 恢复和正常退出，不以进程启动或等待输入作为通过依据。

输入文件示例（只验证相应工具 main，不代表业务数据验收）：

```bash
bash scripts/run-main.sh rt_rule_mk \
  com.test.bigdata.rtmk.demo9.groovy.TestDiaoyong examples/main-inputs/hello.groovy
bash scripts/run-main.sh realtime_dw com.test.bigdata.rtdw.utils.DimRecProcess \
  examples/main-inputs/dim_rec.csv .artifacts/dim-rec-hbase.sh
```

## 运行时与日志边界

镜像构建脚本默认基础镜像与新部署模板已对齐 Flink 1.19.3 / v1_19。现有 `flink-business.yaml`、`flink-standalone.yaml` 仍是已验收的 1.16 部署记录；不能把新依赖 JAR 直接覆盖进旧镜像。本文没有声称已将运行中的 k3s 作业升级为 1.19，也没有触发 GitHub Actions 或 push。

本地证据目录为 `.artifacts/main-audit/k3s/`，包括原集群复验日志、ETL main 验收 JSON、恢复前错误日志、修复后的两次运行日志、savepoint 和各 main 执行记录。凭据 JSON 不提交。

已重跑的有限输入程序：9 个规则 Local、PVUV Local、DimRecProcess、TestDiaoyong、HbaseProfileTagsMoni、HistoryConditionValueMoni、RedisDemo，共 **15 个全部退出码 0**。后两个 Redis 程序使用 DB 15；CSV/Groovy 工具使用仓库 examples/main-inputs 中明确标注的示例输入。


## 后续修复与流作业验收方法

补齐 `flink-connector-files:1.19.3`，解决 StandaloneEventsJob 的 filesystem factory 缺失。MySQL CDC、Spark/JDBC 支持 `MYSQL_DATABASE`，验收库的基础表见 `deploy/k3s/business/init-main-validation.sql`。

ETL03、ETL05、ETL06 同步/异步版本原来在 SQL INSERT 已提交后又调用 `env.execute()`，产生两个本地集群/作业，停止其中一个后 main 仍不退出。现在使用 INSERT 返回值的 `.await()` 等待单个作业。

Groovy demo9 与 demo8 一样，只把元数据写入广播状态；GroovyClassLoader、Redis 客户端和 State 句柄在事件路径重建。实际验收消费 Kafka `main-audit-rule-results` 中指定用户的 ruleId=900017 命中，完成 3 次 checkpoint、0 失败，并生成 savepoint 后以 0 正常退出。输出 topic 支持 `KAFKA_OUTPUT_TOPIC`。

`verify-flink-mains.py` 用于验证无界 main 的运行生命周期，必须先完成编译和依赖 classpath 生成。示例：

```bash
python3 scripts/verify-flink-mains.py \
  --env-file .artifacts/main-audit/k3s/runtime-env.json \
  --main com.test.bigdata.rtdw.deployment.StandaloneEventsJob
```

判定条件：所有 task RUNNING、至少 2 个成功 checkpoint、stop-with-savepoint 成功、main 退出码为 0。首次 checkpoint 可能早于 task 部署完成而中止，记录中保留失败计数及原因；这种启动阶段的 `Not all required tasks are currently running` 单独注明，不冒充 0 失败。其他 checkpoint 错误仍判为失败。超时或 SIGTERM 退出不算通过。运行生命周期通过不代表聚合口径、窗口边界和全部业务输出已验收。

`python3 scripts/main-validation-sql.py` 输出 MySQL 隔离验收 SQL，其中 Groovy 代码读取仓库实际源文件。将输出通过 stdin 送给 master 上 middleware/mysql 的客户端执行；凭据使用该容器已有的 `MYSQL_ROOT_PASSWORD`。如果之前已有不含 `model_calculator_code` 的验收表，先为该表补充 `MEDIUMTEXT` 列。

demo6 仅支持模型 1–4，需设置 `MYSQL_RULE_DATABASE=doit36_main_audit_demo6`；demo7/8/9 使用 `doit36_main_audit` 中的模型 5。不能将模型 5 元数据直接交给 demo6。

HBase 实际写入核对：`dim_user_info/main-audit-user` 的手机号、城市与 MySQL 一致；`dim_video_info` 的 BIGINT 行键 990017 对应 `main-audit-video`；Spark 写入 `dim_geo_area` 行键 `s3wtw`，省份为 `AuditProvince`。


异步搜索与 EtlAsync 的 RichAsyncFunction 增加 HTTP client 的 `close()`，避免作业停止后 I/O reactor 线程阻止 JVM 退出。两个独立 HTTP 示例也在成功/失败路径关闭客户端。这些资源修复不提供缺失的语义服务；没有真实接口响应时，不能声称搜索词业务链路已通过。

Standalone 最终验证得到 **2 个已提交文件、30 条 JSON**，`event_type=page_view`、`channel=WEB`；完成 4 次 checkpoint、0 失败，savepoint 后退出码 0。详见 `standalone-committed/file-acceptance.json`。

独立 HTTP 两个 main 已使用内网 IP 重试，均明确以 1 退出并报告连接拒绝，没有异步线程挂住；EtlAsync 因 Socket 9090 拒绝连接持续重试，15 秒后由验收程序停止，退出码 143，计为未通过。相应记录见 `blocked-final/results.json`；未将这些失败当作通过。

## 43 个 main 最终执行表

成功/失败列为 checkpoint 次数；启动阶段 task 尚未就绪的中止保留在失败列。有限输入程序无需 checkpoint。生命周期通过不等同于每个业务指标已验收。所有名称均链接到实际源码。

| main | 结果 | 退出码 | checkpoint 成功/失败 |
| --- | --- | --- | --- |
| [rtdw.data_etl.EtlJob01_UserEventsLogCommonDim](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim.java) | 业务链路通过，输入/Kafka/Doris 各 5 | 0 | 6/2 |
| [rtdw.data_etl.EtlJob02_MallTfcAg01](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob02_MallTfcAg01.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_etl.EtlJob03_PageAccessTimeLong](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob03_PageAccessTimeLong.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_etl.EtlJob04_InteractiveEventsOverview](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob04_InteractiveEventsOverview.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_etl.EtlJob05_VideoEventsAgg](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob05_VideoEventsAgg.java) | 运行生命周期通过 | 0 | 2/5 |
| [rtdw.data_etl.EtlJob06_SearchEventsAnalyse](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob06_SearchEventsAnalyse.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_etl.EtlJob06_SearchEventsAnalyse_Async](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob06_SearchEventsAnalyse_Async.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_etl.EtlJob07_AdRequestSessionProcess](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_etl/EtlJob07_AdRequestSessionProcess.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.data_sync.SyncJob01_UserInfo2Hbase](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_sync/SyncJob01_UserInfo2Hbase.java) | 运行生命周期通过 | 0 | 2/1 |
| [rtdw.data_sync.SyncJob02_VideoInfo2Hbase](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_sync/SyncJob02_VideoInfo2Hbase.java) | 运行生命周期通过 | 0 | 2/1 |
| [rtdw.data_sync.SyncJob03_AdRequestFeatures2Hbase](../realtime_dw/src/main/java/com/test/bigdata/rtdw/data_sync/SyncJob03_AdRequestFeatures2Hbase.java) | 运行生命周期通过 | 0 | 2/3 |
| [rtdw.deployment.StandaloneEventsJob](../realtime_dw/src/main/java/com/test/bigdata/rtdw/deployment/StandaloneEventsJob.java) | 运行生命周期通过 | 0 | 4/0 |
| [rtdw.rt_dashboard.Promotion01_PVUV](../realtime_dw/src/main/java/com/test/bigdata/rtdw/rt_dashboard/Promotion01_PVUV.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtdw.rt_dashboard.Promotion01_PVUVLocal](../realtime_dw/src/main/java/com/test/bigdata/rtdw/rt_dashboard/Promotion01_PVUVLocal.java) | 通过，有限输入 | 0 | — |
| [rtdw.utils.DimRecProcess](../realtime_dw/src/main/java/com/test/bigdata/rtdw/utils/DimRecProcess.java) | 通过，有限输入 | 0 | — |
| [rtdw.utils.HttpRequestDemo](../realtime_dw/src/main/java/com/test/bigdata/rtdw/utils/HttpRequestDemo.java) | 条件不足：缺少 SIMWORDS_URL 对应的语义 HTTP 服务，仓库未包含服务端 | 未通过 | — |
| [EtlAsync](../realtime_dw/src/test/java/com/test/bigdata/EtlAsync.java) | 条件不足：缺少 SOCKET_HOST:SOCKET_PORT 文本输入及语义 HTTP 服务 | 未通过 | — |
| [Http](../realtime_dw/src/test/java/com/test/bigdata/Http.java) | 条件不足：缺少语义 HTTP 服务；已修复异步客户端未关闭 | 未通过 | — |
| [WindowTest](../realtime_dw/src/test/java/com/test/bigdata/WindowTest.java) | 运行生命周期通过 | 0 | 3/1 |
| [rtmk.demo1.TargetProductCartMonitor](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo1/TargetProductCartMonitor.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtmk.demo1.TargetProductCartMonitorLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo1/TargetProductCartMonitorLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo2.MultiRuleEngine](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo2/MultiRuleEngine.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtmk.demo2.MultiRuleEngineLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo2/MultiRuleEngineLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo3.RuleCalculatorPattern](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo3/RuleCalculatorPattern.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtmk.demo3.RuleCalculatorPatternLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo3/RuleCalculatorPatternLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo4.ModelBasedRuleEngine](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo4/ModelBasedRuleEngine.java) | 运行生命周期通过 | 0 | 2/1 |
| [rtmk.demo4.ModelBasedRuleEngineLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo4/ModelBasedRuleEngineLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo5.BitmapOptimizedRuleEngine](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo5/BitmapOptimizedRuleEngine.java) | 运行生命周期通过 | 0 | 2/2 |
| [rtmk.demo5.BitmapOptimizedRuleEngineLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo5/BitmapOptimizedRuleEngineLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo6.DynamicRuleManagement](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo6/DynamicRuleManagement.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtmk.demo6.DynamicRuleManagementLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo6/DynamicRuleManagementLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo7.DynamicRuleManagementV2](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo7/DynamicRuleManagementV2.java) | 运行生命周期通过 | 0 | 2/0 |
| [rtmk.demo7.DynamicRuleManagementV2Local](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo7/DynamicRuleManagementV2Local.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo8.RuleEngineRecovery](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo8/RuleEngineRecovery.java) | 通过，命中及 savepoint 恢复 | 0 / 0 | 2+2 / 0 |
| [rtmk.demo8.RuleEngineRecoveryLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo8/RuleEngineRecoveryLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo9.DynamicRuleWithGroovy](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo9/DynamicRuleWithGroovy.java) | 运行生命周期通过 | 0 | 3/0 |
| [rtmk.demo9.DynamicRuleWithGroovyLocal](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo9/DynamicRuleWithGroovyLocal.java) | 通过，有限输入 | 0 | — |
| [rtmk.demo9.groovy.TestDiaoyong](../rt_rule_mk/src/main/java/com/test/bigdata/rtmk/demo9/groovy/TestDiaoyong.java) | 通过，有限输入 | 0 | — |
| [test.EventsMoni](../rt_rule_mk/src/test/java/com/test/bigdata/test/EventsMoni.java) | 通过，有限批次、Kafka ACK | 0 | — |
| [test.HbaseProfileTagsMoni](../rt_rule_mk/src/test/java/com/test/bigdata/test/HbaseProfileTagsMoni.java) | 通过，有限输入 | 0 | — |
| [test.HistoryConditionValueMoni](../rt_rule_mk/src/test/java/com/test/bigdata/test/HistoryConditionValueMoni.java) | 通过，有限输入 | 0 | — |
| [jedis.RedisDemo](../redis_demo/src/main/java/com/test/bigdata/jedis/RedisDemo.java) | 通过，有限输入 | 0 | — |
| [batch.jobs.DataLoadJob01_GeoAreaInfo2Hbase](../batch_jobs/src/main/java/com/test/bigdata/batch/jobs/DataLoadJob01_GeoAreaInfo2Hbase.scala) | 通过，HBase 写入已核对 | 0 | — |

明细证据：`.artifacts/main-audit/k3s/lifecycle-*/results.json` 与对应 main 日志；有限输入见 `finite/results.json`，Groovy Kafka 命中见 `lifecycle-groovy-final/groovy-kafka-result.log`。业务 ETL、恢复验收见前文。旧的超时/错误日志保留，不作为通过证据。

搜索 ETL06 同步/异步版本的生命周期验证不包含语义接口响应与搜索聚合结果。ETL02/03/04/05/07、PVUV、WindowTest 也未逐项核对所有窗口和业务口径。HTTP 服务不是把 doitedu 换成一个 IP 就能补齐，当前集群没有已确认的 simwords 实现；不会把 Mock 输出称为真实语义业务验收。
