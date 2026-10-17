# k3s 分支合并与原程序执行验收

执行日期：2026-10-17，macOS 12.7.6 / Intel / JDK 17.0.18。

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

在 Mac 上用 JDK 17 启动原 `cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim`，连接集群真实 Kafka/HBase/Doris。

为避免与现有作业竞争 consumer group 或重复写入，使用独立输入 `main-audit-input`、输出 `main-audit-output` 和目标表 `dwd.mall_events_wide_main_validation`。没有用 Local 示例替代业务 main。

- job id：`a367f01e1370907240b5758032d9bd2b`。
- 唯一批次：`m1792226477267`。
- 输入 5 条、Kafka 输出 5 条、Doris 入库 5 行。
- `user_id=10001`、`gps_region=Haidian`、`page_type=product` 等 HBase 维度一致。
- 完成 6 个 checkpoint，失败 0。
- 作业属于无界流；验证结束后主动停止，不把强制停止退出码当作“main 正常退出”。

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

RedisDemo、Scala 批作业使用相应环境变量；Scala 数据库仍按源码的实时数仓业务配置。Java 程序未设置参数时保留课程默认值，不能期望本机自动解析 doitedu。

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
bash scripts/run-main.sh rt_rule_mk cn.doitedu.rtmk.demo8.RuleEngineRecovery

# 或用私有 JSON 连接配置（值须为字符串）；不会执行其中的 shell 内容。
MAIN_ENV_JSON=.artifacts/main-audit/k3s/runtime-env.json \
  bash scripts/run-main.sh rt_rule_mk cn.doitedu.rtmk.demo8.RuleEngineRecovery
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
  cn.doitedu.rtmk.demo9.groovy.TestDiaoyong examples/main-inputs/hello.groovy
bash scripts/run-main.sh realtime_dw cn.doitedu.rtdw.utils.DimRecProcess \
  examples/main-inputs/dim_rec.csv .artifacts/dim-rec-hbase.sh
```

## 运行时与日志边界

镜像构建脚本默认基础镜像与新部署模板已对齐 Flink 1.19.3 / v1_19。现有 `flink-business.yaml`、`flink-standalone.yaml` 仍是已验收的 1.16 部署记录；不能把新依赖 JAR 直接覆盖进旧镜像。本文没有声称已将运行中的 k3s 作业升级为 1.19，也没有触发 GitHub Actions 或 push。

本地证据目录为 `.artifacts/main-audit/k3s/`，包括原集群复验日志、ETL main 验收 JSON、恢复前错误日志、修复后的两次运行日志、savepoint 和各 main 执行记录。凭据 JSON 不提交。

已重跑的有限输入程序：9 个规则 Local、PVUV Local、DimRecProcess、TestDiaoyong、HbaseProfileTagsMoni、HistoryConditionValueMoni、RedisDemo，共 **15 个全部退出码 0**。后两个 Redis 程序使用 DB 15；CSV/Groovy 工具使用仓库 examples/main-inputs 中明确标注的示例输入。
