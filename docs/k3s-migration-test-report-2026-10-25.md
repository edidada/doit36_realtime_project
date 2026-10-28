# K3s Namespace 迁移与功能测试报告

**执行日期：** 2026-10-25（Asia/Shanghai）  
**目标：** 移除错误命名的 `doit` Namespace；将 Kafka/HBase/Doris 归入 `middleware`，将 Flink 业务作业归入 `realtime-dw`，并在迁移后验证本仓库代码。

## 结果

迁移完成，`doit` Namespace 已删除。

| 原对象 | 目标 Namespace | 结果 |
| --- | --- | --- |
| `business-kafka` | `middleware` 的既有 `kafka` | 已停掉旧 broker；业务 Topic 已在共享 Kafka 建立。旧 5Gi 本地卷保留为恢复证据。 |
| `business-hbase` | `middleware` | 已重建并绑定原 5Gi `local-path` PV；Pod `1/1 Running`。 |
| `business-doris-fe` / `business-doris-be` | `middleware` | 已重建并分别绑定原 3Gi / 8Gi PV；均为 `1/1 Running`。 |
| `realtime-dw-etl-01` FlinkDeployment | `realtime-dw` | 已重建并绑定原 50Gi Flink state PV；JobManager、TaskManager 均 `1/1 Ready`，作业 `RUNNING/STABLE`。 |

旧 Kafka PV `pvc-30fd8a13-29ac-4fcd-9cbc-978fa25db35f` 当前是 `Released + Retain`。它没有 Pod 或 Namespace 引用，不消耗 CPU/内存；为保留回滚证据，暂不删除其 5Gi 本地数据。确认不再需要历史恢复后，单独删除该 PV 才会释放磁盘。

## 为可迁移运行所做的代码修复

`EtlJob01_UserEventsLogCommonDim` 已改为：

1. `KAFKA_BOOTSTRAP_SERVERS`、`HBASE_ZOOKEEPER_QUORUM`、`DORIS_FENODES`、Doris 用户名/密码和 `FLINK_CHECKPOINT_URI` 均由环境变量优先注入，保留课程环境的 `doitedu` 默认值。
2. 两个 sink 改为一个 `StatementSet.execute()` 提交。原实现先后两次 `executeSql(INSERT ...)`，Flink 1.16 会报“同一环境不能多次 execute”，导致 JobManager 退出。
3. Flink Pod 的启动探针改为适用于 JobManager 和 TaskManager 的本地目录探针；原 HTTP 8081 探针会错误用于 TaskManager。

Maven 聚合工程的 artifactId 与所有子模块 parent 引用也由 `doit36_realtime_project` 更名为 `realtime-dw`。本报告记录的是 2026-10-25 的运行结果；2026-10-28 已将课程旧包名统一迁移为 `com.test.bigdata`，文档中的类名和源码链接随之更新，不代表已在集群重新部署验证。

生产作业实际使用的地址为：

```text
Kafka: kafka.middleware.svc.cluster.local:9092
HBase: business-hbase.middleware.svc.cluster.local:2181
Doris: business-doris-fe.middleware.svc.cluster.local:8030
Flink: realtime-dw / realtime-dw-etl-01
```

## 测试证据

| 项目 | 命令/方法 | 结果 |
| --- | --- | --- |
| 全仓库 Maven 测试 | `mvn -B -ntp test` | 5 个模块均为 `BUILD SUCCESS`；现有 Surefire 测试共发现 0 个实际断言测试。 |
| `realtime_dw` 构建 | `mvn -B -ntp -pl realtime_dw -am -DskipTests package` | 成功，JDK 17。 |
| HBase 连通性 | 迁移后以 HBase shell 校验课程维表可打开 | 通过。 |
| Doris 连通性 | 以 `doit_etl` 读取 `dwd.mall_events_wide` | 通过。 |
| Flink 作业状态 | Flink REST `/jobs/overview` | `RUNNING`，一个运行任务。 |
| Checkpoint | JobManager 日志 | 已连续完成 checkpoint 1–11。 |
| Kafka → Flink → Kafka | 向 `mall-events-log` 写入唯一 `migrateuser` 事件 | `dwd-events-log` offset 从 0 增至 1。 |
| Kafka → Flink → Doris | 查询测试事件 | 查询到 `migrateuser / mw-migrate-20261025- / mwtest001`。`session_id` 截断是 Doris 表字段 `varchar(20)` 的既有定义。 |

现有 `Java CI with Maven` 工作流已使用 Temurin JDK 17；通过 `gh` 查到的最近三次 Maven CI 都成功，但它们对应历史提交，不能替代本次未提交变更的远端 CI。最近失败的其他工作流是旧 Semgrep/K3s 镜像运行，也不对应本次变更。

## 资源结论

本次实际释放的是旧 `doit/business-kafka` 的运行资源：其声明请求为 **250m CPU、1Gi 内存**，迁移前采样约 **9m CPU、510Mi 内存**。HBase/Doris 仅改变归属 Namespace，保留同一 PV、同一节点和同一资源请求，因此不应虚报为硬件节省。

## 后续事项

1. 已将不含密钥、PVC 和二进制 Jar 的运行清单纳入 [`deploy/k8s`](../deploy/k8s/README.md)；操作前快照仍保留于 `k3s-master:/root/realtime-dw-migration-20261025`。后续应把 Maven 产物生成 `business-etl-job` ConfigMap 的过程纳入发布流水线，不能长期手工维护。
2. 课程的完整 HBase 初始化数据、Doris 建表脚本和 Topic 数据不在本仓库。此次端到端测试只使用一条隔离测试事件，不代表全部 41 个示例作业均具备数据前置条件。
3. 为防止测试数据持续留存，可在确认后删除 `dwd.mall_events_wide` 中 `username = 'migrateuser'` 的行和两个测试 Topic 消息；当前保留作为本报告的可核验证据。
4. 回收旧 Kafka PV 前，应确认无需从旧 5Gi 数据卷恢复；删除 PV 会删除本地数据目录。
