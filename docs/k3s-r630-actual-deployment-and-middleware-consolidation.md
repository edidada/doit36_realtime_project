# R630 K3s 实际部署与中间件收敛

> 盘点时间：2026-10-25（Asia/Shanghai）。
>
> 信息来源：本仓库源码、`D:\develops\git\github\r630-proxmox8` 的集群与命名空间文档，以及对 `k3s-master` 的只读 `kubectl` 实测。本文描述的是现场，不把规划当成已部署事实。

> **实施更新（2026-10-25）：** 本文的迁移方案已经执行：`doit` 已删除，Kafka/HBase/Doris 位于 `middleware`，Flink 作业位于 `realtime-dw`。实际过程与功能验收见 [K3s Namespace 迁移与功能测试报告](k3s-migration-test-report-2026-10-25.md)，脱敏运行清单见 [`deploy/k8s`](../deploy/k8s/README.md)。其余“未执行”措辞仅保留为迁移前风险记录。

## 结论先行

本项目有一条运行中的 K3s 实时链路：`realtime-dw` 中的 Flink 作业使用 `middleware` 中的 Kafka、HBase 与 Doris。脱敏后的运行清单已进入 `deploy/k8s`；密钥、PV/PVC 绑定以及二进制 Jar 仍必须由部署前置步骤提供，不能仅凭 Git 直接重建。

“把 Pod 改到另一个 Namespace”只是重建资源的命名空间归属，**不会减少 CPU、内存或磁盘**。对于带 PVC 的工作负载，PVC 还不能通过修改 `metadata.namespace` 迁移。本次实际释放的常驻资源来自停掉 `doit/business-kafka` 并复用 `middleware/kafka`；HBase 与 Doris 是带 Retain PV 的原地重绑，未被计为硬件节省。

不要对 HBase/Doris 执行 `kubectl delete -n doit ...` 或“导出 YAML、替换 namespace、apply”的操作。它们使用单副本、本地 `local-path` 持久卷与 `hostNetwork`，这会造成不可验证的数据迁移或直接中断服务。

## 实测现场

### `doit`：本项目实时链路

| 工作负载 | 运行形态 | 观测到的 CPU / 内存 | 请求值 | 持久数据与位置 |
| --- | --- | --- | --- | --- |
| `business-kafka` | 单副本 Kafka 3.9.1，固定 `k3s-worker1`，`hostNetwork: true` | 9m / 510Mi | 250m / 1Gi | 5Gi `local-path` PVC，worker1 |
| `business-hbase` | 单副本 HBase，固定 `k3s-worker1`，`hostNetwork: true` | 5m / 361Mi | 500m / 1536Mi | 5Gi `local-path` PVC，worker1 |
| `business-doris-fe` | 单副本 Doris FE 2.1.8，固定 `k3s-worker2`，`hostNetwork: true` | 189m / 1537Mi | 500m / 2Gi | 3Gi `local-path` PVC，worker2 |
| `business-doris-be` | 单副本 Doris BE 2.1.8，固定 `k3s-worker2`，`hostNetwork: true` | 1175m / 1675Mi | 750m / 3Gi | 8Gi `local-path` PVC，worker2 |
| `realtime-dw-etl-01` | Flink JobManager + TaskManager，镜像 `doit36/flink-jobs:mac12-podman495-20261017-1230` | 12m / 840Mi；12m / 1872Mi | 见 Flink CR/Deployment | 50Gi `flink-state` PVC |

Flink 作业的实际环境变量是节点 IP，而不是 Service DNS：

```text
KAFKA_BOOTSTRAP_SERVERS=192.168.31.211:9092
HBASE_ZOOKEEPER_QUORUM=192.168.31.211:2181
DORIS_FENODES=192.168.31.251:8030
FLINK_CHECKPOINT_URI=file:///flink-data/checkpoints
```

`business-kafka` 有 12 个 Topic（包括 `mall-events-log`、`dwd-events-log`、`ad-request-log`、`show-click-features`）和一个消费者组 `goo1`。因此它不是可直接删除的空测试实例。

### `middleware`：实际已经存在的共享服务

现场已有 MySQL、Nacos、Redis、RocketMQ、`kafka`、AutoMQ（以及 MinIO）和 Pulsar。后面三个消息系统同时存在，说明“统一中间件”目前尚未完成，反而已经有多套消息系统并存。

`middleware/kafka` 是单副本 Kafka 3.9.2，当前观测为 11m CPU / 334Mi 内存，请求为 500m CPU / 1Gi，限制为 2 CPU / 2Gi。它是本项目 Kafka 收敛的目标；AutoMQ 与 Pulsar 不在本项目的 Kafka 客户端兼容迁移范围内。

## 为什么不能直接把 HBase、Doris 移到 `middleware`

1. `doit` 的 Kafka、HBase、Doris 都是单副本 `local-path` PVC。PVC/PV 绑定命名空间；重建到 `middleware` 要新卷与数据迁移，不能修改 YAML 的 namespace 后保留原数据。
2. 三者均启用 `hostNetwork`，并固定调度到指定节点。`r630-proxmox8` 的治理方案把 `middleware` 设计为 Pod Security `baseline`，而 `hostNetwork` 不符合该基线；把 `middleware` 降到 `privileged` 会扩大所有共享中间件的权限面。
3. 当前 `doit` 没有配额、LimitRange 或 NetworkPolicy；未来若应用 R630 仓库的 `middleware` 默认拒绝入站策略，还必须显式放行 `doit -> middleware/kafka:9092`。否则切换后会表现为连接超时。
4. HBase 和 Doris 没有对应的已运行共享集群。仅改变命名空间会额外创建同样大小的一套服务，既不省硬件，也不提升可用性。

因此，HBase/Doris 的合理后续工程是 Service DNS、探针、备份与 StatefulSet/Operator 化；它是独立重构任务，不应伪装成命名空间治理。

## 可实施的节省方案：只收敛 Kafka

成功收敛且停止 `doit/business-kafka` 后，保守按已声明请求值计算可释放 **250m CPU、1Gi 内存和 5Gi 本地盘预留**；按当次实时采样，实际常驻约为 **9m CPU、510Mi 内存**。注意：直到旧 Kafka 缩容/停止，资源都不会释放；迁移期间会短暂同时占用两套资源。

### 先决条件（缺一不可）

1. 指定维护窗口，并确认允许 Kafka 消费暂停及一次 Flink savepoint/restart。
2. 为 `doit` 的 12 个 Topic 记录分区、副本、配置、消息量、消费者组 offset；备份旧 Kafka 的 5Gi 数据卷或至少完成可验证的数据导出。
3. 验证 `middleware/kafka` 的 `advertised.listeners` 对集群内客户端返回 `kafka.middleware.svc.cluster.local:9092`（或等价 Service DNS），而不是节点 IP。仅能连 bootstrap 地址不代表后续分区连接可用。
4. 在目标 Kafka 创建同名 Topic，且分区数、保留策略与旧集群一致；确认容量可以容纳要保留的历史数据。
5. 所有生产者、消费者和本项目 Flink 作业均改为 Service DNS。仓库源码大量硬编码 `doitedu:9092`、`doitedu:2181`、`doitedu:8030`，而现场 Flink 则在环境变量中写节点 IP；必须以可配置环境变量覆盖，不应再写死新地址。

### 切换顺序与验收

1. 导出并保存 `doit` 的 Deployment、Service、ConfigMap、PVC 描述；同时记录 Topic 和 `goo1` 的 offset。该备份是回滚依据，不含 Secret 明文。
2. 先对 `middleware/kafka` 做集群内连通性测试、Topic 创建测试和读写测试；若已启用 NetworkPolicy，添加仅允许 `doit` 工作负载访问 TCP 9092 的规则。
3. 停止外部生产者，触发并确认 Flink savepoint；冻结旧 Kafka 的写入边界。
4. 使用 Kafka 官方复制工具或经过校验的一次性迁移作业复制所需 Topic；逐 Topic 比较末端 offset、消息数与抽样 payload。不要用“创建空 Topic”冒充数据迁移。
5. 将部署参数 `KAFKA_BOOTSTRAP_SERVERS` 切为 `kafka.middleware.svc.cluster.local:9092`，恢复 Flink，并观察 Topic 写入、`goo1` lag、checkpoint 和 Doris/HBase 结果。
6. 保留旧 `business-kafka` 与其 PVC 作为回滚点至少一个完整观察周期。若任一验收失败，恢复旧地址和 savepoint，不删除 PVC。
7. 观察期通过后，先将旧 Kafka 缩容为 0 以释放 CPU/内存；确认备份可恢复且不再需要历史回放，再单独审批删除旧 PVC。`local-path` 卷删除不可当作日常回滚手段。

## 迁移前的高风险操作记录（已完成）

本节记录实施前的风险判断。后续已在备份、PV Retain、服务 DNS 及端到端数据写入验收后完成迁移；具体实测证据和仍未完成的 Kafka 历史消息迁移说明以 [迁移测试报告](k3s-migration-test-report-2026-10-25.md) 为准。
