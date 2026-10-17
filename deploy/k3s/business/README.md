# 原 ETL 的真实业务链路验收

`doitedu` 是课程部署主机名。替换为内网 IP 可行的前提是目标地址上确实有对应服务，并且 Kafka 返回的 broker 地址、HBase 在 ZooKeeper 登记的 RPC 地址、Doris FE 返回的 BE 地址也对 Flink 可达。这里只改变 DNS 映射不能补齐缺失服务。

本环境将状态服务部署在独立的 `doit` 命名空间，使用固定节点 LAN 地址通告协议端点：

| 组件 | 节点 | 地址 | 内存上限 |
|---|---|---|---|
| Kafka 3.9.1 KRaft | worker1 | 192.168.31.211:9092 | 1 GiB |
| HBase 2.4.18 standalone（内置 ZooKeeper） | worker1 | 192.168.31.211:2181 / 16020 | 1.5 GiB |
| Doris 2.1.8 FE | worker2 | 192.168.31.251:8030 / 9030 | 2 GiB |
| Doris 2.1.8 BE no-AVX2 | worker2 | 192.168.31.251:8040 / 9050 | 3 GiB |
| Flink 原 ETL | worker3 | UI 192.168.31.88:30816 | JM 2 GiB + TM 4 GiB |

这是单副本验收环境，PVC 使用 local-path；数据和服务绑定节点。Pod 使用 hostNetwork，因此不把协议内部返回的节点地址错误地当成普通 HTTP 代理来处理。验收涉及的端口只允许受信任内网访问。

## 运行时准备

Kafka 镜像为 `apache/kafka:3.9.1`。HBase 使用 `eclipse-temurin:11-jdk-jammy` 运行官方二进制包；包从华为 Apache 镜像下载，通过 archive.apache.org 的官方 SHA512 校验后解压到 worker1 `/var/lib/doit36/hbase-2.4.18`。

worker2 CPU 是 QEMU 虚拟 CPU，未暴露 AVX2，因此使用官方 `apache-doris-2.1.8-bin-x64-noavx2.tar.gz` 的 BE，校验 SHA512 后放在 `/var/lib/doit36/apache-doris-2.1.8-bin-x64-noavx2/be`，挂载到同版本 FE 镜像提供的 Ubuntu/JDK 运行环境。无需重启节点调整 CPU。worker2 的 `vm.max_map_count` 设置为 2000000，原值记录于 `/var/lib/doit36/downloads/vm-max-map-count.before`。

镜像从 DaoCloud 镜像源下载到目标节点 containerd 缓存，清单使用 Never 拉取策略；重新部署前必须准备相同缓存和安装目录。下载目录为各节点 `/var/lib/doit36/downloads`。

## 配置与初始化

原入口 `EtlJob01_UserEventsLogCommonDim` 支持以下环境变量；不设置时保留课程的 doitedu 默认值：

- `KAFKA_BOOTSTRAP_SERVERS`
- `HBASE_ZOOKEEPER_QUORUM`
- `DORIS_FENODES`
- `DORIS_USERNAME`、`DORIS_PASSWORD`（密码通过 Secret `business-doris-auth` 注入）
- `FLINK_CHECKPOINT_URI`

Kafka/Doris 两个 INSERT 使用 StatementSet 一起提交，在一个真实业务作业中完成读取、维度关联和双输出。`flink-business.yaml` 复用已有作业资源和 PVC，切换时为 stateless；它会替换之前的 datagen 验收入口。

初始化脚本：`init-hbase.rb` 创建三个维表并写入带明确类型编码的测试维度；`init-doris.sql` 创建单副本目标表。HBase 的 long/int/date/timestamp 列使用 Flink HBase connector 要求的二进制编码，不能直接写数字文本冒充维度。Kafka Topic 为 `mall-events-log` 与 `dwd-events-log`。

部署或恢复 Flink 前，必须执行上级文档的 VM 107 关机门禁。

## 复现命令

在 Mac 仓库根目录执行（目标主机 SSH 已免密配置）：

```bash
# 首次才需要下载镜像/安装包并创建中间件；缓存和安装目录已存在时会跳过下载。
bash deploy/k3s/business/prepare-middleware.sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash deploy/k3s/business/deploy-business.sh
```

第二个脚本编译原 ETL 的三个类，制作小 JAR，通过带 SHA256 摘要名称的 ConfigMap 挂载到此前 Podman 构建的项目镜像中。init 容器复制镜像中的运行时依赖、排除重复 planner，并用新 JAR 替换旧入口；本次源码修改尚未重新封装进基础镜像本身。重新发布业务镜像时可将这些类正常打包，不再依赖小 JAR ConfigMap。大 JAR 不适用此挂载方式。

脚本在 master 执行 `init-on-master.py`：幂等初始化 Topic、HBase 维度、Doris FE/BE 注册和目标表，创建/复用 Secret，并为专用用户 `doit_etl` 授予目标库 SELECT/LOAD 权限。密码不写入版本控制或脚本日志。部署前再次确认 VM 107 已停止，然后切换 FlinkDeployment，运行 `verify-business.py` 验收；脚本失败会返回非零，不能把清单 apply 成功当成业务成功。

单独重跑业务验收：

```bash
scp deploy/k3s/business/verify-business.py k3s-master:/var/tmp/
ssh k3s-master 'cd /var/tmp && python3 verify-business.py'
```

每次验收生成一个唯一 `session_id`，向 `mall-events-log` 发送 5 条事件，再检查：

1. 原业务入口及所有任务均为 RUNNING。
2. `dwd-events-log` 包含全部 5 个事件时间戳；用户 ID=10001、手机号=13800000000、区域=Haidian、页面类型=product、服务=mall。
3. Doris 中同一 session 恰好有 5 行，且以上维度字段匹配。
4. checkpoint 成功数增加，观察期间没有新增失败，作业仍为 RUNNING。

验收结果保存到 master `/var/tmp/business-acceptance.json`，包括作业 ID、session、Kafka 输出样本、Doris 查询结果和前后 checkpoint 计数。测试维度和事件均为合成数据，不代表课程原有数据已迁移；验收覆盖的是原入口中的真实连接器、三张 HBase 维表及 Kafka/Doris 双输出。

Flink Dashboard：<http://192.168.31.88:30816/>。

## 2026-10-17 实测验收通过

- 原业务入口作业 ID：`afc56b7678fd7ab68b4270bbe4d014da`，状态 `RUNNING`。
- 唯一批次：`a1792223311704`，输入 5 条，Kafka 宽表输出 5 条，Doris 查询 5 行。
- 三张 HBase 维度均关联成功：`user_id=10001`、`register_phone=13800000000`、`gps_region=Haidian`、`page_type=product`、`page_service=mall`。生日二进制解码为 `1990-01-01`，注册时间为 `2024-01-01 00:00:00`。
- 验收期间成功 checkpoint 从 0 增至 7；启动等待 TaskManager 时已有 6 次未触发记录，验收期间该失败计数保持 6，没有新增失败。
- Kafka、HBase、Doris FE/BE、JobManager、TaskManager 共 6 个 Pod 均 `1/1 Ready`，重启次数为 0。节点实测内存使用率约为 worker1 53%、worker2 46%、worker3 36%；VM 107 保持停止。

```json
{"input_events":5,"kafka_output_events":5,"doris_verified":["5","10001","13800000000","Haidian","product","mall"]}
```

该验收确认真实读写和维度关联，尚未执行节点故障、savepoint 恢复或生产级高可用测试，也不宣称双端写入在任何故障下具有跨系统原子性。Kafka 与 Doris 的投递保证应分别按连接器配置评估。自动代码审查按本次要求暂不执行。
