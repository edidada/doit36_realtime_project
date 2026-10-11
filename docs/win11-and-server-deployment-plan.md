# Win11 ThinkBook + 二手企业服务器运行规划

## 目标与边界

目标是在一台 Win11 ThinkBook 2024 上开发、调试和提交作业，在一台二手企业服务器上运行本项目依赖的中间件、Flink/Spark 作业及测试数据。

本项目包含四个模块：

- `realtime_dw`：Flink 实时数仓与实时看板。
- `rt_rule_mk`：Flink 实时规则引擎示例。
- `redis_demo`：Redis 连通性/读写示例。
- `batch_jobs`：Spark 3.1.2 离线地理维表加载任务。

不要同时启动全部 41 个 `main` 方法：其中大量入口是同一主题的递进 Demo 或 `Local` 本地版，会重复消费同一个 Kafka Topic、写同一张表或争抢检查点目录。应按“基础设施 → 初始化/造数 → 一条实时链路 → 规则引擎 Demo → Spark 批任务”的顺序分批验证。

## 推荐总体拓扑

```text
ThinkBook（Win11）
  IDEA + JDK 17 + Maven + Git + SSH
  └── 通过 LAN / VPN 访问服务器

二手企业服务器（建议 Linux）
  ├── Docker Compose：MySQL、Redis、Kafka、ZooKeeper、HBase、Doris、Elasticsearch
  ├── Flink standalone：JobManager + TaskManager，提交 realtime_dw / rt_rule_mk
  ├── Spark local/standalone：执行 batch_jobs
  ├── 本地检查点与数据目录
  └── Nginx/Mock：相似词 HTTP API（项目未提供其服务端源码时）
```

服务端推荐安装 Debian 12 或 Ubuntu Server 22.04 LTS，而不是安装桌面 Windows。理由是 Kafka、HBase、Doris、Flink、Spark、Docker、磁盘挂载和 SSH 运维在 Linux 上更成熟；ThinkBook 保留 Windows 作为开发桌面即可。

## 先盘点二手服务器

品牌不重要，以下指标决定它能否“一机运行全部课程组件”。安装系统前，在 BIOS/iDRAC/iLO 或 Live Linux 下记录：

```bash
sudo lscpu
free -h
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL
sudo dmidecode -t memory
ip -br addr
```

还应确认：

- CPU 是否支持 VT-x/VT-d 或 AMD-V；若未来需要虚拟机或 Proxmox 很重要。
- 内存是否为 ECC；二手服务器常有 ECC，这是优势。
- 系统盘与数据盘是否为 SSD/NVMe；Kafka、Doris、Flink RocksDB 和 HBase 都明显受随机 I/O 影响。
- RAID 卡是否有缓存电池，以及磁盘是否健康（SMART）。
- 网卡至少千兆；ThinkBook 与服务器最好接同一千兆交换机，2.5GbE 更舒适。
- 噪音、功耗与 UPS：企业服务器通常空载功耗和风扇噪音明显高于家用主机。

## 资源分档

| 服务器规格 | 能做什么 | 建议 |
| --- | --- | --- |
| 4–8 核 / 32 GB / 1 TB SSD | 一次启动少数服务与单个作业 | 可学习，但不要同时启动 HBase、Doris、ES、Flink、Spark |
| 8–16 物理核 / 64 GB ECC / 1–2 TB NVMe SSD | 完整单机课程环境 | 推荐起步配置 |
| 16+ 核 / 128 GB ECC / 2 TB NVMe + 大容量 HDD | 多作业并行、保留数据、压测 | 理想配置 |

建议把 NVMe/SSD 用于 Kafka、Doris、Flink checkpoint 和 HBase WAL；大容量 HDD 用于备份、冷数据和镜像归档。内存不足时优先减少并发服务，**不要依赖 Swap 让 Kafka/HBase/Doris 继续工作**。

## 两个运行阶段

### 阶段 A：原样跑通课程环境

目标是尽量少改业务代码，先让硬编码的 `doitedu` 主机名、端口、Topic、表名和本地文件路径都可用。

1. 在服务器上安装 Docker Engine、Docker Compose、OpenJDK 17、OpenJDK 11、Maven 和 Git。
2. 在服务器宿主机执行 `/etc/hosts` 映射：

   ```text
   127.0.0.1 doitedu
   ```

   如果 ThinkBook 上直接运行作业，则在 Windows 的 `hosts` 中将 `doitedu` 映射为服务器 LAN IP，例如：

   ```text
   192.168.1.20 doitedu
   ```

   不要把数据库或消息队列直接暴露到公网。

3. 服务使用 Docker Compose 启动，服务端口发布到服务器宿主机；这样项目中 `doitedu:9092`、`doitedu:6379`、`doitedu:2181` 等硬编码地址仍可工作。
4. 检查点路径先统一准备：

   ```text
   /srv/doit/checkpoints
   /srv/doit/savepoints
   /srv/doit/data
   /srv/doit/backups
   ```

   原代码中的 `file:/d:/ckpt`、`file:/d:/checkpoint` 和 `file:/tmp/ckpt` 不适合 Linux 服务端运行；在阶段 A 可为演示任务建立单独配置/运行参数，阶段 B 再完全改为配置项。

### 阶段 B：改成可维护的个人实验平台

目标是把硬编码地址、密码和本地盘符移出 Java 源码。

- 将 `doitedu`、端口、MySQL 密码、Kafka Topic、Doris FE、Redis、HBase ZooKeeper、HTTP API 地址改为环境变量或 `application.properties`/Flink 参数。
- 检查点、保存点、CSV、Groovy 文件改为服务器挂载目录。
- MySQL `root/root` 改为最小权限业务账号；密码写入 `.env`，不提交 Git。
- 每个演示作业使用独立的 checkpoint/savepoint 子目录和独立 consumer group。
- 为所有组件固定镜像版本、挂载卷和备份策略；Docker Compose 文件也应纳入版本控制。

## 服务清单与资源建议

| 服务 | 项目用途 | 单机学习环境建议 | 备注 |
| --- | --- | --- | --- |
| MySQL 8 | 业务表、CDC 源、规则元数据、看板结果 | 2 CPU / 2–4 GB | 需建 `realtimedw`、`rtrpt`、`rtmk` 及表结构 |
| Redis 7 | 看板结果、规则历史条件、画像缓存 | 1 CPU / 1–2 GB | `redis_demo` 与规则计算器直接依赖 |
| Kafka | 行为日志、广告日志、宽表 Topic | 2 CPU / 4–6 GB | 单机 KRaft 足够；保留 ZooKeeper 给 HBase |
| ZooKeeper | HBase 协调 | 1 CPU / 1–2 GB | 单节点仅用于开发/学习 |
| HBase | 用户、视频、地理、广告特征、规则画像维表 | 2–4 CPU / 6–10 GB | 单节点适合学习；生产不可这样部署 |
| Doris | 实时宽表和聚合分析 | 2–4 CPU / 6–12 GB | 可先用单 FE + 单 BE |
| Elasticsearch 7.17 | batch 模块的依赖和扩展示例 | 2 CPU / 4 GB | 当前唯一 Spark 作业未直接写 ES，可按需后启 |
| Flink standalone | `realtime_dw`、`rt_rule_mk` | JM 1 GB；TM 4–8 GB | 本机先单 JobManager + 单 TaskManager |
| Spark 3.1.2 | `batch_jobs` | 2–4 GB | 当前作业写 HBase，`master("local")` 已固定本地模式 |
| 相似词 HTTP API | 搜索分析任务 | 1 CPU / 512 MB | 仓库未提供服务端，需要取得原服务或实现 Mock |

建议先不要把所有容器一起启动。64 GB 内存的合理首批组合是 MySQL + Redis + Kafka + ZooKeeper + HBase + Flink；验证完再加入 Doris，最后按需要启动 Elasticsearch 和 Spark。

## Java 与版本策略

| 组件 | 建议运行 JDK | 原因 |
| --- | --- | --- |
| IDEA / Maven 编译 / Flink 项目 | JDK 17 | 根 POM 已限定 Java 17 |
| Flink 1.16.1 作业 | JDK 17 + `--add-opens` | 本项目实测需要开放 `java.lang`、`java.util`、`java.util.concurrent.atomic` |
| Spark 3.1.2 / Hive 依赖 | 优先 JDK 11，单独验证 | 组件版本较旧，不建议未经验证直接使用 JDK 17 |

Flink 作业在 IDEA、Flink CLI 和服务器服务文件中都要添加：

```text
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
```

项目当前 Flink 1.16.1、Spark 3.1.2 和部分连接器较旧。第一阶段应锁定已验证版本跑通；第二阶段再规划 Flink 2.x、Spark 新版本及 connector 兼容性升级，不能直接混升。

## 必需数据、表和 Topic

启动作业前需准备以下内容；其中部分建表/造数脚本不在仓库内，需要从课程资料取得或自行补建。

### Kafka Topic

- `mall-events-log`：用户行为原始日志。
- `dwd-events-log`：退维后的行为宽表，是多个聚合任务和规则引擎的输入。
- `ad-request-log`：广告请求日志。
- `show-click-features`：广告展示/点击特征流。

### MySQL 数据

- `realtimedw.ums_member`、`realtimedw.cms_video`、`realtimedw.t_md_areas`。
- `rtrpt.dashboard_promotion_abc_pvuv`。
- 规则元数据相关表（供 `rt_rule_mk` 的动态规则 Demo 使用）。

### HBase 表

- `dim_user_info`、`dim_video_info`、`dim_geo_area`、`dim_page_info`。
- `ad_features`、`ad_features_hbase`。
- `rule_meta_doit36` 与用户画像/标签相关表。

### 本地输入与外部接口

- `DimRecProcess`：`/srv/doit/data/dim_red.csv`。
- `TestDiaoyong`：`/srv/doit/data/a.groovy`。
- 搜索任务：`http://doitedu:8081/api/post/simwords`。如果没有原服务端，先部署接口 Mock，再验证 Flink 的同步/异步 HTTP 逻辑。

## 推荐执行顺序

### 1. 基础设施验收

依次确认 MySQL、Redis、Kafka、ZooKeeper、HBase、Doris 的健康检查、端口和 DNS/hosts 映射。再确认服务器上的 `/srv/doit` 挂载盘可写。

### 2. 初始化数据

1. 建 MySQL 库表和 HBase 表。
2. 运行 Spark 作业 `DataLoadJob01_GeoAreaInfo2Hbase`，将 `t_md_areas` 生成地理维表 `dim_geo_area`。
3. 启动 MySQL CDC 的用户/视频维表同步任务。
4. 写入规则元数据、HBase 画像测试数据和 Redis 历史条件测试数据。
5. 创建 Kafka Topic，并向 `mall-events-log` 或 `dwd-events-log` 发送测试事件。

### 3. 数仓主链路

1. 先启动 `EtlJob01_UserEventsLogCommonDim`，生成 `dwd-events-log` 宽表。
2. 按需启动 `EtlJob02` 至 `EtlJob07` 中的一个或多个聚合任务。
3. 启动 `Promotion01_PVUV`，检查 Redis 和 MySQL/Doris 中的结果。

### 4. 规则引擎链路

按 Demo 编号顺序学习和运行：Demo1 → Demo2 → Demo3 → Demo4 → Demo5 → Demo6 → Demo7 → Demo8 → Demo9。每一阶段只启动一个主作业；对应 `Local` 版用于本机小范围调试，不和正式版同时跑。

### 5. 测试与观测

- Flink Web UI：查看 Job、Checkpoint、Backpressure 和失败栈。
- Kafka：检查 consumer group、lag 和 Topic 消息。
- HBase：验证 RowKey、列族和 scan/get 结果。
- Doris：验证明细及聚合行数。
- Redis：验证 PV/UV 与规则历史值。
- 将服务日志、Flink savepoint 和数据库备份放在 `/srv/doit/backups`。

## 打包和提交方式

当前 POM 没有完整的 fat-jar/shade 打包配置，不能假设 `java -cp xxx.jar 类名` 会携带全部 Flink、HBase、Kafka 依赖。推荐流程：

1. ThinkBook 上用 IDEA/Maven 编译、运行单元/本地测试。
2. 通过 Git 推送代码，服务器 `git pull`；不要手工复制未版本化的 jar。
3. 为 Flink 模块补充 Maven Shade/Assembly 配置后，以 `flink run -c <主类> <作业包>` 提交到服务器的 Flink 集群。
4. Spark 作业使用匹配 Spark 3.1.2 与 Scala 2.12 的 `spark-submit`；由于源码强制 `.master("local")`，第一阶段在服务器本地执行即可。

## 网络与安全底线

- ThinkBook 到服务器仅开放 SSH（22）和必要的 Web UI；优先通过局域网、Tailscale 或 WireGuard 访问。
- Kafka、MySQL、Redis、HBase、Doris 不暴露到公网。
- 生产级多节点架构与本单机学习环境不同：Kafka/HBase/Doris 都需要多副本、独立磁盘和监控，不能从本计划直接照搬。
- 为二手服务器配置定时备份、SMART 检查和 UPS；数据盘故障是家庭实验环境最常见的不可恢复风险。

## 最小验收标准

完成下列事项即代表“所有工程可以按需运行”，而不是要求 41 个常驻流任务同时运行：

1. 四个 Maven 模块均能编译。
2. `RedisDemo` 能连接 Redis。
3. Spark 地理维表作业能把 MySQL 区域数据写入 HBase。
4. 用户/视频同步任务可写 HBase。
5. `EtlJob01` 能从 `mall-events-log` 生成 `dwd-events-log`。
6. 任一 Doris 聚合任务和 `Promotion01_PVUV` 能产出可查询的结果。
7. 任一规则引擎 Demo 能消费事件、读取规则元数据并产生规则结果。
8. Flink Web UI 能看到健康作业与成功 checkpoint。

## 下一步需要补充的信息

获取服务器的 CPU 型号、核心数、内存容量、SSD/HDD 容量、RAID 状态和网络接口后，可据此把上面的资源配额收敛成一份可直接执行的 Docker Compose/Flink 配置。还需要确认课程环境中 MySQL 建表/初始化数据、Doris 建表和相似词 API 的源码或镜像位置；这些内容目前不在本仓库。
