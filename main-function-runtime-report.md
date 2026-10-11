# Main 函数运行检查报告

检查时间：2026-10-11（Asia/Shanghai）

## 执行方式

- 先执行 `mvn -o -pl realtime_dw,redis_demo,rt_rule_mk test-compile`，三个模块均编译成功，编译目标为 JDK 17。
- 枚举到 41 个 `public static void main(String[] args)` 入口；全部实际启动。
- 流式任务最多观察 8 秒后主动停止，以免持续消费或写入外部系统。`超时` 仅表示已进入持续任务/连接等待，不能视为运行成功。
- Flink 1.16 在 JDK 17 下需要额外开放模块。本次探测加入了 `java.lang` 与 `java.util`；后续还发现部分入口还需开放 `java.util.concurrent.atomic`。

## 结论摘要

| 项目 | 数量 |
| --- | ---: |
| 已启动入口 | 41 |
| 正常结束 | 0 |
| 明确失败 | 33 |
| 8 秒内未结束（已停止） | 8 |

当前机器首先缺少/不可达的是课程环境中的 `doitedu` 主机。Redis 入口实际报 `UnknownHostException: doitedu`，因此这不是单纯 Redis Server 未启动，而是 DNS 或 hosts 映射尚未配置。Kafka、HBase/ZooKeeper、MySQL、Doris 和 HTTP 服务也都配置为该主机，故需要先恢复这台主机的网络可达性。

## 明确需要处理的问题

1. **JDK 17 + Flink 1.16 模块访问**
   - 报错：`InaccessibleObjectException`，涉及 `java.lang.String`、`java.util.LinkedHashMap`、`java.util.concurrent.atomic.AtomicReference`。
   - IDEA 的 Run Configuration 或 Maven Runner VM options 至少加入：
     ```text
     --add-opens=java.base/java.lang=ALL-UNNAMED
     --add-opens=java.base/java.util=ALL-UNNAMED
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
     ```
   - 这是当前 JDK 17 配置下 Flink 1.16 的运行兼容问题，不是业务代码编译错误。

2. **主机与外部服务**
   - 为 `doitedu` 配置可解析的 DNS 或 hosts 记录，并确保以下端口服务可用：
     - Redis：6379
     - Kafka：9092
     - ZooKeeper / HBase：2181
     - HDFS：8020（`WindowTest`）
     - MySQL：3306（`rtrpt`、`rtmk`）
     - Doris FE HTTP：8030
     - 相似词 HTTP API：8081
     - Socket 测试数据源：9090

3. **本地文件与检查点目录**
   - 创建可写的 `D:\ckpt`、`D:\checkpoint`，并将使用 `file:/tmp/ckpt` 的示例改为 Windows 可写的实际目录或确认对应目录可创建。
   - `DimRecProcess` 缺少 `D:\dim_red.csv`。
   - `TestDiaoyong` 缺少 `D:\a.groovy`。

4. **HDFS 依赖**
   - `WindowTest` 报 `NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration`。
   - `realtime_dw` 已有 `hadoop-common`，但该入口还需要增加 `org.apache.hadoop:hadoop-hdfs`（版本需与现有 Hadoop 3.3.2 对齐）。

## 各入口启动结果

### realtime_dw

| 入口 | 结果 | 首要现象 |
| --- | --- | --- |
| `EtlJob01_UserEventsLogCommonDim` | 失败 | JDK 17 模块访问：`java.lang` / `java.util` |
| `EtlJob02_MallTfcAg01` | 超时 | 流任务未在观察窗口内退出 |
| `EtlJob03_PageAccessTimeLong` | 失败 | Doris REST 连接失败（`doitedu:8030`） |
| `EtlJob04_InteractiveEventsOverview` | 超时 | 流任务未在观察窗口内退出 |
| `EtlJob05_VideoEventsAgg` | 失败 | JDK 17 模块访问：`java.util` |
| `EtlJob06_SearchEventsAnalyse_Async` | 超时 | 流任务未在观察窗口内退出 |
| `EtlJob06_SearchEventsAnalyse` | 超时 | 流任务未在观察窗口内退出 |
| `EtlJob07_AdRequestSessionProcess` | 超时 | 流任务未在观察窗口内退出 |
| `SyncJob01_UserInfo2Hbase` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建（开放 `atomic` 后复测） |
| `SyncJob02_VideoInfo2Hbase` | 失败 | JDK 17 模块访问：`java.util.concurrent.atomic` |
| `SyncJob03_AdRequestFeatures2Hbase` | 失败 | JDK 17 模块访问：`java.util.concurrent.atomic` |
| `Promotion01_PVUV` | 超时 | 流任务未在观察窗口内退出 |
| `Promotion01_PVUVLocal` | 失败 | 检查点目录 `file:/tmp/ckpt/...` 无法创建 |
| `DimRecProcess` | 失败 | 缺少 `D:\dim_red.csv` |
| `HttpRequestDemo` | 失败 | HTTP API 主机 `doitedu` 无法解析 |
| `EtlAsync`（test） | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `Http`（test） | 超时 | 等待 `doitedu:8081` HTTP 服务 |
| `WindowTest`（test） | 失败 | 缺少运行时类 `org.apache.hadoop.hdfs.HdfsConfiguration` |

### redis_demo

| 入口 | 结果 | 首要现象 |
| --- | --- | --- |
| `RedisDemo` | 失败 | `UnknownHostException: doitedu`；目标是 `doitedu:6379` |

### rt_rule_mk

| 入口 | 结果 | 首要现象 |
| --- | --- | --- |
| `TargetProductCartMonitor` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `TargetProductCartMonitorLocal` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `MultiRuleEngine` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `MultiRuleEngineLocal` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `RuleCalculatorPattern` | 失败 | Flink 本地作业初始化阶段失败 |
| `RuleCalculatorPatternLocal` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `ModelBasedRuleEngine` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `ModelBasedRuleEngineLocal` | 失败 | 检查点目录 `file:/tmp/ckpt/...` 无法创建 |
| `BitmapOptimizedRuleEngine` | 失败 | Flink 本地作业初始化阶段失败 |
| `BitmapOptimizedRuleEngineLocal` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `DynamicRuleManagement` | 失败 | Flink 本地作业初始化阶段失败 |
| `DynamicRuleManagementLocal` | 失败 | 检查点目录 `file:/tmp/ckpt/...` 无法创建 |
| `DynamicRuleManagementV2` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `DynamicRuleManagementV2Local` | 失败 | 检查点目录 `file:/tmp/ckpt/...` 无法创建 |
| `RuleEngineRecovery` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `RuleEngineRecoveryLocal` | 失败 | Flink 本地作业初始化/检查点阶段失败 |
| `DynamicRuleWithGroovy` | 失败 | 检查点目录 `file:/d:/ckpt/...` 无法创建 |
| `DynamicRuleWithGroovyLocal` | 失败 | Flink 本地作业初始化阶段失败 |
| `TestDiaoyong` | 失败 | 缺少 `D:\a.groovy` |
| `EventsMoni`（test） | 失败 | Kafka `bootstrap.servers=doitedu:9092` 不可解析/无效 |
| `HbaseProfileTagsMoni`（test） | 超时 | 等待 HBase/ZooKeeper（`doitedu:2181`） |
| `HistoryConditionValueMoni`（test） | 失败 | Redis 主机 `doitedu` 无法解析 |

## 建议的恢复顺序

1. 为 `doitedu` 配置 DNS/hosts，并启动或连通 Redis、Kafka、ZooKeeper/HBase、MySQL、Doris、HDFS 与 HTTP API。
2. 创建本地数据和检查点目录，或将这些硬编码路径改为配置项。
3. 在 IDEA 和 Maven 的运行 VM options 中添加上述 JDK 17 `--add-opens` 参数。
4. 在 `realtime_dw` 添加 `hadoop-hdfs` 后重新运行 `WindowTest`。
5. 再按入口启动各流任务；它们会持续运行，应由 Flink UI/日志确认健康状态，而非等待进程自然退出。
