# 可执行程序说明

本项目共有 41 个 Java `main` 入口。大部分是 Flink 流任务：启动后会持续消费数据，不会自然结束。名称带 `Local` 的程序是对应方案的本地演示版，名称未带 `Local` 的程序通常连接 Kafka、HBase、Redis、MySQL 或 Doris 等课程集群服务。

> 运行所需服务、缺失项和本机实测结果请看 [main-function-runtime-report.md](main-function-runtime-report.md)。

## realtime_dw：实时数仓与看板

### 维表同步（`data_sync`）

| 入口 | 做什么 | 主要输入 → 输出 |
| --- | --- | --- |
| `SyncJob01_UserInfo2Hbase` | 将会员用户维度同步为 HBase 维表。 | MySQL CDC `ums_member` → HBase `dim_user_info` |
| `SyncJob02_VideoInfo2Hbase` | 将视频基础信息同步为 HBase 维表。 | MySQL CDC `cms_video` → HBase `dim_video_info` |
| `SyncJob03_AdRequestFeatures2Hbase` | 将广告请求特征同步为 HBase 表。 | Kafka 广告/行为事件 → HBase `ad_features_hbase` |

### 明细、聚合与特征任务（`data_etl`）

| 入口 | 做什么 | 主要输入 → 输出 |
| --- | --- | --- |
| `EtlJob01_UserEventsLogCommonDim` | 对用户行为日志做公共维度退维：关联用户、视频、广告等 HBase 维表，形成宽表。 | Kafka 行为日志 + HBase 维表 → Kafka 宽表、Doris 宽表 |
| `EtlJob02_MallTfcAg01` | 实时统计商城/流量概况的轻度聚合指标。 | Kafka 宽表 → Doris 流量聚合表 |
| `EtlJob03_PageAccessTimeLong` | 计算页面访问停留时长，并输出页面访问指标。 | Kafka 行为事件 → Doris 页面访问时长统计 |
| `EtlJob04_InteractiveEventsOverview` | 汇总点赞、收藏、分享等互动事件的概况指标。 | Kafka 宽表 → Doris 互动概况表 |
| `EtlJob05_VideoEventsAgg` | 按视频维度聚合播放及互动行为，并关联视频维表。 | Kafka 宽表 + HBase 视频维表 → Doris 视频聚合表 |
| `EtlJob06_SearchEventsAnalyse` | 同步调用相似词 HTTP 服务扩展搜索词，再计算搜索分析指标。 | Kafka 搜索事件 + HTTP 相似词 API → Doris 搜索分析表 |
| `EtlJob06_SearchEventsAnalyse_Async` | 上一任务的异步 I/O 实现，避免 HTTP 调用阻塞流处理线程。 | Kafka 搜索事件 + 异步 HTTP API → Doris 搜索分析表 |
| `EtlJob07_AdRequestSessionProcess` | 处理广告请求会话，构造广告相关特征并写入下游。 | Kafka 宽表 + HBase 特征表 → HBase/Doris 广告分析结果 |

### 实时看板（`rt_dashboard`）

| 入口 | 做什么 | 主要输入 → 输出 |
| --- | --- | --- |
| `Promotion01_PVUV` | 实时计算 PV、UV 等推广流量指标；同时落 Redis，供看板读取。 | Kafka 宽表 + MySQL 维度 → Redis / 看板指标 |
| `Promotion01_PVUVLocal` | PV/UV 推广看板的本地演示版。 | 本地 Flink 运行环境 → 控制台/本地结果 |

### 工具与实验入口（`utils` / `src/test`）

| 入口 | 做什么 |
| --- | --- |
| `DimRecProcess` | 读取 `D:\dim_red.csv`，处理维度推荐/召回相关的本地数据。 |
| `HttpRequestDemo` | 向相似词服务 `POST /api/post/simwords` 发起 HTTP 请求的最小示例。 |
| `EtlAsync` | 使用 Flink 异步函数调用相似词 HTTP 服务的测试程序。 |
| `Http` | 相似词 HTTP 服务请求测试。 |
| `WindowTest` | Flink 窗口、Kafka 读写与 HDFS 检查点的综合实验。 |

## redis_demo：Redis 基础示例

| 入口 | 做什么 |
| --- | --- |
| `RedisDemo` | 使用 Jedis 连接 Redis（代码目标为 `doitedu:6379`），演示基本读写/连通性操作。 |

## rt_rule_mk：实时规则引擎演进示例

这些程序以同一类事件流为输入，逐步展示从硬编码规则到模型化、动态化、可恢复和 Groovy 热更新的实现方式。

| 入口 | 做什么 |
| --- | --- |
| `TargetProductCartMonitor` | 规则引擎第 1 步：监听指定商品的加购物车行为。 |
| `TargetProductCartMonitorLocal` | 上一规则的本地测试版。 |
| `MultiRuleEngine` | 规则引擎第 2 步：在同一流中同时执行多条规则。 |
| `MultiRuleEngineLocal` | 多规则引擎本地测试版。 |
| `RuleCalculatorPattern` | 规则引擎第 3 步：以规则计算器接口/实现类解耦规则逻辑。 |
| `RuleCalculatorPatternLocal` | 规则计算器模式本地测试版。 |
| `ModelBasedRuleEngine` | 规则引擎第 4 步：根据规则模型配置加载和执行规则。 |
| `ModelBasedRuleEngineLocal` | 基于模型的规则引擎本地测试版。 |
| `BitmapOptimizedRuleEngine` | 规则引擎第 5 步：用 Bitmap 优化用户/条件命中状态。 |
| `BitmapOptimizedRuleEngineLocal` | Bitmap 优化规则引擎本地测试版。 |
| `DynamicRuleManagement` | 规则引擎第 6 步：动态管理规则元数据和规则生命周期。 |
| `DynamicRuleManagementLocal` | 动态规则管理本地测试版。 |
| `DynamicRuleManagementV2` | 规则引擎第 7 步：第二版动态规则管理，扩展模型和状态处理。 |
| `DynamicRuleManagementV2Local` | 动态规则管理 V2 本地测试版。 |
| `RuleEngineRecovery` | 规则引擎第 8 步：演示借助检查点/状态恢复规则引擎。 |
| `RuleEngineRecoveryLocal` | 规则引擎恢复本地测试版。 |
| `DynamicRuleWithGroovy` | 规则引擎第 9 步：从外部规则元数据中加载 Groovy 规则，实现动态规则脚本。 |
| `DynamicRuleWithGroovyLocal` | Groovy 动态规则本地测试版。 |
| `TestDiaoyong` | 读取 `D:\a.groovy`，直接演示 Groovy 脚本编译和调用。 |

### 规则引擎测试数据入口（`rt_rule_mk/src/test`）

| 入口 | 做什么 |
| --- | --- |
| `EventsMoni` | 向 Kafka 生产规则引擎需要的模拟行为事件。 |
| `HbaseProfileTagsMoni` | 向 HBase 写入用户画像标签测试数据。 |
| `HistoryConditionValueMoni` | 向 Redis 写入历史条件命中值，用于规则判断。 |

## 推荐运行顺序

1. 先启动基础设施：DNS/hosts、Kafka、ZooKeeper、HBase、Redis、MySQL、Doris、HDFS 和相似词 HTTP API。
2. 先运行维表同步任务，再运行 `EtlJob01_UserEventsLogCommonDim` 生成宽表。
3. 启动各聚合/看板任务或规则引擎任务。
4. 最后运行 `EventsMoni`、`HbaseProfileTagsMoni`、`HistoryConditionValueMoni` 等测试数据程序，向已运行的流任务提供数据。

运行任何 Flink 入口前，请按运行检查报告为 JDK 17 配置 `--add-opens` 参数，并准备好对应的检查点目录。
