# 金融计算平台 Java 工业级软件设计方案
**对标国内外大型银行、证券公司、交易所及金融科技企业的软件工程规范**

建议将两个方向设计为独立的金融计算系统，而不是简单的 Java 算法演示项目。

| 项目 | 金融大数据报表计算引擎 | 金融风险数值计算引擎 |
|---|---|---|
| 项目代号 | FIN-OLAP | FIN-RISK |
| Git 仓库 | `fin-olap-engine-java` | `fin-risk-engine-java` |
| 系统类型 | 分布式 OLAP / 报表计算平台 | 分布式金融风险计算平台 |
| 核心语言 | Java 17 | Java 17 |
| 主要业务 | 交易统计、经营分析、监管报表 | 期权定价、VaR、Monte Carlo |
| 主要计算 | 扫描、过滤、JOIN、GROUP BY | 矩阵、随机模拟、数值优化 |
| 主要性能瓶颈 | 内存带宽、磁盘 I/O、网络 Shuffle | CPU、缓存、SIMD、浮点运算 |
| 主要参考 | ClickHouse、Doris、Spark、Trino | QuantLib、Strata、OpenGamma |
| 工程目标 | 企业级金融数据分析服务 | 企业级金融风险计量服务 |

两个项目分别编写以下规范化设计文档：

| 文档编号 | 文档名称 | 主要内容 |
|---|---|---|
| FIN-XXX-REQ-001 | 软件需求规格说明书 SRS | 功能需求、非功能需求、验收标准 |
| FIN-XXX-ARC-001 | 软件架构设计说明书 SAD | 架构模式、组件、部署、技术选型 |
| FIN-XXX-HLD-001 | 软件概要设计说明书 HLD | 模块划分、接口、数据流、职责 |
| FIN-XXX-LLD-001 | 软件详细设计说明书 LLD | 类、算法、线程、状态、异常处理 |
| FIN-XXX-DATA-001 | 数据设计说明书 | 数据模型、存储、生命周期、数据质量 |
| FIN-XXX-API-001 | 接口设计规范 | REST、gRPC、错误码、幂等性 |
| FIN-XXX-SEC-001 | 安全设计说明书 | 身份认证、授权、审计、加密 |
| FIN-XXX-PERF-001 | 性能设计与测试方案 | 吞吐量、延迟、CPU、内存、基准测试 |
| FIN-XXX-TEST-001 | 测试设计说明书 | 单元、集成、压力、故障、验收测试 |
| FIN-XXX-OPS-001 | 部署运维设计说明书 | Kubernetes、监控、告警、灾备 |
| FIN-XXX-ADR-001 | 架构决策记录 | 技术方案选择与权衡 |

这里采用 IEEE 1016 的设计描述思想、ISO/IEC/IEEE 29148 的需求工程思想，并结合大型企业常见的 SAD、HLD、LLD、ADR、SLO 和可追溯性管理实践。

需要区分两件事：

- **工业级架构设计**：从可扩展性、容错、安全、精度、运维等方面设计可生产化的系统。
- **已经达到工业级**：必须通过真实数据规模、性能、可靠性、安全和业务验收测试证明。

下面是两个系统的正式设计基线。性能数字属于拟定验收目标，不代表已经实现或经过测试。

---

# 第一部分：FIN-OLAP 金融大数据报表计算引擎

# 文档一：软件架构设计说明书 SAD

## 1. 文档基本信息

| 属性 | 内容 |
|---|---|
| 文档名称 | FIN-OLAP 软件架构设计说明书 |
| 文档编号 | FIN-OLAP-ARC-001 |
| 文档版本 | V1.0 Draft |
| 文档状态 | 待架构评审 |
| 系统名称 | Financial OLAP Engine |
| 开发语言 | Java 17 LTS |
| 部署环境 | Linux / Kubernetes |
| 构建系统 | Maven |
| 系统架构 | Coordinator-Worker 分布式计算架构 |
| 主要数据格式 | Parquet、Arrow、CSV |
| 服务协议 | REST、gRPC |
| 适用场景 | 银行经营报表、财务统计、监管数据分析 |

## 2. 系统建设目标

### 2.1 业务背景

大型金融机构每天产生大量业务数据，包括：

- 账户交易流水。
- 支付清算记录。
- 信贷业务数据。
- 资金交易数据。
- 客户行为数据。
- 风险监控数据。
- 财务会计数据。

传统关系型数据库在处理大规模历史数据聚合时，可能面临查询成本、存储成本和资源隔离等问题。

本项目建设一个独立的金融 OLAP 计算平台，用于执行大规模历史交易数据分析。

### 2.2 业务目标

系统支持：

1. 交易金额和笔数统计。
2. 日报、月报、年度报表。
3. 按账户、机构、交易类型分组。
4. 多维度金融指标计算。
5. SQL 查询。
6. 大规模历史数据扫描。
7. 并发报表任务。
8. 查询结果导出。
9. 数据质量验证。
10. 查询任务审计。

### 2.3 设计规模

| 指标 | 初始设计目标 |
|---|---|
| 数据规模 | 10 亿条交易记录 |
| 数据量 | 约 40～200 GB，取决于字段及编码 |
| 计算节点 | 3～10 个 Worker |
| 单节点 CPU | 16～32 vCPU |
| 单节点内存 | 64～128 GB |
| 数据存储 | 对象存储或分布式文件系统 |
| 单次查询最大扫描量 | 10 亿行 |
| 并发查询 | 初期 20 个，后续压测调整 |
| 报表任务 | 支持异步执行 |
| 可用性目标 | 月度 99.9% |
| 数据一致性 | 基于固定数据快照 |

以上为容量规划起点，不是经验证的性能承诺。

### 2.4 非功能需求

| 编号 | 需求 | 验收方法 |
|---|---|---|
| NFR-001 | 查询结果准确 | 与独立参考实现逐项核对 |
| NFR-002 | 支持 10 亿行数据集 | 完整数据集加载和查询测试 |
| NFR-003 | 支持多节点执行 | 3 节点及以上集成测试 |
| NFR-004 | Worker 故障后任务可恢复 | 故障注入测试 |
| NFR-005 | 金额聚合不得静默溢出 | 边界测试和属性测试 |
| NFR-006 | 查询具备资源限制 | 内存、CPU、超时测试 |
| NFR-007 | 查询结果可追溯 | 审计日志检查 |
| NFR-008 | 支持水平扩展 | 3、5、10 节点扩展测试 |
| NFR-009 | 服务接口鉴权 | 安全测试 |
| NFR-010 | 支持数据快照版本 | 一致性测试 |

**金融系统尤其需要重视 NFR-001、NFR-005 和 NFR-010。** 计算速度再快，如果金额错误、结果不可复现，就不能用于正式财务或监管报表。

## 3. 总体架构设计

### 3.1 架构模式

采用：

**Control Plane + Data Plane + Storage Plane**

三个逻辑层次。

```text
                        FIN-OLAP
                           |
                  API Gateway / LB
                           |
               +-----------+-----------+
               |                       |
          REST API                  gRPC API
               |                       |
               +-----------+-----------+
                           |
                    Control Plane
                           |
               +-----------+-----------+
               |                       |
          Query Service          Report Service
               |                       |
          SQL Parser             Report Scheduler
               |                       |
          Query Planner           Task Metadata
               |
          Query Optimizer
               |
          Distributed Scheduler
               |
     +---------+----------+----------+
     |                    |          |
     v                    v          v
 Worker Node 1       Worker Node 2  Worker Node N
     |                    |          |
 Scan Operator        Scan Operator  Scan Operator
     |                    |          |
 Filter               Filter         Filter
     |                    |          |
 Aggregate            Aggregate      Aggregate
     |                    |          |
     +--------------------+----------+
                          |
                     Shuffle
                          |
                    Final Aggregate
                          |
                    Result Service
                          |
                   CSV / Parquet
                          |
                    Report Output

                Storage Plane
                      |
       +--------------+--------------+
       |              |              |
   Object Store    Metadata DB    Result Store
       |
   Parquet Files
```

### 3.2 架构设计原则

**原则一：计算与存储分离**

Worker 不长期保存原始交易数据，数据存储在统一的数据湖或对象存储中。

优点：

- Worker 可以独立扩缩容。
- 存储容量与计算容量分别规划。
- 计算节点故障不导致原始数据丢失。

**原则二：查询任务与计算任务分离**

Coordinator 负责：

- SQL 解析。
- 查询计划。
- 任务调度。
- 任务状态管理。
- 结果协调。

Worker 负责：

- 数据扫描。
- 数据过滤。
- 数据聚合。
- 中间结果交换。

**原则三：数据分块处理**

不允许默认将 10 亿条交易全部加载到 JVM Heap。

采用：

```text
Parquet File
    |
    v
Row Group
    |
    v
Column Batch
    |
    v
Vectorized Operator
    |
    v
Partial Aggregate
```

**原则四：计算结果可复现**

同一份数据快照、同一查询语义和同一参数，应产生相同的逻辑结果。

对于金融金额，必须明确定义：

- 金额精度。
- 溢出行为。
- NULL 处理。
- 汇率版本。
- 日期边界。
- 时区规则。
- 数据修订版本。

## 4. 核心技术选型

| 技术领域 | 选择 | 原因 |
|---|---|---|
| JDK | Java 17 | LTS，具备成熟生态 |
| SQL Parser | Apache Calcite | SQL 解析、关系代数、优化规则 |
| 内存数据格式 | Apache Arrow Java | 列式内存与批处理 |
| 文件格式 | Apache Parquet | 列式压缩、谓词下推 |
| RPC | gRPC Java | 内部服务通信 |
| API | Spring Boot | 管理接口与业务服务 |
| 元数据 | PostgreSQL | 查询、任务、快照元数据 |
| 计算调度 | 自研 Coordinator | 研究分布式执行机制 |
| 指标 | Micrometer / Prometheus | 性能与运行监控 |
| 链路追踪 | OpenTelemetry | 分布式任务追踪 |
| 测试 | JUnit 5、Testcontainers | 自动化测试 |
| 基准测试 | JMH | JVM 微基准测试 |
| 部署 | Kubernetes | 容器编排 |

技术选型存在重要边界：Calcite 不直接提供完整的分布式物理执行引擎；Arrow 也不负责分布式任务调度。这些部分是项目需要自主研发的核心。

# 文档二：软件概要设计说明书 HLD

## 5. 模块划分

系统拆分为以下核心模块。

| 模块编号 | 模块名称 | 主要职责 |
|---|---|---|
| M01 | fin-olap-api | 对外查询接口 |
| M02 | fin-olap-sql | SQL 解析、校验 |
| M03 | fin-olap-planner | 逻辑计划、物理计划 |
| M04 | fin-olap-optimizer | 查询优化 |
| M05 | fin-olap-scheduler | 分布式任务调度 |
| M06 | fin-olap-worker | 分布式计算执行 |
| M07 | fin-olap-storage | Parquet、Arrow 数据访问 |
| M08 | fin-olap-execution | 物理算子 |
| M09 | fin-olap-vector | SIMD 与向量化 |
| M10 | fin-olap-shuffle | 节点间数据交换 |
| M11 | fin-olap-report | 财务报表生成 |
| M12 | fin-olap-metadata | 元数据、任务记录 |
| M13 | fin-olap-security | 鉴权、审计、权限 |
| M14 | fin-olap-observability | 监控、日志、追踪 |

## 6. 查询执行流程

以以下 SQL 为例：

```sql
SELECT
    trade_date,
    account_id,
    transaction_type,
    COUNT(*) AS trade_count,
    SUM(amount_cents) AS total_amount
FROM transactions
WHERE trade_date BETWEEN DATE '2026-01-01'
                     AND DATE '2026-01-31'
GROUP BY
    trade_date,
    account_id,
    transaction_type;
```

### 6.1 逻辑执行计划

```text
LogicalAggregate
       |
LogicalProject
       |
LogicalFilter
       |
LogicalTableScan
```

### 6.2 物理执行计划

```text
HashAggregate(FINAL)
        |
Exchange(HASH_PARTITION)
        |
HashAggregate(PARTIAL)
        |
VectorizedFilter
        |
ParquetScan
```

### 6.3 分布式执行过程

```text
Coordinator
    |
    | 1. SQL 解析
    v
Logical Plan
    |
    | 2. 优化
    v
Physical Plan
    |
    | 3. 划分 Stage
    v
Distributed Scheduler
    |
    +----------------+----------------+
    |                |                |
    v                v                v
 Worker 1         Worker 2         Worker 3
    |                |                |
 Parquet A        Parquet B        Parquet C
    |                |                |
 Partial Agg      Partial Agg      Partial Agg
    |                |                |
    +----------------+----------------+
                     |
                Hash Shuffle
                     |
                Final Aggregate
                     |
                  Result
```

### 6.4 关键约束

分布式执行必须处理：

1. 数据倾斜。
2. Worker 失联。
3. 网络传输失败。
4. 中间结果重复发送。
5. 内存不足。
6. 查询取消。
7. 数据快照变更。
8. 查询超时。
9. Coordinator 故障。
10. Shuffle 数据清理。

尤其需要注意：如果 Worker 失败后重新执行任务，必须通过 Stage、Partition、Attempt ID 等机制避免重复计算结果被最终聚合接受。


# 文档三：软件详细设计说明书 LLD

## 7. Java 模块与接口设计

### 7.1 物理算子接口

```java
public interface PhysicalOperator extends AutoCloseable {

    void open(ExecutionContext context);

    ColumnBatch nextBatch();

    @Override
    void close();
}
```

接口语义：

| 方法 | 说明 |
|---|---|
| open | 初始化执行资源 |
| nextBatch | 获取下一批数据 |
| close | 释放资源 |

`nextBatch()` 返回 `null` 表示数据结束。这是简化设计，正式实现建议使用显式的 `BatchResult`，区分正常结束、取消和异常。

### 7.2 列式批次接口

```java
public interface ColumnBatch extends AutoCloseable {

    int rowCount();

    LongColumn getLongColumn(int index);

    IntColumn getIntColumn(int index);

    @Override
    void close();
}
```

具体实现可以封装 Arrow 的 `VectorSchemaRoot`。

需要定义明确的内存所有权：

- 谁创建批次。
- 谁持有批次。
- 谁负责释放。
- 是否允许多个算子共享。
- 异步传输时何时释放。

Arrow 使用堆外内存时，这些规则尤其重要。

### 7.3 HashAggregate 详细设计

```java
public interface AggregationFunction {

    void add(
        AggregateState state,
        ColumnBatch batch,
        int rowIndex
    );

    void merge(
        AggregateState target,
        AggregateState source
    );
}
```

建议将聚合划分为：

```text
Aggregation
    |
    +-- CountAggregation
    |
    +-- SumAggregation
    |
    +-- MinAggregation
    |
    +-- MaxAggregation
    |
    +-- AverageAggregation
```

AVG 不应直接合并各分区平均值，而应合并 `sum` 和 `count` 后再计算。

金额求和还应采用可检测溢出的定点计算方案。

### 7.4 HashAggregate 算法

```text
输入：ColumnBatch

1. 初始化 HashTable
2. 遍历输入批次
3. 计算 GROUP BY Key
4. 计算 Key Hash
5. 查询 HashTable
6. 若 Key 不存在，创建 AggregateState
7. 更新 COUNT、SUM
8. 检查内存使用量
9. 达到阈值时执行 Spill
10. 所有批次结束后输出结果
```

时间复杂度：

平均情况下约为 O(N)，但具体性能受到哈希冲突、分组基数、数据倾斜和内存布局影响。

### 7.5 内存管理设计

建议为每个查询设置独立的资源配额。

```java
public interface MemoryManager {

    MemoryReservation reserve(
        long bytes,
        String operatorId
    );

    long usedBytes();

    long limitBytes();
}
```

资源层次：

```text
Global Memory Manager
        |
        v
Query Memory Manager
        |
        v
Stage Memory Manager
        |
        v
Operator Memory Manager
```

必须设计：

- 内存预留。
- 内存释放。
- 内存不足检测。
- Spill 到磁盘。
- 资源清理。
- 查询取消后的回收。
- 堆内和堆外内存分别统计。

### 7.6 SQL 接口设计

提交查询：

```http
POST /api/v1/queries
Content-Type: application/json
```

请求：

```json
{
  "sql": "SELECT account_id, SUM(amount_cents) FROM transactions GROUP BY account_id",
  "snapshotId": "snapshot-20261026",
  "timeoutSeconds": 300,
  "resultFormat": "PARQUET"
}
```

响应：

```json
{
  "queryId": "q-10001",
  "status": "QUEUED",
  "submittedAt": "2026-10-26T10:00:00Z"
}
```

查询状态：

```text
QUEUED
   |
   v
PLANNING
   |
   v
RUNNING
   |
   +----> SUCCEEDED
   |
   +----> FAILED
   |
   +----> CANCELLED
```

所有查询应记录提交者、授权范围、数据快照、SQL 摘要、执行版本、耗时和结果位置。

---

# 第二部分：FIN-RISK 金融风险数值计算引擎

# 文档四：软件架构设计说明书 SAD

## 8. 文档基本信息

| 属性 | 内容 |
|---|---|
| 文档名称 | FIN-RISK 软件架构设计说明书 |
| 文档编号 | FIN-RISK-ARC-001 |
| 文档版本 | V1.0 Draft |
| 文档状态 | 待架构评审 |
| 系统名称 | Financial Risk Calculation Engine |
| 开发语言 | Java 17 |
| 系统类型 | 分布式金融风险计算系统 |
| 计算模式 | 批量计算 + 异步任务 |
| 部署环境 | Linux / Kubernetes |
| 主要业务 | 期权定价、VaR、Monte Carlo、组合风险 |
| 计算精度 | 以 double 为主，明确误差容限 |
| 主要参考 | OpenGamma Strata、QuantLib |

## 9. 系统业务目标

### 9.1 核心业务

系统支持五类计算。

| 编号 | 功能 | 计算方式 |
|---|---|---|
| R01 | 期权定价 | Black-Scholes、二叉树、Monte Carlo |
| R02 | Monte Carlo 模拟 | 随机路径生成 |
| R03 | VaR 风险价值 | 历史模拟、参数法、蒙特卡洛法 |
| R04 | 投资组合分析 | 收益、波动率、相关性 |
| R05 | 利率曲线计算 | 插值、贴现、现金流估值 |

### 9.2 业务数据

```text
Market Data
    |
    +-- Stock Prices
    +-- Interest Rates
    +-- FX Rates
    +-- Volatility Surfaces
    +-- Yield Curves

Portfolio Data
    |
    +-- Positions
    +-- Instruments
    +-- Trades
    +-- Cash Flows

Risk Configuration
    |
    +-- Valuation Date
    +-- Risk Factors
    +-- Scenarios
    +-- Confidence Level
    +-- Simulation Count
```

这些数据必须带有版本和估值日期，不能简单使用“当前最新数据”。

金融风险结果的可复现性依赖市场数据快照、模型版本、参数版本、随机数种子和计算环境。

## 10. 总体架构

```text
                   FIN-RISK PLATFORM
                           |
                      API Gateway
                           |
                     Risk Service
                           |
                  Calculation Manager
                           |
             +-------------+-------------+
             |             |             |
         Pricing       Portfolio       Risk
         Engine        Engine          Engine
             |             |             |
             +-------------+-------------+
                           |
                     Model Registry
                           |
                     Task Scheduler
                           |
             +-------------+-------------+
             |             |             |
          Worker 1      Worker 2      Worker N
             |             |             |
          CPU SIMD      CPU SIMD      CPU SIMD
             |             |             |
             +-------------+-------------+
                           |
                     Result Aggregator
                           |
                     Risk Result Store
                           |
                   Risk Report Service
```

## 11. 核心技术选型

| 技术领域 | 选择 | 说明 |
|---|---|---|
| JDK | Java 17 | 统一运行环境 |
| Web 框架 | Spring Boot | 对外服务 |
| RPC | gRPC | Worker 通信 |
| 数值计算 | Apache Commons Math、ojAlgo | 统计、矩阵与优化 |
| SIMD | Java Vector API | 批量数值计算 |
| 并行计算 | ExecutorService / ForkJoinPool | CPU 并行任务 |
| 数据库 | PostgreSQL | 任务和结果元数据 |
| 对象存储 | S3 兼容存储 | 大规模计算结果 |
| 指标 | Prometheus | 运行监控 |
| 基准测试 | JMH | 数值内核测试 |
| 自动化测试 | JUnit 5 | 正确性测试 |

风险计算平台与 OLAP 引擎不同：它通常不是 SQL 执行引擎，而是围绕金融模型、市场数据、估值和风险场景组织计算。

---

# 文档五：软件概要设计说明书 HLD

## 12. 核心模块划分

| 模块 | 名称 | 职责 |
|---|---|---|
| M01 | fin-risk-api | REST/gRPC 接口 |
| M02 | fin-risk-marketdata | 市场数据管理 |
| M03 | fin-risk-instrument | 金融工具定义 |
| M04 | fin-risk-pricing | 金融工具定价 |
| M05 | fin-risk-montecarlo | 蒙特卡洛计算 |
| M06 | fin-risk-var | VaR 计算 |
| M07 | fin-risk-portfolio | 投资组合分析 |
| M08 | fin-risk-curve | 利率曲线 |
| M09 | fin-risk-math | 数值计算基础库 |
| M10 | fin-risk-vector | SIMD 优化 |
| M11 | fin-risk-scheduler | 分布式任务调度 |
| M12 | fin-risk-worker | 计算节点 |
| M13 | fin-risk-model-registry | 模型及参数版本管理 |
| M14 | fin-risk-report | 风险报表 |
| M15 | fin-risk-observability | 监控和审计 |

## 13. Monte Carlo 计算流程

```text
Portfolio
    |
    v
Market Data Snapshot
    |
    v
Model Configuration
    |
    v
Random Number Generator
    |
    v
Scenario Generation
    |
    v
Path Simulation
    |
    v
Instrument Valuation
    |
    v
Portfolio P&L
    |
    v
Risk Aggregation
    |
    v
VaR / Expected Shortfall
    |
    v
Risk Report
```

### 13.1 分布式计算

假设需要执行 1 亿次模拟。

将模拟任务分为 1,000 个分片：

```text
Monte Carlo Job
       |
       v
1000 Simulation Partitions
       |
       v
Distributed Scheduler
       |
       +----------+----------+
       |          |          |
    Worker 1   Worker 2   Worker N
       |          |          |
    100000     100000     100000
    Paths      Paths      Paths
       |          |          |
       +----------+----------+
                  |
             Aggregation
                  |
             Risk Metrics
```

这里的 100,000 条路径是每个分片的数量，而不是每个 Worker 固定处理的总量。

为保证任务重试后结果可复现，随机数生成不能简单依赖各 Worker 的启动顺序。应采用由 Job ID、场景 ID、路径编号和固定种子确定的随机数生成策略。

---

# 文档六：软件详细设计说明书 LLD

## 14. 金融工具模型

建议首先支持以下金融工具：

```java
public sealed interface Instrument
        permits EuropeanOption, Bond, Forward {
}
```

欧式期权：

```java
public record EuropeanOption(
        String instrumentId,
        OptionType optionType,
        double strike,
        double maturityYears
) implements Instrument {
}
```

期权类型：

```java
public enum OptionType {
    CALL,
    PUT
}
```

市场数据：

```java
public record MarketData(
        double spot,
        double riskFreeRate,
        double volatility,
        double dividendYield
) {
}
```

正式版本还需要估值日期、货币、日历、计息规则、曲线标识、波动率曲面及市场数据版本。

## 15. 定价引擎接口

```java
public interface PricingEngine<I extends Instrument> {

    PricingResult price(
        I instrument,
        PricingContext context
    );
}
```

计算结果：

```java
public record PricingResult(
        double presentValue,
        double delta,
        double gamma,
        double vega,
        String modelVersion
) {
}
```

应将 `PricingContext` 设计为不可变对象，包含市场数据快照、估值日期、模型参数及数值精度策略。

### 15.1 Black-Scholes 模型

欧式看涨期权的经典公式：

\[
C=S_0e^{-qT}N(d_1)-Ke^{-rT}N(d_2)
\]

其中：

\[
d_1=
\frac{\ln(S_0/K)+(r-q+\sigma^2/2)T}
{\sigma\sqrt{T}}
\]

\[
d_2=d_1-\sigma\sqrt{T}
\]

变量含义：

- \(S_0\)：标的资产价格。
- \(K\)：执行价格。
- \(r\)：无风险利率。
- \(q\)：连续股息收益率。
- \(\sigma\)：波动率。
- \(T\)：到期时间。
- \(N(x)\)：标准正态分布累计概率。

该模型适用于特定假设下的欧式期权，不能直接作为所有金融衍生品的统一定价方法。

### 15.2 Java 实现

```java
public final class BlackScholesPricer {

    public double callPrice(
            double spot,
            double strike,
            double rate,
            double dividendYield,
            double volatility,
            double maturity) {

        if (spot <= 0 || strike <= 0 ||
            volatility <= 0 || maturity <= 0) {
            throw new IllegalArgumentException(
                "Invalid pricing parameters"
            );
        }

        double sqrtT = Math.sqrt(maturity);

        double d1 = (
            Math.log(spot / strike)
            + (rate - dividendYield
            + 0.5 * volatility * volatility) * maturity
        ) / (volatility * sqrtT);

        double d2 = d1 - volatility * sqrtT;

        return spot * Math.exp(-dividendYield * maturity)
                * normalCdf(d1)
             - strike * Math.exp(-rate * maturity)
                * normalCdf(d2);
    }

    private double normalCdf(double x) {
        // 使用经过验证的正态分布 CDF 实现。
        throw new UnsupportedOperationException(
            "Normal CDF implementation required"
        );
    }
}
```

这里的代码是接口和算法结构示例，并非完整可运行的定价实现。`normalCdf` 必须使用经过验证的数值实现，并测试尾部概率的精度。

## 16. Monte Carlo 详细设计

### 16.1 核心接口

```java
public interface MonteCarloEngine {

    SimulationResult simulate(
        SimulationRequest request
    );
}
```

任务请求：

```java
public record SimulationRequest(
        String jobId,
        long simulationCount,
        long randomSeed,
        int partitionCount,
        String modelVersion
) {
}
```

### 16.2 计算内核

对于几何布朗运动模型：

\[
S_T=S_0\exp\left[
(r-q-\frac{\sigma^2}{2})T
+\sigma\sqrt{T}Z
\right]
\]

其中 \(Z\) 为标准正态随机变量。

基础计算代码：

```java
public static double simulateTerminalPrice(
        double spot,
        double rate,
        double dividendYield,
        double volatility,
        double maturity,
        double normalRandom) {

    double drift =
        (rate - dividendYield
         - 0.5 * volatility * volatility) * maturity;

    double diffusion =
        volatility * Math.sqrt(maturity) * normalRandom;

    return spot * Math.exp(drift + diffusion);
}
```

该方法是单路径计算内核。

正式版本还需要：

- 批量路径计算。
- 随机数流管理。
- 方差缩减。
- 置信区间。
- 收敛性分析。
- SIMD 优化。
- 并行任务划分。
- 结果合并。
- 数值稳定性测试。

---

# 第三部分：两个项目的工业级工程规范

## 17. 代码分层规范

建议采用以下依赖方向：

```text
API / Application
        |
        v
Domain Services
        |
        v
Domain Model
        ^
        |
Infrastructure Adapters
```

核心计算模块不应直接依赖 Spring Boot。

例如：

```text
fin-risk-pricing
```

应该能够作为普通 Java Library 独立运行和测试。

这样可以：

- 独立进行 JMH 测试。
- 避免框架初始化影响计算性能。
- 支持命令行运行。
- 支持嵌入其他系统。
- 便于未来扩展 JNI、FFM 或 GPU 实现。

## 18. Git 仓库规范

两个仓库都建议使用：

```text
project/
│
├── .github/
│   ├── workflows/
│   │   ├── build.yml
│   │   ├── test.yml
│   │   ├── benchmark.yml
│   │   └── security.yml
│   │
│   └── CODEOWNERS
│
├── docs/
│   ├── 01-requirements/
│   ├── 02-architecture/
│   ├── 03-high-level-design/
│   ├── 04-low-level-design/
│   ├── 05-data-design/
│   ├── 06-api-design/
│   ├── 07-security/
│   ├── 08-performance/
│   ├── 09-testing/
│   ├── 10-operations/
│   └── adr/
│
├── modules/
├── benchmarks/
├── integration-tests/
├── deployment/
├── scripts/
├── pom.xml
└── README.md
```

### 18.1 代码质量要求

| 项目 | 建议规范 |
|---|---|
| Java | Google Java Style 或统一团队规范 |
| 静态分析 | Checkstyle、SpotBugs、PMD |
| 依赖漏洞 | OWASP Dependency-Check 等 |
| 单元测试 | JUnit 5 |
| 集成测试 | Testcontainers |
| 性能测试 | JMH |
| 覆盖率 | JaCoCo |
| API 文档 | OpenAPI |
| 架构测试 | ArchUnit |
| 依赖管理 | Maven BOM |
| 发布版本 | Semantic Versioning |
| 架构决策 | ADR |
| 代码审查 | Pull Request + CODEOWNERS |

测试覆盖率不能替代金融计算正确性验证。

## 19. 性能测试规范

### FIN-OLAP

| 测试编号 | 场景 | 数据规模 | 重点指标 |
|---|---|---|---|
| PERF-001 | 顺序扫描 | 1 亿行 | Rows/s |
| PERF-002 | 条件过滤 | 1 亿行 | Rows/s |
| PERF-003 | SUM 聚合 | 10 亿行 | 吞吐量 |
| PERF-004 | GROUP BY | 10 亿行 | 耗时、内存 |
| PERF-005 | 多线程聚合 | 1 亿行 | 扩展效率 |
| PERF-006 | 分布式聚合 | 10 亿行 | Shuffle 流量 |
| PERF-007 | Parquet 读取 | 100 GB | MB/s |
| PERF-008 | SIMD 聚合 | 1 亿个数值 | 加速比 |

### FIN-RISK

| 测试编号 | 场景 | 数据规模 | 重点指标 |
|---|---|---|---|
| PERF-101 | Black-Scholes 定价 | 100 万份合约 | Options/s |
| PERF-102 | Monte Carlo | 100 万路径 | Paths/s |
| PERF-103 | Monte Carlo | 1 亿路径 | 总耗时 |
| PERF-104 | VaR | 10 万场景 | 耗时 |
| PERF-105 | 矩阵运算 | 1000×1000 | GFLOPS |
| PERF-106 | SIMD 对比 | 100 万元素 | 加速比 |
| PERF-107 | 多线程扩展 | 1～32 线程 | 扩展效率 |
| PERF-108 | 分布式计算 | 3～10 节点 | 扩展效率 |

所有性能结果必须记录 CPU 型号、核心数、内存、JDK 版本、GC、JVM 参数、数据分布、预热方式和测量方法。

## 20. 开发里程碑

建议按四个阶段实施，而不是一开始就开发完整分布式平台。

| 阶段 | FIN-OLAP | FIN-RISK |
|---|---|---|
| V0.1 | 单机 CSV/Parquet 扫描、SUM、COUNT | Black-Scholes、基础 Monte Carlo |
| V0.2 | Arrow 列式执行、GROUP BY、JMH | 组合估值、VaR、数值验证 |
| V0.3 | Calcite SQL、并行计算、Spill | SIMD、并行模拟、模型注册 |
| V1.0 | Coordinator-Worker、Shuffle、故障恢复 | 分布式任务、结果审计、故障恢复 |

V1.0 仍需经过安全评审、金融模型验证、性能验收和生产运维评审，才能进入真实金融生产环境。

---

# 第四部分：对标国内外成熟系统

两个项目应该分别对标不同类型的成熟软件。

| 项目 | 对标系统 | 重点研究 |
|---|---|---|
| FIN-OLAP | [Apache Doris](https://doris.apache.org/?utm_source=chatgpt.com) | MPP、向量化、聚合 |
| FIN-OLAP | [ClickHouse](https://clickhouse.com/?utm_source=chatgpt.com) | 列式存储、查询执行 |
| FIN-OLAP | [Apache Spark](https://spark.apache.org/?utm_source=chatgpt.com) | 分布式调度、SQL 执行 |
| FIN-OLAP | [Trino](https://trino.io/?utm_source=chatgpt.com) | Java MPP 查询引擎 |
| FIN-RISK | [OpenGamma Strata](https://strata.opengamma.io/?utm_source=chatgpt.com) | Java 金融产品建模、定价、风险计算 |
| FIN-RISK | [QuantLib](https://www.quantlib.org/?utm_source=chatgpt.com) | 金融数学、模型与数值算法 |
| FIN-RISK | [ojAlgo](https://www.ojalgo.org/?utm_source=chatgpt.com) | Java 矩阵计算与优化 |

其中，**Trino 和 OpenGamma Strata 特别适合研究 Java 工业级源码**。

Trino 可以重点阅读查询计划、Stage、Task、Operator 和内存管理；Strata 可以重点阅读金融工具模型、市场数据、定价函数、敏感度计算和测试组织方式。

这两个系统的研究价值，比简单使用 Spring Boot 搭建两个计算接口更高。

---

# 第五部分：工业级设计还必须补充的关键内容

前面六份文档构成架构、概要和详细设计的主体。若要按照大型金融机构的正式软件设计评审要求推进，还需要明确以下内容。

## 17. 数据正确性与数值精度设计

这是两个系统最重要的区别。

| 设计维度 | FIN-OLAP | FIN-RISK |
|---|---|---|
| 计算类型 | 财务金额、交易笔数、统计指标 | 金融模型、概率统计、风险指标 |
| 核心数据类型 | long、Decimal128、BigDecimal | double、矩阵、向量 |
| 精度要求 | 财务金额不得出现未经定义的舍入误差 | 必须定义数值误差容限 |
| 溢出处理 | 必须检测并拒绝静默溢出 | 检测 NaN、Infinity 和数值异常 |
| 并行合并 | 整数精确聚合或受控十进制聚合 | 稳定求和、确定性归约 |
| 正确性验证 | 财务对账、SQL 参考结果 | 解析解、基准模型、统计检验 |
| 版本管理 | 数据快照、指标口径 | 市场数据、模型、参数、随机数种子 |

例如 FIN-RISK 的 Monte Carlo 计算，需要区分：

**数值正确性：** 算法实现是否符合数学模型。

**统计正确性：** 模拟结果是否在预期置信区间内。

**金融业务正确性：** 使用的模型、市场数据、风险因子及计量方法是否符合业务要求。

VaR 还必须明确持有期、置信水平、损益定义、估值方式及分位数计算规则，并通过回测评估模型表现。

## 22. 高可用与故障恢复设计

两个项目应统一设计任务状态机：

```text
CREATED
   |
   v
VALIDATED
   |
   v
QUEUED
   |
   v
RUNNING
   |
   +------> SUCCEEDED
   |
   +------> RETRYING
   |            |
   |            +------> RUNNING
   |
   +------> FAILED
   |
   +------> CANCELLED
```

关键设计要求：

- Coordinator 无状态化或实现明确的主备选举机制。
- 任务状态持久化。
- Worker 心跳和租约机制。
- 任务超时与取消。
- 有界重试和指数退避。
- 分区任务幂等执行。
- 结果提交去重。
- 中间文件生命周期管理。
- 系统重启后任务恢复。
- 不允许将部分计算结果标记为完整成功。

对于 FIN-RISK，还应保证任务重新调度后使用相同的模型、参数和市场数据快照。

## 23. 安全与审计设计

金融计算系统不能只依靠 API Gateway 鉴权。

建议建立四级安全控制：

| 层级 | 主要措施 |
|---|---|
| 用户层 | OIDC、RBAC、必要时 ABAC |
| 服务层 | mTLS、服务身份认证 |
| 数据层 | 行列权限、敏感字段脱敏、加密 |
| 审计层 | 查询审计、计算审计、结果审计 |

每次正式报表或风险计算至少记录：

```json
{
  "jobId": "risk-job-20261026-001",
  "operatorId": "user-001",
  "modelVersion": "1.2.0",
  "marketDataSnapshot": "md-20261026",
  "parameterVersion": "param-003",
  "engineVersion": "1.0.0",
  "executionStatus": "SUCCEEDED",
  "resultChecksum": "sha256:...",
  "completedAt": "2026-10-26T12:30:00Z"
}
```

这只是审计数据结构示例。生产环境还需要审计日志完整性保护、保留期限、访问控制及敏感信息处理策略。

---

# 第六部分：最终 Git 仓库与文档交付结构

建议两个仓库分别维护完整的设计文档，不要将两套系统混在一个项目中。

### 仓库一：`fin-olap-engine-java`

```text
fin-olap-engine-java/
├── docs/
│   ├── requirements/
│   │   └── SRS.md
│   ├── architecture/
│   │   └── SAD.md
│   ├── high-level-design/
│   │   └── HLD.md
│   ├── low-level-design/
│   │   ├── SQL-Engine.md
│   │   ├── Query-Planner.md
│   │   ├── Vectorized-Execution.md
│   │   ├── Hash-Aggregation.md
│   │   ├── Distributed-Scheduler.md
│   │   ├── Shuffle.md
│   │   └── Memory-Management.md
│   ├── data-design/
│   ├── api-design/
│   ├── security/
│   ├── performance/
│   ├── testing/
│   ├── operations/
│   └── adr/
├── modules/
├── benchmarks/
├── integration-tests/
├── deployment/
└── pom.xml
```

### 仓库二：`fin-risk-engine-java`

```text
fin-risk-engine-java/
├── docs/
│   ├── requirements/
│   │   └── SRS.md
│   ├── architecture/
│   │   └── SAD.md
│   ├── high-level-design/
│   │   └── HLD.md
│   ├── low-level-design/
│   │   ├── Instrument-Model.md
│   │   ├── Pricing-Engine.md
│   │   ├── Black-Scholes.md
│   │   ├── Monte-Carlo.md
│   │   ├── VaR-Engine.md
│   │   ├── Portfolio-Engine.md
│   │   ├── Yield-Curve.md
│   │   ├── SIMD-Optimization.md
│   │   └── Distributed-Calculation.md
│   ├── model-validation/
│   ├── data-design/
│   ├── api-design/
│   ├── security/
│   ├── performance/
│   ├── testing/
│   ├── operations/
│   └── adr/
├── modules/
├── benchmarks/
├── integration-tests/
├── deployment/
└── pom.xml
```

## 设计文档评审标准

建议采用以下检查项作为两个项目进入开发阶段的门槛。

| 评审领域 | 通过标准 |
|---|---|
| 需求完整性 | 每项功能有唯一编号及验收条件 |
| 架构合理性 | 关键组件职责、边界、依赖清晰 |
| 详细设计 | 核心类、接口、算法和状态机明确 |
| 数据设计 | 数据类型、精度、生命周期明确 |
| 一致性 | 快照、重试、结果提交语义明确 |
| 性能设计 | 有容量模型、测试场景和性能预算 |
| 可靠性 | 单点故障、恢复和资源耗尽有处理方案 |
| 安全性 | 身份、权限、数据保护、审计设计完整 |
| 可测试性 | 每项关键需求可关联测试用例 |
| 可运维性 | 监控、日志、告警、部署和回滚方案明确 |

## 最终技术路线建议

**FIN-OLAP：重点研究 Java 分布式系统和数据库内核。**

建议优先实现：

`Parquet Scan → Arrow Batch → Filter → HashAggregate → Parallel Execution → SQL Planner → Distributed Execution`

**FIN-RISK：重点研究 Java 高性能数值计算。**

建议优先实现：

`Financial Instruments → Black-Scholes → Monte Carlo → VaR → Portfolio Risk → SIMD → Distributed Calculation`

两个项目均应采用**先验证单机计算正确性，再优化 CPU 性能，最后实现分布式计算**的开发顺序。

以上是 V1.0 设计基线，包含总体架构、主要模块、核心接口、关键算法和工程规范。它还不是逐类、逐接口、逐状态转移的完整 LLD：例如分布式 Shuffle 协议、查询优化规则、VaR 具体估计器、数据库 DDL、API 错误码、任务恢复事务边界等，需要在各自的详细设计子文档中进一步展开。

**建议以 Trino 的 Java 查询执行架构作为 FIN-OLAP 的主要工程参照，以 OpenGamma Strata 的金融领域模型作为 FIN-RISK 的主要工程参照。** 两者都能提供比普通 CRUD 项目更深入的 Java 工业级系统设计和源码研究价值。